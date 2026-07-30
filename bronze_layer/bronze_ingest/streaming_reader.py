"""
Incremental ingestion via Databricks Auto Loader (cloudFiles).

This is the production-recommended path for anything beyond a one-off
backfill: Auto Loader tracks which files have already been processed
(via the checkpoint), handles new files landing continuously or on a
schedule, and evolves the schema safely instead of re-scanning the whole
source directory on every run.
"""

from typing import Iterable, List

from pyspark.sql.functions import col, lower

from .config import IngestionConfig
from .json_reader import JSON_LINES_EXTENSIONS, effective_multiline, is_json_lines_path
from .logging_utils import logger

#: How many offending file names to name in the error. Enough to recognise
#: the pattern, few enough that the message stays readable when an entire
#: directory is JSON-lines.
_MAX_REPORTED_FILES = 5


class JsonLinesTruncationError(Exception):
    """
    Raised when a streaming micro-batch was read with `multiLine=true` but
    contains files whose extension says JSON-lines (#146).

    Deliberately fatal. See `assert_no_silent_truncation` for why failing is
    the recoverable outcome and succeeding is not.
    """


def should_guard_truncation(config: IngestionConfig) -> bool:
    """
    Whether the truncation guard applies to this config at all.

    The guard is not asking "what `multiLine` did the reader use?" - it is
    asking the narrower question "could this silently truncate, and has the
    operator not already told us they know?". Two conditions:

    **1. An explicit `reader_options: {multiLine: ...}` suppresses the
    guard, whatever its value.** `read_json_stream` applies `reader_options`
    last, so such an entry is the operator overriding the package on
    exactly this point. Both values end up in the same place:

      - `"false"` - the read is correct, there is nothing to guard
      - `"true"` - the documented escape hatch for `.jsonl` files that are
        really single JSON documents. The operator asked for this parse and
        must not be blocked from it

    Because the value does not change the answer, it is never coerced -
    which removes the YAML-string-vs-bool question entirely rather than
    solving it.

    An earlier version of this resolved the applied value instead and then
    guarded on it, which read as more precise and was wrong: it fired on
    the escape hatch, turning the documented way out of this failure into
    another instance of it.

    **2. Otherwise, guard exactly when `multiLine` is on.** With it off,
    JSON-lines files read correctly, and a single JSON document read as
    JSON-lines fails LOUDLY into `_corrupt_record` - visible already, and
    not this function's problem.

    Note the asymmetry with the batch reader: there, `.jsonl` in the path
    forces `multiLine` off and the file is simply read correctly. Here
    `source_path` is usually a directory, so `effective_multiline` returns
    the configured value unchanged and this returns True - which is the
    whole reason the guard exists.
    """
    if "multiLine" in (config.reader_options or {}):
        return False
    return effective_multiline(config)


def json_lines_files(paths: Iterable[str]) -> List[str]:
    """
    The subset of `paths` whose extension means JSON-lines, deduplicated and
    sorted.

    Split out from the DataFrame work so the decision this guard makes is a
    pure function that can be tested without a Spark session - which matters
    because Auto Loader cannot run outside Databricks at all, so the
    surrounding code is not locally testable.
    """
    return sorted({p for p in paths if p and is_json_lines_path(p)})


def read_json_stream(spark, config: IngestionConfig):
    reader = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", config.schema_location)
        .option("cloudFiles.schemaEvolutionMode", config.schema_evolution_mode)
        # Not config.multiline directly (#146) - same rule as the batch
        # reader. This only bites when source_path names a single .jsonl
        # file; for the usual directory source it returns config.multiline
        # unchanged and `assert_no_silent_truncation` is what protects the
        # data. See that function.
        .option("multiLine", effective_multiline(config))
        .option("rescuedDataColumn", config.rescued_data_column)
    )

    if config.max_files_per_trigger:
        reader = reader.option(
            "cloudFiles.maxFilesPerTrigger",
            config.max_files_per_trigger,
        )

    if config.schema_hint_ddl:
        reader = reader.schema(config.schema_hint_ddl)

    if config.reader_options:
        # Keys only - see the equivalent note in json_reader (#154/#115).
        logger.info("Applying reader_options: %s", sorted(config.reader_options))
        for key, value in config.reader_options.items():
            reader = reader.option(key, value)

    df = reader.load(config.source_path)

    # Unity Catalog lineage
    df = (
        df.select("*", "_metadata")
        .withColumn("_input_file_name", col("_metadata.file_path"))
        .drop("_metadata")
    )

    return df


def assert_no_silent_truncation(
    micro_batch_df, config: IngestionConfig, lineage_col: str = "_input_file_name"
):
    """
    Fails the micro-batch if it was read with `multiLine=true` and contains
    JSON-lines files, whose records have therefore already been discarded.

    Why this exists as well as `effective_multiline` (#146)
    ------------------------------------------------------
    The batch reader decides `multiLine` per file, because directory
    ingestion enumerates files and reads them one at a time. Auto Loader
    cannot work that way: it is handed a directory and one `multiLine` value
    fixed when the stream is constructed, and it streams whatever appears in
    that directory afterwards. A file that does not exist yet cannot be
    classified by any amount of validation at config load - which is why
    this is a runtime guard and not a `__post_init__` check.

    What goes wrong without it is the same silent loss the batch path had:
    `multiLine=true` on a JSON-lines file parses the first JSON value and
    discards the rest of the bytes. No error, nothing in `_corrupt_record`,
    a plausible row count on the audit row. 10,000 records become 1.

    Why raising is the SAFE option here
    -----------------------------------
    This runs after the read, so the records are already gone from this
    micro-batch - raising cannot recover them in-flight, and at first glance
    failing looks like it only converts silent loss into an outage.

    It does more than that, and this is the whole point. Structured
    Streaming commits a batch to the checkpoint only when the `foreachBatch`
    handler returns normally. Raising means the batch is never committed, so
    Auto Loader does not mark these files as processed: after the config is
    corrected and the stream restarted, the same files are read again, in
    full. The data is recovered.

    Succeeding is what makes the loss permanent. A committed batch advances
    the checkpoint past those files, and a later run sees no new files to
    read - there is no second chance and no signal that one is needed. That
    asymmetry is why this raises rather than warns.

    Cost
    ----
    One filtered scan of a micro-batch that is about to be scanned anyway,
    bounded by `limit` so a fully-offending batch stops after a handful of
    rows rather than collecting every path to the driver (the mistake #155
    documents in `replay.py`). On the healthy path the filter matches
    nothing and this is one pass with no shuffle and no collect.
    """
    if not should_guard_truncation(config):
        return

    if lineage_col not in micro_batch_df.columns:
        # read_json_stream always attaches this. A caller passing a
        # hand-built DataFrame gets no protection rather than a crash -
        # this guard must never be the reason a working stream fails.
        logger.warning(
            "%s not present on the micro-batch, so the JSON-lines truncation "
            "guard (#146) cannot run. Records in any .jsonl file in this batch "
            "may have been silently discarded.",
            lineage_col,
        )
        return

    ext_match = None
    for ext in JSON_LINES_EXTENSIONS:
        cond = lower(col(lineage_col)).endswith(ext)
        ext_match = cond if ext_match is None else (ext_match | cond)

    # distinct() BEFORE limit(), so the cap counts distinct FILES rather
    # than rows. Limiting first would sample rows, and a batch where one
    # .jsonl file contributed every row would report a single offender and
    # omit the "possibly others" hint while other offending files went
    # unmentioned - an error message that quietly understates the problem.
    # The distinct is a shuffle, but only over rows that already matched the
    # filter: nothing on the healthy path, and on the failing path this
    # batch is being abandoned anyway.
    offenders = json_lines_files(
        row[0]
        for row in (
            micro_batch_df.select(lineage_col)
            .filter(ext_match)
            .distinct()
            .limit(_MAX_REPORTED_FILES + 1)
            .collect()
        )
    )
    if not offenders:
        return

    shown = offenders[:_MAX_REPORTED_FILES]
    more = " (and possibly others)" if len(offenders) > _MAX_REPORTED_FILES else ""

    raise JsonLinesTruncationError(
        f"This micro-batch was read with multiLine=true but contains JSON-lines "
        f"file(s){more}: {shown}. multiLine=true on a JSON-lines file returns only "
        f"its FIRST record and discards the rest with no error (#146), so these "
        f"rows are incomplete and have not been written.\n\n"
        f"This batch has not been committed, so the checkpoint at "
        f"{config.checkpoint_location!r} has NOT advanced past these files. They "
        f"will be re-read in full once the config is corrected - nothing is lost "
        f"yet.\n\n"
        f"To fix, choose one:\n"
        f"  - set multiline: false, if this source is JSON-lines (most likely)\n"
        f"  - point this stream at a source containing only one of the two "
        f"formats, and run a second stream for the other\n"
        f"  - set reader_options: {{'multiLine': 'true'}} to override deliberately, "
        f"if these files are misnamed and really are single JSON documents"
    )


def get_trigger_kwargs(config: IngestionConfig):
    if config.trigger_mode == "availableNow":
        return {"availableNow": True}

    if config.trigger_mode == "once":
        return {"once": True}

    if config.trigger_mode == "processingTime":
        return {"processingTime": config.trigger_processing_time}

    raise ValueError(f"Unknown trigger_mode: {config.trigger_mode}")
