"""
Reads nested JSON from any Spark-readable location.

Because Spark's DataFrameReader works off a URI scheme, this module doesn't
need special-case logic per cloud provider - abfss://, s3://, s3a://, gs://,
dbfs:/, /Volumes/..., and local file:/ paths all go through the same code
path as long as the cluster/session has the right auth configured. This
function just centralizes the read options driven by config.
"""

from typing import Optional

from .config import IngestionConfig
from .logging_utils import logger
from .retry import with_retry

#: Extensions that mean "one JSON value per line" by definition of the
#: format. `.json` is deliberately absent: a .json file may legitimately be
#: either a single pretty-printed document or JSON-lines, so only the config
#: can decide for it.
#:
#: Public because `streaming_reader` needs the same rule (#146). The batch
#: and streaming readers are peers, so this could equally live in a module
#: of its own - it stays here because this is where "how JSON is read" is
#: already documented, and two functions do not earn a module.
JSON_LINES_EXTENSIONS = (".jsonl", ".ndjson")


def is_json_lines_path(path: Optional[str]) -> bool:
    """
    True when `path` names a FILE whose extension states JSON-lines.

    False for a directory, for `.json`, and for an empty/None path - none of
    which are unambiguous enough to override a config value.

    A query string is stripped first so a signed URL
    (`.../events.jsonl?sig=...`) is still recognised, and a trailing slash is
    stripped so a directory never matches by accident.
    """
    cleaned = (path or "").split("?", 1)[0].rstrip("/")
    return cleaned.lower().endswith(JSON_LINES_EXTENSIONS)


def effective_multiline(config: IngestionConfig) -> bool:
    """
    `multiLine` to actually use, which is not always `config.multiline`.

    `multiline` is one flag on a config that a directory run shares across
    every file it discovers, but the correct value is a property of each
    individual FILE. Discovery accepts `.json` and `.jsonl`; the deployed
    job sets `multiline: "true"`, which is right for the pretty-printed
    `.json` documents this package was built for and silently wrong for
    every `.jsonl` file in the same folder.

    Silently is the operative word, and it is why this is a data-loss bug
    rather than a configuration annoyance (#146). Measured against a local
    Spark session on a 3-record `.jsonl` file:

        multiLine=false -> 3 rows
        multiLine=true  -> 1 row

    No error, no warning, and nothing in `_corrupt_record` - Spark parses
    the first JSON value in the file and discards the remaining bytes. The
    run reports success, the audit row records a row count that looks
    plausible, and the other 2 records are simply gone.

    So: when the path names a file whose extension means JSON-lines, that
    wins over the config, because the extension is a statement about the
    file's format and the config is a default across many files. A path
    that is a directory, or a `.json` file, keeps the configured value -
    neither is unambiguous enough to override.

    An override is logged at WARNING rather than applied silently: the
    config asked for something that was not honoured, and that should be
    visible in the run log even though the outcome is correct. A file
    genuinely named `.jsonl` that contains one pretty-printed document is
    misnamed, and the warning is the thread to pull.

    Escape hatch: `reader_options` is applied after this in `read_json`, so
    `reader_options: {multiLine: "true"}` still forces the issue if a
    source really does need it.

    Streaming uses this too, but it can only ever help there when
    `source_path` names a single file. Auto Loader is normally pointed at a
    DIRECTORY, where there is no extension to inspect and files that do not
    exist yet cannot be classified at all - so the streaming path pairs this
    with a per-micro-batch guard. See `streaming_reader`.
    """
    path = (config.source_path or "").split("?", 1)[0].rstrip("/")
    if not is_json_lines_path(path):
        return config.multiline

    if config.multiline:
        logger.warning(
            "%s has a JSON-lines extension, so reading it with multiLine=false "
            "despite multiline=True in config. multiLine=true on a JSON-lines "
            "file silently returns only its first record (#146). Set "
            "reader_options={'multiLine': 'true'} to override if this file "
            "really is a single JSON document.",
            path,
        )
    return False


def read_json(spark, config: IngestionConfig):
    """
    Batch read of nested JSON from config.source_path into a DataFrame.

    Production behavior:
      - mode=PERMISSIVE (Spark default) so unparseable records don't kill the
        whole job; they land in `config.corrupt_record_column` instead of
        being silently dropped or failing the read.
      - When a schema_hint_ddl is supplied, `rescued_data_column` captures
        any fields present in the source JSON that don't fit that schema
        (extra/renamed fields), so nothing is silently lost on drift.
      - `multiLine` comes from `effective_multiline(config)`, not straight
        from config: a `.jsonl`/`.ndjson` path forces it off, since
        multiLine=true on a JSON-lines file returns only its first record
        with no error (#146).
      - Adds `_input_file_name` for lineage, used later for the audit
        `_source_file` column.
      - The actual load (which triggers schema inference / file listing,
        and can hit transient cloud-storage errors) is wrapped in the same
        exponential-backoff retry as the write path, via
        `retry_attempts` / `retry_delay_seconds`.
    """
    reader = (
        spark.read.format("json")
        # Not config.multiline directly - a JSON-lines extension overrides
        # it, because multiLine=true on such a file silently drops every
        # record but the first (#146). See effective_multiline.
        .option("multiLine", effective_multiline(config))
        .option("mode", "PERMISSIVE")
        .option("columnNameOfCorruptRecord", config.corrupt_record_column)
    )

    if config.schema_hint_ddl:
        # rescuedDataColumn only has an effect when an explicit schema is provided -
        # otherwise Spark infers a schema that already includes every field.
        reader = reader.option("rescuedDataColumn", config.rescued_data_column)

    if config.reader_options:
        # Log the KEYS, never the values (#154/#115): once reader_options can
        # carry a secret reference, printing values would put it in the run
        # log. Keys are enough to answer "what was applied to this read?".
        logger.info(
            "Applying reader_options: %s",
            sorted(config.reader_options),
        )
        for key, value in config.reader_options.items():
            reader = reader.option(key, value)

    if config.schema_hint_ddl:
        reader = reader.schema(config.schema_hint_ddl)

    from pyspark.sql.functions import col

    @with_retry(attempts=config.retry_attempts, delay_seconds=config.retry_delay_seconds)
    def _do_read():
        # Track provenance regardless of flatten mode - cheap and always useful in bronze.
        # Uses _metadata.file_path (works on Unity Catalog shared clusters,
        # unlike input_file_name()).
        return reader.load(config.source_path).select(
            "*", col("_metadata.file_path").alias("_input_file_name")
        )

    return _do_read()
