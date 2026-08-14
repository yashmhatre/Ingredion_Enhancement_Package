"""
Batch-read dispatch: which reader runs for a given `source_format`.

Before this module existed, `pipeline.py`'s `BronzeIngestion.read()` and
`directory_ingestion.py`'s `_ingest_folder_as_table` each called
`json_reader.read_json` directly - two independent call sites with no shared
dispatcher. Adding a second format without one would have meant writing the
same if/else ladder twice, in two files that have no reason to import each
other, which is exactly the shape that lets two copies of one decision drift
apart (see `formats.py`'s module docstring for the same argument applied to
extensions and reader-option allowlists).

So the dispatch lives here, once: `_BATCH_READERS` maps `source_format` to
the zero-surprise callable that reads it, and `read_source` is the only
thing either call site needs to know about.

**This closes the other half of `formats.py`'s promise.** That module's
docstring has said, since before this file existed, that adding a format is
"two edits, in two issues: an entry [in `formats.FORMATS`], and a line in
`readers._BATCH_READERS`" and that "neither alone does anything". Until this
module existed, that second half was aspirational - `config.py` was the only
consumer of the registry, so a `FormatSpec` added with no reader would
validate clean, discover files, and read nothing, silently. See
`test_readers.py::test_every_registered_format_has_a_batch_reader` for the
test that makes the claim enforced rather than merely written down.

Today `_BATCH_READERS` has exactly one entry. This is a #306 change only -
`read_json` is unmodified and still does all the work for "json"; nothing
here reshapes or reinterprets what it reads. csv/parquet/xml are #310/#313/
#317, not this issue.
"""

from typing import Any, Callable, Dict

from .config import IngestionConfig
from .json_reader import effective_multiline, read_json

#: `source_format` -> the batch reader for it. `config.__post_init__`
#: already restricts `source_format` to `formats.supported_formats()`, so in
#: normal operation every key `read_source` looks up here is guaranteed to
#: exist by the time a real `IngestionConfig` reaches it. The `KeyError`
#: handling in `read_source`/`batch_reader_options` below exists anyway,
#: for the moment (see both docstrings) where `formats.FORMATS` gains an
#: entry this dict has not caught up with yet.
_BATCH_READERS: Dict[str, Callable[[Any, IngestionConfig], Any]] = {
    "json": read_json,
}


def read_source(spark, config: IngestionConfig):
    """
    Batch-reads `config.source_path` per `config.source_format`, dispatching
    to the reader registered in `_BATCH_READERS`.

    This is the single call both `BronzeIngestion.read()` and
    `directory_ingestion._ingest_folder_as_table` make; neither calls a
    format-specific reader directly any more.

    Raises `ValueError` - never returns `None`, never falls through to the
    JSON reader - when `config.source_format` has no registered entry here.
    A silent fallback to JSON would be the worst of the three options: it
    would make a half-wired format (registered in `formats.FORMATS`, not yet
    given a reader) look like it works, for exactly as long as nobody checks
    the columns it produced.
    """
    try:
        reader = _BATCH_READERS[config.source_format]
    except KeyError:
        raise ValueError(
            f"No registered batch reader for source_format={config.source_format!r}. "
            f"Registered readers: {sorted(_BATCH_READERS)}."
        ) from None
    return reader(spark, config)


def _json_batch_reader_options(config: IngestionConfig) -> Dict[str, Any]:
    """
    The Spark reader options `read_json` applies via `.option(...)`, as a
    plain dict, in the same order/precedence `read_json` applies them:
    the fixed parsing options first, `rescuedDataColumn` only when a schema
    hint is set (mirroring `read_json`'s own condition), then
    `config.reader_options` layered on top so an explicit override wins -
    same as `read_json`'s `for key, value in config.reader_options.items()`
    loop, which runs last.

    Deliberately excludes the read SCHEMA: `read_json` sets that via
    `.schema(...)`, a different call on the reader than `.option(...)`, so
    it is not part of "the option dict" this returns.

    This does not change what `read_json` does - it is a second,
    independent description of the same decision, kept next to it so a
    change to one is easy to notice needs a change to the other. It exists
    so the decision can be asserted on without a SparkSession, the same
    reason `streaming_reader.json_lines_files` was split out of the
    DataFrame-touching code around it (`streaming_reader.py:69-79`).
    """
    options: Dict[str, Any] = {
        "multiLine": effective_multiline(config),
        "mode": "PERMISSIVE",
        "columnNameOfCorruptRecord": config.corrupt_record_column,
    }
    if config.schema_hint_ddl:
        options["rescuedDataColumn"] = config.rescued_data_column
    if config.reader_options:
        options.update(config.reader_options)
    return options


#: `source_format` -> the pure function computing that format's batch
#: reader options. Kept as its own table, parallel to `_BATCH_READERS`
#: rather than folded into it, because `batch_reader_options` answers a
#: narrower question ("what options WOULD be applied?") than `read_source`
#: ("read the data") - the two can be tested independently, and the second
#: needs no SparkSession to do it.
_BATCH_READER_OPTIONS: Dict[str, Callable[[IngestionConfig], Dict[str, Any]]] = {
    "json": _json_batch_reader_options,
}


def batch_reader_options(config: IngestionConfig) -> Dict[str, Any]:
    """
    The Spark reader options `config.source_format`'s batch reader would
    apply, as a plain dict - no SparkSession required.

    Exists so CI can assert on what a read would configure without needing
    Spark/Java at all, which matters concretely in this repo: the Spark
    suite reports NOT RUN on Windows (no winutils.exe - see AGENTS.md), so a
    behavior expressed only as Spark reader calls is untestable on a
    contributor's machine until CI runs. A pure function is testable
    everywhere, immediately.

    Raises `ValueError` for a `source_format` with no registered entry, same
    condition and reasoning as `read_source`.
    """
    try:
        builder = _BATCH_READER_OPTIONS[config.source_format]
    except KeyError:
        raise ValueError(
            f"No registered reader options for source_format={config.source_format!r}. "
            f"Registered: {sorted(_BATCH_READER_OPTIONS)}."
        ) from None
    return builder(config)
