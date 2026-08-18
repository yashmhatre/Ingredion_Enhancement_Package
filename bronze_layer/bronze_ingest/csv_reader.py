"""
Reads CSV from any Spark-readable location.

Peer of `json_reader.read_json`: same option ordering, the same
`@with_retry` wrapper, and the same `_metadata.file_path` lineage select.
`formats.FORMATS` and `readers._BATCH_READERS` register it as the batch CSV
implementation.
"""

from typing import Any

from .config import IngestionConfig
from .logging_utils import logger
from .retry import with_retry


def read_csv(spark: Any, config: IngestionConfig):
    """
    Batch read of CSV from config.source_path into a DataFrame.

    Production behavior:
      - `header` and `inferSchema` come from `config.csv_header` /
        `config.csv_infer_schema` - CSV's own knobs, added for exactly this
        purpose (#308).
      - `mode=PERMISSIVE` (Spark's default) unconditionally, so unparseable
        rows don't kill the whole job, same as `read_json`.
      - `config.multiline` is NEVER read here. It is a JSON-only field:
        `multiLine` means "this file is one document, not one per line" for
        JSON, but "a quoted field may contain a newline" for CSV - an
        unrelated meaning attached to the same option name. The deployed
        job (`bronze_layer/resources/bronze_ingest_jobs.yml`) hardcodes
        `multiline: "true"` for JSON's benefit; if this reader took that
        value, every CSV run would silently set `multiLine=true` too. A
        source whose quoted fields genuinely contain newlines can still set
        it, explicitly, via `reader_options={'multiLine': 'true'}` - the
        same escape hatch `read_json` documents for its own override.
        Deliberately no warning here (unlike `json_reader.effective_multiline`):
        `multiline` defaults to `True` and the bundle sets it explicitly too,
        so config has no way to tell "the operator asked for this" from "this
        is just the default" - a warning would fire on every single CSV run.
      - `columnNameOfCorruptRecord` and `rescuedDataColumn` are set ONLY
        when `schema_hint_ddl` is supplied - one branch, mirroring how
        `read_json` gates `rescuedDataColumn` (rescued data only has an
        effect once a schema is explicit; an inferred schema already
        includes every field). For CSV this is not merely mirroring
        `read_json`'s style - it is required: Spark only materialises
        `columnNameOfCorruptRecord` for CSV when that column is DECLARED in
        the read schema. Against an inferred schema (the default here),
        malformed rows do not raise and do not populate the corrupt-record
        column; they surface as NULLs in whatever columns failed to parse,
        indistinguishable from genuinely missing values. There is no
        runtime warning for this, and the column is never auto-appended to
        an operator-supplied `schema_hint_ddl` - if corrupt-record capture
        is needed, the operator must name the column in their own DDL.
      - `config.reader_options` is applied after the fixed options above,
        so an explicit override always wins - same precedence as
        `read_json`. Only the option KEYS are logged, never the values
        (#154/#115): once `reader_options` can carry a secret reference,
        logging values would put it in the run log.
      - `.schema(config.schema_hint_ddl)` is applied last, when set.
      - Adds `_input_file_name` for lineage (`_metadata.file_path`, works
        on Unity Catalog shared clusters unlike `input_file_name()`), used
        later for the audit `_source_file` column.
      - The actual load (file listing, schema inference if enabled, and
        anything that can hit a transient cloud-storage error) is wrapped
        in the same exponential-backoff retry as `read_json` / the write
        path, via `retry_attempts` / `retry_delay_seconds` /
        `retry_max_total_seconds`.

    No reshaping of any kind - bronze stays flattening-free
    (`docs/bronze_silver_contract.md`).
    """
    reader = (
        spark.read.format("csv")
        .option("header", config.csv_header)
        .option("inferSchema", config.csv_infer_schema)
        .option("mode", "PERMISSIVE")
    )

    if config.schema_hint_ddl:
        # Both only have an effect with an explicit schema - see the
        # docstring above for why that is a hard Spark requirement for CSV's
        # columnNameOfCorruptRecord specifically, not just a style choice.
        reader = reader.option("columnNameOfCorruptRecord", config.corrupt_record_column)
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

    @with_retry(
        attempts=config.retry_attempts,
        delay_seconds=config.retry_delay_seconds,
        max_total_seconds=config.retry_max_total_seconds,
    )
    def _do_read():
        # Track provenance regardless of flatten mode - cheap and always useful in bronze.
        # Uses _metadata.file_path (works on Unity Catalog shared clusters,
        # unlike input_file_name()).
        return reader.load(config.source_path).select(
            "*", col("_metadata.file_path").alias("_input_file_name")
        )

    return _do_read()
