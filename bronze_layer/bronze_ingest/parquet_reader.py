"""Read Parquet payloads without reshaping their schema."""

from typing import Any

from .config import IngestionConfig
from .logging_utils import logger
from .retry import with_retry


def read_parquet(spark: Any, config: IngestionConfig):
    """Batch-read a Parquet file or dataset and attach source-file lineage."""
    reader = spark.read.format("parquet")
    if config.schema_hint_ddl:
        reader = reader.schema(config.schema_hint_ddl)
    if config.reader_options:
        logger.info("Applying reader_options: %s", sorted(config.reader_options))
        for key, value in config.reader_options.items():
            reader = reader.option(key, value)

    from pyspark.sql.functions import col

    @with_retry(
        attempts=config.retry_attempts,
        delay_seconds=config.retry_delay_seconds,
        max_total_seconds=config.retry_max_total_seconds,
    )
    def _do_read():
        return reader.load(config.source_path).select(
            "*", col("_metadata.file_path").alias("_input_file_name")
        )

    return _do_read()
