"""
Writes a prepared DataFrame into a Delta bronze table, adding standard audit
columns and supporting append / overwrite / merge semantics with optional
schema evolution.
"""

from datetime import datetime, timezone

from pyspark.sql.functions import lit, current_timestamp, col

from .config import IngestionConfig
from .retry import with_retry
from .logging_utils import logger


def add_audit_columns(df, config: IngestionConfig):
    if not config.add_audit_columns:
        return df

    batch_id = config.batch_id or datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")

    df = df.withColumn(config.audit_ingest_ts_col, current_timestamp())
    df = df.withColumn(config.audit_batch_id_col, lit(batch_id))

    if "_input_file_name" in df.columns:
        df = df.withColumnRenamed("_input_file_name", config.audit_source_file_col)
    else:
        df = df.withColumn(config.audit_source_file_col, lit(None).cast("string"))

    return df


def _table_exists(spark, full_table_name: str) -> bool:
    try:
        return spark.catalog.tableExists(full_table_name)
    except Exception:
        # Older runtimes without tableExists - fall back to a DESCRIBE probe.
        try:
            spark.sql(f"DESCRIBE TABLE {full_table_name}")
            return True
        except Exception:
            return False


def _write_core(spark, df, config: IngestionConfig, txn_options=None):
    schema_ref = f"{config.catalog}.{config.schema_name}" if config.catalog else config.schema_name
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema_ref}")

    full_name = config.full_table_name
    writer = df.write.format("delta")

    if config.merge_schema:
        writer = writer.option("mergeSchema", "true")
    if config.partition_by:
        writer = writer.partitionBy(*config.partition_by)
    if txn_options:  # idempotent-write options for streaming foreachBatch (txnAppId/txnVersion)
        for k, v in txn_options.items():
            writer = writer.option(k, v)

    if config.write_mode == "append":
        writer.mode("append").saveAsTable(full_name)

    elif config.write_mode == "overwrite":
        writer.mode("overwrite").saveAsTable(full_name)

    elif config.write_mode == "merge":
        from delta.tables import DeltaTable

        if not _table_exists(spark, full_name):
            # Nothing to merge into yet - first load is just a write.
            writer.mode("append").saveAsTable(full_name)
        else:
            target = DeltaTable.forName(spark, full_name)
            condition = " AND ".join(f"target.`{k}` = source.`{k}`" for k in config.merge_keys)
            (
                target.alias("target")
                .merge(df.alias("source"), condition)
                .whenMatchedUpdateAll()
                .whenNotMatchedInsertAll()
                .execute()
            )
    else:
        raise ValueError(f"Unknown write_mode: {config.write_mode}")

    return full_name


def write_bronze(spark, df, config: IngestionConfig):
    """
    Writes df to the configured Delta bronze table (batch mode). Creates the
    schema (database) if it doesn't exist. Retries on transient failures
    (throttling, concurrent-write conflicts). Returns the full table name.
    """
    @with_retry(attempts=config.retry_attempts, delay_seconds=config.retry_delay_seconds)
    def _do_write():
        return _write_core(spark, df, config)

    return _do_write()


def write_bronze_micro_batch(spark, micro_batch_df, batch_id: int, config: IngestionConfig):
    """
    Used as the body of `foreachBatch` for streaming ingestion. Achieves
    exactly-once sink writes (even across job restarts / retried batches)
    using Delta Lake's idempotent-write transaction options, keyed by this
    pipeline's checkpoint location as the app id and the Structured
    Streaming batch_id as the version.

    See: https://docs.delta.io/latest/delta-streaming.html#idempotent-table-writes-in-foreachbatch

    Note: txnAppId/txnVersion idempotency applies to the append/overwrite
    write path. If write_mode="merge", Delta's MERGE itself does not accept
    those options - a retried micro-batch merging the same rows on the same
    merge_keys is naturally safe (updates just re-apply), but true
    exactly-once row counting for merge + streaming should additionally rely
    on Auto Loader's own checkpoint (which prevents re-reading the same
    source files) rather than txnVersion.
    """
    if micro_batch_df.rdd.isEmpty():
        logger.info("Micro-batch %s is empty - skipping write.", batch_id)
        return

    txn_app_id = config.checkpoint_location or config.full_table_name
    txn_options = {"txnAppId": txn_app_id, "txnVersion": str(batch_id)}

    @with_retry(attempts=config.retry_attempts, delay_seconds=config.retry_delay_seconds)
    def _do_write():
        return _write_core(spark, micro_batch_df, config, txn_options=txn_options)

    full_name = _do_write()
    logger.info("Micro-batch %s written to %s", batch_id, full_name)
