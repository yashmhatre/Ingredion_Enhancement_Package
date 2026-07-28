"""
Run-level audit trail — one record per pipeline execution, independent of
any single bronze table. Answers "did this run succeed, how many rows,
how long did it take" without needing to inspect any specific table.

Separate from the per-row audit columns in bronze_writer.py
(_ingested_at, _source_file, _batch_id), which describe individual rows
within a table. This module describes the run itself.
"""

import uuid
from contextlib import contextmanager
from datetime import datetime, timezone

from pyspark.sql import Row
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType, BooleanType, TimestampType,
)

from .config import IngestionConfig
from .logging_utils import logger


# Fixed schema - see Phase 1 task for the design rationale (strict,
# no catch-all column, no reshaping/transformation details).
AUDIT_SCHEMA = StructType([
    StructField("run_id", StringType(), nullable=False),
    StructField("table", StringType(), nullable=False),
    StructField("status", StringType(), nullable=False),
    StructField("row_count", LongType(), nullable=True),
    StructField("quarantined_row_count", LongType(), nullable=True),
    StructField("failure_stage", StringType(), nullable=True),
    StructField("schema_fingerprint", StringType(), nullable=True),
    StructField("schema_changed", BooleanType(), nullable=True),
    StructField("started_at", TimestampType(), nullable=False),
    StructField("finished_at", TimestampType(), nullable=True),
    StructField("error_message", StringType(), nullable=True),
    StructField("source_path", StringType(), nullable=True),
])

AUDIT_SCHEMA_DDL = (
    "run_id STRING, table STRING, status STRING, row_count LONG, "
    "quarantined_row_count LONG, failure_stage STRING, "
    "schema_fingerprint STRING, schema_changed BOOLEAN, "
    "started_at TIMESTAMP, finished_at TIMESTAMP, error_message STRING, "
    "source_path STRING"
)


def tag_failure_stage(exc: Exception, stage: str) -> None:
    """
    Attaches a failure_stage ("read" | "quality" | "write") to an
    exception so the failed audit row records which stage failed without
    needing to parse error_message text. Idempotent - re-raising through
    a nested handler won't overwrite a stage an inner handler already set.
    """
    if not hasattr(exc, "failure_stage"):
        exc.failure_stage = stage


def _write_audit_row(spark, config: IngestionConfig, row_dict: dict) -> None:
    """
    Writes a single audit row to config.resolved_audit_table. Never raises -
    a failure to write an audit record should not fail the ingestion run
    itself. Uses an explicit schema (AUDIT_SCHEMA) since
    spark.createDataFrame([row]) cannot safely infer nullability from a
    single-row list.
    """
    try:
        schema_ref = (
            f"{config.audit_catalog or config.catalog}.{config.audit_schema_name}"
            if (config.audit_catalog or config.catalog)
            else config.audit_schema_name
        )
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema_ref}")

        row = Row(**row_dict)
        df = spark.createDataFrame([row], schema=AUDIT_SCHEMA)
        df.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(
            config.resolved_audit_table
        )
    except Exception as exc:
        logger.warning(
            "Failed to write audit record for run against %s: %s",
            config.full_table_name, exc,
        )


def record_replay_run(
    spark, config: IngestionConfig, *, status: str,
    row_count, quarantined_row_count, source_path: str = None,
) -> None:
    """
    Writes a single run-level audit row for a quarantine-replay operation
    (see replay.py, #60) - reuses the same audit table/schema as normal
    ingestion runs, but with a distinguishable status (e.g.
    "success_replay") so replays are queryable separately from normal
    ingestion runs without a dedicated table. No-ops entirely if
    config.enable_run_audit is False, matching audited_run().
    """
    if not config.enable_run_audit:
        return

    now = datetime.now(timezone.utc)
    _write_audit_row(spark, config, {
        "run_id": str(uuid.uuid4()),
        "table": config.full_table_name,
        "status": status,
        "row_count": row_count,
        "quarantined_row_count": quarantined_row_count,
        "failure_stage": None,
        "schema_fingerprint": None,
        "schema_changed": None,
        "started_at": now,
        "finished_at": now,
        "error_message": None,
        "source_path": source_path or config.source_path,
    })


@contextmanager
def audited_run(spark, config: IngestionConfig, source_path: str = None):
    """
    Context manager wrapping a single ingestion run (or one streaming
    micro-batch). Writes exactly one audit row on exit, success or
    failure. No-ops entirely if config.enable_run_audit is False.

    Usage:
        with audited_run(spark, config, source_path=config.source_path) as audit:
            summary = do_the_actual_ingestion()
            audit["row_count"] = summary["row_count"]
            audit["quarantined_row_count"] = summary["quarantined_row_count"]

    The yielded dict is mutable - the caller fills in row_count,
    quarantined_row_count, schema_fingerprint and schema_changed on
    success. Status/timestamps/error_message are handled automatically by
    this context manager. On failure, quarantined_row_count and
    failure_stage are recovered from the raised exception's `bad_count`
    /`failure_stage` attributes if present (see quality.DataQualityError
    and tag_failure_stage()), so a failed run's audit row still carries
    those numbers instead of leaving them None.
    """
    if not config.enable_run_audit:
        yield {}
        return

    run_id = config.run_id or str(uuid.uuid4())
    started_at = datetime.now(timezone.utc)
    result = {
        "row_count": None, "quarantined_row_count": None,
        "schema_fingerprint": None, "schema_changed": None,
    }

    try:
        yield result
        finished_at = datetime.now(timezone.utc)
        _write_audit_row(spark, config, {
            "run_id": run_id,
            "table": config.full_table_name,
            "status": "success",
            "row_count": result.get("row_count"),
            "quarantined_row_count": result.get("quarantined_row_count"),
            "failure_stage": None,
            "schema_fingerprint": result.get("schema_fingerprint"),
            "schema_changed": result.get("schema_changed"),
            "started_at": started_at,
            "finished_at": finished_at,
            "error_message": None,
            "source_path": source_path or config.source_path,
        })
    except Exception as exc:
        finished_at = datetime.now(timezone.utc)
        bad_count = getattr(exc, "bad_count", None)
        if bad_count is not None and result.get("quarantined_row_count") is None:
            result["quarantined_row_count"] = bad_count
        _write_audit_row(spark, config, {
            "run_id": run_id,
            "table": config.full_table_name,
            "status": "failed",
            "row_count": result.get("row_count"),
            "quarantined_row_count": result.get("quarantined_row_count"),
            "failure_stage": getattr(exc, "failure_stage", None),
            "schema_fingerprint": result.get("schema_fingerprint"),
            "schema_changed": result.get("schema_changed"),
            "started_at": started_at,
            "finished_at": finished_at,
            "error_message": str(exc),
            "source_path": source_path or config.source_path,
        })
        raise