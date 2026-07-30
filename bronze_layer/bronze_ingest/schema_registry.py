"""
Schema registry — one row per bronze table, recording its current schema
and when that schema last changed.

Distinct from audit.py, which records one row per *run*. This records one
row per *table's schema state*: a table ingesting daily for a year with a
stable schema stays at exactly one row here.

Upsert-on-change, not append-only. Delta's own versioning
(DESCRIBE HISTORY) provides the change history, so there is no need for
manually-maintained SCD2 rows - and the table's size stays bounded by the
number of bronze tables, not by run count.

Per the three-table model in docs/architecture.md, this is a *fact* table
written synchronously by the pipeline. The AI metadata layer reads from
it; nothing writes here but the pipeline.
"""

import hashlib
import json
from datetime import datetime, timezone
from typing import Optional, Tuple

from pyspark.sql import Row
from pyspark.sql.functions import col
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    TimestampType,
)

from .config import IngestionConfig
from .logging_utils import logger


REGISTRY_SCHEMA = StructType(
    [
        StructField("table_name", StringType(), nullable=False),
        StructField("source_path", StringType(), nullable=True),
        StructField("schema_fingerprint", StringType(), nullable=False),
        StructField("schema_json", StringType(), nullable=False),
        StructField("first_seen_at", TimestampType(), nullable=False),
        StructField("last_updated_at", TimestampType(), nullable=False),
    ]
)


def _schema_pairs(df):
    """Sorted (name, type) pairs - sorted so column reordering alone
    doesn't register as schema drift."""
    return sorted((f.name, f.dataType.simpleString()) for f in df.schema.fields)


def _fingerprint(df) -> str:
    """Stable hash of the schema, for cheap change detection without
    comparing full schema strings on every run."""
    payload = json.dumps(_schema_pairs(df), separators=(",", ":"))
    return hashlib.sha256(payload.encode()).hexdigest()[:16]


def _schema_json(df) -> str:
    return json.dumps([{"name": n, "type": t} for n, t in _schema_pairs(df)])


def _read_current_row(spark, config: IngestionConfig):
    """Returns the existing registry row for this table, or None if the
    table or row doesn't exist yet."""
    try:
        if not spark.catalog.tableExists(config.resolved_registry_table):
            return None
        # A Column expression, not an f-string SQL predicate (#154). The old
        # form was `.filter(f"table_name = '{config.full_table_name}'")`,
        # which put a config value inside a SQL string literal unescaped: a
        # table name of `x' OR '1'='1` made the filter match every row, and
        # _read_current_row then returned some OTHER table's registry row -
        # so the drift comparison and first_seen_at were silently taken from
        # it. A name containing an apostrophe was the same bug arriving as an
        # opaque parse error instead.
        #
        # Escaping the literal would have worked. Building a Column is
        # better: there is no string for anything to escape out of, and it
        # cannot regress the way a quoting helper someone forgets to call
        # can.
        rows = (
            spark.read.table(config.resolved_registry_table)
            .filter(col("table_name") == config.full_table_name)
            .collect()
        )
        return rows[0] if rows else None
    except Exception:
        return None


def _write_row(spark, config: IngestionConfig, row_dict: dict) -> None:
    """
    Upserts a single registry row. Never raises - a registry failure must
    never fail an ingestion run.

    Uses an explicit schema since spark.createDataFrame([row]) cannot
    safely infer nullability from a single-row list.
    """
    try:
        # resolved_registry_schema, not registry_schema_name - see the
        # equivalent note in audit.py (#54).
        schema_ref = (
            f"{config.registry_catalog or config.catalog}.{config.resolved_registry_schema}"
            if (config.registry_catalog or config.catalog)
            else config.resolved_registry_schema
        )
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema_ref}")

        df = spark.createDataFrame([Row(**row_dict)], schema=REGISTRY_SCHEMA)
        target = config.resolved_registry_table

        if not spark.catalog.tableExists(target):
            df.write.format("delta").saveAsTable(target)
            return

        df.createOrReplaceTempView("_registry_updates")
        spark.sql(f"""
            MERGE INTO {target} AS t
            USING _registry_updates AS s
            ON t.table_name = s.table_name
            WHEN MATCHED THEN UPDATE SET
                t.source_path = s.source_path,
                t.schema_fingerprint = s.schema_fingerprint,
                t.schema_json = s.schema_json,
                t.last_updated_at = s.last_updated_at
            WHEN NOT MATCHED THEN INSERT *
        """)
    except Exception as exc:
        logger.warning(
            "Failed to write schema registry row for %s: %s",
            config.full_table_name,
            exc,
        )


def record_schema(
    spark, config: IngestionConfig, df, source_path: str = None
) -> Tuple[Optional[str], bool]:
    """
    Records the current schema for config's target table, but only if it
    differs from what's already registered.

    The unchanged path costs one small read and zero writes - this is the
    cost-safety property that keeps per-run overhead negligible.

    Returns (fingerprint, changed) so callers - notably the run-level audit
    trail (#51) - can cheaply surface per-run schema drift without
    re-deriving the fingerprint themselves. `changed` is True only when a
    previously-registered fingerprint existed and differed from this run's;
    False for the first-ever registration, for an unchanged schema, and
    when the registry is disabled or the check itself fails (a registry
    failure must never fail the ingestion run, so this always returns
    rather than raising).
    """
    if not config.enable_schema_registry:
        return None, False

    try:
        fingerprint = _fingerprint(df)
        current = _read_current_row(spark, config)

        if current is not None and current["schema_fingerprint"] == fingerprint:
            return fingerprint, False  # unchanged - nothing to write

        now = datetime.now(timezone.utc)
        _write_row(
            spark,
            config,
            {
                "table_name": config.full_table_name,
                "source_path": source_path or config.source_path,
                "schema_fingerprint": fingerprint,
                "schema_json": _schema_json(df),
                "first_seen_at": current["first_seen_at"] if current is not None else now,
                "last_updated_at": now,
            },
        )

        if current is None:
            logger.info("Registered schema for %s (%s)", config.full_table_name, fingerprint)
            return fingerprint, False

        logger.warning(
            "Schema drift detected for %s: %s -> %s",
            config.full_table_name,
            current["schema_fingerprint"],
            fingerprint,
        )
        return fingerprint, True
    except Exception as exc:
        logger.warning("Schema registry check failed for %s: %s", config.full_table_name, exc)
        return None, False
