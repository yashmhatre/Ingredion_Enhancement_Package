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
    StringType,
    StructField,
    StructType,
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
    except Exception:  # noqa: BLE001 - no readable registry row means 'first time seen'
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
        # NOTE: bandit logs "nosec encountered (B608), but no failed test on
        # this file:line" for the marker below. That warning is spurious and
        # the marker is load-bearing - verified by removing it, at which
        # point B608 fires and the build fails. bandit anchors the finding on
        # the line where the f-string opens but honours a nosec anywhere in
        # the statement, so the two disagree about which line to name. Do not
        # "clean up" the marker on the strength of the warning.
        #
        # nosec B608 - `target` is not user input at this point. It is
        # composed of catalog/schema/table identifiers that __post_init__
        # ran through validate_identifier() at config load (#154), so it
        # cannot carry quotes, semicolons or whitespace. The row VALUES are
        # never interpolated - they arrive through a temp view built from a
        # typed DataFrame. Spark SQL has no bind parameters for a table
        # name, so interpolation here is unavoidable; validating at the
        # boundary is the mitigation.
        merge_sql = f"""
            MERGE INTO {target} AS t
            USING _registry_updates AS s
            ON t.table_name = s.table_name
            WHEN MATCHED THEN UPDATE SET
                t.source_path = s.source_path,
                t.schema_fingerprint = s.schema_fingerprint,
                t.schema_json = s.schema_json,
                t.last_updated_at = s.last_updated_at
            WHEN NOT MATCHED THEN INSERT *
        """  # nosec B608
        spark.sql(merge_sql)
    except Exception as exc:  # noqa: BLE001 - the registry must never fail the ingestion it describes
        logger.warning(
            "Failed to write schema registry row for %s: %s",
            config.full_table_name,
            exc,
        )


def _fields_by_name(schema_json: str) -> dict:
    """
    {column_name: type} from a registry `schema_json` value.

    Reads the shape `_schema_json` actually writes - a JSON ARRAY of
    {"name", "type"} objects - and not Spark's own `schema.json()`, which
    wraps its fields in a {"fields": [...]} object. Writing this against the
    wrong one of those two is not hypothetical: the first version of #256 did,
    and because `describe_drift` swallows parse errors by design, it returned
    None on every real drift while its unit tests - written against the same
    wrong assumption - passed. CI's integration test caught it.
    """
    return {f["name"]: f.get("type") for f in json.loads(schema_json)}


def describe_drift(old_schema_json: Optional[str], new_schema_json: str) -> Optional[str]:
    """
    A compact, queryable description of what changed between two registered
    schemas, as JSON: added / removed columns and type changes (#256).

    Until now drift was recorded two ways, neither of them usable: a boolean
    (`schema_changed`) on the audit row, which says THAT something changed, and
    a log line, which says what but is not data. Reconstructing the detail
    meant diffing two `schema_json` blobs by hand, after finding them.

    Returns None when there is nothing to describe - no previous schema (a
    first registration is not drift), unparseable input, or a diff that comes
    out empty. None rather than "{}" so a consumer can tell "no drift recorded"
    from "drift recorded, and it was nothing", and so the column stays NULL for
    every row that predates this.

    Column ORDER is deliberately not drift. Reordering columns changes the
    fingerprint, so `schema_changed` will be True and this will be None - which
    is the honest answer: something changed, and it was not the set of columns
    or their types.
    """
    if not old_schema_json:
        return None
    try:
        old_fields = _fields_by_name(old_schema_json)
        new_fields = _fields_by_name(new_schema_json)
    except (ValueError, KeyError, TypeError):
        # Advisory metadata must never fail the run that produced it - the
        # same contract the rest of this module keeps.
        return None

    added = sorted(set(new_fields) - set(old_fields))
    removed = sorted(set(old_fields) - set(new_fields))
    type_changed = [
        {"column": name, "from": old_fields[name], "to": new_fields[name]}
        for name in sorted(set(old_fields) & set(new_fields))
        if old_fields[name] != new_fields[name]
    ]
    if not (added or removed or type_changed):
        return None

    return json.dumps(
        {"added": added, "removed": removed, "type_changed": type_changed},
        separators=(",", ":"),
        sort_keys=True,
    )


def record_schema(
    spark, config: IngestionConfig, df, source_path: Optional[str] = None
) -> Tuple[Optional[str], bool, Optional[str]]:
    """
    Records the current schema for config's target table, but only if it
    differs from what's already registered.

    The unchanged path costs one small read and zero writes - this is the
    cost-safety property that keeps per-run overhead negligible.

    Returns (fingerprint, changed, drift_json) so callers - notably the
    run-level audit trail (#51) - can cheaply surface per-run schema drift
    without re-deriving any of it themselves. `drift_json` is `describe_drift`'s
    structured summary of WHAT changed (#256), and is None whenever `changed`
    is False, plus for a first registration and for a change that is not a
    column add/remove/retype (a pure reordering, say). `changed` is True only when a
    previously-registered fingerprint existed and differed from this run's;
    False for the first-ever registration, for an unchanged schema, and
    when the registry is disabled or the check itself fails (a registry
    failure must never fail the ingestion run, so this always returns
    rather than raising).
    """
    if not config.enable_schema_registry:
        return None, False, None

    try:
        fingerprint = _fingerprint(df)
        current = _read_current_row(spark, config)

        new_schema_json = _schema_json(df)
        if current is not None and current["schema_fingerprint"] == fingerprint:
            return fingerprint, False, None  # unchanged - nothing to write

        now = datetime.now(timezone.utc)
        _write_row(
            spark,
            config,
            {
                "table_name": config.full_table_name,
                "source_path": source_path or config.source_path,
                "schema_fingerprint": fingerprint,
                "schema_json": new_schema_json,
                "first_seen_at": current["first_seen_at"] if current is not None else now,
                "last_updated_at": now,
            },
        )

        if current is None:
            logger.info("Registered schema for %s (%s)", config.full_table_name, fingerprint)
            return fingerprint, False, None

        logger.warning(
            "Schema drift detected for %s: %s -> %s",
            config.full_table_name,
            current["schema_fingerprint"],
            fingerprint,
        )
        drift = describe_drift(current["schema_json"], new_schema_json)
        return fingerprint, True, drift
    except Exception as exc:  # noqa: BLE001 - as above - drift detection is advisory, never a gate
        logger.warning("Schema registry check failed for %s: %s", config.full_table_name, exc)
        return None, False, None
