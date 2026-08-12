"""
Run-level audit trail — one record per pipeline execution, independent of
any single bronze table. Answers "did this run succeed, how many rows,
how long did it take" without needing to inspect any specific table.

Separate from the per-row audit columns in bronze_writer.py
(_ingested_at, _source_file, _batch_id), which describe individual rows
within a table. This module describes the run itself.
"""

import threading
import uuid
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Optional

from pyspark.sql.types import (
    BooleanType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from .config import IngestionConfig
from .logging_utils import logger

# Fixed schema - see Phase 1 task for the design rationale (strict,
# no catch-all column, no reshaping/transformation details).
#
# Extended once, by #149 and #156 together (see each below for why). Doing
# them in one change was deliberate: both open this schema, and two
# migrations against a live table for what is one edit is the avoidable
# version. `_write_audit_row` writes with mergeSchema, so existing rows get
# NULLs in the new columns - acceptable, but it means rows written before
# this change cannot be reinterpreted, only recognised as older by their
# NULL `write_mode`.
AUDIT_SCHEMA = StructType(
    [
        StructField("run_id", StringType(), nullable=False),
        # Renamed from `table` (#149). `table` is a SQL reserved word and
        # needed backticking in every query written against it - which every
        # dashboard tile in #62 and every baseline query in #61 would have
        # had to remember. `_schema_registry` already used `table_name`, so
        # this also stops the two metadata tables disagreeing. Cheapest now,
        # while nothing queries it.
        StructField("table_name", StringType(), nullable=False),
        StructField("status", StringType(), nullable=False),
        # Rows written to the TARGET by this run. Comparable across write
        # modes, which it was not before #149.
        StructField("row_count", LongType(), nullable=True),
        # Rows offered to the writer after the quality gate. Equal to
        # row_count for append/overwrite; under merge the difference is the
        # dedupe/no-op ratio.
        StructField("source_row_count", LongType(), nullable=True),
        # Merge only, NULL otherwise - an update is not an insert, and a
        # single row_count could never say which happened.
        StructField("rows_inserted", LongType(), nullable=True),
        StructField("rows_updated", LongType(), nullable=True),
        StructField("rows_deleted", LongType(), nullable=True),
        # So a consumer can interpret the numbers above without joining back
        # to a config it does not have.
        StructField("write_mode", StringType(), nullable=True),
        # Structured Streaming's micro-batch id (#156). NULL for batch runs.
        # `run_id` identifies a job run and every micro-batch in a streaming
        # run shares it; this is what makes each row individually
        # addressable.
        StructField("stream_batch_id", LongType(), nullable=True),
        StructField("quarantined_row_count", LongType(), nullable=True),
        StructField("failure_stage", StringType(), nullable=True),
        StructField("schema_fingerprint", StringType(), nullable=True),
        StructField("schema_changed", BooleanType(), nullable=True),
        # WHAT changed, not merely that something did (#256). Compact JSON:
        # {"added":[...],"removed":[...],"type_changed":[{"column","from","to"}]}.
        # schema_changed answers a yes/no a dashboard can count; this answers
        # the question anyone who sees that count actually asks next, without
        # hand-diffing two schema_json blobs out of the registry.
        # NULL when there is no previous schema to compare against, and on
        # every row written before this column existed.
        StructField("schema_drift_json", StringType(), nullable=True),
        # Tag application outcome (#64). A PAIR, deliberately mirroring
        # schema_changed / schema_drift_json above: the boolean is the signal a
        # dashboard counts without parsing JSON (#62), the JSON carries the
        # detail whoever investigates needs.
        #
        # Tag writes never fail an ingestion run - they are governance
        # metadata, not data - so without this a failed tag write was visible
        # only in a log line nobody greps. That is the exact shape of silent
        # success this repo keeps finding.
        #
        # Both NULL when no tags are configured, which is the default: absent
        # is "nothing was attempted", distinct from false/"attempted and fine".
        StructField("tags_failed", BooleanType(), nullable=True),
        StructField("tag_outcome_json", StringType(), nullable=True),
        StructField("started_at", TimestampType(), nullable=False),
        StructField("finished_at", TimestampType(), nullable=True),
        StructField("error_message", StringType(), nullable=True),
        StructField("source_path", StringType(), nullable=True),
    ]
)

AUDIT_SCHEMA_DDL = ", ".join(
    f"{field.name} {field.dataType.simpleString().upper()}" for field in AUDIT_SCHEMA.fields
)


def tag_failure_stage(exc: Exception, stage: str) -> None:
    """
    Attaches a failure_stage ("read" | "quality" | "write") to an
    exception so the failed audit row records which stage failed without
    needing to parse error_message text. Idempotent - re-raising through
    a nested handler won't overwrite a stage an inner handler already set.
    """
    if not hasattr(exc, "failure_stage"):
        # Dynamic attribute on an arbitrary exception - deliberate, so that
        # audited_run can read the stage back off whatever propagated.
        exc.failure_stage = stage  # type: ignore[attr-defined]


#: Active audit-row buffer, or None when writes go straight through (#159).
#:
#: Thread-local rather than a plain module global. Nothing in the package runs
#: units concurrently today, but `bronze_writer` already has a concurrent-write
#: test (#46) and a buffer shared across threads would interleave rows from
#: unrelated runs into one commit - a failure that would only appear under
#: load, which is the worst kind to leave available.
_buffer_state = threading.local()


def _active_buffer() -> Optional[dict]:
    return getattr(_buffer_state, "buffer", None)


@contextmanager
def buffered_audit_writes(spark):
    """
    Collects audit rows for the duration of the block and writes them in ONE
    commit per audit table at the end, instead of one commit per row (#159).

    The measured problem: `_ingestion_audit`, at 15 runs, held 15 files of
    4-7 KB - exactly one file per run, forever. Directory ingestion is the
    sharp case because it runs one audited unit per file, so a 50-file
    directory produced 50 single-row commits per run.

    This mirrors what `RetryState` already does one level up in
    `directory_ingestion`: one load and at most one write for the whole run,
    rather than a read-modify-write per file (#151).

    Rows are grouped by resolved audit table, not merged into one write, because
    per-file config overrides can legitimately redirect individual units to a
    different audit schema.

    **Flushing never raises**, and never leaves the trail worse off than
    unbuffered writes would have: if the single batched write fails, each row is
    retried individually so a malformed row loses itself rather than the whole
    run's history. Buffering is a file-count optimisation and must not become a
    durability regression.
    """
    if _active_buffer() is not None:
        # Already buffering - a nested block writes into the outer buffer and
        # the outermost one flushes. Re-entrancy is not expected today; making
        # it a no-op is cheaper than making it an error nobody can act on.
        yield
        return

    _buffer_state.buffer = {}
    try:
        yield
    finally:
        buffered = _buffer_state.buffer
        _buffer_state.buffer = None
        _flush_audit_buffer(spark, buffered)


def _batched_append(spark, schema_ref: str, table: str, rows: list) -> None:
    """
    The raw write, for one or many rows. **Raises** - it is the only writer
    here that does, which is what lets `_flush_audit_buffer` tell a failed
    batch from a successful one and fall back. Every caller that must not fail
    the ingestion run goes through `_append_audit_rows` instead.
    """
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema_ref}")
    # coalesce(1) because one COMMIT is not one FILE, which is the whole point
    # of #159. Spark writes one file per non-empty partition, so buffering 50
    # audit rows into a single commit still produced one file per partition
    # Spark happened to spread them across - CI caught this as 2 files for 3
    # rows. Without this, buffering reduces commits and barely touches the file
    # count it exists to reduce.
    #
    # Safe at this size by construction: a batch is bounded by the number of
    # units in one directory run, each row is a handful of scalar fields, and
    # the unbuffered path passes a single row (where coalesce is a no-op).
    df = spark.createDataFrame(rows, schema=AUDIT_SCHEMA).coalesce(1)
    df.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(table)


def _append_audit_rows(spark, schema_ref: str, table: str, rows: list) -> None:
    """Never raises - see `_write_audit_row`'s contract."""
    try:
        _batched_append(spark, schema_ref, table, rows)
    except Exception as exc:  # noqa: BLE001 - the audit trail must never fail the run it records
        logger.warning("Failed to write %d audit record(s) to %s: %s", len(rows), table, exc)


def _flush_audit_buffer(spark, buffered: dict) -> None:
    """Writes each audit table's buffered rows in one commit. Never raises."""
    for table, entry in buffered.items():
        rows = entry["rows"]
        if not rows:
            continue
        schema_ref = entry["schema_ref"]
        try:
            _batched_append(spark, schema_ref, table, rows)
            logger.info("Wrote %d buffered audit row(s) to %s in one commit.", len(rows), table)
        except Exception as exc:  # noqa: BLE001 - as above
            # Fall back to one write per row rather than dropping the batch.
            # This gives up the file-count win for this run, which is the right
            # trade: the point of the audit trail is that it exists. A single
            # malformed row then loses only itself.
            logger.warning(
                "Batched audit write of %d row(s) to %s failed (%s) - falling back to "
                "individual writes.",
                len(rows),
                table,
                exc,
            )
            for row in rows:
                _append_audit_rows(spark, schema_ref, table, [row])


def _write_audit_row(spark, config: IngestionConfig, row_dict: dict) -> None:
    """
    Writes a single audit row to config.resolved_audit_table. Never raises -
    a failure to write an audit record should not fail the ingestion run
    itself. Uses an explicit schema (AUDIT_SCHEMA) since
    spark.createDataFrame([row]) cannot safely infer nullability from a
    single-row list.

    Inside a `buffered_audit_writes` block the row is queued instead of
    written, and the block flushes it in one commit with the rest (#159).
    """
    try:
        # resolved_audit_schema, not audit_schema_name: the latter is None by
        # default and means "use schema_name" (#54). Reading the raw field
        # here would produce "CREATE SCHEMA IF NOT EXISTS None".
        schema_ref = (
            f"{config.audit_catalog or config.catalog}.{config.resolved_audit_schema}"
            if (config.audit_catalog or config.catalog)
            else config.resolved_audit_schema
        )
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema_ref}")

        # Projected into AUDIT_SCHEMA's field order explicitly, rather than
        # relying on the caller's dict happening to be built in that order.
        #
        # `Row(**row_dict)` + an explicit schema binds POSITIONALLY, so a dict
        # whose keys are in a different order than the schema silently
        # produces a row with values in the wrong columns - or a type error,
        # which this function's never-raise contract then swallows into a
        # warning. The visible symptom is not a bad row: it is no table at
        # all, and every later read failing with TABLE_OR_VIEW_NOT_FOUND a
        # long way from the cause.
        #
        # That is not hypothetical - it happened while adding #149's columns,
        # and only the tests that read the table back caught it.
        unexpected = sorted(set(row_dict) - {f.name for f in AUDIT_SCHEMA.fields})
        if unexpected:
            # A typo'd key would otherwise be dropped here and read as a NULL
            # column, which looks like "the pipeline did not record that".
            logger.warning(
                "Ignoring unknown audit field(s) %s - not in AUDIT_SCHEMA. This is a "
                "bug in the caller, not in the data.",
                unexpected,
            )

        row = tuple(row_dict.get(field.name) for field in AUDIT_SCHEMA.fields)
        table = config.resolved_audit_table

        buffer = _active_buffer()
        if buffer is not None:
            # Queued, not written. The CREATE SCHEMA above has already run, so
            # the schema exists by flush time regardless of which unit's config
            # got here first.
            entry = buffer.setdefault(table, {"schema_ref": schema_ref, "rows": []})
            entry["rows"].append(row)
            return

        _append_audit_rows(spark, schema_ref, table, [row])
    except Exception as exc:  # noqa: BLE001 - the audit trail must never fail the ingestion it records
        logger.warning(
            "Failed to write audit record for run against %s: %s",
            config.full_table_name,
            exc,
        )


def record_replay_run(
    spark,
    config: IngestionConfig,
    *,
    status: str,
    row_count,
    quarantined_row_count,
    source_path: Optional[str] = None,
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
    _write_audit_row(
        spark,
        config,
        {
            "run_id": str(uuid.uuid4()),
            "table_name": config.full_table_name,
            "status": status,
            "row_count": row_count,
            # Replay promotes previously-rejected rows, so every promoted row
            # was offered to the writer - source and target counts agree here
            # by construction, unlike the merge path.
            "source_row_count": row_count,
            "rows_inserted": None,
            "rows_updated": None,
            "rows_deleted": None,
            "write_mode": config.write_mode,
            "stream_batch_id": None,
            "quarantined_row_count": quarantined_row_count,
            "failure_stage": None,
            "schema_fingerprint": None,
            "schema_changed": None,
            "started_at": now,
            "finished_at": now,
            "error_message": None,
            "source_path": source_path or config.source_path,
        },
    )


#: Everything a caller may fill in on the yielded dict. Declared once so the
#: success and failure paths cannot drift apart - they previously listed the
#: same keys twice, which is how a new column gets recorded on success and
#: silently omitted on failure.
_CALLER_FIELDS = (
    "row_count",
    "source_row_count",
    "rows_inserted",
    "rows_updated",
    "rows_deleted",
    "quarantined_row_count",
    "schema_fingerprint",
    "schema_changed",
)


@contextmanager
def audited_run(
    spark,
    config: IngestionConfig,
    source_path: Optional[str] = None,
    stream_batch_id: Optional[int] = None,
):
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
    result = dict.fromkeys(_CALLER_FIELDS)

    def _row(status, *, error_message=None, failure_stage=None):
        return {
            "run_id": run_id,
            "table_name": config.full_table_name,
            "status": status,
            "write_mode": config.write_mode,
            "stream_batch_id": stream_batch_id,
            "failure_stage": failure_stage,
            "started_at": started_at,
            "finished_at": datetime.now(timezone.utc),
            "error_message": error_message,
            "source_path": source_path or config.source_path,
            **{field: result.get(field) for field in _CALLER_FIELDS},
        }

    try:
        yield result
        _write_audit_row(spark, config, _row("success"))
    except Exception as exc:
        bad_count = getattr(exc, "bad_count", None)
        if bad_count is not None and result.get("quarantined_row_count") is None:
            result["quarantined_row_count"] = bad_count
        _write_audit_row(
            spark,
            config,
            _row(
                "failed",
                error_message=str(exc),
                failure_stage=getattr(exc, "failure_stage", None),
            ),
        )
        raise
