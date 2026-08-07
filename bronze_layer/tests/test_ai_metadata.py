"""
Local, workspace-free tests for the AI metadata job (#208).

Follows the pattern in test_audit.py / test_schema_registry.py: no network,
no credentials, no live model call. `MetadataDrafter` is faked throughout -
see the fakes below - so these tests exercise `run_ai_metadata_job`'s own
logic (candidate selection, failure handling, malformed-output handling)
without ever importing `anthropic`.
"""

import uuid
from datetime import datetime, timedelta, timezone

from bronze_ingest.ai_metadata import (
    AI_METADATA_SCHEMA,
    AIMetadataJobConfig,
    _parse_draft,
    run_ai_metadata_job,
)
from bronze_ingest.audit import AUDIT_SCHEMA
from bronze_ingest.schema_registry import REGISTRY_SCHEMA

# ---------------------------------------------------------------------------
# Fakes - the whole point of MetadataDrafter being a narrow, one-method
# interface is that these can be this small.
# ---------------------------------------------------------------------------


class _FakeDrafter:
    """Returns a fixed, well-formed JSON response for every table."""

    def __init__(self, response=None):
        self.response = response or (
            '{"table_description": "Orders placed by customers.", '
            '"column_descriptions": {"id": "Order id."}, '
            '"schema_drift_summary": null, '
            '"pii_flags": ["email"]}'
        )
        self.calls = []

    def draft(self, prompt: str) -> str:
        self.calls.append(prompt)
        return self.response


class _FailingDrafter:
    """Always raises - simulates a timed-out or failed model call."""

    def __init__(self):
        self.calls = 0

    def draft(self, prompt: str) -> str:
        self.calls += 1
        raise TimeoutError("simulated model call failure")


class _MalformedDrafter:
    """Returns text that cannot be turned into a usable draft."""

    def __init__(self, response="not json at all"):
        self.response = response
        self.calls = 0

    def draft(self, prompt: str) -> str:
        self.calls += 1
        return self.response


class _PerTableDrafter:
    """Routes to a different fake per table_name, so a test can make one
    table fail and another succeed in the same job run."""

    def __init__(self, by_table):
        self.by_table = by_table
        self.calls = []

    def draft(self, prompt: str) -> str:
        self.calls.append(prompt)
        for table_name, drafter in self.by_table.items():
            if table_name in prompt:
                return drafter.draft(prompt)
        raise AssertionError("prompt did not match any configured table")


# ---------------------------------------------------------------------------
# Seed helpers - build directly off AUDIT_SCHEMA / REGISTRY_SCHEMA so these
# tests can never drift from what those modules actually write.
# ---------------------------------------------------------------------------


def _unique(name: str) -> str:
    return f"default._{name}_test_{uuid.uuid4().hex[:8]}"


def _job_config(**overrides) -> AIMetadataJobConfig:
    return AIMetadataJobConfig(
        audit_table=_unique("ingestion_audit"),
        registry_table=_unique("schema_registry"),
        ai_metadata_table=_unique("ai_metadata"),
        **overrides,
    )


def _seed_registry(
    spark, table, table_name, fingerprint, schema_json='[{"name":"id","type":"int"}]'
):
    now = datetime.now(timezone.utc)
    row = (table_name, "file:///dummy", fingerprint, schema_json, now, now)
    spark.createDataFrame([row], schema=REGISTRY_SCHEMA).write.format("delta").mode(
        "append"
    ).saveAsTable(table)


def _seed_audit(
    spark, table, table_name, run_id=None, status="success", row_count=10, started_at=None
):
    started_at = started_at or datetime.now(timezone.utc)
    values = dict.fromkeys(f.name for f in AUDIT_SCHEMA.fields)
    values.update(
        run_id=run_id or str(uuid.uuid4()),
        table_name=table_name,
        status=status,
        row_count=row_count,
        write_mode="append",
        started_at=started_at,
        finished_at=started_at,
    )
    row = tuple(values[f.name] for f in AUDIT_SCHEMA.fields)
    spark.createDataFrame([row], schema=AUDIT_SCHEMA).write.format("delta").mode("append").option(
        "mergeSchema", "true"
    ).saveAsTable(table)


# ---------------------------------------------------------------------------
# Reading recent activity / candidate selection
# ---------------------------------------------------------------------------


def test_job_drafts_for_a_table_with_new_activity_and_no_prior_draft(spark):
    cfg = _job_config()
    _seed_registry(spark, cfg.registry_table, "bronze.orders", "fp-1")
    _seed_audit(spark, cfg.audit_table, "bronze.orders", run_id="run-1")

    drafter = _FakeDrafter()
    summary = run_ai_metadata_job(spark, cfg, drafter)

    assert summary == {
        "processed": 1,
        "skipped_unchanged": 0,
        "skipped_failed": 0,
        "skipped_malformed": 0,
    }
    assert len(drafter.calls) == 1

    row = spark.read.table(cfg.ai_metadata_table).collect()[0]
    assert row["table_name"] == "bronze.orders"
    assert row["schema_fingerprint"] == "fp-1"
    assert row["source_run_id"] == "run-1"
    assert row["table_description"] == "Orders placed by customers."
    assert "id" in row["column_descriptions_json"]
    assert "email" in row["pii_flags_json"]
    assert row["model_id"] == cfg.model_id


def test_table_with_unchanged_fingerprint_and_no_new_activity_is_skipped(spark):
    cfg = _job_config()
    _seed_registry(spark, cfg.registry_table, "bronze.orders", "fp-1")
    _seed_audit(
        spark,
        cfg.audit_table,
        "bronze.orders",
        started_at=datetime.now(timezone.utc) - timedelta(days=5),  # outside the lookback window
    )

    drafter = _FakeDrafter()
    # First run: nothing recent in the lookback window, but no prior draft
    # either - the "never drafted" branch still fires once.
    run_ai_metadata_job(spark, cfg, drafter)
    assert len(drafter.calls) == 1

    # Second run: same fingerprint, no new activity since the draft -
    # must be skipped entirely, not re-drafted.
    summary = run_ai_metadata_job(spark, cfg, drafter)
    assert summary["processed"] == 0
    assert summary["skipped_unchanged"] == 1
    assert len(drafter.calls) == 1  # drafter not called again

    rows = spark.read.table(cfg.ai_metadata_table).collect()
    assert len(rows) == 1  # still exactly one row - upsert target, not appended to


def test_schema_drift_triggers_reprocessing_even_without_new_audit_activity(spark):
    cfg = _job_config()
    _seed_registry(spark, cfg.registry_table, "bronze.orders", "fp-1")
    _seed_audit(spark, cfg.audit_table, "bronze.orders")

    drafter = _FakeDrafter()
    run_ai_metadata_job(spark, cfg, drafter)

    # Simulate schema drift by upserting a new fingerprint into the registry.
    spark.sql(f"DELETE FROM {cfg.registry_table} WHERE table_name = 'bronze.orders'")
    _seed_registry(spark, cfg.registry_table, "bronze.orders", "fp-2")

    summary = run_ai_metadata_job(spark, cfg, drafter)
    assert summary["processed"] == 1
    assert len(drafter.calls) == 2

    row = spark.read.table(cfg.ai_metadata_table).collect()[0]
    assert row["schema_fingerprint"] == "fp-2"


def test_no_candidates_when_registry_and_audit_are_both_empty(spark):
    cfg = _job_config()
    summary = run_ai_metadata_job(spark, cfg, _FakeDrafter())
    assert summary == {
        "processed": 0,
        "skipped_unchanged": 0,
        "skipped_failed": 0,
        "skipped_malformed": 0,
    }
    assert not spark.catalog.tableExists(cfg.ai_metadata_table)


# ---------------------------------------------------------------------------
# Failure handling: log and skip, job continues, zero rows written
# ---------------------------------------------------------------------------


def test_failed_model_call_is_logged_and_skipped_without_raising(spark, caplog):
    cfg = _job_config()
    _seed_registry(spark, cfg.registry_table, "bronze.orders", "fp-1")
    _seed_audit(spark, cfg.audit_table, "bronze.orders")

    summary = run_ai_metadata_job(spark, cfg, _FailingDrafter())  # must not raise

    assert summary["skipped_failed"] == 1
    assert summary["processed"] == 0
    assert "AI metadata draft failed" in caplog.text
    # Zero rows written for that table on that run - the table was never
    # even created, since nothing was ever accepted.
    assert not spark.catalog.tableExists(cfg.ai_metadata_table)


def test_failed_call_does_not_halt_the_job_for_other_tables(spark):
    """One bad response must never stop the batch (architecture.md)."""
    cfg = _job_config()
    _seed_registry(spark, cfg.registry_table, "bronze.orders", "fp-1")
    _seed_audit(spark, cfg.audit_table, "bronze.orders")
    _seed_registry(spark, cfg.registry_table, "bronze.customers", "fp-1")
    _seed_audit(spark, cfg.audit_table, "bronze.customers")

    drafter = _PerTableDrafter(
        {
            "bronze.orders": _FailingDrafter(),
            "bronze.customers": _FakeDrafter(),
        }
    )
    summary = run_ai_metadata_job(spark, cfg, drafter)

    assert summary["processed"] == 1
    assert summary["skipped_failed"] == 1

    rows = spark.read.table(cfg.ai_metadata_table).collect()
    assert len(rows) == 1
    assert rows[0]["table_name"] == "bronze.customers"


def test_failed_call_on_a_previously_drafted_table_leaves_the_existing_row_untouched(spark):
    cfg = _job_config()
    _seed_registry(spark, cfg.registry_table, "bronze.orders", "fp-1")
    _seed_audit(spark, cfg.audit_table, "bronze.orders")
    run_ai_metadata_job(spark, cfg, _FakeDrafter())
    before = spark.read.table(cfg.ai_metadata_table).collect()[0]

    # New activity (schema drift) makes it a candidate again, but this time
    # the call fails.
    spark.sql(f"DELETE FROM {cfg.registry_table} WHERE table_name = 'bronze.orders'")
    _seed_registry(spark, cfg.registry_table, "bronze.orders", "fp-2")

    summary = run_ai_metadata_job(spark, cfg, _FailingDrafter())
    assert summary["skipped_failed"] == 1

    rows = spark.read.table(cfg.ai_metadata_table).collect()
    assert len(rows) == 1  # no new row, and the old one was not overwritten
    assert rows[0]["schema_fingerprint"] == before["schema_fingerprint"]


# ---------------------------------------------------------------------------
# Malformed output: discarded, never written
# ---------------------------------------------------------------------------


def test_malformed_output_is_discarded_and_writes_zero_rows(spark, caplog):
    cfg = _job_config()
    _seed_registry(spark, cfg.registry_table, "bronze.orders", "fp-1")
    _seed_audit(spark, cfg.audit_table, "bronze.orders")

    summary = run_ai_metadata_job(spark, cfg, _MalformedDrafter("not json at all"))

    assert summary["skipped_malformed"] == 1
    assert summary["processed"] == 0
    assert "Discarding malformed AI metadata output" in caplog.text
    assert not spark.catalog.tableExists(cfg.ai_metadata_table)


def test_json_missing_all_expected_keys_is_discarded(spark):
    cfg = _job_config()
    _seed_registry(spark, cfg.registry_table, "bronze.orders", "fp-1")
    _seed_audit(spark, cfg.audit_table, "bronze.orders")

    summary = run_ai_metadata_job(spark, cfg, _MalformedDrafter('{"unrelated_key": "value"}'))

    assert summary["skipped_malformed"] == 1
    assert not spark.catalog.tableExists(cfg.ai_metadata_table)


def test_json_with_wrong_shaped_column_descriptions_is_discarded(spark):
    cfg = _job_config()
    _seed_registry(spark, cfg.registry_table, "bronze.orders", "fp-1")
    _seed_audit(spark, cfg.audit_table, "bronze.orders")

    # column_descriptions must be an object, not a list.
    malformed = '{"table_description": "x", "column_descriptions": ["not", "an", "object"]}'
    summary = run_ai_metadata_job(spark, cfg, _MalformedDrafter(malformed))

    assert summary["skipped_malformed"] == 1
    assert not spark.catalog.tableExists(cfg.ai_metadata_table)


def test_parse_draft_directly_rejects_non_json():
    assert _parse_draft("definitely not json") is None


def test_parse_draft_directly_rejects_a_json_array():
    assert _parse_draft('["table_description", "x"]') is None


def test_parse_draft_directly_accepts_a_well_formed_response():
    parsed = _parse_draft(
        '{"table_description": "A table.", "column_descriptions": {"id": "The id."}, '
        '"schema_drift_summary": "No change.", "pii_flags": []}'
    )
    assert parsed["table_description"] == "A table."
    assert parsed["column_descriptions_json"] == '{"id": "The id."}'
    assert parsed["schema_drift_summary"] == "No change."
    assert parsed["pii_flags_json"] is None  # empty list -> no flags to record


# ---------------------------------------------------------------------------
# Schema / config shape
# ---------------------------------------------------------------------------


def test_ai_metadata_schema_matches_documented_fields():
    """Pins the advisory table's shape deliberately, same reasoning as
    test_audit.test_audit_schema_matches_documented_fields."""
    field_names = {f.name for f in AI_METADATA_SCHEMA.fields}
    assert field_names == {
        "table_name",
        "schema_fingerprint",
        "source_run_id",
        "table_description",
        "column_descriptions_json",
        "schema_drift_summary",
        "pii_flags_json",
        "model_id",
        "generated_at",
    }


def test_job_config_rejects_invalid_table_identifiers():
    import pytest

    with pytest.raises(ValueError):
        AIMetadataJobConfig(
            audit_table="bad-name; DROP TABLE x",
            registry_table="default.reg",
            ai_metadata_table="default.ai",
        )


def test_job_config_rejects_non_positive_lookback_hours():
    import pytest

    with pytest.raises(ValueError):
        AIMetadataJobConfig(
            audit_table="default.audit",
            registry_table="default.reg",
            ai_metadata_table="default.ai",
            lookback_hours=0,
        )
