import uuid

import pytest

from bronze_json_loader.config import IngestionConfig
from bronze_json_loader.audit import audited_run, AUDIT_SCHEMA


def _cfg(spark, tmp_path, table_suffix, **overrides):
    return IngestionConfig(
        source_path=f"file://{tmp_path}/dummy.json",
        table=f"audit_test_{table_suffix}",
        schema_name="default",
        catalog=None,
        audit_schema_name="default",
        audit_table=f"_ingestion_audit_test_{uuid.uuid4().hex[:8]}",
        **overrides,
    )


def test_audited_run_writes_one_row_on_success(spark, tmp_path):
    cfg = _cfg(spark, tmp_path, "success")

    with audited_run(spark, cfg, source_path=cfg.source_path) as audit:
        audit["row_count"] = 42
        audit["quarantined_row_count"] = 3

    df = spark.read.table(cfg.resolved_audit_table)
    assert df.count() == 1

    row = df.collect()[0]
    assert row["status"] == "success"
    assert row["row_count"] == 42
    assert row["quarantined_row_count"] == 3
    assert row["error_message"] is None
    assert row["table"] == cfg.full_table_name
    assert row["source_path"] == cfg.source_path
    assert row["started_at"] is not None
    assert row["finished_at"] is not None
    assert row["started_at"] <= row["finished_at"]


def test_audited_run_writes_one_row_on_failure(spark, tmp_path):
    cfg = _cfg(spark, tmp_path, "failure")

    with pytest.raises(ValueError, match="simulated failure"):
        with audited_run(spark, cfg, source_path=cfg.source_path) as audit:
            audit["row_count"] = 10
            raise ValueError("simulated failure")

    df = spark.read.table(cfg.resolved_audit_table)
    assert df.count() == 1

    row = df.collect()[0]
    assert row["status"] == "failed"
    assert row["error_message"] == "simulated failure"
    assert row["row_count"] == 10  # captured before the failure


def test_audited_run_generates_unique_run_ids_across_calls(spark, tmp_path):
    cfg = _cfg(spark, tmp_path, "unique_ids")

    with audited_run(spark, cfg, source_path=cfg.source_path) as audit:
        audit["row_count"] = 1

    with audited_run(spark, cfg, source_path=cfg.source_path) as audit:
        audit["row_count"] = 2

    df = spark.read.table(cfg.resolved_audit_table)
    rows = df.collect()
    assert df.count() == 2
    run_ids = {r["run_id"] for r in rows}
    assert len(run_ids) == 2  # both unique, no accidental reuse


def test_audited_run_respects_explicit_run_id(spark, tmp_path):
    fixed_id = "test-fixed-run-id-123"
    cfg = _cfg(spark, tmp_path, "fixed_id", run_id=fixed_id)

    with audited_run(spark, cfg, source_path=cfg.source_path) as audit:
        audit["row_count"] = 5

    df = spark.read.table(cfg.resolved_audit_table)
    row = df.collect()[0]
    assert row["run_id"] == fixed_id


def test_audited_run_disabled_writes_nothing(spark, tmp_path):
    cfg = _cfg(spark, tmp_path, "disabled", enable_run_audit=False)

    with audited_run(spark, cfg, source_path=cfg.source_path) as audit:
        audit["row_count"] = 99

    # Table should never have been created at all - check tableExists,
    # not just row count, since a 0-row table would also technically pass
    # a naive count-based assertion.
    assert not spark.catalog.tableExists(cfg.resolved_audit_table)


def test_audit_schema_matches_documented_fields():
    field_names = {f.name for f in AUDIT_SCHEMA.fields}
    expected = {
        "run_id", "table", "status", "row_count", "quarantined_row_count",
        "started_at", "finished_at", "error_message", "source_path",
    }
    assert field_names == expected, "audit schema drifted from the documented 9-field design"