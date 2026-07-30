import json
import uuid

import pytest

from bronze_ingest.config import IngestionConfig
from bronze_ingest.pipeline import BronzeIngestion
from bronze_ingest.quality import DataQualityError


def _write_json(path, rows):
    with open(path, "w") as fh:
        for row in rows:
            fh.write(json.dumps(row) + "\n")


def _cfg(tmp_path, table, **overrides):
    return IngestionConfig(
        source_path=f"file://{tmp_path}/data.json",
        multiline=False,
        table=table,
        schema_name="default",
        catalog=None,
        audit_schema_name="default",
        audit_table=f"_ingestion_audit_test_{uuid.uuid4().hex[:8]}",
        registry_schema_name="default",
        registry_table=f"_schema_registry_test_{uuid.uuid4().hex[:8]}",
        **overrides,
    )


def test_run_failure_records_bad_count_and_quality_stage_in_audit(spark, tmp_path):
    """
    End-to-end regression test for #50: a quality-gate failure used to
    leave quarantined_row_count and failure_stage None on the failed
    audit row, even though bad_count was known moments before the raise.
    """
    _write_json(tmp_path / "data.json", [{"id": 1, "name": "a"}, {"id": 2, "name": None}])
    cfg = _cfg(
        tmp_path, f"pipeline_fail_{uuid.uuid4().hex[:8]}",
        required_columns=["name"], fail_on_quality_error=True,
    )
    job = BronzeIngestion(spark, cfg)

    with pytest.raises(DataQualityError):
        job.run()

    row = spark.read.table(cfg.resolved_audit_table).collect()[0]
    assert row["status"] == "failed"
    assert row["quarantined_row_count"] == 1
    assert row["failure_stage"] == "quality"
    assert not spark.catalog.tableExists(cfg.full_table_name), "bronze table should never have been written"


def test_run_success_records_schema_fingerprint_in_audit(spark, tmp_path):
    """
    End-to-end regression test for #51: a successful run should surface
    the schema fingerprint (and whether it changed) on the run-level
    audit row, not just in the separate schema registry table.
    """
    _write_json(tmp_path / "data.json", [{"id": 1, "name": "a"}, {"id": 2, "name": "b"}])
    table = f"pipeline_success_{uuid.uuid4().hex[:8]}"
    cfg = _cfg(tmp_path, table)
    job = BronzeIngestion(spark, cfg)

    job.run()

    row = spark.read.table(cfg.resolved_audit_table).collect()[0]
    assert row["status"] == "success"
    assert row["schema_fingerprint"] is not None
    assert row["schema_changed"] is False  # first-ever registration, nothing to have drifted from


# ---- one orchestration body, three entry points (#150) ----
#
# The refactor's risk is concentrated in one place: run() performs its read
# INSIDE the audited_run block so a read failure is tagged and still produces
# an audit row. Extracting the shared body naively - by materialising the
# DataFrame at the call site and passing it in - moves the read outside the
# block, and a failing read stops being recorded at all. Hence read_fn is a
# callable, and hence this test.


def test_failed_read_still_records_a_read_stage_audit_row(spark, tmp_path):
    """The regression the extraction could silently introduce."""
    cfg = _cfg(tmp_path, f"pipeline_read_fail_{uuid.uuid4().hex[:8]}")
    job = BronzeIngestion(spark, cfg)

    boom = RuntimeError("storage unreachable")

    def _explode():
        raise boom

    job.read = _explode

    with pytest.raises(RuntimeError, match="storage unreachable"):
        job.run()

    rows = spark.read.table(cfg.resolved_audit_table).collect()
    assert len(rows) == 1, "a failed read must still produce exactly one audit row"
    assert rows[0]["status"] == "failed"
    assert rows[0]["failure_stage"] == "read"


def test_read_is_not_invoked_until_inside_the_audited_run(spark, tmp_path):
    """Stronger than the above: proves laziness directly rather than
    inferring it from the audit row. If read_fn were called eagerly at the
    call site, the read would happen before audited_run opened."""
    _write_json(tmp_path / "data.json", [{"id": 1, "name": "a"}])
    cfg = _cfg(tmp_path, f"pipeline_lazy_{uuid.uuid4().hex[:8]}")
    job = BronzeIngestion(spark, cfg)

    calls = []
    original_read = job.read
    job.read = lambda: (calls.append("read"), original_read())[1]

    # Nothing read at construction or at call-argument evaluation time.
    assert calls == []
    job.run()
    assert calls == ["read"]


def test_write_failure_is_still_tagged_write_on_the_shared_body(spark, tmp_path, monkeypatch):
    """failure_stage for the write stage must survive the consolidation."""
    _write_json(tmp_path / "data.json", [{"id": 1, "name": "a"}])
    cfg = _cfg(tmp_path, f"pipeline_write_fail_{uuid.uuid4().hex[:8]}")
    job = BronzeIngestion(spark, cfg)

    import bronze_ingest.pipeline as pipeline_module

    def _explode(*args, **kwargs):
        raise RuntimeError("disk full")

    monkeypatch.setattr(pipeline_module, "write_bronze", _explode)

    with pytest.raises(RuntimeError, match="disk full"):
        job.run()

    row = spark.read.table(cfg.resolved_audit_table).collect()[0]
    assert row["status"] == "failed"
    assert row["failure_stage"] == "write"


def test_run_and_run_on_dataframe_return_identical_summary_shapes(spark, tmp_path):
    """Both entry points now share one body, so their summaries must agree
    field for field - the acceptance criterion for a no-behaviour-change
    refactor."""
    rows = [{"id": 1, "name": "a"}, {"id": 2, "name": "b"}]
    _write_json(tmp_path / "data.json", rows)

    cfg_run = _cfg(tmp_path, f"pipeline_shape_a_{uuid.uuid4().hex[:8]}")
    summary_run = BronzeIngestion(spark, cfg_run).run()

    cfg_df = _cfg(tmp_path, f"pipeline_shape_b_{uuid.uuid4().hex[:8]}")
    raw = spark.read.option("multiLine", False).json(cfg_df.source_path)
    summary_df = BronzeIngestion(spark, cfg_df).run_on_dataframe(raw)

    assert set(summary_run) == set(summary_df) == {
        "table", "row_count", "quarantined_row_count",
        "quarantine_table", "columns", "write_mode",
    }
    assert summary_run["row_count"] == summary_df["row_count"] == 2
    assert summary_run["quarantined_row_count"] == summary_df["quarantined_row_count"] == 0
    assert summary_run["quarantine_table"] is None and summary_df["quarantine_table"] is None
    assert summary_run["write_mode"] == summary_df["write_mode"] == "append"


def test_quarantine_table_is_reported_only_when_rows_were_quarantined(spark, tmp_path):
    _write_json(tmp_path / "data.json", [{"id": 1, "name": "a"}, {"id": 2, "name": None}])
    cfg = _cfg(
        tmp_path, f"pipeline_quarantine_{uuid.uuid4().hex[:8]}",
        required_columns=["name"], fail_on_quality_error=False,
    )

    summary = BronzeIngestion(spark, cfg).run()

    assert summary["row_count"] == 1
    assert summary["quarantined_row_count"] == 1
    assert summary["quarantine_table"] == cfg.resolved_quarantine_table
