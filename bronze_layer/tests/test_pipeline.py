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
