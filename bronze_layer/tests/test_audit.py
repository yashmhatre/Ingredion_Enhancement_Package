import uuid
from datetime import datetime, timezone

import pytest

from bronze_ingest.audit import AUDIT_SCHEMA, audited_run, tag_failure_stage
from bronze_ingest.config import IngestionConfig
from tests.conftest import file_uri


def _cfg(spark, tmp_path, table_suffix, **overrides):
    return IngestionConfig(
        source_path=file_uri(tmp_path, "dummy.json"),
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
    assert row["table_name"] == cfg.full_table_name
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
    """
    Pins the schema deliberately. AUDIT_SCHEMA is strict by design - no
    catch-all column - so a new field is a migration against a live table and
    should be a decision, not a diff someone notices later.

    Extended once, by #149 and #156 together, in a single change because both
    needed to open it.
    """
    field_names = {f.name for f in AUDIT_SCHEMA.fields}
    expected = {
        "run_id",
        # Renamed from `table` (#149) - a SQL reserved word that needed
        # backticking in every query written against it.
        "table_name",
        "status",
        "row_count",
        "source_row_count",
        "rows_inserted",
        "rows_updated",
        "rows_deleted",
        "write_mode",
        "stream_batch_id",
        "quarantined_row_count",
        "failure_stage",
        "schema_fingerprint",
        "schema_changed",
        "started_at",
        "finished_at",
        "error_message",
        "source_path",
    }
    assert field_names == expected, "audit schema drifted from the documented design"


def test_audit_schema_ddl_matches_the_struct():
    """The DDL string is derived from AUDIT_SCHEMA rather than maintained
    beside it, so the two cannot disagree - they previously could."""
    from bronze_ingest.audit import AUDIT_SCHEMA_DDL

    assert len(AUDIT_SCHEMA_DDL.split(", ")) == len(AUDIT_SCHEMA.fields)
    for field in AUDIT_SCHEMA.fields:
        assert f"{field.name} " in AUDIT_SCHEMA_DDL


def test_write_mode_is_recorded_so_counts_can_be_interpreted(spark, tmp_path):
    """#149: a consumer must be able to interpret row_count without joining
    back to a config it does not have."""
    cfg = _cfg(spark, tmp_path, "write_mode", write_mode="overwrite")

    with audited_run(spark, cfg, source_path=cfg.source_path) as audit:
        audit["row_count"] = 7

    row = spark.read.table(cfg.resolved_audit_table).collect()[0]
    assert row["write_mode"] == "overwrite"
    assert row["stream_batch_id"] is None  # batch runs carry no micro-batch id


def test_merge_counts_are_recorded_separately(spark, tmp_path):
    """#149: a merge that updates 500 and inserts 10 must record 500/10
    distinguishably, not a single ambiguous number."""
    cfg = _cfg(
        spark,
        tmp_path,
        "merge_counts",
        write_mode="merge",
        merge_keys=["id"],
        required_columns=["id"],
    )

    with audited_run(spark, cfg, source_path=cfg.source_path) as audit:
        audit["row_count"] = 510
        audit["source_row_count"] = 600
        audit["rows_inserted"] = 10
        audit["rows_updated"] = 500
        audit["rows_deleted"] = 0

    row = spark.read.table(cfg.resolved_audit_table).collect()[0]
    assert row["rows_inserted"] == 10
    assert row["rows_updated"] == 500
    assert row["row_count"] == 510
    # source - target is the dedupe/no-op ratio, previously unobservable.
    assert row["source_row_count"] - row["row_count"] == 90


def test_stream_batch_id_is_recorded_when_supplied(spark, tmp_path):
    """
    #156: every micro-batch in a streaming run shares one `run_id`, because
    the deployed job pins it to `{{job.id}}-{{job.run_id}}`. A day of
    30-second triggers therefore wrote 2,880 rows that a dashboard could not
    tell apart. `stream_batch_id` is what makes each one addressable while
    `run_id` keeps meaning "one job run".
    """
    cfg = _cfg(spark, tmp_path, "stream_batch", run_id="job-1-run-9")

    for batch in (0, 1, 2):
        with audited_run(spark, cfg, source_path=cfg.source_path, stream_batch_id=batch) as audit:
            audit["row_count"] = batch

    rows = spark.read.table(cfg.resolved_audit_table).collect()
    assert {r["run_id"] for r in rows} == {"job-1-run-9"}  # groups the job run
    assert sorted(r["stream_batch_id"] for r in rows) == [0, 1, 2]  # identifies the batch


def test_failure_path_records_every_caller_field(spark, tmp_path):
    """
    The success and failure paths used to list the same keys twice, which is
    how a new column gets recorded on success and silently omitted on
    failure. Both now build from one declared field list.
    """
    cfg = _cfg(
        spark,
        tmp_path,
        "failure_fields",
        write_mode="merge",
        merge_keys=["id"],
        required_columns=["id"],
    )

    with pytest.raises(RuntimeError):
        with audited_run(spark, cfg, source_path=cfg.source_path, stream_batch_id=4) as audit:
            audit["rows_inserted"] = 3
            audit["source_row_count"] = 9
            raise RuntimeError("write blew up after the counts were known")

    row = spark.read.table(cfg.resolved_audit_table).collect()[0]
    assert row["status"] == "failed"
    assert row["rows_inserted"] == 3
    assert row["source_row_count"] == 9
    assert row["write_mode"] == "merge"
    assert row["stream_batch_id"] == 4


def test_audited_run_failure_recovers_bad_count_and_stage_from_exception(spark, tmp_path):
    """#50: a DataQualityError-style failure should populate
    quarantined_row_count and failure_stage on the failed audit row,
    instead of leaving them None."""
    cfg = _cfg(spark, tmp_path, "quality_failure")

    class _FakeQualityError(Exception):
        pass

    with pytest.raises(_FakeQualityError):
        with audited_run(spark, cfg, source_path=cfg.source_path):
            exc = _FakeQualityError("17 rows failed data quality checks")
            exc.bad_count = 17
            tag_failure_stage(exc, "quality")
            raise exc

    row = spark.read.table(cfg.resolved_audit_table).collect()[0]
    assert row["status"] == "failed"
    assert row["quarantined_row_count"] == 17
    assert row["failure_stage"] == "quality"


def test_tag_failure_stage_does_not_overwrite_existing_stage(spark):
    exc = Exception("boom")
    tag_failure_stage(exc, "read")
    tag_failure_stage(exc, "write")  # simulates re-raising through a nested handler
    assert exc.failure_stage == "read"


def test_audited_run_success_records_schema_fingerprint(spark, tmp_path):
    """#51: schema_fingerprint/schema_changed populated on success should
    land in the run-level audit row, not just the separate registry table."""
    cfg = _cfg(spark, tmp_path, "schema_fp")

    with audited_run(spark, cfg, source_path=cfg.source_path) as audit:
        audit["row_count"] = 5
        audit["schema_fingerprint"] = "abc123"
        audit["schema_changed"] = True

    row = spark.read.table(cfg.resolved_audit_table).collect()[0]
    assert row["schema_fingerprint"] == "abc123"
    assert row["schema_changed"] is True


def test_audit_row_is_written_by_field_name_not_dict_order(spark, tmp_path):
    """
    The regression that made #149 fail in CI and pass locally.

    `_write_audit_row` used `Row(**row_dict)` with an explicit schema, which
    binds POSITIONALLY. When #149 added columns, the dict was built in a
    different order than AUDIT_SCHEMA, so every write raised - and the
    never-raise contract turned that into a warning nobody saw. The symptom
    was not a wrong row; it was NO TABLE AT ALL, surfacing as
    TABLE_OR_VIEW_NOT_FOUND in six unrelated tests.

    Passing a deliberately shuffled dict proves the projection is by name.
    """
    from bronze_ingest.audit import AUDIT_SCHEMA, _write_audit_row

    cfg = _cfg(spark, tmp_path, "field_order")
    shuffled = {
        "source_path": "/some/path",
        "status": "success",
        "run_id": "r-1",
        "rows_updated": 2,
        "table_name": cfg.full_table_name,
        "row_count": 5,
        "started_at": datetime(2026, 7, 30, tzinfo=timezone.utc),
        "write_mode": "merge",
    }
    _write_audit_row(spark, cfg, shuffled)

    row = spark.read.table(cfg.resolved_audit_table).collect()[0]
    assert row["run_id"] == "r-1"
    assert row["status"] == "success"
    assert row["row_count"] == 5
    assert row["rows_updated"] == 2
    assert row["write_mode"] == "merge"
    assert row["source_path"] == "/some/path"
    # Fields the caller omitted are NULL, not shifted from a neighbour.
    assert row["rows_inserted"] is None
    assert row["stream_batch_id"] is None
    # And the schema is intact.
    assert [f.name for f in spark.read.table(cfg.resolved_audit_table).schema.fields] == [
        f.name for f in AUDIT_SCHEMA.fields
    ]


def test_unknown_audit_field_is_reported_not_silently_dropped(spark, tmp_path, caplog):
    """A typo'd key would otherwise be dropped and read as a NULL column,
    which looks like 'the pipeline did not record that'."""
    from bronze_ingest.audit import _write_audit_row

    cfg = _cfg(spark, tmp_path, "unknown_field")
    _write_audit_row(
        spark,
        cfg,
        {
            "run_id": "r-2",
            "table_name": cfg.full_table_name,
            "status": "success",
            "started_at": datetime(2026, 7, 30, tzinfo=timezone.utc),
            "rows_inserted_typo": 3,
        },
    )

    assert "Ignoring unknown audit field(s)" in caplog.text
    assert "rows_inserted_typo" in caplog.text
