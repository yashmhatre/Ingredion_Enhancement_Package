import json
import os
import uuid

import pytest

import bronze_ingest.directory_ingestion as di
from bronze_ingest.config import IngestionConfig
from bronze_ingest.quality import enforce_quality, write_quarantine
from bronze_ingest.bronze_writer import add_audit_columns
from bronze_ingest.replay import reprocess_quarantine, reprocess_quarantined_files


def _cfg(table, **overrides):
    return IngestionConfig(
        source_path="file:///dev/null",
        table=table,
        schema_name="default",
        catalog=None,
        audit_schema_name="default",
        audit_table=f"_ingestion_audit_test_{uuid.uuid4().hex[:8]}",
        **overrides,
    )


def _table(table):
    return f"default.{table}"


def _quarantine(spark, df, cfg):
    """Runs df through the real quality gate + write_quarantine, exactly
    matching pipeline.py's production flow (add_audit_columns before the
    quarantine write), so replay tests exercise realistic quarantine rows."""
    good_df, bad_df, bad_count = enforce_quality(df, cfg)
    write_quarantine(spark, add_audit_columns(bad_df, cfg), bad_count, cfg)
    return bad_count


def _write(dir_path, name, content):
    with open(os.path.join(dir_path, name), "w") as f:
        f.write(content)


# ---- row replay ----

def test_reprocess_quarantine_no_table_is_noop(spark):
    cfg = _cfg(f"replay_no_table_{uuid.uuid4().hex[:8]}")
    result = reprocess_quarantine(spark, cfg)
    assert result == {
        "table": cfg.full_table_name, "replayed_row_count": 0,
        "still_quarantined_row_count": 0, "replay_batch_id": None,
    }


def test_reprocess_quarantine_promotes_now_passing_and_leaves_still_failing(spark):
    """
    Two rows quarantined because both `name` and `email` were required;
    the rule is then relaxed to only require `email` (simulating a fixed/
    relaxed quality rule). The row with only a null name now passes and
    should be promoted to bronze with fresh audit columns and removed
    from quarantine; the row with a null email should still fail and stay
    quarantined, untouched.
    """
    table = f"replay_mixed_{uuid.uuid4().hex[:8]}"
    quarantine_cfg = _cfg(table, required_columns=["name", "email"], fail_on_quality_error=False)

    df = spark.createDataFrame(
        [(1, None, "a@x.com"), (2, "Bob", None)],
        ["id", "name", "email"],
    )
    bad_count = _quarantine(spark, df, quarantine_cfg)
    assert bad_count == 2

    replay_cfg = _cfg(table, required_columns=["email"])
    result = reprocess_quarantine(spark, replay_cfg)

    assert result["replayed_row_count"] == 1
    assert result["still_quarantined_row_count"] == 1
    assert result["replay_batch_id"].startswith("replay-")

    bronze_rows = spark.read.table(_table(table)).collect()
    assert len(bronze_rows) == 1
    assert bronze_rows[0]["id"] == 1
    assert bronze_rows[0]["_batch_id"] == result["replay_batch_id"]
    assert bronze_rows[0]["_source_file"] is not None  # original lineage preserved, not blanked

    remaining = spark.read.table(replay_cfg.resolved_quarantine_table).collect()
    assert len(remaining) == 1
    assert remaining[0]["id"] == 2
    assert remaining[0]["_quarantine_reason"] == "null:email"  # untouched, specific to the still-null column


def test_reprocess_quarantine_is_idempotent_on_rerun(spark):
    table = f"replay_idempotent_{uuid.uuid4().hex[:8]}"
    quarantine_cfg = _cfg(table, required_columns=["name"], fail_on_quality_error=False)
    _quarantine(spark, spark.createDataFrame([(1, None)], "id INT, name STRING"), quarantine_cfg)

    replay_cfg = _cfg(table, required_columns=[])
    first = reprocess_quarantine(spark, replay_cfg)
    assert first["replayed_row_count"] == 1

    second = reprocess_quarantine(spark, replay_cfg)
    assert second["replayed_row_count"] == 0

    assert spark.read.table(_table(table)).count() == 1, "re-running replay must not duplicate bronze rows"


def test_reprocess_quarantine_filters_by_batch_id(spark):
    table = f"replay_batch_filter_{uuid.uuid4().hex[:8]}"
    cfg_a = _cfg(table, required_columns=["name"], fail_on_quality_error=False, batch_id="batch-A")
    cfg_b = _cfg(table, required_columns=["name"], fail_on_quality_error=False, batch_id="batch-B")

    _quarantine(spark, spark.createDataFrame([(1, None)], "id INT, name STRING"), cfg_a)
    _quarantine(spark, spark.createDataFrame([(2, None)], "id INT, name STRING"), cfg_b)

    replay_cfg = _cfg(table, required_columns=[])
    result = reprocess_quarantine(spark, replay_cfg, batch_id="batch-A")

    assert result["replayed_row_count"] == 1
    bronze_rows = spark.read.table(_table(table)).collect()
    assert len(bronze_rows) == 1
    assert bronze_rows[0]["id"] == 1

    remaining = spark.read.table(replay_cfg.resolved_quarantine_table).collect()
    assert len(remaining) == 1
    assert remaining[0]["id"] == 2


def test_reprocess_quarantine_records_audit_row(spark):
    table = f"replay_audit_{uuid.uuid4().hex[:8]}"
    quarantine_cfg = _cfg(table, required_columns=["name"], fail_on_quality_error=False)
    _quarantine(spark, spark.createDataFrame([(1, None)], "id INT, name STRING"), quarantine_cfg)

    replay_cfg = _cfg(table, required_columns=[])
    result = reprocess_quarantine(spark, replay_cfg)

    audit_rows = spark.read.table(replay_cfg.resolved_audit_table).collect()
    assert len(audit_rows) == 1
    assert audit_rows[0]["status"] == "success_replay"
    assert audit_rows[0]["row_count"] == result["replayed_row_count"]
    assert audit_rows[0]["quarantined_row_count"] == result["still_quarantined_row_count"]


# ---- file replay ----

def test_reprocess_quarantined_files_moves_files_back(spark, json_test_dir):
    write_dir, source_dir = json_test_dir
    qdir = os.path.join(write_dir, "quarantine_files")
    os.makedirs(qdir, exist_ok=True)
    _write(qdir, "bad.json", json.dumps({"id": 1}))

    result = reprocess_quarantined_files(spark, source_dir)

    assert result["count"] == 1
    assert result["moved"][0]["status"] == "moved"
    assert os.path.exists(os.path.join(write_dir, "bad.json"))
    assert not os.path.exists(os.path.join(qdir, "bad.json"))


def test_reprocess_quarantined_files_pattern_filter(spark, json_test_dir):
    write_dir, source_dir = json_test_dir
    qdir = os.path.join(write_dir, "quarantine_files")
    os.makedirs(qdir, exist_ok=True)
    _write(qdir, "orders_1.json", json.dumps({"id": 1}))
    _write(qdir, "customers_1.json", json.dumps({"id": 2}))

    result = reprocess_quarantined_files(spark, source_dir, pattern="orders_*.json")

    assert result["count"] == 1
    assert os.path.exists(os.path.join(write_dir, "orders_1.json"))
    assert not os.path.exists(os.path.join(write_dir, "customers_1.json"))
    assert os.path.exists(os.path.join(qdir, "customers_1.json"))


def test_reprocess_quarantined_files_missing_dir_is_noop(spark, json_test_dir):
    _, source_dir = json_test_dir
    result = reprocess_quarantined_files(spark, source_dir)
    assert result == {"moved": [], "count": 0}


def test_reprocess_quarantined_files_resets_retry_state(spark, json_test_dir):
    write_dir, source_dir = json_test_dir
    qdir = os.path.join(write_dir, "quarantine_files")
    os.makedirs(qdir, exist_ok=True)
    _write(qdir, "bad.json", json.dumps({"id": 1}))

    dest_path = f"{source_dir.rstrip('/')}/bad.json"
    di._write_retry_state(source_dir, {dest_path: 2})

    reprocess_quarantined_files(spark, source_dir)

    state = di._read_retry_state(source_dir)
    assert dest_path not in state


def test_replay_does_not_leak_quarantine_bookkeeping_into_bronze(spark):
    """`_occurrence_count` and `_first_quarantined_at` describe a row's
    history in QUARANTINE (#148), not the data. If replay doesn't drop them
    they ride the promotion and become columns on the bronze table."""
    table = f"replay_no_leak_{uuid.uuid4().hex[:8]}"
    cfg = _cfg(table, required_columns=["name"], fail_on_quality_error=False)
    df = spark.createDataFrame([(1, None), (2, "Bob")], "id INT, name STRING")
    _quarantine(spark, df, cfg)

    # Relax the rule so the previously-bad row now passes and is promoted.
    from dataclasses import replace

    result = reprocess_quarantine(spark, replace(cfg, required_columns=[]))

    assert result["replayed_row_count"] == 1
    bronze_cols = spark.read.table(_table(table)).columns
    for leaked in ("_occurrence_count", "_first_quarantined_at", "_quarantine_id",
                   "_quarantine_reason"):
        assert leaked not in bronze_cols, f"{leaked} leaked into bronze table"
