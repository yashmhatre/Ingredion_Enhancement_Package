import json
import os
import uuid

import bronze_ingest.directory_ingestion as di
from bronze_ingest.bronze_writer import add_audit_columns
from bronze_ingest.config import IngestionConfig
from bronze_ingest.quality import enforce_quality, write_quarantine
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
        "table": cfg.full_table_name,
        "replayed_row_count": 0,
        "still_quarantined_row_count": 0,
        "replay_batch_id": None,
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
    assert (
        remaining[0]["_quarantine_reason"] == "null:email"
    )  # untouched, specific to the still-null column


def test_reprocess_quarantine_is_idempotent_on_rerun(spark):
    table = f"replay_idempotent_{uuid.uuid4().hex[:8]}"
    quarantine_cfg = _cfg(table, required_columns=["name"], fail_on_quality_error=False)
    _quarantine(spark, spark.createDataFrame([(1, None)], "id INT, name STRING"), quarantine_cfg)

    replay_cfg = _cfg(table, required_columns=[])
    first = reprocess_quarantine(spark, replay_cfg)
    assert first["replayed_row_count"] == 1

    second = reprocess_quarantine(spark, replay_cfg)
    assert second["replayed_row_count"] == 0

    assert spark.read.table(_table(table)).count() == 1, (
        "re-running replay must not duplicate bronze rows"
    )


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
    for leaked in (
        "_occurrence_count",
        "_first_quarantined_at",
        "_quarantine_id",
        "_quarantine_reason",
    ):
        assert leaked not in bronze_cols, f"{leaked} leaked into bronze table"


# ---------------------------------------------------------------------------
# Scale characteristics (#155)
# ---------------------------------------------------------------------------


def test_replay_uses_no_driver_side_collect_of_row_data():
    """
    Static guard on the property, not just on one execution.

    The old form was `[r["_quarantine_id"] for r in ...collect()]` followed by
    a driver-built `IN (...)` list. Both scaled linearly with the replayed row
    count and neither had a bound: ~200 bytes of driver heap per id, and ~39
    bytes of SQL text per id in a single predicate. A test at a realistic size
    would not have caught it - the old code worked fine at the scale it was
    tested at. That is the whole point of #155.
    """
    import inspect

    from bronze_ingest import replay

    source = inspect.getsource(replay.reprocess_quarantine)
    assert ".collect()" not in source, (
        "reprocess_quarantine must not collect per-row data to the driver - "
        "replay is exactly the operation that gets large"
    )
    assert "IN (" not in source, "the quarantine delete must not build a SQL IN list"
    assert "whenMatchedDelete" in source, "the delete should be a distributed MERGE"


def test_replay_promotes_and_removes_a_batch_far_larger_than_the_old_in_list(spark):
    """
    5,000 quarantined rows - comfortably past the low tens of thousands where
    the old `IN (...)` predicate started failing unpredictably, and large
    enough that a driver-side collect would show up, while still quick.

    Asserts the invariant the whole operation rests on: the set promoted to
    bronze and the set removed from quarantine are the same set.
    """
    table = f"replay_scale_{uuid.uuid4().hex[:8]}"
    cfg = _cfg(table, required_columns=["id"], fail_on_quality_error=False)

    # Every row fails the gate (id is null), so all 5,000 land in quarantine.
    rows = [(None, f"payload-{i}") for i in range(5000)]
    df = spark.createDataFrame(rows, "id STRING, note STRING")
    assert _quarantine(spark, df, cfg) == 5000
    assert spark.read.table(cfg.resolved_quarantine_table).count() == 5000

    # Drop the rule so every row now passes, then replay.
    relaxed = _cfg(
        table, required_columns=[], fail_on_quality_error=False, audit_table=cfg.audit_table
    )
    result = reprocess_quarantine(spark, relaxed)

    assert result["replayed_row_count"] == 5000
    assert result["still_quarantined_row_count"] == 0
    assert spark.read.table(_table(table)).count() == 5000
    # Removed from quarantine, not left behind to be promoted again.
    assert spark.read.table(cfg.resolved_quarantine_table).count() == 0


def test_replay_is_idempotent_after_a_full_promotion(spark):
    """A second replay must find nothing left, which is only true if the
    delete actually removed what the write promoted."""
    table = f"replay_idem_{uuid.uuid4().hex[:8]}"
    cfg = _cfg(table, required_columns=["id"], fail_on_quality_error=False)

    df = spark.createDataFrame([(None, "a"), (None, "b")], "id STRING, note STRING")
    _quarantine(spark, df, cfg)

    relaxed = _cfg(
        table, required_columns=[], fail_on_quality_error=False, audit_table=cfg.audit_table
    )
    first = reprocess_quarantine(spark, relaxed)
    second = reprocess_quarantine(spark, relaxed)

    assert first["replayed_row_count"] == 2
    assert second["replayed_row_count"] == 0
    assert spark.read.table(_table(table)).count() == 2, "no double promotion"


def test_max_rows_guard_refuses_an_oversized_replay_before_writing(spark):
    """
    The guard must fire BEFORE the bronze write. Failing afterwards would
    leave rows in both tables, which is the state #155 is about avoiding.
    """
    import pytest

    table = f"replay_guard_{uuid.uuid4().hex[:8]}"
    cfg = _cfg(table, required_columns=["id"], fail_on_quality_error=False)

    df = spark.createDataFrame([(None, f"r{i}") for i in range(10)], "id STRING, note STRING")
    _quarantine(spark, df, cfg)

    relaxed = _cfg(
        table, required_columns=[], fail_on_quality_error=False, audit_table=cfg.audit_table
    )
    with pytest.raises(ValueError, match="exceeds max_rows"):
        reprocess_quarantine(spark, relaxed, max_rows=3)

    # Nothing promoted, nothing removed - the quarantine table is untouched.
    assert not spark.catalog.tableExists(_table(table))
    assert spark.read.table(cfg.resolved_quarantine_table).count() == 10


def test_max_rows_message_points_at_the_way_out(spark):
    """An operator who hits this needs to know what to do next, not just
    that a number was exceeded."""
    import pytest

    table = f"replay_guard_msg_{uuid.uuid4().hex[:8]}"
    cfg = _cfg(table, required_columns=["id"], fail_on_quality_error=False)
    df = spark.createDataFrame([(None, "a"), (None, "b")], "id STRING, note STRING")
    _quarantine(spark, df, cfg)

    relaxed = _cfg(
        table, required_columns=[], fail_on_quality_error=False, audit_table=cfg.audit_table
    )
    with pytest.raises(ValueError) as excinfo:
        reprocess_quarantine(spark, relaxed, max_rows=1)

    message = str(excinfo.value)
    assert "batch_id" in message and "since" in message
    assert "max_rows" in message


def test_max_rows_none_lifts_the_guard(spark):
    table = f"replay_nolimit_{uuid.uuid4().hex[:8]}"
    cfg = _cfg(table, required_columns=["id"], fail_on_quality_error=False)
    df = spark.createDataFrame([(None, "a"), (None, "b")], "id STRING, note STRING")
    _quarantine(spark, df, cfg)

    relaxed = _cfg(
        table, required_columns=[], fail_on_quality_error=False, audit_table=cfg.audit_table
    )
    result = reprocess_quarantine(spark, relaxed, max_rows=None)
    assert result["replayed_row_count"] == 2
