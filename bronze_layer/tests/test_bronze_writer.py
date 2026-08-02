import threading
import uuid

import pytest

from bronze_ingest.bronze_writer import (
    DuplicateMergeKeyError,
    NullMergeKeyError,
    _resolve_idempotent_txn_version,
    add_audit_columns,
    write_bronze,
)
from bronze_ingest.config import IngestionConfig


def _cfg(table, **overrides):
    return IngestionConfig(
        source_path="file:///dev/null",
        table=table,
        schema_name="default",
        catalog=None,
        **overrides,
    )


def _table(table):
    return f"default.{table}"


def test_merge_refuses_null_merge_keys(spark):
    table = f"bw_null_key_{uuid.uuid4().hex[:8]}"
    # retry_attempts=1: this is a deterministic config error, not a
    # transient failure - no point burning the default 10s/20s backoff
    # retrying something that will fail identically every time.
    cfg = _cfg(
        table, write_mode="merge", merge_keys=["id"], required_columns=["id"], retry_attempts=1
    )

    df = spark.createDataFrame([(1, "a"), (None, "b")], ["id", "name"])

    with pytest.raises(NullMergeKeyError):
        write_bronze(spark, df, cfg)

    assert not spark.catalog.tableExists(_table(table))


def test_merge_first_load_creates_table_and_merges(spark):
    table = f"bw_first_load_{uuid.uuid4().hex[:8]}"
    # dedupe_before_merge is orthogonal to this test - disable it so this
    # test doesn't depend on audit columns being present (see #48 tests).
    cfg = _cfg(
        table,
        write_mode="merge",
        merge_keys=["id"],
        required_columns=["id"],
        dedupe_before_merge=False,
    )

    df = spark.createDataFrame([(1, "a"), (2, "b")], ["id", "name"])
    full_name = write_bronze(spark, df, cfg)

    assert full_name == _table(table)
    assert spark.read.table(_table(table)).count() == 2


def test_concurrent_first_loads_do_not_duplicate_rows(spark):
    """
    Regression test for #46: two concurrent first-runs against the same
    not-yet-existing table used to both observe "table doesn't exist" and
    both take an append path, duplicating the whole first batch. The fix
    (atomic CREATE TABLE IF NOT EXISTS + always MERGE) should converge to
    the deduplicated row count no matter how the two writers interleave.
    """
    table = f"bw_race_{uuid.uuid4().hex[:8]}"
    cfg = _cfg(
        table,
        write_mode="merge",
        merge_keys=["id"],
        required_columns=["id"],
        retry_attempts=5,
        retry_delay_seconds=0.1,
        dedupe_before_merge=False,
    )

    barrier = threading.Barrier(2)
    errors = []

    def _run(rows):
        try:
            barrier.wait(timeout=10)
            write_bronze(spark, spark.createDataFrame(rows, ["id", "name"]), cfg)
        except Exception as exc:  # noqa: BLE001 - collects whatever a concurrent writer thread raised, for later assertion
            errors.append(exc)

    t1 = threading.Thread(target=_run, args=([(1, "a"), (2, "b")],))
    t2 = threading.Thread(target=_run, args=([(1, "a"), (2, "b")],))
    t1.start()
    t2.start()
    t1.join(timeout=60)
    t2.join(timeout=60)

    assert not errors, f"unexpected errors from concurrent first loads: {errors}"
    assert spark.read.table(_table(table)).count() == 2


def test_merge_updates_matched_and_inserts_new_rows(spark):
    table = f"bw_merge_{uuid.uuid4().hex[:8]}"
    cfg = _cfg(
        table,
        write_mode="merge",
        merge_keys=["id"],
        required_columns=["id"],
        dedupe_before_merge=False,
    )

    write_bronze(spark, spark.createDataFrame([(1, "a"), (2, "b")], ["id", "name"]), cfg)
    write_bronze(spark, spark.createDataFrame([(1, "a-updated"), (3, "c")], ["id", "name"]), cfg)

    rows = {r["id"]: r["name"] for r in spark.read.table(_table(table)).collect()}
    assert rows == {1: "a-updated", 2: "b", 3: "c"}


def test_merge_dedupes_duplicate_keys_before_merge(spark):
    """#48: a source batch with more than one row per merge key used to
    make Delta MERGE throw a cryptic 'multiple source rows matched' error.
    dedupe_before_merge=True (default) should keep exactly one row per key,
    picking the highest dedupe_order_by value."""
    table = f"bw_dedupe_{uuid.uuid4().hex[:8]}"
    cfg = _cfg(
        table,
        write_mode="merge",
        merge_keys=["id"],
        required_columns=["id"],
        dedupe_order_by="version",
    )

    df = spark.createDataFrame(
        [(1, "a-v1", 1), (1, "a-v2", 2), (2, "b", 1)],
        ["id", "name", "version"],
    )
    write_bronze(spark, df, cfg)

    rows = {r["id"]: r["name"] for r in spark.read.table(_table(table)).collect()}
    assert rows == {1: "a-v2", 2: "b"}


def test_merge_raises_clear_error_on_duplicates_when_dedupe_disabled(spark):
    table = f"bw_dupe_fail_{uuid.uuid4().hex[:8]}"
    cfg = _cfg(
        table,
        write_mode="merge",
        merge_keys=["id"],
        required_columns=["id"],
        dedupe_before_merge=False,
        retry_attempts=1,
    )

    df = spark.createDataFrame([(1, "a"), (1, "a-again"), (2, "b")], ["id", "name"])

    with pytest.raises(DuplicateMergeKeyError, match=r"\{'id': 1\}"):
        write_bronze(spark, df, cfg)

    assert not spark.catalog.tableExists(_table(table))


def test_merge_dedupe_missing_order_column_raises_clear_error(spark):
    table = f"bw_dedupe_missing_col_{uuid.uuid4().hex[:8]}"
    # dedupe_before_merge defaults True, and dedupe_order_by defaults to
    # audit_ingest_ts_col ("_ingested_at"), which isn't present here since
    # add_audit_columns() was never called on this raw DataFrame.
    cfg = _cfg(
        table, write_mode="merge", merge_keys=["id"], required_columns=["id"], retry_attempts=1
    )
    df = spark.createDataFrame([(1, "a"), (2, "b")], ["id", "name"])

    with pytest.raises(ValueError, match="_ingested_at"):
        write_bronze(spark, df, cfg)


def test_append_mode_does_not_require_merge_keys(spark):
    table = f"bw_append_{uuid.uuid4().hex[:8]}"
    cfg = _cfg(table, write_mode="append")

    df = spark.createDataFrame([(1, None), (2, "b")], ["id", "name"])
    write_bronze(spark, df, cfg)

    assert spark.read.table(_table(table)).count() == 2


def test_add_audit_columns_renames_input_file_name(spark):
    cfg = _cfg("bw_audit_with_lineage")
    df = spark.createDataFrame(
        [(1, "a", "abfss://x/orders/f1.json")], ["id", "name", "_input_file_name"]
    )

    result = add_audit_columns(df, cfg)

    assert "_input_file_name" not in result.columns
    row = result.collect()[0]
    assert row[cfg.audit_source_file_col] == "abfss://x/orders/f1.json"


def test_add_audit_columns_falls_back_to_source_path_without_lineage(spark, caplog):
    cfg = _cfg("bw_audit_no_lineage")
    df = spark.createDataFrame([(1, "a"), (2, "b")], ["id", "name"])

    with caplog.at_level("WARNING"):
        result = add_audit_columns(df, cfg)

    rows = result.collect()
    assert all(r[cfg.audit_source_file_col] == cfg.source_path for r in rows)
    assert any("_input_file_name" in rec.message for rec in caplog.records)


def _layout(spark, table):
    """(clusteringColumns, properties) for an already-written table - see #57."""
    row = (
        spark.sql(f"DESCRIBE DETAIL {_table(table)}")
        .select("clusteringColumns", "properties")
        .collect()[0]
    )
    return row["clusteringColumns"], (row["properties"] or {})


def test_append_creates_liquid_clustered_table(spark):
    table = f"bw_cluster_append_{uuid.uuid4().hex[:8]}"
    cfg = _cfg(table, write_mode="append", cluster_by=["id"])

    write_bronze(spark, spark.createDataFrame([(1, "a"), (2, "b")], ["id", "name"]), cfg)

    cluster_cols, _ = _layout(spark, table)
    assert cluster_cols == ["id"]
    assert spark.read.table(_table(table)).count() == 2


def test_overwrite_preserves_clustering_across_runs(spark):
    """
    Regression test for a real Delta quirk found while implementing #57:
    an unqualified mode("overwrite").saveAsTable(...) performs an implicit
    REPLACE TABLE that silently drops CLUSTER BY unless restored - verified
    empirically against this package's delta-spark version. Clustering
    must still be in place after every overwrite run, not just the first.
    """
    table = f"bw_cluster_overwrite_{uuid.uuid4().hex[:8]}"
    cfg = _cfg(table, write_mode="overwrite", cluster_by=["id"])

    write_bronze(spark, spark.createDataFrame([(1, "a"), (2, "b")], ["id", "name"]), cfg)
    cluster_cols, _ = _layout(spark, table)
    assert cluster_cols == ["id"], "clustering must survive the first overwrite"

    write_bronze(spark, spark.createDataFrame([(3, "c")], ["id", "name"]), cfg)
    cluster_cols, _ = _layout(spark, table)
    assert cluster_cols == ["id"], "clustering must survive a second overwrite too"
    assert spark.read.table(_table(table)).count() == 1


def test_merge_creates_liquid_clustered_table(spark):
    table = f"bw_cluster_merge_{uuid.uuid4().hex[:8]}"
    cfg = _cfg(
        table,
        write_mode="merge",
        merge_keys=["id"],
        required_columns=["id"],
        cluster_by=["id"],
        dedupe_before_merge=False,
    )

    write_bronze(spark, spark.createDataFrame([(1, "a"), (2, "b")], ["id", "name"]), cfg)
    cluster_cols, _ = _layout(spark, table)
    assert cluster_cols == ["id"]

    write_bronze(spark, spark.createDataFrame([(1, "a-updated"), (3, "c")], ["id", "name"]), cfg)
    cluster_cols, _ = _layout(spark, table)
    assert cluster_cols == ["id"], "clustering must survive a subsequent merge too"


def test_table_properties_applied_at_creation(spark):
    table = f"bw_props_{uuid.uuid4().hex[:8]}"
    cfg = _cfg(
        table,
        write_mode="append",
        table_properties={"delta.enableChangeDataFeed": "true"},
    )

    write_bronze(spark, spark.createDataFrame([(1, "a")], ["id", "name"]), cfg)

    _, props = _layout(spark, table)
    assert props.get("delta.enableChangeDataFeed") == "true"


def test_table_properties_altered_when_config_changes(spark):
    table = f"bw_props_drift_{uuid.uuid4().hex[:8]}"
    cfg = _cfg(table, write_mode="append", table_properties={"delta.enableChangeDataFeed": "true"})
    write_bronze(spark, spark.createDataFrame([(1, "a")], ["id", "name"]), cfg)

    cfg2 = _cfg(
        table,
        write_mode="append",
        table_properties={
            "delta.enableChangeDataFeed": "true",
            "delta.logRetentionDuration": "interval 60 days",
        },
    )
    write_bronze(spark, spark.createDataFrame([(2, "b")], ["id", "name"]), cfg2)

    _, props = _layout(spark, table)
    assert props.get("delta.enableChangeDataFeed") == "true"
    assert props.get("delta.logRetentionDuration") == "interval 60 days"


def test_cluster_by_alters_existing_table_on_drift(spark, caplog):
    table = f"bw_cluster_drift_{uuid.uuid4().hex[:8]}"
    cfg = _cfg(table, write_mode="append", cluster_by=["id"])
    write_bronze(spark, spark.createDataFrame([(1, "a")], ["id", "name"]), cfg)
    assert _layout(spark, table)[0] == ["id"]

    cfg2 = _cfg(table, write_mode="append", cluster_by=["name"])
    with caplog.at_level("WARNING"):
        write_bronze(spark, spark.createDataFrame([(2, "b")], ["id", "name"]), cfg2)

    assert _layout(spark, table)[0] == ["name"]
    assert any("Cluster-by columns changed" in rec.message for rec in caplog.records)


def test_cluster_by_auto_degrades_gracefully_when_unsupported(spark, caplog):
    """
    CLUSTER BY AUTO is a Databricks Runtime-only SQL extension - verified
    it isn't parseable against this package's supported delta-spark
    versions when run outside Databricks Runtime. The write itself must
    still succeed; only a WARNING should be logged, never a hard failure.
    """
    table = f"bw_cluster_auto_{uuid.uuid4().hex[:8]}"
    cfg = _cfg(table, write_mode="append", cluster_by_auto=True)

    with caplog.at_level("WARNING"):
        write_bronze(spark, spark.createDataFrame([(1, "a")], ["id", "name"]), cfg)

    assert spark.read.table(_table(table)).count() == 1
    assert any("cluster_by_auto=True" in rec.message for rec in caplog.records)


# ---- idempotent batch writes (#63) ----


def test_resolve_idempotent_txn_version_cases():
    cfg_int_str = _cfg("t", batch_id="12345")
    assert _resolve_idempotent_txn_version(cfg_int_str) == 12345

    cfg_timestamp = _cfg("t", batch_id="20260728T120000000000Z")
    version = _resolve_idempotent_txn_version(cfg_timestamp)
    assert isinstance(version, int) and version > 0
    # Same string must always resolve to the same version (stable, not wall-clock-dependent).
    assert _resolve_idempotent_txn_version(cfg_timestamp) == version

    cfg_arbitrary = _cfg("t", batch_id="not-a-number-or-timestamp")
    assert _resolve_idempotent_txn_version(cfg_arbitrary) is None

    cfg_none = _cfg("t")
    assert _resolve_idempotent_txn_version(cfg_none) is None


def test_idempotent_batch_writes_prevents_duplicate_append_on_retry(spark):
    """
    #63: a retried batch job (write succeeded, a downstream step then
    failed) re-running with the SAME explicit batch_id must converge to
    one copy of the data, not duplicate it.
    """
    table = f"bw_idempotent_append_{uuid.uuid4().hex[:8]}"
    cfg = _cfg(table, write_mode="append", batch_id="1001")

    df = spark.createDataFrame([(1, "a"), (2, "b")], ["id", "name"])
    write_bronze(spark, df, cfg)
    write_bronze(spark, df, cfg)  # simulated retry - same batch_id, same data

    assert spark.read.table(_table(table)).count() == 2


def test_idempotent_batch_writes_different_batch_ids_append_normally(spark):
    table = f"bw_idempotent_diff_batch_{uuid.uuid4().hex[:8]}"
    cfg1 = _cfg(table, write_mode="append", batch_id="2001")
    cfg2 = _cfg(table, write_mode="append", batch_id="2002")

    write_bronze(spark, spark.createDataFrame([(1, "a")], ["id", "name"]), cfg1)
    write_bronze(spark, spark.createDataFrame([(2, "b")], ["id", "name"]), cfg2)

    assert spark.read.table(_table(table)).count() == 2


def test_idempotent_batch_writes_skipped_when_batch_id_none(spark, caplog):
    """An auto-generated (None) batch_id can't provide retry protection,
    since it's a fresh value on every attempt - document this rather than
    pretending it's protected. The write itself must still succeed."""
    table = f"bw_idempotent_no_batch_id_{uuid.uuid4().hex[:8]}"
    cfg = _cfg(table, write_mode="append")

    with caplog.at_level("DEBUG"):
        write_bronze(spark, spark.createDataFrame([(1, "a")], ["id", "name"]), cfg)

    assert spark.read.table(_table(table)).count() == 1


def test_idempotent_batch_writes_warns_on_unparseable_batch_id(spark, caplog):
    table = f"bw_idempotent_bad_batch_id_{uuid.uuid4().hex[:8]}"
    cfg = _cfg(table, write_mode="append", batch_id="release-2026-07-28")

    with caplog.at_level("WARNING"):
        write_bronze(spark, spark.createDataFrame([(1, "a")], ["id", "name"]), cfg)

    assert spark.read.table(_table(table)).count() == 1
    assert any("can't derive a stable txnVersion" in rec.message for rec in caplog.records)


def test_idempotent_batch_writes_disabled_via_config(spark):
    """Opt-out must be honored - the same batch_id written twice with
    idempotent_batch_writes=False duplicates, as a plain append would."""
    table = f"bw_idempotent_disabled_{uuid.uuid4().hex[:8]}"
    cfg = _cfg(table, write_mode="append", batch_id="3001", idempotent_batch_writes=False)

    df = spark.createDataFrame([(1, "a")], ["id", "name"])
    write_bronze(spark, df, cfg)
    write_bronze(spark, df, cfg)

    assert spark.read.table(_table(table)).count() == 2


def test_idempotent_batch_writes_not_applied_to_merge(spark, monkeypatch):
    """Delta MERGE doesn't accept txn options - write_bronze must not pass
    any for write_mode='merge', even with a stable batch_id configured."""
    import bronze_ingest.bronze_writer as bw

    table = f"bw_idempotent_merge_{uuid.uuid4().hex[:8]}"
    cfg = _cfg(
        table,
        write_mode="merge",
        merge_keys=["id"],
        required_columns=["id"],
        batch_id="4001",
        dedupe_before_merge=False,
    )

    captured = {}
    real_write_core = bw._write_core

    def spy(spark_arg, df_arg, config_arg, txn_options=None):
        captured["txn_options"] = txn_options
        return real_write_core(spark_arg, df_arg, config_arg, txn_options=txn_options)

    monkeypatch.setattr(bw, "_write_core", spy)

    write_bronze(spark, spark.createDataFrame([(1, "a")], ["id", "name"]), cfg)

    assert captured["txn_options"] is None


def test_read_write_metrics_never_raises_on_an_unreadable_table(caplog):
    """
    #149: metrics come from Delta's transaction log, and reading them must
    never fail a write that has ALREADY COMMITTED. A run that wrote its data
    successfully and then lost its row counts is an annoyance; the same run
    reported as failed is a false alarm someone gets paged for.

    No Spark session needed - passing None makes the Delta call fail, which
    is exactly the path under test.
    """
    from bronze_ingest.bronze_writer import EMPTY_WRITE_METRICS, read_write_metrics

    metrics = read_write_metrics(None, "no.such.table", "append")

    assert metrics == EMPTY_WRITE_METRICS
    assert all(v is None for v in metrics.values())
    assert "Could not read write metrics" in caplog.text


def test_resolve_batch_id_is_stable_for_one_config_and_explicit_value():
    """#148: an explicit batch_id must survive verbatim - the deployed job
    passes {{job.run_id}} and #63's idempotency is keyed on it."""
    from bronze_ingest.bronze_writer import resolve_batch_id

    cfg = _cfg("t", batch_id="12345")
    assert resolve_batch_id(cfg) == "12345"
    assert resolve_batch_id(cfg) == "12345"


def test_resolve_batch_id_generates_a_distinct_value_when_unset():
    """
    The generated form is a timestamp, so two SEPARATE runs get different
    ids - which is correct. The bug #148 fixed was calling this twice within
    ONE run, which is why the pipeline resolves it once and passes it down.
    """
    from bronze_ingest.bronze_writer import resolve_batch_id

    cfg = _cfg("t")
    first = resolve_batch_id(cfg)
    assert first and first.endswith("Z")
