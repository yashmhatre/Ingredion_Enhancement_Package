import uuid

import pytest
from bronze_ingest.config import IngestionConfig
from bronze_ingest.quality import enforce_quality, split_good_bad, write_quarantine, DataQualityError

def _df(spark):
    return spark.createDataFrame(
        [(1, "Alice"), (2, None), (3, "Carol")],
        ["id", "name"],
    )


def test_no_required_columns_passes_everything(spark):
    df = _df(spark)
    cfg = IngestionConfig(source_path="x", table="t")
    good, bad = split_good_bad(df, cfg)
    assert good.count() == 3
    assert bad.count() == 0


def test_split_good_bad_partitions_nulls(spark):
    df = _df(spark)
    cfg = IngestionConfig(source_path="x", table="t", required_columns=["name"])
    good, bad = split_good_bad(df, cfg)
    assert good.count() == 2
    assert bad.count() == 1


def test_enforce_quality_raises_when_fail_on_error(spark):
    df = _df(spark)
    cfg = IngestionConfig(source_path="x", table="t", required_columns=["name"], fail_on_quality_error=True)
    with pytest.raises(DataQualityError):
        enforce_quality(df, cfg)


def test_data_quality_error_carries_bad_count(spark):
    """#50: the failure audit row needs to recover how many rows failed -
    DataQualityError must carry bad_count when enforce_quality raises it."""
    df = _df(spark)
    cfg = IngestionConfig(source_path="x", table="t", required_columns=["name"], fail_on_quality_error=True)
    with pytest.raises(DataQualityError) as exc_info:
        enforce_quality(df, cfg)
    assert exc_info.value.bad_count == 1


def test_missing_required_column_error_has_no_bad_count(spark):
    df = _df(spark)
    cfg = IngestionConfig(source_path="x", table="t", required_columns=["does_not_exist"], fail_on_quality_error=True)
    with pytest.raises(DataQualityError) as exc_info:
        enforce_quality(df, cfg)
    assert exc_info.value.bad_count is None


def test_enforce_quality_quarantines_when_not_failing(spark):
    df = _df(spark)
    cfg = IngestionConfig(source_path="x", table="t", required_columns=["name"], fail_on_quality_error=False)
    good, bad, bad_count = enforce_quality(df, cfg)
    assert bad_count == 1
    assert good.count() == 2


def test_missing_required_column_always_raises(spark):
    df = _df(spark)
    cfg = IngestionConfig(source_path="x", table="t", required_columns=["does_not_exist"], fail_on_quality_error=False)
    with pytest.raises(DataQualityError):
        enforce_quality(df, cfg)


def test_split_good_bad_does_not_leak_tag_column(spark):
    df = _df(spark)
    cfg = IngestionConfig(source_path="x", table="t", required_columns=["name"])
    good, bad = split_good_bad(df, cfg)
    assert "_dq_bad" not in good.columns
    assert "_dq_bad" not in bad.columns


def test_write_quarantine_writes_rows_when_bad_count_positive(spark):
    table = f"quality_quarantine_{uuid.uuid4().hex[:8]}"
    cfg = IngestionConfig(
        source_path="x", table=table, schema_name="default", catalog=None,
        required_columns=["name"], fail_on_quality_error=False,
    )
    good, bad, bad_count = enforce_quality(_df(spark), cfg)

    write_quarantine(spark, bad, bad_count, cfg)

    assert spark.read.table(cfg.resolved_quarantine_table).count() == 1


def test_write_quarantine_adds_unique_quarantine_id(spark):
    """#60: replay needs a stable per-row identifier to know exactly which
    quarantined rows were successfully re-promoted to bronze."""
    table = f"quality_quarantine_id_{uuid.uuid4().hex[:8]}"
    cfg = IngestionConfig(
        source_path="x", table=table, schema_name="default", catalog=None,
        required_columns=["name"], fail_on_quality_error=False,
    )
    df = spark.createDataFrame([(1, None), (2, None)], "id INT, name STRING")
    good, bad, bad_count = enforce_quality(df, cfg)

    write_quarantine(spark, bad, bad_count, cfg)

    rows = spark.read.table(cfg.resolved_quarantine_table).collect()
    ids = {r["_quarantine_id"] for r in rows}
    assert len(ids) == 2
    assert all(qid is not None for qid in ids)


def test_write_quarantine_uses_specific_reason(spark):
    table = f"quality_quarantine_reason_{uuid.uuid4().hex[:8]}"
    cfg = IngestionConfig(
        source_path="x", table=table, schema_name="default", catalog=None,
        required_columns=["name"], fail_on_quality_error=False,
    )
    good, bad, bad_count = enforce_quality(_df(spark), cfg)
    write_quarantine(spark, bad, bad_count, cfg)
    rows = spark.read.table(cfg.resolved_quarantine_table).collect()
    assert rows[0]["_quarantine_reason"] == "null:name"


# ---- unique_columns (#59, narrowed scope) ----

def test_split_good_bad_partitions_duplicates(spark):
    df = spark.createDataFrame(
        [(1, "Alice", "2024-01-01"), (1, "Alice2", "2024-01-02"), (2, "Bob", "2024-01-01")],
        ["id", "name", "ts"],
    )
    cfg = IngestionConfig(source_path="x", table="t", unique_columns=["id"], dedupe_order_by="ts")
    good, bad = split_good_bad(df, cfg)
    assert good.count() == 2
    assert bad.count() == 1
    kept = good.filter(good.id == 1).collect()[0]
    assert kept["name"] == "Alice2"  # higher ts wins the tie-break


def test_duplicate_quarantine_reason_is_specific(spark):
    df = spark.createDataFrame(
        [(1, "Alice", "2024-01-01"), (1, "Alice2", "2024-01-02")],
        ["id", "name", "ts"],
    )
    cfg = IngestionConfig(source_path="x", table="t", unique_columns=["id"], dedupe_order_by="ts")
    good, bad = split_good_bad(df, cfg)
    reasons = {r["_quarantine_reason"] for r in bad.collect()}
    assert reasons == {"duplicate:id"}


def test_null_and_duplicate_reasons_combine(spark):
    df = spark.createDataFrame(
        [(1, None, "2024-01-01"), (2, "Bob", "2024-01-01"), (2, "Bob2", "2024-01-02")],
        "id INT, name STRING, ts STRING",
    )
    cfg = IngestionConfig(
        source_path="x", table="t", required_columns=["name"], unique_columns=["id"], dedupe_order_by="ts",
    )
    good, bad = split_good_bad(df, cfg)
    reasons = {r["id"]: r["_quarantine_reason"] for r in bad.collect()}
    assert reasons[1] == "null:name"
    assert reasons[2] == "duplicate:id"
    assert good.count() == 1


def test_enforce_quality_raises_on_duplicates_when_fail_on_error(spark):
    df = spark.createDataFrame([(1, "Alice"), (1, "Bob")], ["id", "name"])
    cfg = IngestionConfig(source_path="x", table="t", unique_columns=["id"], fail_on_quality_error=True)
    with pytest.raises(DataQualityError) as exc_info:
        enforce_quality(df, cfg)
    assert exc_info.value.bad_count == 1


def test_enforce_quality_quarantines_duplicates_when_not_failing(spark):
    df = spark.createDataFrame([(1, "Alice"), (1, "Bob")], ["id", "name"])
    cfg = IngestionConfig(source_path="x", table="t", unique_columns=["id"], fail_on_quality_error=False)
    good, bad, bad_count = enforce_quality(df, cfg)
    assert bad_count == 1
    assert good.count() == 1


def test_missing_unique_column_always_raises(spark):
    df = _df(spark)
    cfg = IngestionConfig(source_path="x", table="t", unique_columns=["does_not_exist"], fail_on_quality_error=False)
    with pytest.raises(DataQualityError):
        enforce_quality(df, cfg)


def test_duplicate_check_does_not_leak_tag_columns(spark):
    df = spark.createDataFrame([(1, "Alice"), (1, "Bob")], ["id", "name"])
    cfg = IngestionConfig(source_path="x", table="t", unique_columns=["id"])
    good, bad = split_good_bad(df, cfg)
    for c in ("_dq_bad", "_dq_null", "_dq_dup"):
        assert c not in good.columns
        assert c not in bad.columns
    assert "_quarantine_reason" not in good.columns
    assert "_quarantine_reason" in bad.columns


def test_duplicate_tie_break_without_dedupe_order_by_is_deterministic(spark):
    """No dedupe_order_by and no matching source column - falls back to the
    monotonically_increasing_id() tie-break rather than raising."""
    df = spark.createDataFrame([(1, "Alice"), (1, "Bob")], ["id", "name"])
    cfg = IngestionConfig(source_path="x", table="t", unique_columns=["id"])
    good, bad = split_good_bad(df, cfg)
    assert good.count() == 1
    assert bad.count() == 1


def test_write_quarantine_skips_when_bad_count_zero(spark):
    table = f"quality_quarantine_skip_{uuid.uuid4().hex[:8]}"
    cfg = IngestionConfig(
        source_path="x", table=table, schema_name="default", catalog=None,
        required_columns=["name"], fail_on_quality_error=False,
    )
    all_good_df = spark.createDataFrame([(1, "Alice"), (2, "Bob")], ["id", "name"])
    good, bad, bad_count = enforce_quality(all_good_df, cfg)

    assert bad_count == 0
    write_quarantine(spark, bad, bad_count, cfg)

    assert not spark.catalog.tableExists(cfg.resolved_quarantine_table)
