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
