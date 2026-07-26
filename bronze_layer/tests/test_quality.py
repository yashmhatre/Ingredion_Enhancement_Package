import pytest
from bronze_json_loader.config import IngestionConfig
from bronze_json_loader.quality import enforce_quality, split_good_bad, DataQualityError


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
