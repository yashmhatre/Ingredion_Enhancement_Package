from pyspark.sql import Row
from bronze_json_loader.flattener import flatten_dataframe, _max_struct_depth
from bronze_json_loader.config import IngestionConfig
from bronze_json_loader.flattener import apply_flatten_mode


def _sample_df(spark):
    # Row(...) (not plain dicts) so Spark infers real StructType columns,
    # matching what the JSON reader actually produces from nested JSON.
    data = [
        Row(id=1, customer=Row(name="Alice", address=Row(city="Mumbai")),
            items=[Row(sku="A1", qty=2)]),
        Row(id=2, customer=Row(name="Bob", address=Row(city="Pune")),
            items=[Row(sku="B2", qty=1)]),
    ]
    return spark.createDataFrame(data)


def test_flatten_dataframe_expands_nested_structs(spark):
    df = _sample_df(spark)
    flat = flatten_dataframe(df, separator="_", explode_arrays=False)
    assert "customer_name" in flat.columns
    assert "customer_address_city" in flat.columns
    assert "customer" not in flat.columns


def test_flatten_dataframe_explodes_arrays(spark):
    df = _sample_df(spark)
    flat = flatten_dataframe(df, separator="_", explode_arrays=True)
    assert "items_sku" in flat.columns
    assert "items_qty" in flat.columns
    # exploding a 1-element array shouldn't change row count here
    assert flat.count() == 2


def test_raw_mode_leaves_structs_untouched(spark):
    df = _sample_df(spark)
    cfg = IngestionConfig(source_path="x", table="t", flatten_mode="raw")
    out = apply_flatten_mode(df, cfg)
    assert "customer" in out.columns
    assert out.schema["customer"].dataType.typeName() == "struct"


def test_auto_mode_flattens_when_shallow(spark):
    df = _sample_df(spark)
    cfg = IngestionConfig(source_path="x", table="t", flatten_mode="auto", auto_flatten_threshold=5)
    out = apply_flatten_mode(df, cfg)
    assert "customer_name" in out.columns


def test_auto_mode_keeps_raw_when_too_deep(spark):
    df = _sample_df(spark)
    cfg = IngestionConfig(source_path="x", table="t", flatten_mode="auto", auto_flatten_threshold=0)
    out = apply_flatten_mode(df, cfg)
    assert "customer" in out.columns  # too deep for threshold=0, stays raw
