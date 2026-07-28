import uuid

import pytest

from bronze_ingest.config import IngestionConfig
from bronze_ingest.bronze_writer import write_bronze, add_audit_columns, NullMergeKeyError


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
    cfg = _cfg(table, write_mode="merge", merge_keys=["id"], required_columns=["id"])

    df = spark.createDataFrame([(1, "a"), (None, "b")], ["id", "name"])

    with pytest.raises(NullMergeKeyError):
        write_bronze(spark, df, cfg)

    assert not spark.catalog.tableExists(_table(table))


def test_merge_first_load_is_a_plain_append(spark):
    table = f"bw_first_load_{uuid.uuid4().hex[:8]}"
    cfg = _cfg(table, write_mode="merge", merge_keys=["id"], required_columns=["id"])

    df = spark.createDataFrame([(1, "a"), (2, "b")], ["id", "name"])
    full_name = write_bronze(spark, df, cfg)

    assert full_name == _table(table)
    assert spark.read.table(_table(table)).count() == 2


def test_merge_updates_matched_and_inserts_new_rows(spark):
    table = f"bw_merge_{uuid.uuid4().hex[:8]}"
    cfg = _cfg(table, write_mode="merge", merge_keys=["id"], required_columns=["id"])

    write_bronze(spark, spark.createDataFrame([(1, "a"), (2, "b")], ["id", "name"]), cfg)
    write_bronze(spark, spark.createDataFrame([(1, "a-updated"), (3, "c")], ["id", "name"]), cfg)

    rows = {r["id"]: r["name"] for r in spark.read.table(_table(table)).collect()}
    assert rows == {1: "a-updated", 2: "b", 3: "c"}


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
