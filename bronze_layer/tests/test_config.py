import pytest
from bronze_json_loader.config import IngestionConfig


def test_requires_source_path_and_table():
    with pytest.raises(ValueError):
        IngestionConfig(source_path="", table="t")
    with pytest.raises(ValueError):
        IngestionConfig(source_path="s3://x", table="")


def test_invalid_flatten_mode_rejected():
    with pytest.raises(ValueError):
        IngestionConfig(source_path="s3://x", table="t", flatten_mode="bogus")


def test_merge_requires_merge_keys():
    with pytest.raises(ValueError):
        IngestionConfig(source_path="s3://x", table="t", write_mode="merge")
    # should not raise
    IngestionConfig(source_path="s3://x", table="t", write_mode="merge", merge_keys=["id"])


def test_streaming_requires_checkpoint_and_schema_location():
    with pytest.raises(ValueError):
        IngestionConfig(source_path="s3://x", table="t", ingestion_mode="streaming")
    IngestionConfig(
        source_path="s3://x",
        table="t",
        ingestion_mode="streaming",
        checkpoint_location="/chk",
        schema_location="/schema",
    )


def test_full_table_name_and_quarantine_name():
    cfg = IngestionConfig(source_path="s3://x", table="orders", schema_name="bronze", catalog="main")
    assert cfg.full_table_name == "main.bronze.orders"
    assert cfg.resolved_quarantine_table == "main.bronze.orders_quarantine"


def test_from_dict_ignores_unknown_keys():
    cfg = IngestionConfig.from_dict({"source_path": "s3://x", "table": "t", "not_a_real_field": 123})
    assert cfg.table == "t"
