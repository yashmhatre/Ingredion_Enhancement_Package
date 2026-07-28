import pytest
from bronze_ingest.config import IngestionConfig


def test_requires_source_path_and_table():
    with pytest.raises(ValueError):
        IngestionConfig(source_path="", table="t")
    with pytest.raises(ValueError):
        IngestionConfig(source_path="s3://x", table="")


def test_merge_requires_merge_keys():
    with pytest.raises(ValueError):
        IngestionConfig(source_path="s3://x", table="t", write_mode="merge")
    # should not raise - merge_keys is also listed in required_columns
    IngestionConfig(
        source_path="s3://x", table="t", write_mode="merge",
        merge_keys=["id"], required_columns=["id"],
    )


def test_merge_requires_merge_keys_in_required_columns():
    # merge_keys not covered by required_columns - a NULL merge key would
    # never match in a MERGE condition and silently duplicate forever (#47)
    with pytest.raises(ValueError):
        IngestionConfig(source_path="s3://x", table="t", write_mode="merge", merge_keys=["id"])
    with pytest.raises(ValueError):
        IngestionConfig(
            source_path="s3://x", table="t", write_mode="merge",
            merge_keys=["id", "region"], required_columns=["id"],
        )
    # should not raise - all merge_keys covered
    IngestionConfig(
        source_path="s3://x", table="t", write_mode="merge",
        merge_keys=["id", "region"], required_columns=["id", "region", "other"],
    )


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


def test_cluster_by_and_partition_by_mutually_exclusive():
    with pytest.raises(ValueError):
        IngestionConfig(source_path="s3://x", table="t", partition_by=["date"], cluster_by=["id"])
    with pytest.raises(ValueError):
        IngestionConfig(source_path="s3://x", table="t", partition_by=["date"], cluster_by_auto=True)
    # should not raise - only one of the two layout strategies set
    IngestionConfig(source_path="s3://x", table="t", partition_by=["date"])
    IngestionConfig(source_path="s3://x", table="t", cluster_by=["id"])
    IngestionConfig(source_path="s3://x", table="t", cluster_by_auto=True)


def test_cluster_by_and_cluster_by_auto_mutually_exclusive():
    with pytest.raises(ValueError):
        IngestionConfig(source_path="s3://x", table="t", cluster_by=["id"], cluster_by_auto=True)


def test_cluster_by_empty_list_rejected():
    with pytest.raises(ValueError):
        IngestionConfig(source_path="s3://x", table="t", cluster_by=[])
