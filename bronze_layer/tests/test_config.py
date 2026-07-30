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


def test_unique_columns_empty_list_rejected():
    with pytest.raises(ValueError):
        IngestionConfig(source_path="s3://x", table="t", unique_columns=[])


def test_unique_columns_none_or_populated_is_valid():
    IngestionConfig(source_path="s3://x", table="t")
    IngestionConfig(source_path="s3://x", table="t", unique_columns=["order_id"])


# ---- identifier safety (#154) ----
#
# Every one of these values reaches spark.sql() by string interpolation.
# Validating at config load means the run fails before a cluster starts and
# the message names the field, instead of a Spark parse error surfacing from
# inside a generated statement 40 minutes in.


def _cfg(**overrides):
    base = dict(source_path="x", table="t")
    base.update(overrides)
    return IngestionConfig(**base)


@pytest.mark.parametrize("bad", [
    "orders-2024",          # the realistic case: a hyphen
    "it's",                 # an apostrophe - breaks the literal
    "x' OR '1'='1",         # the injection shape #154 describes
    "2024_orders",          # leading digit
    "orders raw",           # space
    "orders;DROP TABLE x",  # statement separator
    "`orders`",             # backtick
    "",                     # empty
])
def test_invalid_table_identifier_raises_at_config_load(bad):
    with pytest.raises(ValueError, match="table"):
        _cfg(table=bad)


@pytest.mark.parametrize("field_name", [
    "schema_name", "catalog", "audit_catalog", "registry_catalog",
    "audit_schema_name", "audit_table", "registry_schema_name",
    "registry_table", "audit_ingest_ts_col", "audit_source_file_col",
    "audit_batch_id_col", "rescued_data_column", "corrupt_record_column",
    "dedupe_order_by",
])
def test_every_identifier_field_is_validated(field_name):
    with pytest.raises(ValueError, match=field_name):
        _cfg(**{field_name: "bad-name"})


@pytest.mark.parametrize("field_name", [
    "required_columns", "unique_columns", "partition_by", "cluster_by",
])
def test_identifier_lists_are_validated_with_the_offending_index(field_name):
    with pytest.raises(ValueError, match=rf"{field_name}\[1\]"):
        _cfg(**{field_name: ["ok", "not ok"]})


def test_merge_keys_identifiers_are_validated():
    """Reported against required_columns rather than merge_keys, because
    merge_keys must be a subset of it - so a bad merge key is necessarily a
    bad required column, and that list is checked first. Either name points at
    the same one-character fix."""
    with pytest.raises(ValueError, match="bad-key"):
        _cfg(write_mode="merge", merge_keys=["bad-key"], required_columns=["bad-key"])


def test_quarantine_table_is_validated_per_part():
    _cfg(quarantine_table="main.bronze.orders_quarantine")  # qualified name is fine
    with pytest.raises(ValueError, match="quarantine_table part 2"):
        _cfg(quarantine_table="main.bad-schema.orders_quarantine")


def test_table_properties_keys_are_validated_per_dotted_part():
    _cfg(table_properties={"delta.enableChangeDataFeed": "true"})
    with pytest.raises(ValueError, match="table_properties key"):
        _cfg(table_properties={"delta.bad-key": "true"})


def test_table_property_values_are_not_identifier_checked():
    """Values are free text and escaped at the call site, not validated."""
    cfg = _cfg(table_properties={"delta.someProp": "it's fine; really"})
    assert cfg.table_properties["delta.someProp"] == "it's fine; really"


def test_nested_column_comment_keys_are_allowed():
    """catalog_metadata skips nested paths with a warning by design - catalog
    documentation must never fail an ingestion run - so config load must not
    reject them either. Per-part validation still blocks the unsafe shapes."""
    cfg = _cfg(column_comments={"customer.name": "the customer"})
    assert "customer.name" in cfg.column_comments

    with pytest.raises(ValueError, match="column_comments key"):
        _cfg(column_comments={"customer-name": "nope"})


def test_valid_identifiers_are_accepted():
    cfg = _cfg(
        catalog="main", schema_name="bronze_dev", table="orders_raw",
        required_columns=["order_id", "customer_id"], dedupe_order_by="updated_at",
    )
    assert cfg.full_table_name == "main.bronze_dev.orders_raw"


# ---- reader_options allowlist (#154) ----

def test_allowlisted_reader_options_are_accepted():
    cfg = _cfg(reader_options={"multiLine": "true", "dateFormat": "yyyy-MM-dd"})
    assert cfg.reader_options["multiLine"] == "true"


def test_cloudfiles_prefix_is_allowed_wholesale():
    """Auto Loader's surface is large, versioned and fully namespaced, and
    every key under it configures discovery for the path already given."""
    cfg = _cfg(reader_options={"cloudFiles.maxBytesPerTrigger": "10g"})
    assert cfg.reader_options


def test_path_option_is_refused():
    """The specific thing the allowlist exists for: `path` would redirect the
    read while every log line and audit row still reports source_path."""
    with pytest.raises(ValueError, match="allowlist"):
        _cfg(reader_options={"path": "/Volumes/somewhere/else"})


def test_unsafe_reader_options_can_be_opted_into():
    cfg = _cfg(
        reader_options={"path": "/Volumes/somewhere/else"},
        allow_unsafe_reader_options=True,
    )
    assert cfg.reader_options["path"] == "/Volumes/somewhere/else"


# ---- numeric ranges (#54) ----

@pytest.mark.parametrize("attempts", [0, -1])
def test_retry_attempts_below_one_raises(attempts):
    """with_retry loops range(1, attempts + 1). Below 1 the body never runs
    and it raises `last_exc`, still None - surfacing as "exceptions must
    derive from BaseException" and hiding the real failure entirely."""
    with pytest.raises(ValueError, match="retry_attempts"):
        _cfg(retry_attempts=attempts)


def test_retry_attempts_one_is_valid():
    """1 means try once, do not retry - the way to disable retries."""
    assert _cfg(retry_attempts=1).retry_attempts == 1


def test_negative_retry_delay_raises():
    with pytest.raises(ValueError, match="retry_delay_seconds"):
        _cfg(retry_delay_seconds=-5)


def test_zero_retry_delay_is_valid():
    assert _cfg(retry_delay_seconds=0).retry_delay_seconds == 0


@pytest.mark.parametrize("value", [0, -1])
def test_max_files_per_trigger_below_one_raises(value):
    with pytest.raises(ValueError, match="max_files_per_trigger"):
        _cfg(max_files_per_trigger=value)


def test_max_files_per_trigger_none_means_no_limit():
    assert _cfg(max_files_per_trigger=None).max_files_per_trigger is None


# ---- audit/registry schema default (#54) ----

def test_audit_and_registry_default_to_the_target_schema():
    """The trap this closes: these used to default to the literal "bronze",
    so every environment sharing a catalog wrote its run history to one
    table - and _write_audit_row's CREATE SCHEMA IF NOT EXISTS created that
    shared schema silently rather than failing."""
    cfg = _cfg(catalog="ingredion_en", schema_name="ingredion_prd")

    assert cfg.resolved_audit_schema == "ingredion_prd"
    assert cfg.resolved_registry_schema == "ingredion_prd"
    assert cfg.resolved_audit_table == "ingredion_en.ingredion_prd._ingestion_audit"
    assert cfg.resolved_registry_table == "ingredion_en.ingredion_prd._schema_registry"


def test_explicit_audit_schema_still_wins():
    cfg = _cfg(catalog="c", schema_name="s", audit_schema_name="governance")
    assert cfg.resolved_audit_table == "c.governance._ingestion_audit"


# ---- cross-field combinations (#54) ----

def test_streaming_with_overwrite_raises():
    """Every micro-batch would replace the whole table, so only the last one
    survives. There is no case where this is intended."""
    with pytest.raises(ValueError, match="micro-batch"):
        _cfg(
            ingestion_mode="streaming", write_mode="overwrite",
            checkpoint_location="/c", schema_location="/s",
        )


def test_merge_dedupe_without_audit_columns_or_order_by_raises():
    """_dedupe_for_merge defaults its order column to audit_ingest_ts_col,
    which only exists because add_audit_columns creates it."""
    with pytest.raises(ValueError, match="dedupe_order_by"):
        _cfg(
            write_mode="merge", merge_keys=["id"], required_columns=["id"],
            dedupe_before_merge=True, add_audit_columns=False,
        )


def test_merge_dedupe_without_audit_columns_is_fine_with_explicit_order_by():
    cfg = _cfg(
        write_mode="merge", merge_keys=["id"], required_columns=["id"],
        dedupe_before_merge=True, add_audit_columns=False,
        dedupe_order_by="updated_at",
    )
    assert cfg.dedupe_order_by == "updated_at"


def test_dedupe_before_merge_on_a_non_merge_write_warns(caplog):
    """Warns rather than raises: the setting is merely ignored, and raising
    would break working configs carrying a harmless leftover."""
    import logging
    with caplog.at_level(logging.WARNING, logger="bronze_ingest"):
        _cfg(write_mode="append", dedupe_before_merge=True)
    assert "dedupe_before_merge" in caplog.text


def test_registry_without_run_audit_warns(caplog):
    import logging
    with caplog.at_level(logging.WARNING, logger="bronze_ingest"):
        _cfg(enable_schema_registry=True, enable_run_audit=False)
    assert "drift" in caplog.text


def test_unique_columns_without_dedupe_order_by_is_not_an_error():
    """#54 asked whether this should raise, on the grounds that the tie-break
    was monotonically_increasing_id(). #147 replaced that with a content
    hash, so the fallback is now deterministic and this is a valid config."""
    cfg = _cfg(unique_columns=["order_id"])
    assert cfg.dedupe_order_by is None


def test_dedupe_before_merge_default_is_effectively_true():
    """The raw field is None so validation can tell an explicit choice from
    silence. None is FALSY, so anything reading it directly would silently
    disable deduplication - resolved_dedupe_before_merge is the accessor."""
    cfg = _cfg(write_mode="merge", merge_keys=["id"], required_columns=["id"])
    assert cfg.dedupe_before_merge is None
    assert cfg.resolved_dedupe_before_merge is True


def test_dedupe_before_merge_default_does_not_warn_on_append(caplog):
    """The counterpart to the warning test above: an ordinary append config
    must stay silent. Warning on the default would fire for every append
    pipeline, which is how a codebase teaches people to ignore its warnings."""
    import logging
    with caplog.at_level(logging.WARNING, logger="bronze_ingest"):
        _cfg(write_mode="append")
    assert "dedupe_before_merge" not in caplog.text


def test_dedupe_before_merge_explicitly_false_is_respected():
    cfg = _cfg(write_mode="merge", merge_keys=["id"], required_columns=["id"],
               dedupe_before_merge=False)
    assert cfg.resolved_dedupe_before_merge is False


# ---- IngestionConfig.resolve (#150) ----
#
# The three-branch merge that lived in ingest_json_to_bronze. Moved here
# because this class already owns from_dict / load / to_dict, and because it
# gives the unknown-key check one place to live.


def test_resolve_from_kwargs_only():
    cfg = IngestionConfig.resolve(source_path="x", table="t")
    assert cfg.table == "t"


def test_resolve_overrides_beat_the_dict():
    cfg = IngestionConfig.resolve(
        config={"source_path": "x", "table": "from_dict", "schema_name": "s"},
        table="from_kwargs",
    )
    assert cfg.table == "from_kwargs"
    assert cfg.schema_name == "s"


def test_resolve_overrides_beat_the_file(tmp_path):
    import json
    path = tmp_path / "c.json"
    path.write_text(json.dumps({"source_path": "x", "table": "from_file", "schema_name": "s"}))

    cfg = IngestionConfig.resolve(config_path=str(path), table="from_kwargs")
    assert cfg.table == "from_kwargs"
    assert cfg.schema_name == "s"


def test_resolve_rejects_both_config_and_config_path(tmp_path):
    """The old code silently used config_path and discarded config entirely,
    handing the caller a config they did not ask for with no signal."""
    import json
    path = tmp_path / "c.json"
    path.write_text(json.dumps({"source_path": "x", "table": "t"}))

    with pytest.raises(ValueError, match="not both"):
        IngestionConfig.resolve(config={"source_path": "x", "table": "t"}, config_path=str(path))


def test_resolve_rejects_a_typo_in_kwargs():
    """`tabel="orders"` used to be dropped, and the run then failed with
    "table is required" - an error pointing at the wrong thing."""
    with pytest.raises(ValueError, match="tabel"):
        IngestionConfig.resolve(source_path="x", table="t", tabel="orders")


def test_from_dict_stays_lenient_about_unknown_keys():
    """Deliberately NOT strict, unlike resolve's kwargs: config files are
    versioned artifacts that may carry keys a given package version does not
    know yet. The strictness belongs where a human just typed the key."""
    cfg = IngestionConfig.from_dict(
        {"source_path": "x", "table": "t", "some_future_field": 1}
    )
    assert cfg.table == "t"
