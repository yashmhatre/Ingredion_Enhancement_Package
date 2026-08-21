from pathlib import Path

import pytest

from bronze_ingest import formats
from bronze_ingest.config import IngestionConfig

#: Non-recursive on purpose: config/contracts/ holds data-contract specs, not
#: IngestionConfig YAML, and glob("**/*.yaml") would pull those in too and
#: fail them against a schema they were never meant to satisfy.
CONFIG_DIR = Path(__file__).resolve().parent.parent / "config"
CONFIG_YAML_FILES = sorted(CONFIG_DIR.glob("*.yaml")) + sorted(CONFIG_DIR.glob("*.yml"))


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
        source_path="s3://x",
        table="t",
        write_mode="merge",
        merge_keys=["id"],
        required_columns=["id"],
    )


def test_merge_requires_merge_keys_in_required_columns():
    # merge_keys not covered by required_columns - a NULL merge key would
    # never match in a MERGE condition and silently duplicate forever (#47)
    with pytest.raises(ValueError):
        IngestionConfig(source_path="s3://x", table="t", write_mode="merge", merge_keys=["id"])
    with pytest.raises(ValueError):
        IngestionConfig(
            source_path="s3://x",
            table="t",
            write_mode="merge",
            merge_keys=["id", "region"],
            required_columns=["id"],
        )
    # should not raise - all merge_keys covered
    IngestionConfig(
        source_path="s3://x",
        table="t",
        write_mode="merge",
        merge_keys=["id", "region"],
        required_columns=["id", "region", "other"],
    )


def test_merge_requires_either_merge_keys_or_content_hash_columns():
    """write_mode='merge' with neither strategy set is exactly the pre-#84
    "merge_keys must be provided" case, now phrased to mention both."""
    with pytest.raises(
        ValueError, match="merge_keys.*content_hash_columns|content_hash_columns.*merge_keys"
    ):
        IngestionConfig(source_path="s3://x", table="t", write_mode="merge")


def test_merge_keys_and_content_hash_columns_are_mutually_exclusive():
    with pytest.raises(ValueError, match="mutually exclusive"):
        IngestionConfig(
            source_path="s3://x",
            table="t",
            write_mode="merge",
            merge_keys=["id"],
            required_columns=["id"],
            content_hash_columns=["id", "amount"],
        )


def test_content_hash_columns_is_a_valid_merge_key_strategy():
    # Deliberately no merge_keys and no required_columns entry for the
    # hashed columns - the #47 nullable-key guard doesn't apply here, there
    # is no natural key to null-check.
    cfg = IngestionConfig(
        source_path="s3://x",
        table="t",
        write_mode="merge",
        content_hash_columns=["id", "amount"],
    )
    assert cfg.content_hash_columns == ["id", "amount"]
    assert cfg.merge_keys is None
    assert cfg.content_hash_key_col == "_content_hash_key"  # the default


def test_content_hash_columns_must_be_non_empty_when_provided():
    with pytest.raises(ValueError, match="non-empty"):
        IngestionConfig(
            source_path="s3://x", table="t", write_mode="merge", content_hash_columns=[]
        )


@pytest.mark.parametrize(
    "reserved_column",
    [
        "_ingested_at",  # audit_ingest_ts_col default
        "_batch_id",  # audit_batch_id_col default
        "_source_file",  # audit_source_file_col default
        "_rescued_data",  # rescued_data_column default
        "_corrupt_record",  # corrupt_record_column default
        "_content_hash_key",  # content_hash_key_col default - hashing itself
    ],
)
def test_content_hash_columns_rejects_columns_bronze_adds_itself(reserved_column):
    """Hashing an audit/rescued/corrupt column (or the hash key column's own
    name) would make the hash depend on ingest-time metadata rather than row
    content - identical source bytes ingested on two different runs would
    then never produce a matching hash (#84's design note)."""
    with pytest.raises(ValueError, match="content_hash_columns"):
        IngestionConfig(
            source_path="s3://x",
            table="t",
            write_mode="merge",
            content_hash_columns=["id", reserved_column],
        )


def test_content_hash_columns_identifiers_are_validated():
    with pytest.raises(ValueError, match="content_hash_columns"):
        IngestionConfig(
            source_path="s3://x",
            table="t",
            write_mode="merge",
            content_hash_columns=["bad-column"],
        )


def test_content_hash_key_col_identifier_is_validated():
    with pytest.raises(ValueError, match="content_hash_key_col"):
        IngestionConfig(
            source_path="s3://x",
            table="t",
            write_mode="merge",
            content_hash_columns=["id"],
            content_hash_key_col="bad-name",
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
    cfg = IngestionConfig(
        source_path="s3://x", table="orders", schema_name="bronze", catalog="main"
    )
    assert cfg.full_table_name == "main.bronze.orders"
    assert cfg.resolved_quarantine_table == "main.bronze.orders_quarantine"


def test_from_dict_ignores_unknown_keys():
    cfg = IngestionConfig.from_dict(
        {"source_path": "s3://x", "table": "t", "not_a_real_field": 123}
    )
    assert cfg.table == "t"


def test_cluster_by_and_partition_by_mutually_exclusive():
    with pytest.raises(ValueError):
        IngestionConfig(source_path="s3://x", table="t", partition_by=["date"], cluster_by=["id"])
    with pytest.raises(ValueError):
        IngestionConfig(
            source_path="s3://x", table="t", partition_by=["date"], cluster_by_auto=True
        )
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


@pytest.mark.parametrize(
    "bad",
    [
        "orders-2024",  # the realistic case: a hyphen
        "it's",  # an apostrophe - breaks the literal
        "x' OR '1'='1",  # the injection shape #154 describes
        "2024_orders",  # leading digit
        "orders raw",  # space
        "orders;DROP TABLE x",  # statement separator
        "`orders`",  # backtick
        "",  # empty
    ],
)
def test_invalid_table_identifier_raises_at_config_load(bad):
    with pytest.raises(ValueError, match="table"):
        _cfg(table=bad)


@pytest.mark.parametrize(
    "field_name",
    [
        "schema_name",
        "catalog",
        "audit_catalog",
        "registry_catalog",
        "audit_schema_name",
        "audit_table",
        "registry_schema_name",
        "registry_table",
        "audit_ingest_ts_col",
        "audit_source_file_col",
        "audit_batch_id_col",
        "rescued_data_column",
        "corrupt_record_column",
        "dedupe_order_by",
    ],
)
def test_every_identifier_field_is_validated(field_name):
    with pytest.raises(ValueError, match=field_name):
        _cfg(**{field_name: "bad-name"})


@pytest.mark.parametrize(
    "field_name",
    [
        "required_columns",
        "unique_columns",
        "partition_by",
        "cluster_by",
    ],
)
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
        catalog="main",
        schema_name="bronze_dev",
        table="orders_raw",
        required_columns=["order_id", "customer_id"],
        dedupe_order_by="updated_at",
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


def test_reader_options_rejection_names_the_configured_source_format():
    """The allowlist is per-format (#303) - the error should say which
    format's allowlist it checked against, not just "the allowlist"."""
    with pytest.raises(ValueError, match=r"source_format='json'"):
        _cfg(reader_options={"path": "/Volumes/somewhere/else"})


# ---- source_format (#303) ----


def test_source_format_defaults_to_json():
    cfg = _cfg()
    assert cfg.source_format == "json"


def test_source_format_accepts_every_approved_format():
    for fmt in formats.approved_formats():
        kwargs = {"xml_row_tag": "record"} if fmt == "xml" else {}
        cfg = _cfg(source_format=fmt, **kwargs)
        assert cfg.source_format == fmt


def test_source_format_refuses_a_registered_format_awaiting_signoff():
    """A registered format is not automatically a usable one. The refusal
    names the decision records so the operator can see what is pending
    rather than reading it as a defect."""
    for fmt in formats.supported_formats():
        if fmt in formats.approved_formats():
            continue
        with pytest.raises(ValueError) as excinfo:
            _cfg(source_format=fmt, xml_row_tag="record")
        assert "sign-off" in str(excinfo.value)


def test_the_signoff_gate_is_not_vacuous():
    """Guards the test above: it must not pass by finding zero gated
    formats. Delete this together with the last _PENDING_SIGNOFF entry."""
    assert set(formats.supported_formats()) - set(formats.approved_formats())


def test_source_format_rejects_an_unregistered_format():
    with pytest.raises(ValueError) as excinfo:
        _cfg(source_format="avro")

    message = str(excinfo.value)
    assert "avro" in message
    for fmt in formats.supported_formats():
        assert fmt in message


def test_xml_requires_a_non_blank_row_tag(xml_signed_off):
    with pytest.raises(ValueError, match="xml_row_tag is required"):
        _cfg(source_format="xml")
    with pytest.raises(ValueError, match="xml_row_tag must be non-empty"):
        _cfg(source_format="xml", xml_row_tag="   ")


def test_xml_row_tag_is_the_only_row_tag_source(xml_signed_off):
    with pytest.raises(ValueError, match="reader_options.*rowTag"):
        _cfg(
            source_format="xml",
            xml_row_tag="order",
            reader_options={"rowTag": "other"},
            allow_unsafe_reader_options=True,
        )


def test_xml_rejects_schema_hint_until_canonical_mapping_is_defined(xml_signed_off):
    with pytest.raises(ValueError, match="schema_hint_ddl is not supported"):
        _cfg(
            source_format="xml",
            xml_row_tag="order",
            schema_hint_ddl="a__id STRING",
        )


def test_xml_safety_options_cannot_be_overridden_even_when_unsafe_is_enabled(xml_signed_off):
    for key, value in (("mode", "PERMISSIVE"), ("ignoreNamespace", "true")):
        with pytest.raises(ValueError, match=key):
            _cfg(
                source_format="xml",
                xml_row_tag="order",
                reader_options={key: value},
                allow_unsafe_reader_options=True,
            )


def test_row_tag_option_remains_a_normal_unknown_option_for_non_xml_formats():
    cfg = _cfg(reader_options={"rowTag": "ignored"}, allow_unsafe_reader_options=True)
    assert cfg.reader_options == {"rowTag": "ignored"}


def test_non_xml_format_does_not_require_xml_row_tag():
    assert _cfg().xml_row_tag is None


@pytest.mark.parametrize("source_format", ["csv", "parquet", "xml"])
def test_non_json_streaming_is_deferred(source_format, xml_signed_off):
    kwargs = {"xml_row_tag": "record"} if source_format == "xml" else {}
    with pytest.raises(ValueError, match="streaming.*JSON"):
        _cfg(
            source_format=source_format,
            ingestion_mode="streaming",
            checkpoint_location="/tmp/checkpoint",
            schema_location="/tmp/schema",
            **kwargs,
        )


def test_there_is_at_least_one_shipped_config_to_check():
    """Guards the parametrised test below - it must not be able to pass by
    discovering zero config files."""
    assert CONFIG_YAML_FILES


@pytest.mark.parametrize("config_path", CONFIG_YAML_FILES, ids=lambda p: p.name)
def test_shipped_config_yaml_still_constructs_unchanged(config_path):
    """New fields must be additive (CONTRIBUTING.md) - every shipped config
    must still load, and must default (or, if it sets the field explicitly,
    resolve) to the format it was already implicitly using."""
    cfg = IngestionConfig.load(str(config_path))
    assert cfg.source_format == "json"


# ---- csv_header / csv_infer_schema (#308/#310) ----


def test_csv_header_defaults_to_true_and_csv_infer_schema_to_false():
    """csv_infer_schema is False by default since #371: inference read a
    zero-padded SAP key as a number and dropped the padding without an error,
    a warning or a quarantine row. CSV lands as strings; callers cast."""
    cfg = _cfg()
    assert cfg.csv_header is True
    assert cfg.csv_infer_schema is False


def test_csv_multiline_defaults_to_false():
    """Off by default (#375): a multiLine CSV file cannot be split across
    tasks, so turning it on costs parallelism on large extracts."""
    assert _cfg().csv_multiline is False


def test_csv_multiline_is_independent_of_the_json_multiline_field():
    cfg = _cfg(multiline=True)
    assert cfg.csv_multiline is False


def test_csv_header_and_csv_infer_schema_settable_from_config_file(tmp_path):
    """Additive-config acceptance criterion: both fields load from a real
    config file, not just from constructor kwargs. source_format is left at
    the default ("json") deliberately - these fields must be settable
    regardless of format; only the headerless-csv combination is policed."""
    import json

    path = tmp_path / "c.json"
    path.write_text(
        json.dumps(
            {
                "source_path": "x",
                "table": "t",
                "csv_header": False,
                "csv_infer_schema": False,
            }
        )
    )

    cfg = IngestionConfig.load(str(path))
    assert cfg.csv_header is False
    assert cfg.csv_infer_schema is False


def test_headerless_csv_without_schema_hint_raises():
    with pytest.raises(ValueError) as excinfo:
        _cfg(source_format="csv", csv_header=False)

    message = str(excinfo.value)
    assert "_c0" in message
    assert "schema_hint_ddl" in message


def test_headerless_csv_with_schema_hint_does_not_raise():
    cfg = _cfg(source_format="csv", csv_header=False, schema_hint_ddl="a INT, b STRING")
    assert cfg.csv_header is False
    assert cfg.schema_hint_ddl == "a INT, b STRING"


def test_csv_header_true_needs_no_schema_hint():
    cfg = _cfg(source_format="csv")
    assert cfg.csv_header is True
    assert cfg.schema_hint_ddl is None


def test_non_csv_format_ignores_csv_header_false_with_no_schema_hint():
    """csv_header is CSV-only - a headerless-csv-shaped combination on a
    different format must not trip the check."""
    cfg = _cfg(source_format="json", csv_header=False)
    assert cfg.csv_header is False


def test_bad_source_format_reports_source_format_error_not_csv_header():
    """Ordering matters: an unregistered source_format must fail at the
    source_format check, not at the csv_header check that follows it in
    __post_init__ - a caller who typo'd source_format should not be told
    about _c0 positional columns, which has nothing to do with their mistake.

    Uses a name no issue will ever register so this remains ordering coverage
    as real formats are added."""
    with pytest.raises(ValueError) as excinfo:
        _cfg(source_format="not_a_format", csv_header=False)

    message = str(excinfo.value)
    assert "source_format must be one of" in message
    assert "_c0" not in message
    assert "schema_hint_ddl" not in message


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
            ingestion_mode="streaming",
            write_mode="overwrite",
            checkpoint_location="/c",
            schema_location="/s",
        )


def test_merge_dedupe_without_audit_columns_or_order_by_raises():
    """_dedupe_for_merge defaults its order column to audit_ingest_ts_col,
    which only exists because add_audit_columns creates it."""
    with pytest.raises(ValueError, match="dedupe_order_by"):
        _cfg(
            write_mode="merge",
            merge_keys=["id"],
            required_columns=["id"],
            dedupe_before_merge=True,
            add_audit_columns=False,
        )


def test_merge_dedupe_without_audit_columns_is_fine_with_explicit_order_by():
    cfg = _cfg(
        write_mode="merge",
        merge_keys=["id"],
        required_columns=["id"],
        dedupe_before_merge=True,
        add_audit_columns=False,
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
    cfg = _cfg(
        write_mode="merge", merge_keys=["id"], required_columns=["id"], dedupe_before_merge=False
    )
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
    cfg = IngestionConfig.from_dict({"source_path": "x", "table": "t", "some_future_field": 1})
    assert cfg.table == "t"


# ---------------------------------------------------------------------------
# Change Data Feed (#58)
# ---------------------------------------------------------------------------


def test_cdf_is_on_by_default_with_a_retention_floor():
    """The default is the point of #58: CDF only captures changes from the
    moment it is enabled, so anything ingested before someone opts in is
    history no Silver consumer can read incrementally. Retention ships with
    it because a feed whose files are vacuumed away is a guarantee that looks
    real and is not."""
    props = _cfg().resolved_table_properties

    assert props["delta.enableChangeDataFeed"] == "true"
    assert props["delta.logRetentionDuration"] == "interval 30 days"
    # Both keys, from one number. Delta bounds readable change data by the
    # SHORTER of the two, and deletedFileRetentionDuration defaults to 7 days
    # - so setting only logRetentionDuration would advertise 30 and deliver 7.
    assert props["delta.deletedFileRetentionDuration"] == "interval 30 days"


def test_retention_days_drives_both_retention_keys():
    props = _cfg(change_data_feed_retention_days=90).resolved_table_properties

    assert props["delta.logRetentionDuration"] == "interval 90 days"
    assert props["delta.deletedFileRetentionDuration"] == "interval 90 days"


def test_disabling_cdf_removes_all_three_keys_but_keeps_user_properties():
    props = _cfg(
        enable_change_data_feed=False,
        table_properties={"delta.appendOnly": "true"},
    ).resolved_table_properties

    assert "delta.enableChangeDataFeed" not in props
    assert "delta.logRetentionDuration" not in props
    assert "delta.deletedFileRetentionDuration" not in props
    assert props == {"delta.appendOnly": "true"}


def test_explicit_table_properties_win_over_the_cdf_defaults():
    """The precedence #58 asked to have decided rather than left to whichever
    code path runs last. Someone writing the raw property has said something
    more specific than the boolean default."""
    props = _cfg(
        table_properties={
            "delta.enableChangeDataFeed": "false",
            "delta.deletedFileRetentionDuration": "interval 7 days",
        }
    ).resolved_table_properties

    assert props["delta.enableChangeDataFeed"] == "false"
    assert props["delta.deletedFileRetentionDuration"] == "interval 7 days"
    # Untouched keys still come from the defaults.
    assert props["delta.logRetentionDuration"] == "interval 30 days"


def test_resolved_table_properties_does_not_alias_the_config_dict():
    """Callers diff and mutate this; handing out the stored dict would let a
    caller edit config by accident."""
    cfg = _cfg(table_properties={"delta.appendOnly": "true"})

    first = cfg.resolved_table_properties
    first["delta.enableChangeDataFeed"] = "tampered"
    first.pop("delta.appendOnly")

    assert cfg.table_properties == {"delta.appendOnly": "true"}
    assert cfg.resolved_table_properties["delta.enableChangeDataFeed"] == "true"


@pytest.mark.parametrize("bad_days", [0, -1])
def test_zero_or_negative_retention_raises_when_cdf_is_on(bad_days):
    """ "interval 0 days" is a feed that is switched on and immediately
    discards its own history - enabled and empty is the precise
    looks-real-and-is-not failure this issue exists to prevent."""
    with pytest.raises(ValueError, match="change_data_feed_retention_days"):
        _cfg(change_data_feed_retention_days=bad_days)


def test_retention_value_is_not_policed_when_cdf_is_off():
    """No feed, no window to get wrong - refusing here would be validation
    theatre over a field with no effect."""
    cfg = _cfg(enable_change_data_feed=False, change_data_feed_retention_days=0)

    assert cfg.resolved_table_properties == {}


def test_overwrite_defaults_cdf_off_rather_than_failing():
    """bronze_silver_contract.md 2 puts overwrite-mode tables out of contract
    for incremental reads: CDF there emits the whole table as deletes then
    inserts every run. Silence resolves to off, so every existing overwrite
    config keeps loading - a plain default of True would have made the
    refusal below unreachable by breaking them all at load."""
    cfg = _cfg(write_mode="overwrite")

    assert cfg.enable_change_data_feed is None
    assert cfg.resolved_enable_change_data_feed is False
    assert cfg.resolved_table_properties == {}


def test_explicitly_asking_for_cdf_on_overwrite_is_refused():
    """The enforcement the contract asked for when #58 landed. Without it the
    restriction is violated by someone configuring a table reasonably and
    having no way to know."""
    with pytest.raises(ValueError, match="overwrite"):
        _cfg(write_mode="overwrite", enable_change_data_feed=True)


@pytest.mark.parametrize("mode", ["append", "merge"])
def test_silence_means_cdf_on_for_every_other_write_mode(mode):
    extra = {"merge_keys": ["id"], "required_columns": ["id"]} if mode == "merge" else {}
    cfg = _cfg(write_mode=mode, **extra)

    assert cfg.resolved_enable_change_data_feed is True
    assert cfg.resolved_table_properties["delta.enableChangeDataFeed"] == "true"


def test_raw_property_remains_the_escape_hatch_on_overwrite():
    """The flag is refused with overwrite; the raw property is not. Someone
    writing the Delta property by hand has read the contract and decided -
    that is the documented I-know-what-I-am-doing path, and keeping it open is
    what makes the flag-level refusal a guardrail rather than a wall."""
    cfg = _cfg(
        write_mode="overwrite",
        table_properties={"delta.enableChangeDataFeed": "true"},
    )

    assert cfg.resolved_table_properties["delta.enableChangeDataFeed"] == "true"


@pytest.mark.parametrize("bad", ["false", "true", 0, 1, "", None, "n"])
def test_csv_header_must_be_a_real_bool(bad):
    """
    `is False` would have made the headerless-CSV guard silently inoperative
    for every one of these. The dangerous row is the string "false": truthy
    in Python, false to Spark's CSV reader - so the guard would pass and the
    read would be headerless anyway.

    Not hypothetical. resources/bronze_ingest_jobs.yml ships booleans as
    quoted strings (`multiline: "true"`, `fail_on_quality_error: "false"`),
    and per_file_config_json reaches IngestionConfig with only its keys
    validated.
    """
    with pytest.raises(ValueError, match="csv_header must be a bool"):
        _cfg(csv_header=bad)


@pytest.mark.parametrize("bad", ["false", 0, None])
def test_csv_infer_schema_must_be_a_real_bool(bad):
    with pytest.raises(ValueError, match="csv_infer_schema must be a bool"):
        _cfg(csv_infer_schema=bad)


@pytest.mark.parametrize("bad", ["true", "false", 1, 0, None])
def test_csv_multiline_must_be_a_real_bool(bad):
    with pytest.raises(ValueError, match="csv_multiline must be a bool"):
        _cfg(csv_multiline=bad)


def test_quoted_yaml_bool_is_rejected_by_the_loader_too(tmp_path):
    """The realistic route: a config file, not a constructor call. YAML
    `csv_header: "false"` parses to the string, so it must fail at load
    rather than reaching Spark as a header-on read."""
    p = tmp_path / "c.yaml"
    p.write_text('source_path: "/tmp/x.json"\ntable: "t"\ncsv_header: "false"\n', encoding="utf-8")
    with pytest.raises(ValueError, match="csv_header must be a bool"):
        IngestionConfig.load(str(p))
