"""
Tests for the framework metadata writer (#324).

Most of this needs no SparkSession: vocabulary validation, row/schema coverage
and `unfilled_columns` are pure functions over dicts and `StructType`, and
`pyspark.sql.types` imports without a session. Only the append/replace
semantics need real Delta tables.
"""

import uuid

import pytest
from pyspark.sql.types import (
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from bronze_ingest.metadata_store import (
    VOCABULARIES,
    MetadataRowError,
    MetadataStore,
    VocabularyError,
    missing_fields,
    unexpected_fields,
    unfilled_columns,
    validate_rows,
    validate_vocabulary,
)

_SCHEMA = StructType(
    [
        StructField("table_name", StringType(), nullable=False),
        StructField("note", StringType(), nullable=True),
        StructField("generation", LongType(), nullable=True),
    ]
)


# ---------------------------------------------------------------------------
# Vocabularies - no Spark
# ---------------------------------------------------------------------------


def test_a_known_value_passes():
    assert validate_vocabulary("MASTER_ENTITY", "entity_type", "entity_type") == "MASTER_ENTITY"


def test_an_unknown_value_raises_naming_the_field_and_the_allowed_set():
    """Free text in a controlled column cannot be switched on by any consumer,
    and a reader has no way to know it was never valid."""
    with pytest.raises(VocabularyError) as exc:
        validate_vocabulary("FACT_TABLE_MAYBE", "entity_type", "entity_type")

    message = str(exc.value)
    assert "entity_type" in message
    assert "FACT_TABLE_MAYBE" in message
    assert "MASTER_ENTITY" in message, "the allowed set must be in the error, not just a rejection"


def test_none_is_allowed_and_is_not_the_same_as_unknown():
    """UNKNOWN is a classification someone made. None is the absence of one.
    Forcing a caller to write UNKNOWN when it means "not looked at yet" throws
    away the only distinction between them."""
    assert validate_vocabulary(None, "entity_type", "entity_type") is None
    assert "UNKNOWN" in VOCABULARIES["entity_type"]


def test_asking_for_a_vocabulary_that_does_not_exist_raises():
    with pytest.raises(VocabularyError, match="no vocabulary named"):
        validate_vocabulary("X", "not_a_vocabulary", "f")


def test_validate_rows_checks_every_row_and_returns_them_unchanged():
    rows = [{"entity_type": "TRANSACTION"}, {"entity_type": "REFERENCE"}]
    assert validate_rows(rows, {"entity_type": "entity_type"}) == rows


def test_validate_rows_raises_on_the_offending_row():
    rows = [{"entity_type": "TRANSACTION"}, {"entity_type": "nonsense"}]
    with pytest.raises(VocabularyError, match="nonsense"):
        validate_rows(rows, {"entity_type": "entity_type"})


def test_validate_rows_ignores_fields_the_row_does_not_carry():
    """A partial row is not an invalid one - detectors fill different fields at
    different phases."""
    assert validate_rows([{"other": 1}], {"entity_type": "entity_type"}) == [{"other": 1}]


@pytest.mark.parametrize("name", sorted(VOCABULARIES))
def test_every_vocabulary_is_non_empty_and_upper_case(name):
    """A vocabulary is a contract between phases; an empty or inconsistently
    cased one silently accepts nothing or everything."""
    values = VOCABULARIES[name]
    assert values, f"{name} has no values"
    assert all(v == v.upper() for v in values), f"{name} mixes case"


def test_column_kind_vocabulary_matches_what_profiling_actually_emits():
    """Pinned to the real producer rather than a remembered list - the mistake
    #276 recorded. If profiling learns a new kind, this fails instead of the
    kind arriving as unvalidated text."""
    from bronze_ingest.profiling import ARRAY, MAP, SCALAR, STRUCT

    assert VOCABULARIES["column_kind"] == frozenset({SCALAR, STRUCT, ARRAY, MAP})


# ---------------------------------------------------------------------------
# Row / schema coverage - no Spark
# ---------------------------------------------------------------------------


def test_missing_required_field_is_reported_by_name():
    assert missing_fields({"note": "x"}, _SCHEMA) == ["table_name"]


def test_a_required_field_present_but_none_still_counts_as_missing():
    """Present-but-None is the failure #276 and #64 actually had: the key was
    there, the value never arrived."""
    assert missing_fields({"table_name": None}, _SCHEMA) == ["table_name"]


def test_nullable_fields_are_not_required():
    assert missing_fields({"table_name": "t"}, _SCHEMA) == []


def test_a_field_with_no_column_is_reported():
    """Silently discarding it is how a caller comes to believe a value is
    recorded when it goes nowhere."""
    assert unexpected_fields({"table_name": "t", "typo": 1}, _SCHEMA) == ["typo"]


def test_unfilled_columns_finds_the_column_nothing_ever_fills():
    rows = [
        {"table_name": "a", "note": None, "generation": 1},
        {"table_name": "b", "note": None, "generation": 2},
    ]
    assert unfilled_columns(rows, _SCHEMA) == ["note"]


def test_unfilled_columns_ignores_a_column_filled_even_once():
    rows = [
        {"table_name": "a", "note": None, "generation": 1},
        {"table_name": "b", "note": "here", "generation": 2},
    ]
    assert unfilled_columns(rows, _SCHEMA) == []


def test_unfilled_columns_on_no_rows_reports_nothing():
    """With nothing written there is nothing to conclude. Reporting every
    column would be a false alarm on a table that simply has not run yet."""
    assert unfilled_columns([], _SCHEMA) == []


# ---------------------------------------------------------------------------
# Names - no Spark
# ---------------------------------------------------------------------------


def test_qualified_name_includes_the_catalog_when_there_is_one():
    store = MetadataStore(None, "meta", catalog="cat")
    assert store.qualified("t") == "cat.meta.t"
    assert store.schema_ref == "cat.meta"


def test_qualified_name_omits_an_absent_catalog():
    store = MetadataStore(None, "meta")
    assert store.qualified("t") == "meta.t"


# ---------------------------------------------------------------------------
# Write-time guards - no Spark, because they must fail BEFORE Spark is touched
# ---------------------------------------------------------------------------


def test_a_row_missing_a_required_field_raises_naming_it():
    """createDataFrame would also fail, but with a Py4J error naming neither
    the column nor the row. The whole point is that the message says which
    field nothing filled."""
    store = MetadataStore(None, "meta")
    with pytest.raises(MetadataRowError, match="table_name"):
        store.append("t", _SCHEMA, [{"note": "x"}])


def test_a_row_with_an_unknown_field_raises_naming_it():
    store = MetadataStore(None, "meta")
    with pytest.raises(MetadataRowError, match="typo"):
        store.append("t", _SCHEMA, [{"table_name": "a", "typo": 1}])


def test_replace_for_refuses_a_mixed_key():
    """Replacing on a mixed key would delete rows this call was never given
    replacements for."""
    store = MetadataStore(None, "meta")
    rows = [{"table_name": "a"}, {"table_name": "b"}]
    with pytest.raises(MetadataRowError, match="same non-null"):
        store.replace_for("t", _SCHEMA, rows)


def test_writing_no_rows_touches_nothing():
    """spark is None here, so anything that reached Spark would raise."""
    store = MetadataStore(None, "meta")
    store.append("t", _SCHEMA, [])
    store.replace_for("t", _SCHEMA, [])


# ---------------------------------------------------------------------------
# Append / replace semantics - Spark required
# ---------------------------------------------------------------------------


def _store(spark):
    return MetadataStore(spark, "default")


def _rows(spark, store, table, key):
    from pyspark.sql import functions as F

    return spark.read.table(store.qualified(table)).filter(F.col("table_name") == key).collect()


def test_append_keeps_every_generation(spark):
    store = _store(spark)
    table = f"ms_append_{uuid.uuid4().hex[:8]}"
    key = "tbl_a"

    store.append(table, _SCHEMA, [{"table_name": key, "note": "one", "generation": 1}])
    store.append(table, _SCHEMA, [{"table_name": key, "note": "two", "generation": 2}])

    assert sorted(r["generation"] for r in _rows(spark, store, table, key)) == [1, 2]


def test_replace_for_leaves_other_keys_alone(spark):
    """The property that matters: a widened predicate here silently deletes
    another table's metadata."""
    store = _store(spark)
    table = f"ms_replace_{uuid.uuid4().hex[:8]}"

    store.replace_for(table, _SCHEMA, [{"table_name": "keep", "note": "mine", "generation": 1}])
    store.replace_for(table, _SCHEMA, [{"table_name": "churn", "note": "first", "generation": 1}])
    store.replace_for(table, _SCHEMA, [{"table_name": "churn", "note": "second", "generation": 2}])

    kept = _rows(spark, store, table, "keep")
    churned = _rows(spark, store, table, "churn")

    assert [r["note"] for r in kept] == ["mine"], "another key's rows must survive"
    assert [r["note"] for r in churned] == ["second"], "the replaced key keeps one generation"


def test_a_key_containing_an_apostrophe_does_not_widen_the_predicate(spark):
    """#154, in the form that matters here. Raw interpolation would either
    break the statement or match rows it was never meant to."""
    store = _store(spark)
    table = f"ms_quote_{uuid.uuid4().hex[:8]}"
    awkward = "it's_a_table"

    store.replace_for(
        table, _SCHEMA, [{"table_name": "bystander", "note": "safe", "generation": 1}]
    )
    store.replace_for(table, _SCHEMA, [{"table_name": awkward, "note": "first", "generation": 1}])
    store.replace_for(table, _SCHEMA, [{"table_name": awkward, "note": "second", "generation": 2}])

    assert [r["note"] for r in _rows(spark, store, table, "bystander")] == ["safe"]
    assert [r["note"] for r in _rows(spark, store, table, awkward)] == ["second"]


def test_read_for_returns_none_before_the_table_exists(spark):
    store = _store(spark)
    assert store.read_for(f"ms_absent_{uuid.uuid4().hex[:8]}", "anything") is None


def test_latest_for_returns_the_most_recent_row(spark):
    from datetime import datetime, timedelta, timezone

    schema = StructType(
        [
            StructField("table_name", StringType(), nullable=False),
            StructField("recorded_at", TimestampType(), nullable=False),
            StructField("note", StringType(), nullable=True),
        ]
    )
    store = _store(spark)
    table = f"ms_latest_{uuid.uuid4().hex[:8]}"
    now = datetime.now(timezone.utc)

    store.append(table, schema, [{"table_name": "t", "recorded_at": now, "note": "new"}])
    store.append(
        table,
        schema,
        [{"table_name": "t", "recorded_at": now - timedelta(hours=1), "note": "old"}],
    )

    assert store.latest_for(table, "t", order_by="recorded_at")["note"] == "new"


def test_profiling_writes_through_the_store():
    """The refactor's whole point: profiling owns no table-writing code, so
    both modules cannot drift apart on schema creation or replace semantics."""
    import inspect

    from bronze_ingest import profiling

    source = inspect.getsource(profiling)
    assert "MetadataStore" in source
    assert "saveAsTable" not in source, "profiling must not write tables itself any more"
