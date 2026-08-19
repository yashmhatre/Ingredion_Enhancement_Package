"""
Tests for data contracts (#264).

Note what these do NOT need: a SparkSession. `validate_schema` takes a
StructType rather than a DataFrame precisely so the contract check costs
nothing and can be exercised anywhere - which matters in this repo, where the
Spark-backed suite cannot run on the maintainer's machine at all.
"""

import json
import os

import pytest
from pyspark.sql.types import LongType, StringType, StructField, StructType

from bronze_ingest.contract import (
    MISSING_COLUMN,
    TYPE_MISMATCH,
    UNDECLARED_COLUMN,
    ContractError,
    DataContract,
    format_violations,
    validate_schema,
)

CONTRACTS_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "config", "contracts")


def _contract(**overrides):
    raw = {
        "source": "orders",
        "columns": [
            {"name": "order_id", "type": "string", "nullable": False},
            {"name": "amount", "type": "bigint"},
        ],
    }
    raw.update(overrides)
    return DataContract.from_dict(raw)


def _schema(*fields):
    return StructType([StructField(n, t) for n, t in fields])


# ---- the happy path ----


def test_a_matching_schema_produces_no_violations():
    violations = validate_schema(
        _schema(("order_id", StringType()), ("amount", LongType())), _contract()
    )
    assert violations == []


def test_undeclared_column_is_allowed_by_default():
    """Bronze is additive: a new upstream column is normal, and is what #256's
    drift detection reports rather than something to reject."""
    schema = _schema(("order_id", StringType()), ("amount", LongType()), ("currency", StringType()))
    assert validate_schema(schema, _contract()) == []


# ---- violations ----


def test_missing_column_is_reported():
    violations = validate_schema(_schema(("order_id", StringType())), _contract())
    assert [(v.kind, v.column) for v in violations] == [(MISSING_COLUMN, "amount")]
    assert violations[0].expected == "bigint"


def test_type_mismatch_is_reported_with_both_types():
    violations = validate_schema(
        _schema(("order_id", StringType()), ("amount", StringType())), _contract()
    )
    assert violations[0].kind == TYPE_MISMATCH
    assert violations[0].expected == "bigint"
    assert violations[0].actual == "string"


def test_undeclared_column_is_reported_when_the_contract_forbids_it():
    schema = _schema(("order_id", StringType()), ("amount", LongType()), ("surprise", StringType()))
    violations = validate_schema(schema, _contract(allow_undeclared_columns=False))
    assert [(v.kind, v.column) for v in violations] == [(UNDECLARED_COLUMN, "surprise")]


def test_every_violation_is_reported_not_just_the_first():
    """One run should tell you everything that is wrong with the batch."""
    violations = validate_schema(_schema(("amount", StringType())), _contract())
    kinds = sorted((v.kind, v.column) for v in violations)
    assert kinds == [(MISSING_COLUMN, "order_id"), (TYPE_MISMATCH, "amount")]


# ---- required_columns: the single-source-of-truth seam (#250, #265) ----


def test_required_columns_derives_from_nullable_false():
    assert _contract().required_columns() == ["order_id"]


def test_required_columns_is_empty_when_nothing_is_declared_non_null():
    """An honest empty list. #250's lesson is that an empty required_columns
    must not be mistaken for a configured one - the caller decides what to do
    about it, this just reports what the contract says."""
    contract = _contract(
        columns=[{"name": "order_id", "type": "string"}, {"name": "amount", "type": "bigint"}]
    )
    assert contract.required_columns() == []


def test_nullability_is_never_a_schema_violation():
    """Spark's JSON inference marks every field nullable regardless of the
    data, so a schema-level nullability check would pass on anything and mean
    nothing - the exact shape of no-op #250 is about. Non-null is a per-row
    property and belongs to the quality gate."""
    schema = StructType(
        [
            StructField("order_id", StringType(), nullable=True),
            StructField("amount", LongType(), nullable=True),
        ]
    )
    assert validate_schema(schema, _contract()) == []


# ---- malformed contracts fail loudly ----


def test_contract_with_no_columns_is_rejected():
    """An empty contract validates everything and therefore checks nothing."""
    with pytest.raises(ContractError, match="checks nothing"):
        DataContract.from_dict({"source": "orders", "columns": []})


def test_contract_without_a_source_is_rejected():
    with pytest.raises(ContractError, match="name the source"):
        DataContract.from_dict({"columns": [{"name": "a", "type": "string"}]})


def test_duplicate_column_is_rejected():
    with pytest.raises(ContractError, match="twice"):
        DataContract.from_dict(
            {
                "source": "orders",
                "columns": [
                    {"name": "order_id", "type": "string"},
                    {"name": "order_id", "type": "bigint"},
                ],
            }
        )


def test_unknown_top_level_key_is_rejected():
    """Same reasoning as the per_file_config guard: a silently dropped key is
    a rule the author believes is in force and which never runs."""
    with pytest.raises(ContractError, match="unknown key"):
        DataContract.from_dict(
            {"source": "orders", "columns": [{"name": "a", "type": "string"}], "colums": []}
        )


def test_unknown_column_key_is_rejected():
    with pytest.raises(ContractError, match="unknown key"):
        DataContract.from_dict(
            {"source": "orders", "columns": [{"name": "a", "type": "string", "nulable": True}]}
        )


def test_column_without_a_type_is_rejected():
    with pytest.raises(ContractError, match="missing 'type'"):
        DataContract.from_dict({"source": "orders", "columns": [{"name": "a"}]})


# ---- loading, including the real shipped contract ----


def test_load_json_contract(tmp_path):
    path = tmp_path / "c.json"
    path.write_text(
        json.dumps({"source": "orders", "columns": [{"name": "order_id", "type": "string"}]}),
        encoding="utf-8",
    )
    assert DataContract.load(str(path)).source == "orders"


def test_the_shipped_orders_contract_parses_and_declares_its_merge_key_non_null():
    """Pins the real file rather than a fixture the test author imagined -
    the mistake #276 recorded, where unit tests confirmed a bug because they
    were written against a format nothing produced.

    order_id is non-null because config.py ALREADY enforces that merge_keys
    are a subset of required_columns; it is the one claim here that is not a
    guess.
    """
    contract = DataContract.load(os.path.join(CONTRACTS_DIR, "orders.yaml"))

    assert contract.source == "orders"
    assert contract.required_columns() == ["order_id"]
    assert contract.column("order_id").nullable is False
    assert contract.allow_undeclared_columns is True


def test_format_violations_lists_one_per_line():
    violations = validate_schema(_schema(("amount", StringType())), _contract())
    rendered = format_violations(violations)
    assert len(rendered.splitlines()) == 2
    assert "order_id" in rendered


def test_the_type_vocabulary_is_sparks_own():
    """Pins `type:` to what Spark actually emits, not to a remembered list.

    The first draft of this module declared `amount` as `long`, because that
    is what the Python type is called. `LongType().simpleString()` returns
    **`bigint`**, so every batch would have reported a type_mismatch on a
    column that was perfectly fine.

    Same failure mode as #276, where unit tests were written against a schema
    format nothing produced and so confirmed a bug instead of catching it.
    The fix there was to tie the test to the real producer; this does the
    same. If Spark ever changes a spelling, this fails rather than every
    contract silently going wrong.
    """
    from pyspark.sql.types import (
        BooleanType,
        DateType,
        DecimalType,
        DoubleType,
        IntegerType,
        TimestampType,
    )

    assert LongType().simpleString() == "bigint"
    assert StringType().simpleString() == "string"
    assert IntegerType().simpleString() == "int"
    assert DoubleType().simpleString() == "double"
    assert BooleanType().simpleString() == "boolean"
    assert TimestampType().simpleString() == "timestamp"
    assert DateType().simpleString() == "date"
    assert DecimalType(10, 2).simpleString() == "decimal(10,2)"


def test_the_shipped_contract_uses_type_names_spark_will_actually_produce():
    """A contract whose types are misspelled is a contract that rejects
    correct data on every run. Checks the real file, not a fixture."""
    contract = DataContract.load(os.path.join(CONTRACTS_DIR, "orders.yaml"))
    schema = _schema(
        ("order_id", StringType()), ("customer_id", StringType()), ("amount", LongType())
    )
    assert validate_schema(schema, contract) == []
