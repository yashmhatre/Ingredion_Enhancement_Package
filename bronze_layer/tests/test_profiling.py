"""
Tests for the Bronze data profiler (#299).

Split deliberately. The schema-walking, classification, ratio and AI-boundary
logic are pure functions over types and dicts, so they run with **no
SparkSession** - which matters in this repo, where the Spark suite cannot run
on the maintainer's machine at all. Only the tests that genuinely need data
take the `spark` fixture.

The cost claims are asserted rather than described: `test_profile_table_makes_
exactly_one_pass` counts `agg` and `count` calls, and the skip test asserts
zero passes. Those are the two properties #149's lesson is about, and a comment
promising them would be worth nothing.
"""

import uuid

import pytest
from pyspark.sql import functions as F
from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    DecimalType,
    LongType,
    MapType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from bronze_ingest.profiling import (
    ARRAY,
    COLUMN_METADATA_SCHEMA,
    COLUMN_PROFILE_SCHEMA,
    MAP,
    SCALAR,
    STRUCT,
    TABLE_METADATA_SCHEMA,
    ColumnNode,
    ProfileConfig,
    _ratio,
    ai_safe_profile_payload,
    describe_columns,
    profile_table,
    schema_fingerprint,
)


def _cfg(**overrides):
    base = {"metadata_schema": "default", "metadata_catalog": None}
    base.update(overrides)
    return ProfileConfig(**base)


# ---------------------------------------------------------------------------
# Schema walking - no Spark session needed
# ---------------------------------------------------------------------------


def test_top_level_scalars_are_found():
    schema = StructType([StructField("id", LongType()), StructField("name", StringType())])
    nodes = describe_columns(schema)
    assert [(n.path, n.kind) for n in nodes] == [("id", SCALAR), ("name", SCALAR)]


def test_structs_are_descended_and_leaves_get_dotted_paths():
    schema = StructType(
        [
            StructField("order_id", StringType()),
            StructField(
                "payment",
                StructType(
                    [StructField("method", StringType()), StructField("status", StringType())]
                ),
            ),
        ]
    )
    nodes = {n.path: n for n in describe_columns(schema)}

    assert nodes["payment"].kind == STRUCT
    assert nodes["payment.method"].kind == SCALAR
    assert nodes["payment.method"].parent == "payment"
    assert nodes["payment.method"].depth == 1


def test_arrays_are_recorded_but_not_descended():
    """Reaching inside an array needs an explode - a different query with a
    different row count. Phase 3's job, per the module docstring."""
    schema = StructType(
        [
            StructField("id", LongType()),
            StructField("items", ArrayType(StructType([StructField("sku", StringType())]))),
        ]
    )
    paths = {n.path: n.kind for n in describe_columns(schema)}

    assert paths == {"id": SCALAR, "items": ARRAY}
    assert "items.sku" not in paths


def test_maps_are_recorded_as_map_and_not_descended():
    """#298 called MAP out specifically because nothing in this repo handled
    it - not the flattener, not the schema registry."""
    schema = StructType(
        [StructField("id", LongType()), StructField("attrs", MapType(StringType(), StringType()))]
    )
    paths = {n.path: n.kind for n in describe_columns(schema)}
    assert paths == {"id": SCALAR, "attrs": MAP}


def test_struct_descent_stops_at_max_depth():
    inner = StructType([StructField("deep", StringType())])
    middle = StructType([StructField("mid", inner)])
    schema = StructType([StructField("top", middle)])

    shallow = {n.path for n in describe_columns(schema, max_depth=1)}
    assert "top.mid" in shallow
    assert "top.mid.deep" not in shallow

    deeper = {n.path for n in describe_columns(schema, max_depth=5)}
    assert "top.mid.deep" in deeper


# ---------------------------------------------------------------------------
# Type classification - the string-prefix trap
# ---------------------------------------------------------------------------


def test_types_are_classified_from_the_datatype_not_the_type_string():
    """The first draft classified numerics with
    `data_type.startswith(("int", ...))`. "int" is also the start of
    "interval", so a string match silently mistypes columns - and a column
    wrongly judged numeric gets an avg() that fails the entire pass, not just
    that column."""
    schema = StructType(
        [
            StructField("n", LongType()),
            StructField("d", DecimalType(10, 2)),
            StructField("s", StringType()),
            StructField("b", BooleanType()),
            StructField("t", TimestampType()),
            StructField("arr", ArrayType(StringType())),
        ]
    )
    nodes = {n.path: n for n in describe_columns(schema)}

    assert nodes["n"].is_numeric and nodes["n"].is_orderable
    assert nodes["d"].is_numeric, "DecimalType is numeric"
    assert nodes["s"].is_string and not nodes["s"].is_numeric
    assert nodes["b"].is_orderable and not nodes["b"].is_numeric
    assert nodes["t"].is_orderable and not nodes["t"].is_numeric
    assert not nodes["arr"].is_orderable, "an array must never reach min/max"


def test_accessor_quotes_each_path_part_separately():
    """`customer.name` must reach the struct field, while a top-level column
    literally named `customer.name` must not be mistaken for one."""
    leaf = ColumnNode(
        path="customer.name",
        parts=("customer", "name"),
        data_type="string",
        kind=SCALAR,
        nullable=True,
        ordinal=1,
        parent="customer",
        depth=1,
    )
    assert leaf.accessor == "`customer`.`name`"

    literal_dot = ColumnNode(
        path="customer.name",
        parts=("customer.name",),
        data_type="string",
        kind=SCALAR,
        nullable=True,
        ordinal=1,
        parent=None,
        depth=0,
    )
    assert literal_dot.accessor == "`customer.name`"


# ---------------------------------------------------------------------------
# Fingerprint agreement - pinned to the real producer
# ---------------------------------------------------------------------------


def test_profiling_fingerprint_matches_the_registry(spark):
    """Both modules decide "did the schema change?". If they disagree, the
    profiler skips a table the registry thinks drifted, or vice versa.

    Pinned against the registry's real function rather than a copy of its rule
    - the mistake #276 recorded, where a test written against an imagined
    format confirmed a bug instead of catching it.
    """
    from bronze_ingest.schema_registry import _fingerprint

    df = spark.createDataFrame([(1, "a")], "id bigint, name string")
    assert schema_fingerprint(df.schema) == _fingerprint(df)


def test_fingerprint_ignores_column_order():
    a = StructType([StructField("x", StringType()), StructField("y", LongType())])
    b = StructType([StructField("y", LongType()), StructField("x", StringType())])
    assert schema_fingerprint(a) == schema_fingerprint(b)


def test_fingerprint_changes_when_a_type_changes():
    a = StructType([StructField("x", StringType())])
    b = StructType([StructField("x", LongType())])
    assert schema_fingerprint(a) != schema_fingerprint(b)


# ---------------------------------------------------------------------------
# Ratios on an empty table
# ---------------------------------------------------------------------------


def test_ratio_is_none_not_zero_when_there_are_no_rows():
    """ "0% null" and "there were no rows to be null" are different facts. A key
    detector reading 0.0 would score an empty column as a perfect primary
    key."""
    assert _ratio(0, 0) is None
    assert _ratio(5, 0) is None
    assert _ratio(1, 4) == 0.25


# ---------------------------------------------------------------------------
# The AI boundary (BR-002)
# ---------------------------------------------------------------------------


def _profile_row(**overrides):
    row = {
        "column_name": "email",
        "null_pct": 0.0,
        "distinct_approx": 100,
        "min_value": "aaa@example.com",
        "max_value": "zzz@example.com",
        "sample_values_json": '["real@person.com"]',
    }
    row.update(overrides)
    return row


def test_sample_values_never_survive_into_an_ai_payload():
    """BR-002 forbids "any row, any sample of rows, or any single
    business-column value" in a metadata payload."""
    safe = ai_safe_profile_payload([_profile_row()])
    assert "sample_values_json" not in safe[0]


def test_min_max_are_suppressed_for_pii_flagged_columns():
    """A min or max IS a real value from the data, so on a column flagged in
    _ai_metadata.pii_flags_json it leaks exactly what BR-002 forbids."""
    safe = ai_safe_profile_payload([_profile_row()], pii_columns=["email"])
    assert "min_value" not in safe[0]
    assert "max_value" not in safe[0]


def test_facts_about_the_column_survive():
    """Null rate and cardinality are facts ABOUT the column, not values FROM
    it - suppressing them would make the payload useless."""
    safe = ai_safe_profile_payload([_profile_row()], pii_columns=["email"])
    assert safe[0]["null_pct"] == 0.0
    assert safe[0]["distinct_approx"] == 100


def test_min_max_survive_for_columns_not_flagged_as_pii():
    safe = ai_safe_profile_payload([_profile_row(column_name="quantity")], pii_columns=["email"])
    assert safe[0]["min_value"] == "aaa@example.com"


def test_ai_safe_payload_does_not_mutate_its_input():
    """The caller still needs the full row to write to the profile table."""
    original = _profile_row()
    ai_safe_profile_payload([original], pii_columns=["email"])
    assert original["sample_values_json"] == '["real@person.com"]'
    assert original["min_value"] == "aaa@example.com"


# ---------------------------------------------------------------------------
# Metadata table schemas
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "schema",
    [TABLE_METADATA_SCHEMA, COLUMN_METADATA_SCHEMA, COLUMN_PROFILE_SCHEMA],
    ids=["table_metadata", "column_metadata", "column_profile"],
)
def test_every_metadata_table_identifies_its_run(schema):
    """Each row must be traceable to the run that produced it, or two profiles
    of the same table are indistinguishable."""
    names = {f.name for f in schema.fields}
    assert "profile_run_id" in names
    assert "table_name" in names


def test_profile_schema_records_whether_the_numbers_came_from_a_sample():
    """A statistic from a 1% sample and one from the whole table are not the
    same claim, and a consumer that cannot tell them apart will treat them as
    if they were."""
    names = {f.name for f in COLUMN_PROFILE_SCHEMA.fields}
    assert {"sampled", "sample_size"} <= names


# ---------------------------------------------------------------------------
# Cost - the properties #149's lesson is about. Spark required.
# ---------------------------------------------------------------------------


class _PassCounter:
    """Counts the DataFrame actions profile_table performs."""

    def __init__(self, monkeypatch):
        from pyspark.sql import DataFrame

        self.aggs = 0
        self.counts = 0
        real_agg, real_count = DataFrame.agg, DataFrame.count

        def agg(df_self, *a, **k):
            self.aggs += 1
            return real_agg(df_self, *a, **k)

        def count(df_self):
            self.counts += 1
            return real_count(df_self)

        monkeypatch.setattr(DataFrame, "agg", agg)
        monkeypatch.setattr(DataFrame, "count", count)


def _table(spark, rows, ddl):
    name = f"profiling_{uuid.uuid4().hex[:8]}"
    spark.createDataFrame(rows, ddl).write.format("delta").saveAsTable(f"default.{name}")
    return f"default.{name}"


def test_profile_table_makes_exactly_one_pass(spark, monkeypatch):
    """The whole cost argument in one assertion. #149 removed a count() from
    the ingestion path because it re-read the source for a number already
    available; profiling would reintroduce that per column."""
    table = _table(spark, [(1, "a", 10.0), (2, "b", 20.0)], "id bigint, name string, amt double")
    counter = _PassCounter(monkeypatch)

    profile_table(spark, table, _cfg())

    assert counter.aggs == 1, "three columns must cost one aggregate, not three"
    assert counter.counts == 0, "row count must ride along in the aggregate, not scan again"


def test_reprofiling_an_unchanged_table_reads_no_data(spark, monkeypatch):
    """The skip test is metadata-only: schema fingerprint plus Delta version.
    If it ever starts scanning, profiling every table nightly gets expensive
    silently."""
    table = _table(spark, [(1, "a")], "id bigint, name string")
    profile_table(spark, table, _cfg())

    counter = _PassCounter(monkeypatch)
    result = profile_table(spark, table, _cfg())

    assert result["status"] == "skipped"
    assert counter.aggs == 0
    assert counter.counts == 0


def test_force_reprofiles_even_when_unchanged(spark):
    table = _table(spark, [(1, "a")], "id bigint, name string")
    profile_table(spark, table, _cfg())
    assert profile_table(spark, table, _cfg(force=True))["status"] == "profiled"


# ---------------------------------------------------------------------------
# Statistics, persisted rather than reported
# ---------------------------------------------------------------------------


def test_profile_writes_one_row_per_profiled_column(spark):
    table = _table(spark, [(1, "a", 10.0)], "id bigint, name string, amt double")
    config = _cfg()

    profile_table(spark, table, config)

    rows = (
        spark.read.table(config.resolved_column_profile)
        .filter(F.col("table_name") == table)
        .collect()
    )
    assert sorted(r["column_name"] for r in rows) == ["amt", "id", "name"]


def test_null_rates_and_cardinality_are_persisted(spark):
    table = _table(spark, [(1, "a"), (2, None), (3, "a")], "id bigint, name string")
    config = _cfg()
    profile_table(spark, table, config)

    rows = {
        r["column_name"]: r
        for r in spark.read.table(config.resolved_column_profile)
        .filter(F.col("table_name") == table)
        .collect()
    }

    assert rows["name"]["null_count"] == 1
    assert rows["name"]["null_pct"] == pytest.approx(1 / 3)
    assert rows["id"]["null_count"] == 0
    assert rows["id"]["distinct_approx"] == 3
    # 3 non-null ids, 3 distinct -> nothing repeated.
    assert rows["id"]["duplicate_count"] == 0
    # 2 non-null names, 1 distinct -> one repeat.
    assert rows["name"]["duplicate_count"] == 1


def test_an_empty_table_profiles_without_dividing_by_zero(spark):
    table = _table(spark, [], "id bigint, name string")
    config = _cfg()

    result = profile_table(spark, table, config)

    assert result["status"] == "profiled"
    assert result["row_count"] == 0
    rows = (
        spark.read.table(config.resolved_column_profile)
        .filter(F.col("table_name") == table)
        .collect()
    )
    assert all(r["null_pct"] is None for r in rows), "no rows means no rate, not a zero rate"


def test_struct_leaves_are_profiled_and_arrays_are_not(spark):
    df = spark.createDataFrame(
        [(1, ("m", "ok"), ["x", "y"])],
        StructType(
            [
                StructField("id", LongType()),
                StructField(
                    "payment",
                    StructType(
                        [StructField("method", StringType()), StructField("status", StringType())]
                    ),
                ),
                StructField("tags", ArrayType(StringType())),
            ]
        ),
    )
    name = f"profiling_{uuid.uuid4().hex[:8]}"
    table = f"default.{name}"
    df.write.format("delta").saveAsTable(table)
    config = _cfg()

    profile_table(spark, table, config)

    profiled = {
        r["column_name"]
        for r in spark.read.table(config.resolved_column_profile)
        .filter(F.col("table_name") == table)
        .collect()
    }
    assert "payment.method" in profiled
    assert "tags" not in profiled, "array interiors are phase 3, not phase 1"

    recorded = {
        r["column_name"]: r["column_kind"]
        for r in spark.read.table(config.resolved_column_metadata)
        .filter(F.col("table_name") == table)
        .collect()
    }
    assert recorded["tags"] == ARRAY, "the array is still recorded, just not profiled"


def test_wide_tables_are_capped_and_say_so(spark):
    ddl = ", ".join(f"c{i} bigint" for i in range(6))
    table = _table(spark, [tuple(range(6))], ddl)
    config = _cfg(max_columns=2)

    result = profile_table(spark, table, config)

    assert result["profile_truncated"] is True
    assert result["columns_profiled"] == 2
    assert result["column_count"] == 6
    row = (
        spark.read.table(config.resolved_table_metadata)
        .filter(F.col("table_name") == table)
        .collect()[0]
    )
    assert row["profile_truncated"] is True, "capping must be discoverable, not just logged"


def test_required_profile_fields_are_never_null(spark):
    """The fillability guard, in the shape #296 taught: a column that no code
    path fills is indistinguishable from one that is legitimately empty."""
    table = _table(spark, [(1, "a")], "id bigint, name string")
    config = _cfg()
    profile_table(spark, table, config)

    row = (
        spark.read.table(config.resolved_column_profile)
        .filter(F.col("table_name") == table)
        .collect()[0]
    )
    for field_name in (
        "table_name",
        "column_name",
        "data_type",
        "row_count",
        "null_count",
        "sampled",
        "schema_fingerprint",
        "profile_run_id",
        "profiled_at",
    ):
        assert row[field_name] is not None, f"{field_name} was never filled"


def test_sample_values_are_off_by_default(spark):
    table = _table(spark, [(1, "secret")], "id bigint, name string")
    config = _cfg()
    profile_table(spark, table, config)

    rows = (
        spark.read.table(config.resolved_column_profile)
        .filter(F.col("table_name") == table)
        .collect()
    )
    assert all(r["sample_values_json"] is None for r in rows)


def test_exact_count_distinct_is_never_used():
    """#299 states it as an acceptance criterion, so it gets a probe rather
    than a promise: `countDistinct` forces a full shuffle per column, which is
    the cost this module exists to avoid. Reads the source because the point is
    that the expensive call is ABSENT - there is no behaviour to observe.
    """
    import inspect

    from bronze_ingest import profiling

    source = inspect.getsource(profiling)
    assert "approx_count_distinct(" in source
    # A CALL, not a mention: the module docstring names countDistinct to
    # explain why it is avoided, and a bare substring check flags that prose.
    assert "countDistinct(" not in source


def test_table_metadata_holds_one_row_per_table_across_runs(spark):
    """Structure is refreshed in place, not appended. Two profiles of one table
    must not leave two rows claiming to describe it."""
    table = _table(spark, [(1, "a")], "id bigint, name string")
    config = _cfg()

    profile_table(spark, table, config)
    profile_table(spark, table, _cfg(force=True))

    rows = (
        spark.read.table(config.resolved_table_metadata)
        .filter(F.col("table_name") == table)
        .collect()
    )
    assert len(rows) == 1


def test_column_profile_keeps_a_row_per_run(spark):
    """The opposite grain, deliberately: how a null rate MOVES is evidence, so
    the profile table is a time series and must not be overwritten."""
    table = _table(spark, [(1, "a")], "id bigint, name string")
    config = _cfg()

    first = profile_table(spark, table, config)
    second = profile_table(spark, table, _cfg(force=True))

    runs = {
        r["profile_run_id"]
        for r in spark.read.table(config.resolved_column_profile)
        .filter(F.col("table_name") == table)
        .collect()
    }
    assert runs == {first["profile_run_id"], second["profile_run_id"]}
