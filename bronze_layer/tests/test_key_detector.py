"""
Tests for candidate-key detection (#327).

Split the same way `test_profiling.py` is, and for the same reason: what to
test, how to score it and what to exclude are pure functions over dicts and
`StructType`, so they run with **no SparkSession**. The Spark suite cannot run
on the maintainer's machine and CI is the only gate, so the more of this that
is Spark-free the more of it can be checked before a push.

The two claims that carry the module are asserted, not described:

  * `test_an_overstated_profile_does_not_become_a_key` - shortlisting is not
    claiming. Approximate cardinality proposes; exact cardinality decides.
  * `test_a_single_column_primary_key_costs_exactly_one_pass` - the cost rule
    from #149, counted rather than promised.
"""

import json
import uuid

import pytest
from pyspark.sql import functions as F
from pyspark.sql.types import (
    ArrayType,
    LongType,
    StringType,
    StructField,
    StructType,
)

from bronze_ingest.key_detector import (
    DEFAULT_EXCLUDED_COLUMNS,
    KEY_CANDIDATE_SCHEMA,
    PRIMARY_KEY,
    UNIQUE,
    Candidate,
    KeyDetectionConfig,
    Verified,
    classify,
    composite_candidates,
    detect_keys,
    is_excluded,
    score,
    shortlist,
    verify,
)
from bronze_ingest.metadata_store import (
    VOCABULARIES,
    VocabularyError,
    unfilled_columns,
    validate_vocabulary,
)
from bronze_ingest.profiling import ProfileConfig, describe_columns, profile_table


def _cfg(**overrides):
    base = {
        "metadata_schema": "default",
        "metadata_catalog": None,
        "key_candidate_table": f"kc_{uuid.uuid4().hex[:8]}",
    }
    base.update(overrides)
    return KeyDetectionConfig(**base)


def _profile(column_name, row_count=100, distinct_approx=100, null_count=0):
    """The subset of a `_column_profile` row that key detection reads."""
    return {
        "column_name": column_name,
        "row_count": row_count,
        "distinct_approx": distinct_approx,
        "null_count": null_count,
    }


def _nodes(*fields):
    return describe_columns(StructType(list(fields)))


def _verified(width=1, row_count=100, distinct_count=100, null_count=0):
    nodes = _nodes(*[StructField(f"c{i}", LongType()) for i in range(width)])
    return Verified(
        candidate=Candidate(tuple(nodes)),
        row_count=row_count,
        distinct_count=distinct_count,
        null_count=null_count,
    )


# ---------------------------------------------------------------------------
# Exclusions - no Spark session needed
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("name", DEFAULT_EXCLUDED_COLUMNS)
def test_columns_bronze_writes_itself_are_never_candidates(name):
    """`_quarantine_id` is the sharpest case: `quality.py` derives it as a
    SHA-256 of the row's own content, so it is unique BY CONSTRUCTION. Without
    this it would be the highest-confidence primary key in every quarantine
    table while saying nothing about the business data."""
    assert is_excluded(name, DEFAULT_EXCLUDED_COLUMNS)


def test_a_struct_field_named_like_an_audit_column_is_excluded_too():
    """The reason `_batch_id` is not a business key is what it holds, not the
    depth it sits at."""
    assert is_excluded("payload._batch_id", DEFAULT_EXCLUDED_COLUMNS)


def test_an_ordinary_column_is_not_excluded():
    assert not is_excluded("order_id", DEFAULT_EXCLUDED_COLUMNS)
    assert not is_excluded("customer._id", DEFAULT_EXCLUDED_COLUMNS)


def test_the_exclusion_list_covers_every_column_bronze_adds():
    """Pinned against config.py's and quality.py's own defaults rather than a
    copy of them: if a new audit column is added there, this fails instead of
    the new column quietly becoming a key candidate."""
    from dataclasses import fields as dataclass_fields

    from bronze_ingest.config import IngestionConfig

    defaults = {f.name: f.default for f in dataclass_fields(IngestionConfig)}
    for field_name in (
        "audit_ingest_ts_col",
        "audit_batch_id_col",
        "audit_source_file_col",
        "rescued_data_column",
        "corrupt_record_column",
        "content_hash_key_col",
    ):
        name = defaults[field_name]
        assert name in DEFAULT_EXCLUDED_COLUMNS, f"{name} would become a key candidate"


def test_excluded_columns_are_dropped_with_a_stated_reason():
    nodes = _nodes(StructField("id", LongType()), StructField("_batch_id", StringType()))
    kept, dropped = shortlist([_profile("id"), _profile("_batch_id")], nodes, _cfg())

    assert [c.label for c in kept] == ["id"]
    assert any("_batch_id" in d and "excluded" in d for d in dropped)


# ---------------------------------------------------------------------------
# Shortlisting - no Spark session needed
# ---------------------------------------------------------------------------


def test_a_low_cardinality_column_is_not_shortlisted():
    nodes = _nodes(StructField("status", StringType()))
    kept, _ = shortlist([_profile("status", distinct_approx=3)], nodes, _cfg())
    assert kept == []


def test_an_approximate_count_slightly_under_the_row_count_still_shortlists():
    """HLL undercounts as readily as it overcounts, so an exact key can
    approximate to just under the row count. A strict `==` would miss real
    keys; verification is what decides, and this only controls what is looked
    at."""
    nodes = _nodes(StructField("id", LongType()))
    kept, _ = shortlist([_profile("id", row_count=100, distinct_approx=97)], nodes, _cfg())
    assert [c.label for c in kept] == ["id"]


def test_columns_without_nulls_are_verified_first():
    """The candidate budget is finite, and only a column with no nulls can be
    a primary key."""
    nodes = _nodes(StructField("nullable_id", LongType()), StructField("id", LongType()))
    kept, _ = shortlist(
        [_profile("nullable_id", null_count=4), _profile("id", null_count=0)],
        nodes,
        _cfg(),
    )
    assert [c.label for c in kept] == ["id", "nullable_id"]


def test_the_single_column_shortlist_is_capped_and_says_so():
    fields = [StructField(f"c{i}", LongType()) for i in range(10)]
    nodes = _nodes(*fields)
    kept, dropped = shortlist(
        [_profile(f"c{i}") for i in range(10)], nodes, _cfg(max_single_candidates=4)
    )

    assert len(kept) == 4
    assert any("max_single_candidates=4" in d for d in dropped)


def test_a_profile_row_for_a_column_no_longer_in_the_schema_is_dropped_loudly():
    """Stale evidence is how a confident wrong answer gets made."""
    nodes = _nodes(StructField("id", LongType()))
    kept, dropped = shortlist([_profile("id"), _profile("gone")], nodes, _cfg())

    assert [c.label for c in kept] == ["id"]
    assert any("gone" in d and "current schema" in d for d in dropped)


def test_structs_and_arrays_are_not_key_candidates():
    nodes = _nodes(
        StructField("id", LongType()),
        StructField("tags", ArrayType(StringType())),
        StructField("customer", StructType([StructField("name", StringType())])),
    )
    kept, _ = shortlist([_profile("id"), _profile("tags"), _profile("customer")], nodes, _cfg())
    assert [c.label for c in kept] == ["id"]


def test_a_struct_field_can_be_a_key_candidate():
    nodes = _nodes(StructField("customer", StructType([StructField("id", LongType())])))
    kept, _ = shortlist([_profile("customer.id")], nodes, _cfg())
    assert [c.label for c in kept] == ["customer.id"]


def test_a_column_with_no_profiled_rows_is_dropped():
    nodes = _nodes(StructField("id", LongType()))
    kept, dropped = shortlist([_profile("id", row_count=0, distinct_approx=0)], nodes, _cfg())

    assert kept == []
    assert any("no rows" in d for d in dropped)


def test_a_column_with_no_distinct_count_is_dropped():
    """A non-scalar or a truncated wide-table run leaves `distinct_approx`
    NULL. Absent evidence is not evidence of uniqueness."""
    nodes = _nodes(StructField("id", LongType()))
    kept, dropped = shortlist([_profile("id", distinct_approx=None)], nodes, _cfg())

    assert kept == []
    assert any("no distinct count" in d for d in dropped)


# ---------------------------------------------------------------------------
# Composite search - no Spark session needed
# ---------------------------------------------------------------------------


def test_composite_search_produces_pairs_and_triples_only():
    fields = [StructField(f"c{i}", LongType()) for i in range(4)]
    nodes = _nodes(*fields)
    rows = [_profile(f"c{i}", distinct_approx=10 + i) for i in range(4)]

    combos, bounded = composite_candidates(rows, nodes, _cfg())

    widths = {c.width for c in combos}
    assert widths == {2, 3}
    assert not bounded


def test_composite_width_is_configurable_down_to_pairs():
    fields = [StructField(f"c{i}", LongType()) for i in range(4)]
    combos, _ = composite_candidates(
        [_profile(f"c{i}", distinct_approx=10 + i) for i in range(4)],
        _nodes(*fields),
        _cfg(max_composite_width=2),
    )
    assert {c.width for c in combos} == {2}


def test_a_constant_column_never_joins_a_composite():
    """Adding a constant to a column set leaves its distinct count exactly
    where it was, so it cannot narrow anything."""
    nodes = _nodes(
        StructField("a", LongType()), StructField("b", LongType()), StructField("k", StringType())
    )
    combos, _ = composite_candidates(
        [
            _profile("a", distinct_approx=9),
            _profile("b", distinct_approx=8),
            _profile("k", distinct_approx=1),
        ],
        nodes,
        _cfg(),
    )
    assert all("k" not in c.paths for c in combos)


def test_a_capped_composite_search_reports_that_it_was_capped():
    """A bounded search reporting 'no composite key found' is a false negative
    dressed as a result (#298 risk 6)."""
    fields = [StructField(f"c{i}", LongType()) for i in range(8)]
    combos, bounded = composite_candidates(
        [_profile(f"c{i}", distinct_approx=10 + i) for i in range(8)],
        _nodes(*fields),
        _cfg(max_composite_candidates=5),
    )

    assert len(combos) == 5
    assert bounded is True


def test_composite_columns_are_ordered_by_cardinality():
    nodes = _nodes(StructField("low", LongType()), StructField("high", LongType()))
    combos, _ = composite_candidates(
        [_profile("low", distinct_approx=2), _profile("high", distinct_approx=99)],
        nodes,
        _cfg(),
    )
    assert combos[0].paths == ("high", "low")


# ---------------------------------------------------------------------------
# Classification and scoring - no Spark session needed
# ---------------------------------------------------------------------------


def test_a_unique_column_with_no_nulls_is_a_primary_key():
    assert classify(_verified(row_count=100, distinct_count=100, null_count=0)) == PRIMARY_KEY


def test_a_unique_but_nullable_column_is_unique_not_a_primary_key():
    """A nullable key cannot identify the rows where it is absent."""
    v = _verified(row_count=100, distinct_count=95, null_count=5)
    assert v.is_unique
    assert classify(v) == UNIQUE


def test_a_column_that_is_null_in_every_row_is_not_unique():
    """`count_distinct` ignores nulls, so 0 distinct over 0 non-null rows
    would otherwise compare equal and read as unique."""
    assert not _verified(row_count=100, distinct_count=0, null_count=100).is_unique


def test_duplicates_score_zero_and_say_why():
    confidence, blockers, reason = score(_verified(row_count=100, distinct_count=99), _cfg())
    assert confidence == 0.0
    assert "duplicate" in reason
    assert any("not unique" in b for b in blockers)


def test_uniqueness_over_one_row_is_vacuous_and_names_the_row_count():
    """Every column of a one-row table is unique, and every table in this repo
    currently holds one row."""
    confidence, blockers, reason = score(
        _verified(row_count=1, distinct_count=1), _cfg(min_rows_for_confidence=1)
    )

    assert confidence == 0.0
    assert "row_count=1" in " ".join(blockers)
    assert "1 row" in reason


def test_thin_data_caps_confidence_below_the_review_threshold():
    config = _cfg()
    confidence, blockers, _ = score(_verified(row_count=50, distinct_count=50), config)

    assert confidence <= config.thin_data_confidence_cap
    assert confidence < config.review_threshold
    assert any("min_rows_for_confidence" in b for b in blockers)


@pytest.mark.parametrize("row_count", [2, 10, 100, 999])
def test_no_candidate_clears_the_review_threshold_below_the_row_floor(row_count):
    """The hard floor, asserted as a property rather than at one point. If the
    cap and the threshold are ever set so that thin data can auto-approve, this
    fails."""
    config = _cfg(min_rows_for_confidence=1000)
    confidence, _, _ = score(_verified(row_count=row_count, distinct_count=row_count), config)
    assert confidence < config.review_threshold


def test_plenty_of_rows_clears_the_review_threshold():
    config = _cfg()
    confidence, blockers, _ = score(_verified(row_count=5000, distinct_count=5000), config)

    assert confidence >= config.review_threshold
    assert blockers == []


def test_nulls_lower_confidence_against_an_otherwise_identical_candidate():
    config = _cfg()
    clean, _, _ = score(_verified(row_count=5000, distinct_count=5000), config)
    nullable, blockers, _ = score(
        _verified(row_count=5000, distinct_count=4990, null_count=10), config
    )

    assert nullable < clean
    assert any("null component" in b for b in blockers)


def test_a_wider_key_is_weaker_evidence_than_a_narrower_one():
    """A three-column set that happens to be unique is a weaker claim than a
    single column that is."""
    config = _cfg()
    one, _, _ = score(_verified(width=1, row_count=5000, distinct_count=5000), config)
    two, _, _ = score(_verified(width=2, row_count=5000, distinct_count=5000), config)
    three, _, _ = score(_verified(width=3, row_count=5000, distinct_count=5000), config)

    assert one > two > three


# ---------------------------------------------------------------------------
# Accessors and vocabulary - no Spark session needed
# ---------------------------------------------------------------------------


def test_the_accessor_comes_from_the_schema_walk_not_a_string_split():
    """A struct field `customer.name` and a top-level column literally named
    `customer.name` produce the same path string and must not resolve to the
    same thing."""
    nested = _nodes(StructField("customer", StructType([StructField("name", StringType())])))
    flat = _nodes(StructField("customer.name", StringType()))

    nested_field = [n for n in nested if n.path == "customer.name"][0]
    assert Candidate((nested_field,)).accessors == ("`customer`.`name`",)
    assert Candidate((flat[0],)).accessors == ("`customer.name`",)


def test_the_candidate_type_vocabulary_rejects_free_text():
    with pytest.raises(VocabularyError):
        validate_vocabulary("pk", "key_candidate_type", "candidate_type")


def test_foreign_key_is_declared_but_not_produced_here():
    """It is a claim about two tables, needs a join to verify, and belongs to
    RelationshipDetector (#298 phase 4). Declared so that phase has somewhere
    to write."""
    import inspect

    from bronze_ingest import key_detector

    assert "FOREIGN_KEY" in VOCABULARIES["key_candidate_type"]
    source = inspect.getsource(key_detector)
    assert '"candidate_type": classify(' in source
    assert "return FOREIGN_KEY" not in source


def test_approximate_counts_are_never_what_decides():
    """A probe rather than a promise, in the direction the module exists for:
    the deciding step must call the EXACT distinct count."""
    import inspect

    from bronze_ingest import key_detector

    source = inspect.getsource(key_detector.verify)
    assert "count_distinct(" in source
    assert "approx_count_distinct(" not in source


# ---------------------------------------------------------------------------
# Spark - the parts that genuinely need data
# ---------------------------------------------------------------------------


class _PassCounter:
    """Counts the DataFrame actions detection performs.

    Patches the CONCRETE runtime class, not `pyspark.sql.DataFrame`. In
    PySpark 4 the public name is an abstract base and
    `pyspark.sql.classic.dataframe.DataFrame` defines its own `agg`, so a
    patch applied to the base is shadowed by the subclass and counts nothing -
    #300, where the version that did that recorded 0 passes and its `== 0`
    assertion passed for entirely the wrong reason.
    """

    def __init__(self, monkeypatch, spark):
        cls = type(spark.range(1))
        self.aggs = 0
        real_agg = cls.agg

        def agg(df_self, *a, **k):
            self.aggs += 1
            return real_agg(df_self, *a, **k)

        monkeypatch.setattr(cls, "agg", agg)


def test_the_pass_counter_actually_counts(spark, monkeypatch):
    counter = _PassCounter(monkeypatch, spark)
    spark.range(3).agg(F.count(F.lit(1))).collect()
    assert counter.aggs == 1


def _table(spark, rows, ddl):
    name = f"keydet_{uuid.uuid4().hex[:8]}"
    spark.createDataFrame(rows, ddl).write.format("delta").saveAsTable(f"default.{name}")
    return f"default.{name}"


def _written(spark, config):
    return [r.asDict() for r in spark.read.table(f"default.{config.key_candidate_table}").collect()]


def test_verify_computes_every_candidate_in_one_pass(spark, monkeypatch):
    """The cost rule from #149, counted rather than promised: N candidates
    must cost one job, not N."""
    table = _table(spark, [(1, "a"), (2, "b"), (3, "c")], "id bigint, name string")
    df = spark.read.table(table)
    nodes = describe_columns(df.schema)
    candidates = [Candidate((nodes[0],)), Candidate((nodes[1],)), Candidate(tuple(nodes))]

    counter = _PassCounter(monkeypatch, spark)
    results = verify(df, candidates)

    assert len(results) == 3
    assert counter.aggs == 1, "three candidates must cost one aggregate, not three"


def test_verify_returns_exact_counts(spark):
    table = _table(spark, [(1, "a"), (2, "a"), (3, None)], "id bigint, name string")
    df = spark.read.table(table)
    nodes = describe_columns(df.schema)

    by_label = {v.candidate.label: v for v in verify(df, [Candidate((n,)) for n in nodes])}

    assert by_label["id"].row_count == 3
    assert by_label["id"].distinct_count == 3
    assert by_label["id"].null_count == 0
    assert by_label["name"].distinct_count == 1
    assert by_label["name"].null_count == 1


def test_a_single_column_primary_key_costs_exactly_one_pass(spark, monkeypatch):
    """Finding one makes the composite search unnecessary, so the common case
    stays at one job."""
    table = _table(spark, [(i, f"n{i}") for i in range(20)], "id bigint, name string")
    config = _cfg(min_rows_for_confidence=5)
    rows = [_profile("id", row_count=20, distinct_approx=20), _profile("name", row_count=20)]

    counter = _PassCounter(monkeypatch, spark)
    result = detect_keys(spark, table, config, profile_rows=rows)

    assert counter.aggs == 1
    assert result["primary_key_found"] is True
    assert any("composite search skipped" in n for n in result["notes"])


def test_an_overstated_profile_does_not_become_a_key(spark):
    """The difference between shortlisting and claiming, in one test.

    `approx_count_distinct` carries error, so a duplicated column can
    approximate to a full distinct count. If shortlisting were claiming, this
    would be written as a primary key - and a wrong key becomes a wrong grain,
    which deduplicates real rows away.
    """
    table = _table(spark, [(1, "dup"), (2, "dup"), (3, "dup")], "id bigint, category string")
    config = _cfg(min_rows_for_confidence=2)
    # The profile claims `category` has three distinct values. It has one.
    rows = [
        _profile("id", row_count=3, distinct_approx=3),
        _profile("category", row_count=3, distinct_approx=3),
    ]

    result = detect_keys(spark, table, config, profile_rows=rows)

    written = _written(spark, config)
    assert [r["column_names"] for r in written] == ["id"]
    assert result["candidates_tested"] == 2
    assert result["candidates_written"] == 1


def test_a_unique_but_nullable_column_is_written_as_unique(spark):
    table = _table(spark, [(1, "a"), (2, "b"), (3, None), (4, None)], "id bigint, code string")
    config = _cfg(min_rows_for_confidence=2)
    rows = [
        _profile("id", row_count=4, distinct_approx=4),
        _profile("code", row_count=4, distinct_approx=4, null_count=2),
    ]

    detect_keys(spark, table, config, profile_rows=rows)

    by_col = {r["column_names"]: r for r in _written(spark, config)}
    assert by_col["id"]["candidate_type"] == PRIMARY_KEY
    assert by_col["code"]["candidate_type"] == UNIQUE
    assert by_col["code"]["null_count"] == 2
    assert "null component" in by_col["code"]["blockers_json"]


def test_a_zero_row_table_produces_no_candidates_and_reads_no_data(spark, monkeypatch):
    """Zero rows makes every ratio undefined and '100% unique' vacuous
    (#298 risk 16). It is reported, not divided by."""
    table = _table(spark, [(1, "a")], "id bigint, name string")
    config = _cfg()
    rows = [_profile("id", row_count=0, distinct_approx=0)]

    counter = _PassCounter(monkeypatch, spark)
    result = detect_keys(spark, table, config, profile_rows=rows)

    assert counter.aggs == 0
    assert result["status"] == "skipped"
    assert result["candidates_written"] == 0
    assert "no rows" in result["reason"]


def test_one_row_of_data_yields_nothing_above_the_review_threshold(spark):
    """The state every table in this repo is actually in."""
    table = _table(spark, [(1, "a")], "id bigint, name string")
    config = _cfg()
    rows = [
        _profile("id", row_count=1, distinct_approx=1),
        _profile("name", row_count=1, distinct_approx=1),
    ]

    result = detect_keys(spark, table, config, profile_rows=rows)

    written = _written(spark, config)
    assert written, "candidates are recorded, not dropped - 'we looked' is information"
    assert result["candidates_above_threshold"] == 0
    assert all(r["confidence"] == 0.0 for r in written)
    assert all("row_count=1" in r["blockers_json"] for r in written)


def test_a_composite_key_is_found_when_no_single_column_is_unique(spark):
    table = _table(
        spark,
        [("a", 1), ("a", 2), ("b", 1), ("b", 2)],
        "grp string, seq bigint",
    )
    config = _cfg(min_rows_for_confidence=2)
    rows = [
        _profile("grp", row_count=4, distinct_approx=2),
        _profile("seq", row_count=4, distinct_approx=2),
    ]

    result = detect_keys(spark, table, config, profile_rows=rows)

    written = _written(spark, config)
    assert result["primary_key_found"] is False
    assert [set(r["column_names"].split(",")) for r in written] == [{"grp", "seq"}]
    assert written[0]["column_count"] == 2
    assert written[0]["candidate_type"] == PRIMARY_KEY


def test_every_key_candidate_column_can_actually_be_filled(spark):
    """The guard from #276/#64, applied to this table. Three audit columns
    were in the schema, set by the caller, and NULL in every row ever written,
    because the write path dropped them - and both features were closed as
    shipped."""
    table = _table(spark, [(i, f"n{i}") for i in range(6)], "id bigint, name string")
    config = _cfg(min_rows_for_confidence=2)
    rows = [
        _profile("id", row_count=6, distinct_approx=6),
        _profile("name", row_count=6, distinct_approx=6),
    ]

    detect_keys(spark, table, config, profile_rows=rows)

    assert unfilled_columns(_written(spark, config), KEY_CANDIDATE_SCHEMA) == []


def test_every_written_candidate_carries_the_counts_it_was_decided_on(spark):
    """#298 requires the evidence behind a recommendation to be persisted, not
    just the recommendation."""
    table = _table(spark, [(i, f"n{i}") for i in range(6)], "id bigint, name string")
    config = _cfg(min_rows_for_confidence=2)
    rows = [_profile("id", row_count=6, distinct_approx=6)]

    detect_keys(spark, table, config, profile_rows=rows)

    row = _written(spark, config)[0]
    evidence = json.loads(row["evidence_json"])
    assert evidence["distinct_exact"] == 6
    assert evidence["distinct_approx"] == 6
    assert evidence["row_count"] == 6
    assert evidence["columns"] == ["id"]
    assert json.loads(row["blockers_json"]) == []


def test_the_exact_and_approximate_counts_are_kept_side_by_side(spark):
    """So a disagreement between them is visible rather than resolved
    silently."""
    table = _table(spark, [(i, f"n{i}") for i in range(6)], "id bigint, name string")
    config = _cfg(min_rows_for_confidence=2, shortlist_ratio=0.5)
    rows = [_profile("id", row_count=6, distinct_approx=5)]

    detect_keys(spark, table, config, profile_rows=rows)

    row = _written(spark, config)[0]
    assert row["distinct_count"] == 6
    assert row["distinct_approx"] == 5


def test_detection_reads_the_latest_profile_when_none_is_supplied(spark):
    """End to end against the real producer: #299 writes the evidence, #327
    reads it. Pinning the two together means a change to the profile schema
    fails here rather than silently starving detection."""
    table = _table(spark, [(i, f"n{i}") for i in range(6)], "id bigint, name string")
    profile_config = ProfileConfig(
        metadata_schema="default",
        column_profile_table=f"cp_{uuid.uuid4().hex[:8]}",
        table_metadata_table=f"tm_{uuid.uuid4().hex[:8]}",
        column_metadata_table=f"cm_{uuid.uuid4().hex[:8]}",
    )
    profile_table(spark, table, profile_config)

    config = _cfg(
        column_profile_table=profile_config.column_profile_table, min_rows_for_confidence=2
    )
    result = detect_keys(spark, table, config)

    assert result["status"] == "detected"
    assert "id" in [r["column_names"] for r in _written(spark, config)]


def test_detection_without_a_profile_says_so(spark):
    table = _table(spark, [(1, "a")], "id bigint, name string")
    config = _cfg(column_profile_table=f"missing_{uuid.uuid4().hex[:8]}")

    with pytest.raises(ValueError, match="profile"):
        detect_keys(spark, table, config)
