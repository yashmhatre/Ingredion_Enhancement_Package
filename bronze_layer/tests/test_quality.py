import uuid
from dataclasses import replace

import pytest

from bronze_ingest.config import IngestionConfig
from bronze_ingest.quality import (
    DataQualityError,
    enforce_quality,
    split_good_bad,
    write_quarantine,
)


def _df(spark):
    return spark.createDataFrame(
        [(1, "Alice"), (2, None), (3, "Carol")],
        ["id", "name"],
    )


def test_no_required_columns_passes_everything(spark):
    df = _df(spark)
    cfg = IngestionConfig(source_path="x", table="t")
    good, bad = split_good_bad(df, cfg)
    assert good.count() == 3
    assert bad.count() == 0


def test_split_good_bad_partitions_nulls(spark):
    df = _df(spark)
    cfg = IngestionConfig(source_path="x", table="t", required_columns=["name"])
    good, bad = split_good_bad(df, cfg)
    assert good.count() == 2
    assert bad.count() == 1


def test_enforce_quality_raises_when_fail_on_error(spark):
    df = _df(spark)
    cfg = IngestionConfig(
        source_path="x", table="t", required_columns=["name"], fail_on_quality_error=True
    )
    with pytest.raises(DataQualityError):
        enforce_quality(df, cfg)


def test_data_quality_error_carries_bad_count(spark):
    """#50: the failure audit row needs to recover how many rows failed -
    DataQualityError must carry bad_count when enforce_quality raises it."""
    df = _df(spark)
    cfg = IngestionConfig(
        source_path="x", table="t", required_columns=["name"], fail_on_quality_error=True
    )
    with pytest.raises(DataQualityError) as exc_info:
        enforce_quality(df, cfg)
    assert exc_info.value.bad_count == 1


def test_missing_required_column_error_has_no_bad_count(spark):
    df = _df(spark)
    cfg = IngestionConfig(
        source_path="x", table="t", required_columns=["does_not_exist"], fail_on_quality_error=True
    )
    with pytest.raises(DataQualityError) as exc_info:
        enforce_quality(df, cfg)
    assert exc_info.value.bad_count is None


def test_enforce_quality_quarantines_when_not_failing(spark):
    df = _df(spark)
    cfg = IngestionConfig(
        source_path="x", table="t", required_columns=["name"], fail_on_quality_error=False
    )
    good, bad, bad_count = enforce_quality(df, cfg)
    assert bad_count == 1
    assert good.count() == 2


def test_missing_required_column_always_raises(spark):
    df = _df(spark)
    cfg = IngestionConfig(
        source_path="x", table="t", required_columns=["does_not_exist"], fail_on_quality_error=False
    )
    with pytest.raises(DataQualityError):
        enforce_quality(df, cfg)


def test_split_good_bad_does_not_leak_tag_column(spark):
    df = _df(spark)
    cfg = IngestionConfig(source_path="x", table="t", required_columns=["name"])
    good, bad = split_good_bad(df, cfg)
    assert "_dq_bad" not in good.columns
    assert "_dq_bad" not in bad.columns


def test_write_quarantine_writes_rows_when_bad_count_positive(spark):
    table = f"quality_quarantine_{uuid.uuid4().hex[:8]}"
    cfg = IngestionConfig(
        source_path="x",
        table=table,
        schema_name="default",
        catalog=None,
        required_columns=["name"],
        fail_on_quality_error=False,
    )
    good, bad, bad_count = enforce_quality(_df(spark), cfg)

    write_quarantine(spark, bad, bad_count, cfg)

    assert spark.read.table(cfg.resolved_quarantine_table).count() == 1


def test_write_quarantine_adds_unique_quarantine_id(spark):
    """#60: replay needs a stable per-row identifier to know exactly which
    quarantined rows were successfully re-promoted to bronze."""
    table = f"quality_quarantine_id_{uuid.uuid4().hex[:8]}"
    cfg = IngestionConfig(
        source_path="x",
        table=table,
        schema_name="default",
        catalog=None,
        required_columns=["name"],
        fail_on_quality_error=False,
    )
    df = spark.createDataFrame([(1, None), (2, None)], "id INT, name STRING")
    good, bad, bad_count = enforce_quality(df, cfg)

    write_quarantine(spark, bad, bad_count, cfg)

    rows = spark.read.table(cfg.resolved_quarantine_table).collect()
    ids = {r["_quarantine_id"] for r in rows}
    assert len(ids) == 2
    assert all(qid is not None for qid in ids)


def test_write_quarantine_uses_specific_reason(spark):
    table = f"quality_quarantine_reason_{uuid.uuid4().hex[:8]}"
    cfg = IngestionConfig(
        source_path="x",
        table=table,
        schema_name="default",
        catalog=None,
        required_columns=["name"],
        fail_on_quality_error=False,
    )
    good, bad, bad_count = enforce_quality(_df(spark), cfg)
    write_quarantine(spark, bad, bad_count, cfg)
    rows = spark.read.table(cfg.resolved_quarantine_table).collect()
    assert rows[0]["_quarantine_reason"] == "null:name"


# ---- unique_columns (#59, narrowed scope) ----


def test_split_good_bad_partitions_duplicates(spark):
    df = spark.createDataFrame(
        [(1, "Alice", "2024-01-01"), (1, "Alice2", "2024-01-02"), (2, "Bob", "2024-01-01")],
        ["id", "name", "ts"],
    )
    cfg = IngestionConfig(source_path="x", table="t", unique_columns=["id"], dedupe_order_by="ts")
    good, bad = split_good_bad(df, cfg)
    assert good.count() == 2
    assert bad.count() == 1
    kept = good.filter(good.id == 1).collect()[0]
    assert kept["name"] == "Alice2"  # higher ts wins the tie-break


def test_duplicate_quarantine_reason_is_specific(spark):
    df = spark.createDataFrame(
        [(1, "Alice", "2024-01-01"), (1, "Alice2", "2024-01-02")],
        ["id", "name", "ts"],
    )
    cfg = IngestionConfig(source_path="x", table="t", unique_columns=["id"], dedupe_order_by="ts")
    good, bad = split_good_bad(df, cfg)
    reasons = {r["_quarantine_reason"] for r in bad.collect()}
    assert reasons == {"duplicate:id"}


def test_null_and_duplicate_reasons_combine(spark):
    df = spark.createDataFrame(
        [(1, None, "2024-01-01"), (2, "Bob", "2024-01-01"), (2, "Bob2", "2024-01-02")],
        "id INT, name STRING, ts STRING",
    )
    cfg = IngestionConfig(
        source_path="x",
        table="t",
        required_columns=["name"],
        unique_columns=["id"],
        dedupe_order_by="ts",
    )
    good, bad = split_good_bad(df, cfg)
    reasons = {r["id"]: r["_quarantine_reason"] for r in bad.collect()}
    assert reasons[1] == "null:name"
    assert reasons[2] == "duplicate:id"
    assert good.count() == 1


def test_enforce_quality_raises_on_duplicates_when_fail_on_error(spark):
    df = spark.createDataFrame([(1, "Alice"), (1, "Bob")], ["id", "name"])
    cfg = IngestionConfig(
        source_path="x", table="t", unique_columns=["id"], fail_on_quality_error=True
    )
    with pytest.raises(DataQualityError) as exc_info:
        enforce_quality(df, cfg)
    assert exc_info.value.bad_count == 1


def test_enforce_quality_quarantines_duplicates_when_not_failing(spark):
    df = spark.createDataFrame([(1, "Alice"), (1, "Bob")], ["id", "name"])
    cfg = IngestionConfig(
        source_path="x", table="t", unique_columns=["id"], fail_on_quality_error=False
    )
    good, bad, bad_count = enforce_quality(df, cfg)
    assert bad_count == 1
    assert good.count() == 1


def test_missing_unique_column_always_raises(spark):
    df = _df(spark)
    cfg = IngestionConfig(
        source_path="x", table="t", unique_columns=["does_not_exist"], fail_on_quality_error=False
    )
    with pytest.raises(DataQualityError):
        enforce_quality(df, cfg)


def test_duplicate_check_does_not_leak_tag_columns(spark):
    df = spark.createDataFrame([(1, "Alice"), (1, "Bob")], ["id", "name"])
    cfg = IngestionConfig(source_path="x", table="t", unique_columns=["id"])
    good, bad = split_good_bad(df, cfg)
    for c in ("_dq_bad", "_dq_null", "_dq_dup"):
        assert c not in good.columns
        assert c not in bad.columns
    assert "_quarantine_reason" not in good.columns
    assert "_quarantine_reason" in bad.columns


def test_duplicate_tie_break_without_dedupe_order_by_is_deterministic(spark):
    """No dedupe_order_by and no matching source column - falls back to the
    content-hash tie-break rather than raising."""
    df = spark.createDataFrame([(1, "Alice"), (1, "Bob")], ["id", "name"])
    cfg = IngestionConfig(source_path="x", table="t", unique_columns=["id"])
    good, bad = split_good_bad(df, cfg)
    assert good.count() == 1
    assert bad.count() == 1


def test_write_quarantine_skips_when_bad_count_zero(spark):
    table = f"quality_quarantine_skip_{uuid.uuid4().hex[:8]}"
    cfg = IngestionConfig(
        source_path="x",
        table=table,
        schema_name="default",
        catalog=None,
        required_columns=["name"],
        fail_on_quality_error=False,
    )
    all_good_df = spark.createDataFrame([(1, "Alice"), (2, "Bob")], ["id", "name"])
    good, bad, bad_count = enforce_quality(all_good_df, cfg)

    assert bad_count == 0
    write_quarantine(spark, bad, bad_count, cfg)

    assert not spark.catalog.tableExists(cfg.resolved_quarantine_table)


# ---- the split must be a partition of the input (#147) ----
#
# good_df and bad_df are two lazy plans over one tagged DataFrame, and Spark
# evaluates each independently. Every expression feeding _dq_bad therefore
# has to be a pure function of row content, or the two evaluations can
# disagree: a row lands in BOTH (written to bronze *and* quarantined) or in
# NEITHER (silently dropped). The tie-break in _duplicate_flag_column used
# monotonically_increasing_id(), which encodes partition index and position
# within the partition - so the disagreement was reachable by nothing more
# exotic than a different partitioning.


def _split_contents(df, cfg):
    """(sorted good rows, sorted bad rows) as plain tuples."""
    good, bad = split_good_bad(df, cfg)
    return (
        sorted(tuple(r) for r in good.collect()),
        sorted(tuple(r)[: len(df.columns)] for r in bad.collect()),
    )


@pytest.mark.parametrize("partitions", [1, 3, 8])
def test_split_is_exact_partition_of_input_at_any_partitioning(spark, partitions):
    """good + bad == input exactly: nothing duplicated, nothing dropped."""
    rows = [(i % 4, f"name_{i}") for i in range(24)]
    df = spark.createDataFrame(rows, ["id", "name"]).repartition(partitions)
    cfg = IngestionConfig(source_path="x", table="t", unique_columns=["id"])

    good_rows, bad_rows = _split_contents(df, cfg)

    assert len(good_rows) + len(bad_rows) == len(rows)
    assert sorted(good_rows + bad_rows) == sorted(rows)
    # 4 distinct ids -> exactly 4 survivors, the rest are duplicates.
    assert len(good_rows) == 4


def test_split_is_stable_across_repartitioning(spark):
    """The SAME rows are kept regardless of how the input is partitioned.

    This is the regression that matters. With the old
    monotonically_increasing_id() tie-break, re-partitioning changed which
    member of each duplicate group got row_number 1, so the two plans could
    select different survivors.
    """
    rows = [(i % 5, f"name_{i}") for i in range(30)]
    cfg = IngestionConfig(source_path="x", table="t", unique_columns=["id"])

    base = spark.createDataFrame(rows, ["id", "name"])
    results = [_split_contents(base.repartition(n), cfg) for n in (1, 2, 7)]

    assert results[0] == results[1] == results[2]


def test_split_is_stable_across_repeated_evaluation(spark):
    """Evaluating the same split twice returns the same rows both times."""
    rows = [(i % 3, f"name_{i}") for i in range(12)]
    df = spark.createDataFrame(rows, ["id", "name"])
    cfg = IngestionConfig(source_path="x", table="t", unique_columns=["id"])

    good, bad = split_good_bad(df, cfg)

    first = (sorted(tuple(r) for r in good.collect()), sorted(tuple(r) for r in bad.collect()))
    second = (sorted(tuple(r) for r in good.collect()), sorted(tuple(r) for r in bad.collect()))
    assert first == second


def test_split_partitions_rows_differing_only_in_null_position(spark):
    """Two bad rows that differ only in WHICH column is null must stay distinct.

    concat_ws skips nulls, so ('x', None) and (None, 'x') would hash
    identically under it and the two rows would be treated as byte-identical
    duplicates. row_content_hash uses to_json(struct(*)) precisely to avoid
    that, and null-bearing rows are exactly what this module quarantines.
    """
    rows = [(1, "x", None), (1, None, "x")]
    df = spark.createDataFrame(rows, "id INT, a STRING, b STRING")
    cfg = IngestionConfig(source_path="x", table="t", unique_columns=["id"])

    good_rows, bad_rows = _split_contents(df, cfg)

    # repr-keyed: these rows contain nulls, which sorted() cannot compare
    # against strings.
    def by_repr(rs):
        return sorted(rs, key=repr)

    assert len(good_rows) == 1 and len(bad_rows) == 1
    assert by_repr(good_rows + bad_rows) == by_repr(rows)


def test_dedupe_order_by_still_wins_over_the_tie_break(spark):
    """The content hash is the FINAL sort key, not the primary one."""
    rows = [(1, "old", 1), (1, "new", 2)]
    df = spark.createDataFrame(rows, ["id", "name", "ts"])
    cfg = IngestionConfig(source_path="x", table="t", unique_columns=["id"], dedupe_order_by="ts")

    good, _ = split_good_bad(df, cfg)

    assert [tuple(r) for r in good.collect()] == [(1, "new", 2)]


# ---- quarantine identity and idempotency (#148) ----
#
# Quarantine is written BEFORE the bronze write, so a run that dies between
# the two and is retried quarantines the same rows again. `_quarantine_id`
# used to be uuid(), which its docstring called "a stable UUID" - stable
# within one query plan, but a fresh evaluation of the same source produces
# entirely different values, so each attempt appended its own copy and
# replay (#60) saw them as distinct rows to re-promote.


def _bad_with_audit(spark, rows, cfg, batch_id):
    """bad_df as pipeline.py produces it: split, then audit columns."""
    from bronze_ingest.bronze_writer import add_audit_columns

    df = spark.createDataFrame(rows, "id INT, name STRING")
    _, bad = split_good_bad(df, cfg)
    return add_audit_columns(bad, replace(cfg, batch_id=batch_id))


def _quarantine_cfg(prefix, **kwargs):
    return IngestionConfig(
        source_path="x",
        table=f"{prefix}_{uuid.uuid4().hex[:8]}",
        schema_name="default",
        catalog=None,
        required_columns=["name"],
        fail_on_quality_error=False,
        **kwargs,
    )


def test_quarantine_id_is_stable_across_fresh_evaluation(spark):
    """The property uuid() did not have: same source in, same id out."""
    cfg = _quarantine_cfg("q_stable")
    rows = [(1, None), (2, None)]

    def ids():
        df = spark.createDataFrame(rows, "id INT, name STRING")
        _, bad = split_good_bad(df, cfg)
        return sorted(r["_quarantine_id"] for r in bad.collect())

    assert ids() == ids()


def test_quarantine_id_differs_per_distinct_row(spark):
    cfg = _quarantine_cfg("q_distinct")
    df = spark.createDataFrame([(1, None), (2, None)], "id INT, name STRING")
    _, bad = split_good_bad(df, cfg)
    assert len({r["_quarantine_id"] for r in bad.collect()}) == 2


def test_rewriting_the_same_batch_does_not_duplicate_rows(spark):
    """The #148 regression: a retried run must not append a second copy."""
    cfg = _quarantine_cfg("q_retry")
    rows = [(1, None), (2, None)]

    for _ in range(3):
        bad = _bad_with_audit(spark, rows, cfg, batch_id="run-1")
        write_quarantine(spark, bad, bad.count(), cfg)

    table = spark.read.table(cfg.resolved_quarantine_table)
    assert table.count() == 2


def test_rewriting_the_same_batch_does_not_inflate_occurrence_count(spark):
    """Idempotent in the counter too, not just in row count."""
    cfg = _quarantine_cfg("q_retry_count")
    rows = [(1, None)]

    for _ in range(3):
        bad = _bad_with_audit(spark, rows, cfg, batch_id="run-1")
        write_quarantine(spark, bad, bad.count(), cfg)

    row = spark.read.table(cfg.resolved_quarantine_table).collect()[0]
    assert row["_occurrence_count"] == 1


def test_a_later_batch_increments_occurrence_count(spark):
    """A genuine re-occurrence in a new batch is still counted."""
    cfg = _quarantine_cfg("q_recur")
    rows = [(1, None)]

    for batch in ("run-1", "run-2", "run-3"):
        bad = _bad_with_audit(spark, rows, cfg, batch_id=batch)
        write_quarantine(spark, bad, bad.count(), cfg)

    table = spark.read.table(cfg.resolved_quarantine_table)
    assert table.count() == 1
    assert table.collect()[0]["_occurrence_count"] == 3


def test_identical_bad_rows_collapse_and_keep_their_count(spark):
    """Delta cannot MERGE many source rows onto one target row, so identical
    bad rows must collapse - their multiplicity goes to _occurrence_count
    rather than being silently dropped."""
    cfg = _quarantine_cfg("q_collapse")
    rows = [(1, None), (1, None), (1, None), (2, None)]

    bad = _bad_with_audit(spark, rows, cfg, batch_id="run-1")
    write_quarantine(spark, bad, bad.count(), cfg)

    table = spark.read.table(cfg.resolved_quarantine_table)
    assert table.count() == 2
    counts = {r["id"]: r["_occurrence_count"] for r in table.collect()}
    assert counts == {1: 3, 2: 1}
    # bad_count counts rows, the table counts identities - _occurrence_count
    # is what reconciles the two.
    assert sum(counts.values()) == len(rows)


def test_first_quarantined_at_survives_a_later_batch(spark):
    cfg = _quarantine_cfg("q_first_seen")
    rows = [(1, None)]

    bad = _bad_with_audit(spark, rows, cfg, batch_id="run-1")
    write_quarantine(spark, bad, bad.count(), cfg)
    first_seen = spark.read.table(cfg.resolved_quarantine_table).collect()[0][
        "_first_quarantined_at"
    ]

    bad = _bad_with_audit(spark, rows, cfg, batch_id="run-2")
    write_quarantine(spark, bad, bad.count(), cfg)
    row = spark.read.table(cfg.resolved_quarantine_table).collect()[0]

    assert row["_first_quarantined_at"] == first_seen
    assert row[cfg.audit_batch_id_col] == "run-2"  # last sighting moves


def test_write_quarantine_rejects_bad_df_without_quarantine_id(spark):
    """Fails with a message naming the cause, not deep inside the MERGE."""
    cfg = _quarantine_cfg("q_no_id")
    df = spark.createDataFrame([(1, None)], "id INT, name STRING")

    with pytest.raises(ValueError, match="_quarantine_id"):
        write_quarantine(spark, df, 1, cfg)


def test_quarantine_meta_columns_are_backfilled_on_older_tables(spark):
    """A quarantine table created before #148 has no _occurrence_count, and
    MERGE cannot reference a target column that does not exist."""
    cfg = _quarantine_cfg("q_backfill")
    table = cfg.resolved_quarantine_table

    # A pre-#148 table: source columns, a reason, and a UUID-shaped id.
    legacy = spark.createDataFrame(
        [(9, None, "null:name", str(uuid.uuid4()))],
        "id INT, name STRING, _quarantine_reason STRING, _quarantine_id STRING",
    )
    legacy.write.format("delta").mode("overwrite").saveAsTable(table)

    bad = _bad_with_audit(spark, [(1, None)], cfg, batch_id="run-1")
    write_quarantine(spark, bad, bad.count(), cfg)

    result = spark.read.table(table)
    assert "_occurrence_count" in result.columns
    # The legacy UUID row cannot match a content hash, so it stays put.
    assert result.count() == 2
