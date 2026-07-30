"""
Lightweight data-quality gate applied right before the bronze write.

Checks:
  - required_columns: configured columns must be non-null in every row.
  - unique_columns: the configured column combination must be unique within
    the batch (duplicates - all but one row per group - are treated as bad).

Depending on `fail_on_quality_error`:
  - True  -> raise DataQualityError if any bad rows are found (fail the job)
  - False -> split the DataFrame into (good_df, bad_df); bad_df is written
             to a quarantine table so the batch can still succeed and be
             re-processed later once the source data is fixed.

Only these two structural checks are in scope here - range/regex/set-
membership/expression/freshness rules require business/domain knowledge
about what "valid" data means, which is a Silver-layer concern, not
Bronze's (see the discussion on #59/#95 and silver_layer/_archive/README.md
for why the flattener was pulled out of Bronze for the same reason).
"""

from typing import Tuple, List, Optional

from pyspark.sql.functions import (
    col,
    lit,
    when,
    concat,
    concat_ws,
    row_number,
    first,
    count,
    current_timestamp,
)
from pyspark.sql.window import Window

from .config import IngestionConfig
from .logging_utils import logger
from .sql_utils import row_content_hash


class DataQualityError(Exception):
    """
    Carries bad_count (when known) so a failed audited_run can record how
    many rows failed the quality gate, instead of leaving
    quarantined_row_count None on the failure audit row (#50).
    """

    def __init__(self, message: str, bad_count: Optional[int] = None):
        super().__init__(message)
        self.bad_count = bad_count


def _missing_columns(df, columns: List[str]) -> List[str]:
    return [c for c in columns if c not in df.columns]


def _duplicate_flag_column(df, config: IngestionConfig):
    """
    row_number() over a window partitioned by unique_columns, ordered by
    dedupe_order_by (descending, highest wins) when that column exists on
    df, then by a content hash as the final tie-break.

    The tie-break must be deterministic, and this is the whole point of it
    (#147). The previous implementation used `monotonically_increasing_id()`
    and its docstring claimed that was "deterministic for a given
    DataFrame". It is not: the value encodes the partition index and the
    row's position within that partition, so re-executing the same plan
    with a different partitioning yields different ids.

    That was not a cosmetic problem. `split_good_bad` derives good_df and
    bad_df from one tagged DataFrame, but they are two lazy plans, and Spark
    evaluates each independently. If the two evaluations disagreed about
    which member of a duplicate group got row_number 1, a row could land in
    BOTH good_df and bad_df (written to the bronze table *and* quarantined)
    or in NEITHER (silently dropped). No error either way.

    A content hash fixes it at the source: rows with identical content hash
    identically, so the ordering is a function of the data alone. Within a
    group of byte-identical rows the ordering is still arbitrary - but those
    rows are interchangeable, so the *content* of good_df and bad_df is
    deterministic even though which physical row was kept is not. That is
    the property the split actually needs.

    dedupe_order_by stays the primary sort where it applies. It is not
    sufficient on its own: two rows can share both the unique_columns and
    the dedupe_order_by value, and that tie was previously broken
    nondeterministically too. It defaults to audit_ingest_ts_col for
    bronze_writer's merge-time dedupe, but that column does not exist yet
    here - this gate runs before add_audit_columns() - so on the common path
    the hash is doing all the work.

    Row 1 per group is the row to keep; every other row in the group is
    flagged as a duplicate.
    """
    tie_break = row_content_hash(df).asc()

    order_col = config.dedupe_order_by
    if order_col and order_col in df.columns:
        order_exprs = [col(f"`{order_col}`").desc(), tie_break]
    else:
        order_exprs = [tie_break]

    w = Window.partitionBy(*config.unique_columns).orderBy(*order_exprs)
    return row_number().over(w) > 1


def split_good_bad(df, config: IngestionConfig) -> Tuple[object, object]:
    """
    Returns (good_df, bad_df). bad_df is empty (0 rows, same schema) if
    neither required_columns nor unique_columns are configured, or no
    violations are found.

    Both checks are evaluated into a single _dq_bad tag column on one pass
    over the data (the unique_columns check adds one window/shuffle
    regardless of how many columns are in the combination - not a
    per-column rescan), and both good_df/bad_df are derived from that same
    tagged DataFrame.

    good_df and bad_df are two lazy plans, not one materialized split -
    Spark evaluates each independently, so every expression feeding
    `_dq_bad` must be a pure function of the row's content or the two
    evaluations can disagree and a row lands in both or neither (#147).
    `isNull()` is; `_duplicate_flag_column`'s window ordering was not until
    it was given a content-hash tie-break. Anything added to `_dq_bad`
    later must clear the same bar.

    bad_df additionally carries:

      - `_quarantine_reason`, per row, describing which check(s) failed
        (e.g. "null:email", "duplicate:order_id,customer_id", or both joined
        with "|") instead of a single hardcoded reason - this is what makes
        quarantined rows and replay (#60) queryable per failure type.
      - `_quarantine_id`, a SHA-256 of the row's source content, which is the
        stable identity write_quarantine() MERGEs on and replay uses to
        remove exactly the rows it re-promoted (#148).
    """
    if not config.required_columns and not config.unique_columns:
        return df, df.limit(0)

    missing = _missing_columns(df, config.required_columns) + _missing_columns(
        df, config.unique_columns or []
    )
    if missing:
        # Columns that don't exist at all are a schema problem, not a per-row
        # quality problem - always a hard failure regardless of fail_on_quality_error.
        raise DataQualityError(
            f"required_columns/unique_columns {missing} not present in source schema. "
            f"Available columns: {df.columns}"
        )

    tagged = df
    reason_parts = []

    if config.required_columns:
        null_cond = None
        for c in config.required_columns:
            cond = col(f"`{c}`").isNull()
            null_cond = cond if null_cond is None else (null_cond | cond)
        null_reason = concat_ws(
            ",", *[when(col(f"`{c}`").isNull(), lit(c)) for c in config.required_columns]
        )
        tagged = tagged.withColumn("_dq_null", null_cond)
        reason_parts.append(when(col("_dq_null"), concat(lit("null:"), null_reason)))
    else:
        tagged = tagged.withColumn("_dq_null", lit(False))

    if config.unique_columns:
        tagged = tagged.withColumn("_dq_dup", _duplicate_flag_column(tagged, config))
        reason_parts.append(
            when(col("_dq_dup"), lit("duplicate:" + ",".join(config.unique_columns)))
        )
    else:
        tagged = tagged.withColumn("_dq_dup", lit(False))

    tagged = tagged.withColumn("_dq_bad", col("_dq_null") | col("_dq_dup")).withColumn(
        "_quarantine_reason", concat_ws("|", *reason_parts)
    )

    good_df = tagged.filter(~col("_dq_bad")).drop(
        "_dq_null", "_dq_dup", "_dq_bad", "_quarantine_reason"
    )

    # `_quarantine_id` is derived HERE, not in write_quarantine(), and that
    # placement is the point (#148).
    #
    # By the time write_quarantine() sees this DataFrame, pipeline.py has
    # already run it through add_audit_columns(), which stamps
    # `_ingested_at` from current_timestamp(). Hashing the row at that point
    # would fold a wall-clock value into the identity and produce a
    # different id on every run - exactly the property we are trying to get
    # rid of. Here there are no audit columns yet, so the hash is over
    # source content alone and no exclusion list has to be maintained as
    # audit columns come and go.
    #
    # `_quarantine_reason` is excluded too: identity is "this source row",
    # not "this source row failed this particular way". If a config change
    # alters why a row is bad, that should UPDATE the existing quarantine
    # entry rather than create a second one for the same data.
    id_columns = [c for c in df.columns if c != "_quarantine_id"]
    bad_df = (
        tagged.filter(col("_dq_bad"))
        .drop("_dq_null", "_dq_dup", "_dq_bad")
        .withColumn("_quarantine_id", row_content_hash(tagged, columns=id_columns))
    )
    return good_df, bad_df


def enforce_quality(df, config: IngestionConfig):
    """
    Applies the quality gate and returns (good_df, bad_df, bad_count).
    Raises DataQualityError if fail_on_quality_error=True and bad rows exist.
    """
    good_df, bad_df = split_good_bad(df, config)
    bad_count = bad_df.count() if (config.required_columns or config.unique_columns) else 0

    if bad_count > 0:
        checks = []
        if config.required_columns:
            checks.append(f"null in one of required_columns={config.required_columns}")
        if config.unique_columns:
            checks.append(f"duplicate on unique_columns={config.unique_columns}")
        msg = f"{bad_count} row(s) failed data quality checks ({'; '.join(checks)})"
        if config.fail_on_quality_error:
            raise DataQualityError(msg, bad_count=bad_count)
        logger.warning("%s - quarantining to %s", msg, config.resolved_quarantine_table)

    return good_df, bad_df, bad_count


#: Bookkeeping columns write_quarantine maintains itself, on top of whatever
#: bad_df carries. Named here because pre-existing quarantine tables predate
#: them and have to be backfilled before the MERGE can reference them.
_QUARANTINE_META_COLUMNS = {
    "_occurrence_count": "BIGINT",
    "_first_quarantined_at": "TIMESTAMP",
}


def _align_quarantine_schema(spark, table_name: str, source_schema) -> None:
    """
    Adds to the quarantine table any top-level column the MERGE is about to
    reference but the table does not have yet.

    `withSchemaEvolution()` is not enough on its own, and the reason is
    specific: Delta resolves the merge condition and the UPDATE SET clause
    against the target's CURRENT schema, *before* evolution adds anything. A
    quarantine table created before #148 has no `_batch_id`, so the guarded
    matched-condition fails to resolve with
    DELTA_MERGE_UNRESOLVED_EXPRESSION - found by running exactly that case.
    Evolution still earns its place for nested and non-top-level changes;
    this handles the columns the statement text names.

    An explicit ALTER rather than flipping
    `spark.databricks.delta.schema.autoMerge.enabled`, which is a
    session-wide switch and would silently change behaviour for every other
    write sharing the session.
    """
    existing = {f.name for f in spark.read.table(table_name).schema.fields}

    additions = {
        f.name: f.dataType.simpleString() for f in source_schema.fields if f.name not in existing
    }
    additions.update({c: t for c, t in _QUARANTINE_META_COLUMNS.items() if c not in existing})
    if not additions:
        return

    cols = ", ".join(f"`{name}` {sql_type}" for name, sql_type in additions.items())
    logger.info("Aligning quarantine table %s - adding column(s): %s", table_name, cols)
    spark.sql(f"ALTER TABLE {table_name} ADD COLUMNS ({cols})")


def write_quarantine(spark, bad_df, bad_count: int, config: IngestionConfig):
    """
    MERGEs bad_df into the quarantine table, keyed on `_quarantine_id`.

    Skips entirely when bad_count is 0 - enforce_quality() already computed
    this count, so there's no need for a separate bad_df.rdd.isEmpty() probe
    (another full scan of the source) to re-derive information the caller
    already has.

    Why MERGE and not append (#148)
    -------------------------------
    Quarantine is written BEFORE the bronze write, so a run that fails
    between the two and is then retried quarantines the same rows twice.
    Under the old code that produced genuine duplicates, because
    `_quarantine_id` was `uuid()` - which its docstring called "a stable
    UUID". It is stable within one query plan, but a re-run of the same
    source produces entirely different ids. Verified against a local
    session: two evaluations of one DataFrame gave identical ids, a fresh
    construction of the same logical DataFrame gave different ones.

    So the same bad row accumulated one quarantine row per attempt, each
    with its own id, and replay (#60) treated them as distinct rows to
    re-promote. `_quarantine_id` is now a SHA-256 of the row's source
    content (see split_good_bad), which makes it a stable identity and makes
    this write idempotent: re-running merges onto the same key instead of
    inserting beside it.

    What happens to duplicate bad rows
    ----------------------------------
    Byte-identical bad rows collapse to ONE quarantine row - they have to,
    since Delta refuses a MERGE where several source rows match one target
    row. Their multiplicity is kept in `_occurrence_count` rather than
    discarded, so `bad_count` (which counts rows) and the quarantine table's
    row count can legitimately differ, and the sum of `_occurrence_count`
    reconciles them.

    `_occurrence_count` only increments when the incoming `_batch_id`
    differs from the one already recorded, so re-running the SAME batch
    leaves the count alone. That guarantee is only as good as `_batch_id`:
    with the deployed job it is `{{job.run_id}}` and stable across task
    attempts (#63), but a config that leaves `batch_id` unset gets a
    generated timestamp that differs per attempt, and the count will drift
    upward on retries. Row identity stays correct either way - only the
    count is affected.

    Pre-existing rows
    -----------------
    Rows quarantined before this change carry UUID `_quarantine_id`s, which
    will never match a content hash. They are left exactly as they are: the
    same source row may therefore appear once under an old UUID and once
    under its hash. Nothing breaks, but the old rows will not deduplicate.
    To clean them out, delete rows whose id is not 64 hex characters after
    confirming the current data has been re-quarantined:

        DELETE FROM <table>_quarantine WHERE length(_quarantine_id) <> 64
    """
    if bad_df is None or bad_count == 0:
        return

    from delta.tables import DeltaTable

    schema_ref = f"{config.catalog}.{config.schema_name}" if config.catalog else config.schema_name
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema_ref}")

    table_name = config.resolved_quarantine_table

    if "_quarantine_id" not in bad_df.columns:
        # Defensive: every in-package caller gets bad_df from
        # split_good_bad(), which sets this. A caller assembling bad_df by
        # hand would otherwise fail deep inside the MERGE condition.
        raise ValueError(
            "bad_df has no _quarantine_id column - it must come from "
            "split_good_bad(), which derives it from the row's source content "
            "before audit columns are added."
        )

    batch_col = config.audit_batch_id_col
    # pipeline.py passes bad_df through add_audit_columns() before calling
    # this, so the audit columns are normally present - but write_quarantine
    # is a public function and is called directly (including by the tests)
    # with a raw bad_df, so neither column can be assumed.
    has_batch_col = batch_col in bad_df.columns
    has_ingest_ts = config.audit_ingest_ts_col in bad_df.columns

    # Collapse byte-identical bad rows, carrying their count. Delta rejects a
    # MERGE where more than one source row matches the same target row, so
    # this is required rather than an optimisation.
    source = (
        bad_df.groupBy("_quarantine_id")
        .agg(
            *[first(col(f"`{c}`")).alias(c) for c in bad_df.columns if c != "_quarantine_id"],
            count(lit(1)).alias("_occurrence_count"),
        )
        # Set here rather than copied from the audit ingest timestamp so it
        # does not depend on add_audit_columns() having run. Only ever
        # written on insert - the matched branch below leaves it alone, which
        # is what makes it "first".
        .withColumn("_first_quarantined_at", current_timestamp())
    )

    # Atomic create-if-not-exists rather than a tableExists() check followed
    # by an append - two concurrent first writes could otherwise both see
    # "missing" and both create/append, which is the #46 race in a different
    # module. Merging into a freshly-created empty table inserts everything.
    creator = DeltaTable.createIfNotExists(spark).tableName(table_name).addColumns(source.schema)
    creator.execute()
    _align_quarantine_schema(spark, table_name, source.schema)

    target = DeltaTable.forName(spark, table_name)

    # Latest reason and sighting win, so a row's quarantine entry reflects why
    # it is CURRENTLY bad rather than why it first was.
    updates = {"_quarantine_reason": "source._quarantine_reason"}
    if has_ingest_ts:
        updates[config.audit_ingest_ts_col] = f"source.`{config.audit_ingest_ts_col}`"

    if has_batch_col:
        updates[batch_col] = f"source.`{batch_col}`"
        updates["_occurrence_count"] = (
            "coalesce(target._occurrence_count, 1) + source._occurrence_count"
        )
        # Guarded, not unconditional: without this a retry of the SAME batch
        # would increment _occurrence_count again, re-introducing the
        # double-counting this change exists to remove.
        matched_condition = (
            f"target.`{batch_col}` is null or target.`{batch_col}` <> source.`{batch_col}`"
        )
    else:
        # No batch column means no way to tell a retry from a genuine
        # re-occurrence, so the count is deliberately left untouched rather
        # than guessed at. Under-counting is recoverable; silently inflating a
        # count that operators use to size a data-quality problem is not.
        matched_condition = None

    (
        target.alias("target")
        .merge(source.alias("source"), "target._quarantine_id = source._quarantine_id")
        # Preserves what the previous append's `mergeSchema: true` gave us.
        # Quarantine tables see schema drift by design - that is half of what
        # bronze is for - and a plain MERGE would start failing the moment a
        # source grew a column. Scoped to this statement rather than set via
        # spark.databricks.delta.schema.autoMerge.enabled, which is a session
        # -wide switch that would silently affect every other write too.
        .withSchemaEvolution()
        .whenMatchedUpdate(condition=matched_condition, set=updates)
        .whenNotMatchedInsertAll()
        .execute()
    )
