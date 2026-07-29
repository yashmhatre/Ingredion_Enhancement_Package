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
    expr,
    when,
    concat,
    concat_ws,
    row_number,
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

    bad_df additionally carries a per-row `_quarantine_reason` describing
    which check(s) failed (e.g. "null:email", "duplicate:order_id,
    customer_id", or both joined with "|") instead of a single hardcoded
    reason - this is what makes quarantined rows and replay (#60) queryable
    per failure type.
    """
    if not config.required_columns and not config.unique_columns:
        return df, df.limit(0)

    missing = _missing_columns(df, config.required_columns) + _missing_columns(df, config.unique_columns or [])
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
        null_reason = concat_ws(",", *[when(col(f"`{c}`").isNull(), lit(c)) for c in config.required_columns])
        tagged = tagged.withColumn("_dq_null", null_cond)
        reason_parts.append(when(col("_dq_null"), concat(lit("null:"), null_reason)))
    else:
        tagged = tagged.withColumn("_dq_null", lit(False))

    if config.unique_columns:
        tagged = tagged.withColumn("_dq_dup", _duplicate_flag_column(tagged, config))
        reason_parts.append(when(col("_dq_dup"), lit("duplicate:" + ",".join(config.unique_columns))))
    else:
        tagged = tagged.withColumn("_dq_dup", lit(False))

    tagged = (
        tagged.withColumn("_dq_bad", col("_dq_null") | col("_dq_dup"))
        .withColumn("_quarantine_reason", concat_ws("|", *reason_parts))
    )

    good_df = tagged.filter(~col("_dq_bad")).drop("_dq_null", "_dq_dup", "_dq_bad", "_quarantine_reason")
    bad_df = tagged.filter(col("_dq_bad")).drop("_dq_null", "_dq_dup", "_dq_bad")
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


def write_quarantine(spark, bad_df, bad_count: int, config: IngestionConfig):
    """
    Writes bad_df to the quarantine table. Skips entirely when bad_count
    is 0 - enforce_quality() already computed this count, so there's no
    need for a separate bad_df.rdd.isEmpty() probe (another full scan of
    the source) to re-derive information the caller already has.

    bad_df already carries a specific `_quarantine_reason` per row from
    split_good_bad() - this only adds `_quarantine_id` (a stable UUID),
    which is the anchor quarantine replay (#60) uses to identify exactly
    which rows were successfully re-promoted to bronze, so they can be
    removed from quarantine precisely rather than by a best-effort content
    match.
    """
    if bad_df is None or bad_count == 0:
        return

    schema_ref = f"{config.catalog}.{config.schema_name}" if config.catalog else config.schema_name
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema_ref}")

    (
        bad_df.withColumn("_quarantine_id", expr("uuid()"))
        .write.format("delta")
        .mode("append")
        .option("mergeSchema", "true")
        .saveAsTable(config.resolved_quarantine_table)
    )
