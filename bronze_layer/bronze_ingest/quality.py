"""
Lightweight data-quality gate applied right before the bronze write.

Checks that configured `required_columns` are non-null. Depending on
`fail_on_quality_error`:
  - True  -> raise DataQualityError if any bad rows are found (fail the job)
  - False -> split the DataFrame into (good_df, bad_df); bad_df is written
             to a quarantine table so the batch can still succeed and be
             re-processed later once the source data is fixed.
"""

from typing import Tuple, List, Optional

from pyspark.sql.functions import col, lit

from .config import IngestionConfig
from .logging_utils import logger


class DataQualityError(Exception):
    """
    Carries bad_count (when known) so a failed audited_run can record how
    many rows failed the quality gate, instead of leaving
    quarantined_row_count None on the failure audit row (#50).
    """

    def __init__(self, message: str, bad_count: Optional[int] = None):
        super().__init__(message)
        self.bad_count = bad_count


def _missing_required_columns(df, required_columns: List[str]) -> List[str]:
    return [c for c in required_columns if c not in df.columns]


def split_good_bad(df, config: IngestionConfig) -> Tuple[object, object]:
    """
    Returns (good_df, bad_df). bad_df is empty (0 rows, same schema) if
    there are no required_columns configured or no violations found.

    The bad-row condition is computed once into a _dq_bad tag column, and
    both good_df/bad_df are derived from that single tagged DataFrame,
    instead of each independently rebuilding the same null-check OR
    expression via its own filter() call.
    """
    if not config.required_columns:
        return df, df.limit(0)

    missing = _missing_required_columns(df, config.required_columns)
    if missing:
        # Columns that don't exist at all are a schema problem, not a per-row
        # quality problem - always a hard failure regardless of fail_on_quality_error.
        raise DataQualityError(
            f"required_columns {missing} not present in source schema. "
            f"Available columns: {df.columns}"
        )

    bad_condition = None
    for c in config.required_columns:
        cond = col(f"`{c}`").isNull()
        bad_condition = cond if bad_condition is None else (bad_condition | cond)

    tagged = df.withColumn("_dq_bad", bad_condition)
    good_df = tagged.filter(~col("_dq_bad")).drop("_dq_bad")
    bad_df = tagged.filter(col("_dq_bad")).drop("_dq_bad")
    return good_df, bad_df


def enforce_quality(df, config: IngestionConfig):
    """
    Applies the quality gate and returns (good_df, bad_df, bad_count).
    Raises DataQualityError if fail_on_quality_error=True and bad rows exist.
    """
    good_df, bad_df = split_good_bad(df, config)
    bad_count = bad_df.count() if config.required_columns else 0

    if bad_count > 0:
        msg = (
            f"{bad_count} row(s) failed data quality checks "
            f"(null in one of required_columns={config.required_columns})"
        )
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
    """
    if bad_df is None or bad_count == 0:
        return

    schema_ref = f"{config.catalog}.{config.schema_name}" if config.catalog else config.schema_name
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema_ref}")

    quarantine_reason = lit("required_column_null")
    (
        bad_df.withColumn("_quarantine_reason", quarantine_reason)
        .write.format("delta")
        .mode("append")
        .option("mergeSchema", "true")
        .saveAsTable(config.resolved_quarantine_table)
    )
