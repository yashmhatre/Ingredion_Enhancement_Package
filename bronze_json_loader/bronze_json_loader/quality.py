"""
Lightweight data-quality gate applied right before the bronze write.

Checks that configured `required_columns` are non-null. Depending on
`fail_on_quality_error`:
  - True  -> raise DataQualityError if any bad rows are found (fail the job)
  - False -> split the DataFrame into (good_df, bad_df); bad_df is written
             to a quarantine table so the batch can still succeed and be
             re-processed later once the source data is fixed.
"""

from typing import Tuple, List

from pyspark.sql.functions import col, lit

from .config import IngestionConfig
from .logging_utils import logger


class DataQualityError(Exception):
    pass


def _missing_required_columns(df, required_columns: List[str]) -> List[str]:
    return [c for c in required_columns if c not in df.columns]


def split_good_bad(df, config: IngestionConfig) -> Tuple[object, object]:
    """
    Returns (good_df, bad_df). bad_df is empty (0 rows, same schema) if
    there are no required_columns configured or no violations found.
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

    bad_df = df.filter(bad_condition)
    good_df = df.filter(~bad_condition)
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
            raise DataQualityError(msg)
        logger.warning("%s - quarantining to %s", msg, config.resolved_quarantine_table)

    return good_df, bad_df, bad_count


def write_quarantine(spark, bad_df, config: IngestionConfig):
    if bad_df is None:
        return
    if bad_df.rdd.isEmpty():
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
