"""
Reads nested JSON from any Spark-readable location.

Because Spark's DataFrameReader works off a URI scheme, this module doesn't
need special-case logic per cloud provider - abfss://, s3://, s3a://, gs://,
dbfs:/, /Volumes/..., and local file:/ paths all go through the same code
path as long as the cluster/session has the right auth configured. This
function just centralizes the read options driven by config.
"""

from .config import IngestionConfig


def read_json(spark, config: IngestionConfig):
    """
    Batch read of nested JSON from config.source_path into a DataFrame.

    Production behavior:
      - mode=PERMISSIVE (Spark default) so unparseable records don't kill the
        whole job; they land in `config.corrupt_record_column` instead of
        being silently dropped or failing the read.
      - When a schema_hint_ddl is supplied, `rescued_data_column` captures
        any fields present in the source JSON that don't fit that schema
        (extra/renamed fields), so nothing is silently lost on drift.
      - Adds `_input_file_name` for lineage, used later for the audit
        `_source_file` column.
    """
    reader = (
        spark.read.format("json")
        .option("multiLine", config.multiline)
        .option("mode", "PERMISSIVE")
        .option("columnNameOfCorruptRecord", config.corrupt_record_column)
    )

    if config.schema_hint_ddl:
        # rescuedDataColumn only has an effect when an explicit schema is provided -
        # otherwise Spark infers a schema that already includes every field.
        reader = reader.option("rescuedDataColumn", config.rescued_data_column)

    for key, value in (config.reader_options or {}).items():
        reader = reader.option(key, value)

    if config.schema_hint_ddl:
        reader = reader.schema(config.schema_hint_ddl)

    from pyspark.sql.functions import col

    # Track provenance regardless of flatten mode - cheap and always useful in bronze.
    # Uses _metadata.file_path (works on Unity Catalog shared clusters,
    # unlike input_file_name()).
    df = (
        reader.load(config.source_path)
            .select("*", col("_metadata.file_path").alias("_input_file_name"))
    )

    return df
