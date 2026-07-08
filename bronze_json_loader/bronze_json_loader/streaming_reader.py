"""
Incremental ingestion via Databricks Auto Loader (cloudFiles).

This is the production-recommended path for anything beyond a one-off
backfill: Auto Loader tracks which files have already been processed
(via the checkpoint), handles new files landing continuously or on a
schedule, and evolves the schema safely instead of re-scanning the whole
source directory on every run.
"""

from .config import IngestionConfig


def read_json_stream(spark, config: IngestionConfig):
    """
    Returns a streaming DataFrame reading new JSON files from
    config.source_path using Auto Loader.
    """
    reader = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", config.schema_location)
        .option("cloudFiles.schemaEvolutionMode", config.schema_evolution_mode)
        .option("multiLine", config.multiline)
        .option("rescuedDataColumn", config.rescued_data_column)
    )

    if config.max_files_per_trigger:
        reader = reader.option("cloudFiles.maxFilesPerTrigger", config.max_files_per_trigger)

    if config.schema_hint_ddl:
        reader = reader.schema(config.schema_hint_ddl)

    for key, value in (config.reader_options or {}).items():
        reader = reader.option(key, value)

    df = reader.load(config.source_path)

    from pyspark.sql.functions import input_file_name
    df = df.withColumn("_input_file_name", input_file_name())

    return df


def get_trigger_kwargs(config: IngestionConfig) -> dict:
    """Translates config.trigger_mode into the kwargs writeStream.trigger() expects."""
    if config.trigger_mode == "availableNow":
        return {"availableNow": True}
    if config.trigger_mode == "once":
        return {"once": True}
    if config.trigger_mode == "processingTime":
        return {"processingTime": config.trigger_processing_time}
    raise ValueError(f"Unknown trigger_mode: {config.trigger_mode}")
