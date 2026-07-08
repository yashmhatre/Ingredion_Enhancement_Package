"""
Top-level orchestrator: BronzeIngestion.

This is the single entry point most users need. It wires together:
  json_reader.read_json -> flattener.apply_flatten_mode -> bronze_writer.write_bronze
"""

from typing import Optional, Dict, Any

from .config import IngestionConfig
from .json_reader import read_json
from .streaming_reader import read_json_stream, get_trigger_kwargs
from .flattener import apply_flatten_mode
from .bronze_writer import add_audit_columns, write_bronze, write_bronze_micro_batch
from .quality import enforce_quality, write_quarantine
from .logging_utils import logger


class BronzeIngestion:
    def __init__(self, spark, config: IngestionConfig):
        self.spark = spark
        self.config = config

    # ---- convenience constructors ----
    @classmethod
    def from_dict(cls, spark, config_dict: Dict[str, Any]) -> "BronzeIngestion":
        return cls(spark, IngestionConfig.from_dict(config_dict))

    @classmethod
    def from_yaml(cls, spark, path: str) -> "BronzeIngestion":
        return cls(spark, IngestionConfig.from_yaml(path))

    @classmethod
    def from_json(cls, spark, path: str) -> "BronzeIngestion":
        return cls(spark, IngestionConfig.from_json(path))

    @classmethod
    def from_config_file(cls, spark, path: str) -> "BronzeIngestion":
        return cls(spark, IngestionConfig.load(path))

    # ---- core run ----
    def read(self):
        return read_json(self.spark, self.config)

    def transform(self, df):
        df = apply_flatten_mode(df, self.config)
        df = add_audit_columns(df, self.config)
        return df

    def run(self) -> Dict[str, Any]:
        """
        Executes the full read -> transform -> quality-gate -> write pipeline
        in batch mode. Returns a summary dict. Raises DataQualityError if
        required_columns validation fails and fail_on_quality_error=True.
        """
        if self.config.ingestion_mode != "batch":
            raise ValueError("run() is for ingestion_mode='batch'. Use run_streaming() for streaming.")

        logger.info("Starting batch ingestion from %s -> %s", self.config.source_path, self.config.full_table_name)

        raw_df = self.read()
        transformed_df = apply_flatten_mode(raw_df, self.config)

        good_df, bad_df, bad_count = enforce_quality(transformed_df, self.config)
        final_df = add_audit_columns(good_df, self.config)

        if bad_count > 0:
            write_quarantine(self.spark, add_audit_columns(bad_df, self.config), self.config)

        table_name = write_bronze(self.spark, final_df, self.config)
        row_count = final_df.count()

        logger.info("Wrote %d row(s) to %s (%d quarantined)", row_count, table_name, bad_count)

        return {
            "table": table_name,
            "row_count": row_count,
            "quarantined_row_count": bad_count,
            "quarantine_table": self.config.resolved_quarantine_table if bad_count > 0 else None,
            "columns": final_df.columns,
            "write_mode": self.config.write_mode,
            "flatten_mode": self.config.flatten_mode,
        }

    def run_streaming(self, await_termination: bool = True):
        """
        Executes incremental ingestion using Auto Loader. New/changed files
        under source_path are picked up automatically via the checkpoint at
        config.checkpoint_location; each micro-batch goes through the same
        flatten -> quality -> audit -> write logic as batch mode.

        Returns the StreamingQuery. If await_termination=True (default),
        blocks until the stream finishes (e.g. under trigger_mode
        "availableNow"/"once" this drains the backlog then returns - the
        right behavior for a scheduled Databricks Job). Set to False if you
        want a continuously running stream (trigger_mode="processingTime")
        and intend to manage the query lifecycle yourself.
        """
        if self.config.ingestion_mode != "streaming":
            raise ValueError("run_streaming() is for ingestion_mode='streaming'. Use run() for batch.")

        logger.info(
            "Starting streaming ingestion from %s -> %s (checkpoint=%s)",
            self.config.source_path, self.config.full_table_name, self.config.checkpoint_location,
        )

        stream_df = read_json_stream(self.spark, self.config)

        def _process_batch(micro_batch_df, batch_id):
            transformed_df = apply_flatten_mode(micro_batch_df, self.config)
            good_df, bad_df, bad_count = enforce_quality(transformed_df, self.config)
            final_df = add_audit_columns(good_df, self.config)

            if bad_count > 0:
                write_quarantine(self.spark, add_audit_columns(bad_df, self.config), self.config)

            write_bronze_micro_batch(self.spark, final_df, batch_id, self.config)

        query = (
            stream_df.writeStream
            .foreachBatch(_process_batch)
            .option("checkpointLocation", self.config.checkpoint_location)
            .trigger(**get_trigger_kwargs(self.config))
            .start()
        )

        if await_termination:
            query.awaitTermination()

        return query


def ingest_json_to_bronze(spark, config: Optional[Dict[str, Any]] = None, config_path: Optional[str] = None, **kwargs) -> Dict[str, Any]:
    """
    One-shot convenience function for the simplest plug-and-play usage:

        from bronze_json_loader import ingest_json_to_bronze
        ingest_json_to_bronze(
            spark,
            source_path="abfss://raw@mystorage.dfs.core.windows.net/orders/",
            schema_name="bronze",
            table="orders_raw",
            flatten_mode="flatten",
        )

    You can also pass a dict via `config=`, or a path to a .yaml/.json file
    via `config_path=`. kwargs override whatever is in config/config_path.
    """
    if config_path:
        cfg = IngestionConfig.load(config_path)
        if kwargs:
            merged = cfg.to_dict()
            merged.update(kwargs)
            cfg = IngestionConfig.from_dict(merged)
    elif config:
        merged = dict(config)
        merged.update(kwargs)
        cfg = IngestionConfig.from_dict(merged)
    else:
        cfg = IngestionConfig.from_dict(kwargs)

    job = BronzeIngestion(spark, cfg)
    if cfg.ingestion_mode == "streaming":
        return job.run_streaming()
    return job.run()
