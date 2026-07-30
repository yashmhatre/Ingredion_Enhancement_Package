"""
Top-level orchestrator: BronzeIngestion.

This is the single entry point most users need. It wires together:
  json_reader.read_json -> add audit columns -> bronze_writer.write_bronze
"""

from typing import Optional, Dict, Any

from .config import IngestionConfig
from .json_reader import read_json
from .streaming_reader import read_json_stream, get_trigger_kwargs, assert_no_silent_truncation
from .bronze_writer import add_audit_columns, write_bronze, write_bronze_micro_batch
from .quality import enforce_quality, write_quarantine
from .logging_utils import logger
from .audit import audited_run, tag_failure_stage
from .schema_registry import record_schema
from .catalog_metadata import apply_catalog_metadata


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
        df = add_audit_columns(df, self.config)
        return df

    def run_on_dataframe(self, raw_df) -> Dict[str, Any]:
        """
        Same as run(), but skips the read step and uses raw_df directly -
        used by directory ingestion's folder-as-table path, where files
        inside a folder are read and unioned individually beforehand
        (so one bad file doesn't break the whole folder's read), rather
        than letting this method read config.source_path itself.
        """
        with audited_run(self.spark, self.config, source_path=self.config.source_path) as audit:
            logger.info(
                "Starting batch ingestion from pre-loaded DataFrame -> %s",
                self.config.full_table_name,
            )

            try:
                good_df, bad_df, bad_count = enforce_quality(raw_df, self.config)
            except Exception as exc:
                tag_failure_stage(exc, "quality")
                raise
            final_df = add_audit_columns(good_df, self.config)

            write_quarantine(self.spark, add_audit_columns(bad_df, self.config), bad_count, self.config)

            try:
                table_name = write_bronze(self.spark, final_df, self.config)
                row_count = final_df.count()
            except Exception as exc:
                tag_failure_stage(exc, "write")
                raise

            audit["row_count"] = row_count
            audit["quarantined_row_count"] = bad_count
            fingerprint, schema_changed = record_schema(self.spark, self.config, final_df)
            audit["schema_fingerprint"] = fingerprint
            audit["schema_changed"] = schema_changed
            apply_catalog_metadata(self.spark, self.config)
            logger.info("Wrote %d row(s) to %s (%d quarantined)", row_count, table_name, bad_count)

            return {
                "table": table_name,
                "row_count": row_count,
                "quarantined_row_count": bad_count,
                "quarantine_table": self.config.resolved_quarantine_table if bad_count > 0 else None,
                "columns": final_df.columns,
                "write_mode": self.config.write_mode,
            }

    def run(self) -> Dict[str, Any]:
        """
        Executes the full read -> transform -> quality-gate -> write pipeline
        in batch mode. Returns a summary dict. Raises DataQualityError if
        required_columns validation fails and fail_on_quality_error=True.
        """
        if self.config.ingestion_mode != "batch":
            raise ValueError("run() is for ingestion_mode='batch'. Use run_streaming() for streaming.")

        with audited_run(self.spark, self.config, source_path=self.config.source_path) as audit:
            logger.info("Starting batch ingestion from %s -> %s", self.config.source_path, self.config.full_table_name)

            try:
                raw_df = self.read()
            except Exception as exc:
                tag_failure_stage(exc, "read")
                raise

            try:
                good_df, bad_df, bad_count = enforce_quality(raw_df, self.config)
            except Exception as exc:
                tag_failure_stage(exc, "quality")
                raise
            final_df = add_audit_columns(good_df, self.config)

            write_quarantine(self.spark, add_audit_columns(bad_df, self.config), bad_count, self.config)

            try:
                table_name = write_bronze(self.spark, final_df, self.config)
                row_count = final_df.count()
            except Exception as exc:
                tag_failure_stage(exc, "write")
                raise

            audit["row_count"] = row_count
            audit["quarantined_row_count"] = bad_count
            fingerprint, schema_changed = record_schema(self.spark, self.config, final_df)
            audit["schema_fingerprint"] = fingerprint
            audit["schema_changed"] = schema_changed
            apply_catalog_metadata(self.spark, self.config)
            logger.info("Wrote %d row(s) to %s (%d quarantined)", row_count, table_name, bad_count)

            return {
                "table": table_name,
                "row_count": row_count,
                "quarantined_row_count": bad_count,
                "quarantine_table": self.config.resolved_quarantine_table if bad_count > 0 else None,
                "columns": final_df.columns,
                "write_mode": self.config.write_mode,
            }

    def run_streaming(self, await_termination: bool = True):
        """
        Executes incremental ingestion using Auto Loader. New/changed files
        under source_path are picked up automatically via the checkpoint at
        config.checkpoint_location; each micro-batch goes through the same
        quality -> audit -> write logic as batch mode.

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
            with audited_run(self.spark, self.config, source_path=self.config.source_path) as audit:
                try:
                    # Before anything reads the data: a JSON-lines file read
                    # with multiLine=true has already lost all but its first
                    # record (#146). Raising here leaves the batch
                    # uncommitted, so the checkpoint does not advance and the
                    # files are re-read once the config is fixed. Tagged as a
                    # read failure because that is the stage that went wrong.
                    assert_no_silent_truncation(micro_batch_df, self.config)
                except Exception as exc:
                    tag_failure_stage(exc, "read")
                    raise

                try:
                    good_df, bad_df, bad_count = enforce_quality(micro_batch_df, self.config)
                except Exception as exc:
                    tag_failure_stage(exc, "quality")
                    raise
                final_df = add_audit_columns(good_df, self.config)

                write_quarantine(self.spark, add_audit_columns(bad_df, self.config), bad_count, self.config)

                try:
                    write_bronze_micro_batch(self.spark, final_df, batch_id, self.config)
                    row_count = final_df.count()
                except Exception as exc:
                    tag_failure_stage(exc, "write")
                    raise

                audit["row_count"] = row_count
                audit["quarantined_row_count"] = bad_count
                fingerprint, schema_changed = record_schema(self.spark, self.config, final_df)
                audit["schema_fingerprint"] = fingerprint
                audit["schema_changed"] = schema_changed
                apply_catalog_metadata(self.spark, self.config)

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

        from bronze_ingest import ingest_json_to_bronze
        ingest_json_to_bronze(
            spark,
            source_path="abfss://raw@mystorage.dfs.core.windows.net/orders/",
            schema_name="bronze",
            table="orders_raw",
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
