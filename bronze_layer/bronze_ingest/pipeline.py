"""
Top-level orchestrator: BronzeIngestion.

This is the single entry point most users need. It wires together:
  json_reader.read_json -> add audit columns -> bronze_writer.write_bronze
"""

from typing import Any, Dict, Optional

from .audit import audited_run, tag_failure_stage
from .bronze_writer import (
    add_audit_columns,
    read_write_metrics,
    resolve_batch_id,
    write_bronze,
    write_bronze_micro_batch,
)
from .catalog_metadata import apply_catalog_metadata
from .config import IngestionConfig
from .json_reader import read_json
from .logging_utils import logger
from .quality import enforce_quality, write_quarantine
from .schema_registry import record_schema
from .streaming_reader import assert_no_silent_truncation, get_trigger_kwargs, read_json_stream


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

    # ---- the one orchestration body ----

    def _execute(
        self,
        read_fn,
        writer,
        start_message,
        *,
        build_summary=True,
        stream_batch_id=None,
        record_metadata=True,
    ):
        """
        The single ingestion sequence, shared by all three entry points:
        read -> quality gate -> audit columns -> quarantine -> write ->
        audit row / schema registry / catalog metadata.

        This existed three times, byte-identical apart from two axes (#150).
        Four open issues each wanted to change these ~40 lines, which meant
        each fix landing in three places - and a fix that lands in two of
        three is indistinguishable from a fix that landed, until the third
        path runs.

        read_fn: a ZERO-ARGUMENT CALLABLE, not a DataFrame. This is the one
            place a naive extraction silently loses behaviour. `run()`
            deliberately performs its read INSIDE the audited_run block, so
            that a read failure is tagged failure_stage="read" and still
            produces an audit row. Passing an already-materialised DataFrame
            would move the read outside the block and a failing read would
            vanish from the audit trail entirely. Covered by a regression
            test.

        writer: callable (df) -> table_name or None. A closure rather than
            the (spark, df, config) + functools.partial shape #150 sketched:
            every caller is a method with self already in scope, so the
            closure carries what it needs without threading three arguments
            through for the benefit of one parameter that varies.
            write_bronze_micro_batch returns None (and returns early on an
            empty batch), hence the fallback when logging.

        build_summary: streaming's foreachBatch handler must return None.

        stream_batch_id: Structured Streaming's micro-batch id, recorded on
            the audit row so streaming rows are individually addressable
            (#156). None on the batch paths.

        record_metadata: whether to record the schema fingerprint and apply
            catalog comments. False for streaming micro-batches, where both
            are per-STREAM concerns that were being re-executed per batch -
            2,880 times a day on a 30-second trigger (#156). run_streaming
            does them once at stream start instead.
        """
        with audited_run(
            self.spark,
            self.config,
            source_path=self.config.source_path,
            stream_batch_id=stream_batch_id,
        ) as audit:
            logger.info(start_message, self.config.full_table_name)

            try:
                raw_df = read_fn()
            except Exception as exc:
                tag_failure_stage(exc, "read")
                raise

            try:
                good_df, bad_df, bad_count = enforce_quality(raw_df, self.config)
            except Exception as exc:
                tag_failure_stage(exc, "quality")
                raise

            # Resolved ONCE and passed to both calls (#148). Deriving it
            # inside add_audit_columns gave the good rows and the bad rows
            # different _batch_id values whenever config.batch_id was unset,
            # which silently broke reprocess_quarantine(batch_id=...).
            batch_id = resolve_batch_id(self.config)
            final_df = add_audit_columns(good_df, self.config, batch_id=batch_id)

            write_quarantine(
                self.spark,
                add_audit_columns(bad_df, self.config, batch_id=batch_id),
                bad_count,
                self.config,
            )

            try:
                table_name = writer(final_df)
            except Exception as exc:
                tag_failure_stage(exc, "write")
                raise

            # From Delta's transaction log, not a recount of final_df (#149).
            # The old `final_df.count()` re-read the source and re-ran the
            # quality gate to produce a number Delta already had, and under
            # merge it was the wrong number anyway.
            metrics = read_write_metrics(
                self.spark, table_name or self.config.full_table_name, self.config.write_mode
            )
            audit.update(metrics)
            audit["quarantined_row_count"] = bad_count
            row_count = metrics["row_count"]

            if record_metadata:
                fingerprint, schema_changed = record_schema(self.spark, self.config, final_df)
                audit["schema_fingerprint"] = fingerprint
                audit["schema_changed"] = schema_changed
                apply_catalog_metadata(self.spark, self.config)

            logger.info(
                "Wrote %s row(s) to %s (%d quarantined)",
                "?" if row_count is None else row_count,
                table_name or self.config.full_table_name,
                bad_count,
            )

            if not build_summary:
                return None

            return {
                "table": table_name,
                "row_count": row_count,
                "quarantined_row_count": bad_count,
                "quarantine_table": self.config.resolved_quarantine_table
                if bad_count > 0
                else None,
                "columns": final_df.columns,
                "write_mode": self.config.write_mode,
            }

    def run_on_dataframe(self, raw_df) -> Dict[str, Any]:
        """
        Same as run(), but skips the read step and uses raw_df directly -
        used by directory ingestion's folder-as-table path, where files
        inside a folder are read and unioned individually beforehand
        (so one bad file doesn't break the whole folder's read), rather
        than letting this method read config.source_path itself.
        """
        return self._execute(
            lambda: raw_df,
            lambda df: write_bronze(self.spark, df, self.config),
            "Starting batch ingestion from pre-loaded DataFrame -> %s",
        )

    def run(self) -> Dict[str, Any]:
        """
        Executes the full read -> transform -> quality-gate -> write pipeline
        in batch mode. Returns a summary dict. Raises DataQualityError if
        required_columns validation fails and fail_on_quality_error=True.
        """
        if self.config.ingestion_mode != "batch":
            raise ValueError(
                "run() is for ingestion_mode='batch'. Use run_streaming() for streaming."
            )

        # self.read, not self.read() - see the read_fn note on _execute.
        return self._execute(
            self.read,
            lambda df: write_bronze(self.spark, df, self.config),
            f"Starting batch ingestion from {self.config.source_path} -> %s",
        )

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
            raise ValueError(
                "run_streaming() is for ingestion_mode='streaming'. Use run() for batch."
            )

        logger.info(
            "Starting streaming ingestion from %s -> %s (checkpoint=%s)",
            self.config.source_path,
            self.config.full_table_name,
            self.config.checkpoint_location,
        )

        stream_df = read_json_stream(self.spark, self.config)

        # Per-STREAM, not per-micro-batch (#156).
        #
        # Both of these ran inside the foreachBatch handler, so under
        # `trigger_mode: processingTime` with "30 seconds" they executed 2,880
        # times a day. record_schema is documented as cheap per call - "one
        # small read and zero writes" - which is true, and 2,880 reads a day
        # of a table whose schema changes approximately never is still waste.
        # apply_catalog_metadata runs DESCRIBE TABLE EXTENDED plus
        # listColumns whenever comments are configured.
        #
        # Once per stream is sufficient because Auto Loader RESTARTS the
        # stream on a schema change under schemaEvolutionMode=addNewColumns -
        # so a within-stream drift check has no work the restart does not
        # already signal. If a case is ever found where a micro-batch schema
        # can differ from the stream schema without a restart, this is the
        # decision to revisit.
        #
        # Deliberately not fatal: a metadata failure must not stop a stream
        # that is otherwise writing correctly, matching the never-raise
        # contract these modules already have.
        record_schema(self.spark, self.config, stream_df)
        apply_catalog_metadata(self.spark, self.config)

        def _process_batch(micro_batch_df, batch_id):
            # #174's truncation guard runs as part of "reading" this
            # micro-batch, rather than as a separate step before the shared
            # body. That placement is what preserves both of its properties
            # for free: it stays INSIDE audited_run, and _execute's own read
            # try/except tags it failure_stage="read" - which is what it was
            # tagged as before the consolidation. No extra parameter on
            # _execute, and the guard stays where it belongs conceptually,
            # since it is an Auto Loader concern and not a batch one.
            def _accept_micro_batch():
                assert_no_silent_truncation(micro_batch_df, self.config)
                return micro_batch_df

            # build_summary=False: foreachBatch's handler must return None.
            # record_metadata=False and stream_batch_id: see #156, above.
            self._execute(
                _accept_micro_batch,
                lambda df: write_bronze_micro_batch(self.spark, df, batch_id, self.config),
                f"Processing micro-batch {batch_id} -> %s",
                build_summary=False,
                stream_batch_id=batch_id,
                record_metadata=False,
            )

        query = (
            stream_df.writeStream.foreachBatch(_process_batch)
            .option("checkpointLocation", self.config.checkpoint_location)
            .trigger(**get_trigger_kwargs(self.config))
            .start()
        )

        if await_termination:
            query.awaitTermination()

        return query


def ingest_json_to_bronze(
    spark, config: Optional[Dict[str, Any]] = None, config_path: Optional[str] = None, **kwargs
) -> Dict[str, Any]:
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
    Passing both `config` and `config_path` raises rather than silently
    ignoring one of them - see IngestionConfig.resolve.
    """
    cfg = IngestionConfig.resolve(config=config, config_path=config_path, **kwargs)

    job = BronzeIngestion(spark, cfg)
    if cfg.ingestion_mode == "streaming":
        return job.run_streaming()
    return job.run()
