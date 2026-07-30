"""
Writes a prepared DataFrame into a Delta bronze table, adding standard audit
columns and supporting append / overwrite / merge semantics with optional
schema evolution.
"""

from datetime import datetime, timezone

from pyspark.sql.functions import lit, current_timestamp, col, row_number
from pyspark.sql.window import Window

from .config import IngestionConfig
from .retry import with_retry
from .logging_utils import logger
from .sql_utils import row_content_hash, quote_literal


def add_audit_columns(df, config: IngestionConfig):
    if not config.add_audit_columns:
        return df

    batch_id = config.batch_id or datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")

    df = df.withColumn(config.audit_ingest_ts_col, current_timestamp())
    df = df.withColumn(config.audit_batch_id_col, lit(batch_id))

    if "_input_file_name" in df.columns:
        df = df.withColumnRenamed("_input_file_name", config.audit_source_file_col)
    else:
        # No per-file lineage on this DataFrame - e.g. a run_on_dataframe()
        # caller that didn't pre-attach _input_file_name. Falling back to
        # NULL would silently defeat the point of _source_file, so use the
        # coarse-but-truthful config.source_path instead and flag the gap.
        logger.warning(
            "No _input_file_name column found - %s will be set to config.source_path "
            "(%r) instead of per-file lineage for every row.",
            config.audit_source_file_col, config.source_path,
        )
        df = df.withColumn(config.audit_source_file_col, lit(config.source_path))

    return df


class NullMergeKeyError(Exception):
    pass


def _assert_no_null_merge_keys(df, merge_keys):
    """
    NULL = NULL evaluates to NULL (not true) in a SQL MERGE condition, so a
    row with a NULL merge key never matches the target and gets inserted as
    a new row on every run - silently duplicating forever. Config validation
    (merge_keys must be a subset of required_columns) should already prevent
    this via the quality gate, but this is a cheap last-line-of-defense
    check directly on the DataFrame about to be merged.
    """
    null_condition = None
    for k in merge_keys:
        cond = col(f"`{k}`").isNull()
        null_condition = cond if null_condition is None else (null_condition | cond)

    if df.filter(null_condition).take(1):
        raise NullMergeKeyError(
            f"Refusing to MERGE: found NULL value(s) in merge_keys={merge_keys}. "
            "These rows would never match the target and would be inserted as "
            "duplicates on every run. Add these columns to required_columns so "
            "the quality gate filters/fails on them before the write."
        )


class DuplicateMergeKeyError(Exception):
    pass


def _dedupe_for_merge(df, config: IngestionConfig):
    """
    Delta MERGE raises "Cannot perform Merge as multiple source rows
    matched..." when the source has more than one row per merge key.
    Bronze sources frequently re-send full-file dumps or contain
    intra-batch duplicates, so deterministically keep one row per key -
    the one with the highest dedupe_order_by value (defaults to the
    ingestion timestamp, so the most-recently-ingested row wins).

    "Deterministically" needs the content-hash tie-break to be true (#147).
    The default order column is the ingestion timestamp, which
    add_audit_columns() sets from `current_timestamp()` - identical for
    every row in the batch, so on the default path EVERY row is a tie and
    the ordering was decided entirely by partition layout. Delta's MERGE can
    re-evaluate the source plan, so which duplicate won was not stable.

    The tie-break makes the choice a function of row content. It does not
    make the *hash* stable when a column is itself nondeterministic
    (`current_timestamp()` re-evaluates across separate actions) - that is
    upstream of here and is what #63's idempotency addresses. What it does
    remove is the dependence on how Spark happened to partition the data.
    """
    order_col = config.dedupe_order_by or config.audit_ingest_ts_col
    if order_col not in df.columns:
        raise ValueError(
            f"dedupe_before_merge=True but the order-by column {order_col!r} is not "
            "present on the DataFrame being merged. Pass dedupe_order_by explicitly, "
            "or leave add_audit_columns=True so the default (audit_ingest_ts_col) exists."
        )

    w = Window.partitionBy(*config.merge_keys).orderBy(
        col(f"`{order_col}`").desc(), row_content_hash(df).asc()
    )
    return (
        df.withColumn("_dedupe_rn", row_number().over(w))
        .filter(col("_dedupe_rn") == 1)
        .drop("_dedupe_rn")
    )


def _assert_no_duplicate_merge_keys(df, merge_keys):
    dup_keys = (
        df.groupBy(*[col(f"`{k}`") for k in merge_keys])
        .count()
        .filter(col("count") > 1)
        .drop("count")
        .limit(20)
        .collect()
    )
    if dup_keys:
        raise DuplicateMergeKeyError(
            f"Refusing to MERGE: found duplicate merge_keys={merge_keys} within the "
            f"source batch (Delta MERGE would raise 'multiple source rows matched'). "
            f"Example duplicated key(s): {[r.asDict() for r in dup_keys]}. Set "
            "dedupe_before_merge=True (default) to auto-dedupe instead of failing."
        )


def _describe_current_layout(spark, full_name):
    """
    Returns (clustering_columns, properties) for an existing table, or
    (None, {}) if it doesn't exist yet or can't be read (never raises -
    layout introspection failing must not fail the ingestion run).
    """
    try:
        row = (
            spark.sql(f"DESCRIBE DETAIL {full_name}")
            .select("clusteringColumns", "properties")
            .collect()[0]
        )
        return row["clusteringColumns"], (row["properties"] or {})
    except Exception:
        return None, {}


def _ensure_liquid_clustering_and_properties(spark, df, config: IngestionConfig, full_name: str):
    """
    Applies cluster_by/cluster_by_auto/table_properties (#57). No-ops
    entirely when none of the three are configured, so existing
    partition_by-based tables are completely unaffected.

    DataFrameWriter's own .clusterBy() doesn't reliably map onto Delta's
    V2 catalog write path in practice (raises DELTA_OPERATION_NOT_ALLOWED
    when tried against this package's supported delta-spark versions), so
    this goes through DeltaTableBuilder for creation and raw ALTER TABLE
    statements to apply/restore settings on an existing table instead.

    Also works around a real Delta quirk: an unqualified `mode("overwrite")
    .saveAsTable(...)` performs an implicit REPLACE TABLE that silently
    drops CLUSTER BY unless re-specified on that same write call (which
    the broken .clusterBy() writer path can't do either) - the overwrite
    branch in _write_core calls this again immediately after its write to
    restore it, rather than relying on it surviving the write.
    """
    if not (config.cluster_by or config.cluster_by_auto or config.table_properties):
        return

    from delta.tables import DeltaTable

    if not spark.catalog.tableExists(full_name):
        creator = DeltaTable.createIfNotExists(spark).tableName(full_name).addColumns(df.schema)
        if config.cluster_by:
            creator = creator.clusterBy(*config.cluster_by)
        elif config.partition_by:
            creator = creator.partitionedBy(*config.partition_by)
        for key, value in (config.table_properties or {}).items():
            creator = creator.property(key, value)
        creator.execute()

    current_cluster_cols, current_props = _describe_current_layout(spark, full_name)

    if config.cluster_by and current_cluster_cols != list(config.cluster_by):
        cols = ", ".join(f"`{c}`" for c in config.cluster_by)
        spark.sql(f"ALTER TABLE {full_name} CLUSTER BY ({cols})")
        if current_cluster_cols is not None:
            logger.warning(
                "Cluster-by columns changed for %s: %s -> %s",
                full_name, current_cluster_cols, config.cluster_by,
            )

    if config.cluster_by_auto:
        try:
            spark.sql(f"ALTER TABLE {full_name} CLUSTER BY AUTO")
        except Exception as exc:
            logger.warning(
                "cluster_by_auto=True but this engine doesn't support CLUSTER BY AUTO "
                "(expected outside Databricks Runtime, which manages predictive "
                "optimization for AUTO-clustered tables): %s", exc,
            )

    changed_props = {
        k: v for k, v in (config.table_properties or {}).items()
        if current_props.get(k) != v
    }
    if changed_props:
        # Both sides escaped (#154). `table_properties` is a free-form
        # Dict[str, str] straight from YAML, and both key and value landed in
        # single-quoted SQL literals raw: a value containing an apostrophe
        # broke the statement, and a crafted one appended arbitrary DDL to it.
        # The keys are additionally validated at config load, per
        # dot-separated part, since they are dotted by convention
        # (delta.enableChangeDataFeed).
        props_clause = ", ".join(
            f"'{quote_literal(k)}' = '{quote_literal(v)}'"
            for k, v in changed_props.items()
        )
        spark.sql(f"ALTER TABLE {full_name} SET TBLPROPERTIES ({props_clause})")
        logger.warning("Table properties changed for %s: %s", full_name, changed_props)


def _resolve_idempotent_txn_version(config: IngestionConfig):
    """
    Derives a numeric Delta txnVersion from config.batch_id for the batch
    write path's idempotent-write options (#63) - mirrors the mechanism
    write_bronze_micro_batch already uses for streaming.

    Returns None (meaning: skip idempotent protection for this write) when
    a *stable* version can't be derived:
      - batch_id is None. An auto-generated batch_id (see add_audit_columns)
        is a fresh timestamp on every call, including every retry attempt -
        it cannot provide retry protection no matter how it's converted,
        since txnVersion would then also differ on every attempt just like
        the value it's derived from. Only an explicitly-set, externally
        stable batch_id (e.g. a Databricks job run ID via #52) can make
        this guarantee real.
      - batch_id is an arbitrary string that's neither an integer nor the
        package's own auto-generated timestamp format.

    batch_id that parses as an integer (e.g. a job run ID) is used
    directly. A string in the auto-generated format (%Y%m%dT%H%M%S%fZ) -
    e.g. if a caller explicitly passes one - converts to a stable
    microsecond-epoch integer.
    """
    if config.batch_id is None:
        return None

    try:
        return int(config.batch_id)
    except (TypeError, ValueError):
        pass

    try:
        dt = datetime.strptime(config.batch_id, "%Y%m%dT%H%M%S%fZ").replace(tzinfo=timezone.utc)
        return int(dt.timestamp() * 1_000_000)
    except (TypeError, ValueError):
        return None


def _write_core(spark, df, config: IngestionConfig, txn_options=None):
    schema_ref = f"{config.catalog}.{config.schema_name}" if config.catalog else config.schema_name
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema_ref}")

    full_name = config.full_table_name
    _ensure_liquid_clustering_and_properties(spark, df, config, full_name)

    writer = df.write.format("delta")

    if config.merge_schema:
        writer = writer.option("mergeSchema", "true")
    if config.partition_by:
        writer = writer.partitionBy(*config.partition_by)
    if txn_options:  # idempotent-write options (txnAppId/txnVersion) - streaming foreachBatch or batch retry-safety (#63)
        for k, v in txn_options.items():
            writer = writer.option(k, v)

    if config.write_mode == "append":
        writer.mode("append").saveAsTable(full_name)

    elif config.write_mode == "overwrite":
        writer.mode("overwrite").saveAsTable(full_name)
        if config.cluster_by:
            # An unqualified overwrite performs an implicit REPLACE TABLE
            # that silently drops CLUSTER BY - verified empirically, not
            # just a defensive guess. Restore it immediately (a cheap,
            # metadata-only ALTER) so the table is never left unclustered
            # between runs.
            cols = ", ".join(f"`{c}`" for c in config.cluster_by)
            spark.sql(f"ALTER TABLE {full_name} CLUSTER BY ({cols})")

    elif config.write_mode == "merge":
        from delta.tables import DeltaTable

        _assert_no_null_merge_keys(df, config.merge_keys)

        # resolved_, not the raw field: it defaults to None so config load can
        # tell an explicit choice from silence, and None is falsy (#54).
        if config.resolved_dedupe_before_merge:
            df = _dedupe_for_merge(df, config)
        else:
            _assert_no_duplicate_merge_keys(df, config.merge_keys)

        # Atomic create-if-not-exists instead of a check-then-act on table
        # existence - two concurrent first-runs against the same
        # not-yet-existing table could otherwise both observe "doesn't
        # exist" and both take an append path, duplicating the entire
        # first batch (#46). Merging into a freshly-created empty table is
        # equivalent to insert-all, so there's no separate "first load"
        # branch needed - and it makes a retried first load idempotent
        # too, since MERGE on merge_keys can't duplicate rows the way a
        # retried append could.
        creator = DeltaTable.createIfNotExists(spark).tableName(full_name).addColumns(df.schema)
        if config.partition_by:
            creator = creator.partitionedBy(*config.partition_by)
        creator.execute()

        target = DeltaTable.forName(spark, full_name)
        condition = " AND ".join(f"target.`{k}` = source.`{k}`" for k in config.merge_keys)
        (
            target.alias("target")
            .merge(df.alias("source"), condition)
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
    else:
        raise ValueError(f"Unknown write_mode: {config.write_mode}")

    return full_name


def write_bronze(spark, df, config: IngestionConfig):
    """
    Writes df to the configured Delta bronze table (batch mode). Creates the
    schema (database) if it doesn't exist. Retries on transient failures
    (throttling, concurrent-write conflicts). Returns the full table name.

    For append/overwrite, wraps the write in Delta's idempotent-write
    transaction options (txnAppId/txnVersion) when
    config.idempotent_batch_writes=True (default) and a stable txnVersion
    can be derived from config.batch_id (#63) - a retried batch job
    (write succeeded, a downstream step then failed) re-running with the
    same batch_id converges to one copy of the data instead of duplicating
    it. Not applied to write_mode="merge" - Delta's MERGE doesn't accept
    txn options, but re-running the same batch is naturally safe there via
    merge_keys upsert semantics anyway.
    """
    txn_options = None
    if config.idempotent_batch_writes and config.write_mode in ("append", "overwrite"):
        txn_version = _resolve_idempotent_txn_version(config)
        if txn_version is not None:
            txn_options = {"txnAppId": config.full_table_name, "txnVersion": str(txn_version)}
        elif config.batch_id is not None:
            logger.warning(
                "idempotent_batch_writes=True but batch_id=%r isn't an integer or a "
                "recognized timestamp format - can't derive a stable txnVersion, so this "
                "write is not idempotent-protected. Pass an integer batch_id (e.g. a "
                "Databricks job run ID) for retry-safe batch writes.", config.batch_id,
            )
        else:
            logger.debug(
                "idempotent_batch_writes=True but no explicit batch_id is set - an "
                "auto-generated batch_id changes on every attempt and can't provide retry "
                "protection. Pass a stable batch_id (e.g. a Databricks job run ID) to get "
                "this guarantee."
            )

    @with_retry(attempts=config.retry_attempts, delay_seconds=config.retry_delay_seconds)
    def _do_write():
        return _write_core(spark, df, config, txn_options=txn_options)

    return _do_write()


def write_bronze_micro_batch(spark, micro_batch_df, batch_id: int, config: IngestionConfig):
    """
    Used as the body of `foreachBatch` for streaming ingestion. Achieves
    exactly-once sink writes (even across job restarts / retried batches)
    using Delta Lake's idempotent-write transaction options, keyed by this
    pipeline's checkpoint location as the app id and the Structured
    Streaming batch_id as the version.

    See: https://docs.delta.io/latest/delta-streaming.html#idempotent-table-writes-in-foreachbatch

    Note: txnAppId/txnVersion idempotency applies to the append/overwrite
    write path. If write_mode="merge", Delta's MERGE itself does not accept
    those options - a retried micro-batch merging the same rows on the same
    merge_keys is naturally safe (updates just re-apply), but true
    exactly-once row counting for merge + streaming should additionally rely
    on Auto Loader's own checkpoint (which prevents re-reading the same
    source files) rather than txnVersion.
    """
    if micro_batch_df.rdd.isEmpty():
        logger.info("Micro-batch %s is empty - skipping write.", batch_id)
        return

    txn_app_id = config.checkpoint_location or config.full_table_name
    txn_options = {"txnAppId": txn_app_id, "txnVersion": str(batch_id)}

    @with_retry(attempts=config.retry_attempts, delay_seconds=config.retry_delay_seconds)
    def _do_write():
        return _write_core(spark, micro_batch_df, config, txn_options=txn_options)

    full_name = _do_write()
    logger.info("Micro-batch %s written to %s", batch_id, full_name)
