"""
Writes a prepared DataFrame into a Delta bronze table, adding standard audit
columns and supporting append / overwrite / merge semantics with optional
schema evolution.
"""

from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from pyspark.sql.functions import col, current_timestamp, lit, row_number
from pyspark.sql.window import Window

from .config import IngestionConfig
from .errors import DuplicateMergeKeyError, NullMergeKeyError, WriteNotCommittedError
from .logging_utils import logger
from .retry import with_retry
from .sql_utils import apply_table_properties, row_content_hash


def resolve_batch_id(config: IngestionConfig) -> str:
    """
    The `_batch_id` value for one run.

    Call this ONCE per run and pass the result down (#148). It used to be
    derived inside `add_audit_columns`, which is called twice per run - once
    for the good rows and once for the bad - so with `config.batch_id` unset
    each call produced its own microsecond timestamp and the bronze rows and
    the quarantine rows from a single run carried DIFFERENT `_batch_id`s.

    The visible consequence was in a recovery tool, several steps away:
    `reprocess_quarantine(batch_id=...)` exists to replay the rows from a
    given run, so an operator would read a `_batch_id` off the bronze table,
    pass it in, and get zero matches with no error - the filter was valid and
    simply matched nothing. "There was nothing to replay" is indistinguishable
    from success, which is what made it worth fixing rather than documenting.

    The deployed job sets `batch_id` to `{{job.run_id}}`, so production was
    never affected. This was the library default failing on its own - the same
    shape as the `audit_schema_name` trap closed in #54, where a correctness
    property held only because one YAML file remembered to set a field.
    """
    return config.batch_id or datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")


def add_audit_columns(df, config: IngestionConfig, batch_id: Optional[str] = None):
    """
    Stamps `_ingested_at`, `_batch_id` and `_source_file` onto df.

    `batch_id` should be supplied by the caller, resolved once per run via
    `resolve_batch_id`. It defaults to resolving its own only so that direct
    callers outside the pipeline keep working; every in-package call site
    passes one.
    """
    if not config.add_audit_columns:
        return df

    batch_id = batch_id if batch_id is not None else resolve_batch_id(config)

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
            config.audit_source_file_col,
            config.source_path,
        )
        df = df.withColumn(config.audit_source_file_col, lit(config.source_path))

    return df


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


def _dedupe_for_merge(df, config: IngestionConfig, merge_key_columns):
    """
    Delta MERGE raises "Cannot perform Merge as multiple source rows
    matched..." when the source has more than one row per merge key.
    Bronze sources frequently re-send full-file dumps or contain
    intra-batch duplicates, so deterministically keep one row per key -
    the one with the highest dedupe_order_by value (defaults to the
    ingestion timestamp, so the most-recently-ingested row wins).

    `merge_key_columns` is the columns to partition by for this run - either
    `config.merge_keys` or, under the content-hash strategy (#84),
    `[config.content_hash_key_col]`. Passed in rather than read from
    `config.merge_keys` directly so this function doesn't need to know which
    strategy is active; `_prepare_merge_keys` already resolved that.

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

    w = Window.partitionBy(*merge_key_columns).orderBy(
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


def _prepare_merge_keys(df, config: IngestionConfig):
    """
    Resolves the MERGE key column(s) for this write and, under the
    content-hash strategy, computes them onto `df` (#84).

    Two mutually exclusive strategies - config guarantees exactly one is set
    when write_mode='merge':

    - Natural key (`config.merge_keys`): used as-is, nothing added to `df`.
    - Content hash (`config.content_hash_columns`): computes
      `row_content_hash(df, config.content_hash_columns)` into
      `config.content_hash_key_col` and returns THAT single column as the
      merge key. See `content_hash_columns`' field comment in config.py for
      why this gives idempotent re-ingestion of identical payloads rather
      than upsert semantics - callers further down this module (dedupe,
      null/duplicate checks, the MERGE condition itself) don't need to know
      which strategy produced the key, only which column(s) to use.

    Returns (df, merge_key_columns) - df is unchanged for the natural-key
    path, and carries the new hash column for the content-hash path.
    """
    if config.content_hash_columns:
        df = df.withColumn(
            config.content_hash_key_col,
            row_content_hash(df, config.content_hash_columns),
        )
        return df, [config.content_hash_key_col]
    return df, list(config.merge_keys or [])


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
    except Exception:  # noqa: BLE001 - DESCRIBE DETAIL is unavailable on some engines; absent state is the answer
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
    # resolved_table_properties, not table_properties: it carries the CDF
    # defaults (#58) as well as the user's dict. One consequence worth naming
    # - with CDF on by default this set is never empty, so the early return
    # below no longer fires for a table that configures no layout at all.
    # That is the intended behaviour change: CDF has to reach every table the
    # package creates, including ones nobody configured.
    desired_props = config.resolved_table_properties

    if not (config.cluster_by or config.cluster_by_auto or desired_props):
        return

    from delta.tables import DeltaTable

    if not spark.catalog.tableExists(full_name):
        creator = DeltaTable.createIfNotExists(spark).tableName(full_name).addColumns(df.schema)
        if config.cluster_by:
            creator = creator.clusterBy(*config.cluster_by)
        elif config.partition_by:
            creator = creator.partitionedBy(*config.partition_by)
        for key, value in desired_props.items():
            creator = creator.property(key, value)
        creator.execute()

    current_cluster_cols, current_props = _describe_current_layout(spark, full_name)

    if config.cluster_by and current_cluster_cols != list(config.cluster_by):
        cols = ", ".join(f"`{c}`" for c in config.cluster_by)
        spark.sql(f"ALTER TABLE {full_name} CLUSTER BY ({cols})")
        if current_cluster_cols is not None:
            logger.warning(
                "Cluster-by columns changed for %s: %s -> %s",
                full_name,
                current_cluster_cols,
                config.cluster_by,
            )

    if config.cluster_by_auto:
        try:
            spark.sql(f"ALTER TABLE {full_name} CLUSTER BY AUTO")
        except Exception as exc:  # noqa: BLE001 - CLUSTER BY AUTO is DBR-only; unsupported elsewhere is expected, not fatal
            logger.warning(
                "cluster_by_auto=True but this engine doesn't support CLUSTER BY AUTO "
                "(expected outside Databricks Runtime, which manages predictive "
                "optimization for AUTO-clustered tables): %s",
                exc,
            )

    # current_props is already in hand from the DESCRIBE DETAIL above, so it
    # is passed in rather than letting the helper re-read it. Escaping and the
    # diff-before-ALTER live in the helper now, shared with the quarantine
    # write (#58) - see sql_utils.apply_table_properties.
    changed_props = apply_table_properties(spark, full_name, desired_props, current_props)
    if changed_props:
        logger.warning("Table properties changed for %s: %s", full_name, changed_props)


def _resolve_idempotent_txn_version(config: IngestionConfig):
    """
    Returns the Delta txnVersion for this write's idempotent-write options
    (#63), or None to skip the protection entirely.

    Nothing is derived here any more. The version is whatever
    `config.idempotent_txn_version` says, and unset means unprotected.

    Why deriving it was removed (#366)
    ----------------------------------
    This used to fall back to `config.batch_id`, using it directly when it
    parsed as an integer and converting the package's own timestamp format
    otherwise. Every deployed job sets `batch_id` to `{{job.run_id}}`, so in
    practice the txnVersion was a Databricks job run ID.

    Delta silently discards a write whose txnVersion is at or below one
    already committed for the same txnAppId. Job run IDs are globally unique
    but not increasing, so a run drawing an ID lower than one already used
    for that table wrote nothing, raised nothing, reported success, and had
    its source file archived out of the landing directory. Reproduced in dev
    with three runs of one unchanged file: the middle run, given a lower
    batch_id, committed nothing while its audit row reported rows written.

    The old docstring recommended a job run ID for exactly this. It confused
    *stable across retries* with *increasing across batches*. Delta needs
    both, and a run ID has only the first, so there is no safe default left
    to derive - which is why this is now opt-in.
    """
    return config.idempotent_txn_version


def table_version(spark, full_name: str) -> Optional[int]:
    """
    Current Delta version of `full_name`, or None if the table does not
    exist yet or the version cannot be read.

    Deliberately quiet: this is used to bracket a write, and a version that
    cannot be read means "unknown", not "failed". Callers treat None as
    no-information rather than as evidence of anything.
    """
    try:
        row = spark.sql(f"DESCRIBE HISTORY {full_name} LIMIT 1").select("version").collect()
        return int(row[0]["version"]) if row else None
    except Exception:  # noqa: BLE001 - absent or unreadable table is the answer, not an error
        return None


def _write_core(spark, df, config: IngestionConfig, txn_options=None):
    schema_ref = f"{config.catalog}.{config.schema_name}" if config.catalog else config.schema_name
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema_ref}")

    full_name = config.full_table_name

    # Merge preparation and its refusals run BEFORE anything can create the
    # table, and the ordering is load-bearing in two ways (both caught by
    # pre-existing tests when #58 made the layout step unconditional):
    #
    # 1. _ensure_liquid_clustering_and_properties creates the table when it
    #    has something to apply. Until #58 it usually had nothing, so it
    #    returned early and a config-error merge left no table behind -
    #    test_merge_refuses_null_merge_keys asserts exactly that. With CDF on
    #    by default it always has something to apply, so a run that is about
    #    to be refused would otherwise leave an empty table behind.
    # 2. Under the content-hash strategy (#84) _prepare_merge_keys ADDS a
    #    column. Creating the table from the pre-hash schema and then merging
    #    the post-hash DataFrame into it is a schema mismatch.
    #
    # So: resolve the keys, refuse if they are unusable, and only then let
    # anything touch the catalog.
    merge_key_columns: List[str] = []
    if config.write_mode == "merge":
        df, merge_key_columns = _prepare_merge_keys(df, config)
        _assert_no_null_merge_keys(df, merge_key_columns)
        # resolved_, not the raw field: it defaults to None so config load can
        # tell an explicit choice from silence, and None is falsy (#54).
        if config.resolved_dedupe_before_merge:
            df = _dedupe_for_merge(df, config, merge_key_columns)
        else:
            _assert_no_duplicate_merge_keys(df, merge_key_columns)

    _ensure_liquid_clustering_and_properties(spark, df, config, full_name)

    writer = df.write.format("delta")

    if config.merge_schema:
        writer = writer.option("mergeSchema", "true")
    if config.partition_by:
        writer = writer.partitionBy(*config.partition_by)
    # idempotent-write options (txnAppId/txnVersion) - streaming foreachBatch
    # or batch retry-safety (#63)
    if txn_options:
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

        # df and merge_key_columns were both resolved above, before anything
        # could create the table - see the comment there for why that ordering
        # matters. merge_key_columns is non-empty here by construction.

        # Atomic create-if-not-exists instead of a check-then-act on table
        # existence - two concurrent first-runs against the same
        # not-yet-existing table could otherwise both observe "doesn't
        # exist" and both take an append path, duplicating the entire
        # first batch (#46). Merging into a freshly-created empty table is
        # equivalent to insert-all, so there's no separate "first load"
        # branch needed - and it makes a retried first load idempotent
        # too, since MERGE on merge_key_columns can't duplicate rows the way
        # a retried append could.
        creator = DeltaTable.createIfNotExists(spark).tableName(full_name).addColumns(df.schema)
        if config.partition_by:
            creator = creator.partitionedBy(*config.partition_by)
        creator.execute()

        target = DeltaTable.forName(spark, full_name)
        condition = " AND ".join(f"target.`{k}` = source.`{k}`" for k in merge_key_columns)
        merge_builder = target.alias("target").merge(df.alias("source"), condition)

        if config.content_hash_columns:
            # Excluded from the matched-row update rather than folded into a
            # blanket whenMatchedUpdateAll(): a genuine match means source
            # and target hash are already equal (that's what made them
            # match), so overwriting it would be a no-op either way - this
            # makes that explicit instead of relying on it being
            # incidentally true, per #84's design note.
            update_columns: Dict[str, Any] = {
                c: f"source.`{c}`" for c in df.columns if c != config.content_hash_key_col
            }
            merge_builder = merge_builder.whenMatchedUpdate(set=update_columns)
        else:
            merge_builder = merge_builder.whenMatchedUpdateAll()

        merge_builder.whenNotMatchedInsertAll().execute()
    else:
        raise ValueError(f"Unknown write_mode: {config.write_mode}")

    return full_name


#: Delta operations that commit metadata/layout changes with no row-level
#: metrics of their own - `operationMetrics` has no `numTargetRows*` or
#: `numOutputRows` keys. Auto-optimize/auto-compaction commits one of these
#: immediately after our write returns, becoming the new latest commit
#: before `read_write_metrics` gets a chance to read it (#385). Kept as a
#: blocklist rather than matching write-mode-specific operations against an
#: allowlist (`"MERGE"`, `"WRITE"`, ...): the exact operation string Delta
#: records for a plain `saveAsTable` write is not a stable contract across
#: Delta versions, and a blocklist is the conservative direction to be
#: wrong in - it only ever skips MORE commits, never mistakes a real write
#: for maintenance.
_MAINTENANCE_OPERATIONS = frozenset(
    {
        "OPTIMIZE",
        "VACUUM START",
        "VACUUM END",
        "SET TBLPROPERTIES",
        "UNSET TBLPROPERTIES",
        "ADD CONSTRAINT",
        "DROP CONSTRAINT",
        "CHANGE COLUMN",
        "UPGRADE PROTOCOL",
        "UPGRADE SCHEMA",
        "COMPUTE STATS",
        "RESTORE",
        "FSCK",
    }
)

#: What an audit row records about a write. Every value is None when the
#: metrics could not be read, so a caller never has to distinguish "absent"
#: from "zero".
EMPTY_WRITE_METRICS: Dict[str, Any] = {
    "row_count": None,
    "source_row_count": None,
    "rows_inserted": None,
    "rows_updated": None,
    "rows_deleted": None,
}


def read_write_metrics(
    spark, full_name: str, write_mode: str, since_version: Optional[int] = None
) -> Dict[str, Any]:
    """
    Row counts for the write that just committed, taken from Delta's own
    transaction log rather than by recounting the DataFrame (#149).

    Pass `since_version` - the table's version before the write - to get the
    guarantee that the numbers belong to this run. Without it the latest
    commit is assumed to be this write's, which is what let #366 report row
    counts copied from an earlier run.

    Why not `final_df.count()`, which is what this replaces
    -----------------------------------------------------
    That count was an action on an uncached lazy plan, executed AFTER the
    write had already consumed it, and `.cache()` is unavailable on
    serverless. So it re-read the source and re-ran the entire quality gate
    to produce a number Delta already knew exactly. On the batch path the
    source was being scanned roughly four times per run; this removes one of
    them for free, since reading the transaction log is a metadata operation.

    It was also the WRONG number under `merge`. `final_df` is the
    post-quality-gate source batch, so it counted rows that
    `_dedupe_for_merge` collapsed before the MERGE ever saw them, and it
    could not distinguish an insert from an update. A merge run that updated
    500 existing rows and inserted nothing reported `row_count: 500`, which
    reads as 500 new rows. Three open issues (#61, #62, #109) consume this
    column as if it were comparable across runs and write modes.

    What each mode reports
    ----------------------
    `append` / `overwrite` : `numOutputRows` - rows written. `source_row_count`
        is the same number, because nothing is dropped between the gate and
        the write.
    `merge` : `numTargetRowsInserted` + `numTargetRowsMatchedUpdated` as
        `row_count` (rows actually changed in the target), the three
        components separately, and `numSourceRows` as `source_row_count`. The
        difference between source and target counts is the dedupe/no-op
        ratio, which is a genuinely useful signal and was previously
        unobservable.

        Uses the `...Matched...` metric keys (`numTargetRowsMatchedUpdated`,
        `numTargetRowsMatchedDeleted`), not the unqualified
        `numTargetRowsUpdated`/`numTargetRowsDeleted` (#376). Verified
        against a real merge commit's `operationMetrics` in dev: the
        unqualified keys were absent (this package's MERGE never issues a
        `WHEN NOT MATCHED BY SOURCE` clause), so every merge audit row read
        back NULL for row_count/rows_updated/rows_deleted even though the
        write itself was correct.

    Never raises. A metrics read failing must not fail an ingestion that has
    already committed - the same rule audit.py and schema_registry.py follow.

    Reads the latest commit that is NOT a maintenance operation
    (`_MAINTENANCE_OPERATIONS`), rather than unconditionally the latest
    commit. On a table with auto-optimize/auto-compaction enabled, Delta
    commits an `OPTIMIZE` right after the MERGE/WRITE - still before this
    function runs - and `OPTIMIZE`'s `operationMetrics` has none of the
    `numTargetRows*`/`numOutputRows` keys this function looks for, so every
    one of them came back `None` (#385). Looking a fixed 20 commits back
    for the nearest non-maintenance one is enough for any realistic run of
    auto-compaction, and is still a metadata-only read.

    One honest caveat: a concurrent writer's non-maintenance commit landing
    between our write and this read would have its metrics attributed to
    our run. The deployed job sets `max_concurrent_runs: 1` (#153/#164),
    which closes it for the case that actually occurs here.
    """
    try:
        from delta.tables import DeltaTable

        history = (
            DeltaTable.forName(spark, full_name)
            .history(20)
            .select("version", "operation", "operationMetrics")
        )
        rows = [row for row in history.collect() if row["operation"] not in _MAINTENANCE_OPERATIONS]
        if not rows:
            return dict(EMPTY_WRITE_METRICS)

        # The latest (non-maintenance) commit is only this run's if it is
        # newer than the version the table was on before the write. When it
        # is not, this run committed nothing and the numbers below belong to
        # somebody else's commit - which is how #366's audit rows came to
        # report five rows written by a run that wrote none.
        if since_version is not None and rows[0]["version"] <= since_version:
            logger.warning(
                "%s is still at version %s, so the latest non-maintenance commit predates "
                "this write. Reporting no metrics rather than attributing an earlier "
                "commit's row counts to this run (#366).",
                full_name,
                rows[0]["version"],
            )
            return dict(EMPTY_WRITE_METRICS)

        metrics = rows[0]["operationMetrics"] or {}

        def _num(key):
            value = metrics.get(key)
            return int(value) if value is not None else None

        if write_mode == "merge":
            inserted = _num("numTargetRowsInserted")
            updated = _num("numTargetRowsMatchedUpdated")
            written = (
                None if inserted is None and updated is None else (inserted or 0) + (updated or 0)
            )
            return {
                "row_count": written,
                "source_row_count": _num("numSourceRows"),
                "rows_inserted": inserted,
                "rows_updated": updated,
                "rows_deleted": _num("numTargetRowsMatchedDeleted"),
            }

        written = _num("numOutputRows")
        return {
            "row_count": written,
            "source_row_count": written,
            "rows_inserted": None,
            "rows_updated": None,
            # An overwrite removes whatever was there. Delta reports it when
            # it knows it; append never deletes.
            "rows_deleted": _num("numDeletedRows") if write_mode == "overwrite" else None,
        }
    except Exception as exc:  # noqa: BLE001 - metrics must never fail a committed write
        logger.warning(
            "Could not read write metrics for %s: %s. The audit row will record "
            "NULL counts for this run.",
            full_name,
            exc,
        )
        return dict(EMPTY_WRITE_METRICS)


def write_bronze(spark, df, config: IngestionConfig):
    """
    Writes df to the configured Delta bronze table (batch mode). Creates the
    schema (database) if it doesn't exist. Retries on transient failures
    (throttling, concurrent-write conflicts). Returns the full table name.

    For append/overwrite, wraps the write in Delta's idempotent-write
    transaction options (txnAppId/txnVersion) when
    config.idempotent_batch_writes=True (default) AND
    config.idempotent_txn_version is set (#63) - a retried batch job (write
    succeeded, a downstream step then failed) re-running with the same
    version converges to one copy of the data instead of duplicating it.

    The version is never derived. It used to come from config.batch_id,
    which every deployed job sets to the Databricks job run ID, and because
    run IDs are not increasing Delta silently discarded writes that drew a
    low one (#366). Unset means unprotected, which is the safe default.

    Every write is bracketed by a table-version check, protection or not, so
    a write that commits nothing raises WriteNotCommittedError instead of
    returning as though it had succeeded.

    Not applied to write_mode="merge" - Delta's MERGE doesn't accept
    txn options, but re-running the same batch is naturally safe there:
    via merge_keys upsert semantics, or, under the content-hash strategy
    (#84), because an identical retried batch re-hashes to the same key and
    matches the rows it already wrote (idempotent re-ingestion - not
    upsert, see content_hash_columns' field comment in config.py).
    """
    txn_options = None
    if config.idempotent_batch_writes and config.write_mode in ("append", "overwrite"):
        txn_version = _resolve_idempotent_txn_version(config)
        if txn_version is not None:
            txn_options = {"txnAppId": config.full_table_name, "txnVersion": str(txn_version)}
        else:
            logger.debug(
                "idempotent_batch_writes=True but idempotent_txn_version is unset, so this "
                "write is not idempotent-protected. That is the default: there is no version "
                "this package can derive safely, and a discarded write loses data where a "
                "duplicated one does not (#366)."
            )

    # Bracket the write so a no-op commit cannot pass for a successful one.
    # Read before anything can create the table: None means the table does
    # not exist yet, and any commit at all is then an advance.
    version_before = table_version(spark, config.full_table_name)

    @with_retry(
        attempts=config.retry_attempts,
        delay_seconds=config.retry_delay_seconds,
        max_total_seconds=config.retry_max_total_seconds,
    )
    def _do_write():
        return _write_core(spark, df, config, txn_options=txn_options)

    full_name = _do_write()

    _assert_write_committed(spark, config, version_before, txn_options)
    return full_name


def _assert_write_committed(spark, config: IngestionConfig, version_before, txn_options):
    """
    Raises WriteNotCommittedError if the write returned without committing.

    A write that commits nothing is indistinguishable from a successful one
    to every caller: the audit row still records a row count, and the source
    file is still archived out of the landing directory. #366 was exactly
    that, undetected across four staging runs, and the only reason anyone
    noticed was a row count that failed to move.

    Checked by version rather than by counting rows, which keeps this a
    metadata read - the same reason read_write_metrics exists at all (#149).

    Deliberately not raised when the version cannot be read before or after.
    Unknown is not evidence, and an engine that cannot answer DESCRIBE
    HISTORY must not start failing every ingestion.
    """
    version_after = table_version(spark, config.full_table_name)
    if version_before is None or version_after is None:
        return
    if version_after > version_before:
        return

    detail = ""
    if txn_options:
        detail = (
            f" The write carried txnAppId={txn_options['txnAppId']!r} and "
            f"txnVersion={txn_options['txnVersion']}, and Delta discards a write whose "
            f"txnVersion is at or below one already committed for that txnAppId. Supply a "
            f"higher idempotent_txn_version, or leave it unset to disable the protection."
        )
    raise WriteNotCommittedError(
        f"{config.full_table_name} is still at version {version_after} after a "
        f"{config.write_mode} write, so nothing was committed and the rows are not in "
        f"the table.{detail}"
    )


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
    # DataFrame.isEmpty(), NOT micro_batch_df.rdd.isEmpty() (#248).
    #
    # `.rdd` does not exist on Spark Connect - it raises
    # PySparkNotImplementedError: [NOT_IMPLEMENTED] rdd is not implemented.
    # Every Databricks compute this project has is serverless (azure_setup.md
    # Step 3: the trial subscription's 4-vCPU quota rules out classic
    # compute), and serverless is Spark Connect. So this line failed the
    # FIRST micro-batch of every streaming run, every time - the streaming
    # write path could never have worked here.
    #
    # It survived because no test could reach it: cloudFiles is Databricks-
    # only, so the suite cannot start a stream at all, and local pyspark is
    # classic Spark where `.rdd` exists and the line is fine. Found by the
    # first real Auto Loader run (#248), not by CI.
    if micro_batch_df.isEmpty():
        logger.info("Micro-batch %s is empty - skipping write.", batch_id)
        return

    txn_app_id = config.checkpoint_location or config.full_table_name
    txn_options = {"txnAppId": txn_app_id, "txnVersion": str(batch_id)}

    @with_retry(
        attempts=config.retry_attempts,
        delay_seconds=config.retry_delay_seconds,
        max_total_seconds=config.retry_max_total_seconds,
    )
    def _do_write():
        return _write_core(spark, micro_batch_df, config, txn_options=txn_options)

    full_name = _do_write()
    logger.info("Micro-batch %s written to %s", batch_id, full_name)
