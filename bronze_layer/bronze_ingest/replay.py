"""
Quarantine replay: reprocesses previously-quarantined rows and files after
an upstream source or quality rule has been fixed (#60). Quarantine
without a way back is a graveyard - this closes the loop back to bronze.

Two independent entry points:
  reprocess_quarantine()          - row replay, from the quarantine table
  reprocess_quarantined_files()   - file replay, from quarantine_files/
"""

import fnmatch
import os
from datetime import datetime, timezone
from typing import Any, Dict, Optional

from pyspark.sql.functions import coalesce, col, current_timestamp, lit

from .audit import record_replay_run
from .bronze_writer import write_bronze
from .config import IngestionConfig

# From `fs`, not `directory_ingestion` (#151). This module previously
# imported three UNDERSCORE-PREFIXED names across a module boundary, which
# meant any refactor of directory ingestion broke replay silently and the
# privacy marker was actively misleading. Quarantine replay depends on file
# movement and retry state; it has never depended on directory ingestion.
from .fs import RetryState, list_json_files, move_file_direct
from .logging_utils import logger
from .quality import split_good_bad

#: Default ceiling on a single replay, in rows.
#:
#: Distinguishes "caller said None" from "caller said nothing" for
#: max_replay_attempts. None is a meaningful VALUE here - it means "ignore the
#: limit and sweep exhausted rows back in" - so it cannot double as the
#: default. Same #54 reasoning the config fields use.
_UNSET = object()

#: Not a performance limit - the delete is distributed now and would cope.
#: It is a guard against the shape of the operation: replay is what an
#: operator runs AFTER fixing an upstream source, against a quarantine table
#: that has been accumulating since the problem started. "We fixed the feed,
#: replay everything" is both the natural usage and the unbounded case, and
#: nothing previously warned when the unfiltered set was large.
DEFAULT_MAX_REPLAY_ROWS = 500_000


def _merge_deleted_count(spark, table_name, merge_result):
    """Rows the MERGE actually removed, from Delta's own metrics.

    Prefers the DataFrame `execute()` returns on newer delta-spark; falls
    back to the transaction log where it returns None. Returns None rather
    than raising - a count that cannot be read must not fail a replay whose
    writes both succeeded."""
    try:
        if merge_result is not None and hasattr(merge_result, "collect"):
            rows = merge_result.collect()
            if rows and "num_deleted_rows" in rows[0].asDict():
                return int(rows[0]["num_deleted_rows"])

        from delta.tables import DeltaTable

        history = DeltaTable.forName(spark, table_name).history(1).select("operationMetrics")
        rows = history.collect()
        if rows and rows[0][0]:
            value = rows[0][0].get("numTargetRowsDeleted")
            return int(value) if value is not None else None
    except Exception as exc:  # noqa: BLE001 - a missing count must not fail a committed replay
        logger.warning("Could not read the quarantine delete count: %s", exc)
    return None


def _merge_updated_count(spark, table_name, merge_result):
    """Rows the MERGE actually updated, from Delta's own metrics.

    Mirrors `_merge_deleted_count`. Reporting the INPUT count instead was a
    real bug: the first version of `_increment_replay_attempts` returned how
    many ids it handed to the MERGE, so `attempts_recorded` said 1 while the
    counter on disk stayed 0. A number that reports intent rather than outcome
    is the failure this repo keeps finding - it must come from what the write
    did.
    """
    try:
        if merge_result is not None and hasattr(merge_result, "collect"):
            rows = merge_result.collect()
            if rows and "num_updated_rows" in rows[0].asDict():
                return int(rows[0]["num_updated_rows"])

        from delta.tables import DeltaTable

        history = DeltaTable.forName(spark, table_name).history(1).select("operationMetrics")
        rows = history.collect()
        if rows and rows[0][0]:
            value = rows[0][0].get("numTargetRowsUpdated")
            return int(value) if value is not None else None
    except Exception as exc:  # noqa: BLE001 - a missing count must not fail a committed replay
        logger.warning("Could not read the replay-attempt update count: %s", exc)
    return None


def _increment_replay_attempts(spark, quarantine_table: str, bad_df) -> int:
    """
    Bumps `_replay_attempts` for the rows that were offered to the gate and
    still failed (#159 item 4). Returns how many rows the MERGE actually
    updated - not how many were offered to it.

    Never raises. This is bookkeeping about a recovery attempt, not the
    recovery itself, and a failure to record the attempt must not fail a replay
    that otherwise promoted rows correctly. The cost of a missed increment is
    that a hopeless row gets rescanned once more, which is exactly the state
    everything was in before this existed.

    `localCheckpoint` is load-bearing, not an optimisation. `bad_df` is derived
    lazily from the quarantine table, so without truncating that lineage the
    MERGE's source reads the very table it is writing to - and Delta resolved
    that to zero updates while reporting success. Materialising the ids first
    makes the source independent of the target.

    Only rows that were actually TRIED are incremented. Rows skipped for being
    exhausted never reach here, so the number stays a count of attempts made
    rather than of replays run.
    """
    try:
        from delta.tables import DeltaTable

        failed_ids = bad_df.select("_quarantine_id").distinct().localCheckpoint()
        if failed_ids.isEmpty():
            return 0

        result = (
            DeltaTable.forName(spark, quarantine_table)
            .alias("q")
            .merge(failed_ids.alias("f"), "q._quarantine_id = f._quarantine_id")
            .whenMatchedUpdate(set={"_replay_attempts": "coalesce(q._replay_attempts, 0) + 1"})
            .execute()
        )
        updated = _merge_updated_count(spark, quarantine_table, result)
        return updated if updated is not None else 0
    except Exception as exc:  # noqa: BLE001 - see docstring
        logger.warning(
            "Could not record replay attempts for %s: %s. The rows remain quarantined; "
            "they will simply be offered again on the next replay.",
            quarantine_table,
            exc,
        )
        return 0


def reprocess_quarantine(
    spark,
    config: IngestionConfig,
    batch_id: Optional[str] = None,
    since=None,
    max_rows: Optional[int] = DEFAULT_MAX_REPLAY_ROWS,
    max_replay_attempts: Any = _UNSET,  # Optional[int]; _UNSET means 'use config'
) -> Dict[str, Any]:
    """
    Re-runs quarantined rows through the CURRENT quality gate - the rule
    that quarantined them may have changed since. Rows that now pass are
    written to the bronze table with fresh audit columns
    (`_batch_id` = "replay-<timestamp>") and removed from the quarantine
    table; rows that still fail remain quarantined, unchanged.

    `_source_file` is preserved as-is rather than regenerated - it's
    already genuine original per-row lineage from the initial ingestion.
    Only `_ingested_at`/`_batch_id` are refreshed to reflect the replay.
    Always uses the batch write path (write_bronze), even if config is
    normally used for streaming - replay is inherently a one-shot batch
    operation.

    Idempotent on the success path: a second call after a successful
    replay finds nothing left matching the filter, since already-replayed
    rows were deleted from quarantine, so it re-promotes nothing. Delta
    has no cross-table transactions, so exactly-once isn't achievable
    end-to-end: the bronze write happens before the quarantine delete, and
    if the delete itself then fails, the affected `_quarantine_id`(s) are
    logged clearly rather than silently risking a duplicate promotion on
    the next replay - ordered semantics with a clear failure signal, not
    a single atomic transaction.

    Args:
        batch_id: only replay rows whose original `_batch_id` matches.
        since: only replay rows whose original `_ingested_at` is >= this
            (a datetime, or anything comparable via Spark's `>=`).
        max_rows: refuse to promote more than this many rows in one call,
            with a message pointing at `batch_id` / `since`. Pass None to
            lift the guard for a replay whose size is deliberate.

    Returns {"table", "replayed_row_count", "still_quarantined_row_count",
    "skipped_exhausted_row_count", "attempts_recorded",
    "replay_batch_id"}.
    """
    quarantine_table = config.resolved_quarantine_table

    if not spark.catalog.tableExists(quarantine_table):
        logger.info("Quarantine table %s does not exist - nothing to replay.", quarantine_table)
        return {
            "table": config.full_table_name,
            "replayed_row_count": 0,
            "still_quarantined_row_count": 0,
            # No table, so nothing was skipped for being exhausted either. The
            # key is present on every return path so a caller can read it
            # without checking which path produced the result.
            "skipped_exhausted_row_count": 0,
            "attempts_recorded": 0,
            "replay_batch_id": None,
        }

    quarantined_df = spark.read.table(quarantine_table)
    if batch_id is not None:
        quarantined_df = quarantined_df.filter(col(f"`{config.audit_batch_id_col}`") == batch_id)
    if since is not None:
        quarantined_df = quarantined_df.filter(col(f"`{config.audit_ingest_ts_col}`") >= since)

    # Rows replay has already tried and failed on, more than the configured
    # number of times (#159 item 4). SKIPPED, not deleted: a row that failed
    # five times may still pass after a genuine source fix, and deletion is
    # irreversible. The counter exists to stop rescanning hopeless rows, not
    # to throw them away - pass max_replay_attempts=None to sweep them back in.
    attempt_limit = (
        config.max_replay_attempts if max_replay_attempts is _UNSET else max_replay_attempts
    )
    exhausted_count = 0
    if attempt_limit is not None and "_replay_attempts" in quarantined_df.columns:
        attempts = coalesce(col("_replay_attempts"), lit(0))
        exhausted_count = quarantined_df.filter(attempts >= lit(attempt_limit)).count()
        quarantined_df = quarantined_df.filter(attempts < lit(attempt_limit))
        if exhausted_count:
            logger.warning(
                "Replay for %s: skipping %d row(s) that have already failed %d or more "
                "attempts. They are still quarantined, not deleted - re-run with "
                "max_replay_attempts=None to include them.",
                quarantine_table,
                exhausted_count,
                attempt_limit,
            )

    # Drop quarantine-specific + stale audit columns so the current quality
    # gate re-derives everything fresh (_source_file is deliberately kept).
    #
    # `_occurrence_count` and `_first_quarantined_at` are quarantine
    # bookkeeping (#148) and must go too. They describe the row's history in
    # quarantine, not the data, and without dropping them here they ride
    # through the promotion and land as columns on the BRONZE table -
    # polluting it with metadata about a table it has no relationship to.
    # `_quarantine_id` is kept for now because the delete below needs it; it
    # is dropped just before the write.
    candidate_df = quarantined_df.drop(
        "_quarantine_reason",
        config.audit_ingest_ts_col,
        config.audit_batch_id_col,
        "_occurrence_count",
        "_first_quarantined_at",
        # Same reason as the two above: quarantine bookkeeping, not data. It
        # would otherwise land as a column on the BRONZE table.
        "_replay_attempts",
    )
    good_df, bad_df = split_good_bad(candidate_df, config)

    # An aggregate, not a collect (#155). The previous form pulled every
    # promoted id into the driver heap - ~200 bytes per row with Row
    # overhead, so 1M replayed rows was ~200MB of driver memory for data
    # that never needed to leave the executors, on serverless compute whose
    # driver size the operator does not control.
    good_count = good_df.count()
    still_bad_count = bad_df.count()

    if max_rows is not None and good_count > max_rows:
        raise ValueError(
            f"Replay would promote {good_count:,} row(s) from {quarantine_table}, "
            f"which exceeds max_rows={max_rows:,}. Replay is the operation an "
            f"operator runs after fixing an upstream source, against a quarantine "
            f"table that has been accumulating since the problem started - so "
            f"'replay everything' is the normal usage and the unbounded case at "
            f"the same time. Narrow it with batch_id= or since=, or raise "
            f"max_rows deliberately if this size is intended."
        )

    replay_batch_id = f"replay-{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S%fZ')}"

    if good_count == 0:
        logger.info(
            "Replay for %s: 0 row(s) now pass (%d still quarantined) - nothing promoted.",
            config.full_table_name,
            still_bad_count,
        )
        record_replay_run(
            spark,
            config,
            status="success_replay",
            row_count=0,
            quarantined_row_count=still_bad_count,
        )
        marked = _increment_replay_attempts(spark, quarantine_table, bad_df)
        return {
            "table": config.full_table_name,
            "replayed_row_count": 0,
            "still_quarantined_row_count": still_bad_count,
            "skipped_exhausted_row_count": exhausted_count,
            "attempts_recorded": marked,
            "replay_batch_id": replay_batch_id,
        }

    replayed_df = (
        good_df.drop("_quarantine_id")
        .withColumn(config.audit_ingest_ts_col, current_timestamp())
        .withColumn(config.audit_batch_id_col, lit(replay_batch_id))
    )
    table_name = write_bronze(spark, replayed_df, config)

    deleted_count = None
    try:
        from delta.tables import DeltaTable

        # A distributed anti-join, not a driver-built IN list (#155).
        #
        # The previous form pasted every promoted id into one SQL predicate:
        # ~39 bytes of text per id, so 1M rows was a ~39MB SQL string. Spark's
        # parser is not built for that - expect quadratic parse time well
        # before then and a StackOverflowError in Catalyst somewhere in the
        # tens of thousands. The exact threshold is version-dependent, which
        # is worse than a fixed limit: it worked in testing and would have
        # failed in production at an unpredictable size.
        #
        # And it failed at the worst possible moment. The bronze write above
        # has ALREADY COMMITTED by this point, so the handler below is the
        # only thing standing between a parser failure and rows that are in
        # bronze and still in quarantine - ready to be promoted a second time
        # by the next replay. Deterministically, since the next attempt would
        # build the same oversized statement. Each retry made it worse.
        good_df.select("_quarantine_id").createOrReplaceTempView("_replayed_quarantine_ids")
        merge_result = (
            DeltaTable.forName(spark, quarantine_table)
            .alias("q")
            .merge(
                spark.table("_replayed_quarantine_ids").alias("r"),
                "q._quarantine_id = r._quarantine_id",
            )
            .whenMatchedDelete()
            .execute()
        )
        # numTargetRowsDeleted is authoritative for "how many were actually
        # removed", and free - same argument #149 makes for ingestion counts.
        deleted_count = _merge_deleted_count(spark, quarantine_table, merge_result)
    except Exception as exc:  # noqa: BLE001 - bronze write already succeeded; the message tells an operator how to recover
        logger.error(
            "Replayed %d row(s) to %s successfully, but failed to remove them from "
            "quarantine table %s: %s. Those rows are now in BOTH tables and may be "
            "re-promoted (and duplicated in bronze) on the next replay. To find "
            "them, join %s to %s on the rows whose %s = %r. Do not simply re-run "
            "replay until this is resolved.",
            good_count,
            table_name,
            quarantine_table,
            exc,
            quarantine_table,
            table_name,
            config.audit_batch_id_col,
            replay_batch_id,
        )

    if deleted_count is not None and deleted_count != good_count:
        # Not fatal - both writes committed - but the two numbers disagreeing
        # means the set promoted and the set removed were not identical, which
        # is the invariant this operation rests on.
        logger.warning(
            "Replay promoted %d row(s) but removed %d from quarantine. These should "
            "match; a difference means rows are in both tables or were removed "
            "without being promoted.",
            good_count,
            deleted_count,
        )

    record_replay_run(
        spark,
        config,
        status="success_replay",
        row_count=good_count,
        quarantined_row_count=still_bad_count,
    )
    logger.info(
        "Replay for %s: %d row(s) promoted to bronze, %d row(s) still quarantined.",
        config.full_table_name,
        good_count,
        still_bad_count,
    )

    # After the promotion and the delete: a row that still failed has now had
    # one more attempt made on it, whether or not anything else was promoted.
    marked = _increment_replay_attempts(spark, quarantine_table, bad_df)

    return {
        "table": table_name,
        "replayed_row_count": good_count,
        "still_quarantined_row_count": still_bad_count,
        "skipped_exhausted_row_count": exhausted_count,
        "attempts_recorded": marked,
        "replay_batch_id": replay_batch_id,
    }


def reprocess_quarantined_files(
    spark, source_dir: str, pattern: Optional[str] = None
) -> Dict[str, Any]:
    """
    Moves files from quarantine_files/ back into source_dir so the next
    ingest_directory_to_bronze() run picks them up - reuses all the usual
    per-file failure isolation, archival, and retry-limit logic for free,
    rather than re-ingesting directly here. Any leftover retry-state entry
    for the file is cleared, so it gets a fresh set of attempts instead of
    counting against a limit it may have already exhausted once.

    Args:
        pattern: optional fnmatch-style glob (e.g. "orders_*.json") to only
            move matching files back; omit to move everything found.

    Returns {"moved": [{"file", "destination", "status"} or
    {"file", "status": "failed", "error"}], "count": <successfully moved>}.
    """
    quarantine_dir = f"{source_dir.rstrip('/')}/quarantine_files"

    try:
        files = list_json_files(spark, quarantine_dir)
    except FileNotFoundError:
        logger.info(
            "No quarantine_files/ directory found under %s - nothing to replay.", source_dir
        )
        return {"moved": [], "count": 0}

    if pattern is not None:
        files = [f for f in files if fnmatch.fnmatch(os.path.basename(f), pattern)]

    if not files:
        logger.info(
            "No quarantined files found in %s%s - nothing to replay.",
            quarantine_dir,
            f" matching {pattern!r}" if pattern else "",
        )
        return {"moved": [], "count": 0}

    retry_state = RetryState.load(source_dir)
    moved = []
    for file_path in files:
        filename = file_path.rsplit("/", 1)[-1]
        dest_path = f"{source_dir.rstrip('/')}/{filename}"
        try:
            move_file_direct(file_path, dest_path)
            retry_state.clear(dest_path)
            retry_state.clear(file_path)
            moved.append({"file": file_path, "destination": dest_path, "status": "moved"})
            logger.info(
                "Restored quarantined file %s -> %s for reprocessing.", file_path, dest_path
            )
        except Exception as exc:  # noqa: BLE001 - per-file isolation; the failure is reported in the results list
            logger.error("Failed to restore quarantined file %s: %s", file_path, exc)
            moved.append({"file": file_path, "status": "failed", "error": str(exc)})

    retry_state.flush()
    return {"moved": moved, "count": sum(1 for m in moved if m["status"] == "moved")}
