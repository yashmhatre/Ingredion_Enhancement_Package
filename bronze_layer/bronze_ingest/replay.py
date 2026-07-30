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

from pyspark.sql.functions import col, current_timestamp, lit

from .config import IngestionConfig
from .quality import split_good_bad
from .bronze_writer import write_bronze
from .audit import record_replay_run
from .directory_ingestion import _move_file_direct, list_json_files, _read_retry_state, _write_retry_state
from .logging_utils import logger


def reprocess_quarantine(
    spark, config: IngestionConfig, batch_id: Optional[str] = None, since=None,
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

    Returns {"table", "replayed_row_count", "still_quarantined_row_count",
    "replay_batch_id"}.
    """
    quarantine_table = config.resolved_quarantine_table

    if not spark.catalog.tableExists(quarantine_table):
        logger.info("Quarantine table %s does not exist - nothing to replay.", quarantine_table)
        return {
            "table": config.full_table_name, "replayed_row_count": 0,
            "still_quarantined_row_count": 0, "replay_batch_id": None,
        }

    quarantined_df = spark.read.table(quarantine_table)
    if batch_id is not None:
        quarantined_df = quarantined_df.filter(col(f"`{config.audit_batch_id_col}`") == batch_id)
    if since is not None:
        quarantined_df = quarantined_df.filter(col(f"`{config.audit_ingest_ts_col}`") >= since)

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
    )
    good_df, bad_df = split_good_bad(candidate_df, config)

    quarantine_ids = [r["_quarantine_id"] for r in good_df.select("_quarantine_id").collect()]
    good_count = len(quarantine_ids)
    still_bad_count = bad_df.count()

    replay_batch_id = f"replay-{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S%fZ')}"

    if good_count == 0:
        logger.info(
            "Replay for %s: 0 row(s) now pass (%d still quarantined) - nothing promoted.",
            config.full_table_name, still_bad_count,
        )
        record_replay_run(
            spark, config, status="success_replay",
            row_count=0, quarantined_row_count=still_bad_count,
        )
        return {
            "table": config.full_table_name, "replayed_row_count": 0,
            "still_quarantined_row_count": still_bad_count, "replay_batch_id": replay_batch_id,
        }

    replayed_df = (
        good_df.drop("_quarantine_id")
        .withColumn(config.audit_ingest_ts_col, current_timestamp())
        .withColumn(config.audit_batch_id_col, lit(replay_batch_id))
    )
    table_name = write_bronze(spark, replayed_df, config)

    try:
        from delta.tables import DeltaTable

        target = DeltaTable.forName(spark, quarantine_table)
        id_list = ", ".join(f"'{qid}'" for qid in quarantine_ids)
        target.delete(f"_quarantine_id IN ({id_list})")
    except Exception as exc:
        logger.error(
            "Replayed %d row(s) to %s successfully, but failed to remove them from "
            "quarantine table %s: %s. These _quarantine_id(s) may be re-promoted "
            "(and duplicated in bronze) on the next replay unless cleaned up "
            "manually: %s", good_count, table_name, quarantine_table, exc, quarantine_ids,
        )

    record_replay_run(
        spark, config, status="success_replay",
        row_count=good_count, quarantined_row_count=still_bad_count,
    )
    logger.info(
        "Replay for %s: %d row(s) promoted to bronze, %d row(s) still quarantined.",
        config.full_table_name, good_count, still_bad_count,
    )

    return {
        "table": table_name, "replayed_row_count": good_count,
        "still_quarantined_row_count": still_bad_count, "replay_batch_id": replay_batch_id,
    }


def reprocess_quarantined_files(spark, source_dir: str, pattern: Optional[str] = None) -> Dict[str, Any]:
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
        logger.info("No quarantine_files/ directory found under %s - nothing to replay.", source_dir)
        return {"moved": [], "count": 0}

    if pattern is not None:
        files = [f for f in files if fnmatch.fnmatch(os.path.basename(f), pattern)]

    if not files:
        logger.info(
            "No quarantined files found in %s%s - nothing to replay.",
            quarantine_dir, f" matching {pattern!r}" if pattern else "",
        )
        return {"moved": [], "count": 0}

    retry_state = _read_retry_state(source_dir)
    moved = []
    for file_path in files:
        filename = file_path.rsplit("/", 1)[-1]
        dest_path = f"{source_dir.rstrip('/')}/{filename}"
        try:
            _move_file_direct(file_path, dest_path)
            retry_state.pop(dest_path, None)
            retry_state.pop(file_path, None)
            moved.append({"file": file_path, "destination": dest_path, "status": "moved"})
            logger.info("Restored quarantined file %s -> %s for reprocessing.", file_path, dest_path)
        except Exception as exc:
            logger.error("Failed to restore quarantined file %s: %s", file_path, exc)
            moved.append({"file": file_path, "status": "failed", "error": str(exc)})

    _write_retry_state(source_dir, retry_state)
    return {"moved": moved, "count": sum(1 for m in moved if m["status"] == "moved")}
