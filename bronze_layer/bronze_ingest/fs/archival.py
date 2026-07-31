"""
Archival: moving ingested files out of the source directory.

Split out of `directory_ingestion` (#151). `replay.py` needs file movement
and has no interest in directory ingestion - it previously reached across
that boundary for `move_file_direct`, an underscore-prefixed name, which
meant any refactor of that module silently broke replay with nothing to
warn about it.
"""

import os
import shutil
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from typing import Dict

from ..databricks_fs import get_dbutils
from ..logging_utils import logger
from .paths import local_path_from_uri


def move_file_direct(src_path: str, dest_path: str) -> None:
    """
    Moves a file from src_path to dest_path directly (both absolute),
    using dbutils.fs.mv when available (works on all Databricks compute,
    including UC Volumes and cloud paths), falling back to shutil.move for
    local/pytest paths. Raises on failure - caller decides how to handle
    it; this function does not swallow errors.
    """
    dbutils = get_dbutils()
    if dbutils is not None:
        # Databricks is available, so a failure here is a real failure -
        # deliberately not caught. Previously any exception fell through to
        # the local move below, which meant a genuine workspace error
        # silently relocated files on the driver's local disk instead.
        dbutils.fs.mv(src_path, dest_path)
        return

    local_src = local_path_from_uri(src_path)
    local_dest = local_path_from_uri(dest_path)
    os.makedirs(os.path.dirname(local_dest), exist_ok=True)
    shutil.move(local_src, local_dest)


def move_file(
    source_dir: str, file_path: str, dest_subfolder: str, relative_subpath: str = ""
) -> str:
    """
    Moves a single file from its current location into `dest_subfolder`
    (relative to source_dir). relative_subpath, if given (e.g. "orders"),
    is preserved between dest_subfolder and the filename - used for files
    ingested as part of a folder-as-table unit, so
    processed/{date}/orders/order1.json keeps its folder context instead
    of flattening to processed/{date}/order1.json.

    Returns the destination path. Raises on failure - caller decides how
    to handle it; this function does not swallow errors.
    """
    filename = file_path.replace("\\", "/").rsplit("/", 1)[-1]
    subpath = f"{relative_subpath.strip('/')}/" if relative_subpath else ""
    dest_path = f"{source_dir.rstrip('/')}/{dest_subfolder}/{subpath}{filename}"
    move_file_direct(file_path, dest_path)
    return dest_path


def archive_ingested_file(
    source_dir: str, file_path: str, relative_subpath: str = ""
) -> Dict[str, str]:
    """
    Moves a successfully-ingested file to processed/{date}/[relative_subpath/].
    If that move fails, falls back to quarantine_files/[relative_subpath/] for
    manual review. If even that fails, the file is left in place (backlog)
    and the failure is logged - data is never silently lost, and ingestion
    of other files is never blocked by one file's move failure.

    relative_subpath preserves folder context for files ingested as part of
    a folder-as-table unit (see _ingest_folder_as_table).

    Returns {"move_status": "moved"|"quarantined"|"failed_left_in_place",
             "move_detail": <destination path or error message>}.
    Never raises.
    """
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    try:
        dest = move_file(
            source_dir, file_path, f"processed/{today}", relative_subpath=relative_subpath
        )
        logger.info("Archived %s -> %s", file_path, dest)
        return {"move_status": "moved", "move_detail": dest}
    except Exception as move_exc:  # noqa: BLE001 - archival failure must not lose the file; quarantine is attempted next
        logger.warning("Failed to archive %s (%s) - attempting quarantine", file_path, move_exc)
        try:
            dest = move_file(
                source_dir, file_path, "quarantine_files", relative_subpath=relative_subpath
            )
            logger.warning("Quarantined %s -> %s (original archive move failed)", file_path, dest)
            return {"move_status": "quarantined", "move_detail": dest}
        except Exception as quarantine_exc:  # noqa: BLE001 - both moves failed - leave the file in place rather than lose it
            logger.error(
                "Failed to archive or quarantine %s - left in place for manual "
                "review (backlog): %s",
                file_path,
                quarantine_exc,
            )
            return {"move_status": "failed_left_in_place", "move_detail": str(quarantine_exc)}


_ARCHIVE_MAX_WORKERS = 10


def archive_files_parallel(source_dir, file_paths, relative_subpath=""):
    """
    Archives multiple files concurrently. Each dbutils.fs.mv / shutil.move
    is independent, so these parallelize safely.

    **On serverless this produces no speedup, and that is measured, not
    assumed.** Archival is the dominant linear cost in folder ingestion
    (~0.45s per file, 9.4x scaling for 10x files vs ~4x for read/write),
    which is why it was parallelized - but the benchmark showed 163.0s with
    10 workers against 161.3s sequential. Logs show files still completing
    in exact input order at consistent ~0.45s intervals: the threads are
    created correctly and serialize below, most likely in the Spark Connect
    gRPC client, which appears to handle one request at a time per session.

    The implementation is kept deliberately - it is correct, costs nothing,
    and would help on any filesystem where moves genuinely parallelize
    (local execution, or if Databricks changes this behaviour). Do not read
    its existence as evidence that archival is parallel on serverless; it
    is not. Full measurement in docs/testing_directory_ingestion.md, which
    owns this benchmark.

    Consequently the single-file path in ingest_directory_to_bronze
    archiving sequentially via archive_ingested_file is immaterial on
    serverless rather than an oversight - there is no speedup being left
    on the table.

    Returns a list of (file_path, move_result_dict) tuples in the same
    order as file_paths, so per-file error attribution is preserved
    despite concurrent execution.

    archive_ingested_file never raises (it handles its own failures and
    returns a status dict), so no exception handling is needed here.
    """
    if not file_paths:
        return []

    workers = min(_ARCHIVE_MAX_WORKERS, len(file_paths))
    with ThreadPoolExecutor(max_workers=workers) as pool:
        results = list(
            pool.map(
                lambda fp: archive_ingested_file(source_dir, fp, relative_subpath=relative_subpath),
                file_paths,
            )
        )

    return list(zip(file_paths, results))
