"""
Directory-level ingestion: discover JSON files in a directory and load each
one into its own bronze table, with the table name derived from the filename
via a configurable template (e.g. "{filename}_bronze" or "bronze_{filename}").

Usage (notebook):

    from bronze_ingest import ingest_directory_to_bronze

    results = ingest_directory_to_bronze(
        spark,
        source_dir="/Volumes/main/default/raw_json/",
        catalog="main",
        schema_name="bronze",
        table_name_template="{filename}_bronze",   # or "bronze_{filename}"
        flatten_mode="auto",
    )

Each file is processed independently: one bad file is logged and reported in
the results list, but does not stop the remaining files from loading.
"""

import os
import re
from typing import Dict, Any, List, Optional
import shutil
import json as _json
from datetime import datetime, timezone
from .config import IngestionConfig
from .logging_utils import logger
from .json_reader import read_json
from concurrent.futures import ThreadPoolExecutor


def sanitize_table_name(filename: str) -> str:
    """
    Converts a filename into a valid Databricks/Unity Catalog table name:
      orders-2026 Jan.json -> orders_2026_jan
    Rules: strip extension, lowercase, replace non [a-z0-9_] with '_',
    collapse repeats, prefix 't_' if it starts with a digit.
    """
    name = os.path.splitext(os.path.basename(filename))[0]
    name = re.sub(r"[^0-9a-zA-Z_]", "_", name).lower()
    name = re.sub(r"_+", "_", name).strip("_")
    if not name:
        raise ValueError(f"Filename {filename!r} produced an empty table name")
    if name[0].isdigit():
        name = f"t_{name}"
    return name


def build_table_name(filename: str, template: str = "{filename}_bronze") -> str:
    """
    Applies the naming template. The template must contain '{filename}'.
      template="{filename}_bronze"  -> orders_bronze
      template="bronze_{filename}"  -> bronze_orders
    """
    if "{filename}" not in template:
        raise ValueError("table_name_template must contain '{filename}'")
    return template.replace("{filename}", sanitize_table_name(filename))


def _try_dbutils_ls(source_dir: str) -> Optional[List[str]]:
    """File listing via dbutils.fs.ls - works on ALL Databricks compute,
    including serverless (where spark._jvm is blocked). Returns None if
    dbutils isn't available (e.g. local pytest runs)."""
    try:
        import IPython
        dbutils = IPython.get_ipython().user_ns["dbutils"]  # type: ignore[union-attr]
    except Exception:
        return None

    try:
        entries = dbutils.fs.ls(source_dir)
    except Exception as exc:
        if "FileNotFoundException" in str(exc) or "does not exist" in str(exc).lower() or "No such file" in str(exc):
            raise FileNotFoundError(f"source_dir does not exist: {source_dir}") from exc
        raise

    return sorted(
        e.path
        for e in entries
        if not e.path.endswith("/") and e.name.lower().endswith((".json", ".jsonl"))
    )


def _try_posix_ls(source_dir: str) -> Optional[List[str]]:
    """File listing via os.listdir for POSIX-style paths: local file:/ paths
    and FUSE-mounted locations like /Volumes/... . Returns None if the path
    isn't visible as a local directory."""
    local = source_dir[len("file://"):] if source_dir.startswith("file://") else source_dir
    if not os.path.isdir(local):
        return None
    return sorted(
        os.path.join(source_dir.rstrip("/"), f)
        for f in os.listdir(local)
        if f.lower().endswith((".json", ".jsonl")) and os.path.isfile(os.path.join(local, f))
    )


def _try_dbutils_ls_dirs(source_dir: str) -> Optional[List[str]]:
    """Lists immediate subdirectories via dbutils.fs.ls. Returns None if
    dbutils isn't available."""
    try:
        import IPython
        dbutils = IPython.get_ipython().user_ns["dbutils"]
    except Exception:
        return None
    entries = dbutils.fs.ls(source_dir)
    return sorted(e.path.rstrip("/") for e in entries if e.path.endswith("/"))


def _try_posix_ls_dirs(source_dir: str) -> Optional[List[str]]:
    """Lists immediate subdirectories via os.listdir for local/FUSE paths."""
    local = source_dir[len("file://"):] if source_dir.startswith("file://") else source_dir
    if not os.path.isdir(local):
        return None
    return sorted(
        os.path.join(source_dir.rstrip("/"), d)
        for d in os.listdir(local)
        if os.path.isdir(os.path.join(local, d))
    )


def list_subfolders(spark, source_dir: str) -> List[str]:
    """
    Lists immediate (one-level-deep) subdirectories of source_dir. Each
    one is treated as a folder-as-table unit by ingest_directory_to_bronze.
    Excludes the reserved _state/ folder (retry-state tracking) and any
    folder starting with an underscore, to avoid accidentally treating
    internal bookkeeping folders as data.
    """
    dirs = _try_dbutils_ls_dirs(source_dir)
    if dirs is None:
        dirs = _try_posix_ls_dirs(source_dir)
    if dirs is None:
        jvm = spark._jvm
        hadoop_conf = spark._jsc.hadoopConfiguration()
        path = jvm.org.apache.hadoop.fs.Path(source_dir)
        fs = path.getFileSystem(hadoop_conf)
        if not fs.exists(path):
            raise FileNotFoundError(f"source_dir does not exist: {source_dir}")
        statuses = fs.listStatus(path)
        dirs = sorted(
            str(status.getPath().toString())
            for status in statuses
            if status.isDirectory()
        )

    return [
        d for d in dirs
        if not os.path.basename(d.rstrip("/")).startswith("_")
        and os.path.basename(d.rstrip("/")) not in ("processed", "quarantine_files")
    ]


def list_json_files(spark, source_dir: str, max_files: Optional[int] = None) -> List[str]:
    """
    Lists .json files in source_dir (non-recursive).

    Strategy, in order:
      1. dbutils.fs.ls - available on every Databricks compute type,
         including serverless.
      2. os.listdir - for local paths and FUSE mounts (/Volumes/...),
         also covers local pytest runs.
      3. Hadoop FileSystem API via spark._jvm - classic clusters only
         (serverless blocks _jvm), needed for direct cloud URIs like
         abfss:// or s3:// when dbutils isn't available.
    """
    files = _try_dbutils_ls(source_dir)

    if files is None:
        files = _try_posix_ls(source_dir)

    if files is None:
        # Classic-cluster / spark-submit fallback for cloud URIs.
        jvm = spark._jvm
        hadoop_conf = spark._jsc.hadoopConfiguration()
        path = jvm.org.apache.hadoop.fs.Path(source_dir)
        fs = path.getFileSystem(hadoop_conf)
        if not fs.exists(path):
            raise FileNotFoundError(f"source_dir does not exist: {source_dir}")
        statuses = fs.listStatus(path)
        files = sorted(
            str(status.getPath().toString())
            for status in statuses
            if status.isFile() and str(status.getPath().getName()).lower().endswith((".json", ".jsonl"))
        )

    if max_files is not None:
        files = files[:max_files]
    return files

def _move_file_direct(src_path: str, dest_path: str) -> None:
    """
    Moves a file from src_path to dest_path directly (both absolute),
    using dbutils.fs.mv when available (works on all Databricks compute,
    including UC Volumes and cloud paths), falling back to shutil.move for
    local/pytest paths. Raises on failure - caller decides how to handle
    it; this function does not swallow errors.
    """
    try:
        import IPython
        dbutils = IPython.get_ipython().user_ns["dbutils"]
        dbutils.fs.mv(src_path, dest_path)
    except Exception:
        # No dbutils available (not installed, no active kernel, or missing
        # from user_ns) - local/pytest environment. Broad catch is
        # deliberate: any failure to obtain a working dbutils should fall
        # through to the local move below.
        local_src = src_path[len("file://"):] if src_path.startswith("file://") else src_path
        local_dest = dest_path[len("file://"):] if dest_path.startswith("file://") else dest_path
        os.makedirs(os.path.dirname(local_dest), exist_ok=True)
        shutil.move(local_src, local_dest)


def _move_file(source_dir: str, file_path: str, dest_subfolder: str, relative_subpath: str = "") -> str:
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
    filename = file_path.rsplit("/", 1)[-1]
    subpath = f"{relative_subpath.strip('/')}/" if relative_subpath else ""
    dest_path = f"{source_dir.rstrip('/')}/{dest_subfolder}/{subpath}{filename}"
    _move_file_direct(file_path, dest_path)
    return dest_path


def _archive_ingested_file(source_dir: str, file_path: str, relative_subpath: str = "") -> Dict[str, str]:
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
        dest = _move_file(source_dir, file_path, f"processed/{today}", relative_subpath=relative_subpath)
        logger.info("Archived %s -> %s", file_path, dest)
        return {"move_status": "moved", "move_detail": dest}
    except Exception as move_exc:
        logger.warning("Failed to archive %s (%s) - attempting quarantine", file_path, move_exc)
        try:
            dest = _move_file(source_dir, file_path, "quarantine_files", relative_subpath=relative_subpath)
            logger.warning("Quarantined %s -> %s (original archive move failed)", file_path, dest)
            return {"move_status": "quarantined", "move_detail": dest}
        except Exception as quarantine_exc:
            logger.error(
                "Failed to archive or quarantine %s - left in place for manual review (backlog): %s",
                file_path, quarantine_exc,
            )
            return {"move_status": "failed_left_in_place", "move_detail": str(quarantine_exc)}

_ARCHIVE_MAX_WORKERS = 10


def _archive_files_parallel(source_dir, file_paths, relative_subpath=""):
    """
    Archives multiple files concurrently. Each dbutils.fs.mv / shutil.move
    is independent, so these parallelize safely - benchmarking showed
    sequential archival at ~0.5s per file was the dominant linear cost in
    folder ingestion (9.4x scaling for 10x files, vs ~4x for read/write).

    Returns a list of (file_path, move_result_dict) tuples in the same
    order as file_paths, so per-file error attribution is preserved
    despite concurrent execution.

    _archive_ingested_file never raises (it handles its own failures and
    returns a status dict), so no exception handling is needed here.
    """
    if not file_paths:
        return []

    workers = min(_ARCHIVE_MAX_WORKERS, len(file_paths))
    with ThreadPoolExecutor(max_workers=workers) as pool:
        results = list(pool.map(
            lambda fp: _archive_ingested_file(source_dir, fp, relative_subpath=relative_subpath),
            file_paths,
        ))

    return list(zip(file_paths, results))

_RETRY_STATE_SUBFOLDER = "_state"
_RETRY_STATE_FILENAME = "retry_state.json"

def _ingest_folder_as_table(spark, source_dir, folder_path, table, shared_config, stop_on_error, max_ingestion_retries):
    """
    Handles a folder-as-table plan item: reads every file inside folder_path
    individually and validates each with a count() (catches bad files early,
    without needing .cache()/.persist() - not supported on serverless
    compute). Successfully-validated files are unioned and written as one
    table via BronzeIngestion.run_on_dataframe(), which re-reads from the
    original file paths (Spark DataFrames are lazy). Only AFTER that write
    succeeds are files archived - this ordering means we never move a file
    out from under a read that hasn't happened yet.

    Archival/quarantine happens per file, with the folder name preserved as
    relative_subpath, so files land at processed/{date}/{folder_name}/{file}
    instead of losing their folder context.

    Returns one result dict summarizing the whole unit, with a nested
    file_results list giving the per-file breakdown.
    """
    from .pipeline import BronzeIngestion

    folder_name = os.path.basename(folder_path.rstrip("/"))
    inner_files = list_json_files(spark, folder_path)

    if not inner_files:
        # A folder with nothing to ingest is not an error - there is no bad
        # data, no unreadable file, and nothing a human needs to fix. Marking
        # it "failed" made the job task exit non-zero and fire alerting for a
        # directory that simply had no JSON in it, which also masked genuine
        # failures in the same run. Reported as its own status so it is
        # neither counted as a success nor treated as a failure.
        logger.warning("Folder %s contains no JSON files - skipping.", folder_path)
        return {
            "file": folder_path, "table": table, "status": "skipped",
            "reason": "no JSON files in folder",
        }

    validated_dataframes = []
    validated_file_paths = []
    file_results = []
    retry_state = _read_retry_state(source_dir)

    for file_path in inner_files:
        try:
            cfg = IngestionConfig.from_dict({**shared_config, "source_path": file_path, "table": table})
            df = read_json(spark, cfg)
            df.count()  # eagerly validate this file is actually readable,
                        # without persisting - files stay in place, safe to
                        # re-read again later at final write time
            validated_dataframes.append(df)
            validated_file_paths.append(file_path)
        except Exception as exc:
            logger.error("Failed to read %s (inside folder %s): %s", file_path, folder_path, exc)
            if stop_on_error:
                raise

            attempts = retry_state.get(file_path, 0) + 1
            if attempts >= max_ingestion_retries:
                retry_state.pop(file_path, None)
                try:
                    dest = _move_file(source_dir, file_path, "quarantine_files", relative_subpath=folder_name)
                    file_results.append({
                        "file": file_path, "status": "failed", "error": str(exc), "attempts": attempts,
                        "move_status": "quarantined", "move_detail": dest,
                    })
                except Exception as move_exc:
                    file_results.append({
                        "file": file_path, "status": "failed", "error": str(exc), "attempts": attempts,
                        "move_status": "failed_left_in_place", "move_detail": str(move_exc),
                    })
            else:
                retry_state[file_path] = attempts
                file_results.append({
                    "file": file_path, "status": "failed", "error": str(exc), "attempts": attempts,
                })

    _write_retry_state(source_dir, retry_state)

    if not validated_dataframes:
        logger.error("All files in folder %s failed to read - no table written.", folder_path)
        return {
            "file": folder_path, "table": table, "status": "failed",
            "error": "all files in folder failed", "file_results": file_results,
        }

    merged_df = validated_dataframes[0]
    for df in validated_dataframes[1:]:
        merged_df = merged_df.unionByName(df, allowMissingColumns=True)

    try:
        cfg = IngestionConfig.from_dict({**shared_config, "source_path": folder_path, "table": table})
        summary = BronzeIngestion(spark, cfg).run_on_dataframe(merged_df)
    except Exception as exc:
        logger.error("Failed to write merged table for folder %s: %s", folder_path, exc)
        if stop_on_error:
            raise
        return {
            "file": folder_path, "table": table, "status": "failed",
            "error": str(exc), "file_results": file_results,
        }

    # Write succeeded - now safe to archive the validated files, since
    # Spark has already finished reading them. Archival is parallelized:
    # benchmarking showed sequential moves at ~0.5s/file were the dominant
    # linear cost (9.4x scaling for 10x files, vs ~4x for read/write).
    retry_state = _read_retry_state(source_dir)
    for file_path in validated_file_paths:
        retry_state.pop(file_path, None)
    _write_retry_state(source_dir, retry_state)

    for file_path, move_result in _archive_files_parallel(
        source_dir, validated_file_paths, relative_subpath=folder_name
    ):
        file_results.append({"file": file_path, "status": "success", **move_result})

    return {
        "file": folder_path,
        "table": summary["table"],
        "status": "success",
        "rows": summary["row_count"],
        "quarantined_rows": summary.get("quarantined_row_count", 0),
        "file_results": file_results,
    }

    
def _retry_state_path(source_dir: str) -> str:
    return f"{source_dir.rstrip('/')}/{_RETRY_STATE_SUBFOLDER}/{_RETRY_STATE_FILENAME}"


def _read_retry_state(source_dir: str) -> Dict[str, int]:
    """Reads the persisted {file_path: consecutive_failure_count} map.
    Returns an empty dict if the state file doesn't exist yet (first run)
    or can't be parsed - never raises, since losing retry counts is a
    minor issue and should not block ingestion."""
    path = _retry_state_path(source_dir)
    try:
        import IPython
        dbutils = IPython.get_ipython().user_ns["dbutils"]
        content = dbutils.fs.head(path, 1_000_000)
    except Exception:
        # No dbutils, or file doesn't exist via dbutils - try local read.
        local_path = path[len("file://"):] if path.startswith("file://") else path
        try:
            with open(local_path, "r") as f:
                content = f.read()
        except Exception:
            return {}

    try:
        return _json.loads(content)
    except Exception:
        logger.warning("Could not parse retry state at %s - starting fresh.", path)
        return {}


def _write_retry_state(source_dir: str, state: Dict[str, int]) -> None:
    """Writes the retry-state map back. Never raises - a failure to persist
    retry counts should not fail the ingestion run itself."""
    path = _retry_state_path(source_dir)
    content = _json.dumps(state)

    try:
        import IPython
        dbutils = IPython.get_ipython().user_ns["dbutils"]
        dbutils.fs.put(path, content, overwrite=True)
        return
    except Exception:
        pass

    try:
        local_path = path[len("file://"):] if path.startswith("file://") else path
        os.makedirs(os.path.dirname(local_path), exist_ok=True)
        with open(local_path, "w") as f:
            f.write(content)
    except Exception as exc:
        logger.warning("Could not persist retry state to %s: %s", path, exc)

def ingest_directory_to_bronze(
        spark,
        source_dir: str,
        table_name_template: str = "{filename}_bronze",
        max_files: Optional[int] = None,
        stop_on_error: bool = False,
        max_ingestion_retries: int = 3,
        allow_overwrite_in_directory_mode: bool = False,
        base_config: Optional[Dict[str, Any]] = None,
        **config_overrides,
    ) -> List[Dict[str, Any]]:
    """
    Discovers JSON files in source_dir and loads each into its own bronze
    table named via table_name_template.

    Args:
        spark: active SparkSession.
        source_dir: directory containing the .json files (any Spark-readable
            path: /Volumes/..., dbfs:/, abfss://, s3://, gs://, file:/...).
        table_name_template: "{filename}_bronze" (default) or
            "bronze_{filename}" - anything containing '{filename}'.
        max_files: optionally cap how many files to process (e.g. 20).
        stop_on_error: if True, the first failing file raises and stops the
            run; if False (default), failures are recorded per-file and the
            remaining files still load.
        allow_overwrite_in_directory_mode: write_mode="overwrite" is
            rejected for directory/folder-as-table ingestion by default -
            each newly-discovered file (or folder union) is written to its
            table, and a fresh file lands under the same name on every
            future run since successfully-ingested files are archived out
            of source_dir. With overwrite, each such run would silently
            replace that table's entire contents, so the table only ever
            holds the most recently ingested file's rows - defeating the
            point of incrementally discovering files over time. Set True
            only if you genuinely want full-refresh-per-run semantics
            (e.g. a folder that's always fully repopulated before a run).
        base_config: optional dict of IngestionConfig fields shared by every
            file (catalog, schema_name, flatten_mode, required_columns, ...).
        **config_overrides: same as base_config, as keyword args (take
            precedence over base_config). 'source_path' and 'table' are set
            per-file and cannot be overridden here.

    Returns:
        A list of per-unit result dicts (one per file, or one per folder in
        folder-as-table mode):
        {"file", "table", "status": "success"|"failed"|"skipped",
         "rows" | "error" | "reason"}

        "skipped" means there was nothing to ingest and nothing to fix -
        currently only a folder containing no JSON files. Callers deciding
        whether to fail a job task should test for "failed" explicitly
        rather than treating anything that isn't "success" as a failure.
    """
    # Imported here to avoid a circular import (pipeline imports nothing from
    # this module, but keeping the dependency one-directional at import time).
    from .pipeline import BronzeIngestion

    shared: Dict[str, Any] = dict(base_config or {})
    shared.update(config_overrides)
    for forbidden in ("source_path", "table"):
        if forbidden in shared:
            raise ValueError(f"{forbidden!r} is derived per file and cannot be set for directory ingestion")

    if shared.get("write_mode") == "overwrite" and not allow_overwrite_in_directory_mode:
        raise ValueError(
            "write_mode='overwrite' is not allowed for directory/folder-as-table ingestion "
            "by default - each newly-discovered file (or folder union) would silently replace "
            "its table's entire contents on every run, so the table would only ever hold the "
            "most recently ingested file's rows. Use write_mode='append' or 'merge' instead, "
            "or pass allow_overwrite_in_directory_mode=True if you specifically want "
            "full-refresh-per-run semantics."
        )

    files = list_json_files(spark, source_dir, max_files=max_files)
    subfolders = list_subfolders(spark, source_dir)
    logger.info(
        "Discovered %d JSON file(s) and %d subfolder(s) in %s",
        len(files), len(subfolders), source_dir,
    )
    if not files and not subfolders:
        logger.warning("No .json files or subfolders found in %s - nothing to do.", source_dir)
        return []

    # Resolve table names up front and de-duplicate collisions deterministically
    # (e.g. 'Orders Jan.json' and 'orders_jan.json' both -> orders_jan).
    seen: Dict[str, int] = {}
    plan = []
    for file_path in files:
        table = build_table_name(file_path, table_name_template)
        if table in seen:
            seen[table] += 1
            table = f"{table}_{seen[table]}"
        else:
            seen[table] = 0
        plan.append({"type": "file", "source": file_path, "table": table})

    for folder_path in subfolders:
        table = build_table_name(folder_path, table_name_template)
        if table in seen:
            seen[table] += 1
            table = f"{table}_{seen[table]}"
        else:
            seen[table] = 0
        plan.append({"type": "folder", "source": folder_path, "table": table})

    results: List[Dict[str, Any]] = []
    for item in plan:
        table = item["table"]

        if item["type"] == "file":
            file_path = item["source"]
            logger.info("Ingesting %s -> %s", file_path, table)
            try:
                cfg = IngestionConfig.from_dict({**shared, "source_path": file_path, "table": table})
                summary = BronzeIngestion(spark, cfg).run()

                retry_state = _read_retry_state(source_dir)
                if file_path in retry_state:
                    retry_state.pop(file_path)
                    _write_retry_state(source_dir, retry_state)

                move_result = _archive_ingested_file(source_dir, file_path)
                results.append({
                    "file": file_path,
                    "table": summary["table"],
                    "status": "success",
                    "rows": summary["row_count"],
                    "quarantined_rows": summary.get("quarantined_row_count", 0),
                    **move_result,
                })
            except Exception as exc:
                logger.error("Failed to ingest %s: %s", file_path, exc)
                if stop_on_error:
                    raise

                retry_state = _read_retry_state(source_dir)
                attempts = retry_state.get(file_path, 0) + 1

                if attempts >= max_ingestion_retries:
                    retry_state.pop(file_path, None)
                    _write_retry_state(source_dir, retry_state)
                    try:
                        dest = _move_file(source_dir, file_path, "quarantine_files")
                        logger.warning(
                            "%s failed ingestion %d time(s) - quarantined to %s", file_path, attempts, dest
                        )
                        results.append({
                            "file": file_path, "table": table, "status": "failed",
                            "error": str(exc), "attempts": attempts,
                            "move_status": "quarantined", "move_detail": dest,
                        })
                    except Exception as move_exc:
                        logger.error(
                            "%s failed ingestion %d time(s) and could not be quarantined: %s",
                            file_path, attempts, move_exc,
                        )
                        results.append({
                            "file": file_path, "table": table, "status": "failed",
                            "error": str(exc), "attempts": attempts,
                            "move_status": "failed_left_in_place", "move_detail": str(move_exc),
                        })
                else:
                    retry_state[file_path] = attempts
                    _write_retry_state(source_dir, retry_state)
                    logger.warning(
                        "%s failed ingestion (attempt %d/%d) - left in raw/ for retry",
                        file_path, attempts, max_ingestion_retries,
                    )
                    results.append({
                        "file": file_path, "table": table, "status": "failed",
                        "error": str(exc), "attempts": attempts,
                    })

        elif item["type"] == "folder":
            folder_path = item["source"]
            folder_result = _ingest_folder_as_table(
                spark, source_dir, folder_path, table, shared,
                stop_on_error=stop_on_error, max_ingestion_retries=max_ingestion_retries,
            )
            results.append(folder_result)

    ok = sum(1 for r in results if r["status"] == "success")
    failed = sum(1 for r in results if r["status"] == "failed")
    skipped = sum(1 for r in results if r["status"] == "skipped")
    logger.info(
        "Directory ingestion finished: %d succeeded, %d failed, %d skipped (of %d unit(s))",
        ok, failed, skipped, len(results),
    )
    return results