"""
Directory-level ingestion: discover files in a directory and load each one
into its own bronze table, named from the filename via a configurable
template (e.g. "{filename}_bronze" or "bronze_{filename}").

Usage (notebook):

    from bronze_ingest import ingest_directory_to_bronze

    results = ingest_directory_to_bronze(
        spark,
        source_dir="/Volumes/main/default/raw_json/",
        catalog="main",
        schema_name="bronze",
        table_name_template="{filename}_bronze",
    )

Each unit - a file, or a folder under folder-as-table - is processed
independently: one bad unit is logged and reported in the results list, but
does not stop the rest from loading.

Orchestration only, as of #151. This module was 729 lines holding four
unrelated responsibilities; table naming, filesystem discovery, archival
and retry-state persistence now live in `naming` and `fs/*`, none of which
depends on directory ingestion. The public API is unchanged - the names
this module used to define are re-exported below, so no caller moves.
"""

import os
from typing import Any, Dict, List, Optional

from .config import IngestionConfig
from .fs import (
    RetryState,
    archive_files_parallel,
    archive_ingested_file,
    list_json_files,
    list_subfolders,
    local_path_from_uri,
    move_file,
    move_file_direct,
    retry_state_path,
)
from .json_reader import read_json
from .logging_utils import logger
from .naming import build_table_name, sanitize_table_name

# Re-exported for backwards compatibility. `sanitize_table_name` and
# `build_table_name` are in the package's `__all__`, and the CI wheel check
# asserts the public imports resolve; the rest were reachable and may be
# imported by callers outside this repo. Moving a symbol should not break
# anyone (#151).
__all__ = [
    "RetryState",
    "archive_files_parallel",
    "archive_ingested_file",
    "build_table_name",
    "ingest_directory_to_bronze",
    "list_json_files",
    "list_subfolders",
    "local_path_from_uri",
    "move_file",
    "move_file_direct",
    "retry_state_path",
    "sanitize_table_name",
]


def _ingest_folder_as_table(
    spark, source_dir, folder_path, table, shared_config, stop_on_error, max_ingestion_retries
):
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
            "file": folder_path,
            "table": table,
            "status": "skipped",
            "reason": "no JSON files in folder",
        }

    validated_dataframes = []
    validated_file_paths = []
    file_results = []
    retry_state = RetryState.load(source_dir)

    for file_path in inner_files:
        try:
            cfg = IngestionConfig.from_dict(
                {**shared_config, "source_path": file_path, "table": table}
            )
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

            attempts = retry_state.increment(file_path)
            if attempts >= max_ingestion_retries:
                retry_state.clear(file_path)
                try:
                    dest = move_file(
                        source_dir, file_path, "quarantine_files", relative_subpath=folder_name
                    )
                    file_results.append(
                        {
                            "file": file_path,
                            "status": "failed",
                            "error": str(exc),
                            "attempts": attempts,
                            "move_status": "quarantined",
                            "move_detail": dest,
                        }
                    )
                except Exception as move_exc:  # noqa: BLE001 - one file's move failure must not abandon the rest of the folder
                    file_results.append(
                        {
                            "file": file_path,
                            "status": "failed",
                            "error": str(exc),
                            "attempts": attempts,
                            "move_status": "failed_left_in_place",
                            "move_detail": str(move_exc),
                        }
                    )
            else:
                # increment() above already recorded the attempt; the state
                # file is written once at the end of the run (#151).
                file_results.append(
                    {
                        "file": file_path,
                        "status": "failed",
                        "error": str(exc),
                        "attempts": attempts,
                    }
                )

    retry_state.flush()

    if not validated_dataframes:
        logger.error("All files in folder %s failed to read - no table written.", folder_path)
        return {
            "file": folder_path,
            "table": table,
            "status": "failed",
            "error": "all files in folder failed",
            "file_results": file_results,
        }

    merged_df = validated_dataframes[0]
    for df in validated_dataframes[1:]:
        merged_df = merged_df.unionByName(df, allowMissingColumns=True)

    try:
        cfg = IngestionConfig.from_dict(
            {**shared_config, "source_path": folder_path, "table": table}
        )
        summary = BronzeIngestion(spark, cfg).run_on_dataframe(merged_df)
    except Exception as exc:
        logger.error("Failed to write merged table for folder %s: %s", folder_path, exc)
        if stop_on_error:
            raise
        return {
            "file": folder_path,
            "table": table,
            "status": "failed",
            "error": str(exc),
            "file_results": file_results,
        }

    # Write succeeded - now safe to archive the validated files, since
    # Spark has already finished reading them. Archival is parallelized:
    # benchmarking showed sequential moves at ~0.5s/file were the dominant
    # linear cost (9.4x scaling for 10x files, vs ~4x for read/write).
    retry_state = RetryState.load(source_dir)
    for file_path in validated_file_paths:
        retry_state.clear(file_path)
    retry_state.flush()

    for file_path, move_result in archive_files_parallel(
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


def ingest_directory_to_bronze(
    spark,
    source_dir: str,
    table_name_template: str = "{filename}_bronze",
    max_files: Optional[int] = None,
    stop_on_error: bool = False,
    max_ingestion_retries: int = 3,
    allow_overwrite_in_directory_mode: bool = False,
    base_config: Optional[Dict[str, Any]] = None,
    per_file_config: Optional[Dict[str, Dict[str, Any]]] = None,
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
            raise ValueError(
                f"{forbidden!r} is derived per file and cannot be set for directory ingestion"
            )

    # Reject unknown config keys loudly. IngestionConfig.from_dict filters
    # unrecognised keys silently by design, so anything misspelled or
    # unsupported used to vanish here with no exception, no warning, and a
    # successful-looking run - which is exactly how `per_file_config` was
    # accepted and discarded for the entire life of the deployed job.
    unknown = sorted(set(shared) - set(IngestionConfig.__dataclass_fields__))
    if unknown:
        raise ValueError(
            f"Unknown IngestionConfig field(s) passed to ingest_directory_to_bronze: {unknown}. "
            "These would be silently dropped rather than applied. Check for a typo, or pass "
            "per-file overrides via the per_file_config argument."
        )

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
        len(files),
        len(subfolders),
        source_dir,
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

    # Per-file overrides are keyed by basename (e.g. "orders.json"), matching
    # how the deployed job's per_file_config_json widget is written. Validate
    # the keys up front: an override naming a file that was not discovered is
    # a configured rule that will never run, which is the failure this whole
    # mechanism exists to avoid.
    per_file_config = per_file_config or {}
    for name, overrides in per_file_config.items():
        bad = sorted(set(overrides) - set(IngestionConfig.__dataclass_fields__))
        if bad:
            raise ValueError(
                f"per_file_config[{name!r}] contains unknown IngestionConfig field(s): {bad}."
            )
    discovered_names = {os.path.basename(i["source"].rstrip("/")) for i in plan}
    unmatched = sorted(set(per_file_config) - discovered_names)
    if unmatched:
        logger.warning(
            "per_file_config entries matched no discovered file or folder: %s. "
            "Those overrides will not be applied. Discovered: %s",
            unmatched,
            sorted(discovered_names),
        )

    results: List[Dict[str, Any]] = []
    # One load and (at most) one write for the whole run, instead of a
    # read-modify-write of the entire map per file (#151).
    retry_state = RetryState.load(source_dir)

    for item in plan:
        table = item["table"]
        overrides = per_file_config.get(os.path.basename(item["source"].rstrip("/")), {})
        item_config = {**shared, **overrides}
        if overrides:
            logger.info(
                "Applying per-file config override for %s: %s",
                item["source"],
                sorted(overrides),
            )

        if item["type"] == "file":
            file_path = item["source"]
            logger.info("Ingesting %s -> %s", file_path, table)
            try:
                cfg = IngestionConfig.from_dict(
                    {**item_config, "source_path": file_path, "table": table}
                )
                summary = BronzeIngestion(spark, cfg).run()

                retry_state.clear(file_path)

                move_result = archive_ingested_file(source_dir, file_path)
                results.append(
                    {
                        "file": file_path,
                        "table": summary["table"],
                        "status": "success",
                        "rows": summary["row_count"],
                        "quarantined_rows": summary.get("quarantined_row_count", 0),
                        **move_result,
                    }
                )
            except Exception as exc:
                logger.error("Failed to ingest %s: %s", file_path, exc)
                if stop_on_error:
                    raise

                attempts = retry_state.increment(file_path)

                if attempts >= max_ingestion_retries:
                    retry_state.clear(file_path)
                    try:
                        dest = move_file(source_dir, file_path, "quarantine_files")
                        logger.warning(
                            "%s failed ingestion %d time(s) - quarantined to %s",
                            file_path,
                            attempts,
                            dest,
                        )
                        results.append(
                            {
                                "file": file_path,
                                "table": table,
                                "status": "failed",
                                "error": str(exc),
                                "attempts": attempts,
                                "move_status": "quarantined",
                                "move_detail": dest,
                            }
                        )
                    except Exception as move_exc:  # noqa: BLE001 - per-unit isolation: one unit's failure must not stop the others
                        logger.error(
                            "%s failed ingestion %d time(s) and could not be quarantined: %s",
                            file_path,
                            attempts,
                            move_exc,
                        )
                        results.append(
                            {
                                "file": file_path,
                                "table": table,
                                "status": "failed",
                                "error": str(exc),
                                "attempts": attempts,
                                "move_status": "failed_left_in_place",
                                "move_detail": str(move_exc),
                            }
                        )
                else:
                    # increment() already recorded it; flushed once at the
                    # end of the run rather than here (#151).
                    logger.warning(
                        "%s failed ingestion (attempt %d/%d) - left in raw/ for retry",
                        file_path,
                        attempts,
                        max_ingestion_retries,
                    )
                    results.append(
                        {
                            "file": file_path,
                            "table": table,
                            "status": "failed",
                            "error": str(exc),
                            "attempts": attempts,
                        }
                    )

        elif item["type"] == "folder":
            folder_path = item["source"]
            folder_result = _ingest_folder_as_table(
                spark,
                source_dir,
                folder_path,
                table,
                item_config,
                stop_on_error=stop_on_error,
                max_ingestion_retries=max_ingestion_retries,
            )
            results.append(folder_result)

    # One write for the whole run, and none at all if nothing changed.
    retry_state.flush()

    ok = sum(1 for r in results if r["status"] == "success")
    failed = sum(1 for r in results if r["status"] == "failed")
    skipped = sum(1 for r in results if r["status"] == "skipped")
    logger.info(
        "Directory ingestion finished: %d succeeded, %d failed, %d skipped (of %d unit(s))",
        ok,
        failed,
        skipped,
        len(results),
    )
    return results
