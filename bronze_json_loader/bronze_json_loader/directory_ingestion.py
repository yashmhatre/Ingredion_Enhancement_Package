"""
Directory-level ingestion: discover JSON files in a directory and load each
one into its own bronze table, with the table name derived from the filename
via a configurable template (e.g. "{filename}_bronze" or "bronze_{filename}").

Usage (notebook):

    from bronze_json_loader import ingest_directory_to_bronze

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

from .config import IngestionConfig
from .logging_utils import logger


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


def ingest_directory_to_bronze(
    spark,
    source_dir: str,
    table_name_template: str = "{filename}_bronze",
    max_files: Optional[int] = None,
    stop_on_error: bool = False,
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
        base_config: optional dict of IngestionConfig fields shared by every
            file (catalog, schema_name, flatten_mode, required_columns, ...).
        **config_overrides: same as base_config, as keyword args (take
            precedence over base_config). 'source_path' and 'table' are set
            per-file and cannot be overridden here.

    Returns:
        A list of per-file result dicts:
        {"file", "table", "status": "success"|"failed", "rows"|"error"}
    """
    # Imported here to avoid a circular import (pipeline imports nothing from
    # this module, but keeping the dependency one-directional at import time).
    from .pipeline import BronzeIngestion

    shared: Dict[str, Any] = dict(base_config or {})
    shared.update(config_overrides)
    for forbidden in ("source_path", "table"):
        if forbidden in shared:
            raise ValueError(f"{forbidden!r} is derived per file and cannot be set for directory ingestion")

    files = list_json_files(spark, source_dir, max_files=max_files)
    logger.info("Discovered %d JSON file(s) in %s", len(files), source_dir)
    if not files:
        logger.warning("No .json files found in %s - nothing to do.", source_dir)
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
        plan.append((file_path, table))

    results: List[Dict[str, Any]] = []
    for file_path, table in plan:
        logger.info("Ingesting %s -> %s", file_path, table)
        try:
            cfg = IngestionConfig.from_dict({**shared, "source_path": file_path, "table": table})
            summary = BronzeIngestion(spark, cfg).run()
            results.append({
                "file": file_path,
                "table": summary["table"],
                "status": "success",
                "rows": summary["row_count"],
                "quarantined_rows": summary.get("quarantined_row_count", 0),
            })
        except Exception as exc:
            logger.error("Failed to ingest %s: %s", file_path, exc)
            if stop_on_error:
                raise
            results.append({
                "file": file_path,
                "table": table,
                "status": "failed",
                "error": str(exc),
            })

    ok = sum(1 for r in results if r["status"] == "success")
    logger.info("Directory ingestion finished: %d/%d file(s) succeeded", ok, len(results))
    return results