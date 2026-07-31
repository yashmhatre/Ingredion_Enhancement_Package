"""
Filesystem discovery: which files and folders are there.

Split out of `directory_ingestion` (#151). Depends only on `databricks_fs`
and `paths` - it knows nothing about ingestion, tables or configs, which is
why it does not belong in a module about orchestrating them.
"""

import os
from typing import List, Optional

from ..databricks_fs import list_entries
from .paths import local_path_from_uri


def _try_dbutils_ls(source_dir: str) -> Optional[List[str]]:
    """Databricks-native file listing - works on ALL Databricks compute,
    including serverless (where spark._jvm is blocked). Returns None if
    Databricks isn't available at all (e.g. local pytest runs); raises if it
    is available and the listing genuinely fails. See databricks_fs.py."""
    entries = list_entries(source_dir)
    if entries is None:
        return None

    return sorted(
        e.path for e in entries if not e.is_dir and e.name.lower().endswith((".json", ".jsonl"))
    )


def _try_posix_ls(source_dir: str) -> Optional[List[str]]:
    """File listing via os.listdir for POSIX-style paths: local file:/ paths
    and FUSE-mounted locations like /Volumes/... . Returns None if the path
    isn't visible as a local directory."""
    local = local_path_from_uri(source_dir)
    if not os.path.isdir(local):
        return None
    # "/" explicitly, not os.path.join: these are URI-shaped strings that the
    # rest of the module joins and splits on "/", and os.path.join would use
    # "\\" on Windows (#74).
    return sorted(
        f"{source_dir.rstrip('/')}/{f}"
        for f in os.listdir(local)
        if f.lower().endswith((".json", ".jsonl")) and os.path.isfile(os.path.join(local, f))
    )


def _try_dbutils_ls_dirs(source_dir: str) -> Optional[List[str]]:
    """Lists immediate subdirectories. Returns None if Databricks isn't
    available. Directory detection uses the authoritative `is_dir` flag
    rather than a trailing-slash convention - see databricks_fs.py."""
    entries = list_entries(source_dir)
    if entries is None:
        return None
    return sorted(e.path.rstrip("/") for e in entries if e.is_dir)


def _try_posix_ls_dirs(source_dir: str) -> Optional[List[str]]:
    """Lists immediate subdirectories via os.listdir for local/FUSE paths."""
    local = local_path_from_uri(source_dir)
    if not os.path.isdir(local):
        return None
    return sorted(
        f"{source_dir.rstrip('/')}/{d}"
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
            str(status.getPath().toString()) for status in statuses if status.isDirectory()
        )

    return [
        d
        for d in dirs
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
            if status.isFile()
            and str(status.getPath().getName()).lower().endswith((".json", ".jsonl"))
        )

    if max_files is not None:
        files = files[:max_files]
    return files
