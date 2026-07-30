"""
Databricks filesystem access, resolved once and shared by every caller (#114).

Replaces five copies of

    dbutils = IPython.get_ipython().user_ns["dbutils"]

each wrapped in a bare `except` that fell through to local-filesystem calls.
That lookup only works inside a notebook kernel: a Python/wheel task, a job
entrypoint, or a local process authenticated against a workspace has no
IPython user namespace, so those paths silently degraded to local behaviour
in exactly the contexts where that is wrong. It was also untestable - the
Databricks branch could never be exercised deliberately.

Two backends, tried in order:

  1. databricks-sdk's WorkspaceClient - works in notebooks, wheel tasks and
     local processes authenticated to a workspace.
  2. The notebook-injected `dbutils` from the IPython user namespace, kept
     as a fallback for runtimes where the SDK isn't importable or can't
     authenticate.

Availability vs. failure
------------------------
Every function here returns None to mean *Databricks is not available, use
the local filesystem* and raises to mean *Databricks is available and the
operation genuinely failed*. The previous code could not tell those apart:
a real failure looked identical to running locally, so a broken workspace
call quietly wrote to the driver's local disk instead.

Listing does not go through dbutils
-----------------------------------
`w.dbutils.fs.ls()` builds its own FileInfo and **discards `is_dir`**:

    FileInfo(f.path, os.path.basename(f.path), f.file_size, f.modification_time)

and the underlying paths carry no trailing slash. This package detects
directories by trailing slash, so routing listing through it would make
`list_entries` report zero subfolders - and folder-as-table ingestion would
silently process nothing, with no error. `w.dbfs.list()` is used instead: it
yields an explicit `is_dir` boolean and already dispatches across DBFS, UC
Volumes and local paths. That is strictly better than inferring
directory-ness from a string suffix, which is a convention the notebook
dbutils happens to follow and nothing guarantees.
"""

import os
from typing import Any, List, NamedTuple, Optional

from .logging_utils import logger


class Entry(NamedTuple):
    """One filesystem entry. `is_dir` is authoritative, never inferred."""

    path: str
    name: str
    is_dir: bool


def _workspace_client() -> Optional[Any]:
    """
    A databricks-sdk WorkspaceClient, or None if the SDK isn't installed or
    can't authenticate (no config, no workspace). Never raises - an
    unavailable SDK is a normal local-development state, not an error.
    """
    try:
        from databricks.sdk import WorkspaceClient

        return WorkspaceClient()
    except Exception:
        return None


def _notebook_dbutils() -> Optional[Any]:
    """
    The notebook-injected `dbutils`, or None outside a notebook kernel.
    Kept as a fallback for runtimes where the SDK is unavailable.
    """
    try:
        import IPython

        return IPython.get_ipython().user_ns["dbutils"]  # type: ignore[union-attr]
    except Exception:
        return None


def get_dbutils() -> Optional[Any]:
    """
    A `dbutils`-shaped object exposing `.fs` (mv/head/put), or None when not
    running against Databricks. SDK first, notebook namespace second.
    """
    client = _workspace_client()
    if client is not None:
        try:
            return client.dbutils
        except Exception as exc:
            logger.warning("Databricks SDK is available but dbutils could not be obtained: %s", exc)
    return _notebook_dbutils()


def _is_not_found(exc: Exception) -> bool:
    text = str(exc).lower()
    return (
        "filenotfoundexception" in text
        or "does not exist" in text
        or "no such file" in text
        or "resource_does_not_exist" in text
        or "not found" in text
    )


def list_entries(path: str) -> Optional[List[Entry]]:
    """
    Lists the immediate contents of `path`, preserving whether each entry is
    a directory.

    Returns None when Databricks isn't available at all, so callers can fall
    back to local listing. Raises FileNotFoundError when Databricks *is*
    available and the path genuinely doesn't exist - a real error, not a
    reason to silently try the local filesystem.
    """
    client = _workspace_client()
    if client is not None:
        try:
            return [
                Entry(f.path, os.path.basename(f.path.rstrip("/")), bool(f.is_dir))
                for f in client.dbfs.list(path)
            ]
        except Exception as exc:
            if _is_not_found(exc):
                raise FileNotFoundError(f"path does not exist: {path}") from exc
            raise

    dbutils = _notebook_dbutils()
    if dbutils is None:
        return None

    try:
        entries = dbutils.fs.ls(path)
    except Exception as exc:
        if _is_not_found(exc):
            raise FileNotFoundError(f"path does not exist: {path}") from exc
        raise

    # The notebook dbutils marks directories with a trailing slash rather
    # than an explicit flag, so it has to be inferred here - unlike the SDK
    # path above, which reports it directly.
    return [
        Entry(e.path, os.path.basename(e.path.rstrip("/")), e.path.endswith("/")) for e in entries
    ]
