"""
Tests for the Databricks filesystem resolver (#114).

The Databricks branches of directory_ingestion were previously untestable -
they depended on `IPython.get_ipython().user_ns["dbutils"]`, which cannot be
produced in a pytest process. Routing everything through databricks_fs makes
both backends injectable, so the SDK path and the notebook path can each be
exercised deliberately.
"""

import pytest

import bronze_ingest.databricks_fs as dfs
from bronze_ingest.fs import discovery

# ---- fakes ----


class _SdkFileInfo:
    """Mirrors databricks.sdk.service.files.FileInfo: explicit is_dir, and
    paths that carry NO trailing slash for directories."""

    def __init__(self, path, is_dir):
        self.path = path
        self.is_dir = is_dir


class _FakeDbfs:
    def __init__(self, entries, error=None):
        self._entries = entries
        self._error = error

    def list(self, path):
        if self._error:
            raise self._error
        return list(self._entries)


class _FakeClient:
    def __init__(self, entries=None, error=None, dbutils=None):
        self.dbfs = _FakeDbfs(entries or [], error)
        self.dbutils = dbutils or object()


class _NotebookFileInfo:
    """Mirrors notebook dbutils.fs.ls: directories marked by trailing slash,
    no is_dir attribute at all."""

    def __init__(self, path):
        self.path = path
        self.name = path.rstrip("/").rsplit("/", 1)[-1]


class _FakeNotebookFs:
    def __init__(self, entries, error=None):
        self._entries = entries
        self._error = error

    def ls(self, path):
        if self._error:
            raise self._error
        return list(self._entries)


class _FakeNotebookDbutils:
    def __init__(self, entries=None, error=None):
        self.fs = _FakeNotebookFs(entries or [], error)


def _no_backends(monkeypatch):
    monkeypatch.setattr(dfs, "_workspace_client", lambda: None)
    monkeypatch.setattr(dfs, "_notebook_dbutils", lambda: None)


# ---- availability vs failure ----


def test_returns_none_when_databricks_unavailable(monkeypatch):
    _no_backends(monkeypatch)
    assert dfs.list_entries("/anything") is None
    assert dfs.get_dbutils() is None


def test_not_found_raises_rather_than_falling_back(monkeypatch):
    """A path that genuinely doesn't exist is an error, not a reason to
    silently try the local filesystem."""
    monkeypatch.setattr(
        dfs,
        "_workspace_client",
        lambda: _FakeClient(error=RuntimeError("RESOURCE_DOES_NOT_EXIST: nope")),
    )
    with pytest.raises(FileNotFoundError):
        dfs.list_entries("/Volumes/missing")


def test_genuine_error_is_not_swallowed(monkeypatch):
    """The old code caught everything and fell through to local behaviour,
    so a real workspace failure was indistinguishable from running locally."""
    monkeypatch.setattr(
        dfs,
        "_workspace_client",
        lambda: _FakeClient(error=RuntimeError("PERMISSION_DENIED")),
    )
    with pytest.raises(RuntimeError, match="PERMISSION_DENIED"):
        dfs.list_entries("/Volumes/forbidden")


# ---- SDK backend ----


def test_sdk_backend_preserves_is_dir(monkeypatch):
    monkeypatch.setattr(
        dfs,
        "_workspace_client",
        lambda: _FakeClient(
            [
                _SdkFileInfo("/Volumes/x/orders.json", False),
                _SdkFileInfo("/Volumes/x/multi_file", True),
            ]
        ),
    )

    entries = dfs.list_entries("/Volumes/x")

    assert entries == [
        dfs.Entry("/Volumes/x/orders.json", "orders.json", False),
        dfs.Entry("/Volumes/x/multi_file", "multi_file", True),
    ]


def test_get_dbutils_prefers_sdk(monkeypatch):
    sentinel = object()
    monkeypatch.setattr(dfs, "_workspace_client", lambda: _FakeClient(dbutils=sentinel))
    monkeypatch.setattr(dfs, "_notebook_dbutils", lambda: "notebook")
    assert dfs.get_dbutils() is sentinel


def test_get_dbutils_falls_back_to_notebook(monkeypatch):
    monkeypatch.setattr(dfs, "_workspace_client", lambda: None)
    monkeypatch.setattr(dfs, "_notebook_dbutils", lambda: "notebook")
    assert dfs.get_dbutils() == "notebook"


# ---- notebook backend ----


def test_notebook_backend_infers_is_dir_from_trailing_slash(monkeypatch):
    monkeypatch.setattr(dfs, "_workspace_client", lambda: None)
    monkeypatch.setattr(
        dfs,
        "_notebook_dbutils",
        lambda: _FakeNotebookDbutils(
            [
                _NotebookFileInfo("dbfs:/x/orders.json"),
                _NotebookFileInfo("dbfs:/x/multi_file/"),
            ]
        ),
    )

    entries = dfs.list_entries("dbfs:/x")

    assert entries == [
        dfs.Entry("dbfs:/x/orders.json", "orders.json", False),
        dfs.Entry("dbfs:/x/multi_file/", "multi_file", True),
    ]


# ---- the regression this refactor exists to prevent ----


def test_subfolders_found_when_sdk_paths_have_no_trailing_slash(monkeypatch):
    """
    The SDK reports directories via an explicit is_dir flag and its paths
    carry no trailing slash. The previous implementation detected folders
    with `e.path.endswith("/")`, so routing listing through the SDK would
    have returned zero subfolders - and because list_source_folders treats
    an empty list as a successful result (only None falls through to the
    next strategy), folder-as-table ingestion would have silently processed
    nothing, with no error and no warning.
    """
    monkeypatch.setattr(
        dfs,
        "_workspace_client",
        lambda: _FakeClient(
            [
                _SdkFileInfo("/Volumes/x/orders", True),
                _SdkFileInfo("/Volumes/x/customers", True),
                _SdkFileInfo("/Volumes/x/loose.json", False),
            ]
        ),
    )

    dirs = discovery._try_dbutils_ls_dirs("/Volumes/x")

    assert dirs == ["/Volumes/x/customers", "/Volumes/x/orders"]


def test_file_listing_excludes_directories(monkeypatch):
    """A directory named `something.json` would pass the suffix filter, so
    the is_dir check is what actually keeps it out."""
    monkeypatch.setattr(
        dfs,
        "_workspace_client",
        lambda: _FakeClient(
            [
                _SdkFileInfo("/Volumes/x/orders.json", False),
                _SdkFileInfo("/Volumes/x/archive.json", True),
                _SdkFileInfo("/Volumes/x/notes.txt", False),
            ]
        ),
    )

    files = discovery._try_dbutils_ls("/Volumes/x")

    assert files == ["/Volumes/x/orders.json"]


def test_listing_returns_none_off_databricks_so_local_path_is_used(monkeypatch):
    _no_backends(monkeypatch)
    assert discovery._try_dbutils_ls("/anything") is None
    assert discovery._try_dbutils_ls_dirs("/anything") is None
