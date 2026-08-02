"""
Filesystem concerns, independent of ingestion (#151).

`directory_ingestion` was 729 lines holding four unrelated responsibilities,
only one of which - orchestration - it was named for. The other three had no
dependency on directory ingestion at all, and `replay.py` was reaching
across the module boundary for three underscore-prefixed names to get at
them, which meant any refactor of that module broke replay silently.

Dependency direction is now strictly one-way:

    directory_ingestion ─┐
                         ├─> fs/* ──> databricks_fs
    replay ──────────────┘     └────> fs/paths

Nothing in here imports `pipeline`, `config` or each other except through
`paths`, so none of it can participate in an import cycle.
"""

from .archival import archive_files_parallel, archive_ingested_file, move_file, move_file_direct
from .discovery import list_json_files, list_subfolders
from .paths import local_path_from_uri
from .retry_state import RetryState, retry_state_path

__all__ = [
    "RetryState",
    "archive_files_parallel",
    "archive_ingested_file",
    "list_json_files",
    "list_subfolders",
    "local_path_from_uri",
    "move_file",
    "move_file_direct",
    "retry_state_path",
]
