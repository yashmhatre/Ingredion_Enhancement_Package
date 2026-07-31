"""
Retry-state persistence: how many times each file has failed to ingest.

Split out of `directory_ingestion` (#151), and reshaped from three module
functions into a class in the same change.

Why a class rather than the three functions it replaces
-------------------------------------------------------
The functions were a lock-free read-modify-write of the whole JSON map,
re-read and re-written once per file inside the ingestion loop - N reads
and N writes of the entire map per run, where N is the number of files.
That was not visible from any single call site; it was emergent from the
loop, which is exactly the kind of cost that survives review.

Holding the map in an object with explicit `load()` and `flush()` makes
both the batching and the concurrency question visible rather than
emergent. Two runs over the same directory still race on this file - the
last writer wins and the loser's counts are lost - and that is now a
property of a named object with a documented lifecycle rather than
something you would have to reconstruct by reading a loop.

Never raises. Losing retry counts is a minor issue and must not block
ingestion; a corrupt or missing file starts from empty.
"""

import json as _json
import os
from typing import Dict

from ..databricks_fs import get_dbutils
from ..logging_utils import logger
from .paths import local_path_from_uri

RETRY_STATE_SUBFOLDER = "_state"
RETRY_STATE_FILENAME = "retry_state.json"


def retry_state_path(source_dir: str) -> str:
    return f"{source_dir.rstrip('/')}/{RETRY_STATE_SUBFOLDER}/{RETRY_STATE_FILENAME}"


class RetryState:
    """
    The `{file_path: consecutive_failure_count}` map for one source
    directory, held in memory for the duration of a run.

    Usage:

        state = RetryState.load(source_dir)
        state.increment(path)        # in-memory
        state.clear(path)            # in-memory
        state.flush()                # one write, at the end

    `flush()` is a no-op when nothing changed, so a run in which every file
    succeeds on the first attempt touches the state file zero times.
    """

    def __init__(self, source_dir: str, counts: Dict[str, int]):
        self.source_dir = source_dir
        self._counts = dict(counts)
        self._dirty = False

    # ---- construction ----

    @classmethod
    def load(cls, source_dir: str) -> "RetryState":
        return cls(source_dir, _read(source_dir))

    # ---- in-memory operations ----

    def attempts(self, file_path: str) -> int:
        return self._counts.get(file_path, 0)

    def increment(self, file_path: str) -> int:
        """Records another failure for this file and returns the new count."""
        self._counts[file_path] = self._counts.get(file_path, 0) + 1
        self._dirty = True
        return self._counts[file_path]

    def clear(self, file_path: str) -> None:
        """Forgets this file's failures - called on success, and by replay
        so a restored file gets a fresh set of attempts rather than counting
        against a limit it already exhausted."""
        if self._counts.pop(file_path, None) is not None:
            self._dirty = True

    def as_dict(self) -> Dict[str, int]:
        return dict(self._counts)

    # ---- persistence ----

    def flush(self) -> None:
        if not self._dirty:
            return
        _write(self.source_dir, self._counts)
        self._dirty = False


def _read(source_dir: str) -> Dict[str, int]:
    path = retry_state_path(source_dir)
    dbutils = get_dbutils()
    content = None

    if dbutils is not None:
        try:
            content = dbutils.fs.head(path, 1_000_000)
        except Exception:  # noqa: BLE001 - a missing retry-state file is the normal first-run case
            # Tolerant even on Databricks, unlike the move/list paths:
            # losing retry counts is explicitly a minor issue.
            return {}
    else:
        local_path = local_path_from_uri(path)
        try:
            with open(local_path) as f:
                content = f.read()
        except Exception:  # noqa: BLE001 - as above, on the POSIX fallback path
            return {}

    try:
        return _json.loads(content)
    except Exception:  # noqa: BLE001 - unparseable state starts fresh rather than blocking the run
        logger.warning("Could not parse retry state at %s - starting fresh.", path)
        return {}


def _write(source_dir: str, state: Dict[str, int]) -> None:
    path = retry_state_path(source_dir)
    content = _json.dumps(state)
    dbutils = get_dbutils()

    if dbutils is not None:
        try:
            dbutils.fs.mkdirs(f"{source_dir.rstrip('/')}/{RETRY_STATE_SUBFOLDER}")
            dbutils.fs.put(path, content, overwrite=True)
            return
        except Exception as exc:  # noqa: BLE001 - losing retry counts is minor; failing the run over it is not
            # Tolerated, but no longer silent: losing retry counts is minor,
            # yet a persistent failure here means the retry limit never
            # triggers and a permanently-broken file is retried forever.
            logger.warning("Could not persist retry state to %s: %s", path, exc)
            return

    local_path = local_path_from_uri(path)
    try:
        os.makedirs(os.path.dirname(local_path), exist_ok=True)
        with open(local_path, "w") as f:
            f.write(content)
    except Exception as exc:  # noqa: BLE001 - as above, on the POSIX fallback path
        logger.warning("Could not persist retry state to %s: %s", path, exc)
