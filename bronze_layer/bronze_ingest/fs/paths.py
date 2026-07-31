"""
`file://` URI <-> local path conversion.

Split out of `directory_ingestion` (#151). Every filesystem helper needs it
and none of them needs the other three, which was the argument for the
split: that module held four unrelated responsibilities and only one of
them was the orchestration it is named for.
"""

import re


def local_path_from_uri(path: str) -> str:
    r"""
    The local filesystem path for a `file://` URI, on any platform (#74).

    Six call sites did this by hand as `path[len("file://"):]`, which is
    correct on POSIX and wrong on Windows. `file:///tmp/x` yields `/tmp/x`,
    which os.listdir and shutil.move accept. `file:///C:/Users/x` yields
    `/C:/Users/x`, which they do not:

        [WinError 123] The filename, directory name, or volume label
        syntax is incorrect: '/C:'

    The archival path therefore reported `failed_left_in_place` for every
    file locally on Windows - a real bug in package code, not a test
    artifact, though it only reaches this fallback off-Databricks (dbutils
    handles the move where it is available).

    Leaves a non-URI path untouched, so callers can pass either.
    """
    if not path.startswith("file://"):
        return path
    stripped = path[len("file://") :]
    # `/C:/Users/...` -> `C:/Users/...`. A leading slash before a drive
    # letter is part of the URI form, not the path.
    if re.match(r"^/[A-Za-z]:", stripped):
        return stripped[1:]
    return stripped
