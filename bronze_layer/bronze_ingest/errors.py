"""
Exception types shared across the package.

Exists to break an import cycle (#152). The retry classifier needs to know
that `DataQualityError` and the merge-key errors are permanent, but they
lived in `quality.py` and `bronze_writer.py`, both of which import `retry`.
`retry.py` importing them back would close the loop.

The alternative was checking exception class names as strings, which works
and is worse: it silently stops working when a class is renamed, and there
is nothing to grep for. A module holding only exception classes imports
nothing from the package, so it can never participate in a cycle.

The classes are re-exported from their original modules, so
`from .quality import DataQualityError` and every existing import keep
working - this is a move, not a rename.
"""

from typing import Optional


class DataQualityError(Exception):
    """
    Raised when the quality gate rejects rows and `fail_on_quality_error`
    is set.

    Carries `bad_count` when known, so a failed `audited_run` can record how
    many rows failed instead of leaving `quarantined_row_count` NULL on the
    failure audit row (#50).
    """

    def __init__(self, message: str, bad_count: Optional[int] = None):
        super().__init__(message)
        self.bad_count = bad_count


class NullMergeKeyError(Exception):
    """
    A merge key was NULL on a row about to be merged.

    `NULL = NULL` is NULL, not true, in a SQL MERGE condition - so the row
    would never match the target and would be inserted as a fresh duplicate
    on every run, silently, forever.
    """


class DuplicateMergeKeyError(Exception):
    """
    The source batch contained several rows per merge key, which Delta MERGE
    refuses ("multiple source rows matched").
    """


class WriteNotCommittedError(Exception):
    """
    The write returned without raising, and Delta committed nothing (#366).

    The table version was the same before and after, so the rows are not in
    the table. The known cause is Delta's idempotent-write protection
    discarding a write whose `txnVersion` is at or below one already
    committed for the same `txnAppId`, which it does silently by design.

    Reported as a failure rather than swallowed because everything
    downstream treats a returning write as a written one: the audit row
    records a `row_count`, and the source file is archived out of the
    landing directory. A silent no-op therefore loses the data.
    """


class JsonLinesTruncationError(Exception):
    """
    A streaming micro-batch was read with `multiLine=true` but contains
    JSON-lines files, whose records have therefore already been discarded
    (#146).
    """


#: Exceptions that a retry can never fix, because the input is identical on
#: every attempt. Consulted by `retry.is_retryable`.
#:
#: `ValueError` and `TypeError` are included deliberately and broadly: in
#: this package they are raised by configuration and programming mistakes -
#: an unknown `write_mode`, a missing order-by column, a bad argument - none
#: of which a second attempt changes.
PERMANENT_ERRORS = (
    DataQualityError,
    NullMergeKeyError,
    DuplicateMergeKeyError,
    JsonLinesTruncationError,
    # Retrying changes nothing: the same txnVersion is discarded by the same
    # rule on every attempt, so a retry loop only delays the report (#366).
    WriteNotCommittedError,
    ValueError,
    TypeError,
    KeyError,
    AttributeError,
    ImportError,
    NotImplementedError,
)
