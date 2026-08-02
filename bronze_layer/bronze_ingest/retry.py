"""
Retry decorator for genuinely transient operations - cloud-storage reads,
and table writes that can hit throttling or concurrent-write conflicts.

It used to retry everything. The module docstring said "transient", the
default was `exceptions=(Exception,)`, and every call site took the default,
so a config typo and a throttled write were treated identically (#152).
"""

import functools
import random
import time
from typing import Optional

from .errors import PERMANENT_ERRORS
from .logging_utils import logger

#: Substrings that identify a permanent server-side failure.
#:
#: String matching is a compromise, not sloppiness, and it is worth saying
#: why rather than leaving it looking accidental: PySpark surfaces a large
#: family of distinct server-side conditions as one exception type
#: (`AnalysisException`, or a bare `Py4JJavaError`), so the class alone
#: cannot distinguish "this table does not exist" from "this write was
#: throttled". The message is the only signal available.
#:
#: Kept in one place so it can be corrected in one place. Matched
#: case-insensitively against the full exception text.
PERMANENT_MESSAGE_MARKERS = (
    "PERMISSION_DENIED",
    "INSUFFICIENT_PERMISSIONS",
    "UNAUTHORIZED",
    "FORBIDDEN",
    "TABLE_OR_VIEW_NOT_FOUND",
    "SCHEMA_NOT_FOUND",
    "PATH_NOT_FOUND",
    "ANALYSIS_EXCEPTION",
    "ANALYSISEXCEPTION",
    "PARSE_SYNTAX_ERROR",
    "UNRESOLVED_COLUMN",
    "CANNOT_INFER_EMPTY_SCHEMA",
    "DELTA_OPERATION_NOT_ALLOWED",
    "UNSUPPORTED_OPERATION",
)

#: Substrings that identify a genuinely transient condition. Checked FIRST,
#: because a retryable failure whose message happens to contain a permanent
#: marker should still be retried - a `ConcurrentAppendException` naming a
#: table, for instance.
TRANSIENT_MESSAGE_MARKERS = (
    "CONCURRENTAPPEND",
    "CONCURRENTDELETEREAD",
    "CONCURRENTDELETEDELETE",
    "CONCURRENTTRANSACTION",
    "PROTOCOLCHANGED",
    "METADATACHANGED",
    "TOO MANY REQUESTS",
    "SERVICEUNAVAILABLE",
    "SERVICE UNAVAILABLE",
    "SLOWDOWN",
    "THROTTL",
    "TIMEOUT",
    "TIMED OUT",
    "CONNECTION RESET",
    "CONNECTION ABORTED",
    "BROKEN PIPE",
    "EOF OCCURRED",
    " 429",
    " 500",
    " 502",
    " 503",
    " 504",
)


def is_retryable(exc: BaseException) -> bool:
    """
    Whether another attempt at the same operation could plausibly succeed.

    Order matters. Transient markers are checked before permanent ones so a
    Delta concurrency conflict that happens to name a table is not
    misclassified by the word "TABLE" appearing in its message.

    The default answer for an unrecognised failure is **True**. That is the
    deliberate choice: this decorator's job is resilience, and wrongly
    retrying a permanent failure costs a bounded amount of time, while
    wrongly refusing to retry a transient one costs a failed run. The
    classification exists to stop the *known* permanent cases from burning
    the retry budget, not to be an exhaustive taxonomy.
    """
    if isinstance(exc, PERMANENT_ERRORS):
        return False

    text = str(exc).upper()
    if any(marker in text for marker in TRANSIENT_MESSAGE_MARKERS):
        return True
    return not any(marker in text for marker in PERMANENT_MESSAGE_MARKERS)


def with_retry(
    attempts: int = 3,
    delay_seconds: float = 10.0,
    backoff: float = 2.0,
    max_total_seconds: Optional[float] = None,
    jitter: bool = True,
    retry_predicate=is_retryable,
):
    """
    Retries the wrapped function, but only for failures a retry could fix.

    What changed and why (#152)
    ---------------------------
    The previous default caught `Exception` and every call site took it, so
    `_write_core` retried these three times with 10s and 20s sleeps before
    surfacing them: `NullMergeKeyError`, `DuplicateMergeKeyError`, an unknown
    `write_mode`, a missing dedupe order-by column, `PERMISSION_DENIED`, and
    every `AnalysisException`. None of those can change between attempts -
    the data and the config are identical.

    Three costs, all real:

    1. **Compute.** Every permanent failure slept 30 seconds. Directory
       ingestion isolates failures per unit and processes units in sequence,
       so a directory of 50 broken files spent **25 minutes sleeping**.
       Against this repo's own finding that 96% of Databricks spend was
       compute time, that is measurable waste on exactly the path that
       should be cheapest.
    2. **Time-to-signal.** The failure was reported 30 seconds after it was
       known.
    3. **Misleading logs.** Two `Retrying in 10.0s...` warnings for a
       condition that was never going to succeed. An operator reading that
       reasonably concludes the problem was transient.

    `NullMergeKeyError` and `DuplicateMergeKeyError` exist precisely to fail
    fast with actionable messages. Wrapping them in a retry loop contradicted
    their purpose.

    Parameters
    ----------
    max_total_seconds:
        Ceiling on time spent SLEEPING, not on the wrapped call. Without it,
        `attempts=5, delay_seconds=30` is up to 8 minutes of driver sleep
        with no way to bound it. When the next wait would exceed the budget,
        the last failure is raised immediately rather than slept on.
    jitter:
        Full jitter - `sleep(uniform(0, wait))`. Concurrent writers that
        collide on a `ConcurrentAppendException` and retry on identical fixed
        backoff simply collide again, in lockstep. Randomising spreads them.
    retry_predicate:
        Overridable for tests and for callers who know better than the
        default classifier.
    """
    if attempts < 1:
        # `range(1, attempts + 1)` is empty below 1, so the loop never runs,
        # the wrapped function is never CALLED, and control falls through to
        # the tail with nothing to raise (#54).
        raise ValueError(
            f"attempts must be >= 1, got {attempts}. 1 means 'try once, do not retry'."
        )

    def decorator(fn):
        @functools.wraps(fn)
        def wrapper(*args, **kwargs):
            wait = delay_seconds
            slept = 0.0
            for attempt in range(1, attempts + 1):
                try:
                    return fn(*args, **kwargs)
                except Exception as exc:  # noqa: PERF203, BLE001 - classified below, never swallowed
                    if not retry_predicate(exc):
                        # Say WHY. Silence here is what made the old
                        # behaviour hard to notice.
                        logger.error(
                            "%s failed with %s and will NOT be retried - this failure "
                            "is not transient, so another attempt would fail "
                            "identically: %s",
                            fn.__name__,
                            type(exc).__name__,
                            exc,
                        )
                        raise

                    if attempt == attempts:
                        logger.error("%s failed after %d attempt(s): %s", fn.__name__, attempt, exc)
                        raise

                    # nosec B311 - retry jitter, not a security decision. The
                    # value only decides how long to wait before trying again;
                    # nothing derives a secret, token or identifier from it.
                    this_wait = random.uniform(0, wait) if jitter else wait  # nosec B311
                    if max_total_seconds is not None and slept + this_wait > max_total_seconds:
                        logger.error(
                            "%s failed on attempt %d/%d and the retry budget of %.1fs is "
                            "exhausted (%.1fs already spent waiting). Giving up: %s",
                            fn.__name__,
                            attempt,
                            attempts,
                            max_total_seconds,
                            slept,
                            exc,
                        )
                        raise

                    logger.warning(
                        "%s failed on attempt %d/%d with %s (%s). Retrying in %.1fs...",
                        fn.__name__,
                        attempt,
                        attempts,
                        type(exc).__name__,
                        exc,
                        this_wait,
                    )
                    time.sleep(this_wait)
                    slept += this_wait
                    wait *= backoff
            # Unreachable: attempts >= 1 is enforced above, so the loop always
            # returns or raises.
            raise AssertionError("with_retry loop exited without returning or raising")

        return wrapper

    return decorator
