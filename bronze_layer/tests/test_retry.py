"""
Tests for the retry decorator (#152).

`retry.py` had zero dedicated tests - flagged as a gap in
`docs/current_behavior.md` and still open when this was written - while
wrapping every table write and every source read in the package. Nothing
here needs Spark.

The thing under test is a policy decision, not an algorithm: which failures
are worth another attempt. Each class of exception gets a test asserting the
number of attempts made, because "did it retry?" is only observable by
counting calls.
"""

import time

import pytest

from bronze_ingest import retry as retry_module
from bronze_ingest.errors import (
    DataQualityError,
    DuplicateMergeKeyError,
    JsonLinesTruncationError,
    NullMergeKeyError,
)
from bronze_ingest.retry import is_retryable, with_retry


@pytest.fixture(autouse=True)
def no_real_sleeping(monkeypatch):
    """Records what would have been slept instead of sleeping it, so the
    whole file runs instantly and the waits are assertable."""
    slept = []
    monkeypatch.setattr(time, "sleep", slept.append)
    monkeypatch.setattr(retry_module.time, "sleep", slept.append)
    return slept


def _counting(exc, succeed_on=None):
    """A function that raises `exc` until `succeed_on`, counting calls."""
    calls = {"n": 0}

    def fn():
        calls["n"] += 1
        if succeed_on is not None and calls["n"] >= succeed_on:
            return "ok"
        raise exc

    fn.calls = calls
    return fn


# ---------------------------------------------------------------------------
# Permanent failures must fail on the FIRST attempt
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "exc",
    [
        NullMergeKeyError("null merge key"),
        DuplicateMergeKeyError("duplicate merge key"),
        DataQualityError("17 rows failed", bad_count=17),
        JsonLinesTruncationError("jsonl read with multiLine=true"),
        ValueError("Unknown write_mode: sideways"),
        TypeError("bad argument"),
        KeyError("missing"),
    ],
    ids=lambda e: type(e).__name__,
)
def test_permanent_exceptions_are_not_retried(exc, no_real_sleeping):
    """
    These exist to fail fast with actionable messages. Retrying them
    contradicts their purpose and, at the old default, cost 30 seconds each.
    """
    fn = _counting(exc)
    wrapped = with_retry(attempts=3, delay_seconds=10.0)(fn)

    with pytest.raises(type(exc)):
        wrapped()

    assert fn.calls["n"] == 1, "a permanent failure must not be attempted twice"
    assert no_real_sleeping == [], "and must not sleep at all"


@pytest.mark.parametrize(
    "message",
    [
        "PERMISSION_DENIED: user lacks MODIFY on table",
        "INSUFFICIENT_PERMISSIONS",
        "[TABLE_OR_VIEW_NOT_FOUND] The table or view `x` cannot be found",
        "org.apache.spark.sql.AnalysisException: cannot resolve column",
        "PARSE_SYNTAX_ERROR near 'FROM'",
        "[UNRESOLVED_COLUMN] A column with name `nope` cannot be resolved",
    ],
)
def test_permanent_server_side_messages_are_not_retried(message, no_real_sleeping):
    """
    PySpark surfaces many distinct server-side conditions as one exception
    type, so the message is the only available signal. String matching is a
    compromise forced by that, not a shortcut.
    """
    fn = _counting(RuntimeError(message))
    wrapped = with_retry(attempts=3, delay_seconds=10.0)(fn)

    with pytest.raises(RuntimeError):
        wrapped()

    assert fn.calls["n"] == 1
    assert no_real_sleeping == []


def test_non_retried_failure_logs_the_classification(caplog):
    """Silence is what made the old behaviour hard to notice."""
    fn = _counting(NullMergeKeyError("null merge key"))
    wrapped = with_retry(attempts=3)(fn)

    with pytest.raises(NullMergeKeyError):
        wrapped()

    assert "will NOT be retried" in caplog.text
    assert "NullMergeKeyError" in caplog.text


# ---------------------------------------------------------------------------
# Transient failures must still retry
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "message",
    [
        "ConcurrentAppendException: Files were added to the root of the table",
        "ConcurrentDeleteReadException",
        "ProtocolChangedException",
        "Operation failed: 503 Service Unavailable",
        "Request throttled, please retry",
        "HTTP 429 Too Many Requests",
        "java.net.SocketTimeoutException: connection reset",
    ],
)
def test_transient_failures_are_retried_to_exhaustion(message, no_real_sleeping):
    fn = _counting(RuntimeError(message))
    wrapped = with_retry(attempts=3, delay_seconds=1.0)(fn)

    with pytest.raises(RuntimeError):
        wrapped()

    assert fn.calls["n"] == 3
    assert len(no_real_sleeping) == 2  # slept between attempts, not after the last


def test_a_transient_failure_that_then_succeeds_returns_normally(no_real_sleeping):
    fn = _counting(RuntimeError("ConcurrentAppendException"), succeed_on=3)
    wrapped = with_retry(attempts=5, delay_seconds=1.0)(fn)

    assert wrapped() == "ok"
    assert fn.calls["n"] == 3


def test_unrecognised_failures_are_retried_by_default(no_real_sleeping):
    """
    The default answer for an unknown failure is "retry". Wrongly retrying a
    permanent failure costs bounded time; wrongly refusing to retry a
    transient one costs a failed run.
    """
    fn = _counting(RuntimeError("something nobody has seen before"))
    wrapped = with_retry(attempts=2, delay_seconds=1.0)(fn)

    with pytest.raises(RuntimeError):
        wrapped()

    assert fn.calls["n"] == 2


def test_transient_marker_wins_over_a_permanent_one_in_the_same_message():
    """
    Order matters: a Delta concurrency conflict often names the table it
    conflicted on, and must not be misclassified by a permanent marker
    appearing incidentally in that text.
    """
    exc = RuntimeError("ConcurrentAppendException: conflict on TABLE_OR_VIEW_NOT_FOUND_lookalike")
    assert is_retryable(exc) is True


# ---------------------------------------------------------------------------
# Budget and jitter
# ---------------------------------------------------------------------------


def test_total_sleep_is_bounded_by_the_budget(no_real_sleeping):
    """
    attempts=5 with delay_seconds=30 is up to 8 minutes of driver sleep
    without a ceiling. The budget stops it before the sleep, not after.
    """
    fn = _counting(RuntimeError("throttled"))
    wrapped = with_retry(
        attempts=10, delay_seconds=30.0, backoff=2.0, max_total_seconds=45.0, jitter=False
    )(fn)

    with pytest.raises(RuntimeError):
        wrapped()

    assert sum(no_real_sleeping) <= 45.0
    assert fn.calls["n"] < 10, "gave up before exhausting attempts, as budgeted"


def test_exceeding_the_budget_logs_why(caplog, no_real_sleeping):
    fn = _counting(RuntimeError("throttled"))
    wrapped = with_retry(attempts=5, delay_seconds=60.0, max_total_seconds=10.0, jitter=False)(fn)

    with pytest.raises(RuntimeError):
        wrapped()

    assert "retry budget" in caplog.text


def test_jitter_randomises_the_wait(monkeypatch, no_real_sleeping):
    """
    Concurrent writers colliding on a ConcurrentAppendException and retrying
    on identical fixed backoff simply collide again, in lockstep.
    """
    monkeypatch.setattr(retry_module.random, "uniform", lambda a, b: b * 0.5)

    fn = _counting(RuntimeError("throttled"))
    wrapped = with_retry(attempts=3, delay_seconds=10.0, backoff=2.0, jitter=True)(fn)
    with pytest.raises(RuntimeError):
        wrapped()

    # Half of 10, then half of 20 - drawn from the range, not the raw backoff.
    assert no_real_sleeping == [5.0, 10.0]


def test_jitter_can_be_disabled_for_deterministic_backoff(no_real_sleeping):
    fn = _counting(RuntimeError("throttled"))
    wrapped = with_retry(attempts=3, delay_seconds=10.0, backoff=2.0, jitter=False)(fn)
    with pytest.raises(RuntimeError):
        wrapped()

    assert no_real_sleeping == [10.0, 20.0]


# ---------------------------------------------------------------------------
# Guard rails
# ---------------------------------------------------------------------------


def test_attempts_below_one_is_rejected_at_decoration_time():
    """
    #54: range(1, 0) is empty, so the wrapped function was never CALLED and
    control fell through to a re-raise of None - surfacing as "exceptions
    must derive from BaseException", an error about the decorator that hid
    whatever the caller was doing.
    """
    with pytest.raises(ValueError, match="attempts must be >= 1"):
        with_retry(attempts=0)

    with pytest.raises(ValueError, match="attempts must be >= 1"):
        with_retry(attempts=-1)


def test_attempts_of_one_calls_once_and_does_not_sleep(no_real_sleeping):
    fn = _counting(RuntimeError("throttled"))
    wrapped = with_retry(attempts=1)(fn)

    with pytest.raises(RuntimeError):
        wrapped()

    assert fn.calls["n"] == 1
    assert no_real_sleeping == []


def test_wrapper_preserves_the_wrapped_functions_identity():
    @with_retry(attempts=2)
    def _do_write():
        """Writes the thing."""
        return 1

    assert _do_write.__name__ == "_do_write"
    assert _do_write.__doc__ == "Writes the thing."
