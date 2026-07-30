"""
Simple retry decorator with exponential backoff, used to wrap transient
operations (cloud storage reads, table writes that can hit throttling or
concurrent-write conflicts).
"""

import functools
import time

from .logging_utils import logger


def with_retry(
    attempts: int = 3, delay_seconds: float = 10.0, backoff: float = 2.0, exceptions=(Exception,)
):
    """
    Decorator factory. Retries the wrapped function up to `attempts` times,
    waiting `delay_seconds`, then `delay_seconds * backoff`, etc. Re-raises
    the last exception if all attempts fail.
    """

    if attempts < 1:
        # `range(1, attempts + 1)` is empty below 1, so the loop never runs,
        # the function is never CALLED, and control falls through to the
        # re-raise with nothing to re-raise. That surfaced as
        # `TypeError: exceptions must derive from BaseException` - an error
        # about the retry decorator, masking whatever the caller was
        # actually doing (#54).
        #
        # IngestionConfig rejects retry_attempts < 1 at config load, but
        # with_retry is importable and callable directly, so it validates
        # its own argument rather than trusting every caller to have come
        # through a validated config.
        raise ValueError(
            f"attempts must be >= 1, got {attempts}. 1 means 'try once, do not retry'."
        )

    def decorator(fn):
        @functools.wraps(fn)
        def wrapper(*args, **kwargs):
            wait = delay_seconds
            for attempt in range(1, attempts + 1):
                try:
                    return fn(*args, **kwargs)
                except exceptions as exc:  # noqa: PERF203
                    if attempt == attempts:
                        logger.error("%s failed after %d attempt(s): %s", fn.__name__, attempt, exc)
                        raise
                    logger.warning(
                        "%s failed on attempt %d/%d (%s). Retrying in %.1fs...",
                        fn.__name__,
                        attempt,
                        attempts,
                        exc,
                        wait,
                    )
                    time.sleep(wait)
                    wait *= backoff
            # Unreachable: attempts >= 1 is enforced above, so the loop always
            # runs at least once and either returns or re-raises. Kept as an
            # explicit assertion rather than a bare `raise last_exc`, which
            # was the thing that used to fail confusingly.
            raise AssertionError("with_retry loop exited without returning or raising")

        return wrapper

    return decorator
