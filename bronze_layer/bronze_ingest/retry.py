"""
Simple retry decorator with exponential backoff, used to wrap transient
operations (cloud storage reads, table writes that can hit throttling or
concurrent-write conflicts).
"""

import time
import functools

from .logging_utils import logger


def with_retry(attempts: int = 3, delay_seconds: float = 10.0, backoff: float = 2.0, exceptions=(Exception,)):
    """
    Decorator factory. Retries the wrapped function up to `attempts` times,
    waiting `delay_seconds`, then `delay_seconds * backoff`, etc. Re-raises
    the last exception if all attempts fail.
    """
    def decorator(fn):
        @functools.wraps(fn)
        def wrapper(*args, **kwargs):
            last_exc = None
            wait = delay_seconds
            for attempt in range(1, attempts + 1):
                try:
                    return fn(*args, **kwargs)
                except exceptions as exc:  # noqa: PERF203
                    last_exc = exc
                    if attempt == attempts:
                        logger.error(
                            "%s failed after %d attempt(s): %s", fn.__name__, attempt, exc
                        )
                        raise
                    logger.warning(
                        "%s failed on attempt %d/%d (%s). Retrying in %.1fs...",
                        fn.__name__, attempt, attempts, exc, wait,
                    )
                    time.sleep(wait)
                    wait *= backoff
            raise last_exc  # pragma: no cover - unreachable safeguard
        return wrapper
    return decorator
