"""
Structured logging for the pipeline. Uses the standard `logging` module so
output shows up correctly in Databricks driver logs / job run logs, and can
be wired into log aggregation (e.g. shipped to a monitoring table or an
external system) without changing call sites.
"""

import logging
import sys

_LOGGER_NAME = "bronze_json_loader"


def get_logger(level=logging.INFO) -> logging.Logger:
    logger = logging.getLogger(_LOGGER_NAME)
    if not logger.handlers:  # avoid duplicate handlers on notebook re-runs
        handler = logging.StreamHandler(sys.stdout)
        formatter = logging.Formatter(
            fmt="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
            datefmt="%Y-%m-%dT%H:%M:%S%z",
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)
    logger.setLevel(level)
    logger.propagate = False
    return logger


logger = get_logger()
