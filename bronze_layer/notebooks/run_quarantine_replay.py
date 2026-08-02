# Databricks notebook source
# MAGIC %md
# MAGIC # Quarantine Replay - Job Entrypoint
# MAGIC Reprocesses previously-quarantined rows and/or files after an upstream
# MAGIC source or quality rule has been fixed. Meant to be run on demand (or as
# MAGIC a scheduled Databricks Job task) once you know a fix has landed - not
# MAGIC part of the normal ingestion schedule.

# COMMAND ----------

# bronze_ingest is installed on the job's compute as a wheel, declared in
# bronze_layer/resources/bronze_ingest_jobs.yml under the task's
# `libraries:`. No sys.path manipulation:
# the wheel is versioned, belongs to no individual's home directory, and
# rolls back with the bundle.
#
# Running this notebook interactively outside a bundle-deployed job? Install
# the same wheel into the session first:
#   %pip install /Volumes/<catalog>/<schema>/<volume>/bronze_ingest-<version>-py3-none-any.whl

from typing import Any, Dict

from bronze_ingest import (
    IngestionConfig,
    get_logger,
    reprocess_quarantine,
    reprocess_quarantined_files,
)

logger = get_logger()

# COMMAND ----------

dbutils.widgets.dropdown("replay_mode", "rows", ["rows", "files", "both"], "What to replay")

# --- Row replay (reads from the quarantine table) ---
dbutils.widgets.text(
    "config_path", "", "Path to config YAML/JSON for the target table (row replay)"
)
dbutils.widgets.text("catalog", "", "Catalog (optional, overrides config)")
dbutils.widgets.text("schema_name", "", "Target schema (overrides config)")
dbutils.widgets.text("table", "", "Target table (overrides config)")
dbutils.widgets.text("batch_id", "", "Only replay rows from this original _batch_id (optional)")
dbutils.widgets.text(
    "since", "", "Only replay rows ingested at/after this ISO timestamp (optional)"
)
# Replay is the operation run after fixing an upstream source, against a
# quarantine table that has been accumulating since the problem started, so
# "replay everything" is both the natural usage and the unbounded case
# (#155). Blank uses the package default; set a number to raise it
# deliberately, or "none" to lift the guard entirely.
dbutils.widgets.text(
    "max_rows",
    "",
    "Refuse to promote more than this many rows (blank = default, 'none' = no limit)",
)

# --- File replay (moves files back from quarantine_files/) ---
dbutils.widgets.text("source_dir", "", "Directory ingestion's source_dir (file replay)")
dbutils.widgets.text(
    "pattern", "", "Only restore files matching this glob (optional, e.g. orders_*.json)"
)

# COMMAND ----------

replay_mode = dbutils.widgets.get("replay_mode")
results = {}

if replay_mode in ("rows", "both"):
    config_path = dbutils.widgets.get("config_path").strip()
    if not config_path:
        raise ValueError("config_path is required for row replay (replay_mode='rows' or 'both')")

    overrides = {}
    for key in ("catalog", "schema_name", "table"):
        val = dbutils.widgets.get(key).strip()
        if val:
            overrides[key] = val

    base_config = IngestionConfig.load(config_path).to_dict()
    base_config.update(overrides)
    config = IngestionConfig.from_dict(base_config)

    batch_id = dbutils.widgets.get("batch_id").strip() or None
    since_raw = dbutils.widgets.get("since").strip()
    since = since_raw or None

    max_rows_raw = dbutils.widgets.get("max_rows").strip().lower()
    # Annotated because the three branches disagree on the value type -
    # absent, explicitly None, or an int - and an unannotated dict takes its
    # type from whichever branch is written first.
    replay_kwargs: Dict[str, Any] = {}
    if max_rows_raw in ("none", "unlimited"):
        replay_kwargs["max_rows"] = None
    elif max_rows_raw:
        replay_kwargs["max_rows"] = int(max_rows_raw)

    row_result = reprocess_quarantine(
        spark, config, batch_id=batch_id, since=since, **replay_kwargs
    )
    logger.info("Row replay result: %s", row_result)
    results["rows"] = row_result

if replay_mode in ("files", "both"):
    source_dir = dbutils.widgets.get("source_dir").strip()
    if not source_dir:
        raise ValueError("source_dir is required for file replay (replay_mode='files' or 'both')")

    pattern = dbutils.widgets.get("pattern").strip() or None
    file_result = reprocess_quarantined_files(spark, source_dir, pattern=pattern)
    logger.info("File replay result: %s", file_result)
    results["files"] = file_result

# COMMAND ----------

dbutils.notebook.exit(str(results))
