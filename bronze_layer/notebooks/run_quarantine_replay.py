# Databricks notebook source
# MAGIC %md
# MAGIC # Quarantine Replay - Job Entrypoint
# MAGIC Reprocesses previously-quarantined rows and/or files after an upstream
# MAGIC source or quality rule has been fixed. Meant to be run on demand (or as
# MAGIC a scheduled Databricks Job task) once you know a fix has landed - not
# MAGIC part of the normal ingestion schedule.

# COMMAND ----------

import sys

sys.path.append("/Workspace/Users/fabricyash@gmail.com/Ingredion_Enhancement_Package/bronze_layer")  # adjust to your deployed path

from bronze_ingest import IngestionConfig, reprocess_quarantine, reprocess_quarantined_files, get_logger

logger = get_logger()

# COMMAND ----------

dbutils.widgets.dropdown("replay_mode", "rows", ["rows", "files", "both"], "What to replay")

# --- Row replay (reads from the quarantine table) ---
dbutils.widgets.text("config_path", "", "Path to config YAML/JSON for the target table (row replay)")
dbutils.widgets.text("catalog", "", "Catalog (optional, overrides config)")
dbutils.widgets.text("schema_name", "", "Target schema (overrides config)")
dbutils.widgets.text("table", "", "Target table (overrides config)")
dbutils.widgets.text("batch_id", "", "Only replay rows from this original _batch_id (optional)")
dbutils.widgets.text("since", "", "Only replay rows ingested at/after this ISO timestamp (optional)")

# --- File replay (moves files back from quarantine_files/) ---
dbutils.widgets.text("source_dir", "", "Directory ingestion's source_dir (file replay)")
dbutils.widgets.text("pattern", "", "Only restore files matching this glob (optional, e.g. orders_*.json)")

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

    row_result = reprocess_quarantine(spark, config, batch_id=batch_id, since=since)
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
