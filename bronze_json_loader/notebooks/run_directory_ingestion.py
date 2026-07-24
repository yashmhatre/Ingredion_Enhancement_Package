# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze JSON Directory Ingestion - Job Entrypoint
# MAGIC Discovers all .json files in `source_dir` and loads each into its own
# MAGIC bronze table, named from the filename via `table_name_template`
# MAGIC (e.g. orders.json -> orders_bronze). Meant to be run as a scheduled
# MAGIC Databricks Job task - all parameters come from job/task parameters.

# COMMAND ----------

import sys

# Adjust to your deployed path (Repos path if using Git folders).
sys.path.append("/Workspace/Users/fabricyash@gmail.com/Ingredion_Enhancement_Package/bronze_json_loader")

from bronze_json_loader import ingest_directory_to_bronze, get_logger

logger = get_logger()

# COMMAND ----------

dbutils.widgets.text("source_dir", "", "Directory containing JSON files")
dbutils.widgets.text("catalog", "workspace", "Catalog")
dbutils.widgets.text("schema_name", "default", "Target schema")
dbutils.widgets.text("table_name_template", "{filename}_bronze", "Table name template")
dbutils.widgets.dropdown("flatten_mode", "raw", ["raw", "flatten", "auto"], "Flatten mode")
dbutils.widgets.dropdown("write_mode", "append", ["append", "overwrite", "merge"], "Write mode")
dbutils.widgets.dropdown("multiline", "true", ["true", "false"], "Multiline JSON")
dbutils.widgets.text("max_files", "", "Max files (blank = no limit)")
dbutils.widgets.dropdown("stop_on_error", "false", ["true", "false"], "Stop on first error")
dbutils.widgets.text("required_columns", "", "Required columns, comma-separated (optional)")
dbutils.widgets.dropdown("fail_on_quality_error", "true", ["true", "false"], "Fail on quality error (false = quarantine)")
dbutils.widgets.text("per_file_config_json", "", "Per-file overrides as JSON (optional)")

# COMMAND ----------

source_dir = dbutils.widgets.get("source_dir").strip()
if not source_dir:
    raise ValueError("source_dir job parameter is required")

max_files_raw = dbutils.widgets.get("max_files").strip()
max_files = int(max_files_raw) if max_files_raw else None

required_columns_raw = dbutils.widgets.get("required_columns").strip()
required_columns = [c.strip() for c in required_columns_raw.split(",") if c.strip()] if required_columns_raw else []

import json as _json
per_file_raw = dbutils.widgets.get("per_file_config_json").strip()
per_file_config = _json.loads(per_file_raw) if per_file_raw else None

results = ingest_directory_to_bronze(
    spark,
    source_dir=source_dir,
    table_name_template=dbutils.widgets.get("table_name_template").strip(),
    max_files=max_files,
    stop_on_error=dbutils.widgets.get("stop_on_error") == "true",
    per_file_config=per_file_config,
    catalog=dbutils.widgets.get("catalog").strip() or None,
    schema_name=dbutils.widgets.get("schema_name").strip(),
    flatten_mode=dbutils.widgets.get("flatten_mode"),
    write_mode=dbutils.widgets.get("write_mode"),
    multiline=dbutils.widgets.get("multiline") == "true",
    required_columns=required_columns,
    fail_on_quality_error=dbutils.widgets.get("fail_on_quality_error") == "true",
)

# COMMAND ----------

import pandas as pd
summary_df = spark.createDataFrame(pd.DataFrame(results))
display(summary_df)

failed = [r for r in results if r["status"] == "failed"]
logger.info("Directory ingestion: %d succeeded, %d failed", len(results) - len(failed), len(failed))

# Fail the job task if anything failed, so alerting/retries kick in -
# successful tables have already been written and won't be duplicated on
# retry if the underlying issue was per-file.
if failed:
    dbutils.notebook.exit(f"FAILED: {len(failed)}/{len(results)} file(s) failed: {[f['file'] for f in failed]}")

dbutils.notebook.exit(f"SUCCESS: {len(results)} file(s) ingested")

