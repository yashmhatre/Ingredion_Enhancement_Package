# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze JSON Directory Ingestion - Job Entrypoint
# MAGIC Discovers all .json files in `source_dir` and loads each into its own
# MAGIC bronze table, named from the filename via `table_name_template`
# MAGIC (e.g. orders.json -> orders_bronze). Meant to be run as a scheduled
# MAGIC Databricks Job task - all parameters come from job/task parameters.

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

from bronze_ingest import ingest_directory_to_bronze, get_logger

logger = get_logger()

# COMMAND ----------

dbutils.widgets.text("source_dir", "", "Directory containing JSON files")
dbutils.widgets.text("catalog", "workspace", "Catalog")
dbutils.widgets.text("schema_name", "default", "Target schema")
dbutils.widgets.text("table_name_template", "{filename}_bronze", "Table name template")
dbutils.widgets.dropdown("write_mode", "append", ["append", "overwrite", "merge"], "Write mode")
dbutils.widgets.dropdown("multiline", "true", ["true", "false"], "Multiline JSON")
dbutils.widgets.text("max_files", "", "Max files (blank = no limit)")
dbutils.widgets.dropdown("stop_on_error", "false", ["true", "false"], "Stop on first error")
dbutils.widgets.text("required_columns", "", "Required columns, comma-separated (optional)")
dbutils.widgets.dropdown("fail_on_quality_error", "true", ["true", "false"], "Fail on quality error (false = quarantine)")
dbutils.widgets.text("per_file_config_json", "", "Per-file overrides as JSON (optional)")
# batch_id/run_id: pass {{job.run_id}} / {{job.id}}-{{job.run_id}} as the
# base_parameters value in databricks.yml so every file's audit row and
# idempotent batch write (#63) in this job run can be joined back to a
# specific Databricks job run instead of fuzzy timestamp matching (#52).
# Applied uniformly to every file in this run. Left blank, both fall back
# to their usual per-file defaults (auto-generated timestamp / UUID).
dbutils.widgets.text("batch_id", "", "Batch ID (e.g. {{job.run_id}} - blank = auto-generated timestamp)")
dbutils.widgets.text("run_id", "", "Audit run ID (e.g. {{job.id}}-{{job.run_id}} - blank = auto-generated UUID)")

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

id_overrides = {}
for key in ("batch_id", "run_id"):
    val = dbutils.widgets.get(key).strip()
    if val:
        id_overrides[key] = val

results = ingest_directory_to_bronze(
    spark,
    source_dir=source_dir,
    table_name_template=dbutils.widgets.get("table_name_template").strip(),
    max_files=max_files,
    stop_on_error=dbutils.widgets.get("stop_on_error") == "true",
    per_file_config=per_file_config,
    catalog=dbutils.widgets.get("catalog").strip() or None,
    schema_name=dbutils.widgets.get("schema_name").strip(),
    write_mode=dbutils.widgets.get("write_mode"),
    multiline=dbutils.widgets.get("multiline") == "true",
    required_columns=required_columns,
    fail_on_quality_error=dbutils.widgets.get("fail_on_quality_error") == "true",
    **id_overrides,
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

