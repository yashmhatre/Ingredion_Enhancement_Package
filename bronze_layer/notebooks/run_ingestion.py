# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "2"
# ///
# MAGIC %md
# MAGIC # Bronze JSON Ingestion - Job Entrypoint
# MAGIC Parameterized entrypoint meant to be run as a Databricks Job task
# MAGIC (scheduled, or triggered via file arrival). Reads all parameters from
# MAGIC job/task parameters (widgets), so the same notebook works for every
# MAGIC table/source - only the parameters change per job.

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

from bronze_ingest import BronzeIngestion, IngestionConfig, get_logger

logger = get_logger()

# COMMAND ----------

dbutils.widgets.text("config_path", "", "Path to config YAML/JSON (optional)")
dbutils.widgets.text("source_path", "", "Source path (overrides config)")
dbutils.widgets.text("catalog", "", "Catalog (optional)")
dbutils.widgets.text("schema_name", "bronze", "Target schema")
dbutils.widgets.text("table", "", "Target table")
dbutils.widgets.dropdown("write_mode", "append", ["append", "overwrite", "merge"], "Write mode")
dbutils.widgets.dropdown("ingestion_mode", "batch", ["batch", "streaming"], "Ingestion mode")
dbutils.widgets.text("checkpoint_location", "", "Checkpoint location (streaming only)")
dbutils.widgets.text("schema_location", "", "Schema location (streaming only)")
# batch_id/run_id: pass {{job.run_id}} / {{job.id}}-{{job.run_id}} as the
# base_parameters value in databricks.yml so audit rows and idempotent
# batch writes (#63) can be joined back to a specific Databricks job run
# instead of fuzzy timestamp matching (#52). Left blank, both fall back
# to their usual defaults (auto-generated timestamp / UUID).
dbutils.widgets.text(
    "batch_id", "", "Batch ID (e.g. {{job.run_id}} - blank = auto-generated timestamp)"
)
dbutils.widgets.text(
    "run_id", "", "Audit run ID (e.g. {{job.id}}-{{job.run_id}} - blank = auto-generated UUID)"
)

# COMMAND ----------

config_path = dbutils.widgets.get("config_path").strip()

overrides = {}
for key in (
    "source_path",
    "catalog",
    "schema_name",
    "table",
    "write_mode",
    "ingestion_mode",
    "checkpoint_location",
    "schema_location",
    "batch_id",
    "run_id",
):
    val = dbutils.widgets.get(key).strip()
    if val:
        overrides[key] = val

if config_path:
    base_config = IngestionConfig.load(config_path).to_dict()
    base_config.update(overrides)
    config = IngestionConfig.from_dict(base_config)
else:
    config = IngestionConfig.from_dict(overrides)

logger.info("Resolved config: %s", config.to_dict())

# COMMAND ----------

job = BronzeIngestion(spark, config)

if config.ingestion_mode == "streaming":
    query = job.run_streaming(await_termination=True)
    result = {"query_id": query.id, "status": "completed"}
else:
    result = job.run()

logger.info("Ingestion result: %s", result)
dbutils.notebook.exit(str(result))
