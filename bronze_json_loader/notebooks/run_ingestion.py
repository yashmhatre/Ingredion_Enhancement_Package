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

import sys

# When shipped as a workspace file alongside this notebook, this makes the
# package importable without a full wheel install. Prefer installing the
# wheel as a cluster/job library in real production use.
sys.path.append("/Workspace/Users/yashmhatre26@gmail.com/Ingredion_Enchancement_Package/bronze_json_loader")  # adjust to your deployed path

# COMMAND ----------

import sys

sys.path.insert(
    0,
    "/Workspace/Users/yashmhatre26@gmail.com/Ingredion_Enchancement_Package/bronze_json_loader"
)

# COMMAND ----------


from bronze_json_loader import BronzeIngestion, IngestionConfig, get_logger

logger = get_logger()

# COMMAND ----------

dbutils.widgets.text("config_path", "", "Path to config YAML/JSON (optional)")
dbutils.widgets.text("source_path", "", "Source path (overrides config)")
dbutils.widgets.text("catalog", "", "Catalog (optional)")
dbutils.widgets.text("schema_name", "bronze", "Target schema")
dbutils.widgets.text("table", "", "Target table")
dbutils.widgets.dropdown("flatten_mode", "auto", ["raw", "flatten", "auto"], "Flatten mode")
dbutils.widgets.dropdown("write_mode", "append", ["append", "overwrite", "merge"], "Write mode")
dbutils.widgets.dropdown("ingestion_mode", "batch", ["batch", "streaming"], "Ingestion mode")
dbutils.widgets.text("checkpoint_location", "", "Checkpoint location (streaming only)")
dbutils.widgets.text("schema_location", "", "Schema location (streaming only)")

# COMMAND ----------

config_path = dbutils.widgets.get("config_path").strip()

overrides = {}
for key in ("source_path", "catalog", "schema_name", "table", "flatten_mode",
            "write_mode", "ingestion_mode", "checkpoint_location", "schema_location"):
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
