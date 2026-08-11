# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze table maintenance — job entrypoint
# MAGIC Runs OPTIMIZE and VACUUM across the tables this package manages.
# MAGIC Retention is never taken below each table's own CDF floor (#58/#159).

# COMMAND ----------

# bronze_ingest is installed on the job's compute as a wheel, declared in
# bronze_layer/resources/maintenance_job.yml under the task's `libraries:`.
from bronze_ingest import get_logger
from bronze_ingest.maintenance import run_maintenance

logger = get_logger()

# COMMAND ----------

dbutils.widgets.text("catalog", "", "Catalog (blank = hive_metastore)")
dbutils.widgets.text("schema_name", "", "Schema holding the tables to maintain")
dbutils.widgets.text("tables", "", "Comma-separated table names (required)")
dbutils.widgets.dropdown("optimize", "true", ["true", "false"], "Run OPTIMIZE")
dbutils.widgets.dropdown("vacuum", "true", ["true", "false"], "Run VACUUM")
# Blank means "use each table's own delta.deletedFileRetentionDuration", which
# is the safe default. A value SHORTER than a table's floor is raised to the
# floor rather than honoured - see maintenance.vacuum_table.
dbutils.widgets.text("vacuum_retention_hours", "", "VACUUM retention hours (blank = table floor)")

# COMMAND ----------

schema_name = dbutils.widgets.get("schema_name").strip()
if not schema_name:
    raise ValueError("schema_name job parameter is required")

raw_tables = dbutils.widgets.get("tables").strip()
if not raw_tables:
    # An explicit list, never a schema scan: discovering tables would make this
    # job's blast radius depend on whatever else lives in the schema, including
    # tables this package did not create.
    raise ValueError(
        "tables job parameter is required - name the tables to maintain explicitly. "
        "This job deliberately does not discover them."
    )

catalog = dbutils.widgets.get("catalog").strip()
prefix = f"{catalog}.{schema_name}" if catalog else schema_name
tables = [f"{prefix}.{t.strip()}" for t in raw_tables.split(",") if t.strip()]

retention_raw = dbutils.widgets.get("vacuum_retention_hours").strip()
retention = float(retention_raw) if retention_raw else None

# COMMAND ----------

results = run_maintenance(
    spark,
    tables,
    optimize=dbutils.widgets.get("optimize") == "true",
    vacuum=dbutils.widgets.get("vacuum") == "true",
    vacuum_retention_hours=retention,
)

summary_rows = [
    (
        str(r.get("table", "")),
        str(r.get("optimize", "skipped")),
        str(r.get("vacuum", "skipped")),
        float(r.get("retain_hours") or 0.0),
        bool(r.get("clamped", False)),
        str(r.get("optimize_error") or r.get("vacuum_error") or ""),
    )
    for r in results
]
# Explicit schema, never inferred - the same #144 lesson as the ingestion
# notebooks: an inferred schema over heterogeneous dicts changes shape with its
# contents, and an empty result cannot infer at all.
display(
    spark.createDataFrame(
        summary_rows,
        schema=(
            "table STRING, optimize STRING, vacuum STRING, "
            "retain_hours DOUBLE, retention_clamped BOOLEAN, detail STRING"
        ),
    )
)

# COMMAND ----------

clamped = [r for r in results if r.get("clamped")]
if clamped:
    # Not a failure. The floor doing its job is the system working, but it
    # means someone configured a retention this job declined to honour, and
    # that should be visible rather than buried in the summary table.
    logger.warning(
        "VACUUM retention was raised to the table floor for %d table(s): %s. "
        "The configured value would have expired change history a CDF consumer "
        "still needs (#58).",
        len(clamped),
        [r["table"] for r in clamped],
    )

failed = [r for r in results if r.get("optimize") == "failed" or r.get("vacuum") == "failed"]
if failed:
    # `raise`, NOT dbutils.notebook.exit("FAILED: ...") (#247). exit() returns a
    # string and always exits 0, so the Jobs UI would mark a run where every
    # table failed to compact as Succeeded.
    raise RuntimeError(
        f"FAILED: maintenance failed on {len(failed)}/{len(results)} table(s): "
        f"{[r['table'] for r in failed]}"
    )

dbutils.notebook.exit(f"SUCCESS: maintained {len(results)} table(s)")
