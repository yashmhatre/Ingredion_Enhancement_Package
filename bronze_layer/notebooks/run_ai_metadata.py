# Databricks notebook source
# MAGIC %md
# MAGIC # AI Metadata Job - Entrypoint
# MAGIC Reads recent activity from `_ingestion_audit` and `_schema_registry`,
# MAGIC asks a model to draft a table description, per-column descriptions, a
# MAGIC drift summary and PII hints for every table with genuinely new
# MAGIC activity, and upserts the result into `_ai_metadata`.
# MAGIC
# MAGIC **Advisory only.** Nothing here writes to a bronze table, and nothing
# MAGIC in the write path reads what this produces. This is the second lane of
# MAGIC the two-lane split in `bronze_layer/docs/architecture.md`, scheduled
# MAGIC separately from ingestion so ingestion latency never depends on model
# MAGIC latency.

# COMMAND ----------

# bronze_ingest is installed on the job's compute as a wheel, declared in
# bronze_layer/resources/ai_metadata_job.yml under the job-level
# `environments:` block (serverless rejects task-level `libraries:`).
#
# Note this job needs no extra dependency: AIFunctionsMetadataDrafter calls
# ai_query in SQL, so there is no SDK and no credential. The `ai` extra
# (anthropic) backs the local-development drafter only and is deliberately
# not installed here - see bronze_layer/setup.py.

from bronze_ingest import get_logger
from bronze_ingest.ai_metadata import (
    AIFunctionsMetadataDrafter,
    AIMetadataJobConfig,
    run_ai_metadata_job,
)

logger = get_logger()

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace", "Catalog")
dbutils.widgets.text("schema_name", "default", "Schema holding the bronze tables")
# Blank defaults are deliberate on these two: they make the bundle state the
# environment's schema explicitly. All three environments share one catalog,
# so an unpinned audit/registry schema would read another environment's run
# history and draft metadata about tables this environment does not own -
# the same trap #54 fixed on the ingestion side.
dbutils.widgets.text("audit_schema_name", "", "Schema holding _ingestion_audit (required)")
dbutils.widgets.text("registry_schema_name", "", "Schema holding _schema_registry (required)")
dbutils.widgets.text("ai_metadata_table_name", "_ai_metadata", "Advisory output table")
dbutils.widgets.text("lookback_hours", "24", "How far back counts as recent activity")
# Blank default, so the bundle has to name the model rather than inheriting a
# default silently. Left blank when run interactively it falls back to
# AIFunctionsMetadataDrafter.DEFAULT_ENDPOINT, which is pinned to a
# batch-inference-capable endpoint - see that constant before changing it.
dbutils.widgets.text("model_id", "", "ai_query serving endpoint (required from the bundle)")
dbutils.widgets.text("run_id", "", "Job run id, e.g. {{job.id}}-{{job.run_id}}")

# COMMAND ----------

catalog = dbutils.widgets.get("catalog").strip()
schema_name = dbutils.widgets.get("schema_name").strip()
audit_schema = dbutils.widgets.get("audit_schema_name").strip() or schema_name
registry_schema = dbutils.widgets.get("registry_schema_name").strip() or schema_name
ai_table_name = dbutils.widgets.get("ai_metadata_table_name").strip() or "_ai_metadata"
run_id = dbutils.widgets.get("run_id").strip()

model_id = dbutils.widgets.get("model_id").strip() or AIFunctionsMetadataDrafter.DEFAULT_ENDPOINT

lookback_raw = dbutils.widgets.get("lookback_hours").strip()
try:
    lookback_hours = float(lookback_raw) if lookback_raw else 24.0
except ValueError as exc:
    # `from exc` so the original ValueError stays in the traceback - the job
    # log should show what could not be parsed, not just this message.
    raise ValueError(f"lookback_hours must be a number, got {lookback_raw!r}") from exc


def _qualify(schema, table):
    """Fully-qualified name, or schema-qualified when no catalog is set - the
    same shape IngestionConfig.resolved_audit_table produces, so this job
    reads exactly the tables the pipeline wrote."""
    return f"{catalog}.{schema}.{table}" if catalog else f"{schema}.{table}"


job_config = AIMetadataJobConfig(
    audit_table=_qualify(audit_schema, "_ingestion_audit"),
    registry_table=_qualify(registry_schema, "_schema_registry"),
    ai_metadata_table=_qualify(schema_name, ai_table_name),
    lookback_hours=lookback_hours,
    model_id=model_id,
)

logger.info(
    "AI metadata job starting (run_id=%s): audit=%s registry=%s output=%s lookback=%sh model=%s",
    run_id or "<none>",
    job_config.audit_table,
    job_config.registry_table,
    job_config.ai_metadata_table,
    job_config.lookback_hours,
    job_config.model_id,
)

# COMMAND ----------

# The drafter is injected rather than constructed inside the job, which is
# what lets the test suite fake it with a five-line class. AI Functions is
# the default per D1 of docs/decisions/2026-08_ai_genie_architecture.md.
drafter = AIFunctionsMetadataDrafter(spark, endpoint=job_config.model_id)

# Raises AuditMigrationIncompleteError before doing any work if
# _ingestion_audit is mid-migration (#231). That is deliberate and is the one
# thing in this job that fails rather than degrades: a half-migrated audit
# table returns a plausible, complete-looking answer covering half the runs,
# and every draft produced from it would be silently wrong.
summary = run_ai_metadata_job(spark, job_config, drafter)

# COMMAND ----------

# Explicit schema, no pandas - same reasoning as run_directory_ingestion.py.
# An inferred schema over a dict changes shape with its contents, and pandas
# is not declared in setup.py; it works only because the runtime happens to
# ship it.
summary_df = spark.createDataFrame(
    [(str(k), int(v)) for k, v in sorted(summary.items())],
    schema="outcome STRING, tables INT",
)
display(summary_df)

processed = summary.get("processed", 0)
failed = summary.get("skipped_failed", 0)
malformed = summary.get("skipped_malformed", 0)
unchanged = summary.get("skipped_unchanged", 0)

logger.info(
    "AI metadata job finished: %d drafted, %d unchanged, %d model failures, %d malformed",
    processed,
    unchanged,
    failed,
    malformed,
)

# COMMAND ----------

# Failure semantics, and the distinction matters.
#
# A per-table model failure is NOT a task failure. run_ai_metadata_job logs
# and skips it, the table still shows as changed, and the next scheduled run
# picks it up - so failing here would page someone for a transient model
# hiccup that self-heals in a day.
#
# But if EVERY candidate failed and none succeeded, that is not bad luck. It
# is structural: a wrong endpoint name, a missing grant, ai_query unavailable
# on this warehouse. Those do not self-heal, and staying silent would leave a
# job that "succeeds" nightly while writing nothing at all - which is exactly
# the silent-no-op this repo keeps finding. That case fails the task.
if processed == 0 and (failed or malformed):
    dbutils.notebook.exit(
        f"FAILED: every candidate failed - {failed} model failure(s), "
        f"{malformed} malformed. Check the endpoint name ({job_config.model_id}) "
        "and that ai_query is available on this compute."
    )

if not processed and not unchanged:
    logger.info("No candidate tables - nothing has been ingested in the lookback window.")
    dbutils.notebook.exit("SUCCESS: no candidate tables in the lookback window")

_note = f", {failed} model failure(s)" if failed else ""
_note += f", {malformed} malformed" if malformed else ""
dbutils.notebook.exit(f"SUCCESS: {processed} table(s) drafted, {unchanged} unchanged{_note}")
