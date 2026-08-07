"""
AI metadata layer — the advisory third table in the metadata model (see
docs/architecture.md, "Metadata: three tables" and "How the AI layer
actually runs").

This module is the logic behind a **standalone, asynchronously scheduled
Databricks job**, not something the ingestion pipeline calls. It reads
recent activity out of `_ingestion_audit` and `_schema_registry` (both
Fact, written synchronously by the pipeline), asks an injected model
interface to draft a table description, per-column descriptions, a
schema-drift summary and PII hints, and writes accepted drafts to
`_ai_metadata` (Advisory).

**Nothing in the write path may import this module.** That is what makes
"advisory only" structural rather than a convention someone can forget -
pipeline.py, directory_ingestion.py, streaming_reader.py and quality.py
must never read `_ai_metadata`, and this module must never be imported
from any of them.

Bronze does not flatten, reshape or apply business rules (see
docs/bronze_silver_contract.md), and this module doesn't either: the
model is asked to summarise facts the pipeline already recorded (schema,
drift, run status) - it never sees or reshapes the underlying rows. PII
flags are a **drafted hint for human review**, not a classifier - a real
PII classifier is a separate, later buy-vs-build decision (see
docs/buy_vs_build_2026-08.md, #64).
"""

import json
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Optional, Protocol

from pyspark.sql.functions import col
from pyspark.sql.types import (
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from .logging_utils import logger
from .sql_utils import validate_identifier

# One row per table - upserted on new draft, not appended. Same reasoning
# as _schema_registry (see schema_registry.py's module docstring): this
# table answers "what's the CURRENT advisory read on this table", not "what
# has every draft ever said", so its size stays bounded by table count
# rather than by how many times the job has run. Delta's own history
# (DESCRIBE HISTORY) is the audit trail for prior drafts if one is ever
# needed.
AI_METADATA_SCHEMA = StructType(
    [
        StructField("table_name", StringType(), nullable=False),
        # The _schema_registry fingerprint this draft was generated
        # against - lets the job tell "already drafted for this schema"
        # from "schema moved on since the last draft" without re-deriving
        # anything (see _needs_reprocessing).
        StructField("schema_fingerprint", StringType(), nullable=True),
        # The _ingestion_audit run this draft is based on, so advisory
        # output can always be traced back to the fact it was drawn from.
        StructField("source_run_id", StringType(), nullable=True),
        StructField("table_description", StringType(), nullable=True),
        # JSON-encoded {"column": "drafted description"}. One row per
        # table rather than one row per column keeps a single draft
        # readable and writable atomically.
        StructField("column_descriptions_json", StringType(), nullable=True),
        StructField("schema_drift_summary", StringType(), nullable=True),
        # JSON-encoded list of column names the model flagged as
        # *possibly* containing PII - a drafted hint, not a
        # classification. See the module docstring.
        StructField("pii_flags_json", StringType(), nullable=True),
        StructField("model_id", StringType(), nullable=True),
        StructField("generated_at", TimestampType(), nullable=False),
    ]
)


class MetadataDrafter(Protocol):
    """
    Narrow, injectable interface between the AI metadata job and whatever
    drafts the text. Exactly one method, so a test double is a five-line
    class with no network, no SDK and no credentials (see
    test_ai_metadata.py) - and so a new backend never needs to touch
    `run_ai_metadata_job`.
    """

    def draft(self, prompt: str) -> str:
        """
        Takes the assembled prompt for one table and returns the model's
        raw text response.

        Implementations may raise on failure (timeout, API error, rate
        limit, etc) - `run_ai_metadata_job` treats any exception here as
        "log and skip this table for this run", never as fatal to the job
        (see architecture.md's failure story). Returning syntactically
        valid but unusable text is not this method's problem to solve:
        `_parse_draft` is what discards output the job cannot use.
        """
        ...


class AnthropicMetadataDrafter:
    """
    The one concrete `MetadataDrafter`, backed by the official Anthropic
    SDK (`anthropic` package).

    The `anthropic` import is deliberately lazy - inside `__init__`, not at
    module level - so importing `ai_metadata` (and therefore running
    test_ai_metadata.py) never requires the package to be installed. Only
    instantiating this class does.
    """

    #: No date suffix - this id is pinned exactly, per #208.
    DEFAULT_MODEL = "claude-opus-5"

    def __init__(
        self,
        api_key: Optional[str] = None,
        model: str = DEFAULT_MODEL,
        max_tokens: int = 4096,
    ):
        import anthropic  # lazy - see class docstring

        self._client = anthropic.Anthropic(api_key=api_key) if api_key else anthropic.Anthropic()
        self._model = model
        self._max_tokens = max_tokens

    def draft(self, prompt: str) -> str:
        # claude-opus-5 rejects temperature, top_p, top_k and budget_tokens
        # outright - do not add any of them here, even as a default.
        #
        # max_tokens is deliberately generous for what is a short JSON
        # object. On claude-opus-5 thinking is ON by default, and max_tokens
        # caps thinking AND visible text together - so a budget sized only
        # for the answer gets spent on thinking and returns a truncated body.
        # _parse_draft would then discard it as malformed and the job would
        # write nothing, while the log blamed the model's output rather than
        # the budget. Do not lower this without also pinning thinking off.
        response = self._client.messages.create(
            model=self._model,
            max_tokens=self._max_tokens,
            messages=[{"role": "user", "content": prompt}],
        )
        return "".join(
            block.text for block in response.content if getattr(block, "type", None) == "text"
        )


@dataclass
class AIMetadataJobConfig:
    """
    Minimal config for the AI metadata job - deliberately separate from
    `IngestionConfig`, which describes ONE source-to-bronze-table pipeline.
    This job runs once per schedule and looks across every table recorded
    in the audit/registry tables at once, so a single-`table` config would
    not fit its shape.

    `audit_table` / `registry_table` are the fully-qualified names a
    pipeline's `IngestionConfig.resolved_audit_table` /
    `resolved_registry_table` already resolve to - callers wire those in
    directly rather than this module re-deriving them.
    """

    audit_table: str
    registry_table: str
    ai_metadata_table: str = "_ai_metadata"
    # How far back into _ingestion_audit counts as "recent activity" when
    # looking for candidate tables to reprocess.
    lookback_hours: float = 24.0
    model_id: str = AnthropicMetadataDrafter.DEFAULT_MODEL

    def __post_init__(self):
        for field_name in ("audit_table", "registry_table", "ai_metadata_table"):
            value = getattr(self, field_name)
            for i, part in enumerate(str(value).split(".")):
                validate_identifier(part, f"{field_name} part {i + 1}")
        if self.lookback_hours <= 0:
            raise ValueError(f"lookback_hours must be > 0, got {self.lookback_hours}")
        if not self.model_id or not self.model_id.strip():
            raise ValueError("model_id must be a non-empty string")


def _resolve_schema_ref(table_name: str) -> Optional[str]:
    """The schema/catalog.schema prefix of a fully-qualified table name, or
    None for an unqualified name - mirrors the CREATE SCHEMA pattern in
    audit.py / schema_registry.py."""
    parts = table_name.split(".")
    return ".".join(parts[:-1]) if len(parts) > 1 else None


def _recent_runs_by_table(spark, audit_table: str, since: datetime) -> Dict[str, Any]:
    """
    Latest `_ingestion_audit` row per table_name with `started_at >= since`.
    Returns {} if the audit table doesn't exist yet - a job running before
    any ingestion has happened has nothing to draft from.
    """
    if not spark.catalog.tableExists(audit_table):
        return {}

    latest: Dict[str, Any] = {}
    for row in spark.read.table(audit_table).filter(col("started_at") >= since).collect():
        current = latest.get(row["table_name"])
        if current is None or row["started_at"] > current["started_at"]:
            latest[row["table_name"]] = row
    return latest


def _registry_rows(spark, registry_table: str) -> Dict[str, Any]:
    """Every `_schema_registry` row, keyed by table_name. Returns {} if the
    registry table doesn't exist yet."""
    if not spark.catalog.tableExists(registry_table):
        return {}
    return {row["table_name"]: row for row in spark.read.table(registry_table).collect()}


def _existing_drafts(spark, ai_metadata_table: str) -> Dict[str, Any]:
    """Every existing `_ai_metadata` row, keyed by table_name. Returns {} if
    the table doesn't exist yet (first-ever run of the job)."""
    if not spark.catalog.tableExists(ai_metadata_table):
        return {}
    return {row["table_name"]: row for row in spark.read.table(ai_metadata_table).collect()}


def _needs_reprocessing(registry_row, audit_row, previous_draft) -> bool:
    """
    True only when there is genuinely new activity to draft against - "Cost
    is bounded by design" (architecture.md). A table with an unchanged
    schema fingerprint and no new audit activity since the last draft is
    skipped entirely, so steady-state cost stays low as table count grows.
    """
    if previous_draft is None:
        return True  # never drafted for this table - always process once

    if (
        registry_row is not None
        and registry_row["schema_fingerprint"] != previous_draft["schema_fingerprint"]
    ):
        return True  # schema moved on since the last draft

    if audit_row is not None and (
        previous_draft["generated_at"] is None
        or audit_row["started_at"] > previous_draft["generated_at"]
    ):
        return True  # a run happened since the last draft, even if the schema didn't change

    return False


def _build_prompt(table_name: str, registry_row, audit_row, previous_draft) -> str:
    """
    Assembles the model prompt from facts already recorded in
    `_schema_registry` and `_ingestion_audit` only. The model drafts a
    summary of what the pipeline already knows about the table's shape and
    recent runs - it is never shown the underlying rows (see the module
    docstring on bronze not reshaping/classifying).
    """
    schema_json = registry_row["schema_json"] if registry_row is not None else "unknown"
    fingerprint = registry_row["schema_fingerprint"] if registry_row is not None else "unknown"
    prior_fingerprint = previous_draft["schema_fingerprint"] if previous_draft is not None else None
    run_status = audit_row["status"] if audit_row is not None else "unknown"
    row_count = audit_row["row_count"] if audit_row is not None else None

    return (
        "You are drafting advisory metadata for a Delta bronze table. "
        "Respond with a single JSON object only, with keys "
        '"table_description" (string), "column_descriptions" (an object '
        "mapping column name to a one-sentence description), "
        '"schema_drift_summary" (a short note, or null if unchanged), and '
        '"pii_flags" (a list of column names that may plausibly contain '
        "personally identifiable information - a hint for human review, "
        "not a determination).\n\n"
        f"Table: {table_name}\n"
        f"Current schema (JSON): {schema_json}\n"
        f"Current schema fingerprint: {fingerprint}\n"
        f"Previously drafted-against fingerprint: {prior_fingerprint}\n"
        f"Most recent ingestion run status: {run_status}\n"
        f"Most recent ingestion run row_count: {row_count}\n"
    )


def _parse_draft(raw_text: str) -> Optional[Dict[str, Any]]:
    """
    Parses the model's raw response into the fields `_ai_metadata` expects.

    Never raises - returns None for anything unusable, which is the one
    place the "malformed output is discarded, not written" contract lives.
    A partial or nonsensical row in `_ai_metadata` is worse than no row
    (architecture.md), so this is deliberately strict rather than lenient:
    unparseable JSON, a non-object response, or a response carrying none of
    the expected content all fail here rather than producing a mostly-NULL
    row that looks like a real draft.
    """
    try:
        parsed = json.loads(raw_text)
    except (TypeError, ValueError):
        return None
    if not isinstance(parsed, dict):
        return None

    table_description = parsed.get("table_description")
    column_descriptions = parsed.get("column_descriptions")
    schema_drift_summary = parsed.get("schema_drift_summary")
    pii_flags = parsed.get("pii_flags")

    if column_descriptions is not None and not isinstance(column_descriptions, dict):
        return None
    if pii_flags is not None and not isinstance(pii_flags, list):
        return None
    if not table_description and not column_descriptions and not schema_drift_summary:
        # A response carrying none of the expected content is indistinguishable
        # from noise - discard rather than write an all-NULL "draft".
        return None

    return {
        "table_description": table_description,
        "column_descriptions_json": json.dumps(column_descriptions)
        if column_descriptions
        else None,
        "schema_drift_summary": schema_drift_summary,
        "pii_flags_json": json.dumps(pii_flags) if pii_flags else None,
    }


def _write_draft_row(spark, job_config: AIMetadataJobConfig, row_dict: dict) -> None:
    """
    Upserts one drafted-metadata row by table_name. Never raises - a
    failure to persist a draft must never fail the job itself, matching the
    never-raise contract on `audit._write_audit_row` and
    `schema_registry._write_row`.
    """
    try:
        schema_ref = _resolve_schema_ref(job_config.ai_metadata_table)
        if schema_ref:
            spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema_ref}")

        # Projected into AI_METADATA_SCHEMA's field order explicitly - see
        # audit._write_audit_row's comment for why binding by name, not by
        # dict insertion order, matters here.
        row = tuple(row_dict.get(f.name) for f in AI_METADATA_SCHEMA.fields)
        df = spark.createDataFrame([row], schema=AI_METADATA_SCHEMA)
        target = job_config.ai_metadata_table

        if not spark.catalog.tableExists(target):
            df.write.format("delta").saveAsTable(target)
            return

        df.createOrReplaceTempView("_ai_metadata_updates")
        # nosec B608 - `target` is composed of identifiers validated by
        # AIMetadataJobConfig.__post_init__ (validate_identifier, per
        # dot-separated part), the same mitigation config.py uses for
        # quarantine_table. No interpolated value here is free text.
        merge_sql = f"""
            MERGE INTO {target} AS t
            USING _ai_metadata_updates AS s
            ON t.table_name = s.table_name
            WHEN MATCHED THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
        """  # nosec B608
        spark.sql(merge_sql)
    except Exception as exc:  # noqa: BLE001 - a draft-write failure must never fail the job
        logger.warning(
            "Failed to write AI metadata draft for %s: %s", row_dict.get("table_name"), exc
        )


def run_ai_metadata_job(
    spark, job_config: AIMetadataJobConfig, drafter: MetadataDrafter
) -> Dict[str, int]:
    """
    Entry point for the standalone, asynchronously scheduled AI metadata
    job (architecture.md, "How the AI layer actually runs").

    Reads recent activity from `job_config.audit_table` and
    `job_config.registry_table`, asks `drafter` for a draft of every
    candidate table that has genuinely new activity since its last draft,
    and writes accepted drafts to `job_config.ai_metadata_table`.

    Never raises on a per-table problem. A failed/timed-out model call or
    malformed model output logs a warning and skips that table - the job
    always finishes processing every candidate rather than stopping at the
    first bad response, and a skipped table is picked up again by the next
    scheduled run once it still shows as changed. Returns a summary dict
    the caller (or a test) can use instead of parsing log lines.
    """
    since = datetime.now(timezone.utc) - timedelta(hours=job_config.lookback_hours)
    registry_rows = _registry_rows(spark, job_config.registry_table)
    recent_audit = _recent_runs_by_table(spark, job_config.audit_table, since)
    existing_drafts = _existing_drafts(spark, job_config.ai_metadata_table)

    candidates = sorted(set(registry_rows) | set(recent_audit))
    summary = {
        "processed": 0,
        "skipped_unchanged": 0,
        "skipped_failed": 0,
        "skipped_malformed": 0,
    }

    for table_name in candidates:
        registry_row = registry_rows.get(table_name)
        audit_row = recent_audit.get(table_name)
        previous_draft = existing_drafts.get(table_name)

        if not _needs_reprocessing(registry_row, audit_row, previous_draft):
            summary["skipped_unchanged"] += 1
            continue

        prompt = _build_prompt(table_name, registry_row, audit_row, previous_draft)

        try:
            raw_text = drafter.draft(prompt)
        except Exception as exc:  # noqa: BLE001 - a bad model call must never halt the job (architecture.md)
            logger.warning(
                "AI metadata draft failed for %s: %s - skipping this run, the next "
                "scheduled run will pick it up.",
                table_name,
                exc,
            )
            summary["skipped_failed"] += 1
            continue

        parsed = _parse_draft(raw_text)
        if parsed is None:
            logger.warning(
                "Discarding malformed AI metadata output for %s - no row written this run.",
                table_name,
            )
            summary["skipped_malformed"] += 1
            continue

        _write_draft_row(
            spark,
            job_config,
            {
                "table_name": table_name,
                "schema_fingerprint": registry_row["schema_fingerprint"] if registry_row else None,
                "source_run_id": audit_row["run_id"] if audit_row else None,
                "model_id": job_config.model_id,
                "generated_at": datetime.now(timezone.utc),
                **parsed,
            },
        )
        summary["processed"] += 1

    return summary
