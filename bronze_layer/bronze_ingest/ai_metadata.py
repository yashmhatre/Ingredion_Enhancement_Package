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
import re
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
    The **local-development escape hatch**, backed by the official Anthropic
    SDK (`anthropic` package). `AIFunctionsMetadataDrafter` is the default for
    anything deployed - see D1 of
    `docs/decisions/2026-08_ai_genie_architecture.md`.

    Retained rather than deleted, deliberately: it needs no Spark session and
    no workspace, so it is the only drafter that can be pointed at a real
    model from a laptop. That is also why it keeps a Claude 5 model while the
    AI Functions path cannot - it calls Anthropic directly, where the Claude 5
    family is available, and is not subject to the batch-inference constraint
    documented on `AIFunctionsMetadataDrafter.DEFAULT_ENDPOINT`.

    **The two drafters therefore run different models on purpose.** A draft
    produced locally and one produced in-platform are not from the same model,
    which is worth remembering before comparing their output.

    Requires a credential; that is the cost of using it, and the reason it is
    not the default.

    The `anthropic` import is deliberately lazy - inside `__init__`, not at
    module level - so importing `ai_metadata` (and therefore running
    test_ai_metadata.py) never requires the package to be installed. Only
    instantiating this class does.
    """

    #: No date suffix - this id is pinned exactly, per #208. Unrelated to
    #: AIFunctionsMetadataDrafter.DEFAULT_ENDPOINT, which is pinned for a
    #: different reason; do not "align" the two.
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


class AIFunctionsMetadataDrafter:
    """
    The default `MetadataDrafter`: Databricks AI Functions (`ai_query`) over a
    Databricks-hosted Claude model, per D1 of
    `docs/decisions/2026-08_ai_genie_architecture.md`.

    Chosen over `AnthropicMetadataDrafter` on governance, not cost - at
    ~20K tokens/day token price is noise. What this buys:

    * **No credential exists.** The Anthropic path needs a PAT in the
      environment; #115 (secret scopes) has not shipped and is blocked on
      #112. The strongest control is the secret that does not exist.
    * Spend lands in `system.billing.usage` under `MODEL_SERVING` /
      `BATCH_INFERENCE`, beside every other line, instead of a second invoice.
    * The call is auditable in Unity Catalog.

    Needs no network access of its own and no SDK - it is a SQL function
    call on the session Spark already has.
    """

    #: Pinned deliberately. **Do not "upgrade" this to a Claude 5 endpoint.**
    #: Verified against workspace adb-7405607398572130 on 2026-08-08: the
    #: `databricks-claude-opus-5` and `databricks-claude-sonnet-5` endpoints
    #: are served and READY, but `ai_query` against either returns
    #: `PERMISSION_DENIED: Endpoint ... is not supported for batch inference`.
    #: The error names the endpoint, not the principal - a capability limit,
    #: not an entitlement one. Changing this to a Claude 5 id turns a working
    #: scheduled job into a runtime failure. See Amendment 1 of the decision
    #: record; re-probe with `SELECT ai_query('databricks-claude-opus-5','ok')`
    #: before assuming the constraint still holds.
    DEFAULT_ENDPOINT = "databricks-claude-opus-4-8"

    def __init__(self, spark, endpoint: str = DEFAULT_ENDPOINT, max_tokens: Optional[int] = None):
        self._spark = spark
        self._endpoint = endpoint
        # Optional, and off by default *because it is unverified*. The two-arg
        # ai_query form was confirmed working in this workspace; passing
        # modelParameters was not. Opt in only after checking it against the
        # target runtime - a malformed named_struct fails the whole statement,
        # and this job would then log a model failure rather than a SQL one.
        self._max_tokens = int(max_tokens) if max_tokens is not None else None

    def draft(self, prompt: str) -> str:
        # Parameter markers, never f-strings. The prompt is assembled from
        # table names, schema JSON and audit rows - `_build_prompt` output is
        # not a literal, it contains apostrophes and newlines routinely, and
        # interpolating it into SQL would be both a quoting bug and an
        # injection surface. `args` binds it as a value.
        #
        # max_tokens is interpolated rather than bound because a parameter
        # marker is not accepted inside named_struct's field list; int() in
        # __init__ is what makes that safe.
        if self._max_tokens is None:
            sql = "SELECT ai_query(:endpoint, :prompt) AS draft"
        else:
            sql = (
                "SELECT ai_query(:endpoint, :prompt, "
                f"modelParameters => named_struct('max_tokens', {self._max_tokens})) AS draft"
            )

        row = self._spark.sql(sql, args={"endpoint": self._endpoint, "prompt": prompt}).collect()[0]
        return row["draft"] or ""


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
    # Recorded on every drafted row so advisory output can always be traced to
    # what produced it. Defaults to the AI Functions endpoint because that is
    # now the default drafter (D1). A caller using AnthropicMetadataDrafter for
    # local development should pass its model explicitly - the two drafters
    # legitimately run different models (see AIFunctionsMetadataDrafter), and a
    # row claiming the wrong one is worse than no row.
    model_id: str = AIFunctionsMetadataDrafter.DEFAULT_ENDPOINT

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


class AuditMigrationIncompleteError(RuntimeError):
    """
    Raised when `_ingestion_audit` is mid-migration and reading it would
    silently return a partial answer. See `_assert_audit_migration_complete`.
    """


# The remediation hint quoted back to the operator, verbatim from
# CHANGELOG 0.5.0. Held as a constant with a placeholder rather than built
# by f-string so it reads as what it is - a documentation string - and not
# as query construction. Nothing here is ever executed.
_HINT_PLACEHOLDER = "<audit_table>"
_BACKFILL_SQL_HINT = "UPDATE <audit_table> SET table_name = table WHERE table_name IS NULL;"


def _assert_audit_migration_complete(spark, audit_table: str) -> None:
    """
    Fail the job, loudly, if `_ingestion_audit` cannot be read completely.

    CHANGELOG 0.5.0 renamed this table's `table` column to `table_name`.
    The audit writer appends with `mergeSchema: true`, so on a table created
    before that release **the write succeeds and nothing fails** - Delta
    relaxes the old column's NOT NULL constraint rather than rejecting the
    row. The table ends up with two columns meaning the same thing, each
    populated for half the runs, and a manual backfill is required per
    environment.

    This job reads `_ingestion_audit` as its primary input, so on a
    half-migrated table every query returns a plausible, complete-looking
    answer covering only half the runs - and the job's entire output is
    derived from a silently truncated read. That is the #146 failure shape
    one abstraction level up: not a crash, a confident wrong answer.

    **This is the one place in this module that raises.** Everything else
    here degrades per-table and keeps going, deliberately (see
    `run_ai_metadata_job`). This check is different in kind: it is a
    precondition on the input, not a problem with one table's draft, and a
    truncated read cannot be detected downstream or recovered from by
    skipping something. Failing the run is the only honest outcome, and a
    warning would be worse than useless - it would be a warning nobody reads
    attached to output that looks fine.

    Deliberately not bypassable. "Fail closed" with an override flag is
    just "fail open" with extra steps, and the fix - one UPDATE, documented
    in CHANGELOG 0.5.0 - is cheaper than the flag.

    No-ops when the table does not exist yet: a job running before any
    ingestion has nothing to read, which is not the same as reading half of
    something.
    """
    if not spark.catalog.tableExists(audit_table):
        return

    columns = set(spark.read.table(audit_table).columns)

    if "table_name" not in columns:
        raise AuditMigrationIncompleteError(
            f"{audit_table} has no `table_name` column, so it predates CHANGELOG 0.5.0 "
            f"and cannot be read by this job. Upgrade the environment, let one run write "
            f"a post-0.5.0 row, then run the backfill from CHANGELOG 0.5.0 "
            f'("Read this before deploying", item 1).'
        )

    stale_rows = spark.read.table(audit_table).filter(col("table_name").isNull()).count()
    if stale_rows:
        raise AuditMigrationIncompleteError(
            f"{audit_table} has {stale_rows} row(s) with `table_name IS NULL` - the "
            f"CHANGELOG 0.5.0 backfill has not been run in this environment. Reading it "
            f"now would silently cover only the runs written after the upgrade. Run:\n"
            f"    {_BACKFILL_SQL_HINT.replace(_HINT_PLACEHOLDER, audit_table)}"
        )


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


#: Chat models routinely wrap a JSON answer in a markdown code fence even when
#: the prompt asks for JSON only. Measured, not guessed: the first real run of
#: this job against Databricks-hosted Claude discarded 9 of 15 drafts, and the
#: raw responses were well-formed JSON inside ```json ... ``` - 1090 characters,
#: so not truncation. `search` rather than `match` because a model sometimes
#: adds a sentence of preamble before the fence.
_CODE_FENCE_RE = re.compile(r"```(?:json)?\s*\n(.*?)\n?\s*```", re.DOTALL | re.IGNORECASE)


def _strip_code_fence(raw_text: str) -> str:
    """The contents of the first markdown code fence, or the input unchanged.

    Unwrapping a transport wrapper is not the same as being lenient about
    content: everything `_parse_draft` rejects below is still rejected, and a
    fence containing prose rather than JSON still fails there.
    """
    if not isinstance(raw_text, str):
        return raw_text
    match = _CODE_FENCE_RE.search(raw_text)
    return match.group(1) if match else raw_text


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

    A markdown code fence around the JSON is stripped first - see
    `_strip_code_fence`. That is a wrapper, not content, and treating it as
    malformed threw away 60% of a real run's drafts.
    """
    try:
        parsed = json.loads(_strip_code_fence(raw_text))
    except (TypeError, ValueError):
        return None
    if not isinstance(parsed, dict):
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

    **One exception, and it is deliberate:** raises
    `AuditMigrationIncompleteError` before doing any work if
    `job_config.audit_table` is mid-migration from CHANGELOG 0.5.0. That is
    a precondition on the input rather than a per-table problem - a
    truncated read produces output that looks complete and is not, so there
    is nothing to skip and nothing downstream that could notice.
    """
    _assert_audit_migration_complete(spark, job_config.audit_table)

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
