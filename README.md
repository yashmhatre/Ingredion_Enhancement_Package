# Ingredion Enhancement Package

This repo is a Databricks + Unity Catalog ELT package focused on a production-ready bronze layer.

The active implementation is in `bronze_layer/`. Silver and Gold are planned, not live.

## What the project does

The Bronze layer ingests raw source data without flattening it. It keeps the original structure, records lineage, captures corrupt rows, and lets downstream layers handle cleanup and business logic.

The package is built to be:

- config-driven
- reusable across sources
- resilient under failures
- traceable at run and row level
- deployable through Databricks Asset Bundles and CI

## Current scope

Implemented:

- JSON ingestion with nested structure preservation
- batch CSV, Parquet, and XML ingestion
- Unity Catalog integration
- required-column and uniqueness validation, with quarantining
- quarantine replay for both rows and files
- retry with backoff, and permanent-failure classification
- lineage columns such as `_ingested_at`, `_source_file`, and `_batch_id`
- directory and folder-as-table ingestion
- processed-file archival
- run-level audit trail (`_ingestion_audit`)
- schema registry (`_schema_registry`), upserted when a table's schema fingerprint changes
- AI-assisted metadata (`_ai_metadata`), advisory only — see [The AI layer](#the-ai-layer)
- table and column comments applied from config, diffed before write
- Unity Catalog tag assignment over REST
- Delta-safe column identifiers for every format
- table maintenance: OPTIMIZE and VACUUM, floored by each table's own CDF retention
- column profiling and candidate-key detection
- per-format reader-option allowlists
- Auto Loader streaming for JSON
- CI checks and Databricks bundle deployment

Spec only, not wired into the pipeline:

- data contracts covering column names, types, and nullability (`contract.py`)

Planned:

- multi-format streaming ingestion (the format registry carries the Auto Loader format string; the streaming reader still handles JSON only)
- Excel support
- control-table-driven configuration
- concurrency locking
- Databricks secret scopes
- Silver transformation framework
- Gold aggregation framework

## The AI layer

AI does not drive ingestion decisions. It runs in a separate lane and stays outside the write path.

### The two-lane split

The metadata model is three tables in two lanes:

| Table | Lane | Written by | Grain |
|---|---|---|---|
| `_ingestion_audit` | Fact | the pipeline, synchronously | one row per run |
| `_schema_registry` | Fact | the pipeline, synchronously | one row per table's current schema |
| `_ai_metadata` | Advisory | a separate scheduled job | one row per table's current draft |

The separation is structural, not a convention. `pipeline.py`, `directory_ingestion.py`, `streaming_reader.py`, and `quality.py` never import `ai_metadata`, and `ai_metadata` never imports them. Ingestion latency therefore cannot depend on model latency, and no ingestion decision can read a drafted value.

### What the model is asked to do

`run_ai_metadata_job` reads recent activity out of the two Fact tables and asks a model to draft four things per table:

- a table description
- a one-sentence description per column
- a schema-drift summary
- a list of columns that may plausibly contain PII

The prompt is assembled from the schema JSON, the schema fingerprint, and the most recent run's status and row count. The model is never shown the underlying rows. The PII list is a hint for human review, not a classification — a real classifier is a separate buy-vs-build decision (`docs/buy_vs_build_2026-08.md`).

### Which model runs

Two drafters implement one narrow `MetadataDrafter` protocol with a single `draft(prompt)` method, so a test double is a five-line class with no network and no credentials.

- **`AIFunctionsMetadataDrafter`** is the default for anything deployed. It calls `ai_query` over a Databricks-hosted Claude endpoint, pinned to `databricks-claude-opus-4-8`. Chosen on governance rather than cost: no credential has to exist, spend lands in `system.billing.usage` beside every other line, and the call is auditable in Unity Catalog. The endpoint is pinned because `ai_query` against the Claude 5 endpoints returns `PERMISSION_DENIED: not supported for batch inference` — a capability limit, verified 2026-08-08. Re-probe before changing it.
- **`AnthropicMetadataDrafter`** is the local-development escape hatch, on `claude-opus-5` through the Anthropic SDK. It needs no Spark session and no workspace, which is why it survives; it needs a credential, which is why it is not the default.

The two drafters run different models on purpose. Compare their output with that in mind.

### Failure behavior

Bad model output is discarded, never written. A response that is not JSON, is not an object, or carries none of the expected fields produces no row at all, on the grounds that a mostly-NULL row looks like a real draft and is worse than nothing. A markdown code fence around otherwise valid JSON is stripped first — treating that wrapper as malformed discarded 60% of the drafts in the first real run.

A failed model call or a malformed draft logs a warning and skips that table. The job finishes every candidate and the next scheduled run picks up whatever it skipped. `run_ai_metadata_job` returns a summary dict — `processed`, `skipped_unchanged`, `skipped_failed`, `skipped_malformed` — so callers and tests do not have to parse log lines.

There is exactly one condition that fails the whole job: an `_ingestion_audit` table left half-migrated by the 0.5.0 `table` → `table_name` rename. Every query against it returns a complete-looking answer covering half the runs, so there is nothing to skip and nothing downstream that could notice. It is deliberately not bypassable.

### Cost

Cost is bounded by design rather than by a budget. A table whose schema fingerprint is unchanged and which has had no audit activity since its last draft is skipped without a model call, so steady-state cost stays flat as table count grows. `lookback_hours` (default 24) bounds what counts as recent activity.

### Running it

The job is defined in `bronze_layer/resources/ai_metadata_job.yml` and deploys with the bundle, but **its schedule is commented out on purpose**. Enable it only after a manual run has succeeded and the rows in `_ai_metadata` have been read by a human. Putting an unverified model call on an unattended schedule is how a silent, billable no-op runs for a month.

Design rationale lives in `docs/decisions/2026-08_ai_genie_architecture.md`. Under D7, `_ai_metadata` is a proposals-and-review table: drafts reach Unity Catalog through a human gate, not automatically.

## Repository structure

```text
.
├── bronze_layer/
│   ├── bronze_ingest/     # ingestion package
│   ├── notebooks/         # Databricks entrypoints
│   ├── config/            # source configs
│   ├── resources/         # Asset Bundle resources
│   ├── tests/             # pytest suite
│   ├── docs/              # architecture and testing docs
│   └── README.md
├── databricks.yml
├── azure_setup.md
├── CONTRIBUTING.md
├── AGENTS.md
├── docs/
└── README.md
```

## Getting started

```bash
git clone https://github.com/yashmhatre/Ingredion_Enhancement_Package.git
cd Ingredion_Enhancement_Package/bronze_layer
pip install -e ".[dev]"
pytest
```

## Testing

The repo uses two layers of validation:

1. automated pytest checks for config, quality gates, retries, archival, quarantine, folder ingestion, and audit behavior
2. Databricks environment validation against real files, Unity Catalog tables, and bundle deployment paths

CI runs the suite automatically on pull requests.

## Roadmap

- [x] production-ready bronze ingestion
- [x] directory ingestion and resilience
- [x] run-level auditing
- [x] CI enforcement
- [x] Auto Loader support
- [x] Asset Bundle deployment
- [x] schema registry
- [x] AI-assisted metadata (advisory lane, manual trigger)
- [x] table maintenance
- [ ] dynamic configuration
- [ ] multi-format streaming ingestion
- [ ] concurrency controls
- [ ] data contracts wired into the pipeline
- [ ] Silver layer
- [ ] Gold layer

Use the repo issues for current implementation tasks and `docs/roadmap.md` for sequencing.
