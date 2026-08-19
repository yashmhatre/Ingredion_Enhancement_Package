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
- required-column validation and quarantining
- retry with backoff
- lineage columns such as `_ingested_at`, `_source_file`, and `_batch_id`
- directory and folder-as-table ingestion
- processed-file archival
- run-level audit trail
- Auto Loader streaming for JSON
- CI checks and Databricks bundle deployment

Planned:

- multi-format streaming ingestion
- Excel support
- control-table-driven configuration
- concurrency locking
- configuration governance and allowlists
- Databricks secret scopes
- schema registry
- Silver transformation framework
- Gold aggregation framework
- AI-assisted metadata such as PII detection, drift summaries, and quarantine reporting

AI does not drive ingestion decisions. It remains outside the write path.

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
- [ ] dynamic configuration
- [ ] multi-format ingestion
- [ ] concurrency controls
- [ ] schema registry
- [ ] AI-assisted metadata
- [ ] Silver layer
- [ ] Gold layer

Use the repo issues for current implementation tasks and `docs/roadmap.md` for sequencing.