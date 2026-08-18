# Ingredion Enhancement Project

A config-driven ELT framework for ingesting and processing data across **Bronze, Silver, and Gold layers** on Databricks.

The project currently focuses on a production-ready **Bronze ingestion framework** using Delta Lake and Unity Catalog. Silver and Gold layers are planned and will follow the same modular structure.

## Overview

The Bronze layer ingests raw source data while preserving the original structure. Transformations such as flattening, cleansing, deduplication, and business logic are handled downstream in the Silver layer.

The framework is designed to be:

- **Config-driven** — ingestion behavior is controlled through `IngestionConfig`
- **Reusable** — onboard new sources without modifying pipeline code
- **Reliable** — retries, quarantine handling, and corrupt-record capture
- **Traceable** — row-level lineage, file tracking, and run-level auditing
- **Deployable** — supports Databricks Asset Bundles and CI/CD

## Implemented

- **JSON** ingestion with nested structure preservation
- **CSV, Parquet, and XML** batch ingestion (streaming multi-format deferred)
- Unity Catalog integration
- Required-column validation and row-level quarantine
- Retry with exponential backoff
- Lineage columns: `_ingested_at`, `_source_file`, `_batch_id`
- Directory ingestion and folder-as-table ingestion
- Automatic processed-file archival
- Retry-limit-based quarantine
- Run-level audit trail
- Auto Loader streaming (JSON only)
- GitHub Actions CI
- Databricks Asset Bundle deployment

## Planned

- Multi-format streaming ingestion
- Excel support
- Control-table-driven configuration
- Concurrency locking
- Configuration governance and allowlists
- Databricks secret scopes
- Schema registry
- Silver transformation framework
- Gold aggregation framework
- Async AI-assisted metadata:
  - PII detection
  - Schema drift summaries
  - Quarantine report generation

> AI capabilities remain outside the ingestion write path and do not control ingestion decisions.

## Project Structure

```text
.
├── bronze_layer/
│   ├── bronze_ingest/     # Core ingestion package
│   ├── notebooks/         # Databricks entrypoints
│   ├── config/            # Source configurations
│   ├── resources/         # Asset Bundle resources
│   ├── tests/             # Pytest suite
│   ├── docs/              # Architecture and testing docs
│   └── README.md
├── databricks.yml
├── azure_setup.md
├── CONTRIBUTING.md
└── README.md
```

Future `silver_layer/` and `gold_layer/` packages will follow the same independently deployable structure.

## Getting Started

```bash
git clone https://github.com/yashmhatre/Ingredion_Enhancement_Package.git
cd Ingredion_Enhancement_Package/bronze_layer
pip install -e ".[dev]"
```

Run tests:

```bash
pytest
```

## Testing

Testing is performed at two levels:

1. **Automated pytest tests** for configuration, quality gates, retries, archival, quarantine, folder ingestion, and auditing.
2. **Databricks environment validation** using real ADLS files, Unity Catalog tables, and Asset Bundle deployments.

CI runs the full test suite automatically on every pull request.

## Roadmap

- [x] Production-ready Bronze ingestion
- [x] Directory ingestion and resilience
- [x] Run-level auditing
- [x] CI enforcement
- [x] Auto Loader support
- [x] Asset Bundle deployment
- [ ] Dynamic configuration
- [ ] Multi-format ingestion
- [ ] Concurrency controls
- [ ] Schema registry
- [ ] AI-assisted metadata
- [ ] Silver layer
- [ ] Gold layer

See the repository **Issues** for the latest implementation tasks.