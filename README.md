# Ingredion Enhancement Project

An ELT (Extract, Load, Transform) pipeline package for ingesting, validating,
and processing data across bronze, silver, and gold layers — built to support
Ingredion's data platform enhancements.

This repository currently focuses on the **bronze layer** (`bronze_layer/`),
a production-ready, config-driven package for loading nested data into
governed Delta bronze tables on Databricks with Unity Catalog. Silver and
gold layers are planned but not yet started, and will follow the same
self-contained folder pattern once built.

---

## Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Features](#features)
- [Getting Started](#getting-started)
- [Project Structure](#project-structure)
- [Testing & Validation](#testing--validation)
- [Roadmap](#roadmap)
- [Contributing](#contributing)
- [License](#license)

---

## Overview

This package implements an ELT pipeline that ingests raw source data
(currently JSON, with multi-format support planned) into a **bronze
layer**, with planned support for transformation into **silver**
(cleaned/validated) and **gold** (business-ready/aggregated) layers.

The bronze layer package is built to be:
- **Plug-and-play** — any team member reuses it by supplying a config
  file; no code changes needed per source
- **Config-driven** — every behavior (flatten mode, write mode, quality
  gates, retries) is controlled via `IngestionConfig`, not hardcoded
- **Reliable by default** — corrupt records captured (not silently
  dropped or crashed on), bad rows quarantined instead of failing whole
  batches, transient failures retried with backoff
- **Traceable** — every row carries lineage (`_source_file`,
  `_ingested_at`, `_batch_id`), every ingested file is tracked through
  processing (archived on success, quarantined after repeated failure),
  and every run writes a record to a dedicated audit trail

## Architecture

See [bronze_layer/docs/architecture.md](bronze_layer/docs/architecture.md)
for the target-state architecture, including planned multi-format
ingestion (CSV/XML/Parquet) and a decoupled, async AI-assisted metadata
layer (PII flagging, schema drift summaries, quarantine report
generation) — designed so AI never sits in the write path or gates any
ingestion decision.

## Features

### Implemented
- [x] JSON bronze loader — nested JSON, configurable schema flattening
      (raw/flatten/auto), Unity Catalog integration
- [x] Data quality gate — required-column validation with automatic
      row-level quarantine
- [x] Retry logic with exponential backoff (read + write paths)
- [x] Ingestion metadata columns (`_ingested_at`, `_source_file`, `_batch_id`)
- [x] Directory ingestion — discover and load every file in a folder,
      one table per file
- [x] Folder-as-table — subfolders automatically merge into a single
      bronze table, with per-file failure isolation
- [x] Automatic file archival — successfully ingested files move to
      `processed/{date}/`; move failures fall back to a quarantine folder
- [x] Retry-limit-before-quarantine — permanently-failing files are
      quarantined after N consecutive failures instead of retrying forever
- [x] Run-level audit trail — one record per pipeline execution
      (`audited_run()`), independent of any single bronze table
- [x] CI enforcement — full test suite runs automatically on every PR
      via GitHub Actions, branch protection requires it to pass
- [x] Incremental ingestion via Auto Loader (`ingestion_mode: streaming`)
- [x] Databricks Asset Bundle for scheduled production deployment

### In Progress / Planned
- [ ] Multi-format source support (CSV, XML, Parquet, Excel)
- [ ] Control-table driven dynamic configuration
- [ ] Concurrency locking for parallel job safety
- [ ] Config validation and allowlist governance
- [ ] Secrets via Databricks secret scopes
- [ ] Schema registry table (tracks current schema per bronze table)
- [ ] Silver layer (cleaned, validated, deduplicated data)
- [ ] Gold layer (business-ready, aggregated data)
- [ ] Async AI-assisted metadata layer (PII detection, schema drift
      summaries, quarantine report generation) — see architecture doc

See open [Issues](../../issues) for the full, up-to-date task list.

## Getting Started

### Prerequisites
- Python 3.8+
- Databricks workspace with Unity Catalog (serverless or classic compute)
- `pip` for local dependency management

### Installation

```bash
git clone https://github.com/yashmhatre/Ingredion_Enhancement_Package.git
cd Ingredion_Enhancement_Package/bronze_layer
pip install -e ".[dev]"
```

See [bronze_layer/README.md](bronze_layer/README.md) for full usage —
quick-start one-liner, config-driven usage, and all available
`IngestionConfig` options.

For setting up the Databricks/Azure environment from scratch (storage
account, Unity Catalog, external volumes), see
[azure_setup.md](azure_setup.md) — a validated, step-by-step runbook.

## Project Structure

```
.
├── bronze_layer/                 # the bronze layer — self-contained unit
│   ├── bronze_ingest/              # importable package (config, pipeline, readers, etc.)
│   ├── notebooks/                  # Databricks notebook entrypoints
│   ├── docs/                        # architecture + testing documentation
│   ├── tests/                        # pytest suite
│   ├── config/                        # sample per-source config files
│   ├── databricks.yml                  # Asset Bundle job definitions
│   └── README.md                       # package-level documentation
├── azure_setup.md                # Azure/Databricks environment setup runbook
├── CONTRIBUTING.md
└── README.md                     # this file
```

Future `silver_layer/` and `gold_layer/` folders, once built, will follow
the same self-contained shape (own package, tests, docs, deployment
config) — each layer independently deployable and versioned.

## Testing & Validation

The package has two layers of testing:

1. **Local pytest suite** (`bronze_layer/tests/`) — config validation,
   flatten logic, quality gates, directory ingestion, file archival,
   retry-limit behavior, folder-as-table, and the run-level audit trail.
   Runs locally at zero cost (Delta-enabled local SparkSession), also
   runs directly on a Databricks cluster (environment-aware fixtures
   handle both), and runs automatically on every PR via GitHub Actions.
2. **Real-environment validation** (`bronze_layer/docs/`) — JSON reader
   behavior against real ADLS-hosted files, and a full end-to-end
   deployment test (bundle deploy + real job runs against the actual
   Unity Catalog environment). See:
   - [testing_json_reader.md](bronze_layer/docs/testing_json_reader.md)
   - [testing_directory_ingestion.md](bronze_layer/docs/testing_directory_ingestion.md)
   - [testing_end_to_end_deployment.md](bronze_layer/docs/testing_end_to_end_deployment.md)

Run the local suite:
```bash
cd bronze_layer
pytest
```

## Roadmap

- [x] Bronze layer core — functionally complete, tested locally and
      end-to-end in a real deployment
- [x] Directory ingestion resilience — archival, retry-limit quarantine,
      folder-as-table
- [x] Phase 1 of the enterprise-hardening roadmap — run-level audit
      trail + CI enforcement
- [ ] Phases 2-5 — dynamic config, concurrency locking, config
      governance, secrets management
- [ ] Multi-format ingestion (CSV/XML/Parquet)
- [ ] Schema registry table
- [ ] Async AI-assisted metadata layer
- [ ] Silver layer transformation logic
- [ ] Gold layer aggregation logic

## Contributing

Contributions are welcome! Please see [CONTRIBUTING.md](CONTRIBUTING.md) for:
- Local dev environment setup
- Branch naming and PR conventions
- How to claim an open issue
- Testing expectations before submitting a PR

Check the [Issues](../../issues) tab for open tasks.

## License

*(Add license information — e.g., internal/proprietary to Ingredion, or specify an open-source license if applicable)*