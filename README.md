# Ingredion ELT Enhancement Project

An ELT (Extract, Load, Transform) pipeline package for ingesting, validating, 
and processing data across bronze, silver, and gold layers — built to support 
Ingredion's data platform enhancements.

This repository currently focuses on the **bronze layer JSON loader**, with 
ongoing enhancements for data quality, error handling, and scalability. 
Contributions are welcome — see [Contributing](#contributing) below.

---

## Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Features](#features)
- [Getting Started](#getting-started)
- [Project Structure](#project-structure)
- [Configuration](#configuration)
- [Usage](#usage)
- [Testing](#testing)
- [Roadmap](#roadmap)
- [Contributing](#contributing)
- [License](#license)

---

## Overview

This package implements an ELT pipeline that ingests raw source data 
(currently JSON) into a **bronze layer**, with planned support for 
transformation into **silver** (cleaned/validated) and **gold** 
(business-ready/aggregated) layers.

The goal is a reliable, extensible ingestion framework that:
- Loads raw data with minimal transformation (bronze principle: preserve source fidelity)
- Handles malformed or unexpected data gracefully without failing entire batches
- Provides traceability and observability into what was loaded, skipped, or quarantined
- Is easy for new contributors to extend with additional sources, validations, or loaders

## Architecture

```
Source Data (JSON files / API / cloud storage)
        │
        ▼
  ┌─────────────┐
  │ Bronze Layer│  ← raw ingestion, schema validation, quarantine for bad records
  └─────────────┘
        │
        ▼
  ┌─────────────┐
  │ Silver Layer│  ← cleaned, validated, deduplicated data (planned)
  └─────────────┘
        │
        ▼
  ┌─────────────┐
  │ Gold Layer  │  ← business-ready, aggregated data (planned)
  └─────────────┘
```

> Update this diagram once the silver/gold layer implementations are added.

## Features

### Implemented
- [ ] JSON bronze loader (core ingestion logic)
- [ ] *(list what's actually built so far — update this section)*

### In Progress / Planned
- [ ] Schema validation and enforcement
- [ ] Dead-letter queue / quarantine for malformed records
- [ ] Schema drift detection
- [ ] Incremental/delta loading
- [ ] Ingestion metadata columns (`_ingested_at`, `_source_file`, `_batch_id`)
- [ ] Load summary reporting (records processed / loaded / rejected)
- [ ] Configurable source support (local disk, S3/ADLS/GCS, API)
- [ ] Retry logic with backoff for transient failures

See open [Issues](../../issues) for the full, up-to-date task list.

## Getting Started

### Prerequisites
- Python 3.x *(confirm version)*
- `pip` or `poetry` for dependency management
- Access to source data location(s) (local path, cloud storage, or API — update as applicable)

### Installation

```bash
git clone https://github.com/<your-org>/<repo-name>.git
cd <repo-name>
pip install -r requirements.txt
```

## Project Structure

```
.
├── src/
│   └── loaders/
│       └── bronze_json.py       # Core bronze JSON loader logic
├── tests/
│   └── bronze_loader/           # Unit tests for the loader
├── config/                      # Pipeline configuration files
├── docs/                        # Additional documentation
├── CONTRIBUTING.md
└── README.md
```

> Update this to match the actual repo layout.

## Configuration

Describe how the loader is configured — e.g., a `config.yaml` or environment 
variables specifying:
- Source path(s)
- Output/bronze destination path
- Schema definitions
- Quarantine folder location

```yaml
# example config.yaml (update to match actual config)
source:
  type: local        # local | s3 | adls | api
  path: /data/raw/
bronze_output:
  path: /data/bronze/
quarantine:
  path: /data/quarantine/
```

## Usage

```bash
python -m src.loaders.bronze_json --config config/config.yaml
```

> Replace with the actual entry point / CLI command once finalized.

## Testing

```bash
pytest tests/
```

Tests cover valid loads, malformed input handling, schema mismatches, and 
edge cases (empty files, large files). See open testing-related issues for 
current coverage gaps.

## Roadmap

- [ ] Finalize bronze layer feature set (validation, DLQ, metadata)
- [ ] Build silver layer transformation logic
- [ ] Build gold layer aggregation logic
- [ ] Add CI/CD pipeline for automated testing
- [ ] Add observability/monitoring dashboard for pipeline runs

## Contributing

Contributions are welcome! Please see [CONTRIBUTING.md](CONTRIBUTING.md) for:
- Local dev environment setup
- Branch naming and PR conventions
- How to claim an open issue
- Testing expectations before submitting a PR

Check the [Issues](../../issues) tab for tasks labeled `good first issue` or 
`help wanted` to get started.

## License

*(Add license information — e.g., internal/proprietary to Ingredion, or specify an open-source license if applicable)*
