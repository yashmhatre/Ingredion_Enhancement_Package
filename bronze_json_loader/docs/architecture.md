# Target Architecture — Bronze Ingestion Framework with Async AI-Assisted Metadata Layer

## Overview

This document describes the proposed target-state architecture for
`bronze_json_loader`, extending the current bronze ingestion pipeline to
(a) support multiple source formats beyond JSON, and (b) incorporate
AI-assisted metadata generation — without introducing risk to the
ingestion pipeline's reliability guarantees.

![Ingestion architecture with async AI layer](images/ingestion_architecture.png)

## Architecture Summary

The design is partitioned into two isolated execution lanes.

### 1. Deterministic ingestion path (unchanged in behavior)

Sources → format-aware reader dispatch → existing pipeline (flatten,
data quality gate, audit, write) → Delta bronze table.

Format support (CSV, XML, Parquet, Excel, alongside existing JSON) is
added via a config-driven reader dispatch, consistent with the
package's existing pattern for `flatten_mode` and `write_mode`. No
change to write semantics, quality enforcement, or existing test
coverage.

### 2. Asynchronous AI-assisted metadata layer

Triggered only after a successful write. Performs PII flagging, schema
drift summarization, and draft column/table descriptions, publishing
results to a dedicated metadata store for human review prior to any
catalog update.

## Design Principle

The AI layer is intentionally decoupled from the write path: no shared
transactions, no blocking calls, and no gating decisions. All AI output
is advisory and routed through human review — acceptance, rejection,
and quarantine decisions remain governed exclusively by the existing
deterministic quality logic (`quality.py`). This satisfies the
requirement that AI adoption introduce no bottleneck or deadlock risk
to the core ingestion SLA.

## Delivery Sequencing

This architecture represents the target state, not the immediate
roadmap. Current priority remains the enterprise-hardening phases
already in progress:

1. Run-level audit trail + CI enforcement *(in progress)*
2. Control-table driven dynamic config
3. Concurrency locking
4. Config validation and allowlist governance
5. Secrets via Databricks secret scopes

Multi-format support and the AI metadata layer are scoped as
subsequent phases, building on the audit trail infrastructure once it
is in place.

## Open Design Question

Whether `source_format` should be explicitly declared per config
(consistent with the framework's config-first philosophy) or
auto-inferred from file extension during directory ingestion.
