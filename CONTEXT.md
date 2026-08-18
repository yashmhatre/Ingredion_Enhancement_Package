# Ingredion Data Platform

The shared language for the repository's medallion pipeline, its governed data,
and the agent-assisted delivery process around it.

## Data layers

**Bronze**:
The source-faithful layer that records what arrived, including nested structure
and ingestion metadata, without business reshaping.
_Avoid_: Raw layer, landing layer

**Silver**:
The lossless, standardized layer that applies structural normalization and
business-aware quality rules to Bronze data.
_Avoid_: Cleaned Bronze, flattened Bronze

**Gold**:
The serving layer of business entities, facts, measures, and curated projections.
_Avoid_: Reporting tables, dashboard layer

## Ingestion

**Source format**:
The explicitly selected representation of source data, such as JSON, CSV,
Parquet, or XML. A source format determines both discovery and interpretation.
_Avoid_: File type, inferred format

**Ingestion unit**:
The atomic item planned for one ingestion result: either one discovered source
file or one immediate subfolder treated as a table.
_Avoid_: Batch, job

**Folder-as-table**:
An immediate source subfolder whose matching files form one ingestion unit and
one destination table.
_Avoid_: Recursive ingestion, dataset folder

**Format registry**:
The authoritative catalog of source formats the package accepts and the
discovery and reader capabilities each format carries.
_Avoid_: Extension list, reader map

**Corrupt record**:
A source record that a format reader can identify but cannot interpret under
the declared read contract.
_Avoid_: Bad file, invalid row

**Document well-formedness**:
The property that a complete document obeys its format's structural grammar.
It is distinct from whether the document's individual records satisfy a schema.
_Avoid_: Data quality, schema validity

**Identifier canonicalization**:
A deterministic, collision-aware conversion of source field names into names
accepted by the platform while preserving their identity and nesting.
_Avoid_: Flattening, sanitization

**Quarantine**:
The governed holding state for source rows or files that cannot safely proceed,
while retaining enough identity and reason to support review and replay.
_Avoid_: Dead-letter table, rejected data

**Replay**:
A later ingestion attempt for quarantined data under the current ingestion
contract. Replay is a normal source of late-arriving records.
_Avoid_: Retry, backfill

## Delivery governance

**Workspace validation**:
Evidence gathered by exercising a capability on Databricks and Unity Catalog
surfaces that local tests cannot represent.
_Avoid_: Manual test, smoke test

**Promotion sign-off packet**:
The evidence bundle presented to the Project Lead for a branch promotion that
requires named approval.
_Avoid_: Release notes, approval request

**Decision record**:
A repository decision that becomes binding only after named Tier 2 sign-off and
remains in force until a later record supersedes it.
_Avoid_: Design note, ADR
