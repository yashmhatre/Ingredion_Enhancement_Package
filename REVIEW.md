# Review guidance

Guidance for automated PR review of this repository. The goal is to catch
correctness bugs in a data pipeline, not to police style.

## What counts as Important

Reserve **Important** for defects that corrupt, lose, or duplicate data, or
that fail in production but not in tests:

- **Delta write semantics** — merge conditions that can match zero or many
  rows, nullable merge keys (`NULL = NULL` never matches, so rows insert
  forever), duplicate keys within one source batch, check-then-act races on
  table existence.
- **Idempotency** — anything that behaves differently when a job is retried,
  a micro-batch is replayed, or the same file is ingested twice.
- **Silent failure** — data dropped, absorbed, or schema-evolved without a
  log line, an audit record, or a raised error. Quarantine is fine; silence
  is not.
- **Serverless constraints** — `.cache()` / `.persist()` are unavailable on
  Databricks serverless compute. Code relying on them is broken in production
  even if local tests pass.
- **Unnecessary Spark actions** — `.count()`, `.rdd.isEmpty()`, or repeated
  filters that trigger extra full scans of the source. Bronze batches are
  large and caching is unavailable, so each extra action is a real cost.
- **Audit and lineage gaps** — a code path that writes rows without
  populating `_source_file`, `_ingested_at`, `_batch_id`, or that finishes a
  run without an audit record.
- **Config validation** — new config options accepted without validation in
  `IngestionConfig.__post_init__`, so a bad value fails mid-job on a cluster
  instead of at load time.

## What counts as a Nit

Naming, structure, simplification, docstring wording, and test-organisation
suggestions. Say them briefly; do not block on them.

## Skip entirely

- Anything CI already enforces (test failures show up as a failed check).
- Formatting and import ordering.
- Speculative performance work with no measurement behind it.
- Suggestions to add type hints purely for their own sake.

## Repository context

- Ingestion path: `read → flatten → quality gate → write`, orchestrated by
  `bronze_ingest/pipeline.py`.
- Bronze preserves source shape. Cleansing and typing belong in silver, so
  do not suggest moving transformation logic into bronze.
- Quarantine over failure is deliberate: bad rows are isolated so a batch can
  still succeed. Do not flag this as swallowing errors — but *do* flag any
  path where bad data is dropped without being quarantined or logged.
- Audit writes are intentionally non-fatal (`audit.py` never raises). Do not
  suggest making them fail the run.

## Tone

Be direct and specific. Point at the line and describe the failure scenario —
concrete inputs leading to a wrong result. If nothing important is found, say
so in one line rather than padding the review.
