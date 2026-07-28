# Current Behavior Audit — Bronze Ingestion (`bronze_layer/bronze_ingest/`)

Audit of the actual code against the claims in `bronze_layer/README.md`,
for issue #6. Validated by reading each module and by executing the
existing pytest suite (48/48 passing at the time of the original audit,
now 50/50 after the retry-gap fix below added 2 tests) plus targeted
ad-hoc checks against
a local Spark session (JSON with a malformed record, a null in a
`required_columns` field, and a flaky function wrapped in `with_retry`).

Note: the issue's file list (`bronze_json_loader/...`, plus a
`flattener.py` with an `auto_flatten_threshold`) does not match this
repo. The actual package lives at `bronze_layer/bronze_ingest/`, and no
`flattener.py` exists — see the Flattening section below.

## `json_reader.py`

**Confirmed as documented.** `read_json()` sets `mode=PERMISSIVE` and
`columnNameOfCorruptRecord=config.corrupt_record_column` (default
`_corrupt_record`). Verified directly: a 3-line JSONL fixture with one
malformed line produced a DataFrame where the malformed row's raw text
landed in `_corrupt_record` and the other rows parsed normally, matching
the README's "Schema drift & bad records" claim. `rescuedDataColumn` is
only applied when `schema_hint_ddl` is set, as documented. `_input_file_name`
is added via `_metadata.file_path` (Unity Catalog-safe) for later use as
the audit `_source_file` column.

## Flattening (`flattener.py` / `auto_flatten_threshold`) — does not exist

**Gap vs. the issue text, not vs. the README.** No `flattener.py` module,
`auto_flatten_threshold`, or raw/flatten/auto mode exists anywhere in the
codebase. This isn't an oversight: `bronze_layer/README.md` ("Handling
nested JSON") explicitly documents that Bronze preserves nested
structs/arrays unchanged and defers all flattening/reshaping to the
Silver layer. The issue checklist appears to reference an older/planned
design that was superseded. No action needed here beyond noting it —
filing a "missing feature" issue would be incorrect since the current
design intentionally has no bronze-side flattening.

## `quality.py`

**Confirmed as documented.** `split_good_bad()` raises `DataQualityError`
immediately if a `required_columns` entry is absent from the schema
entirely (a schema problem, always a hard fail regardless of
`fail_on_quality_error`). Otherwise it splits on a null-check OR across
all required columns. `enforce_quality()` raises when
`fail_on_quality_error=True` and bad rows exist; otherwise logs a warning
and lets the caller quarantine. Verified directly: with
`required_columns=["customer"]` and `fail_on_quality_error=False`, a
3-row fixture (1 good, 1 corrupt-JSON, 1 explicit null) correctly split
2 bad rows (the corrupt record also has a null `customer`, so it's
correctly caught by both mechanisms) and 1 good row.
`write_quarantine()` tags bad rows with `_quarantine_reason` and appends
to `resolved_quarantine_table` — matches the README's
`<table>_quarantine` naming.

## `bronze_writer.py`

**Confirmed as documented.** `add_audit_columns()` adds `_ingested_at`
(via `current_timestamp()`), `_batch_id` (explicit or a generated UTC
timestamp), and renames `_input_file_name` to `_source_file` — verified
present on the resulting DataFrame in the ad-hoc check. `write_bronze()`
supports `append`/`overwrite`/`merge` as documented; `merge` falls back to
a plain append when the target table doesn't exist yet (first load), and
uses `DeltaTable.merge` with `merge_keys` otherwise. Streaming micro-batch
writes use `txnAppId`/`txnVersion` (keyed on `checkpoint_location` +
Structured Streaming `batch_id`) for idempotent writes, matching the
"Idempotent, exactly-once writes" claim.

## `retry.py`

**Confirmed as documented (fixed — see below).** The `with_retry`
decorator itself does exactly what's documented: retries up to
`attempts` times with delay growing by `backoff` each time, re-raising
the final exception. Verified directly: a function failing twice then
succeeding was retried with delays of ~0.2s then ~0.4s (2x backoff)
before returning its result.

**Originally found a gap, now fixed (#81):** the README's "Retries"
section states *"Both read and write paths wrap transient failures ...
in exponential-backoff retries via `retry_attempts` /
`retry_delay_seconds`."* This was only true for the write path —
`json_reader.read_json()` had no `with_retry` usage, so a transient
read-side failure (e.g. a throttled cloud storage read) propagated
immediately instead of being retried. Fixed by wrapping the actual
`reader.load(...)` call inside `read_json()` in
`@with_retry(attempts=config.retry_attempts,
delay_seconds=config.retry_delay_seconds)`, mirroring the write path's
pattern. Both `pipeline.py`'s `BronzeIngestion.read()` and
`directory_ingestion.py`'s per-file `read_json(spark, cfg)` call site
pick up the retry automatically since they call `read_json()` directly.
Verified with two new tests in `tests/test_json_reader.py`: one where
the underlying Spark load fails once then succeeds (confirms retry
recovers and returns data), one where it always fails (confirms the
configured `retry_attempts` is respected and the exception still
propagates once exhausted). Full suite: 48 → 50 tests, all passing.
(This is distinct from directory ingestion's separate
retry-limit-before-quarantine mechanism, which counts failures *across
separate runs* via a persisted JSON state file — not an in-process
exponential backoff — and is accurately documented separately in the
README's "Retry limit before quarantine" section.)

## `config.py`

**Confirmed as documented.** `IngestionConfig.from_yaml`/`from_json`/
`from_dict`/`load` (extension auto-detect) all work as documented;
`__post_init__` validates `write_mode`, `ingestion_mode`,
`schema_evolution_mode`, `trigger_mode`, requires `merge_keys` for
`write_mode="merge"`, and requires `checkpoint_location` +
`schema_location` for `ingestion_mode="streaming"` — all matching the
README's documented required fields. Confirmed via the existing
`tests/test_config.py` suite (all passing).

## Test suite validation

Ran the full existing suite (`pip install -e ".[dev]"` + `pytest`) against
a local Delta-enabled `SparkSession`: originally **48 passed, 0 failed**
at the time of this audit. Confirms `config`, `quality`,
`directory_ingestion` (archival, retry-limit quarantine,
folder-as-table), `audit`, and `schema_registry` behave as their own
docstrings/tests claim. At that point no dedicated test file existed for
`json_reader.py`, `bronze_writer.py`, or `retry.py` individually — the
README didn't claim otherwise, but it was a gap this issue's parent epic
(writing tests against confirmed behavior) should close.

`tests/test_json_reader.py` was added as part of the retry-gap fix below
(2 tests), bringing the suite to **50 passed, 0 failed**.
`bronze_writer.py` and `retry.py` still have no dedicated test file.

## Gaps found (tracked separately)

1. **README overstates read-path retry coverage** — "Retries" section
   claims both read and write paths get exponential-backoff retry; only
   the write path did. Tracked as
   [issue #81](https://github.com/yashmhatre/Ingredion_Enhancement_Package/issues/81)
   and **fixed** in this same branch: `read_json()`'s load call is now
   wrapped in `@with_retry(...)`, matching the README as originally
   written.
