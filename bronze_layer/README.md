# bronze-ingest

Plug-and-play package for reading nested data (currently JSON, with
multi-format support planned) and loading it into a Delta **bronze**
table on Databricks. Built so any user on your workspace can reuse it
just by installing the package and pointing it at a config.

This package lives inside the `bronze_layer/` folder of this repo — a
self-contained unit (package, tests, docs, deployment config) that
parallels future `silver_layer` / `gold_layer` folders as those layers
are built.

## Architecture

See [docs/architecture.md](docs/architecture.md) for the target-state
architecture, including planned multi-format ingestion and the
async AI-assisted metadata layer.

## Install (on a Databricks cluster)

Upload the `bronze_layer` folder as a workspace file, or build a wheel
and install it as a cluster/notebook-scoped library:

```bash
cd bronze_layer
python setup.py bdist_wheel
# upload dist/bronze_ingest-0.4.0-py3-none-any.whl to DBFS/Volumes,
# then in a notebook: %pip install /dbfs/path/to/that.whl
```

Or, for quick iteration, just `%pip install pyyaml` and `sys.path.append(...)`
to the repo folder containing `bronze_ingest/` (i.e. `bronze_layer/`).

## Quick start (one-liner)

```python
from bronze_ingest import ingest_json_to_bronze

result = ingest_json_to_bronze(
    spark,
    source_path="abfss://raw@mystorage.dfs.core.windows.net/orders/",
    schema_name="bronze",
    table="orders_raw",
    write_mode="append",        # "append" | "overwrite" | "merge"
)
print(result)
# {'table': 'bronze.orders_raw', 'row_count': 12045, 'columns': [...], ...}
```

## Config-driven usage (recommended for reuse across pipelines)

```python
from bronze_ingest import BronzeIngestion

job = BronzeIngestion.from_yaml(spark, "/Volumes/main/configs/orders_bronze.yaml")
result = job.run()
```

See `sample_config.yaml` for every available option. A `.json` config also
works via `BronzeIngestion.from_json(...)`, or `.from_config_file(...)` to
auto-detect based on extension.

Any other user just needs their own config file (or their own kwargs) - the
package code itself never changes.

### Config validation

Every rule below is checked in `IngestionConfig.__post_init__`, so a bad
config fails **before a cluster starts** rather than partway into a run.
Compute is the overwhelming majority of what a pipeline costs, and a config
error found 40 minutes in has already been paid for.

| Rule | Why |
| --- | --- |
| **Identifiers** — `catalog`, `schema_name`, `table`, the audit/registry names, the audit column names, and every entry of `required_columns`, `unique_columns`, `merge_keys`, `partition_by`, `cluster_by` must match `[A-Za-z_][A-Za-z0-9_]*` | All of them are interpolated into SQL this package builds. The realistic failure is not an attacker — it's `table: "orders-2024"` producing an opaque parse error mid-run |
| `quarantine_table`, `table_properties` keys, `column_comments` keys — validated **per dot-separated part** | These are legitimately dotted (`main.bronze.x`, `delta.enableChangeDataFeed`, `customer.name`). Per-part checking accepts those and still rejects `bad-key` |
| `reader_options` keys must be on `ALLOWED_READER_OPTIONS`, or `cloudFiles.*` | `reader_options` goes verbatim to the Spark reader, and configs load from a Volume. `path` is a reader option — an unfiltered passthrough lets a config redirect the read while every log line still reports `source_path`. Set `allow_unsafe_reader_options: true` to override; it logs what it let through |
| `retry_attempts >= 1` | Below 1, `with_retry`'s loop body never executes and it raises `last_exc` — still `None`. You get "exceptions must derive from BaseException" and no trace of the real failure. **1 means "try once, don't retry"** |
| `retry_delay_seconds >= 0` | A negative value reaches `time.sleep()` and raises mid-run, on a cluster |
| `max_files_per_trigger >= 1` when set | Leave it `None` for no limit |
| `ingestion_mode: streaming` + `write_mode: overwrite` → **raises** | Every micro-batch would replace the whole table, so only the last one survives. There is no case where this is intended |
| `write_mode: merge` + `dedupe_before_merge` + `add_audit_columns: false` and no `dedupe_order_by` → **raises** | The default order column is `audit_ingest_ts_col`, which exists only because `add_audit_columns` creates it. Otherwise it fails at MERGE time, after the read is paid for |
| `dedupe_before_merge` on a non-merge write → **warns** | Silently ignored today, so a user who thinks they configured deduplication hasn't. Warns rather than raises: the setting is merely inert, and raising would break working configs carrying a leftover |
| `enable_schema_registry` without `enable_run_audit` → **warns** | Legal, but drift visibility works by writing the fingerprint onto the audit row, so drift becomes invisible |

Plus the pre-existing rules: enum membership for `write_mode` /
`ingestion_mode` / `schema_evolution_mode` / `trigger_mode`; `merge_keys`
required for merge and required to be a subset of `required_columns`;
`checkpoint_location` + `schema_location` for streaming;
`trigger_processing_time` for `processingTime`; non-empty `unique_columns` and
`cluster_by`; the `cluster_by` / `cluster_by_auto` / `partition_by` mutual
exclusions.

> **`audit_schema_name` and `registry_schema_name` default to `None`, meaning
> "use `schema_name`".** They previously defaulted to the literal `"bronze"`,
> which under the one-catalog/three-schema model meant every environment that
> didn't override them wrote its audit trail to the same
> `<catalog>.bronze._ingestion_audit` — mixing dev, staging and production run
> histories and giving each service principal read access to the others'. It
> failed silently, because the audit writer issues `CREATE SCHEMA IF NOT
> EXISTS` first and so created the shared schema and carried on. If you were
> relying on the old default, set the value explicitly.

## Directory ingestion (multi-file sources)

```python
from bronze_ingest import ingest_directory_to_bronze

results = ingest_directory_to_bronze(
    spark,
    source_dir="/Volumes/main/default/raw_json/",
    catalog="main",
    schema_name="bronze",
    table_name_template="{filename}_bronze",   # or "bronze_{filename}"
)
```

Discovers every `.json`/`.jsonl` file directly inside `source_dir` and
loads each one into its own bronze table. One bad file is logged and
reported in the results list, but does not stop the remaining files from
loading (`stop_on_error=False`, the default).

**`.jsonl` and `.ndjson` files ignore `multiline` and are always read one
record per line**, logging a warning if `multiline: true` was configured.
`multiline` is a single setting shared across every file a directory run
discovers, but the correct value is a property of each individual file —
and `multiLine=true` on a JSON-lines file makes Spark return only its
*first* record, with no error and nothing in `_corrupt_record` (#146). A
`.json` file is genuinely ambiguous (it may be one pretty-printed document
or JSON-lines), so it keeps whatever `multiline` says. To force multi-line
parsing on a `.jsonl` file anyway — it is misnamed, but that is not the
package's call — set `reader_options: {multiLine: "true"}`, which is
applied last and wins.

`write_mode: overwrite` is rejected by default for both per-file and
folder-as-table directory ingestion (raises `ValueError` before touching
any file) - directory ingestion's whole point is discovering files
incrementally over time, and each successfully-ingested file is archived
out of `source_dir`, so a fresh same-named file lands under the same
derived table name on every future run. With `overwrite`, each such run
would silently replace that table's entire contents, leaving only the
most recently ingested file's rows. Use `write_mode: append` or `merge`
instead - `overwrite` is for single-source, full-refresh tables only. If
you genuinely want directory ingestion to fully replace a table's
contents with whatever files currently exist on each run, pass
`allow_overwrite_in_directory_mode=True` explicitly.

### Folder-as-table (subfolder merging)

Any subfolder found directly inside `source_dir` (one level deep) is
treated as one logical dataset — every file inside it is read
individually, successfully-read files are merged (`unionByName`,
tolerant of minor schema differences between files), and the result is
written to a single table named after the folder (e.g. `orders/` →
`orders_bronze`). A single malformed file inside a folder never blocks
the rest of that folder's files from being ingested. Archival and
retry-limit quarantine both apply per-file inside the folder, and
preserve folder structure (`processed/{date}/orders/order1.json`, not
flattened). Pass `schema_hint_ddl` when ingesting sources where files in
the same folder might have inconsistent inferred types, to keep the
merge predictable.

Note: this means subfolders are no longer silently skipped — if your
`source_dir` has subfolders you don't want treated as tables, keep them
outside `source_dir`, or be aware they'll now produce a table on the
next run.

### Automatic file archival

After a file is successfully ingested, it's moved to `processed/{date}/`
automatically. If the move itself fails, the file falls back to
`quarantine_files/` for manual review; if even that fails, the file is
left in place and clearly logged - never silently lost.

### Retry limit before quarantine

Files that fail *ingestion* (not just the archival move) are retried on
subsequent runs, up to `max_ingestion_retries` (default 3), before being
moved to `quarantine_files/`. This prevents permanently-broken files
(malformed JSON, unsupported structures) from failing identically on
every run forever with no signal that a human needs to intervene. A file
that fails once or twice but later succeeds has its retry counter
cleared automatically.

### Run-level audit trail

Every ingestion run — single file, directory batch, streaming
micro-batch, or folder-as-table merge — writes exactly one record to a
dedicated audit table (`enable_run_audit: true`, the default), independent
of any single bronze table. Answers "did this run succeed, how many
rows, how long did it take" without needing to inspect any specific
table. See `bronze_ingest/audit.py`.

A failed run's audit row still carries `quarantined_row_count` (recovered
from the raised `DataQualityError`'s `bad_count`) and `failure_stage`
(`read` | `quality` | `write`), so an operator can see *how many* rows
failed and *at which stage* without parsing `error_message` text.

#### What each count column means

Take these from the table below rather than guessing — before #149 a single
`row_count` meant something different for every write mode, which is how an
ops surface loses trust in its first month.

| Column | Meaning |
|---|---|
| `row_count` | Rows written to the **target table** by this run. Comparable across write modes. |
| `source_row_count` | Rows offered to the writer after the quality gate. Equal to `row_count` for `append`/`overwrite`. |
| `rows_inserted` / `rows_updated` / `rows_deleted` | `merge` only, `NULL` otherwise (`rows_deleted` also populated for `overwrite` where Delta reports it). |
| `write_mode` | So a dashboard can interpret the above without joining back to a config it does not have. |
| `stream_batch_id` | Structured Streaming's micro-batch id. `NULL` for batch runs. |
| `quarantined_row_count` | Rows routed to the quarantine table by the quality gate. |

Every number comes from **Delta's transaction log** (`operationMetrics` on
the commit the run just made), not from recounting the DataFrame. That is
free — it is a metadata read — and it is authoritative. The previous
`final_df.count()` re-read the source and re-ran the entire quality gate to
produce a number Delta already had, because `.cache()` is unavailable on
serverless.

Two things worth knowing before writing a query against this:

- **`source_row_count - row_count` under `merge` is the dedupe/no-op ratio.**
  A source that starts re-sending full daily dumps shows up here, and
  nowhere else.
- **Rows written before this change carry `NULL` in the new columns.** The
  audit table is written with `mergeSchema`, so the migration is automatic,
  but older rows cannot be reinterpreted — a `NULL` `write_mode` is how you
  recognise one.

The column formerly called `table` is now `table_name`, matching
`_schema_registry`. `table` is a SQL reserved word and needed backticking in
every query written against it.

### Schema registry

Every ingestion records its target table's current schema to a dedicated
registry table (`enable_schema_registry: true`, the default) — one row
per bronze table, upserted **only when the schema actually changes**. A
table ingesting daily with a stable schema stays at exactly one row.

Detect drift by querying the table; recover the history of any change
via Delta versioning:

```sql
DESCRIBE HISTORY <catalog>.<schema>._schema_registry
```

Distinct from the audit trail: that records one row per *run*, this
records one row per *table's schema state*. Registry failures never fail
an ingestion run. See `bronze_ingest/schema_registry.py`.

Every successful run also surfaces that same fingerprint check on its own
run-level audit row (`schema_fingerprint`, `schema_changed`), so drift can
be correlated with a specific run's row counts and duration without
joining against the separate registry table. A `WARNING` is logged
whenever the fingerprint changes from a previously-registered one.

## Handling nested JSON

The Bronze ingestion package preserves nested JSON structures exactly as
they are read from the source. Structs and arrays are stored unchanged in
the Bronze Delta table to maintain source fidelity and support schema
evolution.

Any reshaping, flattening, exploding of arrays, or other business-specific
transformations should be performed in the Silver layer, where data is
prepared for downstream analytics and consumption.

This separation keeps the Bronze layer focused on reliable ingestion and
lineage while allowing the Silver layer to apply transformations without
losing the original source structure.

## Source paths

`source_path` accepts anything Spark can read natively - no special-casing
needed:
- `abfss://container@account.dfs.core.windows.net/path/` (ADLS Gen2)
- `s3://bucket/path/` or `s3a://bucket/path/`
- `gs://bucket/path/`
- `dbfs:/mnt/...` or `dbfs:/FileStore/...`
- `/Volumes/catalog/schema/volume/path/` (Unity Catalog Volumes)
- `file:/local/path/` (driver-local, testing only)

Make sure the cluster already has the relevant storage credentials/mounts
configured - this package does not manage auth.

## Write modes

- `append` - straightforward append to the bronze table (default).
- `overwrite` - full overwrite (with `mergeSchema` if schema changed).
- `merge` - upsert using `merge_keys`; requires `delta-spark`'s `DeltaTable`
  API (available by default on Databricks runtimes). Every column in
  `merge_keys` must also be listed in `required_columns` - `NULL = NULL`
  is `NULL` (not true) in the generated MERGE condition, so a NULL merge
  key would never match the target and would be inserted as a duplicate
  row on every run. Config construction raises a `ValueError` if a merge
  key isn't covered by `required_columns`, and the write itself raises
  `NullMergeKeyError` as a last-line-of-defense if a NULL slips through
  anyway. The target table is created via an atomic `CREATE TABLE IF NOT
  EXISTS` (not a check-then-act `tableExists()` probe) and every load -
  including the first - always runs through `MERGE`; merging into a
  freshly-created empty table is equivalent to insert-all, and it means
  two concurrent first-runs against the same not-yet-existing table can
  no longer both "win" the existence check and both append the whole
  batch as duplicates.

  Delta's `MERGE` also raises a runtime error ("multiple source rows
  matched") if the source batch has more than one row per merge key -
  bronze sources frequently re-send full-file dumps or contain
  intra-batch duplicates. `dedupe_before_merge: true` (default)
  deterministically keeps one row per key before the merge, picking the
  one with the highest `dedupe_order_by` value (defaults to
  `audit_ingest_ts_col`, so the most-recently-ingested row wins - this
  requires `add_audit_columns: true`, or an explicit `dedupe_order_by`
  pointing at a column that exists on the DataFrame). Set
  `dedupe_before_merge: false` to instead raise a clear
  `DuplicateMergeKeyError` naming the duplicated key(s) rather than
  silently deduping or hitting Delta's cryptic error.

## Table layout: liquid clustering vs. partition_by

`partition_by` (hive-style partitioning) is effectively legacy for new
Delta tables. Prefer **liquid clustering**:

- `cluster_by: ["col", ...]` - explicit clustering columns.
- `cluster_by_auto: true` - `CLUSTER BY AUTO`, letting Databricks'
  predictive optimization pick clustering keys automatically. **This is a
  Databricks Runtime-only SQL extension** - it isn't parseable against
  open-source/local Delta at all (verified against this package's
  supported delta-spark versions), so outside Databricks Runtime it logs
  a `WARNING` and the write still succeeds unclustered rather than
  failing the run.
- `partition_by` remains fully supported for existing hive-partitioned
  tables (backward compatible), but is discouraged for new ones.
  `cluster_by`/`cluster_by_auto` and `partition_by` are mutually
  exclusive - setting both raises a `ValueError` at config construction.

`table_properties: {"delta.enableChangeDataFeed": "true", ...}` passes
through to the table (e.g. for CDF, retention, or other `delta.*`
settings) - applied at creation, and re-applied via `ALTER TABLE ... SET
TBLPROPERTIES` if a later run's config no longer matches what's already
on the table.

Both `cluster_by` and `table_properties` are applied via `DeltaTable`'s
builder API and raw `ALTER TABLE` statements rather than
`DataFrameWriter`'s own `.clusterBy()`, which doesn't reliably map onto
Delta's table-creation path for this package's supported delta-spark
versions. One quirk this works around: an unqualified
`mode("overwrite")` write performs an implicit `REPLACE TABLE` that
silently drops `CLUSTER BY` unless it's restored immediately afterward -
verified empirically, not just a defensive guess - so `write_mode:
overwrite` re-applies clustering via a cheap metadata-only `ALTER TABLE`
right after every overwrite. `append` and `merge` don't have this
problem - clustering persists across those write modes without any
extra step.

**Predictive optimization** (automatic `OPTIMIZE`/`VACUUM` for
Unity-Catalog-managed tables) is an account/metastore-level setting,
enabled once by a workspace admin - this package doesn't try to manage
it, only documents it as the recommended companion to `cluster_by_auto`.
There's currently no table-lifecycle management (`OPTIMIZE`, `VACUUM`,
retention policies) in this package beyond what `table_properties` and
predictive optimization already cover.

## Catalog documentation (table/column comments)

Config-driven `COMMENT` support, applied to the target table after every
successful write:

```yaml
table_comment: "Raw orders landed from SAP, one row per source record"
column_comments:
  order_id: "Source system primary key"
  customer_email: "Customer email as provided upstream"
```

Both are optional; when neither is set the whole step is skipped without
so much as a catalog read.

**Only what changed is written.** Comment DDL creates a new Delta table
version *every time it runs, including when the comment is identical* -
verified empirically: applying the same `COMMENT ON TABLE` twice takes the
table from version 1 to 2 to 3. Blind re-stamping would therefore append
two or more junk versions to `DESCRIBE HISTORY` on every ingestion run
forever. So the current table/column comments are read first and DDL is
issued only for values that actually differ.

**Top-level columns only.** Nested paths like `customer.name` are not
supported - bronze preserves nested JSON structures rather than flattening
them, so there's no top-level column by that name. Any configured column
that isn't on the table is logged as a WARNING and skipped; it never fails
the run. Comments containing apostrophes are escaped correctly.

Like the audit trail and schema registry, a failure here is logged and
never fails the ingestion run.

**Unity Catalog tags are not implemented** (`ALTER TABLE ... SET TAGS`,
PII/classification markers). That DDL and the `information_schema` tag
views are Databricks Runtime features that raise `ParseException` on
OSS/local Delta, so neither the apply path nor the read-back path can be
exercised by this package's test suite. Given tag failures would be
non-fatal by design, an unverified implementation would silently report
success while applying nothing - a worse outcome for a governance feature
than not shipping it. Tracked on #64, pending validation against a real UC
workspace.

## Audit columns (per-row lineage)

When `add_audit_columns: true` (default), every load adds:
- `_ingested_at` - ingestion timestamp
- `_source_file` - originating file path per row. This is genuine per-row
  lineage whenever the DataFrame already carries an `_input_file_name`
  column (true for every built-in read path: batch, streaming, and
  directory/folder-as-table ingestion). If you call
  `BronzeIngestion.run_on_dataframe()` with your own DataFrame that never
  attached `_input_file_name`, `_source_file` falls back to the coarser
  `config.source_path` for every row instead of silently writing NULL -
  a WARNING is logged when this fallback is used.
- `_batch_id` - a batch identifier (auto-generated UTC timestamp unless you
  pass `batch_id` explicitly, e.g. from a job run ID)

These are separate from the run-level audit trail described above —
per-row columns describe individual rows within a table; the audit
trail describes the run itself.

## Quarantine replay (operational runbook)

Quarantine without a way back is a graveyard: once an upstream source or
a quality rule is fixed, someone has to hand-craft reprocessing. Two
entry points close that loop, in `bronze_ingest.replay` (also exported
from the top-level package):

```python
from bronze_ingest import IngestionConfig, reprocess_quarantine, reprocess_quarantined_files

# Row replay: re-run quarantined rows through the CURRENT quality gate.
config = IngestionConfig.load("/Volumes/main/configs/orders_bronze.yaml")
result = reprocess_quarantine(spark, config)
# {'table': 'bronze.orders_raw', 'replayed_row_count': 12,
#  'still_quarantined_row_count': 3, 'replay_batch_id': 'replay-20260728T...'}

# Optionally scope to a specific original run or time window:
reprocess_quarantine(spark, config, batch_id="20260101T090000000000Z")
reprocess_quarantine(spark, config, since="2026-01-01T00:00:00Z")

# File replay: move quarantined files back so the next directory
# ingestion run picks them up (does not ingest them directly).
reprocess_quarantined_files(spark, source_dir="/Volumes/main/default/raw_json/")
```

**Row replay** (`reprocess_quarantine`) reads `{table}_quarantine`, drops
the quarantine-only columns — `_quarantine_reason`, `_occurrence_count`,
`_first_quarantined_at` and the stale `_ingested_at`/`_batch_id` (the rule
that quarantined a row may have changed since) — and re-runs the rows
through `required_columns`/`unique_columns` as currently configured. Rows that now pass
are written to the bronze table with a fresh `_batch_id` of the form
`replay-<timestamp>` (so replayed rows are identifiable in the bronze
table itself) and removed from quarantine; rows that still fail are left
quarantined, untouched. `_source_file` is preserved as-is throughout -
it's already genuine original per-row lineage, not regenerated.

Idempotent on the success path: re-running finds nothing left matching
the filter once a replay has succeeded, so it re-promotes nothing.
Cross-table transactions aren't available in Delta, so the bronze write
happens before the quarantine delete; if the delete itself then fails
after a successful write, the affected `_quarantine_id`(s) are logged
clearly so they can be reconciled manually rather than silently risking
a duplicate promotion on the next replay.

Every replay run writes one row to the same run-level audit table as
normal ingestion, with a distinguishable `status` of `success_replay` -
query the audit table to see replay runs separately from normal ones.

**File replay** (`reprocess_quarantined_files`) moves files out of
`quarantine_files/` back into `source_dir`, clearing any leftover
retry-state entry so the file gets a fresh set of attempts. It doesn't
ingest directly - the next `ingest_directory_to_bronze()` run picks the
file up normally, reusing all the usual per-file failure isolation,
archival, and retry-limit logic for free. Pass `pattern` (an fnmatch-style
glob) to restore only matching files.

A notebook entrypoint for both (`notebooks/run_quarantine_replay.py`) is
provided for running replay as an on-demand or scheduled Databricks Job
task, separate from the normal ingestion schedule.

## Package layout

```
bronze_layer/
  bronze_ingest/
    __init__.py          # public API
    config.py            # IngestionConfig dataclass + yaml/json loaders
    json_reader.py        # batch JSON read (PERMISSIVE mode, corrupt-record capture)
    streaming_reader.py    # Auto Loader (cloudFiles) incremental read
    quality.py            # required-column + uniqueness validation, quarantine split
    bronze_writer.py       # audit columns, append/overwrite/merge, idempotent streaming writes
    directory_ingestion.py # multi-file discovery, folder-as-table, archival, retry-limit quarantine
    audit.py               # run-level audit trail (audited_run context manager)
    schema_registry.py      # schema fingerprint + drift detection (one row per table)
    replay.py              # quarantine replay - reprocess_quarantine() / reprocess_quarantined_files()
    catalog_metadata.py     # table/column COMMENT documentation (diff-and-apply)
    retry.py              # exponential-backoff retry decorator
    logging_utils.py       # structured logging
    pipeline.py           # BronzeIngestion orchestrator (run() / run_streaming() / run_on_dataframe())
  notebooks/
    run_ingestion.py             # parameterized Databricks notebook entrypoint (widgets)
    run_directory_ingestion.py    # directory/multi-file ingestion entrypoint
    run_quarantine_replay.py       # quarantine replay entrypoint (row + file replay)
    validate_json_reader.py        # ADLS-based validation notebook (not part of pytest)
  docs/
    architecture.md                    # target-state architecture (multi-format + async AI layer)
    testing_json_reader.md              # JSON reader validation notes + findings
    testing_directory_ingestion.md       # directory ingestion, archival, retry-limit, folder-as-table testing
    testing_end_to_end_deployment.md     # full deployed-bundle validation
  tests/                # pytest suite (config, flatten, quality, directory ingestion, archival, retry-limit, folder-as-table, audit)
  databricks.yml        # Databricks Asset Bundle - scheduled job deployment
  setup.py
  sample_config.yaml
```

## Production features

**Incremental ingestion (Auto Loader).** Set `ingestion_mode: streaming` with
`checkpoint_location` and `schema_location`, then call `job.run_streaming()`
(or `ingest_json_to_bronze(...)`, which dispatches automatically). Auto
Loader tracks which files were already processed, so re-running a job never
reprocesses the whole source directory. Use `trigger_mode: availableNow`
(default) to drain the current backlog and stop - the right mode for a
scheduled Databricks Job; use `processingTime` for an always-on stream.

**Streaming and JSON-lines (`.jsonl` / `.ndjson`).** The per-file rule
described under [Directory ingestion](#directory-ingestion-multi-file-sources)
cannot fully apply here. Auto Loader is handed a *directory* and one
`multiLine` value fixed when the stream starts, then reads whatever appears
in that directory later - so a file that does not exist yet cannot be
classified in advance, by validation or by anything else.

Two things cover the gap:

| Situation | Behaviour |
|---|---|
| `source_path` names a single `.jsonl`/`.ndjson` file | `multiLine` forced off, same as batch, with a warning if `multiline: true` was configured |
| A JSON-lines file arrives in a directory stream reading `multiLine=true` | The micro-batch **fails** with `JsonLinesTruncationError`, naming the files |

**Failing is the recoverable outcome, which is why it fails.** Structured
Streaming commits a batch to the checkpoint only when the batch handler
returns normally, so raising leaves the checkpoint *un*advanced: those files
are not marked processed, and re-reading them in full is a config fix and a
restart away. Succeeding is what makes the loss permanent - the checkpoint
moves past files whose records were silently dropped, and there is no second
read and no signal that one is needed.

If the files really are single JSON documents that happen to be named
`.jsonl`, set `reader_options: {multiLine: "true"}`. That is applied last,
wins, and suppresses the guard - the override is treated as a deliberate
statement about the data.

**Schema drift & bad records.** `schema_evolution_mode` controls how Auto
Loader reacts to new/changed fields (`addNewColumns` is the sane default).
`rescued_data_column` captures anything that doesn't fit an explicit
`schema_hint_ddl`. In batch mode, unparseable JSON records are captured in
`corrupt_record_column` instead of failing the whole read (Spark
`PERMISSIVE` mode).

**Data quality gate.** Two *structural* checks run before the bronze write:

| Config | Check |
|---|---|
| `required_columns: ["order_id", ...]` | every listed column must be non-null in every row |
| `unique_columns: ["order_id"]` | the listed column combination must be unique within the batch; all but one row per group is flagged |

`fail_on_quality_error: true` (default) fails the run on any violation -
fail fast during onboarding of a new source. Set it to `false` once you
trust the pipeline enough to instead quarantine bad rows to
`<table>_quarantine` and let good rows through.

Both checks are evaluated into a single `_dq_bad` tag column on one pass
rather than each independently rebuilding its own condition, and the
quarantine write is skipped entirely when the already-known bad-row count
is 0 - no separate probe re-scans the source to re-derive that.

Quarantined rows carry a specific `_quarantine_reason` naming what failed
(`null:email`, `duplicate:order_id,customer_id`, or both joined with `|`),
so quarantine and replay (#60) are queryable per failure type:

```sql
SELECT _quarantine_reason, count(*) FROM bronze.orders_raw_quarantine GROUP BY 1
```

#### The quarantine table is keyed on content, and written with MERGE

`_quarantine_id` is a **SHA-256 of the row's source content**, and the
quarantine write is a `MERGE` on it rather than an append. That combination
is what makes the write idempotent, and it matters because quarantine is
written *before* the bronze write: a run that dies between the two and is
retried quarantines the same rows again. `_quarantine_id` used to be
`uuid()`, which is stable within one query plan but produces entirely
different values on a fresh evaluation — so every attempt appended its own
copy of the same bad rows, and replay treated them as distinct rows to
re-promote (#148).

Two consequences worth knowing before you query the table:

- **Byte-identical bad rows collapse to one row.** They have to — Delta
  refuses a `MERGE` where several source rows match one target row. Their
  multiplicity is preserved in `_occurrence_count`, so `bad_count` in the
  run log counts *rows* while the table counts *identities*, and
  `SUM(_occurrence_count)` reconciles the two.
- **`_occurrence_count` only increments when `_batch_id` changes**, so
  re-running the same batch does not inflate it. That guarantee is only as
  strong as `_batch_id`: the deployed job passes `{{job.run_id}}`, which is
  stable across task attempts, but a config that leaves `batch_id` unset
  gets a generated timestamp that differs per attempt and the count will
  drift upward on retries. Row identity is correct either way — only the
  count is affected.

`_first_quarantined_at` is set on insert and never updated;
`_ingested_at`/`_batch_id` track the *most recent* sighting.

> **Rows quarantined before this change** carry UUID `_quarantine_id`s,
> which can never match a content hash. They are left untouched, so the same
> source row may appear once under an old UUID and once under its hash.
> Nothing breaks, but those old rows will not deduplicate. Once you've
> confirmed the current data has been re-quarantined, clear them with:
>
> ```sql
> DELETE FROM bronze.orders_raw_quarantine WHERE length(_quarantine_id) <> 64
> ```

For `unique_columns`, the row **kept** is the one with the highest
`dedupe_order_by` value. This quality gate runs before audit columns are
added, so `dedupe_order_by` must name a **source** column (e.g. an upstream
`updated_at`) to control which duplicate survives. If it's unset, not
present on the source data, or tied, the tie breaks on a SHA-256 of the
row's full content — arbitrary, but a function of the data alone, so the
same input always yields the same survivor.

That last property is load-bearing rather than a nicety (#147). `good_df`
and `bad_df` are two lazy plans over one tagged DataFrame and Spark
evaluates each independently, so a tie-break that depended on anything but
row content could let the two evaluations disagree — putting a row in both
(written *and* quarantined) or neither (silently dropped).

> **Scope note.** Only structural checks belong in bronze. Range, regex,
> set-membership, cross-column expression and freshness rules all require
> knowing what "valid" means for *your business*, which is a Silver-layer
> concern - see `silver_layer/_archive/README.md` for the same reasoning
> applied to the flattener, and #59 for the full discussion.

`unique_columns` is independent of `dedupe_before_merge` (see the merge
section): the quality gate runs first and quarantines duplicates as bad
rows, so by the time a MERGE happens there is nothing left for the writer's
dedupe to remove. Configure `unique_columns` when you want duplicates
*visible and reviewable* in quarantine; rely on `dedupe_before_merge` alone
when you just want them silently collapsed.

**Idempotent, exactly-once writes.** Streaming micro-batches are written
using Delta Lake's `txnAppId`/`txnVersion` idempotent-write options (keyed
by checkpoint location + batch id), so a retried/replayed micro-batch after
a job failure doesn't duplicate rows. The **batch** append/overwrite path
gets the same protection when `idempotent_batch_writes: true` (default)
*and* an explicit, stable `batch_id` is set (e.g. a Databricks job run
ID) - see the retry-safety matrix below for exactly what's covered and
what isn't.

### Job-level safety controls

The package's own failure handling is thorough — retry with backoff,
quarantine fallback chains, retry limits across runs, `failure_stage`
tagging, an audit row on every outcome. None of that bounds a run that is
merely *stuck*, and the job wrapper is what stands between a hung run and
the bill. `bronze_ingest_jobs.yml` sets:

| Control | Value | Why |
|---|---|---|
| `max_concurrent_runs` | `1` | Two runs over one `source_dir` race on discovery, archival and the shared `_state/` retry file |
| `queue.enabled` | `true` | An overlapping scheduled run waits instead of being silently dropped |
| `health` warn | 1800s | The only proactive signal — everything else fires on failure |
| task `timeout_seconds` | 3300s | Task dies first, so the run records *which* task hung |
| job `timeout_seconds` | 3600s | Backstop |
| `max_retries` | `2` | Transient platform failures shouldn't need a human |
| `retry_on_timeout` | `false` | A timeout recurs; retrying doubles the cost and delays the alert |

Escalation order is deliberate: **warn (1800s) → task timeout (3300s) → job
timeout (3600s)**, so a stuck run is visible while still stuck rather than
only once it exits.

The timeouts are derived, not round numbers. `docs/testing_directory_ingestion.md`
measures 100 files ≈ 163s, and the job caps at `max_files: 50` — so nominal
is ~82s. The realistic ceiling is a *failing* run, not a slow one: with
`retry_attempts: 3` and `retry_delay_seconds: 10`, each failing file sleeps
10s + 20s before giving up, so 50 failing files is ~1500s of pure waiting.
3600s bounds a genuinely hung run at roughly 2× that.

> **`max_retries` depends on `batch_id` stability.** The job passes
> `batch_id: "{{job.run_id}}"`, which becomes the Delta `txnVersion`, so a
> retried attempt re-writes the same version and Delta skips it — a file
> written but not yet archived is not duplicated. That holds only while
> `{{job.run_id}}` stays constant across task attempts. If a retried run ever
> duplicates rows for already-written files, this is the reason: set
> `max_retries: 0` until idempotency is keyed on something verifiably stable.

### Retry-safety matrix

| Write mode | Retry-safe across a job retry? | Mechanism |
|---|---|---|
| `append` (batch) | Only if `batch_id` is explicit and stable (e.g. job run ID) | `txnAppId=full_table_name` / `txnVersion` derived from `batch_id` |
| `overwrite` (batch) | Same as `append` | same |
| `merge` (batch) | Yes, always | MERGE upsert via `merge_keys` - re-running the same batch just re-applies the same updates; Delta's MERGE doesn't accept txn options at all |
| any write mode (streaming) | Yes, always | `txnAppId=checkpoint_location` / `txnVersion` = Structured Streaming's own micro-batch counter (stable regardless of `batch_id`) |

An **auto-generated `batch_id`** (the default - a fresh UTC timestamp
string on every call) cannot make append/overwrite retry-safe no matter
how it's converted internally, since it differs on every attempt,
including retries of the "same" logical run - there's nothing stable to
key a transaction on. `idempotent_batch_writes=True` with no explicit
`batch_id` logs a DEBUG note and falls back to a plain (non-idempotent)
write rather than pretending to protect something it can't. A
`batch_id` that's neither an integer nor the package's own generated
timestamp format similarly can't be converted to a stable `txnVersion` -
logs a WARNING and falls back the same way. **This is why job run ID
wiring (#52) is what makes batch idempotency airtight in practice** -
pass `batch_id=<job run id>` (an integer, or convertible to one) from
your job/notebook parameters to get the guarantee for real. Set
`idempotent_batch_writes: false` to opt out entirely.

`notebooks/run_ingestion.py` and `notebooks/run_directory_ingestion.py`
both expose `batch_id`/`run_id` as job-task widgets for exactly this
purpose, and `databricks.yml`'s `bronze_directory_ingestion` job wires
them from the job's own run context:

```yaml
base_parameters:
  batch_id: "{{job.run_id}}"
  run_id: "{{job.id}}-{{job.run_id}}"
```

With this in place, `audit_table` rows, bronze `_batch_id` values, and
Databricks job runs all join on the same identifier - no more fuzzy
timestamp matching during incident triage. Leave both blank to keep the
auto-generated defaults (a fresh timestamp / UUID per run).

**Retries.** Both read and write paths wrap transient failures (throttling,
concurrent-write conflicts) in exponential-backoff retries via
`retry_attempts` / `retry_delay_seconds` / `retry_max_total_seconds`. **Only
failures a retry could plausibly fix are retried** — see below.

### Retries: what is and is not retried

| Failure | Retried? | Why |
|---|---|---|
| Storage throttling, 429/503, connection reset, timeout | **Yes** | The next attempt genuinely may succeed |
| `ConcurrentAppendException` and siblings | **Yes** | Delta's own concurrency conflicts are the case backoff exists for |
| An unrecognised failure | **Yes** | The default. See the note below |
| `NullMergeKeyError`, `DuplicateMergeKeyError` | **No** | The data is identical on every attempt |
| `DataQualityError`, `JsonLinesTruncationError` | **No** | Same |
| `ValueError` / `TypeError` — unknown `write_mode`, missing order-by column | **No** | Config and programming errors |
| `PERMISSION_DENIED`, `TABLE_OR_VIEW_NOT_FOUND`, `AnalysisException`, `PARSE_SYNTAX_ERROR` | **No** | Nothing changes between attempts |

Before this, `retry.py` caught `Exception` and every call site took that
default, so all of the above were retried three times with 10s and 20s
sleeps. Directory ingestion processes units sequentially with per-unit
failure isolation, so **a directory of 50 broken files spent 25 minutes
sleeping** — and the log showed two `Retrying in 10.0s...` warnings per
file for conditions that were never going to succeed.

Three things worth knowing before changing this:

- **Unknown failures are retried, deliberately.** Wrongly retrying a
  permanent failure costs a bounded amount of time; wrongly refusing to
  retry a transient one costs a failed run. The classifier exists to stop
  the *known* permanent cases from burning the budget, not to be an
  exhaustive taxonomy.
- **Server-side conditions are matched on message text.** PySpark surfaces a
  large family of distinct failures as one exception type, so the message is
  the only signal available. That is a compromise forced by the platform,
  kept in one place (`retry.PERMANENT_MESSAGE_MARKERS` /
  `TRANSIENT_MESSAGE_MARKERS`) so it can be corrected in one place. Transient
  markers are checked *first*, so a concurrency conflict that names a table
  is not misread as a missing-table error.
- **`retry_max_total_seconds` (default 120s) bounds sleeping, not the
  operation.** Without it, `retry_attempts: 5` with `retry_delay_seconds: 30`
  is up to 8 minutes of driver sleep with no ceiling. Set `None` for the old
  unbounded behaviour.

Backoff uses **full jitter** (`sleep(uniform(0, wait))`). Concurrent writers
that collide on a `ConcurrentAppendException` and retry on identical fixed
backoff simply collide again, in lockstep.

**Directory ingestion resilience.** Multi-file sources get per-file failure
isolation, automatic archival of successfully-ingested files, retry-limit
tracking before quarantining permanently-broken files, and folder-as-table
merging for subfolders - see the Directory ingestion section above.

**Run-level audit trail.** Every ingestion path writes one audit record
per run, success or failure - see above.

**Table layout.** Liquid clustering (`cluster_by`/`cluster_by_auto`) is
the recommended replacement for `partition_by` on new tables, plus
`table_properties` passthrough for CDF/retention/other `delta.*`
settings - see the "Table layout" section above.

**Quarantine replay.** `reprocess_quarantine()` and
`reprocess_quarantined_files()` re-promote recoverable rows/files after a
source or quality rule is fixed, with full lineage and idempotent reruns
- see the "Quarantine replay" section above.

**Logging.** All pipeline stages log through `bronze_ingest.logging_utils`,
which shows up in Databricks driver/job-run logs. Get the same logger in your
own code with `from bronze_ingest import get_logger`.

**Deployment.** `databricks.yml` is a ready-to-adapt Databricks Asset Bundle:
job-compute cluster (not an always-on cluster, for cost control), a cron
schedule, retries at the job level, and failure email notifications. Deploy
with `databricks bundle deploy -t prod`. `notebooks/run_ingestion.py` is the
parameterized entrypoint the job calls - point `config_path` at a config
file per table/source rather than duplicating the notebook.

**Testing.** `tests/` has a pytest suite covering config validation,
quality validation, directory ingestion, archival, retry-limit
quarantine, folder-as-table merging, the run-level audit trail, and
quarantine replay, using a local `SparkSession` (Delta-enabled) - no
Databricks connection needed. The suite is also environment-aware and
runs correctly directly on a Databricks cluster. Runs automatically via
GitHub Actions CI on every PR. Run locally with:
```bash
cd bronze_layer
pip install -e ".[dev]"
pytest
```

For deeper validation notes, findings, and known Spark behaviors
discovered during testing (e.g. duplicate-key handling, schema-hint
rescued data, folder-as-table gotchas), see `docs/testing_json_reader.md`
and `docs/testing_directory_ingestion.md`. Full production deployment
validation (real Databricks jobs, real Unity Catalog environment) is in
`docs/testing_end_to_end_deployment.md`.

## Deployment (Asset Bundles)

### One bundle for the whole repository

The bundle is defined at the **repository root** (`databricks.yml`), not per
layer. Each layer contributes only its own resources:

```
databricks.yml                              # bundle name, variables, targets, artifacts
bronze_layer/resources/bronze_ingest_jobs.yml   # bronze job definitions
silver_layer/resources/*.yml                    # (none yet)
```

`bronze_layer` and `silver_layer` previously each carried their own
`databricks.yml`, which meant the workspace host, the target list, and the
run-as service principal were declared twice and could drift apart. Defining
them once at the root also makes CI deployment a single `bundle deploy`
rather than one per layer, so the layers can never be deployed against
mismatched settings.

Paths inside a resource file are relative to **that file's own directory**,
not to the bundle root — so `bronze_layer/resources/bronze_ingest_jobs.yml`
refers to its notebooks as `../notebooks/run_directory_ingestion.py`.

Paths in the root `databricks.yml` itself (such as the `artifacts:` build
path) are relative to the root, since that is the file declaring them —
same rule, different file.

Verified against Databricks CLI v1.9.0. Using a bundle-root-relative path
inside a resource file resolves it as
`bronze_layer/resources/bronze_layer/notebooks/...` and fails with
`notebook ... not found`.

### Environment model

Environments are separated by **Unity Catalog schema and by the service
principal jobs run as** — not by workspace, and not by catalog. One
workspace, one catalog, one schema and one service principal per
environment. That gives per-environment isolation and audit without paying
for multiple workspaces.

| Target | Catalog | Schema | Deployed job name | Runs as | Schedules |
|---|---|---|---|---|---|
| `dev` | `ingredion_en` | `ingredion_dev` | `bronze_directory_ingestion_dev` | the deploying user | auto-paused |
| `staging` | `ingredion_en` | `ingredion_stg` | `bronze_directory_ingestion_stg` | staging service principal | as configured |
| `prod` | `ingredion_en` | `ingredion_prd` | `bronze_directory_ingestion_prd` | prod service principal | as configured |

Job **display names** carry an environment suffix because all three targets
deploy into the same workspace — without it the Jobs list would show three
identically-named jobs with no way to tell which one is production, and
running the wrong one is a single misclick. The **resource key** is
deliberately not suffixed, so `databricks bundle run
bronze_directory_ingestion -t staging` still addresses it by key with the
target selecting the environment.

**The boundary is the schema, so every grant that matters is a schema
grant.** `USE CATALOG` on its own conveys no data access, which is what
makes a shared catalog sound — but it also means a single
`GRANT SELECT ON CATALOG` would flatten the entire boundary in one
statement. Grant `USE CATALOG` and nothing else at catalog level.

The audit and schema-registry tables follow the same boundary:
`audit_schema_name` and `registry_schema_name` are pinned per environment in
the bundle. Left at their package default (`bronze`) all three environments
would write run history and schema fingerprints into one shared table —
mixing the trails, and giving every service principal read access to the
others'. `_write_audit_row` creates the schema if it is missing, so this
would have worked silently rather than failing.

> **Source-file isolation is not enforced.** All three environments read
> from subpaths of a single volume (`ext-ingredion-dev`). Unity Catalog
> grants `READ VOLUME` at volume granularity — there is no sub-path grant —
> so any principal that can read its own subpath can read the others,
> including `PROD/Raw/`. Directory separation here is a convention, not a
> control. Tables, audit and registry are properly isolated; source files
> are not. Giving each environment its own volume would close this, and
> costs nothing but the metadata objects.
>
> The volume also lives in the `ingredion_dev` schema, so staging and prod
> need `USE SCHEMA` on `ingredion_dev` purely to reach their own source
> data — another reason per-environment volumes are cleaner.

**All three environments are real Databricks environments.** `dev` is a
deployed environment like the others — same code path, same bundle, same UC
semantics — not a fallback for things local Delta can't do. Developing
against it means UC-only behavior (Volumes, tags, Auto Loader,
`information_schema`) is exercised continuously rather than first meeting
production.

Local pytest remains the **fast inner loop**: the full suite against local Spark +
Delta in ~3 minutes with no workspace round-trip, which is where logic bugs
should be caught. It is a complement to the `dev` environment, not a
substitute for it — local Delta cannot reproduce the UC surface, so green
tests alone never prove a deploy will work.

Because `dev` uses `mode: development`, the bundle prefixes every resource
with the deploying user's name and force-pauses schedules. Multiple people
can deploy `dev` concurrently without colliding, and nothing runs on a timer
by accident.

### Deploy prerequisites

Deploying builds the wheel locally before uploading it, so the Python
environment you deploy *from* needs the build tooling — not just the
Databricks CLI:

```bash
pip install build          # or: pip install -e "bronze_layer[dev]"
```

Without it, `bundle deploy` fails at the artifact step with
`No module named build` before it reaches the workspace. The `dev` extra in
`bronze_layer/setup.py` includes it, along with pytest and the local
Spark/Delta stack.

**Deploying `staging` or `prod` also needs the `Service Principal: User` role
on the target service principal**, granted in the Databricks account console
under User management → Service principals → Permissions. This is an
account-level permission on the *identity*, unrelated to any Unity Catalog
grant — `run_as` asks Databricks to let a job execute *as* another identity,
so the deployer must be authorised to act on that identity. Without it:

```
Cannot bind the service principal provided in 'run_as' field ... The user
creating or updating the job must have 'servicePrincipal.user' role on the
service principal. (403 PERMISSION_DENIED)
```

It reads like a data-access problem and is not one; no amount of `GRANT` fixes
it. The requirement disappears once deploys move to OIDC federation, where the
deploying identity *is* the service principal.

Set it in the **Databricks account console, not the Azure portal** — even
though these are Entra ID service principals. Entra ID owns that the identity
exists and how you authenticate as it; Databricks owns who may bind a job to
run as it. Azure RBAC does not reach inside Databricks' permission model, so
being Owner on the subscription conveys nothing here.

```bash
databricks bundle deploy -t dev        # deploys as you, schedules paused

databricks bundle deploy -t prod \
  --var="notification_email=data-platform-oncall@your-org.com" \
  --var="run_as_service_principal=<application-id-of-prod-SP>"
```

`notification_email` and `run_as_service_principal` deliberately have **no
defaults** for staging/prod. A deploy that omits them fails immediately
rather than silently running under a human identity or sending alerts
nowhere. In `dev`, alerts go to `${workspace.current_user.userName}` — the
person who deployed — so no shared inbox collects noise from someone else's
experiment.

### Authentication

The CLI is the tool; OAuth is how it authenticates. They aren't alternatives —
the CLI uses OAuth. What matters is **which** OAuth flow, and that depends on
whether a human or a machine is acting:

| Who | Flow | Credential | Used for |
|---|---|---|---|
| A person at a terminal | **OAuth U2M** (`databricks auth login`) | Short-lived token in the OS keychain | Deploying `dev`, ad-hoc inspection |
| CI/CD | **OIDC federation** | No stored credential at all — a per-run token minted from GitHub's identity | Deploying `staging` / `prod` |
| A running job | Service principal (`run_as`) | Managed by the workspace | Job execution |

OAuth is the enterprise-grade option here, not the shortcut. The thing to
avoid is a **personal access token** — long-lived, invisible once issued, and
rotated only when someone remembers.

A human's U2M login is never on the production path. It exists to bootstrap:
you need an authenticated identity to create service principals in the first
place, and something has to deploy `dev`. Production deploys come from CI via
federation, and jobs run as a service principal — neither involves a person's
browser session.

### Code is shipped as a versioned wheel

The `artifacts:` block builds `bronze_ingest` into a wheel on every deploy;
the job task installs it via `libraries:`. Notebooks then just
`from bronze_ingest import ...`.

This replaced `sys.path.append("/Workspace/Users/<person>/...")` inside each
notebook. That approach tied production to one individual's home directory,
shipped whatever happened to be sitting there at the time, and had no
version or rollback story. The wheel is versioned, belongs to no user, and
rolls back with the bundle.

**On serverless compute, dependencies go in `environments:`, not
`libraries:`.** A task-level `libraries:` field is rejected outright:

```
Libraries field is not supported for serverless task,
please specify libraries in environment.   (400 INVALID_PARAMETER_VALUE)
```

So the job declares a job-level environment and each task binds to it:

```yaml
environments:
  - environment_key: default
    spec:
      client: "3"
      dependencies:
        - ../dist/*.whl
tasks:
  - task_key: ingest_directory
    environment_key: default
```

This workspace is serverless by design — see `azure_setup.md` Step 3, chosen
because trial subscriptions have a hard 4-vCPU quota that blocks classic
compute entirely.

`bronze_ingest/__init__.py`'s `__version__` is the single source of truth —
`setup.py` parses it rather than declaring a second copy. CI enforces that
the wheel builds, contains no test/notebook/config files, and reports a
version matching its own filename, so a deployed job can always report
which version it is running.

To use a notebook interactively outside a deployed job, install the same
wheel into the session:

```python
%pip install /Volumes/<catalog>/<schema>/<volume>/bronze_ingest-<version>-py3-none-any.whl
dbutils.library.restartPython()
```

### What an administrator must provision

The bundle consumes these; it does not create them.

- **Entra ID service principals** — one per non-dev environment. Never a
  personal account: a job running under a named human inherits their full
  permissions and breaks when they leave.
- **One catalog** — `ingredion_en`, shared by all three environments.
- **One schema per environment** — `ingredion_dev` / `ingredion_stg` /
  `ingredion_prd`. **The schema is the isolation boundary**, not the catalog.
- **Volumes** — the `source_volume_path` per environment.
- **An address** for `notification_email` in staging/prod.

**Grant `USE CATALOG` and nothing else at catalog level.** A single
`GRANT SELECT ON CATALOG` flattens the entire boundary in one statement.
Everything that matters is a schema or volume grant:

```sql
GRANT USE CATALOG ON CATALOG ingredion_en TO `<client-id>`;
GRANT USE SCHEMA, CREATE TABLE, MODIFY, SELECT
  ON SCHEMA ingredion_en.ingredion_stg TO `<staging-client-id>`;
```

That one schema grant covers the bronze tables, `_ingestion_audit` and
`_schema_registry` together — they all live in the environment's own schema,
so there is nothing separate to grant or keep in sync.

**Also required, and not a Unity Catalog grant:** the deploying identity
needs the **Service Principal: User** role on each service principal,
granted in the Databricks *account console*. `run_as` asks Databricks to let
a job execute *as* another identity, so the deployer must be authorised to
act on that identity. No `GRANT` fixes it, and `Manage` does not imply
`Use`. See `azure_setup.md` Step 12.

> **Source files are not isolated.** All three environments read subpaths of
> one Volume, and Unity Catalog grants `READ VOLUME` at volume granularity —
> there is no sub-path grant. Any principal that can read its own subpath can
> read `PROD/Raw/`. Tables, audit and registry *are* isolated by schema.
> Per-environment Volumes would close this; tracked as #160.

### Not yet implemented

Kept in step with `docs/architecture.md`'s "Remaining enterprise-hardening
phases" and the issue tracker — if those disagree with this list, this list
is the one that drifted.

- **CI/CD deploy** (#113). CI runs tests and verifies the wheel; it does not
  deploy. Deploys are manual. The intended next step is GitHub OIDC
  federation to a service principal, so no long-lived tokens are stored
  anywhere — which also removes the `Service Principal: User` requirement
  above, since the deploying identity would *be* the service principal.
- **Secret scopes** (#115, architecture.md phase 7).
- **Per-environment Volume isolation** (#160). Source-file separation is
  currently a naming convention, not a control — see the note above.
- **Table lifecycle** (#159). Nothing runs `OPTIMIZE` or `VACUUM`, and no
  retention policy exists for the quarantine or audit tables, which grow
  monotonically.
- **Concurrency locking** (#153, architecture.md phase 5). Mitigated but not
  solved: the job now sets `max_concurrent_runs: 1`, which prevents the
  common case (a scheduled run overlapping its predecessor) without making
  the underlying operations safe against concurrent access.

## Operational notes / known caveats

- If you set `schema_hint_ddl` in batch mode, include
  `corrupt_record_column` (default `_corrupt_record`, type `STRING`) in that
  DDL - Spark's PERMISSIVE mode requires the column to exist in the schema
  when one is explicitly supplied.
- For `write_mode: merge` combined with streaming, exactly-once protection
  comes primarily from Auto Loader's checkpoint (it won't re-read already
  processed files) rather than the `txnVersion` mechanism, which applies to
  the append/overwrite write path. Keep `merge_keys` truly unique per
  business key to keep retried merges safe.
- This package doesn't manage cloud storage credentials/mounts - configure
  those at the cluster or Unity Catalog external-location level as usual.
- `.cache()`/`.persist()` are not supported on Databricks serverless
  compute - avoid relying on them in any custom extensions; see
  `docs/testing_directory_ingestion.md` for how folder-as-table works
  around this constraint.
- Folder-as-table treats *any* subfolder in `source_dir` as ingestible -
  keep test/fixture folders outside real source directories, or they'll
  be auto-ingested on the next run (see `docs/testing_directory_ingestion.md`).