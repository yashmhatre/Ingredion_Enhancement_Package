# bronze-json-loader

Plug-and-play package for reading nested JSON (from any location) and
loading it into a Delta **bronze** table on Databricks. Built so any user
on your workspace can reuse it just by installing the package and pointing
it at a config.

## Architecture

See [docs/architecture.md](docs/architecture.md) for the target-state
architecture, including planned multi-format ingestion and the
async AI-assisted metadata layer.

## Install (on a Databricks cluster)

Upload the `bronze_json_loader` folder as a workspace file, or build a wheel
and install it as a cluster/notebook-scoped library:

```bash
cd bronze_json_loader
python setup.py bdist_wheel
# upload dist/bronze_json_loader-0.1.0-py3-none-any.whl to DBFS/Volumes,
# then in a notebook: %pip install /dbfs/path/to/that.whl
```

Or, for quick iteration, just `%pip install pyyaml` and `sys.path.append(...)`
to the repo folder containing `bronze_json_loader/`.

## Quick start (one-liner)

```python
from bronze_json_loader import ingest_json_to_bronze

result = ingest_json_to_bronze(
    spark,
    source_path="abfss://raw@mystorage.dfs.core.windows.net/orders/",
    schema_name="bronze",
    table="orders_raw",
    flatten_mode="flatten",     # "raw" | "flatten" | "auto"
    write_mode="append",        # "append" | "overwrite" | "merge"
)
print(result)
# {'table': 'bronze.orders_raw', 'row_count': 12045, 'columns': [...], ...}
```

## Config-driven usage (recommended for reuse across pipelines)

```python
from bronze_json_loader import BronzeIngestion

job = BronzeIngestion.from_yaml(spark, "/Volumes/main/configs/orders_bronze.yaml")
result = job.run()
```

See `sample_config.yaml` for every available option. A `.json` config also
works via `BronzeIngestion.from_json(...)`, or `.from_config_file(...)` to
auto-detect based on extension.

Any other user just needs their own config file (or their own kwargs) - the
package code itself never changes.

## Directory ingestion (multi-file sources)

```python
from bronze_json_loader import ingest_directory_to_bronze

results = ingest_directory_to_bronze(
    spark,
    source_dir="/Volumes/main/default/raw_json/",
    catalog="main",
    schema_name="bronze",
    table_name_template="{filename}_bronze",   # or "bronze_{filename}"
    flatten_mode="auto",
)
```

Discovers every `.json`/`.jsonl` file directly inside `source_dir` and
loads each one into its own bronze table. One bad file is logged and
reported in the results list, but does not stop the remaining files from
loading (`stop_on_error=False`, the default).

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

## Handling nested JSON

Set `flatten_mode` per source:

| mode      | behavior |
|-----------|----------|
| `raw`     | Struct/array columns are kept nested exactly as read. Classic bronze pattern - preserve source shape, do transformation later in silver. |
| `flatten` | Structs are recursively expanded into `parent_child_grandchild` columns. Set `explode_arrays: true` to also explode array columns into rows. |
| `auto`    | Flattens automatically if nesting depth is shallow (`auto_flatten_threshold`, default 5); falls back to raw for deeply/variably nested sources to avoid schema explosion. |

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
  API (available by default on Databricks runtimes).

## Audit columns

When `add_audit_columns: true` (default), every load adds:
- `_ingested_at` - ingestion timestamp
- `_source_file` - originating file path per row
- `_batch_id` - a batch identifier (auto-generated UTC timestamp unless you
  pass `batch_id` explicitly, e.g. from a job run ID)

## Package layout

```
bronze_json_loader/
  __init__.py          # public API
  config.py            # IngestionConfig dataclass + yaml/json loaders
  json_reader.py        # batch JSON read (PERMISSIVE mode, corrupt-record capture)
  streaming_reader.py    # Auto Loader (cloudFiles) incremental read
  flattener.py          # raw / flatten / auto nested-field handling
  quality.py            # required-column validation + quarantine split
  bronze_writer.py       # audit columns, append/overwrite/merge, idempotent streaming writes
  directory_ingestion.py # multi-file discovery, folder-as-table, archival, retry-limit quarantine
  retry.py              # exponential-backoff retry decorator
  logging_utils.py       # structured logging
  pipeline.py           # BronzeIngestion orchestrator (run() / run_streaming() / run_on_dataframe())
notebooks/
  run_ingestion.py             # parameterized Databricks notebook entrypoint (widgets)
  run_directory_ingestion.py    # directory/multi-file ingestion entrypoint
  validate_json_reader.py        # ADLS-based validation notebook (not part of pytest)
docs/
  architecture.md                    # target-state architecture (multi-format + async AI layer)
  testing_json_reader.md              # JSON reader validation notes + findings
  testing_directory_ingestion.md       # directory ingestion, archival, retry-limit, folder-as-table testing
  testing_end_to_end_deployment.md     # full deployed-bundle validation
tests/                # pytest suite (config, flatten, quality, directory ingestion, archival, retry-limit, folder-as-table)
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

**Schema drift & bad records.** `schema_evolution_mode` controls how Auto
Loader reacts to new/changed fields (`addNewColumns` is the sane default).
`rescued_data_column` captures anything that doesn't fit an explicit
`schema_hint_ddl`. In batch mode, unparseable JSON records are captured in
`corrupt_record_column` instead of failing the whole read (Spark
`PERMISSIVE` mode).

**Data quality gate.** Set `required_columns: ["order_id", ...]` to assert
non-null values before writing to bronze. `fail_on_quality_error: true`
(default) fails the run on any violation - fail fast during onboarding of a
new source. Set it to `false` once you trust the pipeline enough to instead
quarantine bad rows to `<table>_quarantine` and let good rows through.

**Idempotent, exactly-once writes.** Streaming micro-batches are written
using Delta Lake's `txnAppId`/`txnVersion` idempotent-write options (keyed
by checkpoint location + batch id), so a retried/replayed micro-batch after
a job failure doesn't duplicate rows.

**Retries.** Both read and write paths wrap transient failures (throttling,
concurrent-write conflicts) in exponential-backoff retries via
`retry_attempts` / `retry_delay_seconds`.

**Directory ingestion resilience.** Multi-file sources get per-file failure
isolation, automatic archival of successfully-ingested files, retry-limit
tracking before quarantining permanently-broken files, and folder-as-table
merging for subfolders - see the Directory ingestion section above.

**Logging.** All pipeline stages log through `bronze_json_loader.logging_utils`,
which shows up in Databricks driver/job-run logs. Get the same logger in your
own code with `from bronze_json_loader import get_logger`.

**Deployment.** `databricks.yml` is a ready-to-adapt Databricks Asset Bundle:
job-compute cluster (not an always-on cluster, for cost control), a cron
schedule, retries at the job level, and failure email notifications. Deploy
with `databricks bundle deploy -t prod`. `notebooks/run_ingestion.py` is the
parameterized entrypoint the job calls - point `config_path` at a config
file per table/source rather than duplicating the notebook.

**Testing.** `tests/` has a pytest suite covering config validation,
flatten/raw/auto behavior, the quality gate, directory ingestion, file
archival, retry-limit quarantine, and folder-as-table merging, using a
local `SparkSession` (no Databricks connection needed) - the suite is also
environment-aware and runs correctly directly on a Databricks cluster.
Run with:
```bash
pip install -e ".[dev]"
pytest
```

For deeper validation notes, findings, and known Spark behaviors
discovered during testing (e.g. duplicate-key handling, schema-hint
rescued data, folder-as-table gotchas), see `docs/testing_json_reader.md`
and `docs/testing_directory_ingestion.md`. Full production deployment
validation (real Databricks jobs, real Unity Catalog environment) is in
`docs/testing_end_to_end_deployment.md`.

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