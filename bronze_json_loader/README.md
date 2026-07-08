# bronze-json-loader

Plug-and-play package for reading nested JSON (from any location) and
loading it into a Delta **bronze** table on Databricks. Built so any user
on your workspace can reuse it just by installing the package and pointing
it at a config.

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
  retry.py              # exponential-backoff retry decorator
  logging_utils.py       # structured logging
  pipeline.py           # BronzeIngestion orchestrator (run() / run_streaming())
notebooks/
  run_ingestion.py      # parameterized Databricks notebook entrypoint (widgets)
tests/                  # pytest suite (config, flatten, quality logic)
databricks.yml          # Databricks Asset Bundle - scheduled job deployment
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
flatten/raw/auto behavior, and the quality gate, using a local
`SparkSession` (no Databricks connection needed). Run with:
```bash
pip install -e ".[dev]"
pytest
```

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
