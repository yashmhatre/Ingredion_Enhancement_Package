# Directory Ingestion Testing — Local + Databricks

## Purpose

`tests/test_directory_ingestion.py` covers `directory_ingestion.py`'s file
discovery and naming logic (`list_json_files`, `sanitize_table_name`,
`build_table_name`). Unlike `json_reader.py`'s ADLS-based validation
(see `docs/testing_json_reader.md`), this suite runs as real `pytest` —
either fully locally, or directly on a Databricks cluster (serverless or
classic), using the same test file and fixtures in both places.

## Why both environments needed support

`list_json_files` tries three discovery strategies in order:
`dbutils.fs.ls` → `os.listdir` → Hadoop `FileSystem` API. The original
tests only exercised the `os.listdir` path (via pytest's local `tmp_path`).
Running the same suite on Databricks exercises the `dbutils.fs.ls` path
instead — the one actually used in production on serverless compute — so
it was worth getting this working in both places rather than only ever
testing the fallback path.

## Environment-aware fixtures (`conftest.py`)

**`spark` fixture** — detects `SPARK_REMOTE` in the environment (always
set inside Databricks, since Spark Connect is already configured there)
and either attaches to the existing session or builds a fresh local one:

```python
@pytest.fixture(scope="session")
def spark():
    from pyspark.sql import SparkSession

    if "SPARK_REMOTE" in os.environ:
        spark = SparkSession.builder.getOrCreate()
        yield spark
        return

    spark = (
        SparkSession.builder
        .appName("bronze_ingest-tests")
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    yield spark
    spark.stop()
```

Without this, building a `local[2]` session inside Databricks fails with
`PySparkRuntimeError: CANNOT_CONFIGURE_SPARK_CONNECT_MASTER` — Spark
refuses to let `spark.master` and `spark.remote` coexist.

**`json_test_dir` fixture** — provides a real, writable directory for
file-discovery tests, adapting to environment:

```python
@pytest.fixture
def json_test_dir(tmp_path):
    dbutils = _get_dbutils()

    if dbutils is not None:
        base = os.environ.get(
            "PYTEST_VOLUME_SCRATCH",
            "/Volumes/ingredion_en_dev/ingredion_dev/ext-ingredion-dev/pytest_scratch",
        )
        scratch = f"{base}/{uuid.uuid4().hex}"
        dbutils.fs.mkdirs(scratch)
        yield scratch, scratch
        dbutils.fs.rm(scratch, recurse=True)
    else:
        yield str(tmp_path), f"file://{tmp_path}"
```

- **Locally:** falls straight through to pytest's own `tmp_path`, exactly
  as before — no behavior change for local runs.
- **On Databricks:** `dbutils.fs` cannot access arbitrary local `/tmp`
  paths at all (`LocalFilesystemAccessDeniedException` — a genuine
  security boundary, not a bug), so a real scratch folder is created
  under a Unity Catalog Volume instead, and cleaned up after each test.

Test functions were updated to write files via a small helper instead of
directly into `tmp_path`, and to use `json_test_dir`'s `source_dir` (a
real path/URI in both environments) instead of a hardcoded `file://` URI.

## Unity Catalog path

Catalog/schema/volume for this workspace:

| | Value |
|---|---|
| Catalog | `ingredion_en_dev` |
| Schema | `ingredion_dev` |
| Volume | `ext-ingredion-dev` |

Confirmed via **Catalog Explorer** in the Databricks UI. `azure_setup.md`
has been corrected to reflect these values (previously had placeholder
names — see that doc's own changelog for detail).

Full working scratch path used by the `json_test_dir` fixture:
/Volumes/ingredion_en_dev/ingredion_dev/ext-ingredion-dev/pytest_scratch

If this ever needs to point elsewhere, override without touching code:
```python
os.environ["PYTEST_VOLUME_SCRATCH"] = "/Volumes/<catalog>/<schema>/<volume>/pytest_scratch"
```

## Bugs found and fixed

### 1. `.endswith(".json", ".jsonl")` — wrong argument form (the actual `.jsonl` bug)

All three discovery strategies in `list_json_files` originally filtered
with a single-extension check. The `.jsonl` fix was first written as:
```python
e.name.lower().endswith(".json", ".jsonl")
```
This is **not** "check either suffix" — `str.endswith()`'s second
argument is a slice *start index*, not a second suffix. Passing a string
there raises `TypeError: slice indices must be integers`. Confirmed via
the Databricks test run.

**Correct fix** — wrap both suffixes in a tuple, applied in all three
discovery strategies (`_try_dbutils_ls`, `_try_posix_ls`, the Hadoop
fallback):
```python
e.name.lower().endswith((".json", ".jsonl"))
```

### 2. `dbutils.fs.ls` on a missing directory — unhandled Databricks exception

`_try_dbutils_ls` had no error handling around `dbutils.fs.ls(source_dir)`.
When `source_dir` doesn't exist, Databricks raises its own internal
`CloudFileNotFoundException` (wrapped in a Spark Connect `ExecutionError`)
— not a `FileNotFoundError`. This propagated as a raw, unhandled
100+ line JVM stack trace instead of the clean `FileNotFoundError` the
function is documented to raise (previously only true for the Hadoop
fallback branch).

**Production impact, not just a test issue:** any real ingestion job
pointed at a missing `source_dir`, running on Databricks (where `dbutils`
is always available and tried first), would have surfaced this same raw
internal exception instead of a clear, actionable error message.

**Fix:**
```python
try:
    entries = dbutils.fs.ls(source_dir)
except Exception as exc:
    if "FileNotFoundException" in str(exc) or "does not exist" in str(exc).lower() or "No such file" in str(exc):
        raise FileNotFoundError(f"source_dir does not exist: {source_dir}") from exc
    raise
```

## Retry limit before quarantine (permanently-failing files)

Files that fail *ingestion* (not just the archival move) are tracked
across runs via a persisted retry counter, instead of being retried
forever with no escape hatch.

**Behavior:**
- State stored at `{source_dir}/_state/retry_state.json` — a simple
  `{file_path: consecutive_failure_count}` map
- On each ingestion failure, the counter increments. Below
  `max_ingestion_retries` (default 3), the file is left in place for the
  next run to retry
- At the threshold, the file is moved to `quarantine_files/` (same
  folder/purpose as the existing move-failure quarantine path) and its
  counter entry is cleared
- On a successful ingestion, any existing counter for that file is
  cleared — a file that fails once or twice but later succeeds is never
  quarantined
- The `_state/` subfolder is naturally invisible to `list_json_files`
  (non-recursive, `.json`/`.jsonl` filter only) — confirmed with a
  dedicated test

**Why this was added:** found during end-to-end deployment testing —
permanently broken files (bad syntax, unsupported structures) would
otherwise fail identically on every future run forever, with no signal
that a human needs to intervene.

**Gotcha hit while testing:** file paths returned by `list_json_files`
include Databricks' `dbfs:` scheme prefix (e.g.
`dbfs:/Volumes/.../flaky.json`), not just the plain path. Tests that
reconstruct an expected key manually (e.g. `f"{source_dir}/file.json"`)
will mismatch the actual dictionary key. Fixed by reading the real path
back from the `results` list rather than reconstructing it.

## Folder-as-table (subfolder merging)

Any subfolder found directly inside `source_dir` (one level deep only) is
now treated as one logical multi-file dataset: every file inside it is
read individually, successfully-read files are merged (`unionByName`,
`allowMissingColumns=True`), and the result is written to a single table
named after the folder (via the same `sanitize_table_name`/
`build_table_name` logic used for files).

**This is a deliberate behavior change** — subfolders were previously
silently ignored. The `_state/`, `processed/`, and `quarantine_files/`
bookkeeping folders (and anything else prefixed `_`) are explicitly
excluded from subfolder discovery, so they're never mistaken for a
folder-as-table unit.

**Read strategy — not a plain Spark folder read.** Spark's built-in
multi-file read (`spark.read.json(folder_path)`) is all-or-nothing at
schema-resolution time (see the `duplicate_keys.json` finding above — one
bad file can break the entire read). Since per-file retry-tracking is
required, each file is read individually via `read_json`, validated with
`.count()`, and only successfully-validated files are unioned. A single
malformed file inside a folder does not block the rest.

**Archival timing — validated, then written, then archived.** An early
version tried to archive each file immediately after validating it
(before the final write), which broke because Spark DataFrames are
lazy — the real read only happens when something forces it (`.count()`,
or the final write), and by the time the write ran, the source file had
already been moved. Fixed by moving archival to a second pass, strictly
*after* `run_on_dataframe()` succeeds — validated files are read a second
time by Spark at write time (acceptable I/O cost), and archival only
happens once nothing further needs to read from the original path.

**No `.cache()`/`.persist()` — not supported on serverless.** An
intermediate fix attempted `.cache()` to avoid the double-read above;
this fails outright on serverless compute
(`NOT_SUPPORTED_WITH_SERVERLESS: PERSIST TABLE is not supported`). The
validate-then-archive-after-write ordering above avoids needing
`.cache()` at all.

**Folder structure preserved in archival/quarantine.** Files inside a
folder archive to `processed/{date}/{folder_name}/{filename}` (not
flattened to `processed/{date}/{filename}`), so lineage back to the
source folder-table stays obvious. Same for `quarantine_files/`.
Implemented via a new `relative_subpath` parameter threaded through
`_move_file` and `_archive_ingested_file`.

**Retry-limit and schema_hint_ddl both apply per-file inside a folder,
unchanged.** The existing retry-state mechanism keys on full file path
(e.g. `.../orders/order1.json`), which naturally distinguishes files
across different folders with no code changes needed. `schema_hint_ddl`,
if passed to `ingest_directory_to_bronze`, applies to every file inside
every folder — recommended when a folder's files might have inconsistent
inferred types, since `unionByName` on independently-inferred schemas can
silently coerce mismatched types.

### Gotcha: test fixture folders left in source_dir get auto-ingested

Confirmed in practice: a `schema_hint/` fixture folder (built for the
schema-hint enforcement task, containing `type_mismatch.json` with an
intentionally mistyped `id` field) was left inside the real `raw/JSON/`
source directory. Since folder-as-table treats *any* subfolder as a
table automatically, it was auto-discovered and ingested as
`schema_hint_bronze` on the next real run.

Because no `schema_hint_ddl` was passed for that run, each file's schema
was inferred independently — `id` came out as BIGINT for three files and
STRING for `type_mismatch.json`. `unionByName` merged them without
forcing a single type, and the mismatch only surfaced later, at the
actual Delta write, as a hard `CAST_INVALID_INPUT` failure — not the
"safely rescued via `_rescued_data`" behavior documented for
`schema_hint_ddl`-enforced reads (that rescue behavior only applies when
an explicit schema hint is provided; plain schema inference has no such
safety net).

**Fix:** keep test/fixture folders outside any real `source_dir` used for
production ingestion — folder-as-table has no way to distinguish
intentional test data from real source folders. If a folder's files are
known to have inconsistent types on purpose, pass `schema_hint_ddl`
explicitly so every file is read against one consistent schema.

### Status
All 3 folder-as-table tests passing (merge into one table, one bad file
doesn't block the rest, folder structure preserved in archival) —
locally and on Databricks (serverless).

## Status (overall)

**All tests passing, both locally and on Databricks (serverless)** —
file discovery, `.jsonl` support, file archival, retry-limit quarantine,
and folder-as-table.