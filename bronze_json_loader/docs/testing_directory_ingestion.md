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
        .appName("bronze_json_loader-tests")
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

## Real Unity Catalog path — do not trust `azure_setup.md`'s example names

The actual catalog/schema/volume in this workspace differ from the
placeholder names originally documented in `azure_setup.md`:

| | Documented (azure_setup.md) | Actual |
|---|---|---|
| Catalog | `workspace` | `ingredion_en_dev` |
| Schema | `bronze` | `ingredion_dev` |
| Volume | `ingredion` | `ext-ingredion-dev` |

Confirmed via **Catalog Explorer** in the Databricks UI (catalog → schema
→ Volumes tab). `azure_setup.md` should be corrected to match — filed as
a follow-up, not yet done as of this writing.

Full working scratch path used by the `json_test_dir` fixture:
```
/Volumes/ingredion_en_dev/ingredion_dev/ext-ingredion-dev/pytest_scratch
```

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

## Status

**All tests passing, both locally and on Databricks (serverless).**

## Related issues

- `azure_setup.md` needs correcting — documented catalog/schema/volume
  names don't match the actual workspace (see table above)

## File archival (move/archive processed files)

`ingest_directory_to_bronze` now archives successfully-ingested files via
`_archive_ingested_file` / `_move_file`, with three outcomes:

1. **Clean success** → moved to `processed/{date}/`
2. **Move fails** → falls back to `quarantine_files/` for manual review
   (kept separate from the pre-existing, unused `quarantine/` folder
   reserved for manually-quarantined raw files, to avoid ambiguity between
   a human-driven process and this automated one)
3. **Everything fails** → file left in place in `raw/` (backlog), logged
   clearly — never silently lost, never blocks other files in the same run

Scoped to directory ingestion only — batch/single-table ingestion
(`order_bronze.yaml`-style, reading a whole folder into one table) doesn't
map per-file moves as cleanly, and is left as a possible future
refinement.

Tests use `monkeypatch` to simulate move failures and confirm each
fallback path. All 4 archival tests pass locally and on Databricks.

### Gotcha: mixed import styles broke monkeypatching

Initial test draft mixed `from module import func` (top of file) with
`import module as alias` (inside test functions). If these resolve to
different loaded instances of the same module — a real risk in this repo
given the recurring `sys.path`/duplicate-module issues seen throughout
this testing effort — `monkeypatch.setattr(alias, "func", ...)` patches
the wrong copy, and the original function still runs. Fixed by using a
single `import bronze_json_loader.directory_ingestion as di` consistently
throughout the file, with all calls going through `di.`.

### Gotcha: sys.path via "Run tests" button skips notebook setup

Databricks' file-editor "Run tests" button runs `pytest` directly, without
executing any notebook cell first — so hardcoded `sys.path.insert(...)`
calls in notebooks never take effect in this entry point. Fixed
permanently by having `conftest.py` compute the package path dynamically
from its own file location (`os.path.dirname(os.path.dirname(os.path.abspath(__file__)))`)
instead of relying on a hardcoded workspace path — works identically
regardless of entry point (notebook, "Run tests" button, or terminal),
and removes the recurring stale-path/wrong-identity failure mode for good.

## Status

**All tests passing** — file discovery, `.jsonl` support, and file
archival, both locally and on Databricks (serverless).