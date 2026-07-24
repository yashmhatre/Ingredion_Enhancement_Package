# JSON Reader Validation — ADLS Notebook

## Purpose

Manual validation of `json_reader.py` against real ADLS-hosted JSON files,
covering structural variety, multi-file merges, bad/malformed data, and
schema-hint (`schema_hint_ddl`) enforcement. This is **not** part of the
local `pytest` suite — it requires a live Databricks cluster with Unity
Catalog access to the `ingredion` container.

Notebook: `bronze_json_loader/notebooks/validate_json_reader.py`

## Fixture location

```
abfss://ingredion@ingredionenpkgdev.dfs.core.windows.net/raw/JSON/
  single_object.json
  array_of_objects.json
  lines.jsonl
  nested_one_level.json
  nested_three_level.json
  array_field.json
  array_of_arrays.json
  malformed.json
  empty_file.json
  empty_object.json
  empty_array.json
  mixed_valid_malformed.jsonl
  nulls.json
  unicode.json
  duplicate_keys.json
  numeric_as_string.json
  multi_file/
    orders_a.json
    orders_b.json
  schema_hint/
    full_match.json
    extra_fields.json
    missing_optional.json
    type_mismatch.json
```

Storage account: `ingredionenpkgdev` (container name `ingredion` is
different from the account name — easy to confuse, see gotchas below).

Schema-hint cases are all validated against the same DDL:
`"id INT, name STRING, age INT"`.

## Known gotchas (hit during initial setup)

1. **Storage account name ≠ container name.** The container is `ingredion`;
   the actual storage account is `ingredionenpkgdev`. Using the container
   name as the account name produces `SparkKeyProviderException: Invalid
   configuration value detected for fs.azure.account.key` (Unity Catalog
   doesn't recognize the path as governed, falls back to legacy key auth,
   which isn't configured).

2. **Doubled path segment.** `azure_setup.md`'s `container/folder`
   shorthand (e.g. `ingredion/raw/`) describes container + folder, not a
   literal nested folder named `ingredion` inside the container. The
   correct path has no repeated segment: `.../raw/JSON`, not
   `.../ingredion/raw/JSON`.

3. **Namespace package collision** after moving to Databricks Repos/Git
   folders — the deployed workspace path changed, and Databricks
   auto-adds the repo root to `sys.path` *before* any manual
   `sys.path.append(...)` runs. The package has a nesting quirk (real
   code lives one level deeper, in `bronze_json_loader/bronze_json_loader/`
   — the outer folder has no `__init__.py`), so Python finds the empty
   outer folder first and treats it as a namespace package, producing
   `ImportError: cannot import name 'IngestionConfig'` even though the
   correct path is also present later in `sys.path`.

   **Fix:** use `sys.path.insert(0, ...)` instead of `append(...)`, then
   run `dbutils.library.restartPython()` before re-importing (the broken
   namespace import is cached in memory and won't self-correct just from
   fixing `sys.path`).

   ```python
   import sys
   sys.path.insert(0, "/Workspace/Users/<your-user>/Ingredion_Enhancement_Package/bronze_json_loader")
   dbutils.library.restartPython()
   ```

4. **Missing fixture files produce confusing test failures**, not obvious
   "file not found" errors, if the file was never uploaded. Always verify
   with `dbutils.fs.ls(...)` on the exact fixture path before assuming a
   failure is a code bug.

## Finding: duplicate JSON keys are a hard failure, not silently resolved

**Test case:** `duplicate_keys.json` — `{"id": 1, "name": "Alice", "name": "Bob"}`

**Expected (per original test design):** Spark takes a "last value wins"
approach and reads the row without error.

**Actual behavior:** Spark's schema inference scans the file, detects two
fields both named `name`, and raises:

```
AnalysisException: [COLUMN_ALREADY_EXISTS] The column `name` already
exists. Choose another name or rename the existing column. SQLSTATE: 42711
```

This happens during **schema resolution**, before `PERMISSIVE` mode's
corrupt-record handling ever gets a chance to run — so this is not
something `corrupt_record_column` catches. It is a hard, uncatchable
failure for the whole read, not a per-row quarantine case.

**Conclusion:** `json_reader.py`'s behavior here is correct and consistent
with underlying Spark semantics — this is a genuine data-quality
constraint of the source, not a bug in the reader. The test
(`duplicate_keys_documented_limitation`) now asserts the expected
`AnalysisException` is raised, rather than expecting a clean read.

**Recommendation for source data producers:** JSON files with duplicate
keys at the same nesting level will fail the entire batch read, not just
the offending row. If this is a realistic risk for any upstream source,
it needs to be caught before ingestion (e.g. a pre-validation step), since
the pipeline's existing quarantine mechanism (`quality.py`) operates on
already-successfully-read rows and cannot help here.

## Finding: type mismatches against schema_hint_ddl are safely rescued, not lost

**Test case:** `type_mismatch.json` — `{"id": "not_a_number", "name": "Dave", "age": 40}`
against schema hint `"id INT, name STRING, age INT"`.

**Actual behavior:** the mistyped field (`id`) is set to `NULL` in its
typed column, but the original raw value is preserved in `_rescued_data`:

```
id=NULL, name=Dave, age=40,
_rescued_data={"id":"not_a_number","_file_path":".../type_mismatch.json"}
```

The read succeeds cleanly — no exception, no corrupt-record routing.

**Conclusion:** type mismatches against an explicit schema hint are
non-destructive by default. Downstream consumers relying on strict typing
will see `NULL` for the mistyped field, but nothing is silently dropped —
the original value remains recoverable from `_rescued_data` for debugging
or reprocessing. This is meaningfully safer behavior than the
duplicate-keys case above (a full read failure) — worth keeping in mind
when reasoning about how different kinds of malformed source data will
actually surface downstream.

## How to run

1. Confirm fixture files exist at the path above:
   ```python
   display(dbutils.fs.ls("abfss://ingredion@ingredionenpkgdev.dfs.core.windows.net/raw/JSON/"))
   display(dbutils.fs.ls("abfss://ingredion@ingredionenpkgdev.dfs.core.windows.net/raw/JSON/schema_hint/"))
   ```
2. Open `notebooks/validate_json_reader.py`, attach to a serverless cluster
3. Confirm `sys.path.insert(0, ...)` points at your current deployed path
4. Run all cells
5. Read the final report cell:
   ```python
   report_df[report_df["status"] != "PASS"]
   ```

## Status

**24/24 passing.**

- 19/19 base JSON reader cases (structural variety, multi-file, bad data,
  edge cases) — including `duplicate_keys_documented_limitation`, which
  correctly asserts the expected `AnalysisException` rather than a clean
  read
- 5/5 schema-hint enforcement cases (`schema_hint_full_match`,
  `schema_hint_extra_fields_rescued`, `schema_hint_missing_field_is_null`,
  `schema_hint_absent_regression`, `schema_hint_type_mismatch_nulls_and_rescues`)

## Related issues

- `.jsonl` files not discovered by `directory_ingestion.py`'s file
  listing — separate bug, filed
- Move/archive processed files after ingestion — separate task, filed,
  deferred (streaming/Auto Loader evaluation deferred within it)