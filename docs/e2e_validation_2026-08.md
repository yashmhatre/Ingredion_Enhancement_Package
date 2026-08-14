# End-to-end validation — bronze ingestion on live Databricks

**2026-08-14 · 21/21 checks passed · epic #343**

First run of the bronze package against real compute, real Unity Catalog, and real Delta tables. Until now the 685-test suite proved only that the code works against a local Spark session and a temp directory.

## Environment

| | |
|---|---|
| Workspace | `adb-7405607398572130` (`bronze-json-loader-dev`) |
| Compute | serverless client 3 — Spark 4.1.0, Python 3.12.3, **Spark Connect** |
| Catalog / schema | `ingredion_en.ingredion_dev` |
| Fixtures | `/Volumes/ingredion_en/ingredion_dev/ext-ingredion-dev/DEV/e2e/` |
| Package | `bronze_ingest-0.5.0` wheel, installed as a serverless environment dependency |

No data flows into this environment, so the suite **generates its own fixtures** — nine files, several malformed on purpose. That is the point: you cannot prove a quality gate works using data that is already clean.

## Results

| Issue | Scope | Checks |
|---|---|---|
| #344 E2E-1 | Fixture generation and landing | 2/2 |
| #345 E2E-2 | Single-file JSON → bronze Delta | 4/4 |
| #346 E2E-3 | Quality gate and quarantine | 3/3 |
| #347 E2E-4 | Directory ingestion | 4/4 |
| #348 E2E-5 | Audit trail and schema registry | 3/3 |
| #349 E2E-6 | Replay of quarantined files | 1/1 |
| #350 E2E-7 | Chain A multi-format plumbing | 4/4 |

## What the evidence actually shows

**Ingestion.** `orders_clean.json` → 5 rows in `e2e_orders_clean`, queryable after the writing session ended. Audit columns populated with real values, not placeholders: `_source_file` = the full Volume path, `_ingested_at` a real timestamp, `_batch_id` = `20260814T075738886749Z`. A second run appended to 10 rather than replacing.

**#146 regression held.** `orders.jsonl` read with `multiline=True` returned **3 rows, not 1**. `effective_multiline` forces multiLine off for JSON-lines extensions. Left unguarded this returns only the first record *with no error* — the failure mode that makes it worth an explicit check.

**Quality gate.** 4 input rows, 2 with a null `order_id`: bronze 2, quarantine 2, and the counts add up. `fail_on_quality_error: true` raised `DataQualityError: 2 row(s) failed data quality checks (null in one of required_columns=['order_id'])`. Duplicates: 3 in, bronze 2, quarantine 1. Verified by querying both tables — the run's own return value agreeing with itself proves nothing.

**Audit and drift.** 29 audit rows accumulated. Ingesting `orders_drift.json` over the baseline produced `schema_changed: true`, a changed fingerprint, and `{"added":["new_column"],"removed":["customer"],"type_changed":[]}`. `merge_schema` added the column to the Delta table.

The schema registry is **upsert-keyed on `table_name`**, not append-only — one row per table with `first_seen_at` / `last_updated_at`. A flat row count across a drift run is correct, and an assertion expecting growth would be wrong. Worth knowing before someone writes that assertion.

**Directory ingestion.** Each top-level file became its own table; `folder_a/` merged its two files into one. `folder_empty/` reported `status: "skipped"`, `reason: "no json files in folder"` — the #307 wording, and the skipped-vs-failed distinction intact. No `"failed"` status for a non-failure.

**Replay.** A quarantined `.json` moved back to the source directory; a `.csv` sitting beside it **stayed in quarantine**. Asserted against the Volume listing, not the returned dict — the returned dict is exactly what #305 showed can lie.

## The check CI can never make

`list_source_files` has three listing strategies. **CI only ever reaches `_try_posix_ls`** — the `dbutils` branch is untestable off-cluster by construction, so #304 shipped it unverified.

This run exercised it for real. It found the five JSON fixtures, correctly treated `orders.jsonl` as JSON, and **ignored `orders.csv` without erroring**. The two branches agree.

That was the single most valuable thing this suite could have found, and it found no defect.

## Constraints for anyone writing against this workspace

Three of these cost time during the #318 probe and will cost it again:

- **`.load()` is lazy under Spark Connect.** A read that "succeeds" has proven nothing until an action forces it. The first version of the #318 probe reported every format as present — including a `json` control that should have been impossible to get wrong.
- **Public DBFS root is disabled** (`DBFS_DISABLED`); **`file:/tmp` is refused**. Only `/Workspace` and Volume paths are readable.
- **`/Workspace` has FUSE publish lag.** Files written and read in the same cell hit `PATH_NOT_FOUND`. Write, wait, then read.

## Scope and limits

Dev only. No `GRANT`/`REVOKE`/`DROP`/`VACUUM`, nothing touching staging or prod. Tables are prefixed `e2e_` and are disposable.

**Not covered:** streaming / Auto Loader (#248 covers it separately), merge write mode, retry-under-real-transient-failure, archival to a different Volume, and any non-JSON format — only `json` is registered today.

## Reproducing

The harness lives at `/Users/fabricyash@gmail.com/e2e_bronze` in the workspace and is submitted as a one-off serverless run with the wheel as an environment dependency. Every check reports PASS/FAIL and **never aborts on the first failure** — a run that stops tells you about one defect; a run that completes tells you about all of them.
