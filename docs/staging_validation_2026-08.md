# Staging validation — bronze ingestion on 0.6.0

**2026-08-20 · 13 checks passed, 1 blocking defect found · release #359, #365**

First run of the bronze package in staging. Until now staging had never executed this code: the workspace was serving a `bronze_ingest-0.4.0` wheel deployed on 2026-07-29, with one of the three bundle jobs registered and no successful run in its history.

The `dev` → `staging` promotion merged in git on 2026-08-19. The bundle deploy that makes it real had not been run. This record covers that deploy and the smoke test that followed, worked from `artifacts/staging_smoke_test_0.6.0.html`.

## Environment

| | |
|---|---|
| Workspace | `adb-7405607398572130` |
| Catalog / schema | `ingredion_en.ingredion_stg` |
| Runs as | service principal `6ea945e0-2b4f-4746-b8f7-e7be51adc35a` |
| Branch / commit | `staging` @ `d2cda99` |
| Package | `bronze_ingest-0.6.0` wheel, serverless environment dependency |
| Fixtures | `/Volumes/ingredion_en/ingredion_dev/ext-ingredion-dev/STG/Raw/` |
| Jobs | `bronze_directory_ingestion_stg`, `ai_metadata_stg`, `bronze_maintenance_stg` |

Staging reads and writes `STG/` under the shared `ext-ingredion-dev` volume, per the 2026-08-19 decision in `docs/decisions/2026-08_per_environment_volumes.md`. No data flows into this environment, so the suite generates its own fixtures — nine files, several malformed on purpose.

## Results

| Scope | Checks |
|---|---|
| Deploy and job configuration | 4/4 |
| Ingestion, quality gate, directory handling | 5/5 |
| Audit trail and schema registry | 2/2 |
| Archival | 1/1 |
| 0.6.0 change surface | 1/2 |
| Drifted-schema write | **0/1 — blocking** |

## The blocking defect

**A drifted file ingests as a no-op, reports success, and is archived anyway.**

Reproduced twice. A file whose schema differs from the existing table — one column added, one removed — produces no Delta commit, no error, and no `failure_stage`, while the job returns `SUCCESS: 1 unit(s) ingested` and the source file moves to `processed/`. The rows are not in the table and the source is no longer in `Raw/`.

| | Run 3 | Run 4 |
|---|---|---|
| Input | 2 rows, `new_column` added, `customer` removed | 3 rows, same shape |
| Delta version, before → after | 2 → 2 | 2 → 2 |
| Table rows, before → after | 10 → 10 | 10 → 10 |
| Audit `status` | `success` | `success` |
| Audit `row_count` | 5 | 5 |
| Audit `failure_stage` | NULL | NULL |
| Source file after the run | archived | archived |

`merge_schema` defaults to `True` (`config.py:183`), and the commits that did land record `canMergeSchema: "true"`, so the added column should have merged. It did not: `new_column` is absent from the table schema.

The audit `row_count` of 5 is a second defect on top of the first. It is the `numOutputRows` of the *previous* run's commit. `_write_metrics` reads `history(1)`, and a run that commits nothing reads whatever commit is latest. The docstring at `bronze_writer.py:481` anticipates this for concurrent writers and argues that `max_concurrent_runs: 1` closes it. It does not close this case: no concurrency is involved. The audit trail therefore asserts that five rows landed when none did, which is the failure mode #305 already established for the returned dict.

On prod, archival makes this unrecoverable without going to the storage container.

## What the evidence shows

**Ingestion.** `orders_clean.json` → 5 rows in `orders_clean_bronze`, queryable after the run. Audit columns hold real values: `_source_file` the full Volume path, `_ingested_at` a real timestamp, `_batch_id` the job run id. A second run appended to 10 across 2 distinct batches rather than replacing.

**#146 regression held.** `orders.jsonl` read with `multiline=true` returned 3 rows, not 1.

**Quality gate.** `orders_quarantine_test.json`: 4 source rows, 2 with a null `order_id` — 2 to bronze, 2 to `orders_quarantine_test_bronze_quarantine`, and the counts add up. Verified by querying both tables.

**Directory ingestion.** Each top-level file became its own table; `folder_a/` merged its two files into one table of 2 rows from 2 distinct source files. `orders.csv` and `orders.parquet` were ignored without erroring on a JSON run, and both were left in place rather than archived.

**Archival.** All five ingested units moved to `processed/2026-08-20/`. This is the `WRITE VOLUME` grant working; it was the deploy's one prerequisite.

**Schema registry.** Five rows, one per table, upsert-keyed on `table_name`. A flat row count after a drift run is correct.

**Change Data Feed, on by default.** `delta.enableChangeDataFeed = true` with both `delta.logRetentionDuration` and `delta.deletedFileRetentionDuration` at `interval 30 days`, on the bronze table and the quarantine table alike. This is 0.6.0's highest blast-radius change and a real storage line item. Whether staging keeps it is still an open decision.

**`schema_drift_json` is populated.** The drifted run recorded `{"added":["new_column"],"removed":["customer"],"type_changed":[]}`, `schema_changed: true`, and a changed fingerprint. The 0.6.0 plumbing fix works. It is the one column that correctly recorded a run that then lost the data.

**XML is refused.** `formats.approved_formats()` returns `csv`, `json`, `parquet`. `xml` sits in `_PENDING_SIGNOFF` naming #336 and #337.

## Corrections to the runbook

`artifacts/staging_smoke_test_0.6.0.html` needs six fixes. Each cost time during this run.

1. It pins `HEAD` at `06506fd`. Staging is `d2cda99` since #365.
2. It says to expect `bundle validate` to fail on a missing volume. #363 removed that resource; validate passes.
3. It assumes the operator can query the results. The service principal owns every table it creates, and schema ownership conveys no `SELECT`, so the human running the runbook could read nothing until a `GRANT SELECT ON SCHEMA` was issued. This recurs on prod.
4. It asserts `folder_empty/` "reports `status: skipped`". Skipped units write no audit row at all — `directory_ingestion.py:186` returns before any audit write. The status is visible only in the returned dict, which the same runbook says not to trust.
5. It lists `orders_drift.json` as proving schema drift. As a top-level file it lands in its own new table with no baseline, so no drift occurs. Drift requires a second ingest into an existing table.
6. It asserts the three 0.6.0 audit columns will be "populated, not uniformly NULL". `tags_failed` and `tag_outcome_json` stay NULL because the staging job configures no `table_tags` or `column_tags`, so tagging never runs. The assertion is not executable as written.

`source_row_count` reading 2 against a 4-row source file is not a defect. For append it is documented as rows entering the write, not rows in the file (`bronze_writer.py:470`). The name invites the misreading, and #61, #62 and #109 consume the column.

## Scope and limits

Staging only. No `GRANT`, `REVOKE`, `DROP` or `VACUUM` was executed by an agent; the one grant this run required was executed by Yash.

**Not covered:** `ai_metadata_stg` and `bronze_maintenance_stg` were deployed and their configuration verified, but neither was run — Phase 07 stopped when the defect above was confirmed. Also uncovered: streaming and Auto Loader, merge write mode, CSV and Parquet ingestion runs, retry under real transient failure, and UC tag application.

`bronze_maintenance_stg` holds its weekly Sunday 03:00 UTC schedule `PAUSED` and ships `vacuum: "true"`. It stays paused until it is signed off.

## Deploy notes

The bundle root `/Workspace/Shared/.bundle/ingredion_enhancement_package/staging` is writable by all workspace users, on a `mode: production` target. The CLI warns on every deploy. Restricting it, or granting `CAN_MANAGE` to `group_name: users` deliberately, is an open decision.

## State left behind

`orders_clean_bronze` holds 10 rows and is missing the rows from runs 3 and 4. The drifted source files are in `processed/2026-08-20/`. `orders.csv`, `orders.parquet` and `folder_empty/` remain in `STG/Raw/`. Every table in `ingredion_stg` was created by this run and is disposable.
