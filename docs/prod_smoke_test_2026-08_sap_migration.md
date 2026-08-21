# Prod smoke test — SAP migration extract at 100k rows

**2026-08-21 · 100,012 rows across nine tables, first data prod has ever processed · every count reconciles, zero defects found · release #390**

The first data run against `ingredion_prd`. `main` had not moved since #189 (0.5.0, 2026-08-02); this run follows the promotion of `staging` to `main` in #390, which brought #371, #374, #375, #376, #384 and #385 to production along with everything else closed since 0.5.0. `ingredion_prd` held a schema and an empty volume path before this run and nothing else — no prior job had written to it.

Where the 2026-08-20 staging run (5,012 rows, `docs/staging_smoke_test_2026-08_sap_migration.md`) established correctness against a realistic cutover shape, this run is the one staging explicitly could not do: it puts a fixture 20x the size through the same three-pass, three-format load and checks whether anything that held at 5,012 rows stops holding at 100,012.

## Environment

| | |
|---|---|
| Workspace | `adb-7405607398572130` |
| Catalog / schema | `ingredion_en.ingredion_prd` |
| Runs as | service principal `8cbc9ba5-b4be-47b7-8a1d-576eb7d1a2e9` (prod-sp) |
| Branch / commit | `main` @ `0ceaf28` (PR #390, staging → main) |
| Package | `bronze_ingest-0.6.0` wheel, built and deployed for this run |
| Fixtures | `generate_sap_migration.py --rows 100000 --seed 20260821`, 100,012 rows |
| Volume | `/Volumes/ingredion_en/ingredion_dev/ext-ingredion-dev/PROD/Raw/migration/` |
| Verified from | SQL warehouse `ingredion-st`, as `fabricyash@gmail.com` |

## Git and deploy path to get here

`staging` was not clean until this run's promotion started. The 2026-08-21 re-verification of the six bugfixes promoted in #383 found two of them reproducing under different conditions — #374's collision case and #376's row-count logic — filed as #384 and #385. Both were fixed in #387 and promoted to `staging` in #388 before `staging` went anywhere near `main`. #390 then promoted the resulting `staging` tip — 0.6.0 plus the #384/#385 fix, 246 commits ahead of `main`'s prior tip — in one PR, reviewed and merged rather than fast-forwarded silently.

The bundle was deployed with `databricks bundle deploy -t prod`, and the deploy summary was checked directly rather than assumed: the `bronze_directory_ingestion_prd`, `ai_metadata_prd` and `bronze_maintenance_prd` jobs all show up under the prod target with their expected names and IDs. `bronze_maintenance_prd` deploys paused, per the 0.6.0 changelog note that unpausing it — it runs `VACUUM` — needs Tier 2 approval; it stayed paused for this run.

## Nine tables, 100,012 rows, every count reconciles

Same shape as the staging run: three passes over `migration/`, one per format, `source_format` overridden per pass at the CLI (`databricks bundle run bronze_directory_ingestion -t prod -- --source_dir .../migration --source_format <json|csv|parquet>`). Each pass only picked up the files matching its format and ignored the rest.

| Table | Format | Source rows | Bronze | Quarantine | Reconciles |
|---|---|---|---|---|---|
| `kna1` | JSON | 8,000 | 8,000 | 0 | yes |
| `mara` | JSON | 10,000 | 10,000 | 0 | yes |
| `vbak` | JSON Lines | 14,000 | 14,000 | 0 | yes |
| `aufk` | JSON Lines | 5,000 | 5,000 | 0 | yes |
| `mcha` | JSON Lines | 4,000 | 4,000 | 0 | yes |
| `makt` | CSV | 20,000 | 20,000 | 0 | yes |
| `t001w` | CSV | 12 | 12 | 0 | yes |
| `lfa1` | CSV | 3,000 | 3,000 | 0 | yes |
| `vbap` | Parquet | 36,000 | 36,000 | 0 | yes |
| **Total** | | **100,012** | **100,012** | **0** | |

All nine audit rows report `status: success`, `row_count` equal to `source_row_count`, and zero quarantined rows — read directly from `_ingestion_audit`, not taken from what the job returned. `_schema_registry` holds exactly nine rows, one per bronze table.

The three job runs completed back to back: JSON in 2m40s, CSV in 2m18s, Parquet in 2m00s, roughly 7 minutes of Databricks job wall time end to end for 100,012 rows on the same `2X-Small` compute the staging run used. Nothing here was pushed hard enough to say anything about partitioning or clustering — this is one directory load, not a sustained or concurrent one — but at this volume nothing timed out, throttled, or needed a retry.

## What #371, #374, #376, #384 and #385 predicted, checked at 20x the row count

Everything the staging smoke test found and this project fixed was checked again here, against real row counts rather than the low hundreds staging used.

**#371 — CSV type inference destroying zero-padded keys.** `csv_infer_schema` now defaults to `false`. Across all 20,000 `makt` rows and all 3,000 `lfa1` rows, `MATNR` reads back as `string`, sampled values like `000000000001000001` intact. `t001w.MANDT`, the client field, is also `string` across all 12 rows. None of this depends on row count, but the staging run only had 1,000 and 150 rows respectively to check it against; this run has 20x and 20x.

**#374 / #384 — identifier canonicalization and collision handling.** The migration extract's field names don't contain spaces or collide by design — that's what the 26-case edge matrix on staging exists to isolate, and it isn't rerun here. What this run does confirm is that canonicalization being active on every read path doesn't slow down or break an ordinary 100k-row load: every unit succeeded on the first attempt, no retries, no `write` failures.

**#376 / #385 — merge row counts and OPTIMIZE.** This run used `write_mode: append` throughout, the same as staging's migration extract, so it doesn't exercise the merge path directly. The #376/#385 fix was verified against its own targeted fixtures on staging (`docs/staging_smoke_test_2026-08-21_bugfix_verify.md`, `docs/staging_smoke_test_2026-08-21_384_385_verify.md`) before this promotion; this run doesn't re-test it, and shouldn't be read as having done so.

**Referential integrity holds at scale.** `makt.MATNR subset of mara.MATNR` and `vbap.VBELN subset of vbak.VBELN`: zero orphans on both, checked directly against the 20,000- and 36,000-row tables.

**Trailing-space preservation holds at scale.** All 8,000 `kna1.NAME1` values are still padded to width 35 — `ignoreTrailingWhiteSpace` isn't stripping anything, whether the table has 400 rows or 8,000.

No new defect surfaced in this run. That is a narrower claim than "the pipeline is correct at scale": this run loaded one directory once, in three sequential passes, under no concurrent load and no retry. It says the specific defects staging found and this project fixed stay fixed at 20x the row count, not that scale introduces no risk of its own.

## An observation worth a follow-up, not a defect

Source files were not archived to `processed/` after ingestion. All nine files remain at their original upload path under `migration/` with unchanged modification timestamps, rather than moving to `processed/{date}/` as `fs/archival.py`'s `archive_ingested_file` is documented to do and as the staging run observed for its own migration extract. This didn't affect correctness — audit rows report success, `_schema_registry` is fully populated, and every count reconciles — but a second run against this same path would attempt to re-read files that should have been consumed, since discovery skips `processed/` and `quarantine_files/` but never checked for their absence here. Worth a direct look at why archival didn't run for this ingestion path before this volume sees a second load.

## Scope and limits

Prod only. No `GRANT`, `REVOKE`, `DROP` or `VACUUM` was run by an agent — the `GRANT SELECT ON SCHEMA` needed to verify this run's results was requested and applied by Yash directly, since `ingredion_prd` had never been queried by a human before this run.

**Not covered:**

- The 26-case edge-case matrix. It was exercised twice on staging (`docs/staging_smoke_test_2026-08_sap_migration.md`, `docs/staging_smoke_test_2026-08-21_bugfix_verify.md`, `docs/staging_smoke_test_2026-08-21_384_385_verify.md`) and this run doesn't repeat it — the ask here was volume, not a second correctness pass.
- Merge writes, and by extension a live re-check of #376/#385 against this run's own data. Append only.
- Streaming and Auto Loader.
- XML, which `approved_formats()` still refuses pending #336 and #337.
- Retry under real transient failure, and why archival didn't run for this path.
- Unity Catalog tag application — the prod job configures no tags, so tag outcomes stay null.
- `ai_metadata_prd` and `bronze_maintenance_prd`, neither of which was run. `bronze_maintenance_prd` stays paused pending Tier 2 approval.
- Concurrency and sustained load. Three sequential passes over one directory is not a production traffic pattern.

## State left behind

`ingredion_prd` now holds 9 bronze tables, `_ingestion_audit` (9 rows, all success), and `_schema_registry` (9 rows, one per bronze table). Every table carries CDF with the default 30-day retention. Nothing was dropped or overwritten — this is the schema's first data.

Source files for all nine units remain at `/Volumes/ingredion_en/ingredion_dev/ext-ingredion-dev/PROD/Raw/migration/`, none of them moved to `processed/`. The 26-case edge-case fixtures generated alongside this run's migration extract were not uploaded or run against prod; they exist only in the local fixture output used to generate the migration set.

A second load against this path would attempt to re-read all nine files as new, since discovery has no record that they were already ingested beyond the audit table itself. Clearing or archiving `migration/` by hand before running anything else against this volume is the safe next step.
