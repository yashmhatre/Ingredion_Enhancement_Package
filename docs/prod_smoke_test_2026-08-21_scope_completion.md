# Prod scope completion — the gaps the 100k run left, closed with fresh data

**2026-08-21 · release #390, `ingredion_prd` · 26/26 edge cases pass, merge+OPTIMIZE audit confirmed, 2 defects found**

The 2026-08-21 100k-row prod smoke test (`docs/prod_smoke_test_2026-08_sap_migration.md`) proved the nine-table migration extract at scale, but its own "Not covered" section named eight gaps: the 26-case edge matrix, merge writes, streaming, XML, retry-under-failure, UC tag application, the two advisory jobs, and concurrency. This run closes the three that are realistically testable against `ingredion_prd` right now — the edge matrix, merge writes, and UC tags — plus follows up on that run's archival observation, all with fresh fixtures (`generate_sap_migration.py --seed 20260821002`) rather than reusing the 100k run's data. It does not attempt VACUUM/Tier-2-gated work, XML, streaming, or concurrency; see **Scope and limits** for why each stays out.

## Environment

| | |
|---|---|
| Workspace | `adb-7405607398572130` |
| Catalog / schema | `ingredion_en.ingredion_prd` |
| Runs as | service principal `8cbc9ba5-b4be-47b7-8a1d-576eb7d1a2e9` (prod-sp) |
| Branch / commit | `main` @ `0ceaf28` (unchanged since the 100k run; no redeploy needed) |
| Package | `bronze_ingest-0.6.0`, already deployed |
| Fixtures | `generate_sap_migration.py --rows 2000 --seed 20260821002`, 26 edge cases + a standalone merge/archival/tag set |
| Verified from | SQL warehouse `ingredion-st`, via the Statement Execution API, as `fabricyash@gmail.com` |

## Required scope, closed vs. still open

Every item either the 100k run or this run named as scope. "Closed here" means fresh evidence exists in this run; "closed prior" means an earlier run already covered it and nothing here contradicts that.

| Scope item | Status | Evidence |
|---|---|---|
| Nine-table migration extract at volume | closed prior | 100k run, 2026-08-21 |
| 26-case edge matrix in prod | **closed here** | this run, all 26 pass |
| Merge writes + merge/OPTIMIZE audit interaction | **closed here** | this run, `ec25_merge_optimize_bronze` |
| Referential integrity, trailing-space preservation at scale | closed prior | 100k run |
| Archival of source files after ingestion | **closed here — found broken** | this run, reproduced 3 times |
| UC tag application | **closed here — found broken** | this run, `tag_check_bronze` |
| Retry under real transient failure | still open | needs a fault-injection harness, not attempted |
| `ai_metadata_prd` / `bronze_maintenance_prd` | still open | neither job was run |
| Streaming / Auto Loader | still open | package only supports `source_format: json` in streaming mode (#323); not exercised |
| XML | still open | gated pending Tier 2 sign-off on #336/#337 |
| VACUUM | still open | `bronze_maintenance_prd` stays paused pending Tier 2 approval; no agent runs VACUUM |
| Concurrency / sustained load | still open | explicitly out of scope for this pass; needs a load-test design, not a fixture rerun |

## The 26-case edge matrix, run clean

The first attempt collided: the fixture generator deliberately reuses real SAP table names (`mara`, `kna1`, `vbap`, …) across unrelated cases, and `table_name_template` only keys off the filename — so 17 different runs all wrote into one shared `mara_bronze`. Re-run with a per-case `--table_name_template "<caseid>_{filename}_bronze"` override, matching the pattern the 2026-08-20 staging run already used (`ec20_kna1_bronze`, `ec20ctl_kna1_bronze`). The first attempt's contaminated tables (`mara_bronze`, `kna1_bronze`, `t001w_bronze`, `vbap_bronze`, `vbak_bronze`, `mcha_bronze`, `vbrk_bronze`, `vbrp_bronze`, `baseline_bronze`) are left in place — dropping them needs `DROP TABLE`, which no agent runs — and are called out again under **State left behind**.

Two cases also needed a config field the first pass omitted, because a plain rerun would have only reproduced the default behavior, not the thing being proven:

- **EC-01/EC-02** needed `required_columns`/`unique_columns` set explicitly — without them there's nothing to quarantine against, and the first pass's row counts didn't match the fixture's own manifest.
- **EC-20** needed `csv_multiline: true` — the #375 fix corrected the default *escape* handling, not the multiline default, which stays `false` by design (multiline CSV can't be split across tasks). Without the override, EC-20 still shows the pre-fix 5-row split; that's expected default behavior, not a regression.
- **EC-21** needed `csv_header: false` *and* `schema_hint_ddl` — the first pass set only the hint, so `csv_header` defaulted `true` and the reader silently consumed the first data row as a header instead of refusing.

| ID | Proves | Result | Evidence |
|---|---|---|---|
| EC-01 | `required_columns` quarantines bad rows | pass | 2 bronze + 2 quarantined = 4 source rows |
| EC-02 | `unique_columns` quarantines the duplicate deterministically | pass | 2 bronze + 1 quarantined = 3 source rows |
| EC-03 | schema drift (added column) detected and merged | pass | `added:["NEW_FIELD"]`, column present in the merged table |
| EC-04 | schema drift (removed column) detected, old rows keep their value | pass | `removed:["MATKL"]` |
| EC-05 | schema drift (type change) reported, not silently coerced | pass | `type_changed: BRGEW double → string` |
| EC-06 | PERMISSIVE mode isolates a corrupt line, doesn't fail the file | pass | 3 rows; malformed line verbatim in `_corrupt_record` |
| EC-07 | `.jsonl` overrides `multiline=true` (#146) | pass | 5 rows, not 1 |
| EC-08 | a folder with nothing ingestable reports skipped, not failed | pass | `0 unit(s) ingested, 1 skipped` |
| EC-09 | folder-as-table: 3 files become 1 table | pass | 12 rows in one table |
| EC-10 | format-aware discovery ignores non-matching files | pass | only `mara.jsonl` ingested; `_control.csv`, `extract.log` ignored |
| EC-11 | identifier canonicalization applies to JSON, not just XML (#374) | pass | field names with spaces/parens/umlaut all land, no write rejection |
| EC-12 | colliding canonical names are disambiguated, not last-wins (#384) | pass | `Order_Id`, `Order_Id_2`, `order_id_3` — all 3 source values present |
| EC-13 | CSV inference no longer destroys zero-padded keys (#371) | pass | `MATNR` (18 chars) and `PSTLZ` (`01234`) padding intact |
| EC-14 | SAP null-date conventions stay distinct in bronze | pass | `"00000000"`, `""` and `NULL` all present and distinct |
| EC-15 | `ignoreTrailingWhiteSpace` doesn't strip fixed-width padding | pass | `NAME1` is 35 characters in both rows |
| EC-16 | UTF-8 survives read/write/catalog round-trip | pass | `San Juan del Río`, `Mogi Guaçu`, `Krefeld Werk – Süd`, `上海工厂` |
| EC-17 | deep nesting (4 levels, arrays of scalars and structs) preserved | pass | struct/array shape intact, values readable |
| EC-18 | 0-byte file doesn't crash with `CANNOT_INFER_EMPTY_SCHEMA` | pass | empty table, 0 rows, run succeeds |
| EC-19 | header-only CSV is a valid empty table, not a crash | pass | 4 named columns, 0 rows |
| EC-20 | `csv_multiline` fixes the quoted-newline split, quotes unescape (#375) | pass (with override) | 4 rows, not 5; `NAME1` reads `He said "premium grade"` |
| EC-21 | headerless CSV fails closed without a schema hint, succeeds with one | pass | refused (`RuntimeError`) without hint; 2 named/typed rows with `schema_hint_ddl` |
| EC-22 | numeric precision and negatives survive | pass | `0.0`, `-1250.75`, `9.999999999999E10`, `0.30000000000000004` exact |
| EC-23 | null / empty string / single space stay three distinct values | pass | all three present and distinguishable |
| EC-24 | column count alone doesn't break the write or catalog comment path | pass | 122 source columns present |
| EC-25 | append gives 4 rows; merge on a key gives 3 with the value updated | pass | see **Merge writes**, below |
| EC-26 | numeric-looking strings are never coerced to numbers | pass | `"1.10"` stays `"1.10"`, all keys stay strings |

Twenty-six of twenty-six pass — a improvement on the 2026-08-20 staging baseline of 22/26, because #371/#374/#375/#376/#384/#385 fixed exactly the four that failed there, and this run re-proves all of them fresh in prod rather than trusting the staging record to still apply.

## Merge writes, including the merge+OPTIMIZE audit interaction (#376/#385)

Two sequential merges into `ec25_merge_optimize_bronze` on `merge_keys: ["MATNR"]`, one folder path, files swapped by hand between runs (archival being broken meant it wouldn't have cleared the first file on its own — see below):

1. Batch 1 (2 rows, `MATNR` `...001` and `...002`) → insert 2. Audit: `row_count=2, rows_inserted=2, rows_updated=0`.
2. Batch 2 (`...001` updated `BRGEW` 25.0→27.5, `...003` new) → `DESCRIBE HISTORY` shows an auto-`OPTIMIZE` committed immediately after the `MERGE` (version 3 OPTIMIZE, version 2 MERGE) — the exact shape that broke the audit trail before #385. Audit row for this run: `row_count=2, rows_inserted=1, rows_updated=1` — version 2's numbers, not nulled by the OPTIMIZE commit.

Final table: 3 rows, `MATNR ...001` at `27.5`, `...002` untouched at `30.0`, `...003` inserted at `12.0`. #376/#385 hold in prod, on fresh data, with the same OPTIMIZE-adjacency shape that caused the original failure.

## Two defects found

### Archival silently fails in prod

`archive_ingested_file` (`bronze_layer/bronze_ingest/fs/archival.py`) tries to move a successfully-ingested file to `processed/{date}/`, falls back to `quarantine_files/` if that fails, and leaves the file in place only if both moves fail — logging a warning either way, never raising. That return value (`move_status`/`move_detail`) is **never written to `_ingestion_audit`** — only the per-file job-run result carries it, which nothing queries after the fact.

Reproduced three times with fresh, single-purpose fixtures:

- A standalone one-record file uploaded to a brand-new folder, ingested successfully, checked immediately after: still at its original path. No `processed/` or `quarantine_files/` folder was created under it at all — meaning **both** the primary move and the quarantine fallback failed.
- Every source file across all 26 edge cases, all ingested successfully: none moved.
- The 100k run's own `migration/` folder: still holds all nine original files, unchanged, as the earlier report already noted.

Root cause is not confirmed (querying UC grants directly requires an `auth token` call this session doesn't run), but `bronze_layer/README.md`'s own grant runbook only documents `GRANT USE CATALOG` and the per-environment `GRANT USE SCHEMA, CREATE TABLE, MODIFY, SELECT` — it never lists a `WRITE VOLUME` grant step for any environment's service principal. Reads succeed (ingestion works); only writes to the volume (archival) fail. That is consistent with `prod-sp` — and possibly `staging-sp`/`dev-sp` — never having been granted `WRITE VOLUME`, which the bundle's own comments flag as needed for exactly this archival path. Recommended next step: confirm the grant directly (`SHOW GRANTS ON VOLUME ...` as an admin), and separately, persist `move_status`/`move_detail` to `_ingestion_audit` so a future archival failure is visible without re-deriving it from the volume by hand.

### UC table-tag application fails on every attempt

`apply_catalog_tags` (`bronze_layer/bronze_ingest/catalog_metadata.py`) is reachable via `per_file_config_json`'s `table_tags` field — confirmed by tracing `IngestionConfig.__dataclass_fields__` validation, not assumed. Set a free-form tag (`prod_scope_test`, not a governed `class.*`/`system.*`/`ai.*`/`sap.*` key) on a fresh single-record ingestion:

- Job succeeded; audit row shows `tags_failed: true`.
- `tag_outcome_json`: `{"applied":0,"failed":["...: 'WorkspaceClient' object has no attribute 'entity_tag_assignments'"],"skipped_governed":[]}`.
- `ingredion_en.information_schema.table_tags` confirms zero rows for the table — the tag never reached the catalog.

Unlike archival, this failure **is** correctly surfaced in the audit trail (`tags_failed`/`tag_outcome_json` exist precisely for this — #64's design holds). The defect is a `databricks-sdk` version mismatch: the deployed SDK's `WorkspaceClient` doesn't expose `entity_tag_assignments`, which `tag_client.WorkspaceTagClient` depends on. Job-level tags (the bundle's `environment`/`layer` tags on `bronze_directory_ingestion_prd` itself) are unaffected — those are a different mechanism and were already visible in the job definition before this run. Recommended next step: pin or upgrade `databricks-sdk` in `bronze_layer`'s dependencies to a version that has `entity_tag_assignments`, then re-run this same fixture to confirm.

## Scope and limits

Prod only. No `GRANT`, `REVOKE`, `DROP` or `VACUUM` was run by an agent. `databricks bundle deploy -t prod` was not re-run — `main` hasn't moved since the 100k run's deploy, so the already-deployed `0.6.0` wheel is identical to what a redeploy would produce.

**Still not covered, and why:**

- **Retry under real transient failure.** Needs a fault-injection harness (a source that fails partway through a read/write), not a fixture variation. Not built.
- **`ai_metadata_prd` and `bronze_maintenance_prd`.** Neither job was run. `bronze_maintenance_prd` stays paused pending Tier 2 approval (it runs `VACUUM`).
- **Streaming / Auto Loader.** The package's streaming mode only supports `source_format: json` (#323); multi-format Auto Loader is deferred. Exercising even the json-only path would be a materially different test design (a running stream, not a batch job) and wasn't attempted here.
- **XML.** Still refused by `approved_formats()` pending Tier 2 sign-off on #336/#337.
- **Concurrency and sustained load.** Explicitly deferred by the user's own scoping for this pass — it needs a load-test design (parallel job triggers against the same schema), not a fixture rerun, and risks contending with the prod SP's `max_concurrent_runs: 1` job setting in ways worth designing deliberately rather than as a side effect of this pass.

## State left behind

**New, correctly-isolated tables** (per-case `table_name_template`, no cross-case collisions): `ec01fix_mara_bronze` (+`_quarantine`), `ec02fix_mara_bronze` (+`_quarantine`), `ec03b_baseline_bronze`, `ec03b_mara_bronze`, `ec04b_baseline_bronze`, `ec04b_mara_bronze`, `ec05b_baseline_bronze`, `ec05b_mara_bronze`, `ec06_mara_bronze`, `ec07_vbak_bronze`, `ec08` (skipped, no table), `ec09_vbap_bronze`, `ec10_mara_bronze`, `ec11_zcustom_bronze`, `ec12_zcollide_bronze`, `ec13_mara_bronze`, `ec14_mcha_bronze`, `ec15_kna1_bronze`, `ec16_t001w_bronze`, `ec17_zspec_bronze`, `ec18_vbrp_bronze`, `ec19_vbrk_bronze`, `ec20fix_kna1_bronze`, `ec21refuse2_t001w_bronze` (failed run, no table), `ec21hint2_t001w_bronze`, `ec22_vbap_bronze`, `ec23_kna1_bronze`, `ec24_zmara_ext_bronze`, `ec25_merge_optimize_bronze`, `ec26_zkeys_bronze`, `archival_check_bronze`, `tag_check_bronze`, `prod_scope_merge_202608b`'s `ec25_merge_optimize_bronze`.

**Contaminated tables from the first (mis-namespaced) attempt, needing manual cleanup** — each holds mixed rows from multiple unrelated cases sharing a filename: `mara_bronze` (17 runs' worth), `kna1_bronze` (7), `t001w_bronze` (5), `vbap_bronze` (5), `vbak_bronze` (3), `mcha_bronze` (3), `baseline_bronze` (4), `vbrk_bronze` (2), `vbrp_bronze` (2), plus single-case tables that ran twice due to a background-script race (`zcollide_bronze`, `zcustom_bronze`, `zkeys_bronze`, `zmara_ext_bronze`, `zspec_bronze`, `lfa1_bronze`, `makt_bronze`, `aufk_bronze`) — these are harmless duplicates of correct data, not cross-contaminated, but still extra. None of this was dropped; `DROP TABLE` needs a human.

Source files for every case in `edge_case_matrix_202608b/`, the merge test's `prod_scope_merge_202608b/`, `archival_check/` and `tag_check/` remain at their original upload paths — consistent with the archival defect above, not a separate issue. A second run against any of these paths would re-read the files as new.

`ec21refuse2_t001w_bronze` does not exist — that run's whole point was to fail, and it did (`RuntimeError`, no table written).
