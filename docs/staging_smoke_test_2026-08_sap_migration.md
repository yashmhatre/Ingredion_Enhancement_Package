# Staging smoke test — SAP migration extract

**2026-08-20 · 5,012 rows across nine tables ingested and reconciled · 22 of 26 edge cases pass · 6 defects filed · release #370, #369**

The first run of this package against data shaped like a real cutover extract. Every earlier fixture was hand-built and at most a few hundred bytes; those prove the code runs, not how it behaves when a migration lands on it.

This run also closes out the blocking defect from the 0.6.0 staging validation on 2026-08-19. That run stopped at Phase 07 when a drifted file ingested as a no-op, reported `SUCCESS`, and was archived anyway.

## Environment

| | |
|---|---|
| Workspace | `adb-7405607398572130` |
| Catalog / schema | `ingredion_en.ingredion_stg` |
| Runs as | service principal `6ea945e0-2b4f-4746-b8f7-e7be51adc35a` |
| Branch / commit | `staging` @ `0bda9e4` |
| Package | `bronze_ingest-0.6.0` wheel, rebuilt and redeployed for this run |
| Fixtures | `generate_sap_migration.py --seed 20260820`, 5,012 rows, 26 cases |
| Volume | `/Volumes/ingredion_en/ingredion_dev/ext-ingredion-dev/STG/Raw/` |
| Verified from | SQL warehouse `ingredion-st`, as `fabricyash@gmail.com` |

Staging was serving a 0.6.0 wheel built from `d2cda99`, which predates the #366 fix. Running the drift cases against it would have reproduced the same false green the last run found, so `dev` was promoted to `staging` (`0bda9e4`) and the bundle redeployed first. CI passed on that commit before the deploy, and the deployed wheel was checked directly: `_resolve_idempotent_txn_version` returns `config.idempotent_txn_version` and derives nothing.

`ingredion_stg` held no tables when this started. Every table it now holds was created by this run.

## The blocking defect from the last run is fixed

EC-03 is the same shape that failed on 2026-08-19, run against the rebuilt wheel.

| | 2026-08-19 (pre-fix) | This run |
|---|---|---|
| Delta version | 2 → 2 | 1 → 2 |
| Table rows | 10 → 10 | 3 → 4 |
| Commit | none | `WRITE`, `numOutputRows` 1 |
| Audit `row_count` | 5, from the previous commit | 1 |
| Added column merged | no | yes |

The drifted row lands, `merge_schema` adds the column, and the audit row matches the commit instead of echoing the one before it. EC-04 and EC-05 pass on the same path.

Note that `idempotent_txn_version` is unset on the deployed job, so these writes carry no Delta idempotency protection. That is the documented default after #366: unprotected is safe, and a derived version is not.

## Migration extract

Three passes over `migration/`, one per format, because `source_format` drives discovery. JSON took `kna1.json`, `mara.json`, `vbak.jsonl`, `aufk.jsonl` and `mcha.jsonl`; CSV took the three `.csv` files; Parquet took `vbap.parquet`. Each pass ignored the other formats without erroring.

| Table | Source rows | Bronze | Quarantine | Reconciles |
|---|---|---|---|---|
| `kna1` | 400 | 400 | 0 | yes |
| `mara` | 500 | 500 | 0 | yes |
| `vbak` | 700 | 700 | 0 | yes |
| `vbap` | 1,800 | 1,800 | 0 | yes |
| `aufk` | 250 | 250 | 0 | yes |
| `mcha` | 200 | 200 | 0 | yes |
| `makt` | 1,000 | 1,000 | 0 | yes |
| `t001w` | 12 | 12 | 0 | yes |
| `lfa1` | 150 | 150 | 0 | yes |
| **Total** | **5,012** | **5,012** | **0** | |

All nine audit rows report `status: success` with `row_count` equal to `source_row_count` and to the count in the table. Six referential-integrity checks return zero orphans. CSV and Parquet ran on real compute for the first time.

Trailing spaces survive: `NAME1` is padded to width 35 in all 400 `kna1` rows and all 150 `lfa1` rows, and `MAKTX` keeps its padding. `ignoreTrailingWhiteSpace` is not stripping anything.

### The counts reconcile and the data is still wrong

Every CSV table lost its zero-padded keys to type inference (#371).

| Source | Format | Value in source | Value in bronze | Type |
|---|---|---|---|---|
| `mara.json` | JSON | `000000000001000001` | `000000000001000001` | string |
| `vbap.parquet` | Parquet | `000000000001000001` | `000000000001000001` | string |
| `makt.csv` | CSV | `000000000001000001` | `1000001` | int |
| `lfa1.csv` | CSV | `0000500001` | `500001` | int |
| `t001w.csv` | CSV | `"100"` | `100` | int |

All 1,000 `makt` rows and all 150 `lfa1` rows are affected, and `MANDT` — the client field every SAP table carries — became an integer.

Two things make this the most useful result in the run. Row counts reconcile exactly, so no count-based check notices. And the referential-integrity check `makt.MATNR subset of mara.MATNR` returned zero orphans, because Spark coerced the string side to a number to compare it. The check passed *because* of the corruption. A silver-layer join written the obvious way, string to string, matches nothing.

## Edge cases

Twenty-two of twenty-six pass. Every case has an outcome; none is unrun.

| ID | Result | Evidence |
|---|---|---|
| EC-01 | pass | 2 to bronze, 2 to quarantine, sums to 4 source rows |
| EC-02 | pass | duplicate `MATNR` quarantined, `000...001` and `000...003` kept |
| EC-03 | pass | 4 rows, `added:["NEW_FIELD"]`, column merged |
| EC-04 | pass | `removed:["MATKL"]`, three baseline rows keep `STARCH`, drift row null |
| EC-05 | pass | `type_changed` reports `BRGEW` double → string, values intact |
| EC-06 | pass | 3 rows, malformed line in `_corrupt_record` verbatim |
| EC-07 | pass | 5 rows — `.jsonl` overrode `multiline=true` (#146 holds) |
| EC-08 | pass | folder of one non-ingestable file reports skipped, not failed |
| EC-09 | pass | 12 rows in one table from 3 distinct source files |
| EC-10 | pass | JSON run took `mara.json`, ignored `_control.csv` and `extract.log` |
| **EC-11** | **fail** | write rejects `Gross Weight (KG)`, `Material Number` (#374) |
| **EC-12** | **fail** | write rejects `Order Id`; the collision question never arises (#374) |
| **EC-13** | **fail** | `MATNR`, `PSTLZ`, `MANDT` inferred as int; padding gone (#371) |
| EC-14 | pass | `"00000000"`, `""` and null stay distinct in one column |
| EC-15 | pass | `NAME1` is 35 characters; padding preserved |
| EC-16 | pass | `San Juan del Río`, `Mogi Guaçu`, `Krefeld Werk – Süd`, `上海工厂` |
| EC-17 | pass | four levels preserved; arrays of scalars and of structs intact |
| EC-18 | pass | 0-byte file gives an empty table, no `CANNOT_INFER_EMPTY_SCHEMA` |
| EC-19 | pass | header-only CSV gives 4 columns and 0 rows — the 2026-07-29 crash shape |
| **EC-20** | **fail** | defaults give 5 rows and a shifted garbage row (#375) |
| EC-21 | pass | refused without `schema_hint_ddl`; named and typed with one |
| EC-22 | pass | zero, `-1250.75`, `9.999999999999E10`, `0.30000000000000004` exact |
| EC-23 | pass | null, `""` and `" "` distinct; only null counts as missing |
| EC-24 | pass | 122 columns present |
| EC-25 | pass | append gives 4 rows, merge on `MATNR` gives 3 with the value updated |
| EC-26 | pass | `"1.10"` stays `"1.10"`; EANs and padded keys stay strings |

EC-26 is the control that makes #371 unambiguous. The same shapes that CSV destroyed — a padded `MATNR`, a `01234` postal code, `0001`, a version string `1.10` — all survive intact when they arrive as JSON.

### The failures

**EC-11 and EC-12** fail because identifier canonicalization exists only in `xml_reader.py`. A JSON field named `Gross Weight (KG)` reaches Delta unchanged and the write is rejected. The failure is loud and safe: the job fails, the audit row records `failure_stage: write` with the Delta error, and the source file moves to `quarantine_files/` rather than `processed/`. It is recoverable. It should not fail at all (#374).

**EC-13** is #371 in isolation, on the case written to catch it.

**EC-20** needs `multiLine` and `escape='"'` set by hand. With package defaults the quoted newline splits the record into a fifth row carrying values under the wrong columns; with both options set, all four assertions pass (#375).

## Defects filed

| Issue | Severity | What |
|---|---|---|
| #371 | high | CSV inference destroys zero-padded SAP keys; counts still reconcile |
| #374 | high | Identifier canonicalization is XML-only; JSON field names with spaces fail the write |
| #375 | medium | CSV defaults mis-parse RFC4180 escaped quotes and split quoted newlines |
| #376 | medium | A merge write records no row counts at all in the audit trail |
| #372 | low | Edge-case fixtures write JSON Lines under a `.json` extension |
| #373 | low | Drift cases EC-03/04/05 ship without the baseline they require |

## Corrections to the record

**#372 was filed wrong and has been corrected.** It originally claimed the `.json`/JSON-Lines mismatch causes row loss under the deployed `multiline: "true"`, reasoning from the measurement in `json_reader.effective_multiline`'s docstring. Tested directly, it does not: EC-01 run at `multiline=true` returned the same 2 bronze and 2 quarantine rows as at `multiline=false`, all four records accounted for.

The docstring records `multiLine=true -> 1 row` on a 3-record file, measured against a local Spark session. That did not reproduce on Databricks serverless. The override logic in `effective_multiline` is still right, but its stated justification does not hold on the platform the code runs on, and re-measuring it is worth doing.

**Two apparent defects were harness errors, not product defects.** Both are recorded because both cost time.

`_ingestion_audit` stores fully-qualified table names, so a filter on the bare name returns nothing — which reads exactly like a missing audit row. And a merge write is refused unless every merge key is also in `required_columns`; the first EC-25 merge attempt failed for that reason. The refusal is correct and well-argued: `NULL = NULL` is null in a SQL MERGE, so a null merge key never matches and inserts a duplicate on every run.

**Every ingestion archives its source into `processed/` inside the source directory.** A second run over the same path finds nothing, because discovery skips `processed/` and `quarantine_files/`. This is correct, and it means any case needing a second pass over one file needs a fresh copy at a new path. Four case steps had to be re-run that way.

## Scope and limits

Staging only. No `GRANT`, `REVOKE`, `DROP` or `VACUUM` was run by an agent. The `GRANT SELECT ON SCHEMA` from the previous run was still in place, so no new grant was needed.

**Not covered:** streaming and Auto Loader; XML, which `approved_formats()` still refuses pending #336 and #337; retry under real transient failure; UC tag application, which stays NULL because the staging job configures no tags; `ai_metadata_stg` and `bronze_maintenance_stg`, neither of which was run; and scale — 5,012 rows shows correctness, not partitioning or clustering behaviour.

The drift baseline is harness-built, not shipped by the generator (#373). It reproduces `mara_row()` exactly: the same eight columns, `BRGEW` numeric, three rows with distinct `MATNR`.

EC-12's actual question — whether three colliding names are disambiguated rather than last-wins — is still unanswered. The run fails before reaching it.

## State left behind

36 bronze tables, 3 quarantine tables, `_ingestion_audit` (47 rows: 41 success, 6 failed) and `_schema_registry` (36 rows, one per bronze table; quarantine tables are deliberately not registered). Every table carries CDF and 30-day retention, which is a storage line item.

The 6 failed audit rows are three attempts each for EC-11 and EC-12, which `max_retries: 2` retried to exhaustion. Three of the bronze tables belong to EC-20 rather than to one case each: `ec20_kna1_bronze` from the case itself, plus `ec20ctl_kna1_bronze` and `ec20esc_kna1_bronze` from the two control runs that isolated #375.

Nothing was dropped. The tables are left in place so the numbers in this record can be re-queried; cleanup is a separate decision.

Source files for successful units are in `processed/` under their case directories. The two files that failed the write, `ec11_identifier_canonicalization/zcustom.json` and `ec12_canonicalization_collision/zcollide.json`, are in `quarantine_files/` beside them, along with `ec21_headerless/t001w.csv` from its expected refusal.
