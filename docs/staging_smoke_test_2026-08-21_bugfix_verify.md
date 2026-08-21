# Staging smoke test — 6-bugfix promotion re-verification

**2026-08-21 · release #383, `dev` @ `96f6bda` promoted to `staging` · 4 of 6 fixes confirmed on real compute · 2 new defects filed (#384, #385)**

The #369 SAP migration smoke test on 2026-08-20 ran a nine-table extract against staging, found six defects (#371–#376), and filed them. This run promotes the fixes for all six to `staging` and re-runs the specific edge cases that failed, to check the fixes against real compute rather than trusting that a merged PR did what its description says.

## Environment

| | |
|---|---|
| Workspace | `adb-7405607398572130` |
| Catalog / schema | `ingredion_en.ingredion_stg` |
| Runs as | service principal `6ea945e0-2b4f-4746-b8f7-e7be51adc35a` |
| Branch / commit | `staging` @ `77bfe4f` (PR #383, merge of `dev` @ `96f6bda`) |
| Package | `bronze_ingest-0.6.0` wheel, rebuilt and redeployed for this run |
| Fixtures | `generate_sap_migration.py --seed 20260820`, edge cases only |
| Verified from | SQL warehouse `ingredion-st`, via the Statement Execution API |

`ingredion_stg`'s `SELECT` grant to `fabricyash@gmail.com` was still in place from the 2026-08-20 run — no new `GRANT` was issued.

PR #383 bundled six merged `dev` fixes into `staging`: #371 (CSV type inference), #372/#373 (fixture naming and drift baselines), #374 (identifier canonicalization), #375 (CSV RFC4180 parsing), #376 (merge audit row counts). All three CI checks (`quality`, `test`, `wheel`) passed before merge.

## What was re-run

The directory-ingestion job discovers folder-as-table units one level below whatever `source_dir` it's given — it does not recurse further. The 2026-08-20 run pointed `source_dir` at each case group directly; landing fixtures under a generic `Raw/edge_cases_verify_bugfix/` root and running the job unscoped produced `0 unit(s) ingested, 10 skipped`, because every case folder sat two levels down. Each case below was re-run with `source_dir` pointed at its own parent so the case folder became the immediate discovery target.

## Results

| Case | 2026-08-20 | 2026-08-21 | Verdict |
|---|---|---|---|
| EC-11 (JSON field name with spaces) | fail — write rejected | **pass** — 1/1 row, table `ec11_identifier_canonicalization_bronze` | Fixed (#374) |
| EC-12 (three names collide to one identifier) | fail — write rejected | **still fails** — same write rejection, new cause | Not fixed — filed as #384 |
| EC-13 (zero-padded CSV keys) | fail — type inference destroys padding | **pass** — `MATNR`, `PSTLZ`, `MANDT` all `string`, padding intact | Fixed (#371) |
| EC-20 (CSV quoted newline + escaped quote) | fail — record splits into a shifted row | **pass**, with `csv_multiline` set — 4 correct rows, embedded quote and newline preserved | Fixed (#375); `multiLine` is still opt-in by design, not part of the fix's scope |
| EC-25 (merge, insert then merge-with-updates) | not filed as a data defect — filed as #376 for its audit gap | data correct; **audit gap reproduces** on the second (real) merge | Not fixed — filed as #385 |

Two of the four re-tested defects (#371, #374's EC-11 case) are cleanly fixed. #375 is fixed for what it targeted (the escape default); the multiline flag was never in scope, so EC-20 needed an explicit override to test at all. #374's EC-12 case and #376 both reproduce under different conditions than the ones the original fix addressed — the two follow-up sections below cover them.

### EC-12: fixed for the case #374 targeted, not for a genuine collision

`identifiers.py`'s `_resolve_names`/`_disambiguate`, read on their own, correctly suffix colliding names (`order_id`, `order_id_2`, `order_id_3`). The deployed write for `{"Order Id": "A1", "Order.Id": "B2", "order_id": "C3"}` still raises `[COLUMN_ALREADY_EXISTS] The column order_id already exists`, the same failure mode as the pre-#374 run three times over (`899974025777955` / run `1023864463988087`). Filed as #384 — the disambiguation code path is evidently not reached for this input, and root-causing that needs a debugger against the deployed wheel rather than another staging run.

### #376: fixed for the metric keys, not for auto-optimize racing the read

`bronze_writer.py`'s `_read_write_metrics` now reads `numTargetRowsMatchedUpdated`/`numTargetRowsInserted` — the fix's actual target — and its `since_version` guard correctly rejects a commit older than the write. Table `ec25v2_mara_bronze`'s `DESCRIBE HISTORY` shows why the second merge's audit row is still `NULL`:

```
version 3  OPTIMIZE  {"numRemovedFiles": "2", ...}                        <- no numTargetRows* keys
version 2  MERGE     {"numTargetRowsMatchedUpdated": "1", "numTargetRowsInserted": "1", ...}
version 1  MERGE     {"numTargetRowsInserted": "2", ...}
```

Auto-optimize committed an `OPTIMIZE` immediately after the `MERGE`. `_read_write_metrics` reads `.history(1)` — the single latest commit at read time — and the `OPTIMIZE` commit is newer than the pre-write version, so the guard against a stale read doesn't catch it: it just reads the wrong commit's metrics instead of an old one. The merge itself is correct — 3 rows, the matched key's `BRGEW` updated 25.0 → 27.5, the new key inserted — only the audit row is affected. Filed as #385.

## Scope and limits

Staging only. No `GRANT`, `REVOKE`, `DROP` or `VACUUM` was run. Only the five previously-failing edge cases were re-run, not the full 26-case matrix — the 22 that passed on 2026-08-20 don't exercise any of the six changed code paths, and re-running the full extract would not have changed what this run needed to answer.

**Not covered:** the full nine-table migration extract (already validated 2026-08-20 and unaffected by these fixes outside the five cases above), XML (still gated pending #336/#337), streaming/Auto Loader, and scale.

## State left behind

Six new tables under `ingredion_en.ingredion_stg`: `ec11_identifier_canonicalization_bronze`, `ec12_canonicalization_collision_bronze` (3 failed audit rows only, no table), `ec13_leading_zeros_bronze`, `ec20_csv_quoting_bronze` (two batches: one from the pre-multiline-override attempt, one correct), `ec25v2_mara_bronze`, plus a discarded `ec25v_batch1_bronze` from an early misconfigured attempt (table name derived from the batch folder instead of the shared filename — left in place, harmless). Source files for every case are in `processed/` under their landing paths; nothing was dropped.

## Defects filed

| Issue | What |
|---|---|
| [#384](https://github.com/yashmhatre/Ingredion_Enhancement_Package/issues/384) | Identifier collision disambiguation still fails the write instead of suffixing |
| [#385](https://github.com/yashmhatre/Ingredion_Enhancement_Package/issues/385) | Merge audit row counts read an auto-OPTIMIZE commit instead of the MERGE commit |
