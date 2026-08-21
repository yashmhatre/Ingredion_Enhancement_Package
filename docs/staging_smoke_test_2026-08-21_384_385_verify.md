# Staging smoke test — #384/#385 re-verification

**2026-08-21 · release #388 (pending), `dev` @ `9b32310` deployed to staging · both fixes confirmed on real compute**

The 2026-08-21 promotion re-verification (#386) found that two of the six #371-#376 fixes did not hold under closer testing: #374's EC-12 case still raised `COLUMN_ALREADY_EXISTS`, and #376's merge audit rows still went `NULL` when an auto-OPTIMIZE committed right after a MERGE. Both were re-diagnosed and fixed in commit `99a30f0` (#384, #385), merged to `dev` via PR #387. This run redeploys that commit to staging and re-runs the two failing cases against it.

## Environment

| | |
|---|---|
| Workspace | `adb-7405607398572130` |
| Catalog / schema | `ingredion_en.ingredion_stg` |
| Runs as | service principal `6ea945e0-2b4f-4746-b8f7-e7be51adc35a` |
| Branch / commit | `dev` @ `9b32310` (PR #387), staged via `release/promote-384-385-to-staging` (PR #388, not yet merged) |
| Package | `bronze_ingest-0.6.0` wheel, rebuilt and redeployed for this run |
| Fixtures | hand-built, matching the exact shapes from the #386 run's failures |
| Verified from | SQL warehouse `ingredion-st`, via the Statement Execution API, as `fabricyash@gmail.com` |

`staging` was still running the wheel from PR #383 (`77bfe4f`), which predates both fixes. `databricks bundle deploy -t staging` rebuilt and redeployed the wheel from `dev` tip before either case was run.

## What was re-run

Two fixtures, each in its own top-level folder under `STG/Raw/` so a `write_mode` correct for one case doesn't apply to the other (folder-as-table discovers every immediate subfolder of `source_dir` in one pass):

- `verify_384_385_ec12/ec12_collision/zcollide.json` - `{"Order Id": "A1", "Order.Id": "B2", "order_id": "C3"}`, the exact input #384 was filed against
- `verify_384_385_ec25/ec25_merge_optimize/` - two JSON batches landed one run apart, `merge_keys: ["MATNR"]`, reproducing #385's insert-then-update-with-insert sequence

## Results

| Case | 2026-08-21 (#386) | This run | Verdict |
|---|---|---|---|
| EC-12 (three names collide to one identifier) | fail - `COLUMN_ALREADY_EXISTS` | **pass** - 3 distinct columns, 1 row | Fixed (#384) |
| Merge + auto-OPTIMIZE audit row | fail - audit row `NULL` | **pass** - `row_count=2`, `rows_inserted=1`, `rows_updated=1` | Fixed (#385) |

### EC-12: three colliding names, once

`ec12_collision_bronze` now has three columns - `Order_Id`, `Order_Id_2`, `order_id_3` - instead of the write rejecting on the second `order_id` collision. Queried directly:

```
Order_Id | Order_Id_2 | order_id_3
A1       | B2         | C3
```

All three source values survive under their own column. The fix keys collision grouping on the lowercased canonical name, so `Order Id`/`Order.Id`/`order_id` are recognized as three colliding names up front rather than two looking distinct until Delta's case-insensitive match rejected the write.

### Merge audit row: survives an intervening OPTIMIZE

`ec25_merge_optimize_bronze`'s `DESCRIBE HISTORY`:

```
version 3  OPTIMIZE  auto=true, numRemovedFiles=2, numAddedFiles=1        <- no numTargetRows* keys
version 2  MERGE     numTargetRowsMatchedUpdated=1, numTargetRowsInserted=1
version 1  MERGE     numTargetRowsInserted=2
```

Same shape as the #385 failure: auto-optimize committed immediately after the second MERGE. The audit row for that run now reads `row_count=2, rows_inserted=1, rows_updated=1` - version 2's numbers, not version 3's nulls. `_read_write_metrics` now walks back through history and skips the OPTIMIZE commit instead of reading only the latest one. The underlying data is also correct: `MATNR ...001` updated `BRGEW` 25.0 to 27.5, `MATNR ...003` inserted at 40.0, `MATNR ...002` untouched at 30.0.

## Scope and limits

Staging only. No `GRANT`, `REVOKE`, `DROP` or `VACUUM` was run. Only the two previously-failing cases were re-run - the full 26-case matrix and the nine-table migration extract were already validated on 2026-08-20 and 2026-08-21 and don't exercise either changed code path.

**Not covered:** the full edge-case matrix, the nine-table migration extract, XML (still gated pending #336/#337), streaming/Auto Loader, and scale.

## State left behind

Two new tables under `ingredion_en.ingredion_stg`: `ec12_collision_bronze` (1 row) and `ec25_merge_optimize_bronze` (3 rows, two merge commits plus one auto-OPTIMIZE). Source files are in `processed/` under their landing paths; nothing was dropped.

## Release status

PR #388 (`release/promote-384-385-to-staging` -> `staging`) is open, CI-gated, and unmerged pending approval. It fast-forwards cleanly - no conflicts with the current `staging` tip.
