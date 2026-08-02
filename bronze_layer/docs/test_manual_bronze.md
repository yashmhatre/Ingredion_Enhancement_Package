# Bronze layer — manual test plan

A tester-facing catalogue of what to test, how, and what "correct" looks
like. Written for someone who did not build the package.

Companion to the developer-facing validation logs in this folder
(`testing_json_reader.md`, `testing_directory_ingestion.md`,
`testing_end_to_end_deployment.md`), which record what *was* tested. This
document records what *should* be tested, before a release.

---

## Read this first — readiness, honestly

The two supported input modes are **not** equally proven, and a tester
should know which is which before planning time.

| | Batch JSON | Streaming JSON (Auto Loader) |
| --- | --- | --- |
| Automated test coverage | Extensive — 322 tests, run on every PR | **Partial by construction** |
| Executed end to end on Databricks | **Yes** — `testing_end_to_end_deployment.md` | **No record exists** |
| Deployed job definition | Yes, `bronze_directory_ingestion` | Job definition exists; no validated run |
| Verdict | **Ready to deploy** | **Needs this manual pass before production** |

### Why streaming is the gap

Auto Loader (`cloudFiles`) is a Databricks Runtime feature. **It does not
exist in OSS Spark, so no test in the suite can execute it.** The streaming
path is therefore tested *by construction* rather than *by execution*:

- `read_json_stream`'s options are asserted against a recording fake — this
  proves the right values are passed, and nothing about whether Auto Loader
  honours them
- `assert_no_silent_truncation` is tested against an ordinary batch
  DataFrame shaped like a micro-batch — the logic is covered; it has never
  fired against a real stream
- The per-stream hoisting of schema-registry and catalog-metadata work
  (#156) has never been observed on a real stream

None of that is a defect. It is a limit of what can be automated off-platform,
and it is exactly the gap a manual pass closes. **Sections 6 and 7 below are
the highest-value part of this document.**

### Known gaps a tester should not be surprised by

These are open issues, not bugs to raise:

| Behaviour | Issue |
| --- | --- |
| No `OPTIMIZE`/`VACUUM`/retention on any table this package creates. Small files accumulate | #159 |
| Change Data Feed is not enabled | #58 |
| All environments read subpaths of one Volume; any principal that can read its own can read `PROD/Raw/` | #160 |
| No secret support — a source needing a credential cannot be configured safely | #115 |
| Deploys are manual | #113 |

---

## Environment and fixtures

### What you need

- A Databricks workspace with Unity Catalog, and a catalog/schema you can
  create tables in
- A UC Volume you can write files to
- The bundle deployed to a target (`databricks bundle deploy -t dev`)
- Permission to read `_ingestion_audit` and `_schema_registry`

### Fixture files to prepare

Create these once; most cases reuse them.

| File | Content | Used by |
| --- | --- | --- |
| `orders_good.json` | 5 records, one pretty-printed JSON array or object per file, all fields present | 2.x, 4.x |
| `orders_nulls.json` | 5 records, 2 with `order_id` null | 5.x |
| `orders_dupes.json` | 5 records, 2 sharing an `order_id` | 5.x |
| `orders.jsonl` | **JSON Lines** — 3 records, one per line, no enclosing array | 2.4, 6.3 |
| `orders_nested.json` | Records with a nested `customer` object and an array field | 3.x |
| `orders_malformed.json` | 3 valid records plus 1 line of invalid JSON | 2.5 |
| `orders_empty.json` | `[]` or an empty file | 2.6 |
| `orders_extra_col.json` | Same as `orders_good.json` plus one new field | 9.x |

> **Keep a pristine copy outside the source directory.** Successful
> ingestion **moves** files into `processed/`, so a re-run needs fresh
> copies. This surprises people.

### How to check results

```sql
-- The table
SELECT * FROM <catalog>.<schema>.orders_bronze;

-- The run record (one row per run per table)
SELECT run_id, table_name, status, row_count, source_row_count,
       rows_inserted, rows_updated, write_mode, stream_batch_id,
       quarantined_row_count, failure_stage, error_message, started_at
FROM   <catalog>.<schema>._ingestion_audit
ORDER  BY started_at DESC;

-- Schema state (one row per table, not per run)
SELECT * FROM <catalog>.<schema>._schema_registry;

-- Rejected rows
SELECT * FROM <catalog>.<schema>.orders_bronze_quarantine;
```

---

## 1. Configuration validation — fail before compute starts

The point of these is that a bad config costs **nothing**. If any case here
starts a cluster or reaches a table, that is the finding.

| ID | Set | Expect |
| --- | --- | --- |
| 1.1 | `table: "orders-2026"` (hyphen) | Raises at config load, message names `table` and states the allowed pattern |
| 1.2 | `retry_attempts: 0` | Raises; message explains 1 means "try once, do not retry" |
| 1.3 | `retry_delay_seconds: -5` | Raises |
| 1.4 | `ingestion_mode: streaming`, `write_mode: overwrite` | Raises — every micro-batch would replace the whole table |
| 1.5 | `write_mode: merge`, no `merge_keys` | Raises |
| 1.6 | `write_mode: merge`, `merge_keys: [order_id]`, `required_columns: []` | Raises — merge keys must be non-null-guaranteed |
| 1.7 | `reader_options: {path: "/somewhere/else"}` | Raises — `path` is not on the allowlist |
| 1.8 | `reader_options: {cloudFiles.maxBytesPerTrigger: "10g"}` | **Accepted** — `cloudFiles.*` is allowed wholesale |
| 1.9 | `ingestion_mode: streaming`, no `checkpoint_location` | Raises |
| 1.10 | A key that does not exist, e.g. `tabel: "orders"` | Raises naming the unknown key — it is **not** silently dropped |
| 1.11 | `dedupe_before_merge: true` with `write_mode: append` | **Warns** (does not raise); log says the setting is ignored |

**Why 1.10 matters:** a silently-dropped key is how a configured quality
rule was inert in production (#145). Check the *log*, not just the outcome.

---

## 2. Batch ingestion — single file

| ID | Case | Steps | Expect |
| --- | --- | --- | --- |
| 2.1 | Happy path | Ingest `orders_good.json`, `write_mode: append` | 5 rows; audit row `status=success`, `row_count=5`, `write_mode=append`; registry row created |
| 2.2 | Audit columns present | After 2.1, inspect the table | Every row has `_ingested_at`, `_batch_id`, `_source_file`. `_source_file` names the **actual file**, not the directory |
| 2.3 | Re-run appends | Re-run 2.1 with a fresh copy | 10 rows total, two distinct `_batch_id` values, two audit rows |
| 2.4 | **JSON Lines** | Ingest `orders.jsonl` with `multiline: true` | **3 rows, not 1.** A warning states the extension overrode the config |
| 2.5 | Malformed record | Ingest `orders_malformed.json` | Job succeeds; valid rows land; the bad one is captured in `_corrupt_record` rather than failing the run |
| 2.6 | Empty source | Ingest `orders_empty.json` | Run does **not** fail. No rows, or a clean zero-row outcome |

**2.4 is a regression check for a silent data-loss defect (#146).** Getting
1 row instead of 3 with no error is exactly the failure it guards.

---

## 3. Nested JSON — bronze preserves structure

| ID | Case | Expect |
| --- | --- | --- |
| 3.1 | Ingest `orders_nested.json` | Nested `customer` remains a **struct**, arrays remain arrays. Bronze does **not** flatten |
| 3.2 | Query a nested field | `SELECT customer.name FROM ...` works |
| 3.3 | With `schema_hint_ddl` set, ingest a file containing an extra field | The extra field is captured in `_rescued_data`, not dropped silently |

Flattening is deliberately a Silver concern. A tester seeing flat columns
here should raise it.

---

## 4. Directory ingestion — one table per file

| ID | Case | Expect |
| --- | --- | --- |
| 4.1 | Directory with 3 differently-named `.json` files | 3 tables, named per `table_name_template` (e.g. `orders_bronze`) |
| 4.2 | Filenames needing sanitising, e.g. `orders-2026 Jan.json` | Table `orders_2026_jan_bronze` — no error |
| 4.3 | After a successful run, inspect `source_dir` | Files **moved** to `processed/<date>/`. Source directory is empty of ingested files |
| 4.4 | One bad file among good ones (`stop_on_error: false`) | Good files ingest; bad one reported `status=failed`; **the run continues** |
| 4.5 | Same, with `stop_on_error: true` | Run stops at the first failure |
| 4.6 | Empty directory | Run exits **SUCCESS** with "nothing to ingest" — not a failure |
| 4.7 | Mixed `.json` and `.jsonl` in one directory | Both ingest correctly; the `.jsonl` yields all its records |

**4.6 matters operationally:** an empty watched directory must not page
anyone.

### 4.8 Retry limit before quarantine

1. Place a file that always fails (e.g. invalid JSON throughout)
2. Run the job **three times** (default `max_ingestion_retries: 3`)

Expect: it stays in place for attempts 1 and 2 with a warning naming the
attempt; on attempt 3 it moves to `quarantine_files/` and the log says so.
It must **not** be retried forever.

### 4.9 Folder-as-table

Directory containing a subfolder with several files. Expect: the subfolder's
files union into **one** table named for the folder; archived files preserve
the folder name under `processed/<date>/<folder>/`.

---

## 5. Quality gate and quarantine

| ID | Set | Expect |
| --- | --- | --- |
| 5.1 | `required_columns: [order_id]`, `fail_on_quality_error: true`, ingest `orders_nulls.json` | Run **fails**. Audit row `status=failed`, `failure_stage=quality`, `quarantined_row_count=2` |
| 5.2 | Same but `fail_on_quality_error: false` | Run succeeds. 3 rows in bronze, 2 in quarantine |
| 5.3 | Inspect the quarantine table after 5.2 | Bad rows carry `_quarantine_reason` = `null:order_id`, plus `_quarantine_id` |
| 5.4 | `unique_columns: [order_id]`, ingest `orders_dupes.json` | Duplicates quarantined with reason `duplicate:order_id`. **Exactly one** of each duplicate group is kept |
| 5.5 | Both rules, a row violating both | `_quarantine_reason` shows both, joined with `|` |
| 5.6 | Count check after 5.2 | bronze rows + quarantined rows = source rows. **Nothing vanishes** |

**5.6 is the important one.** The split must be a partition of the input —
no row in both tables, no row in neither (#147).

### 5.7 Quarantine is idempotent

Run 5.2 twice with fresh copies of the same file. Expect: the quarantine
table does **not** double. Rows are keyed on content, so the same bad row
merges rather than inserting beside itself (#148). `_occurrence_count`
increments instead.

---

## 6. Streaming — Auto Loader ⚠️ **highest priority**

**This section has no automated coverage. Everything here is being executed
for the first time.**

Setup: `ingestion_mode: streaming`, `checkpoint_location` and
`schema_location` set to fresh paths, `trigger_mode: availableNow`.

| ID | Case | Expect |
| --- | --- | --- |
| 6.1 | Start with 2 files present, run once | Both ingest. Audit rows carry a **`stream_batch_id`** |
| 6.2 | Re-run with no new files | Nothing re-ingested. Checkpoint prevents reprocessing |
| 6.3 | Add a **new** file, re-run | Only the new file ingests |
| 6.4 | Audit rows across a multi-batch run | One row per micro-batch; **`run_id` is the same** across them, `stream_batch_id` differs |
| 6.5 | Schema registry after several batches | **One** registry row per table, not one per batch |
| 6.6 | With `table_comment` set, run several batches | The comment is applied; check `DESCRIBE HISTORY` — the table version should **not** increment once per batch |

**6.4–6.6 verify #156.** Before it, per-run metadata was rewritten every
micro-batch — 2,880 times a day on a 30-second trigger.

### 6.7 The JSON-lines guard ⚠️ **never executed against a real stream**

1. Configure a streaming source with `multiline: true`
2. Place a `.jsonl` file in the source directory
3. Run

**Expect:** the micro-batch **fails** with `JsonLinesTruncationError`,
naming the offending file, stating the checkpoint has not advanced, and
listing three ways to fix it.

Then:

4. Set `multiline: false`
5. Re-run **without** clearing the checkpoint

**Expect:** the same file is re-read **in full** — all its records land.

This is the whole design claim of #146: failing leaves the checkpoint
un-advanced so nothing is lost. **If step 5 does not recover the records,
that is a serious finding** and the guard's rationale is wrong.

### 6.8 Escape hatch

Set `multiline: false` and `reader_options: {multiLine: "true"}` with a
`.jsonl` file present. Expect: the guard does **not** fire — an explicit
override is treated as deliberate.

### 6.9 `processingTime` trigger

Run with `trigger_mode: processingTime`, `trigger_processing_time: "30 seconds"`,
drop files in while it runs. Expect: files picked up within roughly one
trigger interval; one audit row per micro-batch that did work.

---

## 7. Write modes

| ID | Mode | Steps | Expect |
| --- | --- | --- | --- |
| 7.1 | `append` | Ingest twice | Rows accumulate |
| 7.2 | `overwrite` | Ingest twice | Table holds only the second run's rows |
| 7.3 | `merge` | `merge_keys: [order_id]`, `required_columns: [order_id]`. Ingest, then ingest a file where 2 records share ids with changed values and 1 is new | 2 rows **updated**, 1 **inserted** — not 3 appended |
| 7.4 | Audit row after 7.3 | `rows_updated=2`, `rows_inserted=1`, `row_count=3`, `write_mode=merge` |
| 7.5 | Merge with a duplicate key in the source batch | Deduplicated automatically (default), one row per key wins deterministically |
| 7.6 | Merge with a NULL merge key reaching the writer | Raises `NullMergeKeyError` with an actionable message — **does not** insert a duplicate |

**7.4 verifies #149.** Before it, a merge that updated 500 rows and inserted
nothing reported `row_count: 500`, which reads as 500 new rows.

---

## 8. Audit trail and observability

| ID | Case | Expect |
| --- | --- | --- |
| 8.1 | After any successful run | Exactly one audit row per table per run |
| 8.2 | Force a read failure (point at a non-existent path) | Audit row **still written**, `status=failed`, `failure_stage=read` |
| 8.3 | Force a quality failure (5.1) | `failure_stage=quality`, `quarantined_row_count` populated |
| 8.4 | Column meanings | `row_count` = rows written to target. For append, equals `source_row_count`. For merge, see 7.4 |
| 8.5 | `enable_run_audit: false` | No audit table created at all |
| 8.6 | Two environments (dev and staging) | Each writes to **its own** schema's audit table. They do not share one |

**8.6 verifies #54.** The old default sent every environment's audit trail
to one shared table.

---

## 9. Schema registry and drift

| ID | Case | Expect |
| --- | --- | --- |
| 9.1 | First ingestion of a table | One registry row with a fingerprint |
| 9.2 | Re-ingest identical schema | Still **one** row; `last_updated_at` unchanged. Cheap path — no write |
| 9.3 | Ingest `orders_extra_col.json` (new field) | Registry row **updated**; audit row for that run has `schema_changed=true` |
| 9.4 | Column reordering only | **Not** flagged as drift — the fingerprint is order-independent |

---

## 10. Quarantine replay

| ID | Case | Expect |
| --- | --- | --- |
| 10.1 | After 5.2, fix the rule (`required_columns: []`) and run row replay | Previously-quarantined rows promoted to bronze; removed from quarantine |
| 10.2 | Bronze rows from 10.1 | `_batch_id` starts with `replay-`; `_source_file` is the **original** file, not regenerated |
| 10.3 | Run replay a second time | Nothing promoted. Idempotent |
| 10.4 | Replay with `batch_id=<a real _batch_id from bronze>` | Only that run's rows replay |
| 10.5 | Replay with `max_rows` below the pending count | Refuses **before writing anything**; message names `batch_id`/`since`. Quarantine untouched |
| 10.6 | File replay | Files move from `quarantine_files/` back to the source directory; retry counts cleared so they get a fresh set of attempts |

**10.4 verifies #148's second half.** It previously returned zero matches
with no error, which reads as "nothing to replay".

---

## 11. Resilience and failure handling

| ID | Case | Expect |
| --- | --- | --- |
| 11.1 | Config error (unknown `write_mode`) | Fails on the **first** attempt. No 30-second retry delay |
| 11.2 | Permission error (revoke write, run) | Fails fast; log says the failure is not transient and will not be retried |
| 11.3 | Check timing of 11.1/11.2 | Failure surfaces in seconds, not after ~30s of sleeping |
| 11.4 | Directory with many broken files | Total run time is not dominated by retry sleeps |

**11.1–11.4 verify #152.** Before it, a directory of 50 broken files spent
~25 minutes sleeping.

---

## 12. Catalog metadata

| ID | Case | Expect |
| --- | --- | --- |
| 12.1 | `table_comment` and `column_comments` set | Visible in Catalog Explorer / `DESCRIBE TABLE EXTENDED` |
| 12.2 | Re-run **unchanged** | `DESCRIBE HISTORY` shows **no new version** from comments |
| 12.3 | `column_comments` naming a column that does not exist | Warning; run still succeeds |

**12.2 is the subtle one.** Comment DDL creates a new table version on every
execution, so re-stamping unchanged comments would append junk history
indefinitely.

---

## 13. Deployment

| ID | Case | Expect |
| --- | --- | --- |
| 13.1 | `databricks bundle validate -t dev` | Passes |
| 13.2 | `databricks bundle deploy -t dev` | Succeeds; job appears with the environment suffix in its name |
| 13.3 | Run the deployed job | Real ingestion; audit and registry rows written |
| 13.4 | Job names across targets | Distinguishable — dev/staging/prod do not appear as three identical names |
| 13.5 | Concurrency | Second run while one is active **queues**, does not run concurrently |
| 13.6 | Job exit status | A run where every unit is `skipped` exits SUCCESS. A run with a genuine failure exits FAILED |

---

## Must pass before production

If time is short, this is the subset:

| Priority | Cases | Why |
| --- | --- | --- |
| **P0** | **6.1–6.9** (all of streaming) | No automated coverage exists. This is the real gap |
| **P0** | 5.6, 5.7 | Data conservation and quarantine idempotency |
| **P0** | 2.4, 4.7 | Silent data loss on JSON Lines |
| **P0** | 7.3, 7.4 | Merge semantics and the counts an ops dashboard will trust |
| **P1** | 1.1–1.11 | Cheap, fast, and catches config errors before compute |
| **P1** | 8.2, 8.6 | Failure visibility and environment isolation |
| **P1** | 10.1–10.5 | Recovery path — untested in production so far |
| **P2** | 3.x, 9.x, 12.x, 13.x | Important, lower risk of surprise |

---

## Raising findings

A finding is worth raising when **observed behaviour differs from the
Expect column**. Include:

1. The case ID
2. The exact config used
3. What happened, including the audit row and any log lines
4. Whether it is reproducible

Two things are **not** findings: the known gaps in the readiness table at
the top, and anything Silver-related — this package deliberately stops at
bronze (see `docs/bronze_silver_contract.md`).
