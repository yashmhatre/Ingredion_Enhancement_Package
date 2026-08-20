# Changelog

## 0.6.0 — multi-format bronze, advisory metadata, and table maintenance

172 non-merge commits promoted from `dev` in one release. `main` has not moved since `0.5.0` on 2026-08-02, so production is running the code from before all of it.

Three things arrive together: batch ingestion of formats other than JSON, an advisory AI metadata job that runs beside the pipeline rather than inside it, and the first lifecycle policy any table this package creates has ever had.

---

## ⚠️ Read this before deploying

### 1. Change Data Feed is now on by default

`enable_change_data_feed` defaults to `None`, which resolves to **on** for `append` and `merge`, and off for `overwrite`. It applies to bronze and quarantine tables alike. Existing tables pick this up on their next write.

CDF is not free. It writes a change file per commit, and `change_data_feed_retention_days` (default **30**) sets both `delta.logRetentionDuration` and `delta.deletedFileRetentionDuration` — the effective window is the shorter of the two. On a table with frequent small writes this is a real storage line item.

To keep the previous behaviour, set it off explicitly:

```yaml
enable_change_data_feed: false
```

Two configs that used to load now fail:

- `enable_change_data_feed: true` with `write_mode: overwrite`. Overwrite emits the whole table as deletes then inserts on every run, so the feed carries no incremental information. Only an explicit `true` fails; the default resolves to off for overwrite rather than raising.
- `change_data_feed_retention_days` below 1 while CDF is on. A retention of 0 renders as `interval 0 days` and discards change data immediately.

### 2. Streaming rejects every format except JSON

Batch multi-format does not extend to Auto Loader. A streaming config with `source_format` set to anything but `json` now raises:

```
streaming ingestion currently supports JSON only; batch multi-format support
does not enable CSV, Parquet, or XML Auto Loader reads.
```

Previously such a config was accepted and silently read as JSON. Multi-format streaming is deferred under #323.

### 3. XML is implemented and refused

`source_format: xml` raises at config load. The reader is complete and tested — fail-closed well-formedness checking, prefix-preserving namespace canonicalization, collision detection — but the two decision records governing it (#336 corrupt-record handling, #337 namespaced identifiers) are proposed and awaiting Tier 2 sign-off.

The refusal names both records. Neither ingestion notebook offers `xml` in its dropdown; both build the list from `formats.approved_formats()`. `notebooks/validate_xml_reader.py` lifts the gate for its own run, because it exists to produce the workspace evidence those decisions are waiting on, and a test asserts no other notebook does.

Lifting the gate after sign-off is one deletion: the `"xml"` entry in `formats._PENDING_SIGNOFF`.

### 4. Headerless CSV needs an explicit schema

`source_format: csv` with `csv_header: false` and no `schema_hint_ddl` is refused. Spark names columns `_c0`, `_c1`, … positionally, which leaves `required_columns`, `merge_keys`, `unique_columns`, `cluster_by`, `column_comments` and the data contract all pointing at names that do not exist.

Set `schema_hint_ddl` to name the columns, or `csv_header: true` if the file does have a header row.

### 5. Quoted booleans in YAML are now rejected

`csv_header` and `csv_infer_schema` must be real booleans. `csv_header: "false"` is the string `'false'` — truthy to Python, false to Spark — so it is refused rather than silently inverted:

```yaml
csv_header: false      # correct
csv_header: "false"    # now raises
```

### 6. `write_mode: merge` accepts a second strategy, and refuses both at once

Merge now takes either `merge_keys` (a business key) or `content_hash_columns` (a content-addressed key, #84). Exactly one. Setting both raises; setting neither still raises, with a message that now names both options.

`content_hash_columns` answers "are these the same bytes?", not "is this the same business entity?" — it gives idempotent re-ingestion of identical payloads, not upsert semantics. Columns bronze adds itself are refused inside it, because they either do not exist at hash time or differ on every run.

### 7. The audit table gains three columns

`schema_drift_json`, `tags_failed` and `tag_outcome_json` are appended to `AUDIT_SCHEMA`, all nullable, written with `mergeSchema: true`. Existing rows read as `NULL`; nothing needs backfilling.

This is the same append that `0.5.0` warned about, without the trap: `0.5.0` renamed `table` to `table_name`, leaving two columns meaning one thing and each half-populated. Nothing is renamed here. If you have not run `0.5.0`'s backfill yet, that migration step still applies.

Three of these columns were previously in the schema with no caller able to fill them, so they audited as `NULL` regardless of what the pipeline computed. That is fixed — the rows now record what actually happened.

### 8. Reader options are validated per format

The single JSON-era allowlist is now one allowlist per format. Existing JSON configs are unaffected: `source_format` defaults to `json` and the JSON allowlist is the pre-split set exactly, asserted against a hardcoded copy of the original so the test cannot pass by agreeing with a mistake. A JSON-only option on a CSV, Parquet or XML config is refused.

### 9. Two new jobs deploy, and neither runs

- `ai_metadata_${var.env_suffix}` — **no schedule**. Verify a manual run before enabling it.
- `bronze_maintenance_${var.env_suffix}` — weekly Sunday 03:00 UTC, **paused**. Unpausing needs Tier 2 approval, because it runs `VACUUM`.

Both deploy with `max_concurrent_runs: 1`. Deploying them costs nothing until someone turns them on.

---

## Added — multi-format batch ingestion

`source_format` decides both what discovery lists and which reader parses it, from one registry (#302, #303).

- **CSV** (#309, #310) and **Parquet** (#312, #313) are registered and reachable. **XML** (#315–#317) is implemented and gated, per §3 above.
- `readers.read_source` is the single dispatch both call sites use (#306); a registered format with no reader is a test failure, not a silent fall-through to JSON.
- Discovery is format-aware end to end (#304, #307), including through `reprocess_quarantined_files` (#305).
- `ingest_to_bronze` replaces `ingest_json_to_bronze`, which stays as an alias (#319).
- `csv_header` defaults to `true`, which is not Spark's default — Spark yields positional names (`_c0`, `_c1`) and those are unusable for `required_columns`, `merge_keys` or a data contract (#308).
- `csv_infer_schema` defaults to **`false`**, matching Spark. Inference read an 18-character zero-padded SAP `MATNR` as the integer `1000001` and the padding was gone — no error, no warning, no quarantine row, and a row count that reconciled perfectly (#371). CSV columns land as strings and the caller casts. Set `csv_infer_schema: true` to opt back in. See `docs/decisions/2026-08_csv_schema_inference_default.md`.
- Mixed-format folders are not supported. Two configs pointing at one path is clearer than per-file routing, and format is never inferred from the extension.

## Added — advisory AI metadata layer (#208)

A scheduled job, decoupled from ingestion, that reads `_ingestion_audit` and `_schema_registry` and drafts table descriptions, column descriptions, drift summaries and PII hints into `_ai_metadata`.

Nothing in the write path reads that table. Acceptance, rejection and quarantine remain `quality.py`'s alone, deterministically. `anthropic` is an `extras_require["ai"]` dependency, not a runtime one.

## Added — table lifecycle (#159)

The first maintenance this package has had.

- `maintenance.py` runs `OPTIMIZE` and `VACUUM` over the tables it manages, never below each table's own CDF retention floor.
- Audit rows are buffered across a directory run and written in one commit instead of one per unit. Measured: `_ingestion_audit` at 15 runs held 15 files of 4–7 KB, one per run, forever. A 50-file directory produced 50 single-row commits. Buffering never leaves the trail worse off — a failed batch write retries each row individually.
- Quarantine has an exit path. `max_replay_attempts` stops offering a row after N failed replays; exhausted rows are **skipped, never deleted**. The default is `None`, meaning no limit, which is the previous behaviour.

## Added — Unity Catalog tags (#64)

Tags are applied through the REST assignment API rather than DDL, because tag DDL raises `ParseException` on OSS Delta and could not be covered by the local suite at all.

`table_tags` and `column_tags` accept free-form keys only. Governed keys — the `class.*` PII taxonomy and anything policy-bearing — are refused and must go through the review path. Tag outcomes are recorded in the audit row, so a failure is visible rather than silent.

## Added — profiling and modeling groundwork

Not wired into ingestion; these build the evidence Silver needs.

- `profiling.py` (#299) measures null rates and cardinality into three tables at different grains.
- `key_detector.py` (#327) proposes primary-key candidates from that evidence, verified rather than inferred.
- `contract.py` (#264) is a declarative spec of what a source should look like, with validation.
- `metadata_store.py` (#324) is the single writer for all framework metadata tables, so no two writers can disagree about schema creation or replace semantics.

## Fixed

- **Streaming micro-batch writes were broken on every compute we have** (#248).
- **Schema drift never reached the audit trail from the streaming path**, and `describe_drift` read the wrong `schema_json` shape (#256). Drift now records *what* changed, not only that something did.
- **Three audit columns no caller could fill** (#276, #64, #256) — set by the pipeline, dropped before the write.
- **A failed notebook run was marked Succeeded** (#247). `dbutils.notebook.exit()` exits 0 whatever string it is handed; the notebooks raise instead.
- **Replay used a recomputed quarantine id rather than the original**, and the attempt counter recorded intent rather than outcome (#159).
- **Merge refusals ran after the table could already have been created** (#58).
- **An unconfigured quality gate passed silently** (#250). It now warns when `required_columns` and `unique_columns` are both empty. The shipped configs still set neither — populating them per source is #250's remaining half and needs a business answer.
- **`_ingestion_audit` mid-migration now fails closed** rather than writing into a half-migrated table (#231).

## Changed — engineering and governance

- A seven-role agent roster with pinned configuration, path-ownership enforcement, and a frontmatter gate, all checked in CI.
- The bundle's `validate` step is dropped; it only ever reported authentication (#244).
- Agent definitions are fetched from a pinned private repository rather than committed here.

## Verification

- Full suite green in CI. The Spark-dependent tests cannot run on Windows (no `winutils.exe`); `ruff format`, `ruff check`, `mypy`, `bandit` and the non-Spark tests were run locally.
- One end-to-end run against live Databricks on 2026-08-14: 21/21 checks, serverless Spark 4.1.0, catalog `ingredion_en.ingredion_dev`, wheel `bronze_ingest-0.5.0`. Recorded in `docs/e2e_validation_2026-08.md`.
- **That evidence covers dev only.** Staging has never run this code. CSV and Parquet have no live-workspace validation yet (#311). Deploying to staging and smoke-testing it is the next step, not a completed one.

## 0.5.0 — correctness wave

42 commits were promoted from `dev` in one release. Every known silent data-loss and silent-corruption defect in the bronze layer is closed.

`main` had not moved since `0.4.0`, so production was still running code from before the fix.

---

## ⚠️ Read this before deploying

### 1. The audit table gains a column that means the same thing as an old one

`_ingestion_audit`'s `table` column is renamed to **`table_name`**, and six columns are added: `source_row_count`, `rows_inserted`, `rows_updated`, `rows_deleted`, `write_mode`, and `stream_batch_id`.

The audit writer appends with `mergeSchema: true`, so the write succeeds and nothing fails. That is the problem. This was verified against a local Delta table that still had the old schema:

| rows | `table` | `table_name` |
| --- | --- | --- |
| before the upgrade | populated | `NULL` |
| after the upgrade | `NULL` | populated |

Delta relaxes the old column's `NOT NULL` constraint instead of rejecting the write, so the table ends up with two columns that mean the same thing and are each half-populated. A query against either column gives a plausible, complete-looking answer that covers only half the runs.

**Backfill after the first post-upgrade run**, per environment:

```sql
UPDATE <catalog>.<schema>._ingestion_audit
SET    table_name = table
WHERE  table_name IS NULL;
```

The old `table` column can then be dropped, which requires column mapping:

```sql
ALTER TABLE <catalog>.<schema>._ingestion_audit
  SET TBLPROPERTIES ('delta.columnMapping.mode' = 'name');
ALTER TABLE <catalog>.<schema>._ingestion_audit DROP COLUMN table;
```

Leaving it in place is also fine; it just stays `NULL` for every new run.

### 2. Configs that worked may now fail at load

The following cases used to run and are now rejected before a cluster starts (#154, #54):

- Identifiers outside `[A-Za-z_][A-Za-z0-9_]*` now fail. `table: "orders-2024"` is an error. `quarantine_table`, `table_properties` keys, and `column_comments` keys are checked per dot-separated part, so `main.bronze.x` and `delta.enableChangeDataFeed` still pass.
- `reader_options` keys outside the allowlist now fail; `cloudFiles.*` is allowed wholesale. `allow_unsafe_reader_options: true` opts out.
- `retry_attempts` below 1, negative `retry_delay_seconds`, and `max_files_per_trigger` below 1 now fail.
- `ingestion_mode: streaming` with `write_mode: overwrite` now fails.
- `merge` plus `dedupe_before_merge` plus `add_audit_columns: false` without `dedupe_order_by` now fails.

### 3. A streaming source reading `.jsonl` now fails where it silently truncated

`multiLine=true` on a JSON-lines file returns only the first record. The guard raises instead of warning, and that is the recoverable outcome. Structured Streaming only commits a batch after the handler returns normally, so raising leaves the checkpoint unadvanced and the files are re-read once the config is fixed. Succeeding is what made the loss permanent (#146).

**If a stream starts failing after this deploy, it was losing data before it.** The error names the offending files and the ways out.

### 4. The audit/registry schema default changed

`audit_schema_name` and `registry_schema_name` now default to `None`, which means *use `schema_name`*, instead of the literal `"bronze"` (#54). `databricks.yml` pins both per target, so deployed jobs are unaffected; this only changes the library default. If you depended on the old behavior, set it explicitly.

### 5. Quarantine rows written before this release will not deduplicate

`_quarantine_id` is now a SHA-256 of row content instead of `uuid()`, and the quarantine write is a `MERGE` (#148). Pre-existing rows keep their UUID ids, which can never match a content hash, so the same source row may appear once under each. Clear them once you have confirmed the current data has been re-quarantined:

```sql
DELETE FROM <table>_quarantine WHERE length(_quarantine_id) <> 64;
```

---

## Fixed — silent data loss and corruption

| Issue | What was wrong |
| --- | --- |
| **#146** | `.jsonl` files were read with `multiLine=true` and returned **one record**. Measured: 3 records in, 1 out, no error, and nothing in `_corrupt_record`. Fixed on the batch path by deriving `multiLine` per file, and on the streaming path by a per-micro-batch guard. |
| **#147** | The quality gate's good/bad split was not a partition of its input. `_duplicate_flag_column` broke ties with `monotonically_increasing_id()`, whose value depends on partition layout. `good_df` and `bad_df` are two lazy plans evaluated independently, so a row could be written to bronze **and** quarantined, or dropped by both. |
| **#148** | Quarantine was keyed on `uuid()`, which is stable within one query plan but not across evaluations. A retried run could append a second copy of the same bad rows. It now uses a content hash and a `MERGE`. |
| **#149** | `row_count` meant something different for every write mode and was produced by recounting the DataFrame instead of reading Delta's transaction log. It is now split into `row_count`, `source_row_count`, `rows_inserted`, `rows_updated`, and `rows_deleted`. |
| **#156** | Streaming re-ran the whole per-run metadata sequence on every micro-batch — 2,880 times a day on a 30-second trigger. |

## Fixed — safety and reliability

| Issue | What was wrong |
| --- | --- |
| **#154** | Config values reached `spark.sql()` unescaped and unvalidated in five places. Identifier validation is now done at config load, one shared quoting helper is used, the registry lookup is rebuilt as a Column expression, and `reader_options` are allowlisted. |
| **#54** | Numeric fields accepted nonsense. `retry_attempts: 0` made `with_retry` raise a `None` exception and hide the real failure. This also included the audit-schema default change and four cross-field checks above. |
| **#152** | `with_retry` retried everything, including failures no retry could fix. It now discriminates, and `retry_max_total_seconds` bounds total sleep. |
| **#155** | Replay collected every `_quarantine_id` to the driver and built one `IN (...)` clause. It now uses a distributed, bounded delete. |

## Changed — structure

| Issue | What |
| --- | --- |
| **#150** | The orchestration body existed three times; it is now a single `_execute`. `IngestionConfig.resolve()` owns config merging. |
| **#151** | `directory_ingestion.py` was split into orchestration, `fs/`, and `naming`. |
| **#183** | One failure now maps to retry count to quarantine policy instead of two drifted copies. The folder path's quarantine branch previously logged **nothing**. |

## Added — engineering

| Issue | What |
| --- | --- |
| **#158** | CI quality job: ruff (format + lint), mypy, bandit, pip-audit, coverage reporting, and Dependabot. `pyproject.toml` is the single tool-config home. |
| **#157** | The notebook layer is tested; it is where both live production defects were, and it had zero coverage before. |
| **#74** | The suite runs on Windows, and the path bugs behind that are fixed. |
| — | `docs/roadmap.md`: phase plan for the remaining open issues. |

---

## Verification

The full suite is green. Every fix above has regression tests, and several were verified by running the new test against the *unfixed* code to confirm it genuinely caught the defect.

Not covered by the local suite and unchanged by this release: Auto Loader itself, Unity Catalog tags, and `information_schema` views — all Databricks-runtime-only surfaces. The streaming guard's decision logic is fully tested; whether `cloudFiles` honors the resolved `multiLine` option still needs a Databricks run.
