# Changelog

## Unreleased — multi-format batch support and agent runtime parity

- Added batch CSV and Parquet discovery and readers while keeping the JSON defaults intact.
- Drafted a fail-closed XML ingestion path with required `xml_row_tag`, recursive prefix-preserving identifiers, and collision detection. XML remains blocked until the proposed #336/#337 decision records receive named Tier 2 sign-off.
- Added the format-neutral `ingest_to_bronze`; `ingest_json_to_bronze` remains as a compatibility alias.
- Added format widgets, bundle parameters, and CSV/XML workspace-validation notebooks. Multi-format streaming remains deferred under #323.
- Renamed the third-party review skill to `two-axis-code-review`, left the built-in `/code-review ultra` in place, and added pinned seven-role Codex agent generation alongside the Claude configuration.

Workspace validation, reviewer verdict, and promotion approval are still required before release.

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
