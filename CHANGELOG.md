# Changelog

## 0.5.0 — the correctness wave

42 commits, promoted from `dev` in one release. Every known silent
data-loss and silent-corruption defect in the bronze layer is closed.

`main` had not moved since `0.4.0`, so production was running code from
before all of it.

---

## ⚠️ Read this before deploying

### 1. The audit table gains a column that means the same as an old one

`_ingestion_audit`'s `table` column is renamed to **`table_name`**, and six
columns are added (`source_row_count`, `rows_inserted`, `rows_updated`,
`rows_deleted`, `write_mode`, `stream_batch_id`).

The audit writer appends with `mergeSchema: true`, so **the write succeeds
and nothing fails.** That is the problem. Verified against a local Delta
table carrying the old schema:

| rows | `table` | `table_name` |
| --- | --- | --- |
| written before the upgrade | populated | `NULL` |
| written after the upgrade | `NULL` | populated |

Delta relaxes the old column's `NOT NULL` constraint rather than rejecting
the write, so the table ends up with two columns meaning the same thing,
each half-populated. **A query on either one returns a plausible,
complete-looking answer covering only half the runs.**

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

Leaving it in place is also fine — it just stays `NULL` for every new run.

### 2. Configs that worked may now fail at load

All of these previously ran and are now rejected **before a cluster
starts** (#154, #54):

- Identifiers outside `[A-Za-z_][A-Za-z0-9_]*` — `table: "orders-2024"` is
  now an error. `quarantine_table`, `table_properties` keys and
  `column_comments` keys are checked per dot-separated part, so
  `main.bronze.x` and `delta.enableChangeDataFeed` still pass
- `reader_options` keys outside the allowlist (`cloudFiles.*` is allowed
  wholesale). `allow_unsafe_reader_options: true` opts out
- `retry_attempts` below 1, negative `retry_delay_seconds`,
  `max_files_per_trigger` below 1
- `ingestion_mode: streaming` with `write_mode: overwrite`
- `merge` + `dedupe_before_merge` + `add_audit_columns: false` with no
  `dedupe_order_by`

### 3. A streaming source reading `.jsonl` now fails where it silently truncated

`multiLine=true` on a JSON-lines file returns only its first record. The
guard raises rather than warns, and that is the recoverable outcome:
Structured Streaming only commits a batch when the handler returns
normally, so raising leaves the checkpoint un-advanced and the files are
re-read once the config is fixed. Succeeding is what made the loss
permanent (#146).

**If a stream starts failing after this deploy, it was losing data before
it.** The error names the offending files and the ways out.

### 4. The audit/registry schema default changed

`audit_schema_name` and `registry_schema_name` now default to `None`,
meaning *"use `schema_name`"*, instead of the literal `"bronze"` (#54).
`databricks.yml` pins both per target, so deployed jobs are unaffected —
this only changes the library default. If you relied on the old one, set it
explicitly.

### 5. Quarantine rows written before this release will not deduplicate

`_quarantine_id` is now a SHA-256 of row content instead of `uuid()`, and
the quarantine write is a `MERGE` (#148). Pre-existing rows keep their UUID
ids, which can never match a content hash, so the same source row may
appear once under each. Clear them once you have confirmed the current data
has been re-quarantined:

```sql
DELETE FROM <table>_quarantine WHERE length(_quarantine_id) <> 64;
```

---

## Fixed — silent data loss and corruption

| Issue | What was wrong |
| --- | --- |
| **#146** | `.jsonl` files were read with `multiLine=true` and returned **one record**. Measured: 3 records in, 1 out, no error and nothing in `_corrupt_record`. Fixed on the batch path by deriving `multiLine` per file, and on the streaming path by a per-micro-batch guard |
| **#147** | The quality gate's good/bad split was not a partition of its input. `_duplicate_flag_column` broke ties with `monotonically_increasing_id()`, whose value depends on partition layout — and `good_df`/`bad_df` are two lazy plans evaluated independently, so a row could be written to bronze **and** quarantined, or dropped by both |
| **#148** | Quarantine was keyed on `uuid()`, stable within one query plan but not across evaluations, so a retried run appended a second copy of the same bad rows. Now a content hash, written with `MERGE` |
| **#149** | `row_count` meant something different for every write mode, and was produced by recounting the DataFrame rather than reading Delta's transaction log. Split into `row_count` / `source_row_count` / `rows_inserted` / `rows_updated` / `rows_deleted` |
| **#156** | Streaming re-ran the whole per-run metadata sequence on every micro-batch — 2,880 times a day on a 30-second trigger |

## Fixed — safety and reliability

| Issue | What was wrong |
| --- | --- |
| **#154** | Config values reached `spark.sql()` unescaped and unvalidated in five places. Identifier validation at config load, one shared quoting helper, the registry lookup rebuilt as a Column expression, `reader_options` allowlisted |
| **#54** | Numeric fields accepted nonsense — `retry_attempts: 0` made `with_retry` raise a `None` exception, hiding the real failure. Plus the audit-schema default above and four cross-field checks |
| **#152** | `with_retry` retried everything, including failures no retry could fix. Now discriminates, with `retry_max_total_seconds` bounding total sleep |
| **#155** | Replay collected every `_quarantine_id` to the driver and built one `IN (...)` clause. Now a distributed, bounded delete |

## Changed — structure

| Issue | What |
| --- | --- |
| **#150** | The orchestration body existed three times; now one `_execute`. `IngestionConfig.resolve()` owns config merging |
| **#151** | `directory_ingestion.py` split into orchestration + `fs/` + `naming` |
| **#183** | One failure → retry-count → quarantine policy instead of two drifted copies. The folder path's quarantine branch previously logged **nothing** |

## Added — engineering

| Issue | What |
| --- | --- |
| **#158** | CI quality job: ruff (format + lint), mypy, bandit, pip-audit, coverage reporting, Dependabot. `pyproject.toml` as the single tool-config home |
| **#157** | The notebook layer is tested — it is where both live production defects were, and it had zero coverage |
| **#74** | The suite runs on Windows, and the path bugs behind that are fixed |
| — | `docs/roadmap.md`: phase plan over the remaining open issues |

---

## Verification

Full suite green. Every fix above carries regression tests; several were
verified by running the new test against the *unfixed* code to confirm it
actually caught the defect.

Not covered by the local suite, and unchanged by this release: Auto Loader
itself, Unity Catalog tags, and `information_schema` views — all
Databricks-Runtime-only. The streaming guard's decision logic is fully
tested; whether `cloudFiles` honours the resolved `multiLine` option needs a
run on Databricks.
