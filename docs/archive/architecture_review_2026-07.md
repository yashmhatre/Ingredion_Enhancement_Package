> **ARCHIVED — point-in-time record, not maintained.** Moved from
> `docs/architecture_review_2026-07.md` on 2026-08-06 as part of a
> documentation refresh; content is unchanged. This review's findings
> became issues #145–#164; those issues (and `docs/roadmap.md` for
> current sequencing), not this document, track their state today. See
> `docs/README.md` for what's current.

# Solution Architecture Review — `bronze_ingest`

**Branch reviewed:** `dev` @ `ceeda69`
**Date:** 2026-07-29
**Scope:** whole repository — package, notebooks, bundle, CI, docs, open backlog
**Method:** full read of all 13 package modules (~2,000 lines), 12 test files (142 tests),
4 notebooks, the bundle and CI definitions, all 5 docs, and all 13 open issues.
Findings marked *verified* were reproduced by executing code; the rest are read-level.

> This document is an assessment and a proposal. **No package code was changed.**
> Findings are filed as GitHub issues #145–#164; existing issues were re-triaged.

---

## 1. Verdict

**This is a good codebase.** Judged against what most internal data-platform repositories
look like at this stage, it is well above the line, and the gap is mostly in one direction:
the *reasoning* is unusually well preserved. Decisions carry their rationale, rejected
alternatives are recorded, benchmarks are real numbers rather than impressions, and several
comments document empirically-verified platform behaviour that is not in any vendor doc
(comment DDL creating a new Delta version on unchanged re-application; `.clusterBy()` not
mapping onto Delta's V2 catalog write path; `w.dbutils.fs.ls()` discarding `is_dir`).

That is the property hardest to retrofit, and it is already there.

**The weaknesses are structural rather than local**, and they cluster into four themes:

| # | Theme | Evidence |
|---|---|---|
| 1 | **The tested boundary stops short of the deployed boundary** | 142 tests on the library, **0** on the notebooks that production actually runs. Both known live defects are in that gap |
| 2 | **One orchestration sequence, written three times** | `run()`, `run_on_dataframe()` and the streaming micro-batch handler share ~40 duplicated lines; four open issues each want to modify all three |
| 3 | **Laziness is treated as free** | The quality gate derives two complementary DataFrames from one uncached lazy plan, using a non-deterministic tie-break. Row counts re-scan the source rather than reading Delta's own metrics |
| 4 | **The framework's failure handling is better than the platform's** | Retry-with-backoff, quarantine fallback chains and `failure_stage` tagging inside the code; no timeout, no retry policy, no concurrency limit on the job that runs it |

None of these are visible from any single file. All four are visible from the whole.

---

## 2. What the system is

```
              ┌───────────── config (YAML/JSON on a UC Volume, or kwargs) ───────────────┐
              │                                                                            │
   source ──► json_reader / streaming_reader ──► quality gate ──► audit cols ──► bronze_writer ──► Delta
   (Volume)          (PERMISSIVE, retry)         (null/unique)     (lineage)     (append/overwrite/merge)
                                                       │                                │
                                                       └──► quarantine table            ├──► audit.py      (run facts)
                                                                   │                    ├──► schema_registry (schema state)
                                                                   └──► replay.py ───────┘    └──► catalog_metadata (COMMENTs)
```

Two entry shapes: **single-table** (`BronzeIngestion.run()`) and **directory ingestion**
(`ingest_directory_to_bronze`, which discovers files/folders, derives table names, isolates
failures per unit, archives on success, and enforces a retry limit before quarantining a file).

**13 modules, ~2,000 lines.** Deliberately thin dependencies: `pyyaml` only at runtime;
`pyspark`, `delta-spark` and `databricks-sdk` all treated as runtime-provided.

### What is genuinely well-built

- **`databricks_fs.py`** — the best module in the package. It draws a distinction most code in this space gets wrong: `None` means *"Databricks unavailable, use local"*, an exception means *"Databricks available and the operation failed"*. The previous code conflated them, so a broken workspace call silently wrote to the driver's local disk. It also documents *why* listing bypasses `dbutils` (`w.dbutils.fs.ls()` discards `is_dir`, and folder-as-table would have silently found zero subfolders).
- **Merge-path defences** — `#46` (atomic create-if-not-exists instead of check-then-act), `#47` (NULL merge keys enforced at config load *and* re-checked on the DataFrame), `#48` (intra-batch duplicate collapse). Three distinct silent-duplication failure modes, each closed with a comment explaining the SQL semantics behind it.
- **`catalog_metadata.py`'s diff-and-apply** — driven by a measured observation, not a guess.
- **The bundle** — one root bundle, per-target service principals set in-file rather than by `--var` so a prod deploy cannot inherit the staging identity, environment suffixes on job names, `notification_email` deliberately given no default so a deploy without an owner fails.
- **The CI change-detection** — refuses GitHub's native `paths:` filter because a path-filtered workflow never reports, which deadlocks a required status check. Correct, and the reasoning is written down.

---

## 3. Findings

Ordered by severity. Each links to its issue.

### 3.1 Live production defects

| | Finding | Issue |
|---|---|---|
| **P0** | **`per_file_config` is silently discarded.** The notebook passes it; `ingest_directory_to_bronze` has no such parameter; `**config_overrides` absorbs it; `IngestionConfig.from_dict` filters it out without a word. The **deployed job config declares a `required_columns` rule that therefore never runs.** *Verified by execution.* | [#145](https://github.com/yashmhatre/Ingredion_Enhancement_Package/issues/145) |
| **P0** | **`.jsonl` files are discovered but read with `multiline=true`.** Discovery accepts both extensions; the reader has one global flag defaulting to `True`; the deployed job pins it on. Under `PERMISSIVE` this is not an error — it is one row where there were thousands. Green run, plausible audit row, wrong data. | [#146](https://github.com/yashmhatre/Ingredion_Enhancement_Package/issues/146) |
| **P1** | **Summary display crashes on an empty run** — `spark.createDataFrame(pd.DataFrame([]))`. Fires on the most common scheduled-job outcome: nothing new to ingest. | [#144](https://github.com/yashmhatre/Ingredion_Enhancement_Package/issues/144) |

**The common factor is the point.** All three live in `bronze_layer/notebooks/`, which
`ci.yml` excludes from both `WATCHED` patterns, with the comment *"no test reads them"* —
accurate, and the reason these survived. The library has 142 tests; the four files production
actually executes have none. → [#157](https://github.com/yashmhatre/Ingredion_Enhancement_Package/issues/157)

### 3.2 Correctness

| | Finding | Issue |
|---|---|---|
| **P1** | **The good/bad split is non-deterministic.** `good_df` and `bad_df` are two filters over one *uncached lazy* plan (`.cache()` is unavailable on serverless — a documented constraint). With `unique_columns` set and `dedupe_order_by` unset, the tie-break is `monotonically_increasing_id()`, which is not stable across recomputations. A row can be written to **both** bronze and quarantine, or to **neither**. Counts still add up in each pass, so nothing signals it. | [#147](https://github.com/yashmhatre/Ingredion_Enhancement_Package/issues/147) |
| **P1** | **Quarantine is written before bronze, and is not idempotent.** A write failure leaves quarantine rows committed; the retry appends them again with fresh `_quarantine_id`s. `reprocess_quarantine()` later promotes each copy — so a transient write error becomes duplicated bronze rows, several steps later, looking like a replay bug. The bronze path has txn-based idempotency (`#63`); quarantine has none. | [#148](https://github.com/yashmhatre/Ingredion_Enhancement_Package/issues/148) |
| **P1** | **`_batch_id` differs between bronze and quarantine rows of the same run.** `add_audit_columns()` is called twice and each call independently evaluates `config.batch_id or datetime.now(...)`. Breaks `reprocess_quarantine(batch_id=...)`, whose entire purpose is "replay run X". Masked in the deployed job, which pins `batch_id` — live for every library caller. | [#148](https://github.com/yashmhatre/Ingredion_Enhancement_Package/issues/148) |
| **P2** | **`row_count` means something different per write mode.** Under `merge` it is the source batch size *pre-dedupe*, not rows written, and does not separate inserts from updates. #61 and #62 both build on this column. | [#149](https://github.com/yashmhatre/Ingredion_Enhancement_Package/issues/149) |
| **P2** | **Replay collects every `_quarantine_id` to the driver** and builds one `IN (...)` clause — ~39MB of SQL text at 1M rows. Fails *after* the bronze write has succeeded, and each retry duplicates more. Replay is exactly the operation that runs at scale ("we fixed the feed, replay everything"). | [#155](https://github.com/yashmhatre/Ingredion_Enhancement_Package/issues/155) |

### 3.3 Reliability and concurrency

| | Finding | Issue |
|---|---|---|
| **P1** | **No concurrency control anywhere.** `max_concurrent_runs: 1` is commented out. The retry-state file is a lock-free read-modify-write of a whole JSON map, performed *once per file*. Two overlapping runs lose each other's retry counts (so permanently-failing files retry forever) and can both ingest the same file (duplicate rows — `#63`'s idempotency protects a *retried* run, not a *concurrent* one). `architecture.md` lists this as phase 5; nothing tracked it. | [#153](https://github.com/yashmhatre/Ingredion_Enhancement_Package/issues/153) |
| **P2** | **`with_retry` retries everything**, including `NullMergeKeyError`, `DuplicateMergeKeyError`, `DataQualityError`, config `ValueError`s, `AnalysisException` and permission denials. Each permanent failure sleeps 10s + 20s before surfacing. **A directory of 50 broken files spends ~25 minutes sleeping** — against this repo's own finding that 96% of Databricks spend is compute. Logs say "Retrying…" for conditions that cannot recover. | [#152](https://github.com/yashmhatre/Ingredion_Enhancement_Package/issues/152) |
| **P2** | **The job has no timeout, no retry policy, no tags, no health rules, no schedule.** The framework's internal failure handling is thorough; the job wrapper around it has almost none — and the wrapper is what stands between a stuck run and the bill. | [#164](https://github.com/yashmhatre/Ingredion_Enhancement_Package/issues/164) |
| **P2** | **Streaming runs per-run work per micro-batch.** `record_schema` and `apply_catalog_metadata` execute on every micro-batch (2,880/day at a 30s trigger), and every micro-batch writes an audit row carrying the **same** `run_id`. | [#156](https://github.com/yashmhatre/Ingredion_Enhancement_Package/issues/156) |

### 3.4 Security and governance

| | Finding | Issue |
|---|---|---|
| **P2** | **Config values are interpolated into SQL unescaped and unvalidated** in five places — the registry filter, `SET TBLPROPERTIES`, the replay `IN` list, `ALTER COLUMN` identifiers, and every `CREATE SCHEMA`. Configs load from a UC Volume, so influence requires only `WRITE VOLUME`. The realistic case is not an attacker: it is a legitimate name containing an apostrophe producing an opaque failure 40 minutes into a run. `reader_options` is an unvalidated passthrough into the Spark reader. | [#154](https://github.com/yashmhatre/Ingredion_Enhancement_Package/issues/154) |
| **P2** | **`run_ingestion.py:67` logs the full config**, `reader_options` included, into driver logs readable by anyone with `CAN_VIEW`. Harmless today; a live leak the moment #115's secret support ships. | [#115](https://github.com/yashmhatre/Ingredion_Enhancement_Package/issues/115) |
| **P2** | **Source-file isolation is a naming convention.** All three environments read subpaths of one Volume; UC grants `READ VOLUME` at *volume* granularity. **The staging principal can read `PROD/Raw/`.** Tables, audit and registry are properly isolated by schema; source files are not. Fix costs nothing — volumes are metadata. | [#160](https://github.com/yashmhatre/Ingredion_Enhancement_Package/issues/160) |
| **P3** | The audit/registry schema defaults (`"bronze"`) are a live footgun at the library layer — under one shared catalog they merge all three environments' trails into one table, and `_write_audit_row` calls `CREATE SCHEMA IF NOT EXISTS`, so it works silently. Fixed at the bundle layer only. | [#54](https://github.com/yashmhatre/Ingredion_Enhancement_Package/issues/54) |

### 3.5 Structure

| | Finding | Issue |
|---|---|---|
| **P2** | **The orchestration sequence exists three times** in `pipeline.py` — ~40 near-identical lines, varying only in where the DataFrame comes from and which writer is used. Four open defects each need to modify all three copies. | [#150](https://github.com/yashmhatre/Ingredion_Enhancement_Package/issues/150) |
| **P2** | **`directory_ingestion.py` is four modules in one** (668 lines: naming, discovery, archival, retry-state, orchestration). `replay.py` imports **three underscore-prefixed functions** across that boundary, so any refactor silently breaks it. The failure/retry/archive policy is implemented twice, once per code path. | [#151](https://github.com/yashmhatre/Ingredion_Enhancement_Package/issues/151) |
| **P3** | **Test coverage tracks ease, not risk.** `pipeline.py` — 243 lines holding every orchestration path — has **2** tests. `retry.py` has **0**. `directory_ingestion.py` has 30. No coverage number is produced, so this is invisible without counting by hand. | [#158](https://github.com/yashmhatre/Ingredion_Enhancement_Package/issues/158) |
| **P3** | **CI has no lint, no type check, no security scan, no dependency audit, no coverage.** A type checker on the notebooks catches #145's entire class in seconds; `bandit` catches #154's. | [#158](https://github.com/yashmhatre/Ingredion_Enhancement_Package/issues/158) |

### 3.6 Lifecycle and layering

| | Finding | Issue |
|---|---|---|
| **P2** | **No lifecycle policy for any table the package creates.** `_ingestion_audit` writes one row as its own Delta commit — **~18,000 tiny files/year** for a daily 50-unit batch run, **~1M/year** for one 30-second stream. It is the backing store for #61 and #62. Quarantine grows monotonically with no exit for rows that will never pass. No OPTIMIZE, no VACUUM, no retention — and #58 (CDF) makes retention load-bearing, because VACUUM deletes change-feed history. | [#159](https://github.com/yashmhatre/Ingredion_Enhancement_Package/issues/159) |
| **P3** | **The medallion is one layer deep, and "that belongs in Silver" is deferring to something that does not exist.** `#76` archived the flattener there, `#109` moved five of seven quality rules there, `#58` enables CDF *for* it. Each decision is right; together they mean the contract is being defined by a series of independent "not here" calls rather than by a design. `silver_layer/` is a README plus an archived module; the bundle `include:` is commented out because it would match zero files. | [#162](https://github.com/yashmhatre/Ingredion_Enhancement_Package/issues/162) |
| **P3** | **Docs have drifted in four places**, one materially: `bronze_layer/README.md` § "What an administrator must provision" still describes the **abandoned three-catalog model**, so an administrator following it would provision three catalogs and grant at the wrong level. `architecture.md` and `directory_ingestion.py` also make **opposite claims** about whether archival parallelizes on serverless, each citing a benchmark. | [#161](https://github.com/yashmhatre/Ingredion_Enhancement_Package/issues/161) |

---

## 4. Proposed refactor

No package code was changed. This is the plan, in dependency order.

### Phase 0 — stop the bleeding (days)

Fix what is broken in production, and close the gap that let it through.

1. **#145** `per_file_config` — implement it, make `from_dict` strict, validate overrides before the loop
2. **#146** `.jsonl` — infer `multiline` from the extension, override always honoured
3. **#144** empty summary — explicit schema, drop the undeclared `pandas` import
4. **#157** notebook tests — a stubbed `dbutils`, a widget↔signature contract test, a widget↔bundle contract test; add `notebooks/` and `resources/` to CI's watched paths

> The contract tests are the point. Fixing #145 without them means the next notebook defect
> is equally invisible. A five-line `inspect.signature` assertion closes the entire class.

### Phase 1 — safety net before surgery (days)

5. **#158** formatter baseline (alone, so the diff is reviewable), then ruff → mypy → bandit → pip-audit → coverage; add `pyproject.toml`
6. **#74** split the suite by Spark dependency so `pytest -m "not spark"` gives a sub-second inner loop

> This is what makes Phase 2 safe. Refactoring most of a codebase without a fast local
> cycle and a type checker is materially riskier and slower.

### Phase 2 — structure (1–2 weeks)

7. **#150** one `_execute()` in `pipeline.py` — pure refactor, no behaviour change
   *Watch:* the reader must stay lazily invoked **inside** the audited block, or a read failure stops producing an audit row. This is the one place a naive extraction silently loses behaviour.
8. **#151** split `directory_ingestion.py` into `fs/{discovery,archival,retry_state}.py` + `naming.py`; `RetryState` as a class; unify the two failure blocks; fix `replay.py`'s private imports

> Order matters. Every Phase 3 fix touches code these two move. Doing them first turns
> four three-place changes into four one-place changes.

### Phase 3 — correctness (1–2 weeks)

9. **#147** materialize the quality tag once; make the tie-break deterministic
10. **#148** resolve `batch_id` once per run; make the quarantine write idempotent
11. **#149** row counts from Delta `operationMetrics`; extend `AUDIT_SCHEMA` (`rows_inserted`, `rows_updated`, `source_row_count`, `write_mode`, `stream_batch_id`); rename `table` → `table_name`
12. **#155** replace the driver `collect()` + `IN` list with a distributed MERGE
13. **#152** classify retryable vs. permanent; add jitter and a total-time cap
14. **#156** hoist per-run work out of the micro-batch path

> **#149 and #156 are one schema change if done together.** They are two if not, and the
> second one lands under a live dashboard.

### Phase 4 — operations (1 week)

15. **#153** `max_concurrent_runs: 1`, a TTL lease on `source_dir`, single-flush retry state
16. **#164** timeouts, retry policy, tags, health rules, schedule decision
17. **#159** measure audit-table file growth **first**, then decide compaction and retention; ship a maintenance job

### Phase 5 — direction (ongoing)

18. **#160** three Volumes — the isolation gap costs nothing to close and is permanent to leave
19. **#162** write the Bronze→Silver contract; it unblocks #109 with a checkable prerequisite
20. **#163** evaluate DQX and Lakeflow against a real fixture before building six more framework features
21. **#161** reconcile the docs; add a CI check for facts asserted in more than one place

---

## 5. Sequencing constraints

Dependencies worth respecting, gathered in one place:

```
#150 ──► #147, #148, #149, #156     (three copies become one before four fixes land)
#151 ──► multi-format work           (don't generalize discovery inside a 668-line module)
#153 ──► #164 max_retries            (task retries before concurrency control makes races likelier)
#149 ──► #61, #62                    (don't build alerting on a column whose meaning varies)
#149 + #156 ─► one AUDIT_SCHEMA change, not two
#159 ──► #58                         (CDF without retention is a guarantee that isn't one)
#162 ──► #109                        (a checkable prerequisite instead of a vague one)
#158 ──► #113                        (CD should gate on the security scan, not just tests)
#112 ──► #113, #115                  (federation and secret scopes need the SPs and grants)
```

---

## 6. Reference implementations worth reading

Chosen for *transferable design*, not popularity. Star counts as of 2026-07-29.

| Repo | Why it is relevant here |
|---|---|
| [`databrickslabs/dqx`](https://github.com/databrickslabs/dqx) — 439★ | Databricks Labs quality framework, actively developed. Declarative YAML rules, warn/error severity, **quarantine DataFrame split**, per-check result columns, profiling that generates rules from data. Covers much of **#109**, including the per-rule result surface that issue identifies as the hard part. Evaluate before building a rule compiler. |
| [`adidas/lakehouse-engine`](https://github.com/adidas/lakehouse-engine) — 293★ | **The closest public analogue to this project**: a config-driven Spark framework for lakehouse data products, from a large enterprise, with quality integration and a documented algorithm/ACON model. The single most useful reference for *what this codebase looks like at 10× scale* — read its module boundaries against **#150/#151** before doing those refactors. |
| [`databricks/bundle-examples`](https://github.com/databricks/bundle-examples) — 346★ | Official DAB patterns. Check `databricks.yml` and #113's CD design against it, particularly multi-target layouts and artifact handling. |
| [`databrickslabs/discoverx`](https://github.com/databrickslabs/discoverx) — 143★ | Multi-table scanning and **PII/semantic classification**, with tagging. A likely better foundation for the classification half of `architecture.md`'s AI metadata layer than building it, and directly relevant to **#64**. |
| [`databrickslabs/ucx`](https://github.com/databrickslabs/ucx) — 308★ | Not applicable in purpose (UC migration), highly applicable in **shape**: how a Databricks Labs project structures grants, assessment, installation and workspace-dependent testing. Pattern source for **#112/#160**. |
| [`databricks/delta-live-tables-notebooks`](https://github.com/databricks/delta-live-tables-notebooks) — 408★ | Reference expectation/quarantine patterns. Useful as the "what you get for free if you adopt the platform" baseline in **#163**'s buy-vs-build evaluation. |

---

## 7. Production-grade patterns not yet present

Things a mature deployment of this shape has, mapped to where they would land here.

| Pattern | Status | Where |
|---|---|---|
| **Idempotent sinks on every write path** | Bronze ✅ (`#63`), quarantine ❌, audit ❌ | #148 |
| **Deterministic partitioning of a dataset into disjoint sets** | ❌ — the good/bad split is not | #147 |
| **Metrics read from the engine, not recomputed** | ❌ — counts re-scan the source | #149 |
| **Retryable/permanent error classification** | ❌ — everything is retried | #152 |
| **Single-writer guarantee for a mutable shared resource** | ❌ — no lease, no `max_concurrent_runs` | #153 |
| **Job-level SLOs: timeout, retry, health, duration alerting** | ❌ | #164 |
| **Table lifecycle: compaction, retention, small-file control** | ❌ | #159 |
| **Contract tests across a deployment boundary** (widget ↔ signature ↔ bundle) | ❌ — the cause of #145 | #157 |
| **Static analysis gates in CI** (lint, types, SAST, dependency audit) | ❌ | #158 |
| **Least-privilege verified rather than assumed** | Partial — schemas ✅, volumes ❌ | #160, #112 |
| **Auditable deploy: short-lived federated credentials, approval gate** | ❌ — deploys are manual | #113 |
| **Secrets referenced, never inlined; redaction tested by absence** | ❌ | #115 |
| **A published contract between layers** | ❌ | #162 |

### Three case-shaped notes

**The retry-limit-before-quarantine mechanism is the right pattern, built on the wrong substrate.**
Counting failures *across runs* so a permanently-bad file eventually stops consuming
compute is exactly right, and rare to see implemented. Persisting that count in an
unlocked JSON blob rewritten once per file is the part that does not survive contact with a
second concurrent run (#153). The production equivalent is a Delta table keyed by file path,
updated with a MERGE — same semantics, atomic, queryable, and it appears on the ops
dashboard for free.

**Quarantine is a queue, and every queue needs a drain and a dead-letter.**
`#60` built the drain (replay) — genuinely more than most implementations get. What is
missing is the dead-letter: rows that will never pass have no exit, so they accumulate
forever and make every future replay slower and more expensive (#155, #159). "Quarantined
more than 90 days, never successfully replayed" is a real state and deserves a real
destination.

**The 96%-compute-cost finding deserves to be a design constraint, not an anecdote.**
It already shaped three good decisions — the AI layer as a scheduled job rather than a
background thread, the push for local testing, the archival benchmark. Applied consistently
it also implies: fail at config load rather than mid-run (#54, #154), never sleep on a
non-retryable error (#152), never re-scan to compute a number the engine already has (#149),
and bound every job with a timeout (#164). That is four more findings falling out of a
principle the repo already holds.

---

## 8. Issue ledger

**Filed:** [#145](https://github.com/yashmhatre/Ingredion_Enhancement_Package/issues/145)–[#164](https://github.com/yashmhatre/Ingredion_Enhancement_Package/issues/164) — 20 new issues.

**Rewritten** (materially stale against `dev`):

| Issue | Why |
|---|---|
| [#112](https://github.com/yashmhatre/Ingredion_Enhancement_Package/issues/112) | Phase A complete; environment model changed from 3 catalogs to 1 catalog / 3 schemas, invalidating most of Phase B |
| [#54](https://github.com/yashmhatre/Ingredion_Enhancement_Package/issues/54) | 2 of 4 bullets obsolete — `merge_keys ⊆ required_columns` shipped; `flatten_separator` no longer exists |
| [#64](https://github.com/yashmhatre/Ingredion_Enhancement_Package/issues/64) | Half shipped as `catalog_metadata.py` (COMMENTs); tags deliberately deferred with a stated reason |
| [#58](https://github.com/yashmhatre/Ingredion_Enhancement_Package/issues/58) | The `table_properties` mechanism it depended on shipped in `#57`; now blocked on retention instead |

**Re-triaged with findings:** #144, #109, #113, #115, #74, #65, #62, #61, #84.

---

## 9. If only three things get done

1. **[#157](https://github.com/yashmhatre/Ingredion_Enhancement_Package/issues/157) — test the notebooks.** Two live production defects are there, both trivially catchable, neither reachable by any amount of library testing. A stubbed `dbutils` and two contract tests close the whole class.
2. **[#150](https://github.com/yashmhatre/Ingredion_Enhancement_Package/issues/150) — collapse the three orchestration copies.** Pure refactor, no behaviour change, and it converts four upcoming three-place fixes into one-place fixes. It pays for itself on the first of them.
3. **[#147](https://github.com/yashmhatre/Ingredion_Enhancement_Package/issues/147) — make the quality split deterministic.** It is the only finding here that can silently produce wrong data with no signal anywhere — no failed run, no audit anomaly, no count mismatch.

Everything else is real and can wait a sprint. These three cannot.
