# Bronze ingestion architecture

Target-state design for `bronze_ingest` (`bronze_layer/`): today's shipped
bronze layer, the multi-format ingestion it's built to grow into, and the
AI-assisted metadata layer planned on top of it. Decisions here carry their
reasoning and the alternatives that were rejected, not just the outcome —
that's deliberate, and it's why this document stays long even after a pass
for readability.

**Companion docs:** `docs/overview.md` for a plain-language summary of the
same architecture aimed at non-engineers; `docs/roadmap.md` for what order
this gets built in and why; `docs/bronze_silver_contract.md` for exactly
what Bronze hands Silver.

## At a glance

```mermaid
flowchart LR
    subgraph Deterministic ingestion path
        S[Sources on a Volume] --> D[Discovery + reader dispatch]
        D --> Q[Quality gate]
        Q -->|good rows| B[(Bronze Delta table)]
        Q -->|bad rows| QT[(Quarantine table)]
        QT -. replay .-> Q
    end
    B --> AT[(_ingestion_audit)]
    B --> SR[(_schema_registry)]
    subgraph Async, decoupled — advisory only
        AT --> AI[AI metadata job]
        SR --> AI
        AI --> AM[(_ai_metadata)]
    end
```

Two lanes, deliberately isolated from each other:

| Lane | What it does | Character |
| --- | --- | --- |
| **Deterministic ingestion** | Discover files → read → quality gate → write Delta, with lineage and audit columns on every row | Synchronous, config-driven, no AI, no external calls |
| **AI-assisted metadata** | A separate scheduled job reads the audit trail and schema registry, drafts PII flags / drift summaries / descriptions | Asynchronous, advisory-only, human-reviewed before anything it writes affects the catalog |

**The one rule that governs the second lane:** it never sits in the write
path and never gates an ingestion decision. Acceptance, rejection, and
quarantine are decided exclusively by `quality.py`, deterministically. If a
future proposal would have an AI model decide whether a row is accepted,
that's a different, bigger decision this document does not make — see
`docs/business_requirements.md` BR-001 for a live instance of that question
being asked.

---

## Status of each piece

| Piece | Status |
| --- | --- |
| JSON bronze ingestion, quality gate, quarantine, retries | **Shipped** |
| Directory ingestion (folder-as-table, archival, retry-limit-before-quarantine) | **Shipped** |
| Run-level audit trail + schema registry | **Shipped** |
| Multi-format ingestion (CSV/XML/Parquet) | **Designed below, not built** |
| Schema registry as AI-layer input | **Shipped** (registry); AI layer **not built** |
| AI-assisted metadata layer | **Designed below, not built** — gated on schema registry, which is done, so this can start |
| Silver layer | **Not built** — see `docs/bronze_silver_contract.md` for what it will be handed |

## Multi-format ingestion

**Rule: `source_format` in `IngestionConfig` decides both what's discovered and how it's read** — one config value, two consistent effects:

```
source_format: json     ->  discovers .json, .jsonl   ->  json_reader
source_format: csv      ->  discovers .csv            ->  csv_reader
source_format: xml      ->  discovers .xml            ->  xml_reader
source_format: parquet  ->  discovers .parquet         ->  parquet_reader
```

- **Files of other formats are invisible**, same as `notes.txt` is invisible
  to JSON discovery today — not an error, the existing behavior generalized.
- **Mixed-format folders aren't supported.** Two configs pointing at the
  same path is clearer than implicit per-file routing.
- **Format is never inferred from the file extension**, even though that
  would need less config. Rejected because it would silently ingest a
  `.csv` that today sits harmlessly beside `.json` files — the same class of
  surprise as a stray fixture folder getting auto-ingested (recorded in
  `testing_directory_ingestion.md`'s history). Explicit configuration
  beats implicit discovery here.
- **`multiline` (JSON only) *is* inferred per file, and that's not a
  contradiction.** `source_format` controls which files get pulled in at
  all — getting it wrong means unexpected data, which is recoverable and
  noisy. `multiline` controls how an already-selected file is parsed —
  getting it wrong on a `.jsonl` file silently returns one row instead of
  thousands, with no error. Inference is fine where the failure mode is
  loud; it's rejected where the failure mode is silent data loss.
- **Everything downstream of the reader is already format-agnostic** — the
  quality gate, audit columns, retry logic, archival, and audit trail all
  operate on a DataFrame and need no changes for this to ship.

## Metadata: three tables, facts kept separate from opinion

| Table | Contains | Written by | Trust level |
| --- | --- | --- | --- |
| `_ingestion_audit` | One row per run: status, row counts, timings, errors | Pipeline, synchronously | Fact |
| `_schema_registry` | One row per table: current schema, fingerprint, last-changed | Pipeline, synchronously | Fact |
| `_ai_metadata` | Drafted interpretations, PII flags, descriptions | AI layer, asynchronously | Advisory |

This split makes "AI output is advisory, never authoritative" true by
construction: **nothing in the write path ever reads `_ai_metadata`.** The
registry answers "did the schema change, and to what?" — cheap and factual.
The AI layer answers "what does that change probably mean, and should
someone care?" — commentary built on top of the registry's output, never a
replacement for it.

## How the AI layer runs

**A standalone, scheduled Databricks job** — nothing else. It reads recent
`_ingestion_audit` / `_schema_registry` activity, drafts output, writes to
`_ai_metadata`. Ingestion jobs never call it and never wait on it.

Two mechanisms were considered and rejected:

- **A background thread at the end of the ingestion job** — not actually
  async, since the cluster stays warm until the AI call finishes. A cost
  review already found 96% of this project's Databricks spend is compute
  time; keeping a cluster warm for LLM calls fights that finding directly.
- **Event-driven triggers** — more infrastructure for a workload nobody
  needs within seconds of ingestion. Nothing here is latency-sensitive.

**Failure handling, consistent with the rest of this codebase's failure
story** (retry-with-backoff, quarantine fallbacks, an audit writer that
never raises): a failed or timed-out call logs and skips that table; the
job continues with the rest; there's no aggressive retry, since the next
scheduled run naturally picks up anything still showing as changed;
malformed AI output is discarded rather than written, because a bad row in
`_ai_metadata` is worse than no row.

**Cost is bounded by design** — zero AI cost per ingestion run, one
scheduled job amortizing cluster startup across every table it touches,
and only tables with genuinely new activity get reprocessed.

## Explicitly out of scope: `flatten_mode`

Removed from Bronze entirely. Flattening is a reshaping decision for
downstream consumers — a Silver concern (see
`docs/bronze_silver_contract.md`), not a Bronze one; Bronze preserves
source fidelity. The working `flattener.py` and its tests are archived at
`silver_layer/_archive/` for reuse once Silver is built, not deleted.

## What's built, in delivery order

1. **Bronze core** — config-driven ingestion, quality gate, quarantine, retries, Unity Catalog integration
2. **Directory ingestion resilience** — per-file failure isolation, automatic archival, retry-limit-before-quarantine, folder-as-table merging
3. **Enterprise-hardening phase 1** — run-level audit trail wired into every ingestion path, CI enforcement with branch protection

## What's left, in dependency order

**Enterprise hardening, remaining:**

| # | Item | Status |
| --- | --- | --- |
| 4 | Control-table driven dynamic config | Not started |
| 5 | Concurrency locking (#153) | Job-level done (`max_concurrent_runs: 1`); library-level (two direct callers racing on one `source_dir`) still open |
| 6 | Config validation and allowlist governance | Substance tracked in #154/#54, not implemented |
| 7 | Secrets via Databricks secret scopes (#115) | Not started, blocked on #112 provisioning |

**Target-state build, in the order each piece unblocks the next:**

1. **Schema registry** — cheapest, unblocks the most (AI drift summaries need schema history to exist first). **Done.**
2. **Multi-format ingestion** — independent of the AI layer, can run in parallel with it.
3. **AI metadata layer** — depends on the audit trail (done) and schema registry (done) as its inputs. Can start now.

**What Bronze owes Silver**, from `docs/bronze_silver_contract.md`, in dependency order:

1. Change Data Feed enabled on bronze and quarantine tables (#58), paired with a retention floor (#159) — VACUUM deletes CDF history, so shipping one without the other is a guarantee that isn't real.
2. `overwrite` rejected together with CDF at config load — under `overwrite`, CDF emits the whole table as deletes-plus-inserts every run, which is strictly worse than a full rescan.
3. A `layer` column on `AUDIT_SCHEMA` — decided now because deciding it after #62's dashboard exists would mean rebuilding the dashboard.

Silver's own build isn't scheduled here — see `docs/roadmap.md`. Its
gating prerequisite is "§5 of the Bronze→Silver contract is answered and
the buy-vs-build call is made," both of which are done
(`docs/buy_vs_build_2026-08.md`: build).

## Measured operational characteristics

Numbers, not impressions — `testing_directory_ingestion.md` owns the
benchmark; this section and the `_archive_files_parallel` docstring both
defer to it rather than carrying an independent figure.

- **Archival costs ~0.45s/file**, and is the dominant linear cost of
  folder ingestion. Irreducible on serverless today: `dbutils.fs.mv` calls
  serialize through the Spark Connect client.
- **100 files/folder ≈ 163s total**, ~45s of it archival.
- **Past roughly 100 files/folder, use Auto Loader** (`ingestion_mode:
  streaming`) instead of folder-as-table — it avoids per-file archival
  entirely. A new source's expected file volume should decide its
  ingestion mode before its format does.

## Open questions

- **CDF retention floor** — `docs/bronze_silver_contract.md` recommends 30
  days; it's the one number there that can't be derived from the code
  (needs to exceed the slowest CDF consumer's lag, and no consumer exists
  yet). Confirm before #58 ships.
- **Buy-vs-build — resolved**, not open. `docs/buy_vs_build_2026-08.md`
  verdicts every framework feature in the backlog against a real fixture,
  not a README: Silver's rule engine is **build** (DQX can't be
  constructed without an authenticated workspace, which would make the
  322-test local suite unable to cover it); Lakeflow Declarative Pipelines
  is **not adopted** (no equivalent for folder-as-table, per-file
  archival, cross-run retry-limit, or quarantine replay).
