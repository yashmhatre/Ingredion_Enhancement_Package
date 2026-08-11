# bronze_ingest — Competitive Differentiation Roadmap
*Small, single-session tasks. Each = one GitHub issue → one PR → one diagram → one release-notes post.*

## How to use this
Each task below is scoped to fit in **one implementation session** (one file, one focused change) — matching the existing loop: draft issue → paste → implement → diagram → post to #ingredion-release-notes. Do NOT batch multiple tasks in one session. Tackle in order within each stage; stages can interleave with the CI backlog.

---

## Stage 0 — Fix what's already broken (do first, before new features)
| # | Task | Why first |
|---|---|---|
| 0.1 | Fix `dbutils.notebook.exit("FAILED: ...")` silently marking jobs Succeeded | Known critical bug — alerting is currently inert |
| 0.2 | Test Auto Loader against a real stream | Never validated — foundation for Stage 1 CDC work |
| 0.3 | Backfill audit table column rename (v0.5.0) | Blocks clean schema going forward |
| 0.4 | Populate `required_columns` in quality gate (currently empty) | Quality gate is a no-op right now |

## Stage 1 — Close table-stakes gaps
| # | Task | Scope (1 session each) |
|---|---|---|
| 1.1 | Evaluate DQX vs current `required_columns` gate | Spike only: install, run against one sample table, write findings doc — no migration yet |
| 1.2 | Migrate one table's quality checks to DQX | Single table, prove the pattern |
| 1.3 | Add `MERGE`-based incremental upsert for one source | Single source, batch only, no SCD2 yet |
| 1.4 | Add SCD Type 2 support to the merge logic from 1.3 | Extends 1.3, don't redo it |
| 1.5 | Wire Auto Loader `schemaEvolutionMode` + surface `_rescued_data` into audit trail | Detect drift, don't just rescue silently |
| 1.6 | Minimal DABs bundle (`databricks.yml`) for one job | One job only, prove deploy path via existing CI |

## Stage 2 — Build the differentiators
| # | Task | Scope |
|---|---|---|
| 2.1 | Cost query: join `system.billing.usage` to one pipeline's runs | Read-only query + notebook, no automation yet |
| 2.2 | Turn 2.1 into a per-run cost column in the audit table | Extends existing `audited_run()` context manager |
| 2.3 | Cost anomaly flag (>2× 7-day rolling avg) → log only | No alerting yet, just flag in audit table |
| 2.4 | Freshness SLA check for one table (alert if stale > N hrs) | Single table, single Slack webhook |
| 2.5 | Row-count anomaly detection (±2σ) for one table | Extends 2.4's alerting plumbing |
| 2.6 | Generalize 2.3–2.5 into a reusable alerting module | Only after all three patterns proven individually |
| 2.7 | Data contract schema: define YAML/JSON contract for one source | Spec + validation function, single source |
| 2.8 | Enforce contract at ingestion (fail fast, not downstream) | Wire 2.7 into the pipeline entry point |

## Stage 3 — Adoption & moat (later, lower urgency)
| # | Task | Scope |
|---|---|---|
| 3.1 | Draft config schema for self-service source onboarding | Design doc only |
| 3.2 | Minimal Databricks App or notebook UI for source registration | One source type supported |
| 3.3 | Document connector plugin interface (how to add a new source type) | Docs + one worked example |
| 3.4 | Public capability matrix + benchmark doc (from the competitive research) | Marketing/docs artifact, no code |

---

## Suggested sequencing
1. **Stage 0 fully** (these are correctness bugs, not features)
2. **CI backlog item #1** (PR test-result comments — already agreed priority) can run in parallel
3. **Stage 1** in order (1.1→1.6) — each unlocks the next
4. **Stage 2** — 2.1–2.5 can go in any order once Stage 1 is done; 2.6 waits on all three
5. **Stage 3** — whenever bandwidth allows; not urgent

## Notes
- Re-check native Databricks roadmap (Lakeflow Connect FinOps/alerting features) before starting Stage 2 — if Databricks ships native cost/freshness alerting, pivot Stage 2 effort toward contracts instead.
- Each task above should still get its own GitHub issue drafted from `task.md`/`feature_request.md` — this doc is the backlog, not a replacement for that process.

## Background
Derived from a competitive analysis of Databricks bronze-layer ingestion accelerators (v4c.ai, native Lakeflow/Auto Loader, dlt-meta, DQX, Fivetran/Airbyte/Matillion, and SI accelerators from Cognizant/Bizmetric/CUBEANGLE). Full comparison and rationale available on request.
