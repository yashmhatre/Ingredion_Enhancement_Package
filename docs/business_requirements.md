# Business requirements register

A living log of business problems and opportunities raised by business
owners/stakeholders, each turned into a scoped business case, and — once
reconciled against what this repo has already decided and validated with
Yash (Project Lead) — carried into GitHub issues using the
existing `.github/ISSUE_TEMPLATE/feature_request.md` / `task.md` templates.

Owned by the `business-analyst` agent (see `docs/agent_governance.md`'s
"Agent org chart" — its prompt content lives in the private
`ingredion-agent-config` repo, fetched via `scripts/bootstrap_agents.sh`).
This register sits **upstream** of `docs/roadmap.md`: roadmap.md sequences
work the team has already committed to; this register is where new
candidate work originates before it's scoped, reconciled, and — if
approved — becomes an issue that can eventually earn a place in that
sequencing. A case landing here is not a commitment; an issue is.

**Note on scope:** this doc has a row in `docs/README.md`'s ownership index
under "Business case intake."

## Status vocabulary

| Status | Meaning |
| --- | --- |
| **Proposed** | Captured, not yet reconciled against existing decisions |
| **Under review** | Reconciliation done; open questions or decomposition needed before it can become issues |
| **Approved → issues filed** | Scoped pieces have corresponding GitHub issues; link them here |
| **Deferred** | Real, but not now — record why and what would change that |
| **Rejected** | Not being done — record why, same as this repo already does for rejected technical alternatives (see `docs/buy_vs_build_2026-08.md` for the pattern) |

---

## BR-001 — AI-Powered Metadata-Driven Manufacturing Intelligence Platform

**Raised by:** business stakeholder(s), relayed by Yash, 2026-08-06 — specific
business owner(s) not yet named; capture when known.

**Status:** Under review — genuinely large scope, partially overlapping with
existing decisions, needs decomposition and confirmation before issues are
filed.

### Problem, as stated

Manufacturing data is fragmented across multiple operational systems.
Business users depend on engineers to build reports, investigate production
issues, and onboard new data sources — there's no self-serve path.

### Proposed solution, as stated

A production-grade Databricks platform that:
1. Uses a metadata-driven ingestion framework to onboard new sources without code changes
2. Implements Bronze → Silver → Gold medallion architecture
3. Uses Databricks Asset Bundles for automated deployment
4. Employs AI agents to monitor pipeline health, detect anomalies, suggest fixes, and generate documentation
5. Publishes curated business data for Genie Spaces so users can ask natural-language questions ("Why did Plant 5 production drop yesterday?")
6. Automates data quality validation, lineage, and alerting
7. Provides executive dashboards with AI-generated summaries and recommendations

**Technologies named:** Databricks, Delta Lake, Unity Catalog, Auto Loader,
Asset Bundles, AI Agents, Genie Spaces, PySpark, Delta Live Tables
(optional), MLflow (optional), Lakeflow Jobs/Workflows, metadata-driven
control tables, Power BI/Tableau.

### Reconciliation against what this repo has already decided

This is exactly the check `docs/README.md`'s "code wins over any document"
rule and this repo's decision-recording culture demand before treating
anything as green-field. Item by item:

| Ask | Existing state |
| --- | --- |
| **#3 Asset Bundles for automated deployment** | **Already built.** One root `databricks.yml`, three targets, per-target service principals. Nothing to build here — see `databricks.yml`'s header. |
| **#1 Metadata-driven ingestion, no code changes per source** | **Partially exists, partially open.** The bronze layer is already config-driven (`IngestionConfig` per source, no code changes today). "Control-table driven **dynamic** configuration" — config resolved from a table rather than a file per source — is an open, barely-started item already in `docs/roadmap.md`'s backlog. This ask is that item, not a new one. |
| **#2 Bronze → Silver → Gold medallion** | **Bronze done and in production. Silver and Gold do not exist.** `docs/bronze_silver_contract.md` already defines what Bronze hands Silver; Silver's build is gated on #163 (buy-vs-build, resolved: build) and #162 (contract, resolved). This ask is asking for the single largest item already on the roadmap, not proposing a new one. |
| **#4 AI agents monitoring pipeline health, detecting anomalies, suggesting fixes, generating documentation** | **Overlaps two already-decided designs.** `bronze_layer/docs/architecture.md` already specifies an async, advisory-only AI metadata layer (schema drift summaries, PII flagging, quarantine reports) — explicitly **never in the write path, never gating a decision**. `docs/buy_vs_build_2026-08.md` already verdicts #61 (volume anomaly detection) as "build (small)" — a rolling median, not an AI model. **"Suggest fixes" is new and needs a decision**: does it stay advisory (a person applies the fix), or does it act? The existing architecture's whole design principle is the former. If this ask means the latter, that's a real, separate decision this repo hasn't made, not an implementation detail. |
| **#5 Genie Spaces / natural-language queries over curated business data** | **Genuinely new — not mentioned anywhere in this repo's docs.** Also structurally blocked: Genie Spaces reads governed, curated tables, which means it depends on Gold (or at least Silver) existing. Not actionable until that gate clears. |
| **#6 Automated data quality validation, lineage, alerting** | **Already decided, mostly at the Silver layer.** `docs/bronze_silver_contract.md` §5 assigns business-rule quality checks to Silver's own rule engine (build, per #163). Lineage (`_source_file`/`_ingested_at`/`_batch_id`) and the audit trail already exist in Bronze. Alerting is #62, already verdicted "buy" (Databricks SQL alerts). |
| **#7 Executive dashboards with AI-generated summaries** | **Partially conflicts with an existing decision.** #62 is already verdicted "buy" — Lakeview + Databricks SQL alerts, specifically because "nothing to build but the SQL." **This ask names Power BI/Tableau**, a different tool than what was chosen, and adds "AI-generated summaries," which #62's scope doesn't include. Needs an explicit decision: revise #62's verdict, or keep #62 as the ops/data-quality dashboard and treat this as a separate, new executive-facing deliverable. |
| **Delta Live Tables (listed as optional)** | **Already evaluated and rejected**, not merely undecided. `docs/buy_vs_build_2026-08.md`: *"Lakeflow Declarative Pipelines (formerly DLT) — not adopted. The differentiators still hold"* (folder-as-table ingestion, per-file archival, cross-run retry-limit, quarantine replay — DLT has no equivalent for any of these). Including it here should be read as "revisit this verdict," not as new information the verdict didn't consider — unless something concrete has changed since that evaluation. |
| **MLflow (optional)** | Not mentioned elsewhere. Genuinely open — no verdict exists either way. |

### Recommendation

This is not one feature — it's most of the multi-year roadmap plus two
genuinely new capabilities (Genie Spaces, AI-suggested-and-possibly-applied
fixes), stated as a single ask. Before any GitHub issue gets filed:

1. **Confirm with Yash which pieces are actually new asks** versus a
   restatement of already-planned work (rows above marked "already exists"
   or "already on the roadmap") — those don't need new issues, just
   visibility that they're already in motion.
2. **Get an explicit decision on "suggest fixes"** — advisory-only (matches
   the existing architecture) or autonomous remediation (a new, larger
   decision with its own risk profile) — before any issue is written for it.
3. **Get an explicit decision on the #62 dashboard conflict** — Lakeview/SQL
   alerts (already bought) vs. Power BI/Tableau (named here) — before
   assuming either.
4. **Decompose the genuinely-new, genuinely-actionable remainder** (Genie
   Spaces once Gold exists; MLflow's actual use case, if any) **into
   separate, individually-scoped issues** using the Feature Request template
   — each with its own acceptance criteria, not one issue trying to hold a
   platform.

### Open questions for the business owner(s)

- Which plant(s)/system(s) first? "Plant 5 production drop" in the example
  question implies specific source systems not yet named anywhere in this
  repo's config.
- What's the quantified business impact (time saved, incidents avoided,
  revenue/cost) — not stated in the raw ask, and shouldn't be assumed or
  invented here.
- Who are the intended Genie Spaces users, and what's their current
  reporting workaround today? That's the actual baseline this platform
  would be measured against.
- Timeline/urgency — not stated.
