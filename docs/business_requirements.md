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

**Status:** Under review — **decision-ready**. Decomposed, re-verified against
the tree, conflicts and evidentiary gaps stated below. Awaiting the Project
Lead's disposition. **No issue has been filed for any part of this case**, per
`docs/agent_governance.md`: turning an accepted case into engineering work is
the Project Lead's call, not this agent's.

### What the Project Lead is being asked to decide

1. Which disposition — **narrow** (Option A) or **commission Gold** (Option B).
   See "Recommended disposition" at the end.
2. Three conflicts that must be settled either way, because each one contradicts
   something this repo already wrote down. See "The three conflicts."

**The single most decision-relevant fact:** this is a platform-sized ask —
seven sub-asks spanning three medallion layers, an AI layer, a BI layer and a
natural-language layer — carried by **one sponsor who has not been named**, with
no quantified impact, no timeline and no named source system. The scope and the
evidence behind it are badly mismatched. That mismatch, not any individual
sub-ask, is what should drive the decision.

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

### Reconciliation — decomposition and classification

Re-verified against the working tree at `dev` @ `cdf2c69` on 2026-08-07 by
reading the code and the owning docs, not by inheriting the previous pass.
`docs/README.md`'s ownership index was used as the tiebreak wherever two
documents disagreed about current state.

Each of the seven sub-asks is classified. Several **split**, because their
component parts are in genuinely different states — that splitting is the point
of this section, since "partially built" is where a platform ask hides its real
cost.

| Class | Meaning |
| --- | --- |
| **A — Already delivered** | Built, tested, shipped. Path given as proof. Nothing to file. |
| **B — Already on the roadmap** | An open issue already covers it. Issue number given. Needs visibility, not a new issue. |
| **C — New and scopeable** | No open issue exists. Actionable now; could be scoped into an issue if the Project Lead wants it. |
| **D — New and blocked** | No open issue, and cannot be started. The specific blocker is named. |

| # | Sub-ask (split where the parts differ) | Class | Proof / issue / blocker |
| --- | --- | --- | --- |
| 1a | Config-driven ingestion — onboard a **JSON** source with no code change | **A** | `bronze_layer/bronze_ingest/config.py` — `IngestionConfig.load()` / `from_json` / `from_yaml` / `from_dict`; configs are files in a Unity Catalog Volume. True today. |
| 1b | **Multi-format** source support (CSV, XML, Parquet, Excel) | **C** | Designed in `bronze_layer/docs/architecture.md` ("Target-state build", item 2 — *"independent of the AI layer, can run in parallel with it"*). **No open issue; absent from `docs/roadmap.md`'s 14-issue audit.** Until this lands, "onboard new sources without code changes" holds only for JSON. |
| 1c | **Control-table driven dynamic config** — config resolved from a table, not a file per source | **C** | `bronze_layer/docs/architecture.md` "What's left" item 4, status **Not started**; root `README.md` "In Progress / Planned". **No open issue; absent from `docs/roadmap.md`.** See the correction note below — this is *not* already-sequenced work. |
| 2a | **Bronze** layer | **A** | `bronze_layer/` — ~2,700 lines, 322 local tests, in production on `main` per `docs/roadmap.md` Phase 0. |
| 2b | **Silver** layer | **B** | **#109** (silver-layer business-rule quality engine), `docs/roadmap.md` Phase 5. Both its gates are now resolved — `docs/bronze_silver_contract.md` §5 (contract, #162) and `docs/buy_vs_build_2026-08.md` (verdict: build, #163) — so it is unblocked and is the largest item already on the board. |
| 2c | **Gold** layer | **D** | **No specification exists anywhere in this repo** — not a design, not an issue, not a stub. `AGENTS.md`: *"`gold_layer/` — does not exist yet."* `docs/bronze_silver_contract.md`: *"no `gold_layer/` at all."* Blocked two layers deep: Gold needs Silver's *output* defined, and the contract explicitly leaves **what Silver computes** out of scope (§"Deliberately still open", item 1). |
| 3 | Databricks Asset Bundles for automated deployment | **A** | Root `databricks.yml` — one bundle, three targets (`dev`/`staging`/`prod`), `run_as_service_principal` set explicitly per target. Root `README.md` marks it complete. Nothing to build. |
| 4a | AI **monitors pipeline health / generates documentation** (advisory metadata layer) | **C** | Fully designed in `bronze_layer/docs/architecture.md` — async scheduled job writing `_ai_metadata`, inputs (`_ingestion_audit`, `_schema_registry`) both **Done**, status *"Can start now."* **But no open issue exists for it** and it is absent from `docs/roadmap.md`. Design-complete, unissued. |
| 4b | AI **detects anomalies** | **B** | **#61** (volume anomaly detection), `docs/roadmap.md` Phase 4, after #62. Verdict already recorded as **build (small)** — *"a median over a column this package already owns"*, not an AI model. If the ask specifically wants an ML/LLM anomaly detector, that is a different thing from #61 and needs saying. |
| 4c | AI **suggests fixes** — advisory, a person applies | **C** | No design, no issue. Consistent with the existing architecture (an extra `_ai_metadata` output kind), so scopeable — *but only after Conflict 1 below is settled in this direction*. |
| 4d | AI **applies fixes** — autonomous remediation | **D** | Blocked on **Conflict 1**. This contradicts the founding principle of the AI design (*"nothing in the write path ever reads `_ai_metadata`"*). Not an implementation detail; a reversal. |
| 5 | **Genie Spaces** / natural-language questions over curated business data | **D** | Blocked on **2c (Gold)**, which is itself blocked on Silver's content being defined. Genie reads governed curated tables; there are none and no design for any. `docs/overview.md` already records this exact dependency: *"Ask questions in plain English… Not started — depends on Gold existing first."* Zero design work exists beyond that one line. |
| 6a | **Structural** data-quality validation + quarantine | **A** | `bronze_layer/bronze_ingest/quality.py` — `required_columns` (not-null) and `unique_columns`, with quarantine, content-hash de-duplication and replay. |
| 6b | **Business-rule** data-quality validation (range, regex, set, expression) | **B** | **#109**. `docs/bronze_silver_contract.md` §5 assigns these to Silver's own rule engine. Same item as 2b — this sub-ask and sub-ask #2 converge here. |
| 6c | **Lineage** | **A** *(with a definitional caveat)* | Per-row `_ingested_at` / `_source_file` / `_batch_id` (`config.py`, "Audit / lineage columns added automatically"), plus run-level `_ingestion_audit` and per-table `_schema_registry`. **Caveat:** this is pipeline-emitted provenance. If the ask means Unity Catalog **end-to-end / column-level lineage graphs**, that is a different capability, is nowhere in this repo, and would be class C or D. Listed as an open question — not assumed either way. |
| 6d | **Alerting — operational** (job failure, stuck run) | **A** | `bronze_layer/resources/bronze_ingest_jobs.yml` — `email_notifications.on_failure` and `on_duration_warning_threshold_exceeded`, wired to `${var.notification_email}`. Already ships. |
| 6e | **Alerting — data quality / volume anomaly** | **B** | **#62** (dashboard + SQL alerts over the audit table, `docs/roadmap.md` Phase 4, *nothing gates it*), then **#61**. Distinct from 6d: 6d fires when the *job* breaks, 6e fires when the *data* looks wrong. |
| 7a | Operational dashboard over the audit table | **B** | **#62**, verdict **buy** (Lakeview + Databricks SQL alerts) in `docs/buy_vs_build_2026-08.md` — *"nothing to build but the SQL and the JSON."* Newly viable since #149/#156 made `row_count` mean one thing. |
| 7b | **Executive** dashboards in **Power BI / Tableau**, with **AI-generated summaries and recommendations** | **D** | Blocked on two things at once: **Conflict 2** (tooling contradicts #62's recorded verdict) and, for the content, **2c (Gold)** — executive metrics are Gold-layer aggregates that do not exist. "AI-generated summaries" is additionally outside #62's scope entirely. |

**Named technologies, reconciled separately:**

| Technology | State |
| --- | --- |
| **Delta Live Tables / Lakeflow Declarative Pipelines** (listed "optional") | **Already evaluated and rejected**, not undecided. `docs/buy_vs_build_2026-08.md`: *"not adopted. The differentiators still hold"* — DLT has no equivalent for folder-as-table union ingestion, per-file archival with fallback chain, cross-run retry-limit-before-quarantine, or quarantine replay. Its reappearance here should be read as **"revisit this verdict,"** and answered only if something concrete has changed since that evaluation. Nothing in this ask states that anything has. |
| **MLflow** (listed "optional") | **Genuinely open** — the string appears nowhere in this repo outside this register. No verdict exists either way. It is also **not scopeable as stated**: no use case is given, and MLflow without a stated model to track is a tool in search of a problem. |
| Databricks, Delta Lake, Unity Catalog, Auto Loader, PySpark, Lakeflow Jobs | Already the platform. No decision needed. |

**Tally:** 5 sub-parts already delivered (A), 5 already covered by open issues
(B), 4 new and scopeable (C), 5 new and blocked (D). **Nothing in this ask is
both new and unblocked except 1b, 1c, 4a and 4c** — and of those, 4c is
gated on Conflict 1.

#### What this re-verification corrected from the first pass

Recorded because the previous version of this section was more optimistic than
the tree supports, and the Project Lead should see the delta:

1. **1c was wrongly described as already on `docs/roadmap.md`'s backlog.** It is
   not. Control-table dynamic config appears in `bronze_layer/docs/architecture.md`
   and the root `README.md` checklist, but **has no open issue and no place in the
   roadmap's phase plan**, whose audit covers 14 issues and does not include it.
   Consequence: this is not work already in motion. If it is wanted, someone has
   to file it. Same finding applies to **1b** (multi-format) and **4a** (the
   advisory AI layer): all three are design-complete and issue-less.
2. **Sub-ask #6 was collapsed; it is five different states, not one.** Structural
   quality (A), business-rule quality (B/#109), lineage (A, with a definitional
   caveat), operational alerting (**A — already ships**, previously missed), and
   data-quality alerting (B/#62). The previous line "Alerting is #62" was wrong in
   both directions: job alerting already exists, and #62 is not the whole of
   data alerting either (#61 follows it).
3. **"Genie Spaces is not mentioned anywhere in this repo's docs" was imprecise.**
   The *term* appears nowhere, but the *capability* and its exact blocker are
   already recorded in `docs/overview.md`. This strengthens rather than weakens the
   blocked finding — and the blocker is deeper than first stated: Gold has no
   design, and Silver's *output* is explicitly undecided too.
4. **Sub-ask #1's "no code changes" claim holds only for JSON today.** Not stated
   in the first pass. The framework is genuinely config-driven, but multi-format
   ingestion is unbuilt, so a new CSV/XML/Parquet/Excel source does require code.

One caveat on freshness: `docs/roadmap.md` is written against `dev` @ `79fefbe`
and its own header warns to treat the phase numbering as current only as of its
date. Issue numbers and gating relationships above were cross-checked against
`docs/buy_vs_build_2026-08.md` and `bronze_layer/docs/architecture.md`, which
agree with it.

### The three conflicts

Each of these reopens something this repo already decided explicitly. They are
recorded here rather than folded into a feature description, so they stay visible
whichever way the case goes.

#### Conflict 1 — advisory-only AI vs. autonomous "suggest fixes"

- **What's already decided:** `bronze_layer/docs/architecture.md` splits metadata
  into Fact tables (`_ingestion_audit`, `_schema_registry`) and one Advisory table
  (`_ai_metadata`), and states the principle by construction: **"nothing in the
  write path ever reads `_ai_metadata`."** Malformed AI output is discarded rather
  than written. `docs/overview.md` restates it for non-engineers and pre-emptively
  flags this exact ask: *"If a future ask changes that — for example, 'AI should
  automatically fix data problems it finds' — that's a bigger, separate decision
  that hasn't been made."*
- **What sub-ask #4 introduces:** "suggest fixes," which is ambiguous between
  advisory (4c — AI drafts, a human applies; compatible) and autonomous (4d — AI
  acts on data; a reversal of the principle).
- **Why it can't be deferred:** the two readings have different risk profiles,
  different testing stories, and different governance tiers. 4c is an extra
  advisory output. 4d puts a non-deterministic component in the correctness path
  of a pipeline whose entire recent history (#146–#156) was closing silent
  data-loss and silent-corruption defects.
- **Decision needed:** advisory-only (keep the principle, scope 4c) or autonomous
  (an explicit, recorded reversal with its own design and risk review). **Do not
  scope 4c or 4d until this is answered.**

#### Conflict 2 — Lakeview (#62's recorded verdict) vs. Power BI / Tableau

- **What's already decided:** `docs/buy_vs_build_2026-08.md` verdicts #62 as
  **buy** — *"AI/BI (Lakeview) dashboards and Databricks SQL alerts are the
  product; the work is authoring SQL views over `_ingestion_audit` and checking in
  a `.lvdash.json`… nothing to build but the SQL and the JSON."* `docs/overview.md`
  records the same choice in plain language.
- **What sub-ask #7 introduces:** Power BI / Tableau by name — different tooling,
  with licensing, connectivity and governance implications the buy verdict never
  evaluated — plus "AI-generated summaries and recommendations," which is not in
  #62's scope at all.
- **The two are not necessarily exclusive.** A coherent outcome is: keep #62 as
  the *operational* dashboard on Lakeview (7a, unchanged), and treat
  executive-facing BI (7b) as a separate deliverable with its own tooling
  decision — which then has to wait for Gold regardless of which tool wins.
- **Decision needed:** revise #62's verdict, or split 7a from 7b and leave #62
  intact. Either is defensible; assuming either silently is not.

#### Conflict 3 — Genie Spaces depends on a Gold layer that does not exist as a design

- **The dependency:** Genie Spaces answers questions over governed, curated,
  semantically-described tables. This repo has Bronze — source-fidelity data, by
  deliberate design (`#76` archived the flattener out of Bronze precisely so
  reshaping stays downstream). Pointing Genie at Bronze would produce confident
  answers over unvalidated, unreshaped, un-deduplicated data.
- **How deep the blocker actually runs:** Gold has no design, no issue, no
  owner and no schedule. Silver (#109) is unblocked but unbuilt — and
  `docs/bronze_silver_contract.md` explicitly leaves **"what Silver actually
  computes"** out of scope. So Gold cannot be designed from what exists; it needs
  business definitions (which plants, which metrics, which grain) that this ask
  has not supplied.
- **Why this is a conflict and not just a dependency:** the example question
  quoted in the ask — *"Why did Plant 5 production drop yesterday?"* — is a causal,
  cross-source, business-semantics question. It implies not just Gold but a
  specific set of modelled manufacturing entities and named source systems. **None
  of those are named anywhere in this repo's config.** The ask presents as
  near-term capability something that is at minimum two unbuilt layers and one
  undefined business model away.

### Evidentiary gaps

Stated plainly and **not filled in**. Every one of these is genuinely absent from
what was relayed; none has been estimated, inferred or reconstructed. What each
would change is given, so supplying one has a known effect on the recommendation.

| Missing | Current state | What supplying it would change |
| --- | --- | --- |
| **Named business owner / sponsor** | Not named. Relayed by Yash on behalf of unidentified "business stakeholder(s)." | The most consequential gap. A named owner gives someone to resolve Conflicts 1–3 with, someone to define Gold's business semantics (the Conflict 3 blocker), and someone accountable for whether the built thing was the asked-for thing. **Without it, Option B below cannot responsibly start** — a Gold design commissioned with no business owner to specify grain and metrics would be an engineering guess. |
| **Quantified business impact** | Not stated. No hours saved, incidents avoided, cost or revenue figure. | Determines whether this outranks the currently-sequenced roadmap. Right now BR-001 cannot be compared against #58 (whose value provably decays daily) or #112/#160 (a real, closable security gap) on anything but assertion. A number here is what would justify resequencing. |
| **Timeline / urgency** | Not stated. | Distinguishes "strategic direction, absorb into normal sequencing" from "committed date, resequence the roadmap." Note the one real clock in this repo runs the *other* way: `docs/roadmap.md` Phase 3 flags a possible Azure trial-credit/30-day window that would make provisioning jump the queue over everything here. |
| **Named source systems** | Not named. "Plant 5" appears only in an illustrative question; no MES/ERP/historian/SCADA system is identified, and nothing matching appears in any config in this repo. | Blocks 1b (multi-format) from being scoped at all — you cannot choose between CSV, XML, Parquet and Excel support without knowing what emits what. Also blocks any Gold design, and blocks estimating the ingestion work entirely. |
| **Intended Genie users and their current workaround** | Not stated. | This is the measurement baseline. Without knowing what those users do today, "self-serve" has no comparison point and success cannot be defined in advance. |
| **What "lineage" means in sub-ask #6** | Ambiguous. | Decides whether 6c is class A (already delivered — pipeline provenance columns) or a new, unscoped ask (Unity Catalog column-level lineage). Do not assume; ask. |
| **Whether anything changed to justify revisiting DLT** | Not stated. | The DLT verdict was recorded with specific reasoning. If nothing concrete has changed, listing it here is not new information and the verdict stands unchanged. |

### Recommended disposition

A concrete either/or for the Project Lead. Both are legitimate; they differ in
what they assume about evidence that has not been supplied.

#### Option A — Narrow BR-001 to the already-planned work; defer the rest ✅ *recommended*

- **Do:** mark sub-parts 1a, 2a, 3, 6a, 6c, 6d as **already delivered** — report
  them back to the stakeholder as existing capability rather than building
  anything. File **no new issues** for 2b, 4b, 6b, 6e, 7a: they are #109, #61,
  #62 already, and the roadmap already sequences them (Phase 4: #62 → #61;
  Phase 5: #109).
- **Defer** 2c (Gold), 5 (Genie), 7b (exec BI), 4d (autonomous fixes) with the
  reason recorded: no named owner, no quantified impact, and a hard structural
  dependency on layers that do not exist.
- **Optionally scope now**, as small independent issues, the three items that are
  design-complete but issue-less and genuinely unblocked: **1b** (multi-format,
  *only once source systems are named*), **1c** (control-table dynamic config),
  **4a** (advisory AI metadata layer — architecture says "can start now"). These
  are the honest, buildable residue of this ask.
- **Why this is recommended:** it commits engineering time only to work whose
  value is already established independently of BR-001's unevidenced claims,
  while giving the stakeholder a truthful answer that most of what they asked for
  is either built or already scheduled. It costs nothing if the business case
  later strengthens — every deferred item stays deferred, not rejected.

#### Option B — Commission a Gold-layer design now, to unblock Genie Spaces

- **Do:** treat Genie Spaces as the strategic objective, and commission a Gold
  design (the missing artifact) as the first deliverable — the same shape of work
  `docs/bronze_silver_contract.md` (#162) did for Silver: a document, no
  workspace needed, that decides what Gold contains before anyone builds it.
- **This would reshape `docs/roadmap.md`'s sequencing**, inserting a new design
  phase ahead of or alongside Phase 4/5 and pulling #109 forward as a hard
  prerequisite. Per this repo's own rules that is the Project Lead's call, not
  this register's.
- **What would need to be true for this to be the right call** — all four, not
  some:
  1. **A named business owner exists and is available** to define Gold's grain,
     entities and metrics. Without this the design cannot be written; it would be
     an engineering guess wearing a business document's clothes.
  2. **The source systems are named**, and the data needed to answer the example
     question ("why did Plant 5 production drop") demonstrably exists in them.
  3. **Impact is quantified well enough to outrank what it displaces** —
     specifically #58 (the only item whose value strictly decays, per the
     roadmap) and #112/#160 (an open cross-environment read gap that gets more
     expensive once prod holds real data).
  4. **Silver (#109) is accepted as a committed prerequisite**, not assumed away.
     Genie over Bronze is not a shortcut; it is a worse product that would
     discredit the capability on first use.
- If any of those four is not true, Option B buys a design document that cannot
  be written correctly yet — which is the failure mode
  `docs/bronze_silver_contract.md` was written to prevent, not to repeat.

**Bottom line:** the asked-for platform is largely either built, already
sequenced, or structurally blocked. The genuinely new, genuinely unblocked
residue is small (1b, 1c, 4a, and 4c pending Conflict 1). Given a platform-sized
ask standing on a single unnamed sponsor with no quantified impact, **Option A**
is the proportionate response — and the fastest route to Option B being
answerable is not more analysis here, but the named owner and the source-system
list.

### Open questions for the business owner(s)

Carried forward and still unanswered. These are what a conversation with the
sponsor should cover, in this order:

- **Who is the business owner?** Everything else is downstream of this.
- Which plant(s) and which source systems first? Nothing matching "Plant 5" or
  any manufacturing source system appears in this repo's config.
- What's the quantified business impact — time saved, incidents avoided,
  revenue/cost? Not stated, and deliberately not estimated here.
- Timeline and urgency — not stated.
- Who are the intended Genie Spaces users, and what's their reporting workaround
  today? That's the baseline this platform would be measured against.
- Does "suggest fixes" mean advisory or autonomous? (Conflict 1.)
- Does "lineage" mean the provenance columns that exist, or Unity Catalog
  column-level lineage that doesn't?
- Is Power BI/Tableau a firm requirement, or a default assumption that Lakeview
  would satisfy? (Conflict 2.)
- Has anything concrete changed that would justify revisiting the DLT verdict?
