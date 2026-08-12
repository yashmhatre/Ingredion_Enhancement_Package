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

**Status:** Approved → issues filed — see "Decisions — 2026-08-07" and
"Issues filed" below.

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

### Decisions — 2026-08-07

Yash (Project Lead) made the three decisions the Recommendation section
above asked for. Recorded here with reasoning, per this repo's
decision-recording culture (`docs/buy_vs_build_2026-08.md`'s pattern).

**1. "Suggest fixes" = autonomous remediation, not advisory-only.**
Yash chose autonomous remediation: the AI layer may act on some class of
detected issue, not merely draft a suggestion for a person to apply. **This
directly reverses `bronze_layer/docs/architecture.md`'s stated principle**
for the AI-assisted metadata lane — *"it never sits in the write path and
never gates an ingestion decision... If a future proposal would have an AI
model decide whether a row is accepted, that's a different, bigger decision
this document does not make — see `docs/business_requirements.md` BR-001
for a live instance of that question being asked."* That conflict was
stated to Yash explicitly, in those terms, before this decision was made —
it is being recorded, not re-litigated, here. Its weight is reflected in
how the resulting issues are structured: #206 (the AI epic) requires a
blocking design + risk decision record (#207) — covering blast radius,
rollback, a human-in-the-loop kill switch, which fix classes are eligible,
and audit requirements — with an explicit amendment to `architecture.md`,
before any autonomous-execution implementation work (#209) may start. #209
is marked blocked on #207 in both issues.

**2. Executive dashboard: #62 stays as-is; a new, separate issue for the
business-facing dashboard.** #62 remains the ops/data-quality dashboard on
Lakeview + Databricks SQL alerts, exactly as verdicted in
`docs/buy_vs_build_2026-08.md` — that verdict is not reopened. Ask #7's
business-facing executive dashboard with AI-generated summaries is filed
separately as #211, with the BI tool (Power BI / Tableau / Lakeview) left
as an explicit open question inside that issue, and gated on Gold existing
(Gold has no build plan anywhere in this repo today — see "Issues filed"
below).

**3. Scope for this round of filing = genuinely-new capabilities + one
Silver-layer epic that wraps already-scattered work.** Filed: Genie Spaces
(#210), the AI-assisted monitoring + autonomous remediation layer (#206,
wrapping #207/#208/#209), the executive dashboard (#211), and an MLflow
spike (#212) — all genuinely new. Also filed: a Silver-layer epic (#205)
that takes the three already-open issues covering Silver (#109, #162,
#163) as children rather than restating them. Not re-filed: Asset Bundles
(#3, already built), the already-shipped portion of metadata-driven
ingestion (#1), and control-table-driven dynamic config (already an open
`bronze_layer/docs/architecture.md` backlog item — "What's left" table,
item 4 — not yet a numbered issue, but explicitly not new scope). #58,
#61, and #62 are referenced from the filed issues rather than duplicated.

### Issues filed

- **#213** — BR-001 parent tracking issue (checklist of everything below)
  - **#205** — Silver-layer epic → children **#109**, **#162**, **#163**
    (existing issues, linked as sub-issues, not restated)
  - **#206** — AI-assisted pipeline monitoring + autonomous remediation
    layer → children:
    - **#207** — Design + risk decision record for autonomous remediation
      (blocking gate)
    - **#208** — Async AI metadata job (advisory only; not gated on #207)
    - **#209** — Autonomous remediation executor (blocked on #207)
  - **#210** — Genie Spaces (blocked on Gold — no issue/roadmap entry
    exists for Gold today)
  - **#211** — Executive dashboard with AI-generated summaries (blocked on
    Gold; BI tool an open question inside the issue; separate from #62)
  - **#212** — MLflow spike

### Open questions for the business owner(s) — still unanswered

None of these were answered before this round of filing. They are carried
forward rather than invented, and they gate real work inside the issues
above (in particular #210 and #211, which cannot be fully scoped without
answers to the first two and the third respectively):

- Which plant(s)/system(s) first? "Plant 5 production drop" in the example
  question implies specific source systems not yet named anywhere in this
  repo's config. **Still unanswered.**
- What's the quantified business impact (time saved, incidents avoided,
  revenue/cost) — not stated in the raw ask, and shouldn't be assumed or
  invented here. **Still unanswered.**
- Who are the intended Genie Spaces users, and what's their current
  reporting workaround today? That's the actual baseline this platform
  would be measured against. **Still unanswered.**
- Timeline/urgency — not stated. **Still unanswered.**

---

## BR-002 — AI-assisted Silver transformation, constrained to minimal metadata

**Raised by:** Yash (Project Lead), 2026-08-07. No business owner, quantified
impact, or timeline given — captured as stated, not invented. See "Open
questions" below.

**Status:** Under review

### Ask, as stated

Verbatim: *"process bronze data using AI in silver with giving ai minimal
metadata."*

This is a technical direction, not a stated business problem — there is no
"who is asking / what breaks today / why now" in it. That gap is recorded as
an open question, not filled in.

### Reading the ask — two structurally different products

**Reading A — AI drafts deterministic transformation rules from metadata;
Silver's rule engine executes them against real rows.** The AI sees schema,
drift history, and an aggregate profile — never a row — and produces a
candidate rule/mapping spec (e.g. a flatten mapping, a business-rule
suggestion, a column-rename/type-coercion suggestion). A human (or a review
step) promotes an accepted draft into Silver's own rule engine, which then
runs deterministically over actual data. The AI is design-time and advisory;
the transformation that touches rows is not the AI.

**Reading B — AI is invoked per-batch/per-row as the transformation itself**,
deciding output values live, inside Silver's write path.

**The ask's own qualifier — "minimal metadata" — only supports Reading A.**
If the AI is given schema/profile/drift and *not* rows, it structurally
cannot be the thing computing a per-row transformed value in Reading B —
there is nothing for it to transform. Reading B would require the AI to see
row data, which directly contradicts the "minimal metadata" constraint as
given. Unless Yash means something different by "minimal metadata" than the
literal reading (see open questions), **Reading A is the one recommended**,
and it is also the only one the ask as stated is internally consistent with.

**Reading B is also a bigger, separately-gated decision, not an
implementation detail of this one.** `bronze_layer/docs/architecture.md`'s
governing rule — "it never sits in the write path and never gates a[n]
... decision" — was amended on 2026-08-07 for exactly one bounded exception
(autonomous remediation, BR-001 item 4), and that exception's own NEVER list
(`docs/decisions/2026-08_autonomous_remediation.md` §2, item 9) explicitly
excludes **"anything outside the bronze layer,"** reasoning *"Silver does not
exist; ... nothing autonomous reaches into a layer that has no design yet."*
So Reading B for Silver cannot ride on the 2026-08-07 sign-off — it would
need its own decision record, of the same weight and shape as #207, scoped
to Silver, with its own Tier 2 sign-off. That is a real, separate ask this
repo has not made, and if Yash intends Reading B, it should be named as that
explicitly rather than assumed to follow from BR-001's remediation decision.

### What "minimal metadata" would have to contain, and what it must never contain

This boundary is the heart of the ask, so it needs to be explicit before any
design work starts on it.

**May contain (schema/aggregate facts, no rows):**
- Schema — column names, types, nullability (`_schema_registry`, Fact)
- Schema-drift history / fingerprint changes (`_schema_registry` +
  `_ai_metadata.schema_drift_summary`, already produced by the shipped
  advisory job)
- Nesting/structure shape (array vs. struct depth, keys present) — enough to
  draft a `flatten_dataframe` mapping (§4 of the contract) without seeing a
  single value
- Aggregate, non-identifying column statistics — null-rate, distinct-count /
  cardinality, numeric min/max/mean/stddev, string min/max length. **This
  does not exist anywhere in the repo today** — Bronze computes no per-column
  statistics; this is new scope, not a reuse of an existing table.
- Existing `_ai_metadata` drafts (table/column descriptions, PII flags) —
  reused, not regenerated
- Run-level facts already in `_ingestion_audit` — row counts, quarantine
  reason distribution

**Must never contain:**
- Any row, any sample of rows, or any single business-column value
- Any "most frequent value" / mode / top-N-values statistic — even an
  aggregate can leak an identifying value on a low-cardinality column (a
  common name, a single dominant customer ID)
- Full-precision aggregates on any column `_ai_metadata.pii_flags_json` has
  already flagged as possible PII — at minimum, min/max/mode should be
  suppressed for those columns even though null-rate/cardinality-bucket may
  be safe
- Anything used to derive a *corrected* business value — that would be
  Reading B in different clothing, and reopens the same boundary
  `docs/decisions/2026-08_autonomous_remediation.md` §2 NEVER item 3 already
  drew for Bronze ("a corrected value is a transformation... destroys
  re-derivability"), one layer up

### Reconciliation against what this repo has already decided

| Ask | Existing state |
| --- | --- |
| **AI processing data in Silver** | `docs/bronze_silver_contract.md`'s "Deliberately still open" §1 states *"What Silver actually computes. Entirely out of scope"* — Bronze's contract doesn't decide this, and nothing else in the repo does either. BR-002 is proposing to answer part of that open question, not restating a decision. |
| **Silver has its own transformation/rule mechanism** | Already decided. Contract §5: *"Silver gets its own rule engine. `quality.py` is not extracted."* If Reading A is adopted, AI drafts feed *into* that engine — it doesn't replace or duplicate it. |
| **AI given only metadata, never rows, for a summarization/drafting task** | Already shipped, as a precedent — not a new principle. `bronze_layer/bronze_ingest/ai_metadata.py`'s docstring: *"the model is asked to summarise facts the pipeline already recorded... it never sees or reshapes the underlying rows."* BR-002's Reading A is the same posture, applied to Silver instead of Bronze, and to rule-drafting instead of description-drafting — a larger scope (drafted output would eventually shape a transformation, not just prose) but the same shape. |
| **AI in a layer's write path** | Directly reconciled above (Reading B). The one exception that exists (`docs/decisions/2026-08_autonomous_remediation.md`) is Bronze-scoped and explicitly excludes Silver (§2 NEVER item 9). No exception exists for Silver today. |
| **Silver itself — does it exist to build this on?** | **No.** `silver_layer/` is a README and an archived flattener. The Silver epic (**#205**, filed under BR-001, children #109/#162/#163) has not started; Yash's approved P0–P4 ordering (2026-08-07) places the Silver-adjacent items (#109) in **P4 — later**, behind #112/#113/#115/#160 (P3) and #62/#61/#58 (also P4, ahead of #109). BR-002 is asking to add an AI-assist track to a layer that is not yet actively scheduled. |
| **LLM provider / cost** | Still an open decision. #225 (merged) defaults the existing Bronze advisory job to the Anthropic SDK behind an injectable interface, but the provider choice itself was never made as a buy-vs-build decision — `docs/buy_vs_build_2026-08.md` has no verdict on it. Cost for a Silver-facing use of the same or a different model is therefore **unestimated**, not merely unstated; no number should be assumed here. |
| **Rule profiling from data** (a related but distinct idea) | `docs/buy_vs_build_2026-08.md`'s "Follow-ups this surfaced" names *"Rule profiling — generating candidate quality rules from data, which DQX does and #109 does not propose"* as an open, unfiled idea. That follow-up profiles actual **data**; BR-002's Reading A profiles **metadata only**. They look adjacent and are not the same ask — worth flagging so the two don't get merged into one issue by accident. |
| **Relationship to BR-001** | This ask plausibly **is not a new parent case**. It reads as a refinement of BR-001 item 2 (Silver, already #205) crossed with item 4 (AI agents, already #206/#207/#208/#209) — specifically, it is asking what BR-001 item 2 + item 4 look like once Silver exists, for the transformation (not the monitoring/remediation) half of the AI track. Registering it as BR-002 keeps this specific ask (and its "minimal metadata" constraint) traceable on its own terms, but if approved it should most likely be filed as new sub-issues under **#205**, using the same design-record pattern **#207** established, rather than as an independent epic. That relationship is itself a call for `principal-data-engineer` / Yash, not this register. |
| **`docs/roadmap.md`** | Stale per the live P0–P4 ordering — not treated as authoritative for sequencing here. Neither #205 nor any BR-002-shaped work appears in its phase plan. |

### Recommendation

1. **Reading A is the recommended interpretation** — AI drafts
   metadata-derived transformation/rule specs; Silver's already-decided rule
   engine executes them against real rows; a human promotes drafts before
   they take effect. This is consistent with the shipped Bronze advisory
   pattern and does not reopen any write-path decision.
2. **Reading B (AI as the live, per-row transformation) is not recommended**
   and, if Yash wants it anyway, needs to be named explicitly — it requires a
   new decision record scoped to Silver, of the same weight as #207, because
   the existing bronze exception does not reach Silver.
3. **This depends on Silver existing.** Nothing here should be filed as
   issues, or even fully decomposed, ahead of #205 having enough shape to
   receive them — that sequencing question (does an AI-assist track get
   added to #205 now, or wait until #109/#162/#163 land) is a roadmap
   question for `principal-data-engineer` and Yash, not a unilateral call
   here.
4. **The aggregate-profiling capability is new scope**, independent of the
   AI question — nothing today computes per-column statistics on Bronze or
   Silver data. Whether that gets built as part of this case or as its own
   prerequisite issue is worth deciding explicitly rather than folding it
   silently into a larger AI issue.

### Decomposition (candidate — not filed; Under review, not Approved)

Sized to the shape of the ask today; a `solution-architect` design, once
attached, supersedes this sizing for the pieces it covers.

1. **Decision: reading + whether a Silver-scoped write-path exception is
   needed** — Yash's call, same shape as the BR-001 "suggest fixes" decision.
   Blocking; not an implementation issue.
2. **Minimal-metadata payload definition for Silver** — enumerate the exact
   fields (schema, drift, aggregate profile, reused `_ai_metadata` drafts)
   and the PII-suppression rule for flagged columns; likely a short
   contract doc in `bronze_silver_contract.md`'s style, since it's a
   cross-layer interface decision, not an implementation detail of either
   layer alone.
3. **Aggregate-profiling job** — the new per-column statistics computation
   (null-rate, cardinality, numeric ranges) needed to feed #2; does not
   exist today in either layer.
4. **AI rule-drafting job** — mirrors `ai_metadata.py`'s shape (standalone,
   async, advisory), consumes #2's payload, writes candidate rule specs for
   human review; depends on #205's rule engine existing to have somewhere to
   promote drafts into.

### Open questions for Yash — still unanswered

- **Confirm the reading.** Reading A is recommended and is the only one
  consistent with "minimal metadata" as stated; if Reading B is actually
  intended, that needs to be said explicitly, because it is a different,
  larger decision requiring its own Silver-scoped decision record.
- **Does "minimal metadata" allow any value-level statistic at all**
  (numeric min/max, string length) on columns not flagged as PII, or should
  it be stricter — schema and drift only, no aggregates? The ask doesn't
  define "minimal" precisely enough to build against.
- **Sequencing:** does this become an AI-assist sub-track of #205 now (ahead
  of Silver's core rule engine landing), or wait until #109/#162/#163 ship?
  This reshapes P4 of the approved priority ordering and needs
  `principal-data-engineer` + Yash to decide, not this register.
- **Business owner, quantified impact, timeline** — none given in the raw
  ask. Not invented here; needed before this can move to Approved.
- **LLM provider/cost** — still open repo-wide (see reconciliation above);
  does a Silver-facing use share #225's Anthropic-SDK default, or does the
  higher likely call volume (per-table rule drafting vs. per-table
  description drafting) warrant its own buy-vs-build pass?
