# AI, Genie and the metadata layer — architecture decision record, August 2026

**Date:** 2026-08-08
**Status: signed off by Yash (Project Lead) on 2026-08-08**, by merging PR #230 into `dev`.
Tier 2 under `docs/agent_governance.md`: it changes the target architecture and commits the
platform to a governed-tag and service-principal model. Sign-off was given by merge, following
the precedent set by `2026-08_autonomous_remediation.md` and PR #223.

**All eight verdicts below are now binding**, not proposed. They are amended by writing the
amendment into this record — in particular, anything the #229 verification checklist
disproves becomes an amendment here rather than a silent workaround.

**Decisions D1, D2 and D3-timing below were made by Yash (Project Lead) in session on
2026-08-08.** The rest of this record derives from them or ratifies existing analysis; it is
written so that the derivation is auditable rather than asserted.

---

## What this document is, and what it is not

`docs/architecture_ai_metadata_2026-08.md` (merged as PR #227) is an **assessment and a
proposal**. Its own § 1 says so, and it deliberately rewrote no living document. It has sat
on `dev` since 2026-08-07 with no issue, no owner and no follow-ups — which means the
repository currently holds a 1,273-line argument that nothing has acted on.

**This record is the decision on that proposal.** It accepts most of it, amends one part
materially, and names what changes as a result. It does not restate the review's reasoning —
where a verdict below rests on an argument, it cites the section rather than reproducing it.

It does **not** reopen:

- the autonomous-remediation exception (`2026-08_autonomous_remediation.md`) — signed off
  2026-08-07, eligible set still empty, unaffected by anything here
- `docs/buy_vs_build_2026-08.md`'s verdicts — one addendum is called below, which is the
  deferral that document already scheduled, not a re-litigation
- `docs/bronze_silver_contract.md` — written and settled

---

## Verdicts

| # | Decision | Status |
|---|---|---|
| **D1** | AI generation runs on **Databricks AI Functions** with a Databricks-hosted Claude model, not an external SDK + PAT | Decided — **gate passed 2026-08-08; see Amendment 1**, which pins `databricks-claude-opus-4-8` |
| **D2** | Scope superseded by native platform features is **marked superseded and frozen, not closed**, until each feature is verified available in this workspace | Decided |
| **D3** | **Genie is a consumption surface and stays at the top of the stack.** It is never pointed at Bronze, quarantine, or the audit tables. Enforced by grants, not by guidance | Decided |
| **D4** | **Gold gets its own epic, filed now, started later.** It is the unnamed gate under #210, #211 and the whole consumption plane | Decided |
| **D5** | **Zero runtime agents in phase one.** When they arrive, they are governed by extending `agent_governance.md`, not by a second model | Ratified |
| **D6** | **KPI definitions are human-authored Metric Views.** AI may draft candidates and explain existing ones. It may not define one | Ratified — hard line |
| **D7** | The AI layer's output surface is **Unity Catalog, through a human review gate**. `_ai_metadata` is demoted to a proposals-and-review table | Ratified |
| **D8** | Adopt the current Databricks nouns in all new writing: **Genie Agents**, **Declarative Automation Bundles**, **Data Quality Monitoring**, **Lakeflow Declarative Pipelines** | Ratified |

"Decided" = a call made in session on 2026-08-08. "Ratified" = the review argued it, this
record accepts it and it is now binding rather than proposed.

---

## D1 — AI generation runs on AI Functions

**The decision.** Add an `AIFunctionsMetadataDrafter` implementing the existing
`MetadataDrafter` Protocol (`bronze_layer/bronze_ingest/ai_metadata.py:79`) and make it the
default. Retain `AnthropicMetadataDrafter` (`ai_metadata.py:103`) as the local-development
escape hatch — do not delete it.

**Why this is cheap.** The seam already exists. `MetadataDrafter` is a one-method Protocol
(`draft(self, prompt: str) -> str`), the `anthropic` import is lazy inside `__init__`, and
the test suite stubs the Protocol rather than the SDK. This is a second implementation of an
existing interface — the prompt builder, watermark logic, parser and writer are untouched.

**Why it is worth doing at all**, given that token cost at ~20K tokens/day is noise
(review § 10.1):

1. **It removes the only credential in the layer.** #115 (secret scopes) has not shipped and
   is blocked on #112. AI Functions need no key — the strongest control is the secret that
   does not exist.
2. **Cost lands in `system.billing.usage`** under `MODEL_SERVING`/`BATCH_INFERENCE`, beside
   every other line, instead of on a second invoice nobody reconciles.
3. **The call becomes auditable in Unity Catalog.** A governance layer whose own actions are
   unauditable is a contradiction.
4. **It stays Claude.** This is *Claude on Databricks*, not *Claude or Databricks*.

**The gate — and this is the amendment.** The review asserts Databricks-hosted Claude via
Foundation Model APIs is GA as of August 2026, and separately that AI Functions require
DBR 18.2+ and **do not run on Pro or Classic SQL warehouses**. The second claim is a
deployment constraint that could block this outright on the current warehouse. Neither claim
has been checked against this workspace.

**So D1 does not execute until the verification checklist below passes.** Until then the
shipped `AnthropicMetadataDrafter` remains the default, and #208 does not add more surface
on top of it (see "What is frozen").

### Amendment 1 — 2026-08-08: the gate passes, and the model changes generation

**Status: proposed. Needs the Project Lead's sign-off by merge, same as the record itself.**

Checklist items 1 and 2 were verified live against warehouse `40d23e52f75f292b` in
`adb-7405607398572130`. Evidence and the full probe matrix are on **#229**.

**Item 1 — passes.** `ai_query` resolves and executes on this warehouse. The review's *"not
available on Pro or Classic SQL warehouses"* does not disqualify it: the Warehouses API
represents a Serverless warehouse as `warehouse_type: PRO` **plus**
`enable_serverless_compute: true`, which is exactly what this warehouse reports. The wording in
the checklist conflated a *tier* with a *compute type* and is corrected below.

**Item 2 — passes, with a constraint the record did not anticipate.** Three Claude endpoints
are served and `READY` — `databricks-claude-opus-4-8`, `databricks-claude-opus-5`,
`databricks-claude-sonnet-5`. **Only `opus-4-8` is callable from `ai_query`.** Both Claude 5
endpoints return `PERMISSION_DENIED: Endpoint ... is not supported for batch inference`. The
error names the endpoint, not the principal — a capability limit, not an entitlement one.

**What this disproves.** Nothing in D1's reasoning. One clause in its consequences: point 4,
*"It stays Claude"*, was written to mean the migration changed transport and not model.
`ai_metadata.py:115` sets `DEFAULT_MODEL = "claude-opus-5"`, so adopting AI Functions is now a
**model-generation change as well as a transport change**. It is still Claude. It is not the
same Claude.

**The amended decision.** D1 stands. The AI Functions path pins
**`databricks-claude-opus-4-8`**, named explicitly rather than left to a default that the
transport cannot call.

**Why that and not the alternatives:**

| Option | Gets | Costs |
|---|---|---|
| **AI Functions on `opus-4-8`** ← chosen | No credential, `system.billing.usage` attribution, UC-audited, in-boundary | An older Claude generation than the code runs today |
| Keep the SDK on `opus-5` | Current model generation | Keeps a PAT — the single thing D1 existed to remove |
| Wait for batch support | Both, eventually | Blocks D1 on a roadmap nobody here controls |

The governance gain is the larger term, and the workload is not close to the quality ceiling.
Per review § 10.6, this layer's inputs are schema, statistics, audit history and lineage —
**never data rows** — and its output is a paragraph; the review judged Haiku adequate for
descriptions and Sonnet worth it for drift explanation. Opus 4.8 sits comfortably above the bar
this workload actually sets. Choosing on output quality rather than price, as § 10.6 directs,
does not favour holding a credential to reach Opus 5 for this.

**Consequences for the implementation:**

1. `AIFunctionsMetadataDrafter` pins `databricks-claude-opus-4-8` as a named constant, **with a
   comment recording why** — otherwise the next person "upgrades" it to `opus-5` and gets a
   runtime `PERMISSION_DENIED` on a scheduled job.
2. `AnthropicMetadataDrafter.DEFAULT_MODEL` stays `claude-opus-5`. It is the local-development
   escape hatch and reaches Anthropic directly, where the Claude 5 family is available. **The
   two drafters legitimately run different models**, and that asymmetry is deliberate rather
   than an oversight — say so in the code.
3. This is a difference worth surfacing in any output comparison: a proposal drafted locally
   and one drafted in-platform are not from the same model.

**Re-probe before treating the model choice as settled.** Batch-inference support is
platform-side and may change. It is one query:

```sql
SELECT ai_query('databricks-claude-opus-5', 'ok');
```

If it starts succeeding, this amendment is superseded by a second one, not silently reverted.

**Not amended:** D1's reasoning, the retention of `AnthropicMetadataDrafter`, the freeze on
#208's other halves (still gated on checklist items 3–4, untested), or any of D2–D8.

---

## D2 — Superseded scope is frozen, not closed

The review's most consequential claim is that roughly 70% of the AI metadata layer's scope
is now GA platform functionality, and should be deleted rather than ported:

| Proposed custom scope | Claimed native replacement | Affects |
|---|---|---|
| PII detection via LLM prompt | UC Data Classification | #208 |
| Freshness / volume anomaly baselines | DQ Monitoring anomaly detection | #61, #62 |
| Baseline column/table descriptions | UC AI-generated comments | #208 |
| Schema drift **explanation** | *nothing native* — this is the residual | Keep |

**If the claim holds, deleting this scope is the cheapest win available** — it retires work
from #61 and #62 before either is built.

**But the claim cannot be verified from this repository.** The capability assertions are
dated August 2026 and were not checked against this workspace, this cloud, or this pricing
tier. Acting on them by closing issues risks a silent gap: an issue closed as "the platform
does this now" is an issue nobody looks at again, and if the feature turns out unavailable
on this tier, the gap surfaces as missing governance rather than as an open ticket.

**Therefore:**

- The affected issues are **labelled `superseded-pending-verification` and frozen** — not
  closed, and not worked.
- **No implementation proceeds on any of them in the meantime.** This is the key property:
  freezing costs nothing, because the alternative was to build something the platform may
  already do.
- Each issue is closed **only** when its replacement is confirmed available and enabled in
  this workspace, with the confirming evidence linked in the closing comment.
- If a replacement turns out to be unavailable, the issue thaws with its original scope and
  the review's § 9 mapping is amended rather than the issue being quietly rebuilt.

**Drift explanation is explicitly retained.** It is the one capability with no native
equivalent, it is already shipped in `ai_metadata.py`, and it is what the layer is *for*
once the rest is deleted.

### The verification checklist — the gate for D1 and D2

Runs during #112/Phase 3 provisioning, against the real workspace. Owner:
`platform-engineer`, drafted for Project Lead sign-off where a Tier 2 action is implied.

| # | Verify | Gates | Status |
|---|---|---|---|
| 1 | AI Functions (`ai_query`) available on the warehouse in use | **D1** | ✅ **PASS** 2026-08-08 — see Amendment 1 |
| 2 | A Claude model is served through Foundation Model APIs **and callable from `ai_query`** | **D1** | ⚠️ **PASS with constraint** — only `opus-4-8`; see Amendment 1 |
| 3 | UC Data Classification is available and can be enabled on this metastore | PII scope in #208 | Untested |
| 4 | UC AI-generated comments are available | Description scope in #208 | Untested |
| 5 | DQ Monitoring anomaly detection is available, and what its dashboard actually covers | #61, #62 | Untested |
| 6 | Governed tags + ABAC column masks are available | #64 | Untested |
| 7 | Bundle support for the resource types this implies, at the CLI version in use | Deployment of all of it | Untested |

Anything that fails becomes an amendment to this record, not a silent workaround.

**Two corrections to this checklist, both from the 2026-08-08 run:**

- **Item 1 was mis-specified.** It said "*a serverless SQL warehouse, since Pro/Classic are
  excluded*", which reads `warehouse_type` as the thing to check. It is not: the Warehouses API
  encodes Serverless as `warehouse_type: PRO` **plus** `enable_serverless_compute: true`. Read
  literally, the original wording would have failed a warehouse that in fact works. **Probe the
  behaviour, don't infer it from the field.**
- **Item 2 was too weak.** "Is served through Foundation Model APIs" is satisfied by an endpoint
  that `ai_query` cannot call — which is exactly what the Claude 5 endpoints turned out to be.
  Amended to require it be *callable from `ai_query`*.

Both are the same lesson, and it is the reason this checklist exists: **a capability is
verified by invoking it, not by reading a field or a docs page.**

**Items 3–6 are metastore/UC features, and per #231 only `ingredion_dev` has anything
deployed** — `ingredion_stg` and `ingredion_prd` contain zero tables. Anything verified there
now is verified for dev only.

---

## D3 — Genie is a consumption surface, and it is four gates away

**The premise correction, restated because it is the whole decision.** Genie is not an
alternative to an LLM API. An LLM API is an inference endpoint a batch job calls to *write*
metadata; Genie is a conversational interface a business user asks questions of, which
*reads* metadata. Substituting one for the other is a category error. Genie **consumes**
curation; it cannot produce it.

**The hard rule, and it is enforceable:**

> A Genie Agent may reference **Gold tables and Metric Views only**. Never Bronze, never a
> quarantine table, never `_ingestion_audit` or `_schema_registry`, never
> `_metadata_proposals`.

Enforced by **grants** — the principal backing a Genie Agent holds no `SELECT` on Bronze —
not by space instructions or reviewer discipline.

**Why the rule is absolute.** Bronze in this repository, by deliberate design, contains
`_corrupt_record` rows retained under `PERMISSIVE` mode, verbatim nested structs, operational
columns with no business meaning, a sibling quarantine table of rows that failed the quality
gate, and — after a replay — the same business fact in two places. "How many orders last
month?" against that returns a number wrong in at least three independent ways, phrased
fluently, with the SQL cited. **A plausible wrong answer with a provenance trail is worse
than an error**, and it is unrecoverable in a way a broken pipeline is not: trust does not
come back.

This is also the single easiest mistake to make under demo pressure, because pointing Genie
at Bronze is the fastest demo available in this architecture. That is precisely why the
control is a grant.

### The gating chain

```
CHANGELOG 0.5.0 table -> table_name backfill   ──►  any read of _ingestion_audit
        │
        └──►  Silver transformation code  ──►  Gold  ──►  Metric Views  ──►  Genie Agents
```

Four gates. **Nothing shortens the chain.** #162 closed one of the original five — the
contract is written, so *what* Silver will be handed is settled. What is not built is Silver
itself: `silver_layer/` is a README, an `_archive/`, and an empty `silver_jobs.yml`.

### Timing — decided

**Genie holds at Phase 7. Gold is filed now but not started.** The approved P0–P4 ordering
stands; this record does not re-sequence it.

#210 (Genie Agents) and #211 (executive dashboard) stay blocked — but they gain a **named**
gate instead of the vague "blocked on Gold, which has no issue" they carry today. Both
issues' open business questions (which plant/system first, who the users are, what their
current reporting workaround is) remain unanswered and are unblocked work that can proceed
independently — `data-analyst` and `business-analyst` can gather requirements against a Gold
schema that does not exist yet, because the requirements are about the business, not the
tables.

**What was rejected, and why it is recorded:** a narrow Genie pilot over one thin Gold slice.
It shortens time-to-demo, but a slice built for a demo is a slice that is not conformed or
deduplicated, which reproduces the P0 defect in a smaller blast radius — in front of exactly
the business users whose trust the platform is trying to earn. Recorded so the option is
re-evaluated on evidence rather than re-argued from scratch when demo pressure arrives.

---

## D4 — Gold gets its own epic

Gold has **no issue and no roadmap entry anywhere in this repository.** `docs/roadmap.md`
has no Gold phase. #210 says so explicitly and flags it to `principal-data-engineer`; #211
names the same gap. It is the single largest unnamed dependency in the backlog: the entire
consumption plane sits behind it, and it is invisible.

**File it as an epic now**, sized and sequenced behind the existing P0–P4 queue. Filing is
not starting — the point is that a gate three other issues depend on should be a tracked
object rather than a sentence in someone else's issue body.

Scope for that epic: business entities and facts derived from Silver, then human-authored UC
Metric Views deployed through the bundle. It is **data modelling, not AI** — and per the
review's § 14, it is the most valuable open item in the repository.

---

## D5 — Zero runtime agents in phase one

**Two populations, and conflating them is the risk.** The ten **SDLC** agents in
`docs/agent_governance.md` build this repository, run in Claude Code sessions, and produce
pull requests. They are well governed and nothing here touches them. The proposed **runtime**
agents would execute as Unity Catalog service principals inside Databricks on a schedule and
write catalog metadata. A bad SDLC agent produces a bad PR, caught in review. A bad runtime
agent produces a bad catalog write, caught only if audited.

**Phase one needs zero runtime agents.** "For every table whose fingerprint changed since the
last watermark, generate a description" is a `MERGE` with an `ai_query` in the projection —
a SQL statement on a schedule. It does not branch. Wrapping it in an agent adds a planner, a
tool registry, a trace store, an eval harness and a serving endpoint to a workload with no
decisions in it.

**Three become correct when the platform is agentic — Curator, Observability, Semantic** —
and the platform is not: it is one layer deep in executable code. Ten never become correct.

**Governance, when they arrive: extend `agent_governance.md`, do not build a second model.**
The existing tiers already classify these actions in the project's own language:

| Runtime agent action | Tier | Consequence |
|---|---|---|
| Read audit, registry, lineage, system tables | 0 | Autonomous |
| Write a row to `_metadata_proposals` | 0 | It is a draft by construction |
| Apply a COMMENT to a UC object | 1 | Reviewable, revertible |
| **Apply a governed tag driving an ABAC mask** | **2** | **A tag write is an access-control write — named Project Lead sign-off** |
| Create or rotate a metadata SP credential | 3 | Never autonomous |

That table resolves the identity question the review raised without inventing anything, and
it is added to `docs/agent_governance.md` as part of this record's change set.

---

## D6, D7, D8 — ratified without amendment

**D6 — KPI definitions are human-authored.** A KPI definition is a contract between the
platform and the business; the entire value of a semantic layer is that the definition is
stable, reviewed and version-controlled. AI may draft *candidate* definitions from query
history and may *explain* an existing metric in business language. The definition itself is
a human-authored UC Metric View, reviewed, and deployed through the bundle. **This is a hard
line, not a default.**

**D7 — the output surface is Unity Catalog, through a review gate.** `_ai_metadata` as
designed is a shadow catalog: AI output in a private Delta table that nothing reads governs
nothing, and creates a second place where the truth about a column lives. It is demoted to
`_metadata_proposals` — a staging queue plus the audit trail of what was accepted, by whom,
and when. Approved proposals reach Unity Catalog through `catalog_metadata.py`'s existing
diff-and-apply path, which already solves the hard part (comment DDL creates a new Delta
version even when unchanged) for a measured reason.

The § 2.2 invariant — *nothing in the write path ever reads AI output* — survives, and gets
stronger: there is no path from the intelligence plane to the deterministic plane at all.

Give `_metadata_proposals` a **lifecycle policy at creation.** #159 is the same lesson, and
this repository should not learn it twice.

**D8 — naming.** Databricks renamed several of these in 2026 and the business requirements
use the older names. New writing uses: *Genie Agents* (not Genie Spaces), *Declarative
Automation Bundles* (not Asset Bundles), *Data Quality Monitoring* (not Lakehouse
Monitoring), *Lakeflow Declarative Pipelines* (not DLT). Existing point-in-time records are
not retro-edited. Note the API paths keep the old nouns
(`/api/2.0/genie/spaces/{space_id}`), which is its own source of confusion — worth a comment
wherever that path appears in code.

---

## Prerequisites that genuinely block

Unchanged from the review's § 11, restated because they are the critical path and because
the first one is currently tracked nowhere:

```
CHANGELOG 0.5.0 table -> table_name backfill  ──►  ANY read of _ingestion_audit
#159 audit lifecycle (OPTIMIZE/VACUUM/retention) ──►  intelligence layer at steady state
#112 provisioning + dedicated metadata SP      ──►  ANY UC write by the AI layer
#64  governed tags                             ──►  ABAC masking on classified data
Silver CODE ──► Gold ──► Metric Views          ──►  Genie Agents
```

**The backfill is the urgent one.** `CHANGELOG.md` 0.5.0 renamed `_ingestion_audit.table` to
`table_name` with `mergeSchema: true`, leaving the table with two columns meaning the same
thing, each half-populated, and a backfill that must be run manually **per environment**. A
metadata layer reading it before that backfill returns a plausible, complete-looking answer
covering half the runs — the #146 failure shape, one abstraction level up.

It is an `UPDATE` against higher environments, so it is **Tier 2/3 and needs named sign-off.**
Tracked as **#231**, filed 2026-08-08 — it had gone a full release as a CHANGELOG step with no
owner. It blocks #208, #61, #62 and #229.

**Mitigation regardless:** the intelligence job asserts on startup that no
`table_name IS NULL` rows exist, and fails closed if any do. The backfill closes today's gap;
the assertion closes the class.

**A process note worth carrying:** a migration step in a CHANGELOG is documentation, not a
work item. This one was documented correctly and prominently — item 1 under "read this before
deploying" — and still sat unexecuted long enough to become a blocker for four issues.

---

## What is frozen, and what proceeds

**Frozen pending the verification checklist:**

- #208's **PII-detection** half — checklist item 3 untested
- #208's **description-drafting** half — checklist item 4 untested
- #61 (volume anomaly detection) — mostly superseded if DQ Monitoring is available (item 5)
- #62 (ops dashboard) — re-scope to what DQ Monitoring's dashboard does *not* cover, namely
  this framework's quarantine and replay metrics (item 5)
- Any further build on `AnthropicMetadataDrafter`

**Thawed 2026-08-08 by Amendment 1:**

- **The drafter swap.** Checklist items 1 and 2 pass, so `AIFunctionsMetadataDrafter` is
  unblocked and is the first Phase 5 item. It pins `databricks-claude-opus-4-8` — see
  Amendment 1 for why the model is named rather than defaulted. Owner: `data-engineer`.
- **#208's job YAML and notebook entrypoint** are no longer held back for being "more surface
  on the SDK+PAT path", since the path they land on is now the AI Functions one. Owners:
  `platform-engineer` (`bronze_layer/resources/ai_metadata_job.yml` — no `databricks.yml` edit
  needed, line 46 already globs `resources/*.yml`) and `qa-engineer` (the notebook, with
  `tests/test_notebooks.py` coverage — both known live production defects in this repo shipped
  through untested notebooks).

**Still true regardless:** the job must not read `_ingestion_audit` without the fail-closed
`table_name IS NULL` assertion (#231). The audit found no environment needs the backfill, but
a pre-0.5.0 audit table still exists in this catalog outside the bundle's targets, so the
class is open even though today's instance is not.

**Proceeds now, unblocked by anything here:**

- The **remaining verification checklist items 3–7** — `platform-engineer`, during #112
- The **CHANGELOG backfill (#231)** — `platform-engineer` drafts and audits per environment,
  `devops-lead` presents, Project Lead signs before any staging/prod write. The cheapest
  unblock available: small, mechanical, and it gates four issues
- **#159** measurement half — promoted to blocking by both this record and the decision
  record's § 8; it is the hot read path of a job that will run daily forever
- **#209's safety harness** — kill switch, rollback, fail-closed remediation record,
  promotion gate, empty eligible set. Unaffected by anything in this record
- The **Gold epic** filing (D4)
- **#210/#211 requirements gathering** — the business questions, not the tables
- Drift explanation in `ai_metadata.py` — the retained residual

---

## Change set, if this record is signed off

Per `docs/README.md`, living documents must not drift and point-in-time records are
superseded rather than edited.

| Document | Change |
|---|---|
| `bronze_layer/docs/architecture.md` | Replace the "Asynchronous AI-assisted metadata layer" section, the `_ai_metadata` row of the three-table model, and "How the AI layer actually runs". **Keep** the two-lane principle, the failure handling and the cost position — all three still hold |
| `README.md` | Correct the schema registry from "planned" to implemented; restate the AI bullet in native terms |
| `docs/roadmap.md` | Full re-audit — it audits 14 open issues against `dev` @ `79fefbe`; there are 22, and #205–#213 appear nowhere. Insert the gating chain and the Gold phase |
| `docs/agent_governance.md` | Add the runtime-agent tier table from D5 |
| `docs/buy_vs_build_2026-08.md` | Add the classification addendum — this **calls the deferral that document already scheduled**, it does not reopen a verdict. Note #61's "build (small)" was decided partly because *"DQX inherits the same workspace constraint"*; a platform feature does not inherit it the same way, so #61 is worth re-testing on that basis |
| `docs/business_requirements.md` | BR-001 ask #5 gains the four-gate chain and D3's grant rule as its acceptance constraint |
| `ai_metadata.py` | Add `AIFunctionsMetadataDrafter`; make it the default **after** checklist items 1–2 pass |
| `docs/architecture_ai_metadata_2026-08.md` | Unchanged — it is a point-in-time record. This document is its disposition |

---

## Open — needs the Project Lead

1. ~~**Sign-off on this record**~~ — given 2026-08-08 via PR #230.
2. **Sign-off on Amendment 1** (the `opus-4-8` pin), by merge. It changes which model the
   platform runs, which is a commitment the record's original D1 did not make.
3. ~~**The CHANGELOG backfill (#231)**~~ — audited 2026-08-08: **no environment needs it.**
   `ingredion_dev` was created after 0.5.0 and never had the two-column state; `ingredion_stg`
   and `ingredion_prd` contain zero tables. **Two things still need a decision:** the
   disposition of `ingredion_en.bronze._ingestion_audit` — a pre-0.5.0 table outside the
   bundle's targets, harmless until something writes to it — and whether the fail-closed
   read-side assertion ships with #208. Also worth correcting wherever it is repeated: the
   premise that *production runs pre-0.5.0 code* is false. **There is no production
   deployment.**
4. **BR-002** (AI-assisted Silver transformation) is sitting as ~173 uncommitted lines on
   `dev`, outside any branch or PR, with its open questions unanswered. It proposes AI inside
   a layer that does not exist. It should be branched and PR'd, and its scope re-read against
   D6's hard line before it is worked.
5. **The #159-ahead-of-#209 re-sequence**, flagged in `2026-08_autonomous_remediation.md`
   § 8 and independently promoted by this record. Two documents now agree; it needs a word.

---

## Follow-ups this surfaced

- **Gold has no roadmap entry** — D4 files the epic; `docs/roadmap.md` still needs the phase.
- **`_metadata_proposals` needs a lifecycle policy at creation**, not after. #159 is the
  precedent and it should not be re-learned.
- **Genie warehouse cost has no ceiling.** Every business question is a warehouse query;
  fifty analysts exploring freely on an oversized serverless warehouse will cost more than
  the entire ingestion platform. A right-sized dedicated warehouse with aggressive auto-stop,
  per-agent warehouse assignment, and a `system.billing.usage` breakout belong in the phase
  plan **before** the first business user is onboarded — not in a post-mortem.
- **A compliance claim nobody can defend.** "We detect PII" with unmeasured recall is a
  regulatory exposure. Classification is a control *input*; enforcement is ABAC on a governed
  tag. Measure recall on a labelled fixture before any such claim is made externally.
- **Metric View drift vs. Genie sample SQL** — if a Genie Agent carries sample SQL that
  recomputes a metric, a dashboard number and a Genie answer can disagree about the same KPI.
  Genie references Metric Views only, and both deploy from the same bundle.
- **Native services cannot be exercised by local `pytest`.** "Buy native" trades engineering
  effort for integration coverage. The gap is closed by a workspace-gated smoke test on
  #113's deploy path, and should be scoped into Phase 3 rather than discovered afterwards.
