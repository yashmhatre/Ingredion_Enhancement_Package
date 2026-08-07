# Autonomous remediation — design and risk decision record, August 2026

The blocking deliverable for **#207**, which gates **#209** (the autonomous
remediation executor). Records the bounds on a decision that has already been
made, not the decision itself.

**Status: draft. Requires Yash's explicit, named sign-off before #209 may
start.** #207 is Tier 2 under `docs/agent_governance.md` — this document
authorizes an autonomous agent to write to production data, which is not a
thing `principal-data-engineer` may sign off on its own authority.

## What this document is, and what it is not

On 2026-08-07 Yash (Project Lead) decided that BR-001's "AI agents ... suggest
fixes" means **autonomous remediation** rather than advisory-only. That
decision **directly reverses** the one rule governing the AI lane in
`bronze_layer/docs/architecture.md`:

> it never sits in the write path and never gates an ingestion decision.
> Acceptance, rejection, and quarantine are decided exclusively by
> `quality.py`, deterministically.

The conflict was stated to Yash in those terms before the decision was made,
and is recorded in `docs/business_requirements.md` ("Decisions — 2026-08-07").
**It is not re-argued here.** This document takes the direction as given and
does the only remaining useful thing: it says precisely how far the exception
extends, and it refuses to let it extend one inch further by default.

That is the whole value of this record. An exception with no stated edge is
not an exception — it is a repeal that nobody wrote down. Every section below
exists to give the exception an edge.

This document follows `docs/buy_vs_build_2026-08.md`'s pattern deliberately:
verdicts with their reasoning **and** the alternatives that were rejected,
because a verdict whose rejected alternatives are missing gets re-proposed
every six months by someone who cannot tell it was already considered.

**Out of scope, on purpose:**

- The amendment to `bronze_layer/docs/architecture.md` — that is **#218**, and
  this document does not make it.
- Implementation design for any mechanism named below. Where this record says
  a mechanism must be built, it states **what that mechanism must guarantee**
  and stops. How it is built is **#209**'s and `data-engineer`'s work.
- Reopening the decision. See above.

---

## Verdicts

| Question #207 asks | Verdict |
| --- | --- |
| **Which fix classes are eligible?** | **None. The eligible set starts empty**, and a class earns entry through the promotion gate in §3 — a gate whose first use is a Tier 2 sign-off, not an engineering judgement call |
| **What may a fix touch?** | Only the ceiling in §2, and only the subset of it that a promoted class names. Nothing is in scope by virtue of not being forbidden |
| **What may a fix never touch?** | §2's NEVER list — the audit trail, the schema registry, the quality gate's verdict, business column values, any delete, any Tier 2/3 action, and the safety mechanisms themselves |
| **Kill switch** | **Does not exist and must be built.** There is no mid-flight halt of any kind in this package today. Prerequisite to #209, not a property of it — §4 |
| **Rollback** | **Does not exist and must be built.** Today's only undo is Delta time travel against an unchosen retention horizon. Prerequisite to #209 — §5. Depends on **#159**, and interacts with **#58** |
| **Audit requirements** | A dedicated remediation record that **fails closed** — the deliberate inverse of `_ingestion_audit`'s never-raise contract — §6 |
| **Roadmap impact** | #209 now depends on #159 (Phase 4) and interacts with #58 (Phase 5). That reordering is **the Project Lead's call**, flagged in §8, not made here |

**The honest summary in one sentence:** the evidence gathered for this record
shows that autonomous remediation currently has nothing safe to do and none of
the machinery it would need to do it safely, so #209's realistic first
deliverable is the safety harness shipped with an empty eligible set.

---

## 1. Evidence base

This record rests on two investigations commissioned as #207's dependencies,
both now complete. It is a synthesis of their findings, not an independent
assessment, and where the two disagree with this document the issues win.

| Source | Question it answered |
| --- | --- |
| **#215** (`data-analyst`) | Which observed failure classes are mechanically fixable |
| **#216** (`platform-engineer`) | What kill-switch, rollback and audit surfaces already exist |

#216's mechanism findings were re-verified against `dev` while drafting this
record — every one of them holds, and the specific code that makes each true
is cited inline below rather than asserted. The verification is worth having
because #207 was written assuming several of these mechanisms existed.

---

## 2. Blast radius

Because the eligible set is empty (§3), this section cannot describe what a
fix touches today. It instead sets a **ceiling**: the maximum surface any
future fix class may be permitted to reach. A class promoted under §3 must
name a subset of this ceiling; it does not inherit the whole thing.

### The governing rule

> **A fix class is a candidate only if a human operator already has a
> supported way to perform the same action and a supported way to undo it.**

If no supported human path exists, "autonomous" is not a speed improvement
over a person — it is an agent doing something nobody in this repo has ever
done deliberately, with no reference behaviour to compare against and no
operator who knows what the undo looks like. This rule is what keeps the
ceiling anchored to things this codebase has already reasoned about.

### MAY touch — the outer bound

Each of these has an existing, tested, human-operable entry point and an
existing inverse. None is in scope until a class naming it is promoted.

| Surface | Existing human path | Why the blast radius is bounded |
| --- | --- | --- |
| `_ai_metadata` | The AI layer already owns it (#208) | Advisory by construction; nothing in the write path reads it |
| File placement within a source dir's own `quarantine_files/` ↔ source subtree | `reprocess_quarantined_files()` in `replay.py` | A move, whose inverse is the opposite move. No data is read, written or destroyed |
| Retry-state entries under `_state/` | `RetryState.clear()`, already called by file replay | Re-attempting a file is idempotent by `idempotent_batch_writes`' txn keying (#63) |
| A **named allowlist** of `IngestionConfig` fields whose worst case is a re-run | config load, per-file overrides | Bounded only if the allowlist is enumerated and excludes everything in the NEVER list. An empty allowlist is the correct starting state |

### NEVER touch — hard prohibitions

These are not defaults to be revisited per fix class. A proposal that needs
one of these is out of scope for autonomous remediation entirely and must
come back as its own decision, with the same weight #207 carried.

**1. `_ingestion_audit` and `_schema_registry`.**
`architecture.md` classifies both as **Fact**, against `_ai_metadata`'s
**Advisory**. If the remediator can rewrite facts, the audit trail stops being
evidence of what happened and becomes evidence of what something decided
should have happened. It is also the surface the remediator itself is judged
against (§6) and the surface #62's dashboard and #61's baselines are being
built on. Absolute.

**2. The quality gate's accept/reject/quarantine verdict.**
Yash's decision reverses "the AI never sits in the write path." It does **not**
make the AI the arbiter of whether a row is acceptable. These are separable,
and separating them explicitly is load-bearing, because conflating them is
precisely how a bounded exception silently becomes an unbounded one.
`architecture.md` already anticipates this as *"a different, bigger decision
this document does not make."* It remains unmade.

**3. Business column values in a bronze row.**
Bronze's stated job is source fidelity — `flatten_mode` was removed from
Bronze entirely on exactly this reasoning, and `docs/bronze_silver_contract.md`
places reshaping in Silver. A corrected value is a transformation. An AI
editing a business value in bronze destroys the property that makes bronze
re-derivable and makes any Silver rebuild-from-bronze untrustworthy in a way
no downstream check can detect. This is a bronze/silver contract boundary, and
this record does not reopen it.

**4. Any delete, against any table.**
Deletion is unrecoverable past the time-travel horizon, and that horizon is
currently unchosen and partly outside this repo's control (§5). A fix class
that needs a delete is ineligible until the horizon is a number someone
picked.

**5. Any Tier 2 or Tier 3 action under `docs/agent_governance.md`.**
`GRANT` / `REVOKE` / `DROP` / `VACUUM` / destructive `DELETE`, deploy targets,
`run_as` values, secrets, promotion branches, enabling CDF, changing VACUUM
retention. Governance in this repo binds **by action, not by role** — that is
stated in `agent_governance.md` and it applies to a runtime agent exactly as
it applies to a coding agent. A remediator that could take a Tier 2 action
would be an agent escalating its own privileges by executing rather than by
asking, which is the specific failure that document exists to prevent.

**6. Any `IngestionConfig` field that redirects where data is read from or
written to.** Named, not implied: `source_path`, `catalog`, `schema_name`,
`table`, `quarantine_table`, `reader_options`, `allow_unsafe_reader_options`.

This one has a precedent in this repo worth restating, because it is the same
finding: `reader_options` is passed verbatim to `spark.read.option()`, `path`
*is* a reader option, and #154's allowlist exists because an unfiltered
passthrough *"lets a config redirect the read at a location the config was
never meant to touch, while every log line and audit row still reports
`source_path`."* A remediator with write access to these fields reintroduces
that hole with an agent holding the pen.

**7. Any field that changes what a write means or whether it is recorded:**
`write_mode`, `merge_keys`, `idempotent_batch_writes`, `add_audit_columns`,
`enable_run_audit`, `enable_schema_registry`, `dedupe_before_merge`.

**8. The safety mechanisms themselves** — the kill switch and its state, the
remediation audit records, and the eligible-class allowlist. A system that can
disable its own brakes has no brakes.

**9. Anything outside the bronze layer.** Silver does not exist; `silver_layer/`
is a README and `_archive/`. Nothing autonomous reaches into a layer that has
no design yet.

### Bounded size, independent of surface

Every remediation action carries an explicit cap on rows and files touched,
per action **and** per run, refused rather than truncated when exceeded.

The precedent is `replay.py`'s `DEFAULT_MAX_REPLAY_ROWS = 500_000`, and its
docstring gives the reasoning better than a restatement would: replay is run
after fixing an upstream source, against a quarantine table that has been
accumulating since the problem started, so *"'replay everything' is the normal
usage and the unbounded case at the same time."* That argument applies here
with more force, because substituting an agent for the operator also removes
the person who would have noticed the size before pressing go.

---

## 3. Eligible fix classes

### Verdict: the eligible set starts empty

#215 classified the failure classes it examined into **(a) mechanically
fixable**, **(b) requires judgment**, and **(c) never automate**. The result
is the most important finding in this record:

- **Exactly two classes landed in (a)**, and both are logging or no-op cases:
  *deleted-row count unreadable* (`replay.py:38-48` — the bronze write has
  already committed, so the count is used only for logging) and *no quarantine
  table* (`replay.py:115-130` — the expected empty case, which correctly
  returns zeros). Neither remediates anything.
- **Every failure class that actually corrupts or loses data landed in (b) or
  (c).** Not one of them is mechanically fixable.

There is therefore **no fix class today that is both mechanically fixable and
an actual remediation**. The intersection is empty, and the eligible set is
its intersection.

Stating this plainly is the point. The alternative — assembling a
plausible-sounding eligible list so the feature looks shippable — would put
the machinery of autonomous remediation into the write path in exchange for
nothing, and would do it under a document that claimed otherwise.

**Rejected: ship with #215's two class-(a) items as the initial eligible set.**
This is the tempting move and it is the wrong one, for two reasons. First, it
builds the full apparatus — kill switch, rollback, fail-closed audit, Tier 2
promotion gate — to authorize changes that do nothing to the data. The cost is
real and the benefit is zero. Second, and worse: **the first entry in an
allowlist sets the standard for every later one.** Admitting two items because
they happened to be classifiable, rather than because they passed the gate
below, establishes that the gate is decorative. Every subsequent promotion
argument would begin "we already allowed those."

### The promotion gate

A fix class is added to the eligible set only when **all seven** hold. This
list, not this document's silence, is what makes a class eligible.

1. **#215-style classification places it in (a)** — a deterministic rule can
   state the fix without interpreting business meaning. If describing the fix
   requires the words "usually" or "probably", it is (b).
2. **It actually remediates.** It changes the data or the operational state in
   a way that resolves the failure. A logging change is not a fix class.
3. **It falls inside §2's ceiling**, and it names the specific subset it
   touches rather than inheriting the ceiling.
4. **A human operator already has a supported path for both the action and its
   inverse** (§2's governing rule).
5. **Its inverse is exercised by a test in the local, workspace-free suite.**
   This standard is not new: #64 declined to ship Unity Catalog tags precisely
   because *"an unverified implementation would silently report success while
   applying nothing, which is a worse outcome for a governance feature than
   not shipping it."* An unverified rollback is worse again — it reports
   success while leaving the damage in place.
6. **It is bounded** by an explicit per-action and per-run cap (§2).
7. **Promotion is recorded in this document and signed off by the Project
   Lead.** Promoting a fix class changes what an autonomous agent may do to
   production data, which is a Tier 2 action by any reading of
   `agent_governance.md`. It is not a code review.

### Rejected alternatives

**Rejected: confidence thresholds — auto-apply when the model's confidence
exceeds N.** A confidence score is the model's opinion about its own opinion.
It is not evidence, it is not reviewable after the fact, and it does not
degrade gracefully — the failure mode is a confidently wrong fix, which is the
exact case the threshold was supposed to exclude. This repo already has a
harder-won position on the same shape: `_ai_metadata` is **Advisory** because
there is no mechanism that converts model output into **Fact**. A threshold
would perform that conversion by fiat and call it rigor.

**Rejected: the AI generates a deterministic rule, and the rule is what runs.**
Superficially the best of both — the AI writes code, the code is inspectable.
It moves the problem rather than solving it. Unreviewed generated code in the
write path is a strictly *larger* blast radius than a fix drawn from a fixed
allowlist, because its surface is bounded by nothing except what the generator
happened to emit. It also defeats the purpose of an enumerated eligible set:
there would be no set.

**Rejected: advisory-with-auto-apply — propose the fix, apply it if nobody
objects within N minutes.** This is autonomous execution wearing a
human-in-the-loop costume, and it is worse than honest autonomy because it
launders the decision through an approval nobody gave. `agent_governance.md`
already settles this for Tier 2 sign-off in terms that transfer directly: an
action is approved *"when the Project Lead names the specific action ... not
when a PR sits open unreviewed."* Silence is not approval at 3am either.

**Rejected: adopting DQX or Lakeflow Declarative Pipelines to supply the fix
catalogue.** Not reopening `docs/buy_vs_build_2026-08.md`, and recording why
so it is not re-proposed. DLT's expectations *drop or fail* rows; they neither
retain nor re-promote, let alone remediate — it has no equivalent for any of
this. DQX cannot be constructed without an authenticated workspace, which was
already disqualifying for #109's rule engine; here it would make every safety
mechanism in §4–§6 untestable in the local suite, which is a stricter version
of the same objection applied to a much sharper feature.

---

## 4. Kill switch — a prerequisite to be built

### What exists today: nothing

#207 assumes a kill switch can be specified. It cannot yet be specified,
because there is nothing to configure. Verified against `dev`:

- **There is no mid-flight halt of any kind.** The notebook entry points read
  their configuration once at start from `dbutils.widgets`, and nothing in
  `pipeline.py` / `directory_ingestion.py` re-reads any external state during
  a run. A run's behaviour is fixed at the moment it starts.
- **A config change requires a full bundle deploy and never reaches a running
  job.** Every operational value arrives through `base_parameters` in
  `bronze_layer/resources/bronze_ingest_jobs.yml`; changing one means
  `databricks bundle deploy`, and it takes effect on the *next* run. Deploying
  to staging or prod is itself **Tier 2**. So "turn it off" today means: get
  the Project Lead's named sign-off, deploy a bundle, and wait for the current
  run to finish. That is not a kill switch; it is a change request.
- **There is no control table.** Control-table-driven dynamic config is item 4
  of `architecture.md`'s "What's left" table and is **not started**.
- The only run-terminating controls that exist are `timeout_seconds` (3600
  job / 3300 task) and cancelling the run in the Databricks UI.

**A real kill switch is therefore a prerequisite #209 must build, not a
property #209 may assume.**

### What it must guarantee

Guarantees, not a design. How to satisfy them is #209's work.

1. **Reachable without a deploy.** A switch that requires a Tier 2 bundle
   deploy to pull is not a switch — it inherits the exact latency and approval
   chain that makes the current situation unacceptable.
2. **Effective mid-flight, between actions.** Once set, no *further*
   remediation action starts. The bound must be a **stated** number of actions
   or seconds, decided and written down, not "promptly."
3. **Never effective mid-action.** It must not abort a remediation that has
   begun writing. A half-applied fix is worse than the fault it was fixing,
   and it is worse in a way that is hard to detect. It halts *between*
   actions, never inside one.
4. **Fails closed.** If the switch's state cannot be read — store unreachable,
   value malformed, permission changed — remediation stops. This deliberately
   **inverts** this package's existing convention for advisory surfaces:
   `_write_audit_row` never raises, and a failed AI call *"logs and skips that
   table."* Those fail open because their failure costs an observation. This
   one fails closed because its failure costs a write.
5. **Does not stop ingestion.** Deterministic ingestion continues with
   remediation disabled. If pulling the switch also stops data landing,
   operators will hesitate at the exact moment hesitation is most expensive —
   which makes the switch worse than useless. This is `architecture.md`'s
   two-lane isolation, restated for the new lane.
6. **Operable and visible under pressure.** Pullable by someone who is not the
   engineer who built it, and its state — off, by whom, when — visible without
   reading a driver log.
7. **Its own state changes are audited** (§6) and are on §2's NEVER list.
8. **Scoped per environment, and per fix class or per table.** A single global
   boolean means the only available response to one misbehaving fix class in
   dev is to disable everything, everywhere, including prod.

### Rejected alternatives

**Rejected: job cancellation / `max_concurrent_runs` as the kill switch.**
They exist, which is why they get proposed. They kill ingestion along with
remediation (violates 5), cannot be scoped per fix class (violates 8), and
leave whatever has already committed committed with no record of where the run
stopped (violates 3).

**Rejected: `enable_remediation: false` on `IngestionConfig` as the kill
switch.** Additive with a safe default, cheap, and consistent with the repo's
config conventions — it is the right *second* control, as a per-source opt-in,
and #209 should have it. It is **not** the switch, because it travels through
the same bundle-deploy path that created this problem. Both are needed; only
one of them is the emergency stop.

---

## 5. Rollback — a prerequisite to be built

### What exists today: Delta time travel, against a horizon nobody chose

Verified against `dev`:

- **The only undo for a committed bronze write is Delta time travel /
  `RESTORE`, and nothing in this package uses it.** `DeltaTable.history()` is
  read in exactly two places — `bronze_writer` and `replay._merge_deleted_count`
  — and both read it for *metrics*. There is no restore path anywhere.
- **No retention floor is set anywhere.** There is no
  `delta.deletedFileRetentionDuration` or `delta.logRetentionDuration` in any
  config, resource, or code path; `table_properties` defaults to `{}`. The
  time-travel horizon is therefore the **engine default — 7 days — a number
  nobody in this repo has ever chosen or reviewed.** #159 is open and is where
  that number is supposed to live; `docs/bronze_silver_contract.md` §1
  recommends 30 days and flags it as *"the one number in this document that
  cannot be derived from the code."*
- **The horizon is not merely unchosen — it is partly outside this repo's
  control.** Predictive optimization runs `OPTIMIZE`/`VACUUM` automatically on
  Unity-Catalog-managed tables where a workspace admin has enabled it, and
  `bronze_layer/README.md` records that this package deliberately does not
  manage that setting. So how long an undo remains possible is set by an
  account-level toggle in a different system.
- **`merge` retains no prior values.** `bronze_writer` merges with
  `.whenMatchedUpdateAll()`, and CDF is off by default (#58 open), so there is
  no `update_preimage`. An overwritten value exists **only** in the Delta
  transaction log — that is, only inside the unchosen, externally-controlled
  horizon above.
- **Replay is forward-only and cannot un-promote.** `reprocess_quarantine()`
  promotes quarantine → bronze and deletes from quarantine; there is no
  inverse operation. Its own docstring records that the bronze write commits
  *before* the quarantine delete, that *"Delta has no cross-table
  transactions, so exactly-once isn't achievable end-to-end"*, and that a
  partial failure leaves rows in both tables with recovery handled by a logged
  instruction to a human.

**So the reversibility of an autonomous fix currently depends on an engine
default nobody selected, a workspace-admin toggle this repo does not own, and
a feature that is switched off. A real rollback path is a prerequisite #209
must build.**

### What it must guarantee

1. **Every eligible fix class has a defined, named, tested inverse before it
   is promoted.** "Delta time travel exists" is not an inverse — it is a
   property of the storage layer that no fix class has claimed.
2. **Prior state is captured by the remediator itself, at the moment of the
   fix, not inferred afterwards from Delta history.** This is the guarantee
   that decouples undo from retention, from `VACUUM` scheduling, and from CDF
   — none of which this repo currently controls.
3. **No fix is applied unless its inverse is available at the moment of
   application.** Fail closed, same as §4.4: if prior state cannot be
   captured, the fix does not run.
4. **The rollback window is a stated number**, chosen and recorded here, not
   inherited from an engine default.
5. **Rollback is idempotent and safe to run twice** — the standard
   `reprocess_quarantine` already holds itself to.
6. **A batch of remediations is reversible as one operation**, addressable by
   remediation run identity. Undoing 400 individual fixes one at a time during
   an incident is not a rollback path; it is a second incident.

### Named prerequisites and their dependencies

| Prerequisite | What it blocks | Depends on |
| --- | --- | --- |
| A retention floor **chosen** and set on bronze and quarantine tables | Any remediation whose undo relies on Delta history at all | **#159** (owns the number and the maintenance job); `bronze_silver_contract.md` §1 recommends 30 days. Setting it is **Tier 2** |
| Prior-value capture for any in-place update | Any remediation touching an existing row | **#58** (CDF `update_preimage`) — or guarantee 2 above, which is the preferred route |
| A control channel reachable mid-flight | The kill switch (§4) entirely | `architecture.md` "What's left" item 4, control-table dynamic config — **not started** |
| An un-promote path, **or** a standing rule that remediation never promotes from quarantine | Any remediation crossing quarantine → bronze | Nothing exists; `replay.py` is forward-only by design, not by omission |

**Recommendation: satisfy prior-value capture via guarantee 2 rather than
waiting on #58.** Rejected alternative: *make #209 wait for #58 and read
`update_preimage`*. It is the obvious route and it is worse on two counts. It
couples an unrelated decision — #58 exists for Silver's incremental read — to
this one, dragging a Phase 5 item into a blocking position. And CDF history is
deleted by `VACUUM` exactly like everything else, so it would inherit the same
unchosen horizon it was brought in to work around. #58 remains valuable on its
own terms and its schedule should be decided on those terms.

---

## 6. Audit requirements

### A dedicated record, extending the Fact/Advisory split rather than breaking it

`architecture.md` separates three metadata tables by trust level:
`_ingestion_audit` and `_schema_registry` are **Fact**; `_ai_metadata` is
**Advisory**. A remediation record is neither — it is a record of an **action
taken**, and it needs its own home.

**Rejected: extend `AUDIT_SCHEMA` and reuse `_ingestion_audit`.** Considered
seriously, because the bronze/silver contract already plans one schema change
there (a `layer` column) and batching them would be consistent with how #149
and #156 were done together. Rejected on three grounds:

- `_ingestion_audit` is **one row per run**, and the run a remediation would
  attach to is the *ingestion* run, not the remediation. #156 already had to
  add `stream_batch_id` to make micro-batch rows individually addressable;
  overloading the table a second time repeats a known mistake.
- **`_write_audit_row` never raises, by design and by contract** — *"the audit
  trail must never fail the ingestion it records."* That is right for
  observing ingestion and exactly wrong for a record that authorizes a write
  (see below), and the contract is not overridable per row.
- #62's dashboard and #61's baselines are being built on this table's current
  meaning. #149 and #156 exist *because* that meaning drifted once already —
  `row_count` meaning something different per write mode is precisely what
  happens when one table is asked to mean two things.

### What every remediation record must guarantee

1. **Written before the fix is applied, never after.** An action that fails
   partway must still leave a record that it was attempted. A record written
   afterwards cannot, by construction, describe the failure that stopped it
   from being written.
2. **Fails closed.** If the record cannot be written, the fix does not run.
   This is the deliberate inverse of `_write_audit_row`'s contract, and the
   inversion is the whole point: **the audit trail must never fail the
   ingestion it observes; the remediation record must always fail the action
   it authorizes.**
3. **Sufficient to reconstruct the action and to reverse it.** At minimum: the
   target (table, plus the addressable identity of the affected rows or
   files), the fix class, the prior state, the new state, the bounds it was
   checked against, the kill-switch state observed at decision time, the model
   and prompt version that proposed it, the run identity, and the sign-off
   under which that fix class was promoted.
4. **Immutable, and never itself a remediation target** — §2, NEVER item 8.
5. **Joinable to `_ingestion_audit` by run identity.** An operator
   investigating a wrong value in a bronze table must be able to see, in one
   query, both what ingested and what subsequently changed it. Without this,
   a person debugging bad data has no way to discover that anything other than
   ingestion ever wrote to that table — which is the single worst property
   this feature could have, and the one that would make every future
   production investigation in this repo start from a false premise.
6. **Distinguishes proposed / applied / skipped / failed / rolled back.** A
   remediator that records only its successes conceals its own error rate,
   and the error rate is the number that decides whether a fix class stays
   eligible.
7. **Retained at least as long as the rollback window** (§5.4). A record you
   can read but can no longer act on is not an audit trail.

---

## 7. Relationship to decisions already recorded

Stated explicitly, so none of these is quietly assumed to have moved.

- **`bronze_layer/docs/architecture.md`** — this record is the documented
  **exception** to its one rule, of stated shape, not a repeal. The AI may act
  only within §2's ceiling, only on classes promoted through §3. Acceptance,
  rejection and quarantine of a row remain `quality.py`'s alone (§2, NEVER
  item 2). The amendment itself is **#218** and is not made here.
- **`docs/bronze_silver_contract.md`** — not reopened. Value correction is a
  transformation and belongs in Silver, which is why business column values
  are on the NEVER list. Bronze stays flattening-free and source-faithful.
- **`docs/buy_vs_build_2026-08.md`** — not reopened. Nothing here re-proposes
  DLT/Lakeflow or DQX; §3 records why each was considered and rejected *for
  this feature specifically*, so the question does not return as new.
- **`docs/agent_governance.md`** — unchanged and binding. A fix class whose
  application requires a Tier 2 or Tier 3 action is ineligible by
  construction, because governance binds by action rather than by role, and a
  runtime agent is not exempt from a rule a coding agent must follow.

---

## 8. Roadmap impact — flagged, not decided

`docs/roadmap.md` sequences #159 in **Phase 4** and #58 in **Phase 5**.
Neither #207 nor #209 appears in any phase; BR-001's issues were filed after
that document's last audit.

This record makes **#159 a prerequisite of #209** and puts **#58** in an
adjacent position (§5 recommends routing around it rather than through it).
That pulls a Phase 4 item ahead of a currently-unphased item.

**Re-sequencing the roadmap is the Project Lead's call, not this document's.**
It is raised here so the dependency is visible at sign-off rather than
discovered when #209 starts. Note also that #159's own verdict in
`buy_vs_build_2026-08.md` is *"do the measurement first; it is an afternoon
and it sets the urgency"* — so the blocking part of this dependency is small.

---

## What this unblocks, and what stays blocked

**Unblocked by this record:**

- **#218** — the `architecture.md` amendment can be drafted directly from §1,
  §2 and §7. It should quote the exception's bounds rather than paraphrase
  them, so the two documents cannot drift.
- **#208** — unaffected and never was affected. The async AI metadata job is
  advisory and is explicitly not gated on #207. It should proceed.

**Still blocked, with what would unblock it:**

- **#209** cannot deliver autonomous remediation of anything, because §3's
  eligible set is empty and no candidate class exists. What #209 *can*
  usefully build now is the harness: the kill switch (§4), the rollback path
  (§5), the fail-closed remediation record (§6), and the promotion gate (§3)
  — shipped with an empty eligible set and an integration point for the first
  class that earns one.
- **The first fix class** requires new evidence #215 did not find. Producing a
  class that is mechanically fixable *and* an actual remediation is its own
  piece of work, and it should be scoped as one rather than assumed to fall
  out of #209.
- **#159's retention decision** is a hard gate on any rollback that touches
  Delta history, and it is **Tier 2**.

## Follow-ups this surfaced

Neither is in scope for an open issue; both are worth filing if wanted.

1. **The absence of a mid-flight control channel is a general gap, not a
   remediation-specific one.** Nothing in this package can be told to change
   behaviour while running. That has been acceptable while every lane was
   deterministic and config-driven; it stops being acceptable the moment
   anything in the write path is non-deterministic. Control-table dynamic
   config (`architecture.md` "What's left" item 4) is where this lives and it
   has never been a numbered issue.
2. **`_write_audit_row`'s never-raise contract now has an exception in the
   same codebase**, and the two conventions sit one module apart. Worth a
   short note wherever the convention is documented, so the next person does
   not "fix" the fail-closed path into consistency with the fail-open one.
