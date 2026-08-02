# Buy-vs-build checkpoint — August 2026

A deliberate, documented decision on each framework feature still in the
backlog, for #163. **Not a recommendation to abandon the framework** — the
likely honest answer for several of these is "build anyway", and that is a
fine outcome provided it is written down with its reasoning, as every other
significant decision in this repo is.

Applies the repo's existing habit of recording rejected alternatives
(`architecture.md` on background threads vs scheduled jobs, on event
triggers, on per-file format inference) one level up: to the framework
itself.

---

## Verdicts

| Issue | Verdict | One-line reason |
| --- | --- | --- |
| **#109** silver rule engine | **Build** — revisit if Silver ever runs Databricks-only | DQX cannot be constructed without an authenticated workspace, so adopting it makes the entire local suite untestable |
| **#61** volume anomaly detection | **Build** (small) | It is a median over a column this package already owns. DQX has an `anomaly` module, but it inherits the same workspace constraint |
| **#62** ops dashboard + alerts | **Buy** (Lakeview + Databricks SQL alerts) | Already the plan. There is nothing to build but the SQL and the JSON |
| **#64** Unity Catalog TAGS | **Build**, keep `discoverx` in view | Tagging mechanics are ~50 lines against a proven diff-and-apply pattern; classification is the part worth buying, later |
| **#159** lifecycle / OPTIMIZE / VACUUM | **Hybrid** | Rely on predictive optimization where available; ship a maintenance job for retention and quarantine ageing, which nothing else will do |
| **#153** concurrency locking | **Bought already** | `max_concurrent_runs: 1` in the job definition covers the case that occurs |

---

## DQX — the finding that decides #109

#163 asked for this to be *"answered by running it against a real fixture,
not by reading the README"*. It was.

**Method.** `databricks-labs-dqx==0.15.0` installed against this repo's
PySpark 4.1.1 / Python 3.11 environment, run against a bronze-shaped fixture
(business columns plus `_ingested_at` / `_source_file` / `_batch_id`) with
one `error` rule and one `warn` rule.

**Result: DQX cannot be constructed without an authenticated Databricks
workspace.**

```
DQEngine.__init__(self, workspace_client: WorkspaceClient, spark=None, ...)
DQEngineCore.__init__(self, workspace_client: WorkspaceClient, spark=None, ...)
```

`workspace_client` is a *required positional* argument on both the engine and
its core, and it is not merely stored — a stub client that raises on
attribute access shows it is dereferenced during construction:

```
[core_ctor] FAILED: AssertionError: ws used: .clusters
```

So the failure is not "no credentials configured". It is that constructing
the checker performs a workspace API call, at every layer, before any
DataFrame is touched.

**Two smaller findings from the same run:**

- **DQX requires `pandas`.** The import fails without it. This package
  deliberately *eliminated* pandas in #157, as an undeclared dependency that
  worked only because the Databricks runtime happens to ship it. Adopting
  DQX reintroduces it — declared this time, but reintroduced.
- The library imports cleanly on OSS Spark. The blocker is specifically the
  workspace client, not Spark compatibility.

### Why that decides it

This repo's test suite is **322 tests that need no workspace**, and #74 was
spent making them runnable locally on top of that. Adopting DQX for #109
would mean the silver quality path — the layer whose entire job is deciding
whether data is correct — could not be covered by any test that runs in CI
or on a developer machine.

That is precisely the constraint **#64** documented for Unity Catalog tags,
and the reason tags were deliberately *not* shipped: *"an unverified
implementation would silently report success while applying nothing, which
is a worse outcome for a governance feature than not shipping it."* The same
argument applies with more force to a quality gate.

**Verdict: build #109.** Revisit if two things change together — Silver
becomes Databricks-only by policy, *and* someone is willing to fund
integration testing against a real workspace in CI (which is #113's OIDC
work plus a test catalog).

### What is still worth taking from DQX

Adopting the library is rejected; adopting its *design* is not. Three things
it gets right that #109 should copy rather than reinvent:

1. **Severity as a first-class rule attribute** (`error` / `warn`), not a
   global switch. #109 already proposes this.
2. **Per-rule result columns** describing which check failed, rather than a
   single boolean. This is #109's stated hard part, and DQX's shape is a
   reasonable model.
3. **Profiling to generate candidate rules from data.** Genuinely useful and
   entirely absent from #109's scope. Worth a follow-up issue rather than
   scope creep.

---

## Lakeflow Declarative Pipelines (formerly DLT)

**Verdict: not adopted. The differentiators still hold.**

DLT expectations overlap #109, #61, #62 and #159 simultaneously, which is
why #163 raises it. The honest counter-argument is the one that justified
building originally, and it has not weakened:

| This package does | DLT |
| --- | --- |
| Folder-as-table union ingestion | No equivalent |
| Per-file archival with a fallback chain (`processed/` → `quarantine_files/` → leave in place) | No equivalent |
| Retry-limit-before-quarantine **across runs**, persisted in `_state/` | No equivalent |
| Filename-derived table naming | No equivalent |
| Quarantine replay back into bronze | Expectations drop or fail; they do not retain and re-promote |

What *has* changed since the original decision is the size of the remaining
backlog, which is why re-asking was right. But the answer is unchanged, and
the reason is that DLT's model is "a pipeline defines tables"; this
package's model is "a directory defines tables, discovered at runtime". Those
are different products.

**One thing worth stealing:** DLT's automatic table maintenance is exactly
what #159 needs, and its existence is an argument for the hybrid verdict
below rather than for adopting DLT.

---

## The rest

### #62 — ops dashboard: **buy**

Nothing to build. AI/BI (Lakeview) dashboards and Databricks SQL alerts are
the product; the work is authoring SQL views over `_ingestion_audit` and
checking in a `.lvdash.json`. This was already the plan and the evaluation
does not change it.

Newly viable, and worth stating: #149 and #156 made the audit trail mean one
consistent thing. A dashboard built before that would have plotted
`row_count` values that were not comparable across write modes.

### #61 — volume anomaly detection: **build (small)**

A rolling median over `row_count` for the last N successful runs of a table,
compared against the current run. That is a window function over a table this
package already owns and populates.

DQX ships an `anomaly` module, and it inherits the workspace constraint
above. Adopting a library for one median is the wrong trade regardless.

**Depends on #62** — build the views once and let both read them.

### #64 — Unity Catalog TAGS: **build, keep `discoverx` in view**

Two separable halves:

- **Applying tags** — ~50 lines against `catalog_metadata.py`'s proven
  diff-and-apply pattern. Nothing to buy.
- **Deciding what to tag** (PII/semantic classification) — this is where
  [`databrickslabs/discoverx`](https://github.com/databrickslabs/discoverx)
  is genuinely interesting, and `architecture.md` currently proposes building
  PII detection from scratch.

**Verdict: build the mechanics; do not build a classifier.** When the AI
metadata layer reaches classification, evaluate discoverx *then*, as its own
decision. Note it will hit the same workspace-testability constraint.

### #159 — lifecycle: **hybrid**

| Part | Verdict |
| --- | --- |
| Bronze compaction | **Buy** — Databricks predictive optimization handles it *where enabled*. State that dependency rather than relying on it silently |
| VACUUM retention | **Build** — the floor must exceed the CDF consumer lag from `bronze_silver_contract.md` (recommended 30 days). No platform feature knows that number |
| Quarantine ageing | **Build** — rows that will never pass currently stay forever. Nothing else will do this |
| Audit-table file growth | **Build** — one row per run as its own commit; ~1M files/year on a 30-second stream |

### #153 — concurrency: **already bought**

`max_concurrent_runs: 1` with `queue.enabled` in the job definition closes
the case that actually occurs — a scheduled run outlasting its interval. The
library-level half (two direct callers racing on one `source_dir`) remains
open and is correctly scoped as a separate concern.

---

## `adidas/lakehouse-engine` reviewed against #150 / #151

#163 asked for this as a free design review from a codebase that solved the
same problem at scale. Reviewed after the fact, since both refactors have
landed.

Its structure separates *algorithms* (what to do) from *IO* (where data comes
from and goes) from the *ACON* configuration contract. The equivalent split
here — `pipeline` orchestration, `fs/*` IO, `IngestionConfig` — is the same
shape, arrived at independently.

**Two observations worth carrying:**

1. It keeps a **versioned config contract** with explicit compatibility
   rules. This package's `IngestionConfig` is deliberately lenient in
   `from_dict` and strict at entry points (#183's test pins that asymmetry),
   which is a reasonable smaller version of the same idea.
2. Its IO layer is organised by *source type* rather than by *operation*.
   `fs/` here is organised by operation (discovery / archival / retry-state).
   For a single-format package that is correct; if the multi-format work in
   `architecture.md` lands, revisit — it is the seam that would move.

No change recommended now. Recorded so the next refactor has a reference.

---

## What this unblocks

- **#109** may start once `bronze_silver_contract.md` §5 is confirmed. Its
  prerequisite is now "build, per this document" rather than an open question
- **#62** may start immediately; nothing gates it
- **#61** follows #62
- **#64**'s mechanics may start once #112 provides a workspace to verify
  against

## Follow-ups this surfaced

Neither is in scope for any open issue; both are worth filing if wanted:

1. **Rule profiling** — generating candidate quality rules from data, which
   DQX does and #109 does not propose
2. **A workspace-backed integration test lane** — the constraint that
   decided #109 and #64 is the *same* constraint. If it keeps deciding
   things, it becomes worth removing rather than working around, and that
   is #113's OIDC work plus a test catalog
