# Roadmap — where the package stands and what is left

Audit of all **14 open issues** against `dev` @ `cdf2c69`, **2026-08-07**.
Every "already done?" claim below was checked by reading the code and the
tree, not by reading the issue. Supersedes the audit against `79fefbe`.

This is a living document: phases and ordering change as decisions are made.
The open GitHub issues are the source of truth for *what* is left; this
document is the source of truth for *what order* and *why*.

> **When those two disagree, say so rather than picking one.** This audit
> exists because they did: #162 and #163 shipped their deliverables on
> 2026-08-02 while both issues stayed open, so the stated tiebreak — "the
> open GitHub issues are the tiebreak" — pointed at the wrong answer for
> five days. Recommendations for closing them are in Phase 2.

---

## Where we stand

**The bronze layer's correctness work is complete and shipped.** Every known
silent-data-loss and silent-corruption defect is closed, and — as of
`#187` → `#189` — all of it is on `main`.

| Class | Issues | What it was |
| --- | --- | --- |
| Silent data loss | #146, #147 | `.jsonl` read as one record; the quality gate's split not being a partition of its input |
| Silent duplication | #148 | Quarantine keyed on `uuid()`, so a retry inserted beside the original |
| Meaningless audit data | #149, #156 | `row_count` meaning something different per write mode; streaming rewriting per-run metadata every micro-batch |
| Unsafe SQL construction | #154, #54 | Config values interpolated into `spark.sql()` unescaped and unvalidated |
| Destructive at scale | #155 | Replay collecting every id to the driver and building one giant `IN (...)` |
| Structural | #150, #151, #183 | Three copies of the orchestration body; four modules in one file; two competing failure policies |
| Engineering hygiene | #158, #157, #74, #152 | No lint/types/security/coverage in CI; untested notebook layer; Windows; retrying failures that can never succeed |

### Audit result: one to close, one to narrow

All 14 open issues were re-checked against the code. **Two have moved since
the last audit, and only because their deliverables merged — no issue's
underlying work was found to have been done quietly.**

| Issue | Change since `79fefbe` | Recommendation |
| --- | --- | --- |
| **#163** | `docs/buy_vs_build_2026-08.md` exists and answers every question the issue asked | **Close as delivered** — see Phase 2 |
| **#162** | `docs/bronze_silver_contract.md` exists; its own review checklist has **four unchecked boxes** | **Keep open, narrowed to the sign-offs** — see Phase 2 |

The other twelve are genuinely outstanding, verified absent in the tree at
`cdf2c69` rather than assumed:

| Issue | Verified absent |
| --- | --- |
| #58 | No `enable_change_data_feed` config field. `delta.enableChangeDataFeed` appears only as a **comment example** of a legitimately-dotted `table_properties` key (`bronze_layer/config/sample_config.yaml`, `order_bronze.yaml`) — the generic passthrough, not the feature |
| #61 | No `volume_check` config, no baseline logic |
| #62 | No `dashboards/` or `sql/` directory, no `.lvdash.json` |
| #64 | No `table_tags` / `column_tags` |
| #65 | No UniForm / `universalFormat` setting |
| #84 | No `merge_key_strategy` / `hash_columns`. `sql_utils.row_content_hash` exists and is used internally by `quality.py` and `bronze_writer.py`, but is **not exported** from `bronze_ingest/__init__.py` |
| #109 | No silver pipeline. `silver_layer/` is a README, `_archive/`, and — new since the last audit — `resources/silver_jobs.yml`, which declares `jobs: {}` on purpose (see Phase 2) |
| #113 | `.github/workflows/` contains `ci.yml` only. No deploy workflow, no OIDC reference anywhere |
| #115 | No secret resolution; nothing reads a Databricks secret scope |
| #159 | No maintenance job (`bronze_layer/resources/` holds `bronze_ingest_jobs.yml` alone), no retention config |
| #160 | Still one volume for three environments — all three targets in `databricks.yml` read subpaths of `/Volumes/ingredion_en/ingredion_dev/ext-ingredion-dev/` |
| #112 | Partial, unchanged — see below |

**#112 is the one partial.** Service principals are registered and Step 12
of `azure_setup.md` is written with six `GRANT` statements recorded — the
step's own heading still reads *"(partially done)"*. The issue's acceptance
criteria — a *verified-denied* cross-environment read, and a real file
ingested in each environment — are not met. It stays open, correctly.

---

## Phase 0 — Promote what is built ✅ **DONE**

`dev` had drifted 38 commits ahead of `staging`, with `staging` identical to
`main`: every correctness fix existed only on `dev` and production was
running the code from before all of it. Promoted via `#187` (dev → staging)
and `#189` (staging → main), with a consolidated 0.5.0 CHANGELOG covering
the behaviour changes that would otherwise look like breakages.

Worth keeping the lesson attached: this was the highest-value action
available and it was tracked on no issue. **#113 is the fix for the cause**,
not just this instance.

---

## Phase 1 — Finish what is in flight ✅ **DONE**

#183 (one failure → retry-count → quarantine policy) merged in PR #184.

**Both Dependabot PRs have landed.** Evidence in the tree rather than in the
PR list: `bronze_layer/pyproject.toml` declares
`requires = ["setuptools>=83.0.0", ...]`, which is #191's floor and the CVE
the `quality` job flagged; `.github/workflows/ci.yml` pins
`actions/checkout@v7`, `actions/setup-python@v7`, `actions/upload-artifact@v7`
and `actions/setup-java@v5.6.0`, which is #190's group bump. Nothing is
outstanding here.

---

## Phase 2 — Decide before building ✅ **BOTH DELIVERABLES SHIPPED**

Both issues were **decisions, not tasks**, and both were answered on
2026-08-02. The documents are in the tree and the rest of the repo already
treats them as settled: `bronze_layer/docs/architecture.md` calls
buy-vs-build *"resolved, not open"*, and `docs/business_requirements.md`
reconciles BR-001 against both.

### #163 — Buy-vs-build checkpoint ✅ **delivered**

**Deliverable: `docs/buy_vs_build_2026-08.md`.**

Every question the issue asked is answered, including the one it was most
specific about — that DQX be *"answered by running it against a real
fixture, not by reading the README"*. It was: `databricks-labs-dqx==0.15.0`
against this repo's PySpark 4.1.1 / Python 3.11 environment, with the
finding that `DQEngine` cannot be constructed without an authenticated
workspace. Verdicts are recorded per feature (#109 build, #61 build, #62
buy, #64 build-the-mechanics, #159 hybrid, #153 already bought), Lakeflow
Declarative Pipelines is recorded as **not adopted** with its reasoning, and
the `adidas/lakehouse-engine` design review the issue asked for is included.

There is no checklist, no pending confirmation, and no residue inside the
issue's own scope.

> **Recommendation: close #163 as delivered, deliverable at
> `docs/buy_vs_build_2026-08.md`.**

The document surfaced **two follow-ups that are new scope, not #163's**, and
holding the issue open for them would misrepresent what is undecided:
> 1. **Rule profiling** — generating candidate quality rules from data.
>    Something DQX does and #109 does not propose.
> 2. **A workspace-backed integration test lane** — the constraint that
>    decided both #109 and #64 is the *same* constraint. If it keeps
>    deciding things it becomes worth removing, and that is #113's OIDC work
>    plus a test catalog.
>
> Both should reach `business-analyst` as candidates before they become
> issues, not be filed off this document.

### #162 — Bronze→Silver contract ⚠️ **document delivered, decisions not signed off**

**Deliverable: `docs/bronze_silver_contract.md`**, plus a bundle fix the
issue turned up on the way: `databricks.yml`'s
`include: silver_layer/resources/*.yml` was commented out because an include
pattern matching zero files is a bundle error, so the layer was **not
deployable even in principle**. `silver_layer/resources/silver_jobs.yml` now
exists declaring `jobs: {}`, and the include is live.

The document decides six things and states its rejected alternatives. What
it does **not** have is agreement. Its own review checklist carries **four
unchecked boxes**, and until those are answered the contract is a proposal,
not a contract:

| # | Unsigned decision | What it gates |
| --- | --- | --- |
| §1 | **The 30-day CDF `VACUUM` retention floor** — the one number in the document that cannot be derived from the code | #58, and the number #159 has nowhere to put |
| §2 | **`overwrite` out of contract** for any bronze table Silver consumes | #58's config-load rule |
| §5 | **Silver reimplements rather than shares the quality engine** (contingent on #163, now concluded: build) | #109 |
| §6 | **One audit table with a `layer` column** rather than per-layer tables | #62's tiles, and `AUDIT_SCHEMA` |

> **Recommendation: keep #162 open. Remaining scope is the four sign-offs
> above — nothing else.** No further writing, no code.

The reasoning for not closing it: this repo's tiebreak is that open issues
win over documents. Closing #162 would make that tiebreak assert the
contract's decisions are settled when four of them are not — the same class
of error, in the opposite direction, that this audit exists to fix. Narrowing
the issue's scope to the checklist costs nothing and keeps the thread of
reasoning attached to it.

*Alternative considered and rejected:* close #162 and file a fresh sign-off
issue. It reads tidier, costs an issue round-trip, and detaches four
decisions from the document that argues them.

### What is left of Phase 2, and where it now lives

The residue is **four yes/no answers, not a phase of work**, so it does not
get a phase of its own. Each sign-off is folded into the phase item it
actually gates — see Phases 4 and 5 below.

Two of them are **Tier 2** under `docs/agent_governance.md`: enabling Change
Data Feed and setting `VACUUM` retention are named there as decisions with a
stated irreversible-history consequence, needing the Project Lead's
explicit, named sign-off *before* execution. No agent settles §1 or §2. §5
and §6 are design confirmations and can be settled by
`principal-data-engineer`.

---

## Phase 3 — Provisioning, and the deploy path

A chain, in strict order. Azure/Databricks work rather than package work,
and the only phase that needs a real workspace. **Unblocked by nothing in
Phase 2** — it never was, and it can start today.

```
#112  Phase B provisioning (grants, verification, Key Vault)   ← partially done
  ├── #113  CI/CD: deploy the bundle from GitHub Actions via OIDC
  ├── #115  Databricks secret scopes for source credentials
  └── #160  Per-environment Volumes
```

**#113 has compounding value.** Phase 0 existed because promotion is manual
and drifted 38 commits. Automated deployment fixes the cause. It is also
half of `buy_vs_build`'s second follow-up — a workspace-backed test lane is
#113's OIDC work plus a test catalog.

**#160 closes a gap no grant can close.** All three environments read
subpaths of one Volume (`ext-ingredion-dev`, which lives in the *dev*
schema), and Unity Catalog grants `READ VOLUME` at *volume* granularity —
there is no sub-path grant. Any principal that can read its own subpath can
read `PROD/Raw/`. Tables, audit and registry are isolated by schema; source
files are isolated by nothing. Re-verified in `databricks.yml` at `cdf2c69`;
`databricks.yml`'s own header records it as a known gap. Cheap now,
expensive once prod holds real data.

> **Timing note.** #112 flags that the Azure trial credit or 30-day window
> may bind. If that clock is real, this phase jumps the queue — everything
> else in this document can be done later; a lapsed workspace cannot.

---

## Phase 4 — Operational maturity

Bronze is now *correct*. This phase makes it *operable*.

| Issue | Gate | Why now |
| --- | --- | --- |
| **#159** — lifecycle: OPTIMIZE, VACUUM, retention, small files | **contract §1 sign-off** for the retention number | No table this package creates has any lifecycle policy. The audit table writes one row as its own commit — ~18k tiny files/year for a daily 50-unit run, ~1M/year for a 30-second stream. Costs compound silently. `buy_vs_build` verdicts this **hybrid**: buy compaction (predictive optimization *where enabled*, stated as a dependency rather than assumed), build VACUUM retention, quarantine ageing and audit-table growth. Do the **measurement** first; it is an afternoon and it sets the urgency |
| **#62** — dashboard + SQL alerts over the audit table | **contract §6 sign-off** | **Newly viable, and verdicted buy** — Lakeview + Databricks SQL alerts, so the work is SQL views and a `.lvdash.json`, not a build. #149 and #156 made the audit trail mean one consistent thing; before that a dashboard would have plotted numbers not comparable across write modes. Its tiles should read `table_name`, `row_count`, `write_mode` and `stream_batch_id` as they now exist — and, if §6 is signed, carry a `layer` filter from day one. Deciding §6 *after* the dashboard exists means rebuilding it |
| **#61** — volume anomaly detection from audit baselines | #62 | Verdicted **build (small)** — a rolling median over `row_count`, which is a window function over a table this package already owns. Its median is only meaningful on the corrected `row_count` |

---

## Phase 5 — Features and interoperability

| Issue | Gate | Notes |
| --- | --- | --- |
| **#84** — sha2 row-hash as a merge-key strategy | none | **Cheapest item on the board.** `sql_utils.row_content_hash` already exists, is tested, and is used as a window tie-break; this is exporting it and wiring a config strategy |
| **#58** — Change Data Feed on by default | **contract §1 + §2 sign-off (Tier 2)**, #159 | **The only item whose value strictly decays** — CDF captures nothing retroactively, so every day it is off is history no silver job can ever read incrementally. Ship it *with* #159's retention floor: enabling a feature whose data VACUUM deletes creates a guarantee that looks real and is not. Also carries §2's config rule — reject `enable_change_data_feed` together with `write_mode: overwrite` at config load, in the pattern #54 established |
| **#109** — silver-layer business-rule quality engine | **contract §5 sign-off** | Largest remaining item. Its old prerequisite — *"Silver has a real pipeline"* — was unfalsifiable; the contract replaced it with two checkable ones. **#163 is now concluded (build), so §5 is the only one left.** Do not treat `silver_layer/_archive/flattener.py` as a starting point wholesale: `apply_flatten_mode` is dead (its four config fields were removed in #76 — grep at `cdf2c69` finds them only in a comment and a doc), while `flatten_dataframe` is a pure function and survives |
| **#64** — Unity Catalog TAGS | #112 | The COMMENT half shipped. Verdicted **build the mechanics, do not build a classifier** — `discoverx` stays in view for PII/semantic classification when the AI metadata layer reaches it. Tags are DBR-only and raise `ParseException` on OSS Delta, so the local suite cannot validate them — needs a real workspace or it ships unverified. Also needs `APPLY TAG` adding to #112's grant list |
| **#65** — Delta UniForm / Iceberg interop | — | **Recommend parking.** No stated consumer. Speculative until an external engine needs to read these tables |

---

## Not sequenced here

Two workstreams landed on `dev` since the `79fefbe` audit and are
deliberately **outside** this document's phase numbering. Recorded so a
reader does not conclude they were missed:

- **Agent governance and the docs split** (PRs #197–#200 and the Project
  Lead rename) — `AGENTS.md`, `docs/agent_governance.md`,
  `docs/private_agent_architecture.md`, `docs/overview.md`,
  `.claude/agents/`, `scripts/bootstrap_agents.sh`. Process and
  documentation, not package work; nothing here gates or is gated by it.
- **`docs/business_requirements.md`** — business-case intake, which sits
  **upstream** of this document. BR-001 is *Under review*, not
  *Approved → issues filed*, so it has no issues and therefore no place in
  the ordering. It will earn one if and when it does. Note that BR-001 names
  Delta Live Tables and Power BI/Tableau, both of which touch verdicts
  already recorded in `docs/buy_vs_build_2026-08.md` — reopening either is a
  stated decision, not a detail.

---

## Carried findings — not yet issues

Small items surfaced during the correctness wave that have no home. All
re-checked at `cdf2c69`. None is urgent; each will otherwise be
rediscovered.

| Finding | State at `cdf2c69` | Origin |
| --- | --- | --- |
| `databricks bundle validate` runs in CI but is **non-blocking** (`continue-on-error: true`, `ci.yml`). Dropping it is the workflow's own stated intended end state, not a change of direction | **Still true. Worth promoting to an issue**, with "N consecutive green runs" as the acceptance criterion rather than a judgement call | #157 |
| Coverage is reported, not enforced — no `fail_under` in `pyproject.toml`, no `--cov-fail-under` in `ci.yml`. A floor can now be set from evidence rather than guessed | **Still true. Worth promoting**, paired with the row above as one CI-hardening issue. Set the floor from an observed run — this document deliberately quotes no percentage, per `docs/README.md`'s note on stale numbers | #158 |
| `python_requires>=3.8` (`setup.py`) pins `ruff target-version = "py38"`, blocking PEP 604/585 annotations — while `mypy` in the same `pyproject.toml` is configured at `python_version = "3.11"` and CI runs 3.11. Three declared floors, three values | **Still true. Worth promoting** — but the work is one decision ("what is the real supported floor?"), then making `setup.py`, ruff and mypy agree, in that order. `pyproject.toml` already documents the required order of operations | #158, #74 |
| Whether Python 3.14 works now that `PYSPARK_PYTHON` is set is **untested** — #74's claim that 3.14 is incompatible was disproven, but 3.11 is what is verified | Still true. **Now recorded** in `CONTRIBUTING.md`, so it will not be rediscovered. Fold into the Python-floor issue above rather than tracking separately | #74 |
| Local Spark on Windows starts intermittently even with all five prerequisites set; a re-run clears it. Cause unknown | Still true, still unexplained. **Now recorded** in `CONTRIBUTING.md` as a known limitation. Keep as a finding; do not promote until someone can reproduce it deterministically | #74 |
| The README retry-safety matrix still does not cover the **quarantine** write, though the MERGE-on-content-hash behaviour has shipped | Still true — the matrix's four rows are all bronze write modes; quarantine idempotency is documented well, but elsewhere in the file. **Do not promote**; fold into the next pass that touches `bronze_layer/README.md` | #148 |
| `directory_ingestion.py` is 498 lines against #151's ~300 target. The duplication it was really about is gone; the number needs a *reason* before chasing it | Re-counted: **still exactly 498**. **Do not promote** — the finding's own point stands | #151, #183 |
| `docs/README.md`'s ownership index does not list `docs/bronze_silver_contract.md` or `docs/buy_vs_build_2026-08.md`, though both are living documents that `architecture.md`, `AGENTS.md`, `overview.md` and this file all defer to | New this audit. The index is the mechanism for "which document do I believe" — two authoritative documents missing from it is the gap that mechanism exists to close | this audit |
| `docs/bronze_silver_contract.md` §3 states that **"no test asserts"** the `replay-` `_batch_id` prefix, and recommends promoting it to a contract by asserting it. `bronze_layer/tests/test_replay.py:80` already asserts `result["replay_batch_id"].startswith("replay-")` | New this audit. The assertion exists but sits inside a broader mixed-outcome test rather than a named contract test — so the recommendation is *narrower* than the document thinks, not obsolete | this audit |
| `AGENTS.md` § "What to work on" still describes open work as *"starting with two decisions that need no workspace (#163, #162)"* | New this audit. Stale as of this audit — both decisions shipped. `AGENTS.md` points readers at this document, so the two should agree | this audit |

---

## Suggested order

```
Phase 0   promote dev → staging → main                    ✅ done
Phase 1   #183, #190 / #191 dependabot                    ✅ done

Phase 2   #163  buy-vs-build                              ✅ delivered → close
          #162  bronze→silver contract                    ⚠️ document delivered
                └─ 4 sign-offs outstanding (§1 §2 §5 §6); §1/§2 are Tier 2
                   and fold into Phases 4-5 below rather than blocking a phase

Phase 3   #112 → #113, #115, #160   ← Azure; jumps the queue if trial expiring
Phase 4   #159 (measure first, gated on §1), then #62 (gated on §6) → #61
Phase 5   #84 (quick win), then #58 (§1 + §2) / #109 (§5) / #64 (per #112)
```

Phase 3 needs a workspace and nothing else; the outstanding sign-offs need
no workspace and no engineering time. They run in parallel — neither blocks
the other.

**If only one thing happens next: sign off the four boxes in
`docs/bronze_silver_contract.md`.** It is four yes/no answers, it needs no
workspace, and it unblocks #58 — the single item that gets more expensive
every day it waits, because Change Data Feed captures nothing
retroactively. The reasoning is unchanged from the last audit; only the
shape of the remaining work is.
