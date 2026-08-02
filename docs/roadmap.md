# Roadmap — where the package stands and what is left

Audit of all 15 open issues against `dev` @ `1744f5b`, July 2026. Every
"already done?" claim below was checked by reading the code, not by reading
the issue.

This is a living document: phases and ordering change as decisions are made.
The open GitHub issues are the source of truth for *what* is left; this
document is the source of truth for *what order* and *why*.

---

## Where we stand

The bronze layer's correctness work is essentially complete. In the last
wave the package closed every known silent-data-loss and silent-corruption
defect:

| Class | Issues | What it was |
| --- | --- | --- |
| Silent data loss | #146, #147 | `.jsonl` read as one record; the quality gate's split not being a partition of its input |
| Silent duplication | #148 | Quarantine keyed on `uuid()`, so a retry inserted beside the original |
| Meaningless audit data | #149, #156 | `row_count` meaning something different per write mode; streaming rewriting per-run metadata every micro-batch |
| Unsafe SQL construction | #154, #54 | Config values interpolated into `spark.sql()` unescaped and unvalidated |
| Structural | #150, #151, #183 | Three copies of the orchestration body; four modules in one file; two competing failure policies |
| Engineering hygiene | #158, #157, #74, #152, #155 | No lint/types/security/coverage in CI; untested notebook layer; Windows; retry discrimination; driver-collect at scale |

**Nothing in the open list is stale.** All 15 are genuinely outstanding.

### The one thing that is not on any issue

`dev` is **38 commits ahead of `staging`, and `staging` is identical to
`main`.** Every fix in the table above exists only on `dev`. Production is
running the code from before all of it.

That is the highest-value action available and it is not tracked anywhere,
which is exactly why it is Phase 0.

---

## Phase 0 — Promote what is already built

**Nothing new is worth building until the work that exists is running.**

1. `dev` → `staging`, deploy, smoke-test
2. `staging` → `main`, deploy to prod

Needs a consolidated release note, because the wave carries behaviour
changes that will look like breakages to whoever operates the job:

- `audit_schema_name` / `registry_schema_name` now default to `schema_name`
  instead of the literal `"bronze"` (#54)
- A streaming source reading `.jsonl` with `multiLine=true` now **fails**
  where it previously truncated silently (#146)
- Config load now rejects identifiers outside `[A-Za-z_][A-Za-z0-9_]*`, and
  `reader_options` keys outside the allowlist (#154)
- `retry_attempts` below 1, negative delays, and `streaming` + `overwrite`
  now raise at config load (#54)
- Quarantine rows written before this wave keep UUID `_quarantine_id`s and
  will not deduplicate against new content-hash ids (#148)

**Exit criteria:** prod runs a real ingestion on the new code, with correct
audit and registry rows.

---

## Phase 1 — Finish what is in flight

| Issue | Status |
| --- | --- |
| #183 — one failure → retry-count → quarantine policy | PR #184 open |

---

## Phase 2 — Decide before building

Two issues are **decisions, not tasks**. Both are cheap to answer now and
expensive to answer after the thing they govern has been built.

### #163 — Buy-vs-build checkpoint (DQX, Lakeflow Declarative Pipelines)

Gates **#109**, the single largest remaining item. If Databricks Labs DQX
covers the silver-layer rule engine, building one is a straight loss. Answer
this before #109 starts, not after.

### #162 — Define the Bronze→Silver contract

The medallion is one layer deep. Gates **#58** (Change Data Feed is listed
as a silver prerequisite — enabling it "by default" is only justified if
silver actually consumes it), **#109**, and the shape of anything downstream.

Building more bronze without the contract risks building the wrong bronze.

---

## Phase 3 — Provisioning, and the deploy path

A chain, in strict order. Everything here is Azure/Databricks work rather
than package work.

```
#112  Phase B provisioning (staging + prod grants, Key Vault, verification)
  ├── #113  CI/CD: deploy the bundle from GitHub Actions via OIDC
  ├── #115  Databricks secret scopes for source credentials
  └── #160  Per-environment Volumes
```

**#113 is the one with compounding value.** Phase 0 exists because
promotion is manual and drifted 38 commits; automated deployment is the fix
for the *cause*, not just this instance.

**#160 closes a gap no grant can close.** All three environments read
subpaths of one Volume, and Unity Catalog grants `READ VOLUME` at volume
granularity — there is no sub-path grant. Any principal that can read its
own subpath can read `PROD/Raw/`. Tables, audit and registry are isolated by
schema; source files are not. This is a naming convention presented as a
boundary.

---

## Phase 4 — Operational maturity

Bronze is now *correct*. This phase makes it *operable*.

| Issue | Why now |
| --- | --- |
| **#159** — lifecycle: OPTIMIZE, VACUUM, retention, small files | No table this package creates has any lifecycle policy. Directory ingestion writes one small file per source file, so the small-file problem grows linearly with every run. Costs compound silently |
| **#62** — observability: dashboard + SQL alerts over the audit table | **Newly worth doing.** #149, #156 and #148 just made the audit trail mean one consistent thing; before that, a dashboard would have visualised numbers that were not comparable across write modes |
| **#61** — volume anomaly detection from audit-trail baselines | Depends on #62's baseline work. Build after, not alongside |

---

## Phase 5 — Features and interoperability

Gated on the Phase 2 decisions and, for the Unity Catalog ones, on a real
workspace from Phase 3.

| Issue | Gate | Notes |
| --- | --- | --- |
| **#84** — sha2 row-hash as a merge-key strategy | none | **Cheapest item on the board.** `sql_utils.row_content_hash` already exists and is tested; it is currently used only as a window tie-break. This needs exporting and wiring as a `merge_keys` strategy |
| **#58** — Change Data Feed on by default | #162 | "By default" is a decision about what silver needs, not a bronze preference |
| **#109** — silver-layer business-rule quality engine | #163, #162 | The largest remaining item. Do not start before the buy-vs-build answer |
| **#64** — Unity Catalog TAGS | #112 | The COMMENT half shipped. Tags are Databricks-Runtime-only and raise `ParseException` on OSS Delta, so they cannot be validated by the local suite at all — this needs a real workspace or it ships unverified. The surviving item from discussion #98 |
| **#65** — Delta UniForm / Iceberg interop | — | **Recommend parking.** No stated consumer. Speculative until an external engine actually needs to read these tables |

---

## Suggested order

```
Phase 0   promote dev → staging → main            ← do first, blocks nothing
Phase 1   #183 (PR #184 open)
Phase 2   #163, #162                              ← decisions; cheap now
Phase 3   #112 → #113, #115, #160                 ← Azure work
Phase 4   #159, #62 → #61
Phase 5   #84 (quick), then #58 / #109 / #64 per Phase 2 and 3 outcomes
```

Phases 2 and 3 can run in parallel — one is a decision, the other is
provisioning, and neither blocks the other.
