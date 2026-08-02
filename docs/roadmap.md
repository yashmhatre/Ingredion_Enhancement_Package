# Roadmap — where the package stands and what is left

Audit of all **14 open issues** against `dev` @ `79fefbe`, August 2026. Every
"already done?" claim below was checked by reading the code, not by reading
the issue.

This is a living document: phases and ordering change as decisions are made.
The open GitHub issues are the source of truth for *what* is left; this
document is the source of truth for *what order* and *why*.

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

### Audit result: nothing to close

All 14 open issues were checked against the code. **None is already
resolved** — every one is genuinely outstanding. Specifically verified as
absent: `enable_change_data_feed` (#58), any `volume_check` config (#61),
`dashboards/` or `sql/` (#62), `table_tags`/`column_tags` (#64), any
UniForm/`universalFormat` setting (#65), `merge_key_strategy`/`hash_columns`
(#84), any silver pipeline (#109 — `silver_layer/` is still a README plus
`_archive/`), any deploy workflow or OIDC reference (#113), any secret
resolution (#115), any maintenance job or retention config (#159),
`docs/bronze_silver_contract.md` (#162), and any buy-vs-build evaluation
(#163).

**#112 is the one partial.** Service principals are registered and Step 12
of `azure_setup.md` is written with six `GRANT` statements recorded, but the
issue's own acceptance criteria — a *verified-denied* cross-environment
read, and a real file ingested in each environment — are not met. It stays
open, correctly.

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

Two Dependabot PRs are open and are pure housekeeping: **#190** (5 GitHub
Actions) and **#191** (setuptools floor → 83.0.0, which is the CVE the
`quality` job already flagged). Merge or dismiss; neither blocks anything.

---

## Phase 2 — Decide before building

Two issues are **decisions, not tasks**. Both are cheap to answer now and
expensive to answer after the thing they govern has been built. Neither
needs a workspace, so both can run in parallel with Phase 3.

### #163 — Buy-vs-build checkpoint (DQX, Lakeflow Declarative Pipelines)

Gates **#109**, the largest remaining item, and touches #61, #62, #64, #159.
If Databricks Labs DQX covers the silver rule engine, building one is a
straight loss. Answer before #109 starts.

The issue is explicit that "build anyway" is a fine outcome — the point is
that it be written down with its reasoning, as every other significant
decision in this repo is.

### #162 — Define the Bronze→Silver contract

The medallion is one layer deep, and "that belongs in Silver" has been
deferring to something with no design, no owner and no schedule. Gates
**#58** and **#109**, and settles the retention number #159 needs.

**This is the unblocking move for the only item whose value decays** — see
#58 below.

---

## Phase 3 — Provisioning, and the deploy path

A chain, in strict order. Azure/Databricks work rather than package work,
and the only phase that needs a real workspace.

```
#112  Phase B provisioning (grants, verification, Key Vault)   ← partially done
  ├── #113  CI/CD: deploy the bundle from GitHub Actions via OIDC
  ├── #115  Databricks secret scopes for source credentials
  └── #160  Per-environment Volumes
```

**#113 has compounding value.** Phase 0 existed because promotion is manual
and drifted 38 commits. Automated deployment fixes the cause.

**#160 closes a gap no grant can close.** All three environments read
subpaths of one Volume (`ext-ingredion-dev`), and Unity Catalog grants
`READ VOLUME` at *volume* granularity — there is no sub-path grant. Any
principal that can read its own subpath can read `PROD/Raw/`. Tables, audit
and registry are isolated by schema; source files are isolated by nothing.
Verified still true in `databricks.yml`. Cheap now, expensive once prod
holds real data.

> **Timing note.** #112 flags that the Azure trial credit or 30-day window
> may bind. If that clock is real, this phase jumps the queue — everything
> else in this document can be done later; a lapsed workspace cannot.

---

## Phase 4 — Operational maturity

Bronze is now *correct*. This phase makes it *operable*.

| Issue | Why now |
| --- | --- |
| **#159** — lifecycle: OPTIMIZE, VACUUM, retention, small files | No table this package creates has any lifecycle policy. The audit table writes one row as its own commit — ~18k tiny files/year for a daily 50-unit run, ~1M/year for a 30-second stream. Costs compound silently. Do the **measurement** first; it is an afternoon and it sets the urgency |
| **#62** — dashboard + SQL alerts over the audit table | **Newly viable.** #149 and #156 made the audit trail mean one consistent thing; before that a dashboard would have plotted numbers not comparable across write modes. Its tiles should read `table_name`, `row_count`, `write_mode` and `stream_batch_id` as they now exist |
| **#61** — volume anomaly detection from audit baselines | After #62. Its rolling median is only meaningful on the corrected `row_count` |

---

## Phase 5 — Features and interoperability

| Issue | Gate | Notes |
| --- | --- | --- |
| **#84** — sha2 row-hash as a merge-key strategy | none | **Cheapest item on the board.** `sql_utils.row_content_hash` already exists, is tested, and is used as a window tie-break; this is exporting it and wiring a config strategy |
| **#58** — Change Data Feed on by default | #162, #159 | **The only item whose value strictly decays** — CDF captures nothing retroactively, so every day it is off is history no silver job can ever read incrementally. Ship it *with* #159's retention floor: enabling a feature whose data VACUUM deletes creates a guarantee that looks real and is not |
| **#109** — silver-layer business-rule quality engine | #163, #162 | Largest remaining item. Do not start before the buy-vs-build answer |
| **#64** — Unity Catalog TAGS | #112 | The COMMENT half shipped. Tags are DBR-only and raise `ParseException` on OSS Delta, so the local suite cannot validate them — needs a real workspace or it ships unverified. Also needs `APPLY TAG` adding to #112's grant list |
| **#65** — Delta UniForm / Iceberg interop | — | **Recommend parking.** No stated consumer. Speculative until an external engine needs to read these tables |

---

## Carried findings — not yet issues

Small items surfaced during the correctness wave that have no home. None is
urgent; all are cheap, and each will otherwise be rediscovered.

| Finding | Origin |
| --- | --- |
| `databricks bundle validate` runs in CI but is **non-blocking**. It has passed on every run since. Drop `continue-on-error` and make it a real gate | #157 |
| Coverage is reported, not enforced. The number is now known and stable (**~86.5%**), so a floor can be set from evidence rather than guessed | #158 |
| `python_requires>=3.8` pins `ruff target-version` to `py38`, which blocks PEP 604/585 annotations. If the real floor is the Databricks runtime's Python, raising it unlocks ~83 modernisations | #158 |
| The README retry-safety matrix still does not cover the **quarantine** write, though the MERGE-on-content-hash behaviour has shipped | #148 |
| Local Spark on Windows starts intermittently even with all five prerequisites set; a re-run clears it. Cause unknown | #74 |
| Whether Python 3.14 works now that `PYSPARK_PYTHON` is set is **untested** — #74's claim that 3.14 is incompatible was disproven, but 3.11 is what is verified | #74 |
| `directory_ingestion.py` is 498 lines against #151's ~300 target. The duplication it was really about is gone; the number needs a *reason* before chasing it | #151, #183 |

---

## Suggested order

```
Phase 0   promote dev → staging → main                    ✅ done
Phase 1   #183                                            ✅ done
          #190 / #191 dependabot                          ← housekeeping, 5 min

Phase 2   #163, #162            ← decisions; no workspace needed
Phase 3   #112 → #113, #115, #160   ← Azure; jumps the queue if trial expiring
Phase 4   #159, then #62 → #61
Phase 5   #84 (quick win), then #58 / #109 / #64 per Phase 2 and 3 outcomes
```

Phases 2 and 3 run in parallel — one is a decision, the other is
provisioning, and neither blocks the other.

**If only one thing happens next: #162.** It is a document, it needs no
workspace, and it unblocks #58 — the single item that gets more expensive
every day it waits.
