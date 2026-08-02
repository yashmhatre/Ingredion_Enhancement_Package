# The Bronze → Silver contract

What Bronze promises Silver, and what Silver must be built to expect.

Written against `dev` @ `79fefbe`, August 2026, for #162. In the style of
`bronze_layer/docs/architecture.md`: decisions with their reasoning, and the
alternatives that were rejected and why.

---

## Why this exists

The repository presents a medallion architecture. What exists is
**~2,700 lines of tested, deployed bronze**, a `silver_layer/` holding a
README and an archived flattener, and no `gold_layer/` at all.

That sequencing is right — building bronze properly first was the correct
call. But it has a consequence worth naming, because it is now shaping
decisions:

- **#76** archived `flattener.py` out of bronze: *"business-friendly
  transformation reserved for a future Silver layer"*
- **#109** split five of seven quality rules out of bronze, and is explicitly
  blocked on *"Silver having a real pipeline"*
- **#58** proposes enabling Change Data Feed *specifically so Silver can read
  incrementally later*
- `quality.py`'s module docstring encodes the same layer rule

Each is a good decision, made for a good reason, applied consistently.
Together they mean **the capability the framework defers to has no design,
no owner and no schedule** — and the accumulated assumptions have never been
checked against each other.

The risk is not that Silver is late. It is that **the contract is being
defined by a series of independent "not here" decisions rather than by a
design.** This document converts those into something reviewable.

**It does not propose building Silver.** It decides what Silver will be
handed.

---

## 1. Read mechanism

**Decision: Silver reads Bronze incrementally via Delta Change Data Feed.**

```python
(spark.readStream.format("delta")
    .option("readChangeFeed", "true")
    .option("startingVersion", <last committed version>)
    .table("ingredion_en.ingredion_prd.orders_raw"))
```

Three things follow, and all three are obligations on **Bronze**, not
Silver:

| Obligation | Where it lands |
| --- | --- |
| CDF enabled on bronze **and quarantine** tables | #58 |
| A `VACUUM` retention floor exceeding the slowest consumer's lag | #159 |
| A durable place for Silver to record its read position | Silver's own design |

**CDF only captures changes from the moment it is enabled.** This is the
single fact that makes #58 time-sensitive: every day it is off is a day of
history no future Silver job can read incrementally. That is not a reason to
enable it carelessly — see the retention decision below — but it is a reason
not to defer it indefinitely.

**Rejected: full rescan of bronze on each Silver run.** Correct, simple, and
its cost grows with table size forever rather than with change volume. The
package already argues this in the other direction for Auto Loader; the same
reasoning applies one layer up.

**Rejected: Silver reads the `_ingestion_audit` table to find what changed.**
The audit trail records *runs*, not *rows*. It can tell Silver that a run
touched a table; it cannot tell it which rows. Using it this way would make
an observability surface load-bearing for correctness.

### The retention floor — needs a decision, not a default

⚠️ **This is the one number in this document that cannot be derived from the
code, and it must be decided before #58 ships.**

`VACUUM` deletes CDF history. A Silver job reading `readChangeFeed` from a
starting version **silently loses history** if `VACUUM` removed it first — no
error, just missing changes.

The floor must exceed *the maximum lag of the slowest CDF consumer*. With no
Silver job in existence, that lag is unknown. Options:

| Floor | Buys | Costs |
| --- | --- | --- |
| 7 days (168h, Delta's default) | Nothing extra | A Silver outage over a long weekend loses data |
| **30 days (720h)** ← recommended starting point | Tolerates a multi-week outage, a holiday period, or a Silver rebuild | Storage for 30 days of change history on bronze |
| 90 days | Tolerates almost anything | Materially more storage; probably solving a problem nobody has |

**Recommendation: 30 days**, on the grounds that the realistic failure is
not a one-hour blip but "Silver was broken and nobody noticed until someone
came back from leave". Confirm or override before #58 lands, and record the
number in #159 — which currently has nowhere for it to live.

---

## 2. What Silver sees on a re-run

Bronze supports `append`, `overwrite` and `merge`. They do **not** produce
equivalent change feeds.

| Bronze `write_mode` | What CDF emits | Silver impact |
| --- | --- | --- |
| `append` | `insert` rows only | Trivial |
| `merge` | `update_preimage` / `update_postimage` / `insert` | Normal CDC handling |
| `overwrite` | **The entire table as `delete` rows, then the entire table as `insert` rows** | A full-table churn event on every run |

**Decision: `overwrite` is OUT OF CONTRACT for any bronze table Silver
consumes.**

The reasoning is that `overwrite` makes the change feed meaningless as an
*incremental* mechanism: a daily-overwritten 10M-row table emits 20M change
rows a day regardless of how much data actually changed. Silver would do
strictly more work than a full rescan, while carrying all of CDC's
complexity.

This is a **restriction on configuration**, not a code change — but it needs
enforcing, or it will be violated by someone configuring a table reasonably
and having no way to know. Suggested: when #58 lands, refuse
`enable_change_data_feed` together with `write_mode: overwrite` at config
load, with a message pointing here. That is one more rule in the pattern
#54 established.

`overwrite` remains entirely valid for bronze tables Silver does **not**
consume — full-refresh reference data, for instance.

---

## 3. Replay semantics

`reprocess_quarantine()` promotes previously-rejected rows into bronze
**later**, with a fresh `_ingested_at` and a `_batch_id` of the form
`replay-<timestamp>`, while preserving the original `_source_file`.

To a Silver job reading CDF, these appear as **ordinary inserts arriving out
of order relative to source event time**. A row quarantined in January and
replayed in August arrives in August's change feed.

**Decision: late arrival is normal and Silver must be built to expect it.**
Bronze will not hold rows back to preserve ordering — that would mean bronze
deciding what "late" means, which is a business question and therefore
Silver's.

**The signal is currently an incidental string convention.**
`_batch_id LIKE 'replay-%'` is how a consumer can tell. That works and it is
fragile: nothing enforces the prefix, no test asserts it, and it is a
formatting detail of one f-string.

**Recommendation:** promote it to a contract by asserting it in a test, and
state the prefix here as the documented signal. A dedicated boolean column
on bronze rows was considered and rejected — it would add a column to every
row of every table to describe a property of a small minority, and the
`_batch_id` already carries it.

⚠️ **Open, deliberately:** whether Silver should treat a replayed row as a
*correction* of something it already emitted, or as a new fact. That depends
on what Silver computes and cannot be decided from bronze.

---

## 4. Nested structures — and the flattener does not fit

Bronze preserves nested JSON by design (#76). Silver flattens. The archived
`silver_layer/_archive/flattener.py` is the intended starting point.

**Revalidation result: it does not currently work, and would fail
immediately.** Checked rather than assumed:

| Problem | Detail |
| --- | --- |
| Dead entry point | `apply_flatten_mode(df, config)` reads `config.flatten_mode`, `config.flatten_separator`, `config.max_flatten_depth` and `config.auto_flatten_threshold`. **All four were removed from `IngestionConfig` in #76** — grep count is zero for each |
| Unresolvable import | `from .config import IngestionConfig` is a relative import into `bronze_ingest`, which does not resolve from `silver_layer/_archive/` |
| Columns it has never seen | Bronze output now carries `_ingested_at`, `_source_file`, `_batch_id`, `_rescued_data`, `_corrupt_record`, and on the quarantine path `_quarantine_id`, `_quarantine_reason`, `_occurrence_count`, `_first_quarantined_at`. None existed when it was archived. It would flatten them alongside business columns |

**What survives is worth keeping.** `flatten_dataframe(df, separator,
explode_arrays, max_depth)` is a pure function over a DataFrame with no
config dependency, and its tests came with it. That is the reusable part.

**Decision: treat `flatten_dataframe` as the starting point and
`apply_flatten_mode` as dead.** When Silver is built, the config-binding
layer is rewritten against Silver's own config, and the audit/rescue columns
are excluded from flattening explicitly rather than by accident.

---

## 5. Which quality rules live where

#109's table is the design and stands: structural checks (`not_null`,
`unique`) are Bronze; business-rule checks (`in_range`, `regex`, `in_set`,
`expression`) are Silver. The test is *"does evaluating this rule require
business knowledge, or just structural integrity?"*

**The open question was the mechanism**, and it matters to Bronze because it
determines whether `quality.py` has to be extracted into a shared package —
which would change Bronze's own structure.

**Decision: Silver gets its own rule engine. `quality.py` is not extracted.**

| | |
| --- | --- |
| **Why** | Bronze's gate is two structural checks tightly integrated with quarantine, `_quarantine_id` content-hashing, replay and the audit trail. Silver needs severity routing, per-rule result counts and arbitrary expressions. The shared surface is "split a DataFrame in two", which is not enough to justify coupling two layers' release cycles |
| **Cost accepted** | Two implementations of "split good from bad" |
| **Revisit if** | #163 concludes DQX is adopted — in which case Silver uses DQX and this question is moot, which is why #163 should be answered first |

`_quarantine_reason`'s format (`null:col`, `duplicate:colA,colB`) is worth
Silver reusing as a convention, whatever the implementation.

---

## 6. Metadata continuity

**Decision: Silver writes to the *same* `_ingestion_audit` and
`_schema_registry` tables, with a `layer` column added.**

The alternative — per-layer tables — was rejected because #62's dashboard
and #61's baselines assume one audit table, and deciding this *after* that
dashboard exists means rebuilding it. That is the whole reason this question
is in scope now rather than when Silver starts.

Consequences, all of which are Bronze's to absorb:

- `AUDIT_SCHEMA` gains a `layer STRING` column (`"bronze"` / `"silver"`),
  defaulting to `"bronze"` so existing rows and callers are unaffected
- The strict no-catch-all rule still applies: any Silver-specific metric
  needs its own column and its own justification
- The audit table becomes a cross-layer dependency, which makes #159's
  lifecycle work more important rather than less

⚠️ **Not decided here:** whether Silver's per-rule quality results
(#109's `_quality_results` proposal) belong in a third table or as rows in
the audit table. That depends on the outcome of §5 and #163.

---

## What this unblocks

| Issue | Was blocked on | Now |
| --- | --- | --- |
| **#58** | "is CDF actually what Silver reads?" | **Yes** (§1). Ship with #159's retention floor, and reject `overwrite` + CDF at config load (§2) |
| **#109** | *"Silver has a real pipeline"* — unfalsifiable | Replace with: **§5 answered and #163 concluded**. Both are checkable |
| **#159** | no home for the retention number | §1 recommends **30 days**, pending confirmation |
| **#62** | — | §6 confirms one audit table, so its tiles need a `layer` filter |

---

## Deliberately still open

Three things this document does **not** decide, because they are not
Bronze's to decide:

1. **What Silver actually computes.** Entirely out of scope.
2. **Whether a replayed row corrects or supplements** what Silver already
   emitted (§3).
3. **Where per-rule quality results live** (§6), pending #163.

---

## Review checklist

Before this is treated as settled, someone with the business context should
confirm:

- [ ] **The 30-day CDF retention floor** (§1) — the only number here that
      cannot be derived from the code
- [ ] **`overwrite` out of contract** for Silver-consumed tables (§2)
- [ ] **Silver reimplements rather than shares the quality engine** (§5) —
      and note this is contingent on #163
- [ ] **One audit table with a `layer` column** rather than per-layer tables
      (§6)
