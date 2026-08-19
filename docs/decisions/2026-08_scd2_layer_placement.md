# SCD Type 2 — which layer owns it, August 2026

The blocking deliverable for **#255** ("Add SCD Type 2 support to incremental
merge logic"), which #255 filed as a Stage 1 implementation task against
`bronze_layer/bronze_ingest`.

**Status: proposed. Not signed off.** This record argues that #255 as written
should not be built in Bronze, and that most of what it asks for already
exists. It does not decide whether the project wants SCD2 at all — that is a
Silver modelling question with no stated business owner, and §7 says what
would settle it.

Written against `dev` @ `5d5d279`. In the style of the other records in this
directory: the decision, its reasoning, and the alternatives rejected.

---

## 1. What #255 asks for

Four items, quoted:

1. Extend the merge logic from #254 to track row history (effective/end dates
   or similar)
2. Handle updates as new versioned rows rather than overwrites
3. Validate against a sequence of changes to the same key
4. Document the SCD2 schema convention for future sources

With the acceptance criteria: *"Same source from #254 now retains full change
history via SCD Type 2"* and *"Point-in-time queries against the table return
correct historical state."*

## 2. Item 2 already shipped, on the day #255 was filed

**"Handle updates as new versioned rows rather than overwrites" is the exact,
documented semantics of `content_hash_columns`**, merged the same day under
**#84** (PR #246). From `config.py`'s field comment:

> `content_hash_columns` answers "are these the same bytes?" — on an upstream
> UPDATE the hash no longer matches, so the row **INSERTS**, leaving both
> versions in the table. That is correct for an append-only bronze layer
> capturing every version of a record.

So Bronze has three write semantics today, and two of them already retain
full history:

| Mode | On an upstream UPDATE to an existing entity |
| --- | --- |
| `append` | New row. Every version retained. |
| `merge` + `merge_keys` | Row **updated in place**. Prior version lost. |
| `merge` + `content_hash_columns` (#84) | **New row.** Both versions retained. |

`test_content_hash_merge_key_gives_idempotent_reingestion_not_upsert` pins
this: re-ingesting identical bytes is a no-op, while a changed value against
the same business key produces a second row.

**What is left of #255 after removing item 2 is item 1** — the interval
bookkeeping: `effective_from`, `effective_to`, `is_current`. That is the part
this record is actually about.

## 3. The decision

**SCD Type 2 interval bookkeeping does not belong in Bronze. It belongs in
the layer that models business entities — Silver (#205), or Gold (#228)
depending on the grain.**

Bronze's job is to retain what arrived and when. It does that already. Turning
retained versions into closed intervals is a *derivation*, and it is the
derivation Silver exists to perform.

## 4. Why — the three arguments, strongest first

### 4.1 It requires retroactively editing rows the current batch does not contain

This is the load-bearing argument, and it survives the obvious objection.

**The objection:** "Bronze already updates rows — `merge_keys` calls
`whenMatchedUpdateAll()`. Append-only is not an absolute here, so SCD2's
writes are not categorically new."

That objection is correct about the mechanism and wrong about the category.
A `merge_keys` upsert replaces a row with a newer version of *that same
record, supplied by the incoming batch*. Every byte written is derived from
data in hand. The write is a projection of the input.

An SCD2 close-out is not. Setting `effective_to` on version *N−1* is a write
to a row **the current batch does not contain**, recording a fact — "this
version stopped being current" — that appears in no source file. It is
bookkeeping the pipeline invents about its own history.

The consequence is concrete: **it destroys re-derivability.** Today, dropping
a bronze table and replaying its source files reconstructs it. With SCD2
close-outs, replay reconstructs the rows but not the intervals, because the
intervals depend on the order and timing of past *runs*, not on the source
data. Bronze stops being a function of its inputs.

This repo has drawn that exact line before.
`docs/decisions/2026-08_autonomous_remediation.md` §2's NEVER list, item 3,
rules out an AI editing a business value in bronze:

> Bronze's stated job is source fidelity — `flatten_mode` was removed from
> Bronze entirely on exactly this reasoning, and
> `docs/bronze_silver_contract.md` places reshaping in Silver. A corrected
> value is a transformation. An AI editing a business value in bronze destroys
> the property that makes bronze re-derivable and makes any Silver
> rebuild-from-bronze untrustworthy **in a way no downstream check can
> detect.**

Every clause of that applies here with the actor changed. An invented interval
is the same category as an invented value: not sourced, not re-derivable, and
undetectable downstream — a Silver rebuild would read intervals that look
authoritative and cannot be checked against anything. That record adds *"this
is a bronze/silver contract boundary, and this record does not reopen it."*
Neither does this one.

### 4.2 It changes the CDF stream shape that Silver is contracted to read

`docs/bronze_silver_contract.md` §1 decides:

> **Silver reads Bronze incrementally via Delta Change Data Feed.**

Under CDF, an `UPDATE` does not emit one row — it emits an
`update_preimage` / `update_postimage` pair. Today the only updates in Bronze
come from `merge_keys` upserts, which correspond to something Silver can
interpret plainly: this entity changed.

With SCD2 close-outs, **every history transition emits an extra update pair
whose entire content is bookkeeping**. Silver would have to recognise and
discard the close-out half to avoid double-counting — filtering out
Bronze-generated noise in order to compute the history that Bronze was
generating the noise to record. The layer computing SCD2 would be reading
past a half-built SCD2 to do it.

The contract's §1 obligations already land on Bronze (CDF enabled — #58; a
`VACUUM` retention floor — #159). Adding a semantic that complicates the
stream, before Silver exists to say whether it wants it, inverts the order
this contract was written to protect.

### 4.3 Interval boundaries are a modelling decision Bronze has no basis to make

To close an interval you must answer questions Bronze cannot see:

- **Which columns constitute the business key?** `merge_keys` is the closest
  thing Bronze has, and #47 established it is a *write-correctness* control,
  not a business-identity claim. `content_hash_columns` explicitly is not an
  identity key — it answers "same bytes", not "same entity".
- **What orders versions?** `_ingested_at` is when Bronze saw a row, not when
  the fact became true. Late-arriving data makes those diverge, and SCD2 built
  on arrival order silently misdates every out-of-order batch.
- **Which changes are worth a new version?** Every SCD2 implementation
  distinguishes tracked attributes from ignored ones. That is a business
  judgement about what a meaningful change is.

`docs/bronze_silver_contract.md` §"Deliberately still open" item 1 says *"What
Silver actually computes. Entirely out of scope"* — Bronze's contract
deliberately declines to decide this. #255 would have Bronze decide it by
implementation, without a decision.

## 5. What already answers "point-in-time queries" today

#255's second acceptance criterion — *"Point-in-time queries against the table
return correct historical state"* — has three existing answers, none needing
new code:

- **Delta time travel.** `SELECT * FROM t VERSION AS OF n` / `TIMESTAMP AS OF
  ts` reconstructs the table as it stood, for any mode, with no schema
  changes. This is table-state point-in-time, not entity-state.
- **`_ingested_at` on retained versions.** Under `append` or
  `content_hash_columns`, filtering to the latest row per key as of a
  timestamp is a query, not a schema. It gives entity-state point-in-time
  without any bookkeeping columns.
- **CDF, once #58 lands.** The full change stream is the history, in the form
  Silver is contracted to read.

The gap SCD2 closes over these is *ergonomic* — a pre-computed interval is
cheaper to query than a windowed one. That is a real benefit and it is a
serving-layer benefit, which is another way of saying it belongs downstream.

## 6. What this means for #255 concretely

- **Item 2 — close as delivered.** #84 shipped it.
- **Items 1, 3, 4 — re-file under #205 (Silver epic)**, not against
  `bronze_ingest`, and only once Silver has a pipeline to host them. #109 is
  the nearest existing child.
- **The stated dependency is already void.** #255 says *"Depends on #254 —
  extend that logic."* #254 was closed 2026-08-11 as already-implemented, and
  no deployed config uses `write_mode: merge` at all. There is no "same source
  from #254" for #255's acceptance criteria to refer to.
- **A prerequisite nobody has named:** SCD2 over CDF needs #58 (Change Data
  Feed) for Silver to read transitions incrementally. #58 is open.

## 7. What would change this decision

Stated plainly so this record can be overturned on evidence rather than
re-argued:

1. **A named consumer with a query Bronze cannot serve.** Not "history is
   useful" — a specific question that time travel, `_ingested_at` windowing
   and CDF all fail to answer acceptably. That would justify SCD2 somewhere;
   it would still not put it in Bronze.
2. **A decision that Silver will not be built**, making Bronze the terminal
   modelled layer. That reverses the premise of the whole contract and is a
   Project Lead call, not an implementation detail.
3. **A source that arrives already SCD2-shaped** — carrying its own
   `effective_from` / `effective_to` from upstream. Then Bronze lands those
   columns like any others, no bookkeeping, no close-outs, and none of §4
   applies. This is the one case where "SCD2 in Bronze" is trivially fine, and
   it is worth checking whether it is what #255 actually meant.

## 8. Alternatives considered

| Alternative | Why not |
| --- | --- |
| **Build SCD2 in Bronze as #255 asks** | §4. Destroys re-derivability, complicates the contracted CDF stream, and forces Bronze to make business-identity decisions it has no basis for. |
| **Build it in Bronze but behind a config flag, off by default** | The flag does not remove the cost — it adds a second write semantics to `bronze_writer` that every future change must reason about, and the re-derivability break still applies to any table that enables it. A guarantee that holds only when a flag is unset is not a guarantee. |
| **Do nothing, close #255 outright** | Overreach. The underlying want — queryable entity history — is legitimate. The layer and the timing are what is wrong, not the goal. |
| **A materialised SCD2 view over Bronze, in Bronze's schema** | Closer to right, and still misplaced: a view that models business entities is Silver's output living in Bronze's namespace. If Silver is going to compute it, it should own it. |

---

## Appendix — the shape of the argument, for whoever revisits this

Three separate decisions in this repo have now landed the same way: **#76**
archived the flattener out of Bronze, **#109** split business rules out of
Bronze, and this record keeps SCD2 out of Bronze. Each was argued
independently and reached the same boundary.

`docs/bronze_silver_contract.md` names the risk that creates — *"the contract
is being defined by a series of independent 'not here' decisions rather than
by a design"* — and it is worth restating here rather than adding a fourth
quietly. The deferred capability now has three things waiting on it. That
strengthens the case for scheduling **#205**, and it is the strongest argument
in this record for doing something about Silver rather than merely declining
to do this in Bronze.
