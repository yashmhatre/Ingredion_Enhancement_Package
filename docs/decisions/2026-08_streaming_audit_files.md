# Streaming audit rows — buffer them, or compact them? August 2026

The decision **#159 item 2's streaming half** needs, and the last open item on
that issue. #156 flagged the trade-off; this settles it.

**Status: proposed. Not signed off.**

Written against `dev` @ `4cf1ca8`.

---

## Verdict

**Do not buffer audit rows across micro-batches. Compact the table instead.**

The file-count problem is real. Buffering is the wrong fix for it in
streaming, for a reason that does not apply to the batch path where buffering
*was* the right fix (#275).

## 1. The problem, restated with the measured number

`_ingestion_audit` gets one row — and therefore one file — per unit of work.
Measured on the deployed schema at 15 runs: **15 files, 4–7 KB each** (#274).

For batch directory ingestion that was one file per file ingested per run, and
**#275 fixed it**: `buffered_audit_writes()` collects rows for the duration of
a run and writes one commit, coalesced to one file.

Streaming is worse arithmetically. Each micro-batch opens its own
`audited_run(..., stream_batch_id=batch_id)`, so under
`trigger_mode: processingTime` at 30 seconds that is **2,880 rows/day**, and
#159's projection of roughly **1M files/year** for a single continuous stream
is right.

## 2. Why the batch fix does not transfer

`buffered_audit_writes()` works in `ingest_directory_to_bronze` because of a
property streaming does not have: **a directory run is bounded and it
terminates.** The buffer's lifetime is one function call, the flush point is
that call returning, and the `finally` block guarantees the flush happens even
when the loop raises (#275 tests this).

A streaming query has none of that:

- **No flush point exists.** A continuous query does not end. A buffer would
  have to flush on a count or a timer, which is a new mechanism with its own
  failure modes rather than a reuse of the batch one.
- **The loss window is unbounded, and correlated with exactly the wrong
  event.** Buffered rows live in driver memory. If the driver dies, they are
  gone — and driver death is precisely when the audit trail matters most. A
  design whose records disappear in proportion to how badly the run failed is
  worse than no optimisation.
- **Memory grows with uptime**, not with work. A stream that runs for a week
  holds a week of rows unless the flush interval bounds it, which returns to
  the first point.

The batch case has none of these because it buffers over a *bounded* unit of
work and flushes unconditionally at its end.

## 3. What to do instead: compact, do not avoid writing

Delta is built for the write-many-small-files-then-compact pattern. The
machinery already exists in this repo and it already covers this table.

**#277's maintenance job** (merged, `PAUSED`) runs `OPTIMIZE` over exactly
`_ingestion_audit`, `_schema_registry` and `_ai_metadata` — the three tables
measured to grow one file per run. Compaction is what turns 2,880 small files
a day into a manageable count, without touching the write path or the
durability of a single row.

So the streaming half of #159 item 2 is **already addressed by item 3**, and
that is the finding: it was filed as a write-path problem and it is a
maintenance problem.

**One thing this depends on, and it is not yet true:** #277's schedule ships
`PAUSED` and the job is deployed nowhere. Until it runs, streaming's file
growth is unmitigated. That dependency is the honest cost of this verdict, and
it is a Tier 2 decision rather than an engineering one.

### Worth considering alongside, not instead

`delta.autoOptimize.optimizeWrite` / `autoCompact` compact at write time on
Databricks Runtime. They would reduce the file count *before* maintenance runs
rather than after, which is strictly better for a table nobody is optimising
yet. They are table properties, so they would go through the same
`resolved_table_properties` path #58 added — a small, contained change.

Deliberately **not** bundled into this decision: they change write behaviour on
every table the package creates, which deserves its own evaluation rather than
arriving as a footnote to a streaming-audit question.

## 4. What would change this verdict

1. **A bounded streaming trigger becomes the norm.** Under
   `trigger_mode: availableNow` a stream processes what is available and stops
   — which *is* bounded, and `buffered_audit_writes()` would transfer to it
   directly. If the deployed jobs move to `availableNow`, revisit: the
   objection in §2 is about continuous queries specifically.
2. **Audit rows stop mattering individually.** If the trail were ever
   aggregated rather than per-run, losing a buffered row would cost less and
   the calculus changes. Nothing suggests that today — #61, #62 and #259 all
   read it per-run.

## 5. What this closes

#159's item 2 is complete with this: the batch half shipped in #275, and the
streaming half is decided here as *not buffering, compact instead*. The issue's
remaining exposure is operational, not engineering — #277's job needs
deploying and unpausing.
