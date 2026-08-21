# Auto Loader Stream Validation — Procedure and Findings (#248)

## Purpose

Auto Loader (`cloudFiles`) support exists in `bronze_ingest.streaming_reader`
but has never been run against files that arrive *while the reader is
watching*. Everything proven so far is either a pure function over paths and
config, or the batch path. `tests/test_streaming_reader.py` says so in its own
module docstring, and it is right to:

> Auto Loader (`cloudFiles`) is a Databricks Runtime feature. It does not
> exist in OSS Spark, so `read_json_stream` cannot be executed by this suite
> at all [...] What remains genuinely unverified locally is only the wiring:
> that `read_json_stream` passes the resolved `multiLine` to cloudFiles. That
> is asserted against a recording fake below, which proves the option is set
> and proves nothing about whether Auto Loader honours it.

So this cannot be closed by adding tests. It needs a run on Databricks, and
this document is the procedure plus the place the findings land.

## The one design decision worth stating up front

The roadmap phrases this task as *"run Auto Loader ingestion against it for a
sustained period."* **This procedure deliberately does not do that.** It uses
repeated **bounded `availableNow` runs** with file drops in between.

Two reasons, and the first outranks the second:

1. **Nothing in this workspace runs unattended.** That is a standing
   instruction, and it covers more than `schedule:` blocks — anything that
   keeps working without someone watching counts. A `processingTime` trigger
   is exactly that: a query that stays up, bills compute, and is hard to see.
   `system.billing` is currently unreadable by the dev identity, so its spend
   could not even be measured after the fact.
2. **Bounded runs are the better experiment anyway.** Exactly-once and
   incremental discovery are claims about *what happens across a restart*.
   A long-lived query hides the restart; `availableNow` makes every run
   boundary an observation point, so "did the checkpoint prevent
   reprocessing" becomes a row count instead of an inference.

Conveniently, `notebooks/run_ingestion.py` exposes no `trigger_mode` widget,
so the deployed entrypoint **cannot** be driven continuously without a code
change. The default is `availableNow`, which terminates on its own.

## Prerequisites

| Item | State | Checked |
| --- | --- | --- |
| Dev volume writable from the CLI | yes — `databricks fs cp` into `.../pytest_scratch/` succeeds | 2026-08-12 |
| Streaming entrypoint exists | yes — `notebooks/run_ingestion.py`, `ingestion_mode` dropdown includes `streaming`, dispatches to `run_streaming()` | 2026-08-12 |
| Deployed streaming *job* | **no** — the only streaming-capable job resource (`bronze_orders_ingestion`) is commented out in `resources/bronze_ingest_jobs.yml`. Use a one-off `databricks jobs submit`, which persists no resource and schedules nothing. | 2026-08-12 |
| `system.billing` readable (to cost the run) | no — blocks #258/#259/#260, and means this run's spend is estimated, not measured | 2026-08-12 |

Paths used throughout (dev only):

```
SRC=/Volumes/ingredion_en/ingredion_dev/ext-ingredion-dev/pytest_scratch/al248/incoming
CHK=/Volumes/ingredion_en/ingredion_dev/ext-ingredion-dev/pytest_scratch/al248/_checkpoint
SCH=/Volumes/ingredion_en/ingredion_dev/ext-ingredion-dev/pytest_scratch/al248/_schema
TBL=ingredion_en.ingredion_dev.al248_stream_bronze
```

`pytest_scratch/` is deliberate: it is already the throwaway area, it is not
the directory `bronze_directory_ingestion` reads, and nothing here can be
mistaken for an onboarded source. Teardown is `databricks fs rm -r` on
`al248/` plus a `DROP TABLE` — note the drop is Tier 2 and needs sign-off, so
prefer leaving the table until there is a reason to remove it.

## Procedure

Each wave is: drop files from the local machine with `databricks fs cp`, then
submit one bounded run, then assert on row counts. The drop happens *between*
runs, which is what makes the discovery claim testable.

| # | Action | What it proves | Expected result | Actual |
| --- | --- | --- | --- | --- |
| 1 | Drop 3 well-formed files (wave A), run once | Auto Loader reads a directory at all; checkpoint and schema locations get created | 3 rows in `TBL` | **FAILED first, then PASS.** The first attempt died in our writer - see Finding 1. After the fix: 3 rows. |
| 2 | Run again, **no new files** | The checkpoint suppresses reprocessing - the exactly-once claim | 0 new rows; total still 3 | **PASS.** Still 3. Run exited clean, and wrote no audit row at all: the empty micro-batch is skipped before `audited_run`. |
| 3 | Drop 2 more files (wave B), run once | Incremental discovery of files that arrived after the first run | +2 rows only, total 5 | **PASS.** Exactly `A-1,A-2,A-3,B-1,B-2`. Wave A not re-read. |
| 4 | Drop 1 file carrying a **new column**, run once | `schemaEvolutionMode=addNewColumns` behaviour | Run 1 fails, run 2 succeeds | **PASS, but not as predicted - see Finding 2.** One run, reported SUCCESS. The `UnknownFieldException` did happen and DBR retried it internally. `currency` was added and `D-1` landed. |
| 5 | Inspect the audit table for this table | Whether streaming writes a sane number of audit rows | one row per run, not per file | **PASS.** 8 rows across 6 runs: 3 `failed/write` (Finding 1, one per retry attempt), 3 `success` (3, 2, 1 rows), 2 `failed/read` (wave 7). Empty batches write nothing. |
| 6 | Inspect `_source_file` / lineage columns | Streaming attaches the same lineage the batch path does | non-null per row | **PASS.** `_source_file` non-null on every row. |
| 7 | Drop a `.jsonl` file with `multiline` left at its default (which is `True`) | The truncation guard (#146) fires on the streaming path, not just in the unit tests | `JsonLinesTruncationError` | **PASS.** Raised; batch not committed, checkpoint not advanced, audit tagged `failure_stage=read`. |

## Findings

Run 2026-08-12 against `dev`: six bounded `availableNow` submits.

**Auto Loader itself was never the problem.** Every failure below is ours. In
the very first run `CloudFilesSource` discovered the files, inferred the
schema, projected `_rescued_data` and `_input_file_name`, and committed
offsets - and then our writer raised.

### Finding 1 - the streaming write path had never worked on this platform

`write_bronze_micro_batch` tested emptiness with `micro_batch_df.rdd.isEmpty()`:

```
PySparkNotImplementedError: [NOT_IMPLEMENTED] rdd is not implemented. SQLSTATE: 38000
```

`.rdd` does not exist on Spark Connect. Every compute this project has is
serverless, and serverless *is* Spark Connect, so this failed the **first
micro-batch of every streaming run**, always.

It survived because nothing could reach it: `cloudFiles` is Databricks-only
so the suite cannot start a stream, local pyspark and CI are classic Spark
where `.rdd` is fine, and `write_bronze_micro_batch` had no tests at all.
**This is the defect #248 existed to find, and writing more tests would never
have found it.** Fixed in #295, with regression tests that fake the Connect
behaviour and so need neither a stream nor Spark.

### Finding 2 - a drifting stream reports SUCCESS, and the audit trail cannot say it drifted

Wave 4 was predicted to fail and then succeed on restart. Visibly it did
neither: **one run, reported `SUCCESS`.** The exception did happen -

```
[UNKNOWN_FIELD_EXCEPTION.NEW_FIELDS_IN_RECORD_WITH_FILE_PATH]
Encountered unknown fields during parsing: {"currency":"USD"},
which can be fixed by an automatic retry: true
```

- and the runtime retried it transparently. The only trace is the `error`
field of a run whose state is `SUCCESS`. Nobody reads that field.

Our own trail cannot fill the gap. **`schema_changed` is NULL on every
streaming audit row, structurally.** `run_streaming` calls `record_schema`
once before the stream starts, and every micro-batch runs with
`record_metadata=False` - deliberately, per #156, since otherwise it would
fire per batch. So the boolean a dashboard would count is never set on this
path.

`_schema_registry` *did* update - its fingerprint now carries `currency` -
but it holds current state, not history. It can say the schema differs now;
it cannot say which run changed it.

**Net: a bronze table can gain a column, from a run that reported success,
with nothing in the audit trail marking it.** That is a live gap against
#256's goal, which was believed closed.

### Finding 3 - four streaming knobs have no deployed caller

`run_ingestion.py` has no widget for `trigger_mode`,
`trigger_processing_time`, `schema_evolution_mode`, or `multiline`. So
`get_trigger_kwargs`' `processingTime` branch is unreachable from the only
streaming entrypoint, and only the `addNewColumns` default can be exercised
end to end. (`multiline` having no widget is why wave 7 tested the default
`True` - which turned out to be the interesting case anyway.)

There is also **no deployed streaming job resource**: the only
streaming-capable one is commented out. Stage 1.5 assumes one exists.

### What holds up

Checkpointing, exactly-once and incremental discovery behave exactly as
claimed (waves 2 and 3), and the #146 truncation guard fires correctly on the
streaming path with the batch left uncommitted (wave 7). Once Finding 1 is
fixed, the streaming path works.

### Teardown

Source files, checkpoint and schema store removed. `al248_stream_bronze` and
its audit rows are left in place - `DROP TABLE` is Tier 2.

## Gaps already visible without running anything

These came out of reading the path while writing this procedure, and hold
regardless of what the run shows:

1. **No `trigger_mode` / `trigger_processing_time` widget on
   `run_ingestion.py`.** Helpful today (it cannot be made continuous by
   accident) but it means `get_trigger_kwargs`' `processingTime` branch has no
   deployed caller at all — it is reachable only from library code.
2. **No `schema_evolution_mode` widget either**, so wave 4 exercises the
   `addNewColumns` default and no other mode. Testing `rescue` or
   `failOnNewColumns` end-to-end needs either a widget or a config file.
3. **No deployed streaming job resource.** Whatever this validation concludes,
   there is currently no way to run streaming ingestion except by hand.
   Stage 1.5 assumes there is.
