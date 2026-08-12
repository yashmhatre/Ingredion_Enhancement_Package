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
| 1 | Drop 3 well-formed files (wave A), run once | Auto Loader reads a directory at all; checkpoint and schema locations get created | 3 rows in `TBL`; `_checkpoint/` and `_schema/` populated | _pending_ |
| 2 | Run again, **no new files** | The checkpoint suppresses reprocessing — the exactly-once claim | 0 new rows; total still 3; run exits clean rather than hanging | _pending_ |
| 3 | Drop 2 more files (wave B), run once | Incremental discovery of files that arrived after the first run | +2 rows only, total 5; waves A files not re-read | _pending_ |
| 4 | Drop 1 file carrying a **new column**, run once | `schemaEvolutionMode=addNewColumns` behaviour — Auto Loader is documented to *fail the stream* on an unknown field and pick the new schema up on restart | Run 1 fails with `UnknownFieldException`; the immediately following run succeeds and the column exists | _pending_ |
| 5 | Inspect the audit table for this table | Whether streaming writes one audit row per micro-batch, and whether the count is sane (#156 flagged 2,880/day on a 30s trigger — `availableNow` should make this small) | one row per run, not per file | _pending_ |
| 6 | Inspect `_source_file` / lineage columns | Streaming attaches the same lineage the batch path does | non-null, one distinct value per source file | _pending_ |
| 7 | Drop a `.jsonl` file with `multiline` left at its default | The truncation guard (#146) fires on the streaming path, not just in the unit tests | `JsonLinesTruncationError`, or a documented reason it does not apply here | _pending_ |

## Findings

_Not yet run. This section is the deliverable of #248's second acceptance
criterion ("findings documented — what works, what doesn't, what needs fixing
before Stage 1.5") and stays empty until the runs above have actual results._

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
