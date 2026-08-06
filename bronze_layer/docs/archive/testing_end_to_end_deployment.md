> **ARCHIVED — point-in-time record, not maintained.** Moved from
> `bronze_layer/docs/testing_end_to_end_deployment.md` on 2026-08-06 as
> part of a documentation refresh; content is unchanged. Names the
> catalog as it was at the time (`ingredion_en_dev`), later renamed to
> `ingredion_en` — the run is the record, the names are not current. See
> `docs/README.md` for what's current.

# End-to-End Deployment Validation — bronze_ingest

## Purpose

Validates the full production deployment path — Databricks Asset Bundle,
both scheduled jobs, notebook entrypoints — running for real against the
corrected Unity Catalog environment (`ingredion_en_dev.ingredion_dev.ext-ingredion-dev`),
as opposed to unit-level or notebook-based validation of individual
modules (see `docs/testing_json_reader.md` and
`docs/testing_directory_ingestion.md`).

This closes the gap between "the code is correct in isolation" and "the
deployed product actually works."

## Scope

- `databricks bundle validate` / `deploy` against the `dev` target
- `bronze_orders_ingestion` job (single-table, config-driven via
  `order_bronze.yaml`) — see status note below, since disabled
- `bronze_directory_ingestion` job (multi-file, one-table-per-file),
  including file archival and the retry-limit-before-quarantine feature
- Real fixture files, reused from `docs/testing_json_reader.md`'s
  validated set

## Setup notes / gotchas hit along the way

1. **Bundle deploy required after every `databricks.yml` edit** — file
   changes alone don't reach the workspace; `databricks bundle deploy -t dev`
   must be re-run explicitly each time.

2. **Stale `catalog` value in a deployed bundle** — an early run failed
   with `NO_SUCH_CATALOG_EXCEPTION: Catalog 'workspace' was not found`,
   traced back to a `databricks.yml` edit that hadn't actually been
   deployed yet (edited locally, but `bundle deploy` wasn't re-run before
   testing). Same class of issue as the pytest catalog mismatch earlier
   in this project — always confirm the *deployed* config, not just the
   local file.

3. **`source_dir` was missing the volume name** — an early version of
   `bronze_directory_ingestion`'s `source_dir` read
   `/Volumes/${var.catalog}/ingredion_dev/ingredion/`, skipping the
   actual volume name (`ext-ingredion-dev`) and jumping straight to a
   folder name that was never real (`ingredion` is the *ADLS container*
   name, not the *volume* name — a different layer). Corrected to:
   /Volumes/${var.catalog}/ingredion_dev/ext-ingredion-dev/raw/JSON/

   Confirmed the Unity Catalog external volume and the ADLS container are
   the same underlying storage — the same fixture files uploaded for
   `json_reader.py` validation were visible and usable here.

4. **`multi_file/` subfolder correctly ignored** — `list_json_files` is
   non-recursive and only matches `.json`/`.jsonl` directly inside
   `source_dir`. The `multi_file/` subfolder (built for a different test
   - `json_reader.py` reading a whole folder into one merged table) was
   correctly invisible to directory ingestion, which processes one file
   → one table. No table named `multi_file_bronze` was ever expected to
   exist; confirmed via a `TABLE_OR_VIEW_NOT_FOUND` check.

## Results

### `bronze_directory_ingestion` — first real run
14/17 files succeeded, 3 failed on the first run:
- `duplicate_keys.json` — expected (documented Spark limitation, see
  `docs/testing_json_reader.md`)
- `duplicate_keys_no_crash.json` — a stray leftover file from an earlier
  fixture-naming iteration, not part of the intended fixture set (since
  deleted — see Follow-up items)
- `malformed.json` — expected (deliberately broken JSON syntax)

Each failure was isolated to its own file — the batch continued
processing the remaining 14 without interruption (`stop_on_error=False`
default confirmed working in a real job, not just in unit tests).

### Tables created

Confirmed: one table per successfully-ingested file, following the
`{filename}_bronze` naming convention, in `ingredion_en_dev.ingredion_dev`.
Row counts matched expected values (e.g. `single_object_bronze` = 1 row,
`array_of_objects_bronze` = 2 rows). `DESCRIBE` confirmed audit/lineage
columns (`_ingested_at`, `_source_file`, `_batch_id`) present on every
table.

No tables were created for the 3 failed files — confirmed via
`SHOW TABLES ... LIKE 'malformed*'` / `LIKE 'duplicate_keys*'` returning
empty results. Partial/broken writes never occur on ingestion failure.

### File archival

Successfully-ingested files were correctly moved to
`processed/2026-07-24/` after their run. Confirmed via direct listing.

### Retry-limit before quarantine

Across 3 real, separate job runs of `bronze_directory_ingestion`:
- Run 1: `malformed.json` / `duplicate_keys.json` failed, attempt
  counters incremented to 1, files left in `raw/JSON/`
- Run 2: same files failed again, counters incremented to 2, still left
  in place
- Run 3: same files failed a 3rd time, hit `max_ingestion_retries=3`,
  both moved to `quarantine_files/` and their retry-state entries cleared

This is the strongest validation of the feature so far — confirmed in
real production job runs against real Databricks infrastructure, not
just the pytest simulation using a mocked `BronzeIngestion.run()`.

## Status

**✅ All checks passed.**

- Bundle validates and deploys cleanly
- Both jobs run successfully end-to-end against real files in the
  corrected Unity Catalog environment
- Per-file failure isolation confirmed in a real job (not just unit tests)
- Archival and retry-limit-quarantine both confirmed working in a real,
  repeated production job context

### `multi_file/` subfolder correctly ignored — superseded

At the time of this end-to-end test, `list_json_files` was non-recursive
and `multi_file/` was correctly invisible to directory ingestion — see
above for the original finding.

**This behavior has since changed.** The folder-as-table feature (see
`docs/testing_directory_ingestion.md`) means any subfolder directly
inside `source_dir` — including `multi_file/` — is now treated as one
merged table. Re-running this end-to-end test today would produce a
`multi_file_bronze` table where none existed before. This is a
deliberate, documented behavior change, not a regression; noted here so
this doc's original finding isn't read as still-current.

### `bronze_orders_ingestion` — later disabled

This job (validated above) was a sample/test job for the single-table
ingestion path. It has since been commented out in `databricks.yml` and
undeployed to avoid unnecessary scheduling cost — the reference
notebook (`run_ingestion.py`) and config (`config/order_bronze.yaml`)
are kept as-is for anyone building a similar single-table job later.
`bronze_directory_ingestion` remains the actively used production job.

## Follow-up items (resolved)

- ~~`duplicate_keys_no_crash.json` — stray fixture file~~ — **resolved**,
  file deleted, fixture set now matches `docs/testing_json_reader.md` exactly
- ~~`bronze_orders_ingestion` results not detailed as extensively~~ —
  **superseded**, job disabled entirely (see status note above), no
  longer applicable

## Related docs

- `docs/testing_json_reader.md`
- `docs/testing_directory_ingestion.md`
- `docs/architecture.md`
