# Per-environment Volumes — the plan, and why it is not "effectively nil"

The provisioning design for **#160**: source-file isolation is a naming
convention, not a control. #160 already recommends three Volumes; this records
*how*, in what order, and one measured finding that changes its cost estimate.

**Status: declined, 2026-08-19.** Staging and prod will use subpaths of the
existing `ext-ingredion-dev` volume — `STG/` and `PROD/`, which already exist —
rather than getting volumes of their own. The bundle points there. Everything
below is retained as the analysis that informed the decision, not as a plan
awaiting execution.

### What that costs, recorded so it is not rediscovered later

The plan below treats this as a *read* gap: any principal holding `READ VOLUME`
on the root-scoped volume can read every environment's files. Declining it makes
the gap symmetric, because reading is not all the package does.

`bronze_ingest` archives ingested files into `processed/` and failed ones into
`quarantine_files/`, under the source root. That needs `WRITE VOLUME`, and Unity
Catalog grants it at volume granularity. So the staging principal — and, when it
deploys, the prod principal — can **write anywhere under the storage container**,
including each other's `Raw/` folders. A misconfigured `source_dir` or
`table_name_template` is then a data-loss event in another environment rather
than a failed run in its own.

Two things follow, and both are cheap:

- **Source-file separation is a naming convention, not a control.** Say so
  wherever isolation is described, rather than implying schema-level isolation
  extends to files. It does not.
- **The door stays open.** Nothing here is one-way. Creating
  `ext-ingredion-stg` later and repointing `source_volume_path` is the same two
  statements it always was, and no data has to move — the folders keep their
  paths inside the container either way.

**Original status: proposed. Not executed.** Creating volumes and external
locations, and granting on them, is **Tier 3** under
`docs/agent_governance.md`.

Written against `dev` @ `c983d52`, verified against the workspace 2026-08-12.

---

## 1. The measured finding #160 does not have

#160 proposes three volumes and prices the change at *"effectively nil — volumes
and external locations are metadata."* That is true of two of the three. It is
not true of the third, and the reason is what the existing volume actually
points at:

```
$ databricks volumes read ingredion_en.ingredion_dev.ext-ingredion-dev
full_name       : ingredion_en.ingredion_dev.ext-ingredion-dev
volume_type     : EXTERNAL
storage_location: abfss://ingredion@ingredionenpkgdev.dfs.core.windows.net/
```

**The dev volume spans the container ROOT, not `/DEV/`.** That is the actual
mechanism of the gap — not that three environments share a volume, but that one
volume covers all three environments' data.

The consequence for the plan is direct:

> **Creating `ext-ingredion-stg` and `ext-ingredion-prd` does not close the
> gap.** Anyone holding `READ VOLUME` on `ext-ingredion-dev` can still read
> `STG/` and `PROD/` through it, because that volume still spans the root. The
> gap closes only when the dev volume is *also* narrowed to `.../DEV/`.

And narrowing the dev volume is the part that is not free. It is live — 15
recorded ingestion runs — and `azure_setup.md` records it as the pytest scratch
location for `tests/test_directory_ingestion.py`. A volume's
`storage_location` is immutable, so narrowing means drop and recreate.

The name is also now misleading: `ext-ingredion-dev` is not dev-scoped, it is
root-scoped.

## 2. Target state

| Environment | Schema | Volume | Storage location |
| --- | --- | --- | --- |
| dev | `ingredion_dev` | `ext-ingredion-dev` | `abfss://ingredion@…/DEV/` |
| staging | `ingredion_stg` | `ext-ingredion-stg` | `abfss://ingredion@…/STG/` |
| prod | `ingredion_prd` | `ext-ingredion-prd` | `abfss://ingredion@…/PROD/` |

Each volume lives in **its own schema**, so the schema boundary that already
isolates tables, audit and registry now reinforces the file boundary rather than
cutting across it. One external location (`ext-ingredion`) still covers the
storage account; volumes scope within it.

## 3. Order of operations — and it matters

**Phase A — staging and prod. Safe, do it now.**

Neither environment has data (`ingredion_stg` and `ingredion_prd` contain zero
tables) or a source-file consumer. Creating their volumes and repointing the
bundle disturbs nothing.

```sql
CREATE EXTERNAL VOLUME ingredion_en.ingredion_stg.`ext-ingredion-stg`
  LOCATION 'abfss://ingredion@ingredionenpkgdev.dfs.core.windows.net/STG/';
CREATE EXTERNAL VOLUME ingredion_en.ingredion_prd.`ext-ingredion-prd`
  LOCATION 'abfss://ingredion@ingredionenpkgdev.dfs.core.windows.net/PROD/';
```

Then grant `READ VOLUME` + `WRITE VOLUME` to each environment's service
principal on its own volume only. **Write is required, not optional** — the
package archives ingested files into `processed/` and `quarantine_files/` under
the same root (#160 notes this, and it is why read-only grants are not the
answer).

**Phase B — dev. Disruptive, needs a window.**

1. Confirm nothing else depends on root-level access — in particular the
   pytest scratch path in `tests/test_directory_ingestion.py` and
   `docs/testing_directory_ingestion.md`.
2. Drop and recreate `ext-ingredion-dev` scoped to `.../DEV/`.
3. Re-run the dev ingestion job and the directory-ingestion tests.

**Until Phase B completes, the gap is open**, regardless of Phase A. Phase A is
worth doing first anyway because it establishes the boundary for the
environments that will hold production data — but it should not be reported as
closing #160.

## 4. Bundle change

`source_volume_path` for staging and prod is updated in this change to describe
the target state. Dev's is deliberately **left as-is** until Phase B, because
repointing it before the volume exists would break a working job.

That does mean staging/prod deploys will fail until Phase A runs. That is the
right failure: a deploy that cannot find its source volume should stop, not
silently read another environment's files — which is the very thing this issue
is about.

## 5. What this does not fix

`IngestionConfig.load(config_path)` reads configs from a Volume, so whoever
holds `WRITE VOLUME` can influence what a job ingests (#154 established this as
a wider trust boundary than the repo). Per-environment volumes narrow *whose*
configs a principal can influence; they do not remove the path. That remains
#154's, and it is worth stating so this issue is not read as closing it.
