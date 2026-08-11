# FinOps spike — joining `system.billing.usage` to pipeline runs, August 2026

The deliverable for **#258**, Stage 2.1 of the competitive roadmap: *"write a
query joining `system.billing.usage` to one pipeline's job/run IDs."*

Written against `dev` @ `b7fbdb1`, workspace `adb-7405607398572130`.

---

## Result: blocked on access, and the blocker is not a grant this project can request

`system.billing` **is not readable, and is not even visible** from this
workspace with the `bronze-json-loader-dev` profile.

```sql
SELECT count(*) FROM system.billing.usage;
-- [INSUFFICIENT_PERMISSIONS] Insufficient privileges:
-- User does not have USE SCHEMA on Schema 'system.billing'. SQLSTATE: 42501
```

```sql
SHOW SCHEMAS IN system;
-- ai
-- data_quality_monitoring
-- information_schema
```

That second result is the important one. If `billing` were merely
*ungranted*, it would still appear. It does not, which means the billing
system schema has **not been enabled on this metastore** — enabling a
`system` schema is an account-admin action (`SystemSchemas` API /
account console), not a `GRANT` a workspace user can be given.

**So #258 cannot be completed as written until someone with account-admin
rights enables `system.billing` and grants `USE SCHEMA` + `SELECT` on it.**
That is Tier 3 under `docs/agent_governance.md`.

## What is already ready — the framework half of the join

This is the part worth knowing, because it means the blocker is entirely
external: **the key to join on already exists and is correctly populated.**

```sql
SELECT run_id, table_name, started_at
FROM   ingredion_en.ingredion_dev._ingestion_audit
ORDER  BY started_at DESC LIMIT 1;
-- 819968105186856-179478367465156 | ...sensor_001_bronze | 2026-08-02T06:13:30Z
```

`run_id` is `{{job.id}}-{{job.run_id}}`, set as a job base parameter under
**#52** precisely so an audit row can be tied back to a specific Databricks
job run rather than fuzzy-matched on timestamp. It decomposes exactly onto
what `system.billing.usage` exposes in `usage_metadata`:

| Audit side | Billing side |
| --- | --- |
| `split(run_id, '-')[0]` → `819968105186856` | `usage_metadata.job_id` |
| `split(run_id, '-')[1]` → `179478367465156` | `usage_metadata.job_run_id` |

No schema change is needed on this side. #52 already did the work this spike
would otherwise have had to ask for.

## The query, ready to run once access exists

Unverified — it has never been executed, because it cannot be. Written from
the documented shape of `usage_metadata`, and **every assumption in it is
listed in the next section rather than buried in the SQL.**

```sql
-- Cost per ingestion run. Requires system.billing enabled + granted.
WITH runs AS (
    SELECT
        run_id,
        split(run_id, '-')[0] AS job_id,
        split(run_id, '-')[1] AS job_run_id,
        min(started_at)       AS run_started_at,
        max(finished_at)      AS run_finished_at,
        count(*)              AS tables_ingested,
        sum(row_count)        AS rows_written
    FROM ingredion_en.ingredion_dev._ingestion_audit
    WHERE run_id LIKE '%-%'          -- job-run ids only; auto-generated UUIDs cannot join
    GROUP BY run_id
),
cost AS (
    SELECT
        usage_metadata.job_id      AS job_id,
        usage_metadata.job_run_id  AS job_run_id,
        sum(usage_quantity)        AS dbus
    FROM system.billing.usage
    WHERE usage_metadata.job_id IS NOT NULL
    GROUP BY usage_metadata.job_id, usage_metadata.job_run_id
)
SELECT
    r.run_id,
    r.run_started_at,
    r.tables_ingested,
    r.rows_written,
    c.dbus,
    c.dbus / nullif(r.rows_written, 0) AS dbus_per_row
FROM runs r
LEFT JOIN cost c
       ON c.job_id = r.job_id
      AND c.job_run_id = r.job_run_id
ORDER BY r.run_started_at DESC;
```

## Gotchas — the part #258 actually asks to document

Three are established from this repo; the rest are flagged as **needing
verification against real billing rows**, and are deliberately not asserted
as fact.

**Established here:**

1. **Not every audit row can join.** `run_id` falls back to a generated UUID
   when the job does not pass `{{job.id}}-{{job.run_id}}` — see
   `resolve_batch_id` and #52. Those rows have no billing counterpart at all,
   and a naive inner join silently drops them. Hence `LIKE '%-%'` and a LEFT
   join above: a run with no cost attributed should show as NULL cost, not
   vanish.
2. **Cost is per job run, usage is per table.** One job run ingests many
   tables (`_ingestion_audit` holds one row per table per run — measured at
   #274). So cost cannot be attributed per-table without an allocation rule,
   and any such rule is an invention. `dbus_per_row` above is the honest
   aggregate; a per-table figure would not be.
3. **`_ai_metadata`'s job is separate.** The AI metadata job is a distinct
   job id, so its cost is separately attributable — which is the interesting
   number, since it is the only lane that calls a paid model endpoint.

**Needs verification once access exists — do not treat as known:**

4. Billing data **latency** (how long after a run its usage row appears) and
   whether `usage_quantity` is final or restated. Both change whether a
   cost column on the audit table (#259) can be written by the ingestion run
   itself or must be backfilled by a later job. **This is the finding that
   decides #259's design**, and it cannot be guessed.
5. Whether serverless compute attributes `job_id`/`job_run_id` in
   `usage_metadata` the same way classic job compute does. This workspace is
   serverless by design (`azure_setup.md` Step 3), so if attribution differs
   there, the query above joins nothing in exactly the environment that
   matters.
6. Whether SQL-warehouse usage from `ai_query` (the AI metadata lane) lands
   against the job at all, or against the warehouse independently of it.

## What this means for #259 and #260

- **#259** (a per-run cost column on the audit table) **should not start
  until gotcha 4 is settled.** If billing lands hours late, a column written
  during the run is structurally impossible and #259 is really a backfill
  job — a different design with a different failure mode.
- **#260** (cost anomalies >2× a 7-day rolling average) depends on #259 and
  inherits everything above. It also overlaps #62's alerting mechanism,
  which #263 was folded into — worth building on that rather than beside it.

## To unblock

One action, by someone with account-admin rights:

1. Enable the `billing` system schema on the metastore.
2. `GRANT USE SCHEMA ON SCHEMA system.billing` and `GRANT SELECT ON TABLE
   system.billing.usage` to the principal that runs this exploration.

Then re-run the query above and fill in §"needs verification". Nothing in
this repo has to change first.
