# Azure Databricks setup — bronze_ingest (bronze_layer)

Living runbook for the dev/validation environment used to test `bronze_ingest`
against real Azure infrastructure (Unity Catalog, ADLS Gen2, serverless compute).

Built and validated incrementally — each step below was confirmed working in the
portal before being written down. Day-to-day code iteration still uses local
pytest (`tests/`, local SparkSession) at zero cost; this environment is only for
end-to-end validation.

**Trial context:** Azure free trial, $200 credit, 30-day hard spending limit
(subscription auto-disables at $0 rather than overcharging, as long as the
spending limit isn't removed / account isn't upgraded to Pay-As-You-Go).

**Region used throughout this project:** Central India (keep every resource in
this region to avoid cross-region egress and latency).

**Naming note:** this doc uses placeholder names like `rg-bronze-json-loader-dev`
and `ac-bronze-json-loader-dev` as examples. The actual resources created were
named `rg-ingredion-en-pkg-dev` and `ac-ingredion-en-pkg-dev` — substitute your
own actual names wherever you see the placeholders below.

**Additional naming note:** the Unity Catalog catalog, schema, and volume also
differ from what Steps 5-6 originally planned — see the corrected Step 6 below
for actual values (`ingredion_en_dev.ingredion_dev.ext-ingredion-dev`, not
`workspace.bronze.ingredion`). Confirmed via live testing in the JSON reader
and directory ingestion validation work (see `docs/testing_json_reader.md` and
`docs/testing_directory_ingestion.md`).

**Package rename note:** the package/folder was renamed
`bronze_json_loader` → `bronze_layer` (outer folder) / `bronze_ingest`
(inner importable package), to reflect planned multi-format ingestion
beyond JSON. Any `bronze_json_loader` path references below are historical
context from when the environment was originally set up — substitute
`bronze_layer` for the current repo structure.

**Status as of last session:** Steps 1-11 done and validated. Steps 1-6 built
the Azure/Unity Catalog foundation — resource group, budget alert, ADLS Gen2
storage (`ingredion` container), Databricks serverless workspace, Unity
Catalog wiring (Access Connector, storage credential, external location with
file events), and a dedicated schema + external volume (see corrected Step 6
for actual names). Steps 7-11 took the `dev` environment from "no CLI
installed" to a job running end to end on serverless compute, writing a
bronze table plus audit and schema-registry rows.

The config file edits listed under Step 6 have since been **applied** to the
repo (`order_bronze.yaml`, `sample_config.yaml`, and the bundle all updated to
the real catalog/schema/volume values).

**Still to do:** `staging` and `prod` provisioning — service principals,
`ingredion_en_staging` / `ingredion_en_prod` catalogs, external locations, and
scoped grants. Tracked as Phase B on the deployment-provisioning issue.

---

## Step 1 — Resource group + budget alert ✅ done

**Resource group**
- Portal → search "Resource groups" → **+ Create**
- Subscription: trial subscription (shows as "Azure subscription 1" / "Free Trial")
- Resource group name: `rg-bronze-json-loader-dev`
- Region: Central India
- Review + create → Create

**Budget alert**
- Portal → search "Cost Management + Billing" → **Budgets** → **+ Add**
- Scope: subscription
- Amount: $150 (leaves headroom under the $200 cap)
- Alert thresholds: 50%, 75%, 90% (emails the sign-up address automatically)
- Save

Validated: resource group shows "Succeeded" in Resource groups list; budget
appears under Cost Management → Budgets.

---

## Step 2 — ADLS Gen2 storage account + containers ✅ done

**Storage account**
- Portal → "Storage accounts" → **+ Create**
- Resource group: `rg-bronze-json-loader-dev`
- Region: Central India
- Performance: Standard, Redundancy: LRS
- Advanced tab → **Enable hierarchical namespace** (must be set at creation —
  no converting a plain Blob account to ADLS Gen2 later)

**Confirmed actual storage account name:** `ingredionenpkgdev` (visible in
Azure Portal → Storage accounts → Containers view). Earlier drafts of this doc
and downstream configs assumed the account name matched the container name
(`ingredion`) — it does not. Any `abfss://` URL in configs or notebooks must
use:
```
abfss://ingredion@ingredionenpkgdev.dfs.core.windows.net/
```
not `abfss://ingredion@ingredion.dfs.core.windows.net/`.

**Container / directory structure** (matches the existing `ingredion` naming
already used in `config/order_bronze.yaml`'s `source_path`):
- Container: `ingredion`
  - `ingredion/raw/` — landing zone for incoming JSON files
  - `ingredion/bronze/` — reserved for any bronze-adjacent artifacts (checkpoints, schema location, etc. if streaming is used later)
  - `ingredion/quarantine/` — reserved, though the package's own `write_quarantine()` writes to a Delta *table*, not this folder — this is just for any manually-quarantined raw files

Validated: storage account "Succeeded" with hierarchical namespace enabled;
`ingredion/raw`, `ingredion/bronze`, `ingredion/quarantine` all present.

---

## Step 3 — Databricks workspace + Unity Catalog

**Workspace type decision: Serverless (not Hybrid)**

Azure now asks "Serverless" vs "Hybrid" at workspace creation (GA March 2026).
Chosen: **Serverless**, because:
- Free Trial subscriptions have a hard 4-vCPU regional quota, not eligible for
  increase without upgrading to Pay-As-You-Go. Classic/Hybrid clusters need
  4+ cores minimum (driver alone) — this is the #1 reason trial Databricks
  setups fail to start any compute.
- Serverless compute runs in Databricks' own compute plane, not the Azure
  subscription's VM quota — sidesteps the problem entirely.
- Hybrid also provisions a permanent managed resource group (VNet, NAT
  gateway, etc.) that costs something just by existing. Serverless has none
  of that.
- Serverless workspaces can still connect to an existing ADLS Gen2 account
  (our `ingredion` container) — not limited to Databricks-managed storage.
- `directory_ingestion.py`'s `dbutils.fs.ls`-first strategy and the
  `databricks.yml` jobs (no pinned cluster spec) are already shaped for
  serverless-first execution.

**Create the workspace**
- Portal → "Azure Databricks" → **+ Create**
- Resource group: `rg-bronze-json-loader-dev`
- Workspace name: `dbx-bronze-json-loader-dev`
- Region: Central India
- Pricing tier: Premium (only real option now that Standard is blocked)
- **Workspace type: Serverless**
- Review + create → Create (takes 5-10 min)

Validated: workspace shows "Succeeded"; "Launch Workspace" opens the Databricks UI.

---

## Step 4 — Access Connector for Azure Databricks + storage permissions

Unity Catalog needs an identity it can use to read/write your `ingredion`
container. On Azure, that identity is an **Access Connector for Azure
Databricks** (a first-party resource wrapping a system-assigned managed
identity) — this is workspace-type agnostic, same flow for serverless or
hybrid.

**A. Create the Access Connector**
- Portal → search "Access Connector for Azure Databricks" → **+ Create**
- Resource group: `rg-bronze-json-loader-dev`
- Name: `ac-bronze-json-loader-dev`
- Region: Central India
- Identity type: **System-assigned managed identity**
- Review + create → Create

**B. Grant that identity access to the storage account** (detailed)

Prerequisite: you need the **Owner** or **User Access Administrator** role on
the storage account to do this — if you created the storage account yourself
under your trial subscription, you already have it.

1. Portal → go to your storage account (`ingredionenpkgdev`).
2. Left menu → **Access control (IAM)**.
3. Click **+ Add** (top of page) → **Add role assignment**.
4. **Role** tab: in the search box type `Storage Blob Data Contributor` → select it → **Next**.
5. **Members** tab:
   - "Assign access to": select **Managed identity**.
   - Click **+ Select members**.
   - A panel opens on the right. Under "Managed identity" dropdown, choose **Access connector for Azure Databricks**.
   - In the search box below it, type `ac-bronze-json-loader-dev` (or whatever you named the connector) → click it to select → click **Select** at the bottom of the panel.
6. Click **Review + assign** → **Review + assign** again to confirm.
7. It applies almost instantly — refresh the IAM page and you should see `ac-bronze-json-loader-dev` listed under the **Storage Blob Data Contributor** role assignments (check the **Role assignments** tab, not just Overview).

Validated: Access Connector `ac-bronze-json-loader-dev` created; `Storage Blob Data Contributor` role assignment visible on the storage account's IAM → Role assignments tab.

---

## Step 5 — Storage credential + external location (Unity Catalog)

Two UC objects, created in this order (credential must exist before the
external location can reference it).

**Get the connector's resource ID**
- Azure portal → `ac-bronze-json-loader-dev` → Overview → copy **Resource ID**
  (format: `/subscriptions/<sub-id>/resourceGroups/rg-bronze-json-loader-dev/providers/Microsoft.Databricks/accessConnectors/ac-bronze-json-loader-dev`)

**Storage credential** (Databricks UI, not Azure portal)
- Sidebar → **Catalog** → **Connect** → **Credentials** → **Create credential**
- Type: Azure Managed Identity
- Name: `cred-ingredion-storage`
- Access connector ID: paste the resource ID above
- Managed Identity ID: leave blank (only needed for user-assigned identities)
- Create

**External location**
- Catalog → **Connect** → **External Locations** → **Create external location** → **Manual** → Next
- Name: `ext-ingredion`
- URL: `abfss://ingredion@ingredionenpkgdev.dfs.core.windows.net/`
- Storage credential: `cred-ingredion-storage`
- Create → **Test connection** should pass

**File events permissions (optional, needed for Auto Loader efficiency later — not required for batch/directory ingestion)**
Test connection initially warned "File events permissions not verified." Fixed
by granting the Access Connector (`ac-bronze-json-loader-dev`) 3 more roles:
- `Storage Account Contributor` — scope: storage account
- `Storage Queue Data Contributor` — scope: storage account
- `EventGrid EventSubscription Contributor` — scope: **resource group** (not the storage account — different scope from the other two)

Then: External location `ext-ingredion` → Edit → Enable file events → Auto-fill
access connector ID → Save → Test connection again → should be fully green.

**Troubleshooting log (issues actually hit, in order):**

1. *"The queue is currently still being deleted by Azure. Please wait a few
   seconds before retrying the validation."* — transient eventual-consistency
   delay after a previous failed test-connection attempt left a storage queue
   mid-teardown. Not a permissions problem. Fix: wait 30-60s, click **Test
   connection** again.

2. *"Microsoft.EventGrid is not registered in Azure Subscription ..."* —
   fresh trial subscriptions don't have every resource provider pre-registered.
   Fix: Subscription → **Settings** (expand in left nav) → **Resource
   providers** → search `EventGrid` → select **Microsoft.EventGrid** → click
   **Register** → wait ~1-2 min for status to flip to "Registered" → retry
   Test connection.

Validated: EventGrid provider registered; `ext-ingredion` external location created and Test connection passed fully green (including file events).

---

## Step 6 — Schema + external volume ✅ done (names differ from original plan)

Originally planned to reuse the `default` schema under a `workspace` catalog
(zero config changes). Decided instead to create a dedicated schema — matches
the medallion-layer naming already used throughout the project
(`bronze_ingest`, `bronze_writer.py`, etc.) and keeps `default` untouched
for other work.

**Actual names created** (confirmed via Catalog Explorer UI —
catalog → schema → Volumes tab; these differ from the catalog/schema/volume
names originally planned in earlier drafts of this doc):

| | Originally planned | Actually created |
|---|---|---|
| Catalog | `workspace` | `ingredion_en_dev` |
| Schema | `bronze` | `ingredion_dev` |
| Volume | `ingredion` | `ext-ingredion-dev` |

**Validate:**
```python
dbutils.fs.ls("/Volumes/ingredion_en_dev/ingredion_dev/ext-ingredion-dev/")
```

Validated: schema `ingredion_en_dev.ingredion_dev` and external volume
`ingredion_en_dev.ingredion_dev.ext-ingredion-dev` created;
`dbutils.fs.ls("/Volumes/ingredion_en_dev/ingredion_dev/ext-ingredion-dev/")`
returned successfully with no error. Also confirmed working in practice —
used as the pytest scratch location for `tests/test_directory_ingestion.py`
(see `docs/testing_directory_ingestion.md`).

**Config files updated to use these real values** (previously tracked as a
pending task — now applied):
- `config/order_bronze.yaml`: `schema_name: "ingredion_dev"`,
  `source_path: "/Volumes/ingredion_en_dev/ingredion_dev/ext-ingredion-dev/"`
- `sample_config.yaml`: same two fields
- `databricks.yml`: `catalog` variable default → `ingredion_en_dev`,
  `schema_name` base_parameter for `bronze_directory_ingestion` →
  `ingredion_dev`

**Note:** `bronze_orders_ingestion` (the job that originally used
`order_bronze.yaml`) has since been disabled (commented out) — it was a
sample/test job, not needed for ongoing production use. See
`docs/testing_end_to_end_deployment.md`.

**Layout note:** the values above are still current, but the bundle has
since moved to a single root `databricks.yml` with `dev`/`staging`/`prod`
targets; the per-environment catalog/schema/volume are now set per target
there, and job definitions live in `bronze_layer/resources/`. See the
"Deployment" section of `bronze_layer/README.md`.

---

## Step 7 — Databricks CLI + authentication ✅ done

**Install**
- macOS/Linux: `curl -fsSL https://raw.githubusercontent.com/databricks/setup-cli/main/install.sh | sh`
- Windows: `winget install Databricks.DatabricksCLI`
- Verify: `databricks -v`

**Version matters.** Validated on **CLI v1.9.0**. Asset Bundle path-resolution
rules have changed across CLI versions — Step 9's troubleshooting log depends
on this version's behaviour, so note yours if it differs.

**Authenticate (OAuth U2M — interactive browser login, no service principal needed for solo dev use)**
- Get workspace URL from the workspace Overview page in the portal (e.g. `https://adb-xxxxxxxxxxxxxxxx.x.azuredatabricks.net`)
- `databricks auth login --host <workspace-url>`
- Browser opens, log in with the same Azure AD identity used for the portal
- Name the profile when prompted, e.g. `bronze-json-loader-dev`
- Non-secret config (host, profile name) goes to `~/.databrickscfg`; the OAuth token itself lives in the OS keychain, not that file

**Verify**
```bash
databricks current-user me --profile bronze-json-loader-dev
```
Returns your user JSON (`userName`, `displayName`, SCIM schemas).

Validated: profile created, `current-user me` returned the expected identity.

**On OAuth vs. a personal access token.** OAuth U2M is the enterprise-grade
choice here, not a shortcut — the token is short-lived and lives in the OS
keychain rather than a file. A long-lived PAT is the thing to avoid. This
human login is *not* on the production path: it exists to bootstrap, because
creating service principals requires an already-authenticated identity, and
something has to deploy `dev`. Staging and prod deploy via service principals
(Phase B) and, later, GitHub OIDC federation.

---

## Step 8 — Local deploy prerequisites ✅ done

The bundle builds the `bronze_ingest` wheel **locally** before uploading it,
so the environment you deploy *from* needs the Python build tooling. The
Databricks CLI alone is not enough.

```bash
cd bronze_layer
pip install -e ".[dev]"      # includes build, pytest, pyspark, delta-spark
# or minimally:  pip install build
```

Validated: without it, `bundle deploy` fails at the artifact step before ever
reaching the workspace:

```
Building bronze_ingest_wheel...
Error: build failed bronze_ingest_wheel, error: exit status 1, output:
> python -m build --wheel
python.exe: No module named build
```

---

## Step 9 — Validate the bundle ✅ done (no deploy, no cost)

```bash
databricks bundle validate -t dev --profile bronze-json-loader-dev
```

**Run from the repository root** — the bundle lives at the root
`databricks.yml`, not inside `bronze_layer/`.

Expected output:

```
Name: ingredion_enhancement_package
Target: dev
Workspace:
  Host: https://adb-7405607398572130.10.azuredatabricks.net
  User: <you>@example.com
  Path: /Workspace/Users/<you>@example.com/.bundle/ingredion_enhancement_package/dev

Validation OK!
```

`validate` requires an authenticated workspace connection — it is not an
offline syntax check. It resolves variables, the current user, and file paths
against the real workspace.

**Troubleshooting log (issues actually hit, in order):**

1. *`Error: no value assigned to required variable run_as_service_principal`*
   — the CLI requires **every declared variable to resolve for the selected
   target, even variables that target never references**. `dev` has no
   `run_as` block (it deploys as you), so the variable was left unset and
   validation refused. Fixed by giving `dev` an inert value in its own
   `variables:` block while keeping **no top-level default**, so staging and
   prod still fail fast when the real service principal isn't supplied.

2. *`Error: notebook bronze_layer/resources/bronze_layer/notebooks/run_directory_ingestion.py not found`*
   — note the doubled path segment. Paths inside an **included resource file**
   resolve relative to **that file's own directory**, not the bundle root. So
   `./bronze_layer/notebooks/...` declared in
   `bronze_layer/resources/bronze_ingest_jobs.yml` resolved from
   `bronze_layer/resources/`. Fixed by using `../notebooks/...` and
   `../dist/*.whl`. Paths in the root `databricks.yml` itself (such as the
   `artifacts:` build path) stay root-relative — same rule, different
   declaring file.

Validated: `Validation OK!` with no `--var` arguments needed for `dev`.

---

## Step 10 — Deploy to dev ✅ done

```bash
databricks bundle deploy -t dev --profile bronze-json-loader-dev
```

Expected output:

```
Building bronze_ingest_wheel...
Uploading bronze_layer/dist/bronze_ingest-0.4.0-py3-none-any.whl...
Uploading bundle files to /Workspace/Users/<you>/.bundle/ingredion_enhancement_package/dev/files...
Deploying resources...
Updating deployment state...
Deployment complete!
```

`mode: development` prefixes every resource with your username and force-pauses
schedules, so concurrent deploys by different people cannot collide and nothing
starts running on a timer by accident.

**Troubleshooting log (issues actually hit, in order):**

1. *`Error: cannot create resources.jobs.bronze_directory_ingestion: Libraries
   field is not supported for serverless task, please specify libraries in
   environment. (400 INVALID_PARAMETER_VALUE)`*
   — a task-level `libraries:` field is the **classic-compute** form. This
   workspace is serverless (Step 3) and rejects it. Dependencies must be a
   job-level environment that tasks bind to:

   ```yaml
   environments:
     - environment_key: default
       spec:
         client: "3"
         dependencies:
           - ../dist/*.whl
   tasks:
     - task_key: ingest_directory
       environment_key: default
   ```

   `client: "3"` is accepted by this workspace — confirmed by the successful
   deploy. DAB also resolves the `../dist/*.whl` glob inside
   `environments[].spec.dependencies`, the same as it did inside `libraries:`.

Validated: `Deployment complete!`, wheel uploaded as
`bronze_ingest-0.4.0-py3-none-any.whl`.

---

## Step 11 — Smoke run: end-to-end ingestion ✅ done

```bash
databricks bundle run bronze_directory_ingestion -t dev --profile bronze-json-loader-dev
```

This is the step that proves the *ingestion* path, not just the control plane:
the wheel installs on serverless compute, the notebook imports `bronze_ingest`
**with no `sys.path` manipulation**, and data lands in Unity Catalog.

Expected:

```
Notebook exited: SUCCESS: 1 unit(s) ingested, 1 skipped
```

Then confirm in Catalog Explorer or SQL:

```sql
SELECT * FROM ingredion_en_dev.ingredion_dev.<filename>_bronze;
SELECT * FROM ingredion_en_dev.ingredion_dev._ingestion_audit ORDER BY started_at DESC;
SELECT * FROM ingredion_en_dev.ingredion_dev._schema_registry;
```

**Troubleshooting log (issues actually hit, in order):**

1. *`Notebook exited: FAILED: 1/1 file(s) failed: ['.../multi_file']`*, while
   the log above it said `Folder ... contains no JSON files - skipping` — the
   message and the outcome contradicted each other. A folder with no JSON was
   returning `status: "failed"`, because the result vocabulary had only
   `success` and `failed`. That fired failure alerting for a non-event and,
   worse, made a skip indistinguishable from a real write error in the summary.
   Fixed by adding a `skipped` status; the job task now fails only on genuine
   failures.

2. *`SUCCESS: 0 unit(s) ingested, 1 skipped`* — a clean exit that proves
   nothing about ingestion. The Volume held no top-level JSON and one empty
   subfolder, so the run never reached a write. **A smoke run that ingests
   zero units has not validated the write path.** Drop a small file such as
   `{"order_id": 1, "amount": 10}` into
   `/Volumes/ingredion_en_dev/ingredion_dev/ext-ingredion-dev/raw/JSON/` and
   re-run before believing the environment works.

Validated: `SUCCESS: 1 unit(s) ingested, 1 skipped`, with a bronze table
created and correct rows in both `_ingestion_audit` and `_schema_registry`.

---

## What Steps 7-11 established

| | |
|---|---|
| Wheel built, uploaded, installed on serverless compute | ✅ |
| Notebook imports `bronze_ingest`, no `sys.path` manipulation | ✅ |
| Directory discovery against the UC Volume | ✅ |
| Delta table written to `ingredion_en_dev` | ✅ |
| `_ingestion_audit` and `_schema_registry` rows written | ✅ |
| UC write permissions from the job's execution context | ✅ |

**Caveat carried into Phase B:** all of this ran as the **deploying user**,
who holds broad rights on the catalog. Staging and prod service principals get
deliberately narrow grants (`USE CATALOG` + `USE SCHEMA` + `CREATE TABLE` on
their own schema), so permission failures are the most likely thing to surface
there. The audit and schema-registry tables are the ones to watch: they live in
the same catalog but are written by a different code path, and are easy to
forget when granting.

---

## Step 12 — Staging and prod provisioning (not done)

Phase B. Summarised here so the runbook stays the single entry point; the
authoritative checklist is on the deployment-provisioning issue.

**Azure Portal** — two containers in the existing `ingredionenpkgdev` storage
account (`ingredion-staging`, `ingredion-prod`). No new storage account and no
new Access Connector: the existing connector already holds `Storage Blob Data
Contributor` at account scope, so it reaches new containers automatically.

**Databricks account console** (`accounts.azuredatabricks.net`) — service
principals `sp-ingredion-staging` and `sp-ingredion-prod`, each with an OAuth
secret. The **Client ID** is what the bundle takes as
`run_as_service_principal`.

**Databricks workspace** — external locations for the two new containers
(reusing `cred-ingredion-storage`), then catalogs, schemas, external volumes,
and scoped grants per service principal. Verify the isolation rather than
assuming it: as the staging principal, a `SELECT` against a prod table should
be denied.

**Cost note:** catalogs, schemas, external locations and service principals are
metadata and cost nothing. The spend is serverless compute per job run plus
ADLS storage.