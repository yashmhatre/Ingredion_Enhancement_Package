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
for actual values (`ingredion_en.ingredion_dev.ext-ingredion-dev`, not
`workspace.bronze.ingredion`). Confirmed via live testing in the JSON reader
and directory ingestion validation work (see `docs/testing_json_reader.md` and
`docs/testing_directory_ingestion.md`).

**Catalog rename note:** the catalog was originally created as
`ingredion_en_dev` and has since been renamed to **`ingredion_en`**, because
a catalog named `_dev` now holds staging and production schemas too. Steps
below use the current name. The schema `ingredion_dev` and the volume
`ext-ingredion-dev` keep their original names.

**Environment layout note:** environments are separated by **schema**, not by
catalog - one catalog (`ingredion_en`) with `ingredion_dev` / `ingredion_stg`
/ `ingredion_prd`, and one Entra ID service principal per non-dev
environment. Grants are therefore made at schema level; see the Deployment
section of `bronze_layer/README.md`.

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

**Still to do:** `staging` and `prod` provisioning — the Entra ID service
principals exist and their client IDs are wired into the bundle, but the
`ingredion_stg` / `ingredion_prd` schemas and their scoped grants are not yet
created. Tracked as Phase B on the deployment-provisioning issue.

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
| Catalog | `workspace` | `ingredion_en` |
| Schema | `bronze` | `ingredion_dev` |
| Volume | `ingredion` | `ext-ingredion-dev` |

**Validate:**
```python
dbutils.fs.ls("/Volumes/ingredion_en/ingredion_dev/ext-ingredion-dev/")
```

Validated: schema `ingredion_en.ingredion_dev` and external volume
`ingredion_en.ingredion_dev.ext-ingredion-dev` created;
`dbutils.fs.ls("/Volumes/ingredion_en/ingredion_dev/ext-ingredion-dev/")`
returned successfully with no error. Also confirmed working in practice —
used as the pytest scratch location for `tests/test_directory_ingestion.py`
(see `docs/testing_directory_ingestion.md`).

**Config files updated to use these real values** (previously tracked as a
pending task — now applied):
- `config/order_bronze.yaml`: `schema_name: "ingredion_dev"`,
  `source_path: "/Volumes/ingredion_en/ingredion_dev/ext-ingredion-dev/"`
- `sample_config.yaml`: same two fields
- `databricks.yml`: `catalog` variable default → `ingredion_en`,
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
pip install -e ".[dev]"      # pytest, pyspark, delta-spark for running tests
```

**Deploying needs nothing beyond the CLI.** `databricks.yml` builds the
wheel with `python -m pip wheel`, so a deploy works the same from a laptop,
a notebook, or the Databricks web terminal.

That is a change from how this originally read. The build command was
`python -m build --wheel`, which needs the `build` package, and this step
used to tell you to install it. Validated failure when you did not:

```
Building bronze_ingest_wheel...
Error: build failed bronze_ingest_wheel, error: exit status 1, output:
> python -m build --wheel
python.exe: No module named build
```

It happened twice, months apart — the second time from Databricks compute,
where the Python environment is ephemeral and the fix would not have
survived a cluster restart anyway. The build command now uses pip, which is
always present, so the prerequisite is gone rather than documented.

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
SELECT * FROM ingredion_en.ingredion_dev.<filename>_bronze;
SELECT * FROM ingredion_en.ingredion_dev._ingestion_audit ORDER BY started_at DESC;
SELECT * FROM ingredion_en.ingredion_dev._schema_registry;
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
   `/Volumes/ingredion_en/ingredion_dev/ext-ingredion-dev/raw/JSON/` and
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
| Delta table written to `ingredion_en` | ✅ |
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

## Step 12 — Staging and prod provisioning (partially done)

Phase B. Summarised here so the runbook stays the single entry point; the
authoritative checklist is on the deployment-provisioning issue.

**Done — Entra ID service principals.** Registered in the tenant and added to
Databricks. Their Application (client) IDs are identifiers rather than
credentials and are set per target in the root `databricks.yml`, so a prod
deploy cannot run under the staging identity:

| | Application (client) ID |
|---|---|
| `staging-databricks-service-principal` | `6ea945e0-2b4f-4746-b8f7-e7be51adc35a` |
| `prod-sp` | `8cbc9ba5-b4be-47b7-8a1d-576eb7d1a2e9` |

Tenant: `01984097-743d-456d-9101-11a2e04cb219`.

**Also required — `Service Principal: User` role for whoever deploys.** This
is an account-level permission on the *service principal object*, and is
separate from every Unity Catalog grant below. Without it the deploy fails at
resource creation:

```
Error: cannot create resources.jobs.bronze_directory_ingestion: Cannot bind the
service principal provided in 'run_as' field (staging-databricks-service-principal)
to the job. The user creating or updating the job must have 'servicePrincipal.user'
role on the service principal. (403 PERMISSION_DENIED)
```

Databricks **account console** → **User management** → **Service principals**
→ select the principal → **Permissions** → add the deploying user with role
**Service Principal: User**. Repeat for both principals.

**`Manage` does not imply `Use`.** The console states this outright, and it is
the easiest way to think the permission is already set: being able to
administer a service principal is a separate grant from being able to run
jobs as it. `Use` is the one `run_as` requires.

**Grant `Use` to named principals, not to `account users`.** A default
`account users` → `Use` grant means every user in the Databricks account can
run jobs as that service principal. The principal's own UC grants still limit
what it can reach, but anyone who can act as it inherits exactly that reach —
which defeats the point of a scoped deploy identity. Grant `Use` to the
deploying user, and later the CI federation identity, explicitly. Same
discipline as granting `USE CATALOG` and nothing more at catalog level.

Easy to miss because it reads like a data-access problem and is not one:
`run_as` means the deployer is asking Databricks to let a job execute *as*
another identity, so the deployer must be authorised to act on that identity.
Granting every UC privilege in the world would not fix it.

**Set this in the Databricks account console, not the Azure portal — even
though these are Entra ID service principals.** The two systems own different
halves:

| | Owns |
|---|---|
| Entra ID | that the principal exists, its credentials, lifecycle, conditional access, sign-in logs |
| Databricks | who may bind a job to run *as* it, and what it can reach in Unity Catalog |

`servicePrincipal.user` is a Databricks account-level role on Databricks' own
representation of the principal. Being Owner on the Azure subscription, or
owner of the Entra app registration, conveys none of it — Azure RBAC does not
reach inside Databricks' permission model. Choosing Entra ID over
Databricks-managed principals changed *where the identity lives*, not *who
governs its use inside Databricks*.

Note this requirement disappears under OIDC federation (#113): there the
deploying identity *is* the service principal, so nothing is binding on
anyone else's behalf. One more reason to move deploys into CI rather than
leaving them manual.

**Not done — schemas and grants.** In the Databricks workspace:

```sql
CREATE SCHEMA IF NOT EXISTS ingredion_en.ingredion_stg;
CREATE SCHEMA IF NOT EXISTS ingredion_en.ingredion_prd;

-- Catalog level: USE CATALOG and nothing more. A single
-- GRANT SELECT ON CATALOG would flatten the whole boundary at once.
GRANT USE CATALOG ON CATALOG ingredion_en TO `6ea945e0-2b4f-4746-b8f7-e7be51adc35a`;
GRANT USE CATALOG ON CATALOG ingredion_en TO `8cbc9ba5-b4be-47b7-8a1d-576eb7d1a2e9`;

GRANT USE SCHEMA, CREATE TABLE, MODIFY, SELECT
  ON SCHEMA ingredion_en.ingredion_stg TO `6ea945e0-2b4f-4746-b8f7-e7be51adc35a`;
GRANT USE SCHEMA, CREATE TABLE, MODIFY, SELECT
  ON SCHEMA ingredion_en.ingredion_prd TO `8cbc9ba5-b4be-47b7-8a1d-576eb7d1a2e9`;

-- The source volume lives in the ingredion_dev schema, so staging and prod
-- need USE SCHEMA there purely to reach their own subpath.
GRANT USE SCHEMA ON SCHEMA ingredion_en.ingredion_dev TO `6ea945e0-2b4f-4746-b8f7-e7be51adc35a`;
GRANT USE SCHEMA ON SCHEMA ingredion_en.ingredion_dev TO `8cbc9ba5-b4be-47b7-8a1d-576eb7d1a2e9`;
GRANT READ VOLUME ON VOLUME ingredion_en.ingredion_dev.`ext-ingredion-dev`
  TO `6ea945e0-2b4f-4746-b8f7-e7be51adc35a`;
GRANT READ VOLUME ON VOLUME ingredion_en.ingredion_dev.`ext-ingredion-dev`
  TO `8cbc9ba5-b4be-47b7-8a1d-576eb7d1a2e9`;
```

**Verify the isolation rather than assuming it.** As the staging principal, a
`SELECT` against a table in `ingredion_prd` should be denied. A grant that
looks right and a grant that works are different things.

**Known gap — source files are not isolated.** All three environments read
subpaths of one volume, and `READ VOLUME` is granted at volume granularity;
Unity Catalog has no sub-path grant. Any principal that can read its own
subpath can read `PROD/Raw/` too. Tables, audit and registry are properly
isolated by schema; source files are not. Giving each environment its own
volume would close this and costs nothing but the metadata objects.

**Then deploy:**

```bash
databricks bundle deploy -t staging --profile bronze-json-loader-dev
databricks bundle run bronze_directory_ingestion -t staging --profile bronze-json-loader-dev
```

Do this manually before wiring OIDC federation. A manual deploy separates
"does the service principal have the right grants" from "does federation
work" — debugging both at once is considerably harder, and the grants are
the more likely problem.

**Cost note:** schemas, external locations, volumes and service principals are
metadata and cost nothing. The spend is serverless compute per job run plus
ADLS storage.
