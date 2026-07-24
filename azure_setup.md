# Azure Databricks setup — bronze_json_loader

Living runbook for the dev/validation environment used to test `bronze_json_loader`
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

**Status as of last session:** Steps 1-6 done and validated — resource group,
budget alert, ADLS Gen2 storage (`ingredion` container), Databricks serverless
workspace, Unity Catalog wiring (Access Connector, storage credential,
external location with file events), and a dedicated schema + external volume
(see corrected Step 6 for actual names). Paused before Step 7 (CLI/bundle
setup). One pending action item: the config file edits listed under Step 6
haven't been applied to the repo yet — tracked as a separate task.

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
(`bronze_json_loader`, `bronze_writer.py`, etc.) and keeps `default` untouched
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

**Config files must use these real values, not the originally-planned
`workspace`/`bronze`/`ingredion` names — tracked as a separate task, not yet
applied to the repo:**
- `config/order_bronze.yaml`: `schema_name: "ingredion_dev"`,
  `source_path: "/Volumes/ingredion_en_dev/ingredion_dev/ext-ingredion-dev/"`
- `sample_config.yaml`: same two fields
- `databricks.yml`: `catalog` variable default → `ingredion_en_dev`,
  `schema_name` base_parameter for `bronze_directory_ingestion` →
  `ingredion_dev`

---

## Step 7 — Databricks CLI + authentication (not done, paused here)

(Step numbering note: test ingestion was intentionally deferred rather than
done as Step 7 — it'll be picked up later as its own step once you're ready.)

**Install**
- macOS/Linux: `curl -fsSL https://raw.githubusercontent.com/databricks/setup-cli/main/install.sh | sh`
- Windows: `winget install Databricks.DatabricksCLI`
- Verify: `databricks -v`

**Authenticate (OAuth U2M — interactive browser login, no service principal needed for solo dev use)**
- Get workspace URL from the workspace Overview page in the portal (e.g. `https://adb-xxxxxxxxxxxxxxxx.x.azuredatabricks.net`)
- `databricks auth login --host <workspace-url>`
- Browser opens, log in with the same Azure AD identity used for the portal
- Name the profile when prompted, e.g. `bronze-json-loader-dev`
- Non-secret config (host, profile name) goes to `~/.databrickscfg`; the OAuth token itself lives in the OS keychain, not that file

**Verify**
- `databricks current-user me --profile bronze-json-loader-dev` → should return user JSON

**Validate the bundle (no deploy yet, no cost)**
- `cd bronze_json_loader && databricks bundle validate -t dev --profile bronze-json-loader-dev`

*(checkpoint pending)*