# Microsoft Fabric CI/CD Demo — NYC TLC Yellow Taxi

**Build. Ship. Scale.: CI/CD for Fabric** — demo repository for the Techbayanihan 2026 workshop.

An end-to-end Microsoft Fabric project you can replicate in your own tenant:

- **Terraform** provisions the Fabric capacity (Azure) and the Dev / QA / Prod workspaces (Fabric).
- **Git integration** connects the Dev workspace to this repo: notebooks, the data pipeline, the semantic model (TMDL) and the report (PBIR) are all code.
- **Fabric deployment pipelines** promote Dev → Test (QA) → Production.
- The data: [NYC TLC Trip Record Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page) — yellow taxi trips + taxi zone lookup.

```
             Terraform                          Git (this repo)
   ┌──────────────────────────┐           ┌──────────────────────┐
   │ Azure: F2 capacity       │           │ fabric/  (items)     │
   │ Fabric: 3 workspaces     │           │ infra/   (Terraform) │
   │         + dev lakehouse  │           └──────────▲───────────┘
   └──────────────────────────┘                      │ commit / update
                                                     │
   ws-demo-dev ──── deployment pipeline ────▶ ws-demo-qa ────▶ ws-demo-prod
   (Git-connected)                            (not in Git)     (not in Git)
```

---

## Contents

1. [Architecture](#architecture)
2. [Repository layout](#repository-layout)
3. [Prerequisites](#prerequisites)
4. [Setup](#setup)
   - [Step 1 — Azure tenant and an organizational account](#step-1--azure-tenant-and-an-organizational-account)
   - [Step 2 — Install Azure CLI and Terraform](#step-2--install-azure-cli-and-terraform)
   - [Step 3 — Fork and clone the repo](#step-3--fork-and-clone-the-repo)
   - [Step 4 — Azure: Fabric capacity (Terraform `01-azure`)](#step-4--azure-fabric-capacity-terraform-01-azure)
   - [Step 5 — Fabric tenant settings](#step-5--fabric-tenant-settings)
   - [Step 6 — App registration for the Fabric Terraform provider](#step-6--app-registration-for-the-fabric-terraform-provider)
   - [Step 7 — Fabric: workspaces (Terraform `02-fabric`)](#step-7--fabric-workspaces-terraform-02-fabric)
   - [Step 8 — Connect the Dev workspace to GitHub](#step-8--connect-the-dev-workspace-to-github)
   - [Step 9 — Point the items at your lakehouse](#step-9--point-the-items-at-your-lakehouse)
   - [Step 10 — First run in Dev](#step-10--first-run-in-dev)
   - [Step 11 — Deployment pipeline: Dev → QA → Prod](#step-11--deployment-pipeline-dev--qa--prod)
5. [Day-to-day workflow](#day-to-day-workflow)
6. [Notebooks and data model](#notebooks-and-data-model)
7. [Cost control and teardown](#cost-control-and-teardown)
8. [Troubleshooting](#troubleshooting)

---

## Architecture

| Environment | Workspace | Lakehouse | Source of the items |
|---|---|---|---|
| Dev | `ws-demo-dev` | `AwesomeLH` | Git (this repo, folder `fabric/`) |
| QA | `ws-demo-qa` | `AwesomeLH` | Deployment pipeline (Test stage) |
| Prod | `ws-demo-prod` | `AwesomeLH` | Deployment pipeline (Production stage) |

All three workspaces run on one **F2** Fabric capacity (`fabnyctaxidemo`).

**Same lakehouse name everywhere.** Deployment pipelines pair items by *type + name*; the workspace name already says which environment it is. Notebooks use only relative paths (`Files/...`) and two-part table names (`raw.table`), so the same code runs in every environment.

**Code moves, data doesn't.** Deployments copy item definitions only — never lakehouse files or tables. Each environment builds its own data by running the pipeline.

Lakehouse layers (schema-enabled lakehouse):

| Layer | Location | Content |
|---|---|---|
| data lake | `Files/datalake/nyc_tlc/...` | source files as downloaded (parquet, csv, shapefile zip) |
| raw | schema `raw` | files loaded to Delta + lineage columns (`_source_file`, `_ingested_at`, ...) |
| cleaned | schema `cleaned` | typed data with data quality flags; passed / quarantined materialized lake views |
| mart | schema `mart` | star schema: dimensions + fact |
| semantic | schema `semantic` | report-ready aggregates for Power BI (Direct Lake) |
| ops | schema `ops` | ingestion run logs |

## Repository layout

```
.
├── fabric/                                  # ← Dev workspace Git folder (managed by Fabric Git integration)
│   ├── AwesomeLH.Lakehouse/                 # lakehouse definition (metadata only, no data)
│   ├── nb_00_setup_lakehouse.Notebook/      # one-time: schemas + Files folders
│   └── projects/nyc_tlc/                    # workspace folder "projects/nyc_tlc"
│       ├── nb_01_ingest_nyc_tlc.Notebook/   # source → data lake
│       ├── nb_02_nyc_tlc_raw.Notebook/      # data lake → raw (PySpark, incremental)
│       ├── nb_03_nyc_tlc_cleaned.Notebook/  # raw → cleaned (Spark SQL, DQ + MLVs)
│       ├── nb_04_nyc_tlc_mart.Notebook/     # cleaned → mart (Spark SQL)
│       ├── nb_05_nyc_tlc_semantic.Notebook/ # mart → semantic (Spark SQL)
│       ├── pl_nyc_tlc__end_to_end.DataPipeline/
│       ├── nyc-semantic-model.SemanticModel/  # TMDL (Direct Lake)
│       └── nyc-dashboad.Report/               # PBIR
├── infra/terraform/
│   ├── 01-azure/                            # azurerm: resource group + Fabric F2 capacity
│   └── 02-fabric/                           # microsoft/fabric: 3 workspaces (+ dev lakehouse)
└── scripts/
    ├── create-fabric-tf-app-reg.sh          # app registration for the Fabric Terraform provider
    └── rebind-fabric-ids.sh                 # point notebooks + semantic model at your lakehouse
```

**Who owns what**

| Owner | Objects |
|---|---|
| Terraform | capacity, workspaces (and optionally the Dev lakehouse) |
| Git (Dev workspace) | everything inside `ws-demo-dev`: lakehouse definition, notebooks, pipeline, semantic model, report |
| Deployment pipeline | the copies of those items in QA and Prod |
| Pipeline runs | the data in each environment's lakehouse |

---

## Prerequisites

| You need | Notes |
|---|---|
| An **Azure subscription** | A free account works for the experiment ([azure.microsoft.com/free](https://azure.microsoft.com/free)). F2 costs roughly US$0.36/hour while running and nothing while paused — check your region's price. |
| An **organizational (Entra ID) account** in that tenant | Fabric and Fabric capacities **don't accept personal Microsoft accounts** (Outlook.com, Gmail). Step 1 shows how to create one. |
| **Fabric admin** rights in the tenant | To change tenant settings (Step 5). In your own tenant, the Global Administrator has them. |
| A **GitHub account** | To fork this repo and connect the Dev workspace. |
| **Azure CLI**, **Terraform ≥ 1.8**, **git** | Step 2. |

> **Have a Fabric trial instead?** If your organizational account can start a Fabric trial, you can skip Step 4 (the paid F2) and use the trial capacity's name in Step 7.

---

## Setup

### Step 1 — Azure tenant and an organizational account

If you signed up for Azure with a personal email, Azure created an Entra tenant for you (e.g. `yournameoutlook.onmicrosoft.com`), but your personal account is only a *guest* there. Create a member account:

1. Azure portal → **Microsoft Entra ID → Users → New user → Create new user**
   - User principal name: e.g. `demo@<yourtenant>.onmicrosoft.com`
   - **Assignments** tab → add role **Global Administrator** (makes you Fabric admin too)
2. **Subscriptions → *your subscription* → Access control (IAM) → Add role assignment** → **Owner** (or Contributor) → select the new user.
3. In a private browser window, sign in to [portal.azure.com](https://portal.azure.com) and [app.fabric.microsoft.com](https://app.fabric.microsoft.com) as the new user (you'll set a new password and MFA).

**Use this account for everything below** — Azure CLI, Terraform, Fabric.

### Step 2 — Install Azure CLI and Terraform

**macOS** (Homebrew):

```bash
xcode-select --install                     # git; skip if already installed
/bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"   # skip if `brew` works
brew tap hashicorp/tap
brew install hashicorp/tap/terraform azure-cli
```

**Windows** (PowerShell):

```powershell
winget install -e --id Git.Git
winget install -e --id Microsoft.AzureCLI
winget install -e --id Hashicorp.Terraform
# close and reopen the terminal afterwards
```

Check:

```bash
az version
terraform version
```

### Step 3 — Fork and clone the repo

Fork this repository on GitHub (the Dev workspace will commit to **your** fork), then:

```bash
git clone https://github.com/<your-github-user>/<your-fork>.git
cd <your-fork>
```

### Step 4 — Azure: Fabric capacity (Terraform `01-azure`)

Sign in and register the Fabric resource provider (one-time per subscription):

```bash
az login --tenant <yourtenant>.onmicrosoft.com      # pick the organizational account
az account show -o table                             # note the subscription Id

az provider register --namespace Microsoft.Fabric
az provider show --namespace Microsoft.Fabric --query registrationState -o tsv   # wait for "Registered"
```

Configure and apply:

```bash
cd infra/terraform/01-azure
cp terraform.tfvars.example terraform.tfvars
```

Edit `terraform.tfvars`:

```hcl
subscription_id = "<your subscription id>"
capacity_admins = ["demo@<yourtenant>.onmicrosoft.com"]   # member account(s), not a personal email

# optional
# location      = "southeastasia"
# capacity_name = "fabnyctaxidemo"     # lowercase letters/numbers, unique per region
# capacity_sku  = "F2"
```

```bash
terraform init
terraform plan -out tfplan        # expect: 2 to add (resource group + capacity)
terraform apply tfplan
```

Billing starts as soon as the capacity exists. **Pause it whenever you're not using it:**

```bash
az config set extension.use_dynamic_install=yes_without_prompt
az fabric capacity suspend --resource-group rg-fabric-demo --capacity-name fabnyctaxidemo
az fabric capacity resume  --resource-group rg-fabric-demo --capacity-name fabnyctaxidemo
```

> `terraform.tfvars`, `*.tfstate` and plan files are gitignored. Keep the state files: without them Terraform can't see what it created. `terraform plan` is read-only and safe to demo.

### Step 5 — Fabric tenant settings

[app.fabric.microsoft.com](https://app.fabric.microsoft.com) → ⚙️ **Settings → Admin portal → Tenant settings** (use the search box):

| Setting | Value | Why |
|---|---|---|
| *Users can create Fabric items* | Enabled | usually on already |
| *Users can synchronize workspace items with their Git repositories* | Enabled | Git integration |
| *Users can sync workspace items with GitHub repositories* | **Enabled** | off by default; needed for GitHub |
| *Users can create and use materialized lake views* (if listed) | Enabled | `nb_03` creates MLVs |

Changes can take ~15 minutes to apply.

### Step 6 — App registration for the Fabric Terraform provider

The Fabric Terraform provider can't use a plain `az login` token; it needs a one-time *user-context* app registration in your tenant.

1. Azure portal → **Cloud Shell** (`>_`) → **Bash**, signed in as the organizational account.
2. Upload `scripts/create-fabric-tf-app-reg.sh` and run it:

   ```bash
   bash create-fabric-tf-app-reg.sh
   ```

3. **Microsoft Entra ID → App registrations → All applications → `fabric_terraform_provider` → API permissions → Grant admin consent → Yes.**

Then, on your machine, sign in with the provider's scope:

```bash
TID=$(az account show --query tenantId -o tsv)
az login --tenant "$TID" --scope "api://$TID/fabric_terraform_provider/.default"
```

(PowerShell: `$TID = az account show --query tenantId -o tsv` and the same `az login` line with `"api://$TID/fabric_terraform_provider/.default"`.)

### Step 7 — Fabric: workspaces (Terraform `02-fabric`)

The capacity must be **active** (not paused):

```bash
az fabric capacity resume --resource-group rg-fabric-demo --capacity-name fabnyctaxidemo

cd ../02-fabric
terraform init
terraform plan  -var='lakehouse_environments=[]' -out tfplan    # expect: 3 to add (workspaces)
terraform apply tfplan
```

This creates `ws-demo-dev`, `ws-demo-qa` and `ws-demo-prod` on the capacity.

> **Why `lakehouse_environments=[]`?** This repo already contains the `AwesomeLH` lakehouse definition (`fabric/AwesomeLH.Lakehouse/`), so Git creates the Dev lakehouse in Step 8. If Terraform also created one, the Git sync would collide with it. (The default, `["dev"]`, is for starting a project from an empty repo.) QA and Prod lakehouses are always created by the deployment pipeline in Step 11.

Optional overrides (`terraform.tfvars`): `capacity_name` (if you used a trial or a different name), `lakehouse_name`.

### Step 8 — Connect the Dev workspace to GitHub

1. **Create a GitHub token** — GitHub → Settings → Developer settings → Personal access tokens → **Fine-grained tokens → Generate new token**
   - Repository access: *Only select repositories* → your fork
   - Permissions → Repository → **Contents: Read and write**
2. **Connect** — in Fabric, open `ws-demo-dev` → **Workspace settings → Git integration → GitHub**
   - **Add account**: display name, the token, your fork's URL → **Connect**
   - Branch: **`main`** · Git folder: **`fabric`**
   - **Connect and sync**. The workspace is empty, so Fabric pulls everything from Git: the `AwesomeLH` lakehouse, the notebooks (incl. the `projects/nyc_tlc` folder), the pipeline, the semantic model and the report.

> If the **semantic model** or **report** fails to import on this first sync, carry on — Step 9 points the model at your new lakehouse, and the next **Update all** creates them.

### Step 9 — Point the items at your lakehouse

The notebooks (default lakehouse) and the semantic model (Direct Lake source) still contain the **original author's** workspace and lakehouse IDs. Replace them with yours:

1. Open `AwesomeLH` in `ws-demo-dev`. The URL contains both IDs:
   `https://app.fabric.microsoft.com/groups/<workspace-id>/lakehouses/<lakehouse-id>`
2. In your clone:

   ```bash
   git pull                                    # get the commits Fabric may have made
   scripts/rebind-fabric-ids.sh <workspace-id> <lakehouse-id>
   git diff --stat                             # 6 notebooks + expressions.tmdl
   git commit -am "Point notebooks and semantic model at my dev lakehouse"
   git push
   ```

3. In `ws-demo-dev`: **Source control → Updates → Update all**.
4. Check: open any notebook — `AwesomeLH` should be its default lakehouse.

### Step 10 — First run in Dev

1. **Set the month range** (keeps F2 run times short): open `pl_nyc_tlc__end_to_end` → activity `nb_01_ingest_nyc_tlc` → *Settings → Base parameters* → `START_MONTH` / `END_MONTH`, e.g. `2026-01` / `2026-02` (TLC publishes with a ~2-month lag). Save.
2. **Run `nb_00_setup_lakehouse` once** (creates the `raw`, `cleaned`, `mart`, `semantic`, `ops` schemas and `Files/` folders). It's a one-time setup per environment and isn't part of the pipeline.
3. **Run `pl_nyc_tlc__end_to_end`** and watch it in *Monitor*:
   `nb_01` (download) → `nb_02` (raw) → `nb_03` (cleaned) → `nb_04` (mart) → `nb_05` (semantic).
4. **Check the data** in the lakehouse SQL analytics endpoint:

   ```sql
   SELECT year_month, SUM(trips) AS trips
   FROM semantic.nyc_tlc__sem_daily_zone_trips
   GROUP BY year_month ORDER BY year_month;

   SELECT dataset, period, status, rows, error_message
   FROM ops.ingestion_log ORDER BY started_at DESC;
   ```

5. **Open the report** `nyc-dashboad`. If visuals show errors, open `nyc-semantic-model` → **Refresh** (reframes Direct Lake on the new tables).
6. Commit any changes Fabric shows under **Source control** (e.g. the pipeline's month range).

### Step 11 — Deployment pipeline: Dev → QA → Prod

**Create the pipeline (once)**

1. In `ws-demo-dev` → **Create deployment pipeline** (or Workspaces → **Deployment pipelines → New pipeline**).
2. Name it (e.g. `dp-nyc-tlc`), keep the three stages **Development / Test / Production**.
3. Assign the workspaces: Development = `ws-demo-dev`, Test = `ws-demo-qa`, Production = `ws-demo-prod`.

**Deploy Dev → Test**

1. Select the **Test** stage → compare → select all items → **Deploy**.
   The first deployment **creates** `AwesomeLH` (empty), the notebooks, the pipeline, the semantic model and the report in `ws-demo-qa`.
2. **Check the bindings in QA**
   - *Notebooks*: open one — its default lakehouse should be **QA's** `AwesomeLH` (auto-binding). If not, add a **deployment rule** on the Test stage for the notebook's default lakehouse and redeploy.
   - *Semantic model*: the Direct Lake source can still point at the **Dev** lakehouse. Check the model's settings / source in QA and rebind it to QA's `AwesomeLH` if needed.
   - *Report*: binds to the semantic model in the same stage automatically.
3. **Build QA's data**: run `nb_00_setup_lakehouse` once, then `pl_nyc_tlc__end_to_end`, then refresh the semantic model.
4. Open the report in QA and compare with Dev.

**Deploy Test → Production** — same steps on the Production stage.

> Later deployments only update definitions — they never overwrite QA/Prod data. A change that affects existing data (new column, logic change) is applied by **running the pipeline** in each environment after deploying.

---

## Day-to-day workflow

```
change ──▶ Dev workspace ──commit──▶ main ──▶ Deploy Dev→Test ──▶ run in QA ──▶ Deploy Test→Prod ──▶ run in Prod
   ▲                                   │
   └── or: branch + pull request ──────┘ (merge, then Source control → Update all in Dev)
```

- **Changes made in Fabric** (notebooks, pipeline, report in the browser): **Source control → Commit** in `ws-demo-dev`.
- **Changes made as code** (e.g. measures in TMDL, SQL in a notebook file): branch → pull request → merge → **Source control → Updates → Update all** in `ws-demo-dev`.
- **Release**: deployment pipeline Dev → Test, run the pipeline in QA, check, then Test → Production.
- On a Mac there is no Power BI Desktop: edit the report and model in the browser (web authoring / web modeling), or edit the TMDL/PBIR files and use a pull request.

---

## Notebooks and data model

### Notebooks (`pl_nyc_tlc__end_to_end`)

| # | Notebook | Language | Does |
|---|---|---|---|
| 0 | `nb_00_setup_lakehouse` | PySpark | **one-time per environment**: schemas + `Files/` folders (not in the pipeline) |
| 1 | `nb_01_ingest_nyc_tlc` | PySpark | TLC website → data lake: monthly trip parquet files, `taxi_zone_lookup.csv`, `taxi_zones.zip`; logs to `ops.ingestion_runs` / `ops.ingestion_log` |
| 2 | `nb_02_nyc_tlc_raw` | PySpark | data lake → raw. Trips **incremental by file** (only files not yet loaded); zone lookup full refresh. `FULL_RELOAD = True` rebuilds |
| 3 | `nb_03_nyc_tlc_cleaned` | Spark SQL | `__dq` table with 8 data quality rules; `__cleaned` (passed) and `__quarantined` (failed) materialized lake views |
| 4 | `nb_04_nyc_tlc_mart` | Spark SQL | `dim_date`, `dim_zone`, `dim_vendor`, `dim_payment_type`, `dim_rate_code`, `fact_yellow_taxi_trips` |
| 5 | `nb_05_nyc_tlc_semantic` | Spark SQL | `sem_daily_zone_trips` (day × pickup zone KPIs) |

`nb_01` parameters (set on the pipeline activity): `START_MONTH`, `END_MONTH`, `TRIP_TYPE`, `INCLUDE_TRIPS`, `INCLUDE_REFERENCE`, `OVERWRITE`, `FAIL_ON_MISSING`, … — **names are case-sensitive** and must match the notebook's parameters cell.

### Table naming convention

`<schema>.<domain>__<name>` — the schema is the layer; double underscores separate domain, dataset and role.

| Layer | Pattern | Example |
|---|---|---|
| raw | `raw.<domain>__<dataset>__raw` | `raw.nyc_tlc__yellow_taxi_trips__raw` |
| cleaned | `cleaned.<domain>__<dataset>__dq` / `__cleaned` / `__quarantined` | `cleaned.nyc_tlc__yellow_taxi_trips__cleaned` |
| mart | `mart.<domain>__dim_<entity>` / `mart.<domain>__fact_<dataset>` | `mart.nyc_tlc__fact_yellow_taxi_trips` |
| semantic | `semantic.<domain>__sem_<subject>` | `semantic.nyc_tlc__sem_daily_zone_trips` |

### Semantic model and report

`nyc-semantic-model` (Direct Lake on OneLake) — a star with one fact and two dimensions:

```
mart.nyc_tlc__dim_date [date_key] 1 ──▶ * semantic.nyc_tlc__sem_daily_zone_trips [date_key]
mart.nyc_tlc__dim_zone [location_id] 1 ──▶ * semantic.nyc_tlc__sem_daily_zone_trips [pickup_location_id]
```

Measures (folder *KPIs*): `Total Trips`, `Revenue`, `Avg Revenue per Trip`, `Card Share`; (folder *Trip profile*): `Total Passengers`, `Avg Distance (mi)`, `Avg Duration (min)`, `Tips % of Fare`, `Trips per Day`. Amounts are USD.

`nyc-dashboad` — page *Overview*: Month / Borough / Weekend slicers, four KPI cards, daily trips line, trips by borough bar, pickup-zone table.

---

## Cost control and teardown

- **Pause the capacity** when idle (compute isn't billed while paused; OneLake storage is, but it's tiny):
  `az fabric capacity suspend --resource-group rg-fabric-demo --capacity-name fabnyctaxidemo`
- **Budget alert**: Azure portal → **Cost Management → Budgets → Add** (e.g. US$30/month, email at 50/80/100 %). Alerts lag a few hours — pausing is what actually saves money.
- **Load fewer months** in Dev/QA (`START_MONTH` / `END_MONTH`); each environment loads its own copy.

**Teardown** (resume the capacity first so Fabric can delete the workspaces):

```bash
cd infra/terraform/02-fabric && terraform destroy -var='lakehouse_environments=[]'
cd ../01-azure && terraform destroy
```

Delete the deployment pipeline in Fabric and the `fabric_terraform_provider` app registration in Entra ID if you no longer need them.

---

## Troubleshooting

| Symptom | Cause / fix |
|---|---|
| *Unsupported account — you cannot create a Microsoft capacity using a personal account* | Sign in to Azure/Fabric with the `@<tenant>.onmicrosoft.com` member account (Step 1), and use it in `capacity_admins`. |
| `MissingSubscriptionRegistration` on `terraform apply` | `az provider register --namespace Microsoft.Fabric` and wait for *Registered*. |
| Fabric provider authentication errors in `02-fabric` | Re-run the scoped `az login … --scope api://<tenant-id>/fabric_terraform_provider/.default` (Step 6); check admin consent was granted. |
| Workspaces can't be assigned / capacity not found | Resume the capacity; check `capacity_name`. |
| GitHub not offered under Git integration | Enable *Users can sync workspace items with GitHub repositories* (Step 5), wait ~15 min. |
| `terraform plan` on a new machine wants to create everything | The state files are local and gitignored — copy `terraform.tfstate` from the original machine. `plan` is read-only either way; never `apply` without the state. |
| Pipeline ignores the month range | Notebook-activity parameter names must match the parameters cell exactly (`START_MONTH`, not `start_month`). |
| `nb_02` append fails with a schema mismatch | The raw table was created with another schema — run `nb_02` once with `FULL_RELOAD = True`. |
| **Update all** fails: *measure cannot be created because a column with the same name already exists* | Measure and column names share one case-insensitive namespace per table (e.g. measure `Trips` vs column `trips`) — rename the measure. |
| Report in QA shows Dev numbers, or errors after deployment | The Direct Lake model still points at the Dev lakehouse — rebind it to QA's `AwesomeLH`, then refresh. |
| QA/Prod lakehouse is empty after deploying | Expected: deployments never copy data. Run `nb_00_setup_lakehouse` once, then the pipeline. |
| A second lakehouse appears in QA after deploying | Lakehouse names differ between stages, so the pipeline didn't pair them. Use the same name in every workspace. |
