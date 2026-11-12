# Microsoft Fabric CI/CD Demo — NYC TLC Yellow Taxi

End-to-end CI/CD demo on Microsoft Fabric: infrastructure with Terraform, development in a Git-connected dev workspace, and (later) automated promotion to QA and Prod with GitHub Actions + [`fabric-cicd`](https://microsoft.github.io/fabric-cicd/).

Dataset: [NYC TLC Trip Record Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page) (yellow taxi trips + taxi zone lookup).

## Architecture

| Environment | Workspace | Lakehouse |
|---|---|---|
| Dev | `ws-demo-dev` | `AwesomeLH_dev` |
| QA | `ws-demo-qa` | `AwesomeLH_qa` |
| Prod | `ws-demo-prod` | `AwesomeLH_prod` |

All three workspaces run on one Fabric F2 capacity (`fabnyctaxidemo`).

Lakehouse layers (schema-enabled lakehouse):

| Layer | Location | Purpose |
|---|---|---|
| data lake | `Files/datalake/...` | Raw source files as landed (acts like an S3 bucket) |
| raw | schema `raw` | Source files loaded to Delta tables, plus load metadata |
| cleaned | schema `cleaned` | Typed, deduplicated, validated data |
| mart | schema `mart` | Dimensions and facts |
| semantic | schema `semantic` | Reporting aggregates; Direct Lake semantic model on top |

Inter-layer transforms run as PySpark / Spark SQL notebooks, orchestrated by a data pipeline.

## Repository layout

```
.
├── .github/workflows/        # (planned) GitHub Actions: fabric-cicd deploy to QA → Prod
├── infra/terraform/
│   ├── 01-azure/             # azurerm: resource group + Fabric F2 capacity
│   └── 02-fabric/            # microsoft/fabric: 3 workspaces + 3 schema-enabled lakehouses
├── fabric/                   # (planned) ws-demo-dev Git integration folder — Fabric items live here
├── deploy/                   # (planned) fabric-cicd deploy script + parameters
└── scripts/                  # one-off helper scripts
```

### Who owns what

- **Terraform** owns the capacity, workspaces, and lakehouses (all environments).
- **Fabric Git integration** owns the items inside `ws-demo-dev` (notebooks, dataflows, pipelines, …), synced to `fabric/`.
- **fabric-cicd** (planned) deploys those items to QA and Prod. It skips the `Lakehouse` type, because Terraform already created the lakehouses.
- Schemas and `Files/` folders inside each lakehouse are created by the `nb_00_setup_lakehouse` notebook, the first step of the pipeline in every environment.

## Setup

Prerequisites: Terraform ≥ 1.8, Azure CLI, and a member account in the Entra tenant (e.g. `you@<tenant>.onmicrosoft.com`). Personal Microsoft accounts can't create Fabric capacities.

### 1. Azure: capacity (`infra/terraform/01-azure`)

```bash
az login --tenant <tenant>.onmicrosoft.com
az provider register --namespace Microsoft.Fabric
cd infra/terraform/01-azure
cp terraform.tfvars.example terraform.tfvars   # fill in subscription_id, capacity_admins
terraform init && terraform plan -out tfplan && terraform apply tfplan
```

### 2. Fabric: workspaces + lakehouses (`infra/terraform/02-fabric`)

One-off: create the Fabric Terraform provider's user-context app registration by running `scripts/create-fabric-tf-app-reg.sh` in Azure Cloud Shell (Bash). Then grant admin consent on the app registration's **API permissions** page.

```bash
TID=$(az account show --query tenantId -o tsv)
az login --tenant $TID --scope "api://$TID/fabric_terraform_provider/.default"
az fabric capacity resume --resource-group rg-fabric-demo --capacity-name fabnyctaxidemo
cd infra/terraform/02-fabric
terraform init && terraform plan -out tfplan && terraform apply tfplan
```

### 3. Dev workspace → GitHub

In `ws-demo-dev`, open **Workspace settings → Git integration**, then connect this repo, branch `main`, Git folder `fabric`.

### Cost control

Pause the capacity when you're not using it, because compute is only billed while it's active:

```bash
az fabric capacity suspend --resource-group rg-fabric-demo --capacity-name fabnyctaxidemo
```

## Notes

- Terraform state is local and gitignored for now. It will move to a remote backend (Azure Storage) once Terraform runs in CI.
- Never commit `terraform.tfvars`, `*.tfstate`, or plan files (`tfplan`). They contain environment identifiers.
