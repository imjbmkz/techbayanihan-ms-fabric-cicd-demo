#!/usr/bin/env bash
# One-off: create the service principal GitHub Actions uses to deploy to Fabric.
# Uses OIDC federated credentials, so no client secret is created or stored.
# Run in Azure Cloud Shell (Bash) or Git Bash, signed in as your
# @<tenant>.onmicrosoft.com admin user.
set -euo pipefail

REPO="${REPO:-imjbmkz/techbayanihan-ms-fabric-cicd-demo}"
APP_NAME="${APP_NAME:-sp-fabric-cicd-github}"
GROUP_NAME="${GROUP_NAME:-sg-fabric-api-service-principals}"
ENVIRONMENTS="${ENVIRONMENTS:-qa prod}"

tenant_id=$(az account show --query tenantId -o tsv)

echo "Creating app registration ${APP_NAME}"
app_id=$(az ad app create --display-name "$APP_NAME" --query appId -o tsv)
sp_object_id=$(az ad sp create --id "$app_id" --query id -o tsv 2>/dev/null \
  || az ad sp show --id "$app_id" --query id -o tsv)

for env in $ENVIRONMENTS; do
  echo "Adding federated credential for GitHub environment '${env}'"
  az ad app federated-credential create --id "$app_id" --parameters "{
    \"name\": \"github-${env}\",
    \"issuer\": \"https://token.actions.githubusercontent.com\",
    \"subject\": \"repo:${REPO}:environment:${env}\",
    \"audiences\": [\"api://AzureADTokenExchange\"]
  }" --output none
done

echo "Adding the service principal to security group ${GROUP_NAME}"
group_id=$(az ad group show --group "$GROUP_NAME" --query id -o tsv 2>/dev/null \
  || az ad group create --display-name "$GROUP_NAME" --mail-nickname "$GROUP_NAME" --query id -o tsv)
az ad group member add --group "$group_id" --member-id "$sp_object_id" 2>/dev/null || true

cat <<OUT

Done.
  AZURE_CLIENT_ID  (GitHub variable) : ${app_id}
  AZURE_TENANT_ID  (GitHub variable) : ${tenant_id}
  Service principal object ID        : ${sp_object_id}   <- Terraform var deployer_object_id
  Security group                     : ${GROUP_NAME}     <- Fabric tenant setting

Next:
  1. Fabric Admin portal > Tenant settings > Developer settings >
     "Service principals can use Fabric APIs" (or "...call Fabric public APIs"):
     Enabled for the security group ${GROUP_NAME}.
  2. terraform apply in infra/terraform/02-fabric with -var="deployer_object_id=${sp_object_id}"
  3. GitHub repo > Settings > Environments: create "qa" (and "prod").
     Settings > Secrets and variables > Actions > Variables:
     AZURE_CLIENT_ID=${app_id}, AZURE_TENANT_ID=${tenant_id}
OUT
