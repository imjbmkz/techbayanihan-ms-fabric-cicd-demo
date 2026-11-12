#!/usr/bin/env bash
# One-off: create the user-context app registration the Fabric Terraform
# provider needs. Run in Azure Cloud Shell (Bash) or Git Bash, signed in
# as your @<tenant>.onmicrosoft.com admin user.
# Source: "Terraform Provider for Microsoft Fabric: #1 Accelerating first steps using the CLIs"
set -euo pipefail

name=fabric_terraform_provider
tenant_id=$(az account show --query tenantId -otsv)
uri="api://${tenant_id}/${name}"

echo "Creating app registration ${name} (${uri})"
az ad app create --display-name "$name" --identifier-uris "$uri" --output none

echo "Adding delegated API permissions (Microsoft Graph + Power BI/Fabric)"
az ad app update --id "$uri" --required-resource-accesses '[{"resourceAppId":"00000003-0000-0000-c000-000000000000","resourceAccess":[{"id":"e1fe6dd8-ba31-4d61-89e7-88639da4683d","type":"Scope"},{"id":"b340eb25-3456-403f-be2f-af7a0d370277","type":"Scope"}]},{"resourceAppId":"00000009-0000-0000-c000-000000000000","resourceAccess":[{"id":"4eabc3d1-b762-40ff-9da5-0e18fdf11230","type":"Scope"},{"id":"b2f1b2fa-f35c-407c-979c-a858a808ba85","type":"Scope"},{"id":"445002fb-a6f2-4dc1-a81e-4254a111cd29","type":"Scope"},{"id":"8b01a991-5a5a-47f8-91a2-84d6bfd72c02","type":"Scope"}]}]'

echo "Exposing the 'access' scope"
az ad app update --id "$uri" --set api='{"acceptMappedClaims":null,"knownClientApplications":[],"oauth2PermissionScopes":[{"adminConsentDescription":"Allows connection to backend services for Microsoft Fabric Terraform Provider","adminConsentDisplayName":"Microsoft Fabric Terraform Provider","id":"1ca1271c-e2c0-437c-af9a-3a92e745a24d","isEnabled":true,"type":"User","userConsentDescription":"Allows connection to backend services for Microsoft Fabric Terraform Provider","userConsentDisplayName":"Microsoft Fabric Terraform Provider","value":"access"}],"preAuthorizedApplications":[],"requestedAccessTokenVersion":null}'

echo "Pre-authorizing Azure CLI / Azure PowerShell / Power BI clients"
az ad app update --id "$uri" --set api='{"acceptMappedClaims":null,"knownClientApplications":[],"oauth2PermissionScopes":[{"adminConsentDescription":"Allows connection to backend services for Microsoft Fabric Terraform Provider","adminConsentDisplayName":"Microsoft Fabric Terraform Provider","id":"1ca1271c-e2c0-437c-af9a-3a92e745a24d","isEnabled":true,"type":"User","userConsentDescription":"Allows connection to backend services for Microsoft Fabric Terraform Provider","userConsentDisplayName":"Microsoft Fabric Terraform Provider","value":"access"}],"preAuthorizedApplications":[{"appId":"871c010f-5e61-4fb1-83ac-98610a7e9110","delegatedPermissionIds":["1ca1271c-e2c0-437c-af9a-3a92e745a24d"]},{"appId":"00000009-0000-0000-c000-000000000000","delegatedPermissionIds":["1ca1271c-e2c0-437c-af9a-3a92e745a24d"]},{"appId":"1950a258-227b-4e31-a9cf-717495945fc2","delegatedPermissionIds":["1ca1271c-e2c0-437c-af9a-3a92e745a24d"]},{"appId":"04b07795-8ddb-461a-bbee-02f9e1bf7b46","delegatedPermissionIds":["1ca1271c-e2c0-437c-af9a-3a92e745a24d"]}],"requestedAccessTokenVersion":null}'

echo "Adding you as owner"
az ad app owner add --id "$uri" --owner-object-id "$(az ad signed-in-user show --query id -otsv)"

echo
echo "Done. Next, on your own machine run:"
echo "  az login --tenant ${tenant_id} --scope ${uri}/.default"
