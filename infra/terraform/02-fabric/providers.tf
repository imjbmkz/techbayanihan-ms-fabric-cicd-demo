terraform {
  required_version = ">= 1.8, < 2.0"

  required_providers {
    fabric = {
      source  = "microsoft/fabric"
      version = "~> 1.0"
    }
  }
}

# Auth: uses the token cached by
#   az login --scope api://<tenantId>/fabric_terraform_provider/.default
# (see scripts/create-fabric-tf-app-reg.sh)
provider "fabric" {
  # Set to true only if `terraform plan` says an attribute you use is preview-only.
  preview = false
}
