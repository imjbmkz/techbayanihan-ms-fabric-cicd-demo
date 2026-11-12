variable "subscription_id" {
  description = "Azure subscription ID that will host the Fabric capacity."
  type        = string
}

variable "location" {
  description = "Azure region for the resource group and capacity."
  type        = string
  default     = "southeastasia"
}

variable "resource_group_name" {
  description = "Resource group for the demo resources."
  type        = string
  default     = "rg-fabric-demo"
}

variable "capacity_name" {
  description = "Fabric capacity name (lowercase letters and numbers only, 3-63 chars)."
  type        = string
  default     = "fabnyctaxidemo"

  validation {
    condition     = can(regex("^[a-z][a-z0-9]{2,62}$", var.capacity_name))
    error_message = "capacity_name must be 3-63 chars, lowercase letters and numbers only, starting with a letter."
  }
}

variable "capacity_sku" {
  description = "Fabric capacity SKU (F2, F4, F8, ...)."
  type        = string
  default     = "F2"
}

variable "capacity_admins" {
  description = "UPNs of Fabric capacity administrators (must be member users of the tenant, e.g. josh@<tenant>.onmicrosoft.com)."
  type        = list(string)
}

variable "tags" {
  description = "Tags applied to all resources."
  type        = map(string)
  default = {
    project = "nyctaxi-fabric-cicd"
    purpose = "demo"
  }
}
