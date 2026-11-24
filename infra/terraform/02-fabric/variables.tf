variable "capacity_name" {
  description = "Display name of the Fabric capacity created in 01-azure (must be Active, not paused, when you apply)."
  type        = string
  default     = "fabnyctaxidemo"
}

variable "environments" {
  description = "One workspace per environment."
  type = map(object({
    workspace_name = string
  }))
  default = {
    dev  = { workspace_name = "ws-demo-dev" }
    qa   = { workspace_name = "ws-demo-qa" }
    prod = { workspace_name = "ws-demo-prod" }
  }
}

variable "lakehouse_name" {
  description = "Lakehouse name, identical in every workspace. Fabric deployment pipelines pair items by type + name, so the name must not carry the environment."
  type        = string
  default     = "AwesomeLH"
}

variable "lakehouse_environments" {
  description = <<-EOT
    Environments where Terraform creates the lakehouse. Default: dev only.
    QA and Prod lakehouses are created (and paired) by the Fabric deployment
    pipeline on the first Dev -> QA -> Prod deployment. Set to ["dev", "qa", "prod"]
    only if you deploy without deployment pipelines (e.g. fabric-cicd).
  EOT
  type        = list(string)
  default     = ["dev"]

  validation {
    condition     = alltrue([for e in var.lakehouse_environments : contains(["dev", "qa", "prod"], e)])
    error_message = "lakehouse_environments may only contain dev, qa, prod."
  }
}
