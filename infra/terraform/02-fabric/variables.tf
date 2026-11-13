variable "capacity_name" {
  description = "Display name of the Fabric capacity created in 01-azure (must be Active, not paused, when you apply)."
  type        = string
  default     = "fabnyctaxidemo"
}

variable "environments" {
  description = "One entry per environment: workspace name and lakehouse name."
  type = map(object({
    workspace_name = string
    lakehouse_name = string
  }))
  default = {
    dev = {
      workspace_name = "ws-demo-dev"
      lakehouse_name = "AwesomeLH_dev"
    }
    qa = {
      workspace_name = "ws-demo-qa"
      lakehouse_name = "AwesomeLH_qa"
    }
    prod = {
      workspace_name = "ws-demo-prod"
      lakehouse_name = "AwesomeLH_prod"
    }
  }
}

variable "deployer_object_id" {
  description = "Object ID of the GitHub Actions deployment service principal (scripts/create-github-deployer-sp.sh). Empty = no role assignment."
  type        = string
  default     = ""
}

variable "deployer_role" {
  description = "Workspace role for the deployment service principal."
  type        = string
  default     = "Contributor"
}

variable "deploy_target_environments" {
  description = "Environments the deployment service principal can deploy to (dev is deployed through Git integration)."
  type        = list(string)
  default     = ["qa", "prod"]
}
