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
