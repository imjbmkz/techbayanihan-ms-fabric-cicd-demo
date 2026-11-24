output "workspace_ids" {
  description = "Workspace IDs per environment."
  value       = { for k, w in fabric_workspace.env : k => w.id }
}

output "lakehouse_ids" {
  description = "IDs of the lakehouses Terraform manages (dev by default)."
  value       = { for k, l in fabric_lakehouse.env : k => l.id }
}
