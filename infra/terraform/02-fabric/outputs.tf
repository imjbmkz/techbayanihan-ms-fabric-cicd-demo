output "workspace_ids" {
  description = "Workspace IDs per environment (needed later for GitHub Actions / fabric-cicd)."
  value       = { for k, w in fabric_workspace.env : k => w.id }
}

output "lakehouse_ids" {
  description = "Lakehouse IDs per environment (needed later for fabric-cicd parameter.yml)."
  value       = { for k, l in fabric_lakehouse.env : k => l.id }
}
