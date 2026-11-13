data "fabric_capacity" "this" {
  display_name = var.capacity_name
}

# One workspace per environment, all on the same F2 capacity.
resource "fabric_workspace" "env" {
  for_each = var.environments

  display_name = each.value.workspace_name
  description  = "NYC TLC yellow taxi CI/CD demo - ${upper(each.key)}"
  capacity_id  = data.fabric_capacity.this.id
}

# One schema-enabled lakehouse per environment.
# Terraform owns the lakehouses; Git/fabric-cicd own everything else
# (notebooks, pipelines, etc.). Schemas and folders inside the lakehouse
# are created by the nb_00_setup_lakehouse notebook, not Terraform.
resource "fabric_lakehouse" "env" {
  for_each = var.environments

  display_name = each.value.lakehouse_name
  description  = "Data lake (Files) + raw / cleaned / mart / semantic schemas - ${upper(each.key)}"
  workspace_id = fabric_workspace.env[each.key].id

  configuration = {
    enable_schemas = true
  }
}

# The GitHub Actions service principal (fabric-cicd) can publish items to QA and Prod.
resource "fabric_workspace_role_assignment" "deployer" {
  for_each = var.deployer_object_id == "" ? toset([]) : toset(var.deploy_target_environments)

  workspace_id = fabric_workspace.env[each.key].id
  principal = {
    id   = var.deployer_object_id
    type = "ServicePrincipal"
  }
  role = var.deployer_role
}
