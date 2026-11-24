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

# Schema-enabled lakehouse with the SAME name in every workspace.
# Terraform creates it in dev only (var.lakehouse_environments); the Fabric
# deployment pipeline creates and pairs the QA/Prod copies on first deployment.
# Lakehouse data is never deployed: each environment fills its own lakehouse
# by running pl_nyc_tlc__end_to_end. Schemas/folders come from nb_00_setup_lakehouse.
resource "fabric_lakehouse" "env" {
  for_each = toset(var.lakehouse_environments)

  display_name = var.lakehouse_name
  description  = "Data lake (Files) + raw / cleaned / mart / semantic schemas - ${upper(each.key)}"
  workspace_id = fabric_workspace.env[each.key].id

  configuration = {
    enable_schemas = true
  }
}
