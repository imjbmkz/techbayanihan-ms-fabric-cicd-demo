output "resource_group_name" {
  value = azurerm_resource_group.rg.name
}

output "capacity_id" {
  description = "Azure resource ID of the Fabric capacity."
  value       = azurerm_fabric_capacity.this.id
}

output "capacity_name" {
  description = "Capacity name; the Fabric provider (02-fabric) looks it up by this name."
  value       = azurerm_fabric_capacity.this.name
}
