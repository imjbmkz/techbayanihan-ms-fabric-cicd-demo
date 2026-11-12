resource "azurerm_resource_group" "rg" {
  name     = var.resource_group_name
  location = var.location
  tags     = var.tags
}

resource "azurerm_fabric_capacity" "this" {
  name                   = var.capacity_name
  resource_group_name    = azurerm_resource_group.rg.name
  location               = azurerm_resource_group.rg.location
  administration_members = var.capacity_admins

  sku {
    name = var.capacity_sku
    tier = "Fabric"
  }

  tags = var.tags
}
