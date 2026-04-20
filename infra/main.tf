provider "azurerm" {
  features {}
}

resource "azurerm_resource_group" "rg" {
  name     = "data-pipeline-rg"
  location = "West Europe"
}

resource "azurerm_storage_account" "storage" {
  name                     = "sebastiandehlsenpipeline"
  resource_group_name      = azurerm_resource_group.rg.name
  location                 = azurerm_resource_group.rg.location
  account_tier             = "Standard"
  account_replication_type = "LRS"
}