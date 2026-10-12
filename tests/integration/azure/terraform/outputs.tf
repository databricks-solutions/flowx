output "factory_name" {
  description = "Name of the ephemeral Azure Data Factory."
  value       = azurerm_data_factory.test.name
}

output "resource_group_name" {
  description = "Name of the run-scoped resource group."
  value       = azurerm_resource_group.test.name
}
