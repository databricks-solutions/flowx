output "factory_name" {
  description = "Name of the ephemeral Azure Data Factory."
  value       = azurerm_data_factory.test.name
}

output "resource_group_name" {
  description = "Name of the shared resource group the run deploys into."
  value       = data.azurerm_resource_group.test.name
}

output "storage_account_id" {
  description = "Resource ID substituted into the Blob Events trigger fixture."
  value       = azurerm_storage_account.test.id
  sensitive   = true
}

output "key_vault_uri" {
  description = "Vault URI substituted into linked-service fixtures."
  value       = azurerm_key_vault.test.vault_uri
}

output "adls_dfs_endpoint" {
  description = "DFS endpoint substituted into linked-service fixtures."
  value       = azurerm_storage_account.test.primary_dfs_endpoint
}

output "location" {
  description = "Azure location substituted into integration-runtime fixtures."
  value       = local.location
}
