locals {
  fixture_root = "${path.module}/../adf"
  run_prefix   = substr(var.name_prefix, 0, 12)
  location     = data.azurerm_resource_group.test.location
  tags = {
    purpose   = "flowx-integration-test"
    ephemeral = "true"
    managedBy = "terraform"
    run       = local.run_prefix
    owner     = trimspace(var.owner_email)
  }

  pipeline_files = fileset("${local.fixture_root}/pipelines", "*.json")
  dataset_files  = fileset("${local.fixture_root}/datasets", "*.json")
  dataflow_files = fileset("${local.fixture_root}/dataflows", "*.json")
  integration_runtime_files = fileset(
    "${local.fixture_root}/integration_runtimes",
    "*.json",
  )
  linked_service_files = fileset("${local.fixture_root}/linked_services", "*.json")
  trigger_files        = fileset("${local.fixture_root}/triggers", "*.json")

  pipelines = { for filename in local.pipeline_files : filename => jsondecode(file("${local.fixture_root}/pipelines/${filename}")) }
  datasets  = { for filename in local.dataset_files : filename => jsondecode(file("${local.fixture_root}/datasets/${filename}")) }
  dataflows = { for filename in local.dataflow_files : filename => jsondecode(file("${local.fixture_root}/dataflows/${filename}")) }
  integration_runtimes = {
    for filename in local.integration_runtime_files : filename => jsondecode(
      replace(file("${local.fixture_root}/integration_runtimes/${filename}"), "<LOCATION>", local.location)
    )
  }
  linked_service_sources = {
    for filename in local.linked_service_files : filename => jsondecode(file("${local.fixture_root}/linked_services/${filename}"))
  }
  linked_services = {
    for filename in local.linked_service_files : filename => jsondecode(
      replace(
        replace(file("${local.fixture_root}/linked_services/${filename}"), "<KEY_VAULT_URI>", azurerm_key_vault.test.vault_uri),
        "<ADLS_DFS_ENDPOINT>", azurerm_storage_account.test.primary_dfs_endpoint
      )
    )
  }
  triggers = {
    for filename in local.trigger_files : filename => jsondecode(
      replace(file("${local.fixture_root}/triggers/${filename}"), "<STORAGE_ACCOUNT_ID>", azurerm_storage_account.test.id)
    )
  }

  key_vault_linked_services = {
    for filename, linked_service in local.linked_service_sources : filename => local.linked_services[filename]
    if linked_service.properties.type == "AzureKeyVault"
  }
  dependent_linked_services = {
    for filename, linked_service in local.linked_service_sources : filename => local.linked_services[filename]
    if linked_service.properties.type != "AzureKeyVault" &&
    strcontains(jsonencode(linked_service.properties), "\"type\":\"LinkedServiceReference\"") &&
    !strcontains(jsonencode(linked_service.properties), "\"referenceName\":\"key-vault\"")
  }
  base_linked_services = {
    for filename, linked_service in local.linked_service_sources : filename => local.linked_services[filename]
    if linked_service.properties.type != "AzureKeyVault" &&
    !contains(keys(local.dependent_linked_services), filename)
  }
  base_pipelines = {
    for filename, pipeline in local.pipelines : filename => pipeline
    if !strcontains(jsonencode(pipeline.properties), "\"type\":\"PipelineReference\"")
  }
  dependent_pipelines = {
    for filename, pipeline in local.pipelines : filename => pipeline
    if strcontains(jsonencode(pipeline.properties), "\"type\":\"PipelineReference\"")
  }
}

# The shared resource group is created once by setup-integration.sh; the CI identity may only work inside it.
data "azurerm_resource_group" "test" {
  name = var.resource_group_name
}

resource "azurerm_storage_account" "test" {
  name                      = "${local.run_prefix}flowxsa"
  resource_group_name       = data.azurerm_resource_group.test.name
  location                  = local.location
  account_tier              = "Standard"
  account_replication_type  = "LRS"
  account_kind              = "StorageV2"
  is_hns_enabled            = true
  min_tls_version           = "TLS1_2"
  shared_access_key_enabled = false
  tags                      = local.tags
}

resource "azapi_resource" "containers" {
  for_each  = toset(["raw", "curated"])
  type      = "Microsoft.Storage/storageAccounts/blobServices/containers@2023-05-01"
  name      = each.value
  parent_id = "${azurerm_storage_account.test.id}/blobServices/default"
  body = {
    properties = {
      publicAccess = "None"
    }
  }
}

resource "azurerm_key_vault" "test" {
  name                          = "${local.run_prefix}-flowx-kv"
  location                      = local.location
  resource_group_name           = data.azurerm_resource_group.test.name
  tenant_id                     = var.tenant_id
  sku_name                      = "standard"
  rbac_authorization_enabled    = true
  purge_protection_enabled      = false
  soft_delete_retention_days    = 7
  public_network_access_enabled = true
  tags                          = local.tags
}

resource "azurerm_data_factory" "test" {
  name                = "${local.run_prefix}-flowx-adf"
  location            = local.location
  resource_group_name = data.azurerm_resource_group.test.name
  tags                = local.tags
}

resource "azapi_resource" "key_vault_linked_services" {
  for_each  = local.key_vault_linked_services
  type      = "Microsoft.DataFactory/factories/linkedservices@2018-06-01"
  name      = each.value.name
  parent_id = azurerm_data_factory.test.id
  body      = { properties = each.value.properties }

  schema_validation_enabled = false
}

resource "azapi_resource" "integration_runtimes" {
  for_each  = local.integration_runtimes
  type      = "Microsoft.DataFactory/factories/integrationruntimes@2018-06-01"
  name      = each.value.name
  parent_id = azurerm_data_factory.test.id
  body      = { properties = each.value.properties }

  schema_validation_enabled = false
}

resource "azapi_resource" "base_linked_services" {
  for_each  = local.base_linked_services
  type      = "Microsoft.DataFactory/factories/linkedservices@2018-06-01"
  name      = each.value.name
  parent_id = azurerm_data_factory.test.id
  body      = { properties = each.value.properties }

  schema_validation_enabled = false
  depends_on                = [azapi_resource.key_vault_linked_services]
}

resource "azapi_resource" "dependent_linked_services" {
  for_each  = local.dependent_linked_services
  type      = "Microsoft.DataFactory/factories/linkedservices@2018-06-01"
  name      = each.value.name
  parent_id = azurerm_data_factory.test.id
  body      = { properties = each.value.properties }

  schema_validation_enabled = false
  depends_on                = [azapi_resource.base_linked_services]
}

resource "azapi_resource" "datasets" {
  for_each  = local.datasets
  type      = "Microsoft.DataFactory/factories/datasets@2018-06-01"
  name      = each.value.name
  parent_id = azurerm_data_factory.test.id
  body      = { properties = each.value.properties }

  schema_validation_enabled = false
  depends_on = [
    azapi_resource.key_vault_linked_services,
    azapi_resource.base_linked_services,
    azapi_resource.dependent_linked_services,
  ]
}

resource "azapi_resource" "dataflows" {
  for_each  = local.dataflows
  type      = "Microsoft.DataFactory/factories/dataflows@2018-06-01"
  name      = each.value.name
  parent_id = azurerm_data_factory.test.id
  body      = { properties = each.value.properties }

  schema_validation_enabled = false
  depends_on                = [azapi_resource.datasets]
}

resource "azapi_resource" "base_pipelines" {
  for_each  = local.base_pipelines
  type      = "Microsoft.DataFactory/factories/pipelines@2018-06-01"
  name      = each.value.name
  parent_id = azurerm_data_factory.test.id
  body      = { properties = each.value.properties }

  schema_validation_enabled = false
  depends_on = [
    azapi_resource.datasets,
    azapi_resource.dataflows,
    azapi_resource.integration_runtimes,
  ]
}

resource "azapi_resource" "dependent_pipelines" {
  for_each  = local.dependent_pipelines
  type      = "Microsoft.DataFactory/factories/pipelines@2018-06-01"
  name      = each.value.name
  parent_id = azurerm_data_factory.test.id
  body      = { properties = each.value.properties }

  schema_validation_enabled = false
  depends_on                = [azapi_resource.base_pipelines]
}

resource "azapi_resource" "triggers" {
  for_each  = local.triggers
  type      = "Microsoft.DataFactory/factories/triggers@2018-06-01"
  name      = each.value.name
  parent_id = azurerm_data_factory.test.id
  body = {
    properties = { for key, value in each.value.properties : key => value if key != "runtimeState" }
  }

  schema_validation_enabled = false
  depends_on                = [azapi_resource.base_pipelines, azapi_resource.dependent_pipelines]
}
