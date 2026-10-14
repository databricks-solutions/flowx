#!/usr/bin/env bash
set -euo pipefail
export_dir="${RUNNER_TEMP}/adf-export"
factory_name="$(terraform -chdir=tests/integration/azure/terraform output -raw factory_name)"
resource_group="$(terraform -chdir=tests/integration/azure/terraform output -raw resource_group_name)"
mkdir -p "${export_dir}"/{pipelines,datasets,linked_services,dataflows,integration_runtimes,triggers}
for mapping in "pipelines:pipelines" "datasets:datasets" "linkedservices:linked_services" "dataflows:dataflows" "integrationruntimes:integration_runtimes" "triggers:triggers"; do
  resource_type="${mapping%%:*}"
  directory="${mapping##*:}"
  subscription_id="$(az account show --query id --output tsv)"
  base_url="https://management.azure.com/subscriptions/${subscription_id}/resourceGroups/${resource_group}/providers/Microsoft.DataFactory/factories/${factory_name}/${resource_type}"
  az rest --method get --url "${base_url}?api-version=2018-06-01" --output json |
    jq -c '.value[]' |
    while IFS= read -r resource; do
      name="$(jq -r '.name' <<<"${resource}" | awk -F/ '{print $NF}')"
      jq '{name, properties}' <<<"${resource}" > "${export_dir}/${directory}/${name}.json"
    done
done
{
  echo "ADF_EXPORT_DIR=${export_dir}"
  echo "ADF_CASE_SOURCE_DIR=${export_dir}"
  printf 'ADF_CASE_STORAGE_ACCOUNT_ID=%s\n' "$(terraform -chdir=tests/integration/azure/terraform output -raw storage_account_id)"
  printf 'ADF_CASE_KEY_VAULT_URI=%s\n' "$(terraform -chdir=tests/integration/azure/terraform output -raw key_vault_uri)"
  printf 'ADF_CASE_ADLS_DFS_ENDPOINT=%s\n' "$(terraform -chdir=tests/integration/azure/terraform output -raw adls_dfs_endpoint)"
  printf 'ADF_CASE_LOCATION=%s\n' "$(terraform -chdir=tests/integration/azure/terraform output -raw location)"
} >> "${GITHUB_ENV}"
