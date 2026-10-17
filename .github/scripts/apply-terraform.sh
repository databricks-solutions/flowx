#!/usr/bin/env bash
set -euo pipefail
# Lint checks that run before Azure resources are created. A pull request that edits this script can skip
# them; the environment's required reviewers are the guard for that.
plan="${TF_STATE_DIR}/tfplan"
fail() {
  echo "::error::$1"
  exit 1
}

# The lock file is pinned by hash. It lists only registry.terraform.io/hashicorp/azurerm 4.81.0 and
# registry.terraform.io/azure/azapi 2.13.0 with their package hashes, and -lockfile=readonly makes init refuse
# any other provider. Update the hash below when Dependabot updates the lock file.
lock_sha256=be5b0a021819cdcab3a19877cf81400a8f0611b554c6daeea683460210d1e1c5
[[ "$(sha256sum < .terraform.lock.hcl | cut -d ' ' -f 1)" == "${lock_sha256}" ]] ||
  fail "The Terraform lock file changed; only the pinned azurerm and azapi providers may be used."
# CLI configuration can override where providers come from, or skip the lock file checks entirely.
[[ -z "${TF_CLI_CONFIG_FILE:-}${TF_PLUGIN_CACHE_DIR:-}" && ! -e "${HOME}/.terraformrc" ]] ||
  fail "No Terraform CLI configuration (TF_CLI_CONFIG_FILE, TF_PLUGIN_CACHE_DIR, ~/.terraformrc) may be set."

terraform init -input=false -lockfile=readonly -backend-config="path=${TF_STATE_PATH}"

# Provisioners and the external data source run commands a plan can't show. The scan covers every module init
# recorded: downloaded ones under the data dir, and local ones, which stay wherever their source path points.
config_dirs=(.)
modules_json="${TF_DATA_DIR:-.terraform}/modules/modules.json"
if [[ -f "${modules_json}" ]]; then
  while IFS= read -r module_dir; do config_dirs+=("${module_dir}"); done < <(jq -r '.Modules[].Dir' "${modules_json}")
fi
if grep -rEl --include='*.tf' --include='*.tf.json' \
  'provisioner[[:space:]]+"|"provisioner"[[:space:]]*:|data[[:space:]]+"external"|"external"[[:space:]]*:' \
  "${config_dirs[@]}"; then
  fail "Terraform config, including modules, must not use provisioners or the external data source."
fi

terraform fmt -check
terraform validate
terraform plan -input=false -out="${plan}"
deletes="$(terraform show -json "${plan}" |
  jq -r '.resource_changes[]? | select(.change.actions | index("delete")) | .address')"
if [[ -n "${deletes}" ]]; then
  echo "::error::Terraform plan would delete or replace resources, so apply was stopped:"
  printf '%s\n' "${deletes}"
  exit 1
fi
terraform apply -input=false "${plan}"
