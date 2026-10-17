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
# any other provider. Update the hash below when the lock file changes; see the README for how to regenerate it.
lock_sha256=2cb3c3f60c93da0e8c6d76def055c40dd4fdcd779165c7d703e680bdd50e08dc
sha256=(sha256sum)
command -v sha256sum >/dev/null || sha256=(shasum -a 256)
[[ "$("${sha256[@]}" < .terraform.lock.hcl | cut -d ' ' -f 1)" == "${lock_sha256}" ]] ||
  fail "The Terraform lock file changed; only the pinned azurerm and azapi providers may be used."
# CLI configuration can override where providers come from, or skip the lock file checks entirely.
[[ -z "${TF_CLI_CONFIG_FILE:-}${TF_PLUGIN_CACHE_DIR:-}" && ! -e "${HOME}/.terraformrc" ]] ||
  fail "No Terraform CLI configuration (TF_CLI_CONFIG_FILE, TF_PLUGIN_CACHE_DIR, ~/.terraformrc) may be set."

terraform init -input=false -lockfile=readonly -backend-config="path=${TF_STATE_PATH}"

# Provisioners and the external data source run commands a plan can't show. The scan covers every module init
# recorded: downloaded ones under the data dir, and local ones, which stay wherever their source path points.
# It refuses the words anywhere in .tf files, so layout, comments, or unquoted labels can't hide them, and
# parses .tf.json files so escaped or split keys can't either. (The external data source also needs a provider
# the pinned lock file doesn't list.)
config_dirs=(.)
modules_json="${TF_DATA_DIR:-.terraform}/modules/modules.json"
if [[ -f "${modules_json}" ]]; then
  module_dirs="$(jq -r '.Modules[].Dir' "${modules_json}")" || fail "Could not read ${modules_json}."
  while IFS= read -r module_dir; do config_dirs+=("${module_dir}"); done <<< "${module_dirs}"
fi
# Terraform follows symlinks the scan would skip, so none are allowed. A failed listing fails the check.
symlinks="$(find "${config_dirs[@]}" -type l)" || fail "Could not list the Terraform config files."
[[ -z "${symlinks}" ]] || fail "Terraform config must not contain symlinks: ${symlinks//$'\n'/, }"
config_files="$(mktemp)"
find "${config_dirs[@]}" -type f \( -name '*.tf' -o -name '*.tf.json' \) -print0 > "${config_files}" ||
  fail "Could not list the Terraform config files."
while IFS= read -r -d '' config_file; do
  if [[ "${config_file}" == *.tf.json ]]; then
    jq -e '[.. | objects | keys[]] | any(. == "provisioner" or . == "external") | not' "${config_file}" >/dev/null
  else
    ! grep -Eqw 'provisioner|external' "${config_file}"
  fi || fail "Terraform config must not use provisioners or the external data source: ${config_file}"
done < "${config_files}"
rm -f "${config_files}"

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
