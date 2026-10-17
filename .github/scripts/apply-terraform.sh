#!/usr/bin/env bash
set -euo pipefail
plan="${TF_STATE_DIR}/tfplan"
fail() {
  echo "::error::$1"
  exit 1
}

lock_sha256=2cb3c3f60c93da0e8c6d76def055c40dd4fdcd779165c7d703e680bdd50e08dc
sha256=(sha256sum)
command -v sha256sum >/dev/null || sha256=(shasum -a 256)
[[ "$("${sha256[@]}" < .terraform.lock.hcl | cut -d ' ' -f 1)" == "${lock_sha256}" ]] ||
  fail "The Terraform lock file changed; only the pinned azurerm and azapi providers may be used."
[[ -z "${TF_CLI_CONFIG_FILE:-}${TF_PLUGIN_CACHE_DIR:-}" && ! -e "${HOME}/.terraformrc" ]] ||
  fail "No Terraform CLI configuration (TF_CLI_CONFIG_FILE, TF_PLUGIN_CACHE_DIR, ~/.terraformrc) may be set."

terraform init -input=false -lockfile=readonly -backend-config="path=${TF_STATE_PATH}"

config_dirs=(.)
modules_json="${TF_DATA_DIR:-.terraform}/modules/modules.json"
if [[ -f "${modules_json}" ]]; then
  # `find .` already covers modules under the root, including .terraform/modules; add only local modules outside it.
  module_dirs="$(jq -r '.Modules[].Dir | select(startswith("..") or startswith("/"))' "${modules_json}")" ||
    fail "Could not read ${modules_json}."
  while IFS= read -r module_dir; do [[ -z "${module_dir}" ]] || config_dirs+=("${module_dir}"); done <<< "${module_dirs}"
fi
symlinks="$(find "${config_dirs[@]}" -type l)" || fail "Could not list the Terraform config files."
[[ -z "${symlinks}" ]] || fail "Terraform config must not contain symlinks: ${symlinks//$'\n'/, }"
config_files="$(mktemp)"
trap 'rm -f "${config_files}"' EXIT
find "${config_dirs[@]}" -type f \( -name '*.tf' -o -name '*.tf.json' \) -print0 > "${config_files}" ||
  fail "Could not list the Terraform config files."
while IFS= read -r -d '' config_file; do
  if [[ "${config_file}" == *.tf.json ]]; then
    jq -e '[.. | objects | keys[]] | any(. == "provisioner" or . == "external") | not' "${config_file}" >/dev/null
  else
    ! grep -Eqw 'provisioner|external' "${config_file}"
  fi || fail "Terraform config must not use provisioners or the external data source: ${config_file}"
done < "${config_files}"

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
