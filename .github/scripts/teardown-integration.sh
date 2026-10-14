#!/usr/bin/env bash
set +e -o pipefail
chmod -R u+w "${CI_SCRIPTS}" 2>/dev/null || true
chmod -R u+w "${RUNNER_TEMP}/flowx-plugin" 2>/dev/null || true
chmod 700 "${TF_STATE_DIR}" "${TF_DATA_DIR}" 2>/dev/null || true
fallback_hash="$(printf '%s-%s' "${GITHUB_RUN_ID}" "${GITHUB_RUN_ATTEMPT}" | sha256sum | cut -c1-10)"
fallback_prefix="fx${fallback_hash}"
export TF_VAR_name_prefix="${TF_VAR_name_prefix:-$fallback_prefix}"
resource_group="$(terraform output -raw resource_group_name 2>/dev/null)"
resource_group="${resource_group:-${TF_VAR_name_prefix:0:12}-flowx-rg}"
terraform destroy -auto-approve -input=false
destroy_status=$?
if [[ -n "${resource_group}" ]] && az group exists --name "${resource_group}" | grep -q true; then
  az group delete --name "${resource_group}" --yes --no-wait
fi
exit "${destroy_status}"
