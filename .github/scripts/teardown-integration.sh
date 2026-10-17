#!/usr/bin/env bash
set +e -o pipefail
chmod -R u+w "${CI_SCRIPTS}" 2>/dev/null || true
chmod -R u+w "${RUNNER_TEMP}/flowx-plugin" 2>/dev/null || true
chmod 700 "${TF_STATE_DIR}" "${TF_DATA_DIR}" 2>/dev/null || true
fallback_hash="$(printf '%s-%s' "${GITHUB_RUN_ID}" "${GITHUB_RUN_ATTEMPT}" | sha256sum | cut -c1-10)"
export TF_VAR_name_prefix="${TF_VAR_name_prefix:-fx${fallback_hash}}"
if [[ ! -s "${TF_STATE_PATH}" ]]; then
  echo "No Terraform state, so nothing to destroy."
  exit 0
fi
terraform destroy -auto-approve -input=false
