#!/usr/bin/env bash
set +e -o pipefail
chmod -R u+w "${CI_SCRIPTS}" 2>/dev/null || true
chmod -R u+w "${RUNNER_TEMP}/flowx-plugin" 2>/dev/null || true
chmod 700 "${TF_STATE_DIR}" "${TF_DATA_DIR}" 2>/dev/null || true
fallback_hash="$(printf '%s-%s' "${GITHUB_RUN_ID}" "${GITHUB_RUN_ATTEMPT}" | sha256sum | cut -c1-10)"
export TF_VAR_name_prefix="${TF_VAR_name_prefix:-fx${fallback_hash}}"
# Terraform saves state whenever apply stops, even after an error or interruption, so no state means apply
# never ran (for example, init failed) and there is nothing to destroy.
if [[ ! -s "${TF_STATE_PATH}" ]]; then
  echo "No Terraform state, so nothing to destroy."
  exit 0
fi
# Destroys only this run's resources. The shared resource group stays; the CI identity cannot delete it.
terraform destroy -auto-approve -input=false
