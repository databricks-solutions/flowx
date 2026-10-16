#!/usr/bin/env bash
set +e -o pipefail
chmod -R u+w "${CI_SCRIPTS}" 2>/dev/null || true
chmod -R u+w "${RUNNER_TEMP}/flowx-plugin" 2>/dev/null || true
chmod 700 "${TF_STATE_DIR}" "${TF_DATA_DIR}" 2>/dev/null || true
fallback_hash="$(printf '%s-%s' "${GITHUB_RUN_ID}" "${GITHUB_RUN_ATTEMPT}" | sha256sum | cut -c1-10)"
export TF_VAR_name_prefix="${TF_VAR_name_prefix:-fx${fallback_hash}}"
# Destroys only this run's resources. The shared resource group stays; the CI identity cannot delete it.
terraform destroy -auto-approve -input=false
