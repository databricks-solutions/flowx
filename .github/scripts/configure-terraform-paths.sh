#!/usr/bin/env bash
set -euo pipefail
{
  printf 'TF_DATA_DIR=%s\n' "${RUNNER_TEMP}/flowx-terraform-data"
  printf 'TF_STATE_DIR=%s\n' "${RUNNER_TEMP}/flowx-tfstate"
  printf 'TF_STATE_PATH=%s\n' "${RUNNER_TEMP}/flowx-tfstate/terraform.tfstate"
} >> "${GITHUB_ENV}"
mkdir -p "${RUNNER_TEMP}/flowx-terraform-data" "${RUNNER_TEMP}/flowx-tfstate"

