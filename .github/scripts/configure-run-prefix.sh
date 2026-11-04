#!/usr/bin/env bash
set -euo pipefail

run_hash="$(printf '%s-%s' "${GITHUB_RUN_ID}" "${GITHUB_RUN_ATTEMPT}" | sha256sum | cut -c1-10)"
echo "TF_VAR_name_prefix=fx${run_hash}" >> "${GITHUB_ENV}"
