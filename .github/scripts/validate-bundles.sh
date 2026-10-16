#!/usr/bin/env bash
set -euo pipefail
python_bin=.venv/bin/python
failed_bundles=()
for bundle in "${FLOWX_OUTPUT}"/*/databricks.yml; do
  bundle_name="$(basename "$(dirname "${bundle}")")"
  required_variables=()
  if ! required_output="$("${python_bin}" - "${bundle}" 2>/dev/null <<'PY'
import sys

import yaml

with open(sys.argv[1], encoding="utf-8") as handle:
    config = yaml.safe_load(handle) or {}
target_variables = (((config.get("targets") or {}).get("dev") or {}).get("variables") or {})
for name, declaration in sorted((config.get("variables") or {}).items()):
    if name not in target_variables and (not isinstance(declaration, dict) or "default" not in declaration):
        print(name)
PY
  )"; then
    printf '::error::Could not parse required variables for bundle %s.\n' "${bundle_name}"
    failed_bundles+=("${bundle_name}")
    continue
  fi
  if [[ -n "${required_output}" ]]; then
    mapfile -t required_variables <<< "${required_output}"
  fi
  validate_args=(bundle validate -t dev)
  if (( ${#required_variables[@]} )); then
    printf 'Validating %s with placeholders for required variables:' "${bundle_name}"
    printf ' %s' "${required_variables[@]}"
    printf '\n'
  fi
  for variable in "${required_variables[@]}"; do
    placeholder="flowx-validation-placeholder"
    [[ "${variable}" == *_job_id ]] && placeholder=0
    validate_args+=(--var "${variable}=${placeholder}")
  done
  if ! (cd "$(dirname "${bundle}")" && databricks "${validate_args[@]}"); then
    failed_bundles+=("${bundle_name}")
  fi
done
if (( ${#failed_bundles[@]} )); then
  printf '::error::Bundle validation failed for: %s\n' "${failed_bundles[*]}"
  exit 1
fi
