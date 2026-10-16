#!/usr/bin/env bash
set -euo pipefail
# Applies the fixtures only after checking that the saved plan deletes nothing. A provisioner or the external
# data source would run commands the plan can't show, so the config may not use them.
plan="${TF_STATE_DIR}/tfplan"
if grep -rEl --include='*.tf' --include='*.tf.json' 'local-exec|remote-exec|data[[:space:]]+"external"|"external"[[:space:]]*:' .; then
  echo "::error::Terraform config must not use local-exec or remote-exec provisioners or the external data source."
  exit 1
fi
terraform init -input=false -lockfile=readonly -backend-config="path=${TF_STATE_PATH}"
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
