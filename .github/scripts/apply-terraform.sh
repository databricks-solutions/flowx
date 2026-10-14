#!/usr/bin/env bash
set -euo pipefail
terraform init -input=false -backend-config="path=${TF_STATE_PATH}"
terraform fmt -check
terraform validate
terraform apply -auto-approve -input=false

