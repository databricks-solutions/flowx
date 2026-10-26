#!/usr/bin/env bash
set -euo pipefail

if [[ -z "${RESOURCE_OWNER_EMAIL:-}" ]]; then
  echo "::error::RESOURCE_OWNER_EMAIL is required in the flowx-live-integration environment."
  exit 1
fi
if [[ -z "${TF_VAR_resource_group_name:-}" ]]; then
  echo "::error::AZURE_RESOURCE_GROUP is required in the flowx-live-integration environment; rerun setup-integration.sh."
  exit 1
fi
