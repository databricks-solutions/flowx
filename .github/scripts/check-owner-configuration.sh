#!/usr/bin/env bash
set -euo pipefail

if [[ -z "${RESOURCE_OWNER_EMAIL:-}" ]]; then
  echo "::error::RESOURCE_OWNER_EMAIL is required in the flowx-live-integration environment."
  exit 1
fi
