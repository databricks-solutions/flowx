#!/usr/bin/env bash
set -euo pipefail
if [[ "${ADF_CASES_OUTCOME}" != success ]]; then
  echo "::error::Live ADF expected-output case validation failed."
  exit 1
fi
if [[ "${AGENTIC_CONFIGURED}" != true ]]; then
  exit 0
fi
if [[ "${AGENTIC_OUTCOME}" != success ]]; then
  echo "::error::Claude authentication was configured, but agentic resolution failed or timed out."
  exit 1
fi
if [[ "${AGENTIC_RESOLVED}" != true || "${MERGE_OUTCOME}" != success || "${STRUCTURE_OUTCOME}" != success ]]; then
  echo "::error::Claude authentication was configured, but result validation, merging, or structural validation failed."
  exit 1
fi

