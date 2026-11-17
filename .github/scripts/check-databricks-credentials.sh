#!/usr/bin/env bash
set -euo pipefail

if [[ -n "${DATABRICKS_HOST:-}" && -n "${DATABRICKS_TOKEN:-}" ]]; then
  echo "configured=true" >> "${GITHUB_OUTPUT}"
else
  echo "configured=false" >> "${GITHUB_OUTPUT}"
fi
