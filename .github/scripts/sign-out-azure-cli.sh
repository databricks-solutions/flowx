#!/usr/bin/env bash
set -euo pipefail
az logout 2>/dev/null || true
az account clear 2>/dev/null || true
rm -rf "${HOME}/.azure"
