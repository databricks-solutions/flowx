#!/usr/bin/env bash
set -euo pipefail
# Removes the Azure CLI token cache before Claude runs. Later steps don't need it: Terraform signs in
# through GitHub OIDC on its own.
az logout 2>/dev/null || true
az account clear 2>/dev/null || true
rm -rf "${HOME}/.azure"
