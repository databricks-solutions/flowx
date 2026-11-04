#!/usr/bin/env bash
set -euo pipefail

if [[ -n "$(git diff)" ]]; then
  echo "Code formatting issues detected. Run 'make fmt' locally."
  git diff
  exit 1
fi
