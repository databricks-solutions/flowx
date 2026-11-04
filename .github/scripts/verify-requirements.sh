#!/usr/bin/env bash
set -euo pipefail

make requirements
git diff --exit-code -- requirements.txt || {
  echo "requirements.txt is stale. Run 'make requirements' (or 'make precommit') and commit it."
  exit 1
}
