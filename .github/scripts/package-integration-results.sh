#!/usr/bin/env bash
set -euo pipefail
python_bin=.venv/bin/python
PYTHONPATH=src "${python_bin}" -m flowx.adapter package --output-dir "${FLOWX_OUTPUT}" \
  --catalog main --schema default --no-download-workspace-files
PYTHONPATH=src "${python_bin}" "${CI_SCRIPTS}/package-integration-results.py"
