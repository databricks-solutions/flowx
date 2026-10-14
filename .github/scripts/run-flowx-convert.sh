#!/usr/bin/env bash
set -euo pipefail
python_bin="$(cat .migration-venv)"
PYTHONPATH=src "${python_bin}" -m flowx.adapter discover --source adf --source-path "${ADF_EXPORT_DIR}" --output-dir "${FLOWX_OUTPUT}"
PYTHONPATH=src "${python_bin}" -m flowx.adapter convert --source adf --source-path "${ADF_EXPORT_DIR}" --output-dir "${FLOWX_OUTPUT}"

