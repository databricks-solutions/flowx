#!/usr/bin/env bash
set -euo pipefail
# Strict by design: an unmatched result means the agent output does not belong to this translation report.
python_bin="$(cat .migration-venv)"
PYTHONPATH=src "${python_bin}" -m flowx.adapter convert --source adf --merge-agentic \
  --report "${FLOWX_OUTPUT}/.work/translation_report.json" \
  --agentic-results agentic_results

