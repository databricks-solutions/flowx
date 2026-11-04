#!/usr/bin/env bash
set -euo pipefail
.venv/bin/python "${CI_SCRIPTS}/check-agentic-structure.py" \
  "${RUNNER_TEMP}/expected-agentic-structure.json" \
  "${FLOWX_OUTPUT}/.work/translation_report.json"
