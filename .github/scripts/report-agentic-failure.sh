#!/usr/bin/env bash
set -euo pipefail

echo "::warning::Agentic resolution failed or timed out; deterministic validation will continue before the final gate fails."
echo "- ⚠️ Agentic resolution failed or timed out; deterministic validation continued before final failure." >> "${GITHUB_STEP_SUMMARY}"
