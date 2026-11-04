#!/usr/bin/env bash
set -euo pipefail
if [[ -z "${CLAUDE_CODE_OAUTH_TOKEN:-}" && -z "${ANTHROPIC_API_KEY:-}" ]]; then
  echo "No Claude authentication secret is configured; skipping the Claude Code install."
  exit 0
fi
install_dir="${RUNNER_TEMP}/claude-code"
mkdir -p "${install_dir}"
cp .github/claude-code/package.json .github/claude-code/package-lock.json "${install_dir}/"
npm ci --ignore-scripts --no-audit --no-fund --prefix "${install_dir}"
echo "CLAUDE_BIN=${install_dir}/node_modules/@anthropic-ai/claude-code-linux-x64/claude" >> "${GITHUB_ENV}"
