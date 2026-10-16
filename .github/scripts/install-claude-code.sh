#!/usr/bin/env bash
set -euo pipefail
# Installs the Claude Code version pinned in .github/claude-code/package-lock.json. npm ci checks every
# package against the lock file's integrity hash, and --ignore-scripts skips the postinstall step, so the
# workflow runs the native binary from the platform package directly.
if [[ -z "${CLAUDE_CODE_OAUTH_TOKEN:-}" && -z "${ANTHROPIC_API_KEY:-}" ]]; then
  echo "No Claude authentication secret is configured; skipping the Claude Code install."
  exit 0
fi
install_dir="${RUNNER_TEMP}/claude-code"
mkdir -p "${install_dir}"
cp .github/claude-code/package.json .github/claude-code/package-lock.json "${install_dir}/"
npm ci --ignore-scripts --no-audit --no-fund --prefix "${install_dir}"
echo "CLAUDE_BIN=${install_dir}/node_modules/@anthropic-ai/claude-code-linux-x64/claude" >> "${GITHUB_ENV}"
