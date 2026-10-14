#!/usr/bin/env bash
set -euo pipefail
# shellcheck source=/dev/null
source "${RUNNER_TEMP}/snapshot-worktree.sh"
post_snapshot="${RUNNER_TEMP}/post-claude-worktree.sha256"
snapshot_worktree > "${post_snapshot}"
if ! diff -u "${RUNNER_TEMP}/pre-claude-worktree.sha256" "${post_snapshot}"; then
  echo "::error::Claude changed files outside agentic_results/, including ignored files."
  exit 1
fi

