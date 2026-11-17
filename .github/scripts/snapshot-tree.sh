#!/usr/bin/env bash
set -euo pipefail

snapshot_worktree() {
  find . -path './.git' -prune -o -path './agentic_results' -prune -o \
    -type f -print0 | sort -z | xargs -0 -r sha256sum
  find . -path './.git' -prune -o -path './agentic_results' -prune -o \
    -printf '%y %m %p -> %l\n' | sort
}
