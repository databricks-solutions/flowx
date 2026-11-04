#!/usr/bin/env bash
set -euo pipefail

ci_scripts="${RUNNER_TEMP}/ci-scripts"
cp -R "${GITHUB_WORKSPACE}/.github/scripts" "${ci_scripts}"
chmod -R a-w "${ci_scripts}"
echo "CI_SCRIPTS=${ci_scripts}" >> "${GITHUB_ENV}"
