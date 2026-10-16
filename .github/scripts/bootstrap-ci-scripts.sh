#!/usr/bin/env bash
set -euo pipefail
# A read-only copy outside the checkout keeps Claude, later in this job, from changing scripts that run after it.
# It doesn't protect against the pull request itself; the environment's required reviewers do that.

ci_scripts="${RUNNER_TEMP}/ci-scripts"
cp -R "${GITHUB_WORKSPACE}/.github/scripts" "${ci_scripts}"
chmod -R a-w "${ci_scripts}"
echo "CI_SCRIPTS=${ci_scripts}" >> "${GITHUB_ENV}"
