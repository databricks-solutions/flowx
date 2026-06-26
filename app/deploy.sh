#!/usr/bin/env bash
# Deploy the flowx MCP server as a Databricks App.
#
# Builds a self-contained source bundle (app entrypoint + vendored flowx
# package), syncs it to the workspace, and creates/deploys the app.
#
# Requirements: Databricks CLI v0.230+ authenticated to the target workspace.
#
# Env overrides:
#   APP_NAME            App name (default: mcp-flowx). The `mcp-` prefix makes the app
#                       auto-listed in the AI Playground; Genie Code's "Add Server > Custom MCP
#                       server" picker can also select any Databricks App by name.
#   APP_SOURCE_PATH     Workspace source path the app deploys from (default:
#                       /Workspace/Shared/<APP_NAME>). It MUST be readable by the app's
#                       service principal, so it defaults to /Workspace/Shared — NOT a user's
#                       private /Workspace/Users/<me> home, which the app SP cannot read.
#   DATABRICKS_PROFILE  CLI profile to use (default: env/DEFAULT auth)
set -euo pipefail

APP_NAME="${APP_NAME:-mcp-flowx}"
APP_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$APP_DIR/.." && pwd)"

PROFILE_FLAG=()
if [ -n "${DATABRICKS_PROFILE:-}" ]; then
  PROFILE_FLAG=(--profile "$DATABRICKS_PROFILE")
fi

# Stage the bundle OUTSIDE the repo. `databricks sync` is git-aware and applies the
# enclosing repo's .gitignore: a staging dir inside the repo (e.g. app/.build, which
# .gitignore lists) gets its files excluded, so the deployed source is missing app.py
# and flowx/. A temp dir has no enclosing git repo / .gitignore, so every file syncs.
#
# The CLI is also bundle-aware: if a databricks.yml is discoverable in the working
# directory or any parent (e.g. a generated flowx_output/databricks.yml), `sync` /
# `apps deploy` load it and fail with "Error: please specify target". Running from the
# (databricks.yml-free) staging dir avoids that too. All CLI paths are absolute.
#
# Create the staging dir in a writable location, trying the repo's PARENT first. On
# Databricks (Genie web terminal / serverless) the repo lives on the writable /Workspace
# filesystem while /tmp and $TMPDIR are often unwritable — a bare `mktemp -d` there fails
# with "mkdir: cannot create directory ...: Permission denied". The repo's parent
# (e.g. /Workspace/Shared) is writable AND outside the git repo, so staging there both
# succeeds and avoids the repo's .gitignore (which would otherwise make `databricks sync`
# drop staged files). $TMPDIR/tmp/$HOME are fallbacks for local (non-Databricks) runs.
STAGE_DIR=""
for _base in "$(dirname "$REPO_ROOT")" "${TMPDIR:-}" /tmp /local_disk0/tmp "$HOME"; do
  [ -n "$_base" ] && [ -d "$_base" ] && [ -w "$_base" ] || continue
  STAGE_DIR="$(mktemp -d "${_base%/}/mcp-flowx-build.XXXXXX" 2>/dev/null)" && break
done
if [ -z "$STAGE_DIR" ]; then
  echo "ERROR: could not create a writable staging directory (tried the repo parent, \$TMPDIR, /tmp, \$HOME)." >&2
  echo "       Set TMPDIR to a writable local path and re-run." >&2
  exit 1
fi
trap 'rm -rf "$STAGE_DIR"' EXIT
dbx() { (cd "$STAGE_DIR" && databricks "${PROFILE_FLAG[@]}" "$@"); }

echo "==> Staging self-contained app bundle in $STAGE_DIR (outside the repo so sync includes every file)"
cp "$APP_DIR/app.py" "$APP_DIR/app.yaml" "$APP_DIR/requirements.txt" "$STAGE_DIR/"
cp -R "$REPO_ROOT/src/flowx" "$STAGE_DIR/flowx"
find "$STAGE_DIR/flowx" -type d -name '__pycache__' -prune -exec rm -rf {} + 2>/dev/null || true

# Deploy the source from a location the app's service principal can read. A user's
# private /Workspace/Users/<me> home is NOT readable by the app SP, so default to
# the shared workspace folder. Override with APP_SOURCE_PATH if you use a different
# all-users location.
SOURCE_PATH="${APP_SOURCE_PATH:-/Workspace/Shared/$APP_NAME}"

echo "==> Ensuring app '$APP_NAME' exists"
if ! dbx apps get "$APP_NAME" >/dev/null 2>&1; then
  dbx apps create "$APP_NAME"
fi

echo "==> Syncing bundle to $SOURCE_PATH"
dbx sync --full "$STAGE_DIR" "$SOURCE_PATH"

echo "==> Deploying app"
dbx apps deploy "$APP_NAME" --source-code-path "$SOURCE_PATH"

echo "==> Deployed. App details:"
APP_URL="$(dbx apps get "$APP_NAME" --output json \
  | python3 -c 'import sys, json; print(json.load(sys.stdin).get("url", ""))')"
echo "  name: $APP_NAME"
echo "  url:  ${APP_URL:-(pending — re-run 'databricks apps get $APP_NAME')}"
echo
echo "==> Next steps to use it in Genie Code:"
echo "  1. MCP endpoint:  ${APP_URL:-<app-url>}/mcp"
echo "  2. Grant 'Can use' on the app to the users / service principals that will call it"
echo "     (Apps UI > Permissions, or: databricks apps set-permissions $APP_NAME ...)."
echo "  3. Grant that app's service principal access to the catalogs/schemas/volumes the"
echo "     migration touches (and any SQL warehouse used by the reporting tools)."
echo "  4. Add it in Genie Code (Agent mode): Settings > MCP Servers > Add Server >"
echo "     Custom MCP server > select '$APP_NAME' > Save. Tools appear immediately."
echo "  5. If a browser CORS error appears, set the app env var FLOWX_ALLOWED_ORIGINS"
echo "     to your workspace URL and redeploy."
