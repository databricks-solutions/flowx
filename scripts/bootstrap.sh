#!/usr/bin/env bash
#
# Bootstraps a Python environment for the flowx plugin.
#
# Creates a virtual environment and installs the Python dependencies listed in
# requirements.txt using pip.  The venv location depends on the environment:
#   * Databricks (Genie Code / notebooks): /Workspace/Users/<current user>/.migration-skills
#     so the environment persists in the workspace and is shared across sessions.
#   * Everywhere else: <plugin_root>/.venv
#
# If python3, pip, or the venv module are unavailable, the script prints a clear
# warning telling the user what to install and exits non-zero without making changes.
#
# On Databricks serverless compute (where ensurepip is unavailable), the script
# falls back to --without-pip and installs a pinned, hash-checked pip wheel.
#
# After bootstrapping, run the plugin's Python code with the venv interpreter and
# src/ on PYTHONPATH, e.g.:
#
#   PYTHONPATH="<plugin_root>/src" "<venv>/bin/python" -m flowx.adapter inputs discover --source adf
#
# The resolved interpreter path is also written to <plugin_root>/.migration-venv
# so the skills can discover it without re-deriving the location.
#
# Options:
#   --source-deps <source>  Also installs flowx with the deps needed to extract and profile
#                           that source system live. Only `azure` is supported
#                           (e.g. --source-deps azure or --source-deps=azure).
#
set -euo pipefail

usage() {
  echo "Usage: bootstrap.sh [--source-deps azure]" >&2
  exit 2
}

SOURCE_DEPS=""
while [ $# -gt 0 ]; do
  case "$1" in
    --source-deps=*)
      SOURCE_DEPS="${1#*=}"
      [ -n "$SOURCE_DEPS" ] || usage
      ;;
    --source-deps)
      [ $# -ge 2 ] && [ -n "$2" ] || usage
      SOURCE_DEPS="$2"
      shift
      ;;
    *) usage ;;
  esac
  shift
done
SOURCE_DEPS="$(printf '%s' "$SOURCE_DEPS" | tr '[:upper:]' '[:lower:]')"
case "$SOURCE_DEPS" in
  "" | azure) ;;
  *) usage ;;
esac

# Resolve the plugin root
SOURCE="${BASH_SOURCE[0]}"
while [ -L "$SOURCE" ]; do
  DIR="$(cd -P "$(dirname "$SOURCE")" >/dev/null 2>&1 && pwd)"
  SOURCE="$(readlink "$SOURCE")"
  [[ "$SOURCE" != /* ]] && SOURCE="$DIR/$SOURCE"
done
SCRIPT_DIR="$(cd -P "$(dirname "$SOURCE")" >/dev/null 2>&1 && pwd)"
PLUGIN_ROOT="$(cd -P "$SCRIPT_DIR/.." >/dev/null 2>&1 && pwd)"

VENV_DIR="$PLUGIN_ROOT/.venv"
REQUIREMENTS="$PLUGIN_ROOT/requirements.txt"
PIP_REQUIREMENT="pip==26.2.1 --hash=sha256:71138adf1f4ca900cdb7d289c21b7494329f2332b6d85f0e1c42108c0384ed3e"
PIP_REQUIREMENT_FILE="$(mktemp)"
PIP_WHEEL_DIR="$(mktemp -d)"
trap 'rm -rf "$PIP_REQUIREMENT_FILE" "$PIP_WHEEL_DIR"' EXIT
echo "$PIP_REQUIREMENT" > "$PIP_REQUIREMENT_FILE"

# Verify python3 is available
if ! command -v python3 >/dev/null 2>&1; then
  cat >&2 <<'EOF'
WARNING: python3 was not found on your PATH.

flowx requires Python 3.12+ to run its translation code.
Please install Python (it bundles pip) before continuing:

  - macOS:         brew install python   (or https://www.python.org/downloads/)
  - Debian/Ubuntu: sudo apt-get install python3 python3-venv python3-pip
  - Windows:       https://www.python.org/downloads/  (enable "Add python.exe to PATH")

Re-run this setup step once Python is installed.
EOF
  exit 1
fi

PYTHON_BIN="$(command -v python3)"

# On Databricks (Genie Code / notebooks), create the venv under the current
# user's workspace folder so it persists and is shared across sessions, rather
# than inside the (ephemeral) plugin checkout.  Resolve the current user from the
# notebook runtime context, falling back to the workspace SDK.
if [ -n "${DATABRICKS_RUNTIME_VERSION:-}" ]; then
  CURRENT_USER="$("$PYTHON_BIN" - <<'PYRESOLVE' 2>/dev/null || true
def _resolve():
    try:
        from dbruntime.databricks_repl_context import get_context
        ctx = get_context()
        for attr in ("userName", "user"):
            val = getattr(ctx, attr, None)
            if val and "@" in str(val):
                return str(val)
    except Exception:
        pass
    try:
        from databricks.sdk import WorkspaceClient
        name = WorkspaceClient().current_user.me().user_name
        if name:
            return name
    except Exception:
        pass
    return ""
print(_resolve())
PYRESOLVE
)"
  if [ -n "$CURRENT_USER" ]; then
    VENV_DIR="/Workspace/Users/${CURRENT_USER}/.migration-skills"
    echo "Databricks runtime detected; using workspace venv for ${CURRENT_USER}:"
    echo "  $VENV_DIR"
  else
    echo "Databricks runtime detected but current user could not be resolved;" >&2
    echo "  falling back to plugin-local venv at $VENV_DIR" >&2
  fi
fi

# Verify pip is available
if ! "$PYTHON_BIN" -m pip --version >/dev/null 2>&1; then
  cat >&2 <<'EOF'
WARNING: pip is not available for your python3 installation.

pip is required to install the flowx plugin's dependencies. Install it with:

  - macOS/Linux:   python3 -m ensurepip --upgrade
  - Debian/Ubuntu: sudo apt-get install python3-pip
  - or follow https://pip.pypa.io/en/stable/installation/

Re-run this setup step once pip is installed.
EOF
  exit 1
fi

# Verify the venv module is available
if ! "$PYTHON_BIN" -m venv --help >/dev/null 2>&1; then
  cat >&2 <<'EOF'
WARNING: the Python `venv` module is not available.

It is required to create the virtual environment. Install it with:

  - Debian/Ubuntu: sudo apt-get install python3-venv
  - or reinstall Python from https://www.python.org/downloads/

Re-run this setup step once `venv` is available.
EOF
  exit 1
fi

# Create the virtual environment
if [ ! -x "$VENV_DIR/bin/python" ]; then
  echo "Creating virtual environment at $VENV_DIR ..."
  if "$PYTHON_BIN" -m venv "$VENV_DIR" 2>/dev/null; then
    : # standard venv creation succeeded
  else
    # Fallback for environments where ensurepip is unavailable (e.g. Databricks
    # serverless compute).  Create the venv without pip, download the pinned pip
    # wheel with the system pip (which checks its hash), then run pip from that
    # wheel to install itself into the venv.
    echo "Standard venv failed (ensurepip likely missing); trying --without-pip ..."
    rm -rf "$VENV_DIR"
    "$PYTHON_BIN" -m venv --without-pip "$VENV_DIR"
    echo "Installing pinned pip into the venv ..."
    "$PYTHON_BIN" -m pip download --quiet --no-deps --require-hashes \
      -r "$PIP_REQUIREMENT_FILE" -d "$PIP_WHEEL_DIR"
    PIP_WHEEL="$(ls "$PIP_WHEEL_DIR"/pip-*.whl)"
    "$VENV_DIR/bin/python" "$PIP_WHEEL/pip" install --quiet --no-index "$PIP_WHEEL"
  fi
else
  echo "Using existing virtual environment at $VENV_DIR ..."
fi

VENV_PYTHON="$VENV_DIR/bin/python"

# Install dependencies from requirements.txt
if [ ! -f "$REQUIREMENTS" ]; then
  echo "ERROR: requirements.txt not found at $REQUIREMENTS" >&2
  exit 1
fi

echo "Installing pinned pip ..."
"$VENV_PYTHON" -m pip install --quiet --require-hashes -r "$PIP_REQUIREMENT_FILE"

echo "Installing dependencies from requirements.txt ..."
"$VENV_PYTHON" -m pip install -r "$REQUIREMENTS"

if [ -n "$SOURCE_DEPS" ]; then
  echo "Installing flowx from $PLUGIN_ROOT with the $SOURCE_DEPS extra ..."
  "$VENV_PYTHON" -m pip install -e "${PLUGIN_ROOT}[${SOURCE_DEPS}]"
fi

# ---------------------------------------------------------------------------
# Databricks runtime: pre-configure workspace auth from the notebook context
# ---------------------------------------------------------------------------
# The venv Python does NOT have dbruntime (it's a system-only package), so
# workspace_downloader.py can't auto-configure auth at runtime.  However, the
# system Python ($PYTHON_BIN) DOES have it.  Extract host + token here once and
# write ~/.databrickscfg so all subsequent venv invocations find it immediately.
# ---------------------------------------------------------------------------
if [ -n "${DATABRICKS_RUNTIME_VERSION:-}" ]; then
  CFG_PATH="${HOME}/.databrickscfg"
  if [ -s "$CFG_PATH" ]; then
    echo "Databricks auth already configured at $CFG_PATH"
  else
    echo "Databricks runtime detected; extracting workspace auth ..."
    "$PYTHON_BIN" -c "
from dbruntime.databricks_repl_context import get_context
c = get_context()
host = 'https://' + c.browserHostName
token = c.apiToken
if not host or not token:
    raise SystemExit('host/token unavailable from runtime context')
import pathlib
p = pathlib.Path('$CFG_PATH')
p.parent.mkdir(parents=True, exist_ok=True)
p.write_text(f'[DEFAULT]\nhost = {host}\ntoken = {token}\n')
print(f'  -> {p} written (host={host})')
" 2>/dev/null && true || echo "  -> skipped (runtime context unavailable)"
  fi
fi

# Record the resolved interpreter path so the skills can discover it without
# re-deriving the (environment-dependent) venv location.
echo "$VENV_PYTHON" > "$PLUGIN_ROOT/.migration-venv"

cat <<EOF

flowx Python environment is ready.
  Interpreter : $VENV_PYTHON
  Source path : $PLUGIN_ROOT/src
  Marker file : $PLUGIN_ROOT/.migration-venv

Run the plugin's Python code with src/ on PYTHONPATH, for example:
  PYTHONPATH="$PLUGIN_ROOT/src" "$VENV_PYTHON" -m flowx.adapter inputs discover --source adf
EOF
