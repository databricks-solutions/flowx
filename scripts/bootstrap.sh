#!/usr/bin/env bash
#
# Bootstraps a Python environment for the orchestra plugin.
#
# Creates a virtual environment at <plugin_root>/.venv and installs the Python
# dependencies listed in requirements.txt using pip.
#
# If python3, pip, or the venv module are unavailable, the script prints a clear
# warning telling the user what to install and exits non-zero without making changes.
#
# After bootstrapping, run the plugin's Python code with the venv interpreter and
# src/ on PYTHONPATH, e.g.:
#
#   PYTHONPATH="<plugin_root>/src" "<plugin_root>/.venv/bin/python" -m orchestra.adapter inputs ingest
#
set -euo pipefail

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

# Verify python3 is available
if ! command -v python3 >/dev/null 2>&1; then
  cat >&2 <<'EOF'
WARNING: python3 was not found on your PATH.

Orchestra requires Python 3.12+ to run its translation code.
Please install Python (it bundles pip) before continuing:

  - macOS:         brew install python   (or https://www.python.org/downloads/)
  - Debian/Ubuntu: sudo apt-get install python3 python3-venv python3-pip
  - Windows:       https://www.python.org/downloads/  (enable "Add python.exe to PATH")

Re-run this setup step once Python is installed.
EOF
  exit 1
fi

PYTHON_BIN="$(command -v python3)"

# Verify pip is available
if ! "$PYTHON_BIN" -m pip --version >/dev/null 2>&1; then
  cat >&2 <<'EOF'
WARNING: pip is not available for your python3 installation.

pip is required to install the orchestra plugin's dependencies. Install it with:

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
  "$PYTHON_BIN" -m venv "$VENV_DIR"
else
  echo "Using existing virtual environment at $VENV_DIR ..."
fi

VENV_PYTHON="$VENV_DIR/bin/python"

# Install dependencies from requirements.txt
if [ ! -f "$REQUIREMENTS" ]; then
  echo "ERROR: requirements.txt not found at $REQUIREMENTS" >&2
  exit 1
fi

echo "Upgrading pip ..."
"$VENV_PYTHON" -m pip install --quiet --upgrade pip

echo "Installing dependencies from requirements.txt ..."
"$VENV_PYTHON" -m pip install -r "$REQUIREMENTS"

cat <<EOF

orchestra Python environment is ready.
  Interpreter : $VENV_PYTHON
  Source path : $PLUGIN_ROOT/src

Run the plugin's Python code with src/ on PYTHONPATH, for example:
  PYTHONPATH="$PLUGIN_ROOT/src" "$VENV_PYTHON" -m orchestra.adapter inputs ingest
EOF
