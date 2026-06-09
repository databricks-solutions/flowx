---
name: setup
description: >
  Setup the Python environment for the orchestra plugin. Creates a .venv virtual environment 
  and installs the Python dependencies (from requirements.txt via pip) needed for each phase. 
  Run this once before any other orchestra skill, or whenever dependencies are missing.
triggers:
  - "setup orchestra"
  - "bootstrap orchestra"
  - "install orchestra dependencies"
  - "orchestra environment"
  - "create orchestra venv"
  - "ModuleNotFoundError orchestra"
---

# Create the Python Environment

Create a virtual environment (`.venv`) for the plugin and install its Python dependencies. This 
is the prerequisite for the `ingest`, `translate`, `prepare`, and `migrate` skills which run Python 
from this environment.

## Context

The orchestra plugin ships Python code (in `src/orchestra/`) that the skills invoke (e.g. 
`python -m orchestra.adapter ...`, `adf_loader.py`, `engine.py`, `dab_writer.py`). Some code depends 
on third-party packages (`pyyaml`, `databricks-sdk`, `sqlglot`). Running it against a bare system 
Python fails with `ModuleNotFoundError`. This step provisions an isolated `.venv` with the required
dependencies installed via `pip` from `requirements.txt`.

The environment is created once and reused. Re-running the bootstrapscript  confirms the venv exists 
and ensures that dependencies are satisfied.

## Workflow

### Step 1 — Run the bootstrap script

From the plugin root, run:

```bash
bash <plugin_dir>/scripts/bootstrap.sh
```

Where `<plugin_dir>` is the root of the orchestra plugin (the directory containing `src/`,
`skills/`, and `requirements.txt`).

The script will:
1. Check that `python3`, `pip`, and the `venv` module are available.
2. Create `<plugin_dir>/.venv` if it does not already exist.
3. Install dependencies listed in `requirements.txt` into that venv using `pip`.

### Step 2 — Handle a missing Python or pip

If Python, pip, or the `venv` module are **not** available, the script prints a `WARNING:` block
explaining what to install and exits non-zero **without** creating anything.

When this happens, **do not attempt to work around it**. Relay the warning to the user, ask them
to install, and stop:

> ⚠️ Python must be installed before I can set up the orchestra environment.
> 
>  * On macOS: `brew install python`.
>  * On Debian/Ubuntu: `sudo apt-get install python3 python3-venv python3-pip`.
> 
> Let me know once it's installed and I'll re-run setup.

Re-run this setup skill after the user confirms Python and pip are installed.

### Step 3 — Confirm success and how to run Python code

On success, the script prints the interpreter path and a usage example. After this, every
Python command in the orchestra skills **must** be run with the venv interpreter and `src/` 
on `PYTHONPATH`:

```bash
export PYTHONPATH="<plugin_dir>/src"
"<plugin_dir>/.venv/bin/python" -m orchestra.adapter inputs ingest
```

(On Windows the interpreter is `<plugin_dir>\.venv\Scripts\python.exe`.)

Use `<plugin_dir>/.venv/bin/python` anywhere the other skills show `python3`.

## Output

| Artifact | Description |
|---|---|
| `<plugin_dir>/.venv/` | Virtual environment containing the installed dependencies |
| `requirements.txt` | The dependency list installed into the venv |

## Examples

- "Set up the orchestra environment"
- "Bootstrap orchestra so I can run a migration"
- "I got a ModuleNotFoundError running ingest — fix the environment"
