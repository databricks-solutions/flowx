---
name: flowx-setup
description: >
  Prepare flowx to run its phases (discover, convert, package, migrate). In Databricks
  Genie Code this deploys the phases as an MCP server (a Databricks App) and creates NO virtual
  environment — all code runs through MCP. Everywhere else it provisions a Python virtual
  environment for the CLI skills, and optionally a local (stdio) MCP server. Run this once before
  any other flowx skill, or whenever the environment is missing.
triggers:
  - "setup flowx"
  - "bootstrap flowx"
  - "install flowx dependencies"
  - "flowx environment"
  - "create flowx venv"
  - "ModuleNotFoundError flowx"
  - "install flowx mcp"
  - "deploy flowx mcp"
---

# Set up flowx

flowx runs its phases (`discover`, `convert`, `package`, `migrate`) in one of two ways. This
skill prepares whichever fits your environment, keyed on `DATABRICKS_RUNTIME_VERSION` (the same
signal the rest of the plugin uses to detect Databricks):

- **Databricks Genie Code** (`DATABRICKS_RUNTIME_VERSION` *set*) → the phases run as **MCP tools**
  hosted on a Databricks App. **No virtual environment is created** — the app vendors its own copy
  of the flowx code and dependencies, so the phase skills just call the single `flowx` MCP tool.
- **Local / Claude Code / other agents** (`DATABRICKS_RUNTIME_VERSION` *unset*) → the phases run
  from a **Python virtual environment** via the CLI, with an optional local (stdio) MCP server.

## Step 1 — Pick the path

```bash
if [ -n "${DATABRICKS_RUNTIME_VERSION:-}" ]; then
  echo "Databricks / Genie Code → deploy the MCP server (Path A, no venv)"
else
  echo "Local → create the virtual environment (Path B)"
fi
```

---

## Path A — Databricks Genie Code (MCP, no virtual environment)

In Genie Code the phases run on the deployed app, so **do not run `bootstrap.sh` and do not create a
venv** — it isn't needed. Deploy the MCP server instead:

```bash
bash <plugin_dir>/app/deploy.sh
```

`app/deploy.sh` stages a self-contained bundle (the app entrypoint plus a vendored copy of the
flowx source), syncs it to **`/Workspace/Shared/mcp-flowx`**, and creates/deploys the
**`mcp-flowx`** Databricks App. The script prints the app URL; the MCP endpoint is
`<app-url>/mcp`. It only needs the Databricks CLI and a system `python3` (for parsing CLI output) —
**not** an flowx venv.

> **Clone into a shared location.** The app's service principal cannot read private
> `/Workspace/Users/<you>` folders by default, so `deploy.sh` deploys the source from
> `/Workspace/Shared/<app-name>`. Clone flowx into a Git folder under **`/Workspace/Shared`**
> (e.g. `/Workspace/Shared/flowx`), not your user home. If `/Workspace/Shared` is restricted,
> use another all-users location and pass it via `APP_SOURCE_PATH`.

After it deploys, relay these follow-up steps to the user (the script also prints them):

1. **App access:** grant **Can use** on `mcp-flowx` to the users / service principals that will
   call it (Apps UI → *Permissions*, or `databricks apps set-permissions mcp-flowx ...`).
2. **Data access:** grant the app's service principal access to the catalogs, schemas, and volumes
   the migration touches (plus any SQL warehouse used by the reporting tools).
3. **Add it in Genie Code (Agent mode):** open Genie Code **Settings → MCP Servers → Add Server**,
   choose **Custom MCP server**, select the `mcp-flowx` app, and **Save**. The single
   `flowx` tool then appears (MCP needs Agent mode; it uses one of the 20 tool slots).
   Verify via the health endpoint `<app-url>/`.

Once added, the `discover`, `convert`, `package`, and `migrate` skills run **entirely through the
`flowx` MCP tool** (`flowx(command="…", parameters={…})`) — there is no venv, no
`bootstrap.sh`, and no `.migration-venv` marker on this path.

> **Note:** `databricks apps` deploy commands require a Databricks CLI session (workspace web
> terminal or a local machine), not serverless notebook Python. If the Genie session can't shell
> out to the CLI, run `app/deploy.sh` from the web terminal. (Same constraint as `databricks bundle
> deploy`.) If you see `Error: please specify target`, the CLI attached to a stray `databricks.yml`;
> `deploy.sh` already isolates against this, so re-run it as-is.

---

## Path B — Local / Claude Code (virtual environment)

The flowx code in `src/flowx/` depends on third-party packages (`pyyaml`, `databricks-sdk`,
`sqlglot`); running it against a bare system Python fails with `ModuleNotFoundError`. Provision an
isolated venv (created once and reused).

### Step B1 — Run the bootstrap script

```bash
bash <plugin_dir>/scripts/bootstrap.sh
```

Where `<plugin_dir>` is the flowx plugin root (the directory containing `src/`, `skills/`, and
`requirements.txt`). The script will:

1. Check that `python3`, `pip`, and the `venv` module are available.
2. Create the venv at `<plugin_dir>/.venv`.
3. Install `requirements.txt` into that venv using `pip`.
4. Write the resolved interpreter path to `<plugin_dir>/.migration-venv` for the other skills.

### Step B2 — Handle a missing Python or pip

If Python, pip, or the `venv` module are **not** available, the script prints a `WARNING:` block and
exits non-zero **without** creating anything. Do **not** work around it — relay the warning and stop:

> ⚠️ Python must be installed before I can set up the flowx environment.
>
>  * On macOS: `brew install python`.
>  * On Debian/Ubuntu: `sudo apt-get install python3 python3-venv python3-pip`.
>
> Let me know once it's installed and I'll re-run setup.

Re-run this setup skill after the user confirms Python and pip are installed.

### Step B3 — Confirm success and how to run Python code

On success, the script writes the interpreter path to `<plugin_dir>/.migration-venv`. The phase
skills run Python with that interpreter and `src/` on `PYTHONPATH`. Resolve it from the marker file:

```bash
export PYTHONPATH="<plugin_dir>/src"
PY="$(cat <plugin_dir>/.migration-venv)"
"$PY" -m flowx.adapter inputs discover
```

`$PY` resolves to `<plugin_dir>/.venv/bin/python` (on Windows, `<plugin_dir>\.venv\Scripts\python.exe`).

### Step B4 — (Optional) Run the MCP server locally

To drive the phases through MCP tools locally (instead of the CLI), install the MCP server stack into
the venv and register the stdio server with your MCP client:

```bash
PY="$(cat <plugin_dir>/.migration-venv)"
"$PY" -m pip install "mcp>=1.12" "uvicorn>=0.30" "starlette>=0.40"
PYTHONPATH="<plugin_dir>/src" "$PY" -m flowx.mcp        # stdio (default)
```

```json
{
  "mcpServers": {
    "flowx": {
      "command": "<interpreter from .migration-venv>",
      "args": ["-m", "flowx.mcp"],
      "env": { "PYTHONPATH": "<plugin_dir>/src" }
    }
  }
}
```

## Output

| Path | Artifact | Description |
|---|---|---|
| A (Genie Code) | `mcp-flowx` Databricks App | Hosts the phases as the single `flowx` MCP tool at `<app-url>/mcp`; **no venv** is created |
| B (local) | venv at `<plugin_dir>/.venv` | Virtual environment with the installed dependencies |
| B (local) | `<plugin_dir>/.migration-venv` | Marker file holding the resolved interpreter path |
| B (local, optional) | local MCP server | `mcp` / `uvicorn` / `starlette` installed into the venv; run with `python -m flowx.mcp` |

## Examples

- "Set up flowx" (auto-detects Genie Code vs. local)
- "Deploy the flowx MCP server to Databricks / Genie Code" (Path A)
- "Bootstrap flowx so I can run a migration locally" (Path B)
- "I got a ModuleNotFoundError running discover — fix the environment" (Path B)
