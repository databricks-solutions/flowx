---
name: flowx-setup
description: >
  Prepare flowx to run its phases (discover, convert, package, migrate). In Databricks
  Genie Code this deploys the phases as an MCP server (a Databricks App) and creates NO virtual
  environment — all code runs through MCP. Everywhere else it provisions a Python virtual
  environment for the CLI skills, and optionally a local (stdio) MCP server. Run this once before
  any other flowx skill, or whenever the environment is missing.
  Use when the user asks to set up or bootstrap flowx or install its dependencies or MCP server, or
  when a flowx command fails with ModuleNotFoundError.
---

# Set up flowx

Run once before any other flowx skill, or whenever the environment is missing. The path depends on
`DATABRICKS_RUNTIME_VERSION`, the signal the rest of the plugin uses to detect Databricks. In Databricks
Genie Code (set), the phases run as the single `flowx` MCP tool hosted on a Databricks App, which vendors
its own copy of the flowx code and dependencies, so no virtual environment is created. Everywhere else
(unset: local, Claude Code, other agents), the phases run from a Python virtual environment through the
CLI, with an optional local stdio MCP server.

## Before you start

- In Genie Code, flowx must be cloned into a Git folder under `/Workspace/Shared` (for example
  `/Workspace/Shared/flowx`), not your user home: the app's service principal can't read private
  `/Workspace/Users/<you>` folders by default, so `deploy.sh` deploys the source from
  `/Workspace/Shared/<app-name>`. If `/Workspace/Shared` is restricted, use another all-users location
  and pass it as `APP_SOURCE_PATH`.
- Locally, `<plugin_dir>` is the flowx plugin root: the folder holding `src/`, `skills/` and
  `requirements.txt`.

## How to run

This skill is what makes the other skills runnable, so it has no phase command of its own. Steps 3 and 4
are the Genie Code path; Steps 5 to 8 are the local path. How the other skills then run is in
[`../shared/running-flowx.md`](../shared/running-flowx.md).

## Steps

### Step 1: Pick the path

```bash
if [ -n "${DATABRICKS_RUNTIME_VERSION:-}" ]; then
  echo "Databricks / Genie Code: deploy the MCP server (Steps 3 and 4, no venv)"
else
  echo "Local: create the virtual environment (Steps 5 to 8)"
fi
```

### Step 2: Ask about the live Azure features

Ask before installing anything:

> Do you want the live Azure features: pull ADF definitions with `extract` and estimate current
> spend with `profile`? They need extra Azure packages (`azure-identity`, `requests`) and Azure
> credentials.

Remember the answer. On yes, the steps below add flowx's optional `azure` extra; on no, they install the
base dependencies only (the default). Setup can run again later to add it.

### Step 3: Genie Code: deploy the MCP server

**Don't run `bootstrap.sh` or create a venv on this path.** Deploy the MCP server instead.

- **Recommended, works on serverless:** run the `app/deploy_app.py` notebook. It uses the Databricks SDK
  Workspace API to stage a self-contained bundle (the app entrypoint plus a vendored copy of the flowx
  source) to `/Workspace/Shared/mcp-flowx` and to create and deploy the `mcp-flowx` Databricks App, so
  it runs directly in a serverless Genie Code session. Set its `repo_root` widget to the flowx checkout
  (for example `/Workspace/Shared/flowx`), and its `source_deps` widget to `azure` on yes in Step 2
  (otherwise leave it blank). It prints the app URL; the MCP endpoint is `<app-url>/mcp`.
- **CLI alternative:** `bash <plugin_dir>/app/deploy.sh` does the same through the Databricks CLI
  (`apps deploy`, `sync`); prefix it with `SOURCE_DEPS=azure` on yes in Step 2. It needs a CLI session,
  so run it from a workspace web terminal or a local machine, not from serverless notebook Python. If it
  reports `Error: please specify target`, the CLI attached to a stray `databricks.yml`; the script
  already isolates against this, so run it again as it is.

### Step 4: Genie Code: grant access and add the server

Relay these to the user (the script also prints them):

1. **App access:** grant Can use on `mcp-flowx` to the users and service principals that will call it
   (Apps UI, Permissions, or `databricks apps set-permissions mcp-flowx ...`).
2. **Data access:** grant the app's service principal access to the catalogs, schemas and volumes the
   migration touches, plus any SQL warehouse the reporting tools use.
3. **Add it in Genie Code:** add the `mcp-flowx` app as a custom MCP server in Genie Code as
   https://docs.databricks.com/aws/en/genie-code/mcp describes, and check any limit on tools there; read the
   edition for the target workspace's cloud (the page's cloud selector, or learn.microsoft.com for Azure
   Databricks). The single `flowx` tool then appears. Check the health endpoint `<app-url>/`.
4. **Cost profiling only (yes in Step 2):** give the app an Azure service principal as described in
   `app/README.md` ("Optional: live Azure features"), then run
   `flowx(command="profile", parameters={"source": "adf", ...})`.

The other skills then run entirely through `flowx(command="...", parameters={...})`: there is no venv, no
`bootstrap.sh` and no `.migration-venv` marker on this path.

### Step 5: Local: run the bootstrap script

The flowx code in `src/flowx/` depends on third-party packages (`pyyaml`, `databricks-sdk`, `sqlglot`);
a bare system Python fails with `ModuleNotFoundError`. The venv is created once and reused.

```bash
bash <plugin_dir>/scripts/bootstrap.sh                       # no in Step 2
bash <plugin_dir>/scripts/bootstrap.sh --source-deps=azure   # yes in Step 2
```

The script checks that `python3`, `pip` and the `venv` module are available, creates the venv at
`<plugin_dir>/.venv`, installs `requirements.txt` into it with pip (with `--source-deps=azure`, it also
installs flowx from this checkout with its `azure` extra, `pip install -e '<plugin_dir>[azure]'`, so the
installed version matches the source tree), and writes the interpreter path to
`<plugin_dir>/.migration-venv` for the other skills.

### Step 6: Local: handle a missing Python or pip

If Python, pip or the `venv` module is missing, the script prints a `WARNING:` block and exits non-zero
without creating anything. **Don't work around it:** relay the warning and stop.

> ⚠️ Python must be installed before I can set up the flowx environment.
>
>  * On macOS: `brew install python`.
>  * On Debian/Ubuntu: `sudo apt-get install python3 python3-venv python3-pip`.
>
> Let me know once it's installed and I'll re-run setup.

Run this skill again once the user confirms Python and pip are installed.

### Step 7: Local: confirm the environment

```bash
export PYTHONPATH="<plugin_dir>/src"
PY="$(cat <plugin_dir>/.migration-venv)"
"$PY" -m flowx.adapter inputs discover --source adf   # or --source airflow
```

`$PY` resolves to `<plugin_dir>/.venv/bin/python` (on Windows, `<plugin_dir>\.venv\Scripts\python.exe`).

### Step 8 (optional): Local: run the MCP server over stdio

To drive the phases through MCP locally instead of the CLI, install the MCP server stack into the venv
and register the stdio server with your MCP client:

```bash
PY="$(cat <plugin_dir>/.migration-venv)"
"$PY" -m pip install "mcp>=1.12,<2" "uvicorn>=0.30" "starlette>=0.40"
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

| Path | What it creates |
|---|---|
| Genie Code | The `mcp-flowx` Databricks App, hosting the single `flowx` MCP tool at `<app-url>/mcp`; no venv |
| Local | The venv at `<plugin_dir>/.venv` with the installed dependencies |
| Local | `<plugin_dir>/.migration-venv`, the marker file holding the interpreter path |
| Local, optional | The `mcp`, `uvicorn` and `starlette` packages in the venv; run with `python -m flowx.mcp` |

## Reference

- [`../shared/running-flowx.md`](../shared/running-flowx.md): how the other skills run on each path.
- `app/README.md` (plugin root, not a skill file): the app's optional live Azure features.
