# flowx MCP server (Databricks App)

Hosts flowx's migration phases and helper operations as [Model Context
Protocol](https://modelcontextprotocol.io) tools so agentic clients (Claude
Code, Claude Desktop, Databricks Genie Code) can drive an ADF → Databricks
Lakeflow migration by calling tools instead of shelling out to a CLI.

The MCP server itself lives in the flowx package at
[`src/flowx/mcp/`](../src/flowx/mcp); this directory is just the
Databricks App wrapper and deployment tooling.

## Tool

The server exposes a **single** MCP tool, `flowx(command, parameters)`, to stay well under
host tool-count limits (e.g. Genie Code's 20-tools-across-all-servers cap). `command` selects the
operation; `parameters` is its keyword-argument dict.

| `command` | Wraps | Purpose |
|-----------|-------|---------|
| `inputs` | `adapter inputs` | List a phase's input prompts/defaults |
| `discover` | `adapter discover` | Parse ADF JSON, classify activities |
| `convert` | `adapter convert` | ADF activities → Databricks IR |
| `merge_agentic` | `adapter convert --merge-agentic` | Merge agent-produced results into the report |
| `inspect` | `adapter inspect` | Surface pending translation options |
| `apply_answers` | `adapter modify` | Apply answers → stamped IR |
| `materialize_lookup` | `adapter materialize-lookup` | CSV → lookup-values JSON |
| `workspace_paths` | `adapter workspace-paths` | Detect workspace paths / hosts |
| `package` | `adapter package` | Emit the deployable DAB bundle |
| `migrate` | discover→convert→package | Full non-interactive migration |
| `record_results` | `adapter record-results` | Write coverage to a UC table |
| `install_dashboard` | `adapter install-dashboard` | Publish the coverage dashboard |

Example: `flowx(command="discover", parameters={"adf_source_path": "/Volumes/main/default/adf_export", "output_dir": "./out"})`.

Each command is a thin bridge over `python -m flowx.adapter` (the same entry point the agent
skills use), then reads back the JSON/CSV artifacts each phase writes — so the MCP surface stays in
lockstep with the tested CLI contract.

## Run locally

```bash
pip install -e ".[mcp]"            # from the repo root

# stdio transport (Claude Code / Claude Desktop):
python -m flowx.mcp

# streamable-HTTP transport (same server Databricks Apps runs):
python -m flowx.mcp --http --port 8000
# MCP endpoint: http://localhost:8000/mcp   health: http://localhost:8000/
```

Register the stdio server with a local MCP client, e.g.:

```json
{ "mcpServers": { "flowx": { "command": "python", "args": ["-m", "flowx.mcp"] } } }
```

## Deploy as a Databricks App (for Genie Code)

**Recommended — the `deploy_app.py` notebook (SDK, runs on serverless).** Open `app/deploy_app.py`
in the workspace and run it, setting the `repo_root` widget to the flowx checkout
(e.g. `/Workspace/Shared/flowx`). It uses the Databricks SDK to stage a self-contained bundle (the
app entrypoint plus a vendored copy of the pure-Python `flowx` package) to `/Workspace/Shared/mcp-flowx`
and create/deploy the app (default name **`mcp-flowx`**). Because it uploads through the SDK Workspace
API, it runs directly in a serverless Genie Code session — no CLI needed. The deployment logic lives in
`app/deploy_helpers.py` (imported by the notebook, so it is also unit-testable).

**CLI alternative — `deploy.sh`:**

```bash
# Authenticated Databricks CLI (v0.230+) required — run from a web terminal or local machine.
./app/deploy.sh
```

`deploy.sh` stages a self-contained bundle in a temporary directory outside the repo, syncs it to your
workspace via `databricks sync`, and creates/deploys the app. `databricks apps deploy` / `sync` are
not available from serverless notebook Python, which is why the notebook above is preferred inside
Genie Code.

> **Clone into `/Workspace/Shared`.** `deploy.sh` deploys the app source from
> `/Workspace/Shared/<app-name>` because the app's service principal **cannot read
> private `/Workspace/Users/<you>` folders** by default. Clone flowx into a Git
> folder under `/Workspace/Shared` (e.g. `/Workspace/Shared/flowx`); if that
> folder is restricted in your workspace, use another all-users location and pass it
> via `APP_SOURCE_PATH`.

End-to-end, to use it from Genie Code:

1. **Deploy** by running the `app/deploy_app.py` notebook (or `./app/deploy.sh` from a CLI session).
   The app is named `mcp-flowx` and deploys the source from `/Workspace/Shared/mcp-flowx`. The
   deployer prints the app URL; the MCP endpoint is `<app-url>/mcp`.
2. **Grant app access:** give **Can use** on `mcp-flowx` to the users / service
   principals that will call it (Apps UI → *Permissions*, or
   `databricks apps set-permissions mcp-flowx ...`).
3. **Grant data access:** the app authenticates as its own service principal, so
   grant that principal access to the catalogs / schemas / Unity Catalog volumes
   the migration reads/writes (and any SQL warehouse used by `record-results` /
   `install-dashboard`).
4. **Add it in Genie Code (Agent mode):** open Genie Code **Settings → MCP Servers →
   Add Server**, choose **Custom MCP server**, select the `mcp-flowx` app, and
   **Save**. The `flowx` tool becomes available immediately. Verify via the
   health endpoint `<app-url>/`.

> The `mcp-` name prefix also makes the app **auto-listed in the AI Playground**. Genie Code's
> **Custom MCP server** picker selects any Databricks App by name regardless of prefix.

Genie Code requires a custom MCP app to be (1) in the **same workspace**, (2) reachable
at `https://<app-url>/mcp`, and (3) **stateless** — this server sets
`stateless_http=True` and adds CORS, so it qualifies. If a browser CORS error appears,
set the app env var `FLOWX_ALLOWED_ORIGINS` to your workspace URL and redeploy.
MCP access is capped at **20 tools** across all servers; flowx exposes just **one** tool
(`flowx`, with 12 commands), so it uses a single slot.

> In serverless Genie Code, deploy with the `app/deploy_app.py` notebook (SDK-based). `./app/deploy.sh`
> must run from a Databricks CLI session (workspace web terminal or a local machine), since
> `databricks apps` deploy is not available from serverless notebook Python. See [Connect Genie Code
> to MCP servers](https://learn.microsoft.com/en-us/azure/databricks/genie-code/mcp) and [host a
> custom MCP server](https://docs.databricks.com/aws/en/generative-ai/mcp/custom-mcp).

## Troubleshooting

**Genie Code can't connect / add the server (is it CORS or the server?)** — Tell them apart
from the app's logs (`databricks apps logs <app>` or the app UI):

- **Server-side error (not CORS):** the logs show a `500` / `RuntimeError: Task group is not
  initialized` on `POST /mcp`. That means the StreamableHTTP session manager never started —
  it happens if the MCP app is *mounted inside another Starlette app* (whose lifespan doesn't
  run the sub-app). The server now avoids this by serving FastMCP's own app directly; make sure
  you redeployed the current `app/`. Sanity-check the app is up with `curl <app-url>/`
  (expect `{"status":"ok"}`).
- **CORS:** the request reaches the server fine but the **browser console** shows a CORS error
  (blocked by `Access-Control-Allow-Origin`), with no corresponding 500 in the app logs. Set the
  app env var `FLOWX_ALLOWED_ORIGINS` to your workspace URL and redeploy.

**Server connects but Genie Code "fails to fetch tools"** — the connection (`initialize`) succeeds,
but `tools/list` comes back empty / errors. This is a **schema-compatibility** problem, not a
transport one: Genie Code's MCP client rejects tools that declare an `outputSchema` (the structured-
output feature from the 2025-06-18 spec). FastMCP derives one automatically from a tool's return-type
annotation, so the `flowx` tool registers with `@mcp.tool(structured_output=False)` to suppress
it (the result is still returned as JSON text). If you see this after customizing the server, make
sure no tool emits an `outputSchema` — check with `curl`-ing a `tools/list` request or inspecting
`mcp.list_tools()`.

**`403 Forbidden` with `Invalid Origin header`, or `421 Misdirected Request` with `Invalid Host
header: localhost:8000`** (from `transport_security.py` in the logs) — this is the MCP SDK's
DNS-rebinding protection, **not** CORS. Behind the Databricks Apps OAuth proxy the app sees the
workspace `Origin` and a proxied `Host: localhost:8000`, so that allowlist check misfires. The
server now **disables** DNS-rebinding protection (the OAuth proxy already authenticates every
request); just redeploy the current `app/`. Note: `FLOWX_ALLOWED_ORIGINS` controls **only**
browser CORS now — it does not enable or affect this Host/Origin check.



**`mkdir: cannot create directory ...: Permission denied`** — On Databricks (Genie web
terminal / serverless), `/tmp` and `$TMPDIR` are often not writable, while the `/Workspace`
filesystem where the repo lives is. `deploy.sh` stages the bundle in the **repo's parent
directory** first (e.g. `/Workspace/Shared`, which is writable and outside the git repo),
falling back to `$TMPDIR` / `/tmp` / `$HOME` for local runs. If it still can't find a writable
base, set `TMPDIR` to a writable path and re-run. (Staging is never placed inside the repo,
so `databricks sync` won't drop files via the repo's `.gitignore`.)

**`Error: please specify target`** — The Databricks CLI (v0.298+) makes `sync` and
`apps deploy` bundle-aware: if a `databricks.yml` is discoverable in the working
directory or any parent (for example a generated `flowx_output/databricks.yml`,
or one in your workspace home), the CLI loads that bundle and—when it has multiple
targets with no default—aborts with this error before deploying. `deploy.sh` already
runs every CLI call from a throwaway directory to avoid this; if you invoke the CLI
manually, do the same (or pass `--target <name>`), and don't run it from inside a
generated bundle directory.

## Inputs and outputs on a hosted app

A Databricks App can't read the user's workspace / UC Volume files (only the container's
ephemeral disk is local; `/Volumes/...` is **not** auto-mounted). The MCP surface handles this
by passing data **inline** through the tool, since the calling agent *can* read those files:

- **Input — small jobs:** pass the ADF JSON via `adf_definitions` — a mapping of relative path → JSON
  content mirroring the ADF Git-export layout (`pipeline/…`, `dataset/…`, `linkedService/…`,
  `trigger/…`); a single ARM-template object is also accepted. The server materializes it to a temp
  dir. Capped at ~5 MB (`FLOWX_MAX_INLINE_BYTES`) since it flows through the agent's context.
- **Input — large factories (recommended):** point the server at the source by reference, so the
  bytes bypass the agent and it scales to thousands of pipelines:
  - `adf_volume_path` — a UC Volume directory, downloaded via the SDK **Files API**.
  - `adf_workspace_path` — a `/Workspace` directory (e.g. an ADF Git folder), listed and downloaded
    via the SDK **Workspace API** (workspace files use the Workspace API, *not* the Files API).

  Grant the app's service principal read on whichever path you use. (`adf_source_path` / `source_dir`
  remain for paths the server itself can read — local hosting or a mounted volume.)
- **Output — large bundles (recommended):** `package`/`migrate` write the DAB to the target via the
  SDK so the contents bypass the agent. Pass `output_volume_path` (uploaded to a UC Volume via the SDK
  Files API) **or** `output_workspace_path` (uploaded to a `/Workspace` directory via the SDK Workspace
  API, with `ImportFormat.RAW` so files land verbatim rather than as notebooks). Either returns
  `bundle_uploaded` (location + file list). Grant the service principal write on the target.
- **Output — small bundles:** without an output path, `package`/`migrate` return the DAB inline as
  `bundle = {"files": {relpath: text, …}, "truncated": [...]}` (capped ~2 MB) for the caller to persist.

## Known constraints / follow-ups

- **`databricks bundle validate/deploy`** of the *generated* DAB is a separate,
  user-driven step (run from a CLI session); it is intentionally not invoked by
  these tools.
- **Long-running phases.** Large factories can exceed default client timeouts;
  the server-side subprocess timeout is configurable via `FLOWX_MCP_TIMEOUT`
  (seconds).
