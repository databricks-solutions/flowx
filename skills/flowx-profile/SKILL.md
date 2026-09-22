---
name: flowx-profile
description: >
  Profile an Azure Data Factory / Synapse / Fabric estate before migrating it. Discovers every
  factory, pipeline, activity, dataset, linked service, integration runtime, and trigger, adds a
  runtime + cost profile over a lookback window, and writes CSV/Markdown reports. Runs against a
  live Azure tenant.
triggers:
  - "profile ADF"
  - "profile data factory"
  - "profile pipelines"
  - "extract pipelines"
  - "azure data factory profiler"
  - "scan my data factories"
  - "inventory ADF estate"
---

# Profile an Azure Data Factory estate

Survey a live Azure tenant to inventory and analyze every Data Factory, Synapse, and Fabric
pipeline, then write CSV + Markdown reports. This is a standalone pre-migration survey — it reads
from Azure via the ARM / Synapse / Fabric APIs and does not consume or produce the
`discover`/`convert`/`package` phase artifacts.

The skill's only job is to collect the run's arguments and invoke the `profile` command. Every
argument is optional; with none set the profiler surveys the whole tenant over a 90-day window.

## Step 1 — Gather the run arguments

Ask the user which of these to set, and default the rest:

| Parameter | Purpose | Default |
|---|---|---|
| `subscription_id` | Limit to one subscription | all accessible subscriptions |
| `resource_group` | Limit to one resource group | all |
| `factory_name` | Limit to one data factory | all |
| `days` | Runtime/cost lookback window, in days | `90` |
| `no_synapse` | Skip Synapse workspaces | Synapse included |
| `no_fabric` | Skip Fabric workspaces | Fabric included |
| `output_dir` | Base output dir (a timestamped subfolder is created per run) | `./output` |
| `tenant_id` + `client_id` + `client_secret` | Service-principal auth (all three together) | `az login` session (local only) |

Scoping to a subscription or factory and narrowing `days` makes a first run much faster; suggest
that for large estates.

## Step 2 — Run the profiler (MCP tool or venv CLI)

Run the **`flowx-setup`** skill first if you haven't. The two paths differ only in how the command
is invoked.

- **MCP tool (Databricks Genie Code, or a local stdio registration):** call the single **`flowx`**
  tool. Run **no** `python3`/`$PY` commands on this path.

  ```
  flowx(command="profile", parameters={"subscription_id": "<id>", "resource_group": "<rg>",
                                        "factory_name": "<name>", "days": 90,
                                        "no_synapse": false, "no_fabric": false,
                                        "output_dir": "./output",
                                        "tenant_id": "<t>", "client_id": "<c>", "client_secret": "<s>"})
  ```

  Two constraints on this path:
  - **Auth:** the deployed app has no `az login` and no Azure identity, so you **must** pass the
    `tenant_id` / `client_id` / `client_secret` trio. Without them the run can't reach Azure ARM.
  - **Output:** the app's `output_dir` is ephemeral and not reachable from your workspace. Point
    `output_dir` at a mounted UC Volume path (e.g. `/Volumes/cat/sch/adf_profile`) to keep the
    reports, or run the venv path locally when you need the files. The command returns the run
    summary and the list of files it wrote.
  - A full-estate profile can run for minutes; the server's per-command timeout defaults to 1800s
    (override with the `FLOWX_MCP_TIMEOUT` env var on the app).

- **venv CLI (local, no MCP server):** ensure the venv exists (`flowx-setup` Path B /
  `bootstrap.sh`), authenticate to Azure (`az login`, or pass the service-principal trio), then run
  the `profile` command with the venv interpreter and `src/` on `PYTHONPATH`:

  ```bash
  export PYTHONPATH="<plugin_dir>/src"
  PY="$(cat <plugin_dir>/.migration-venv)"
  "$PY" -m flowx.adapter profile \
    [--subscription-id <id>] [--resource-group <rg>] [--factory-name <name>] \
    [--days <n>] [--no-synapse] [--no-fabric] \
    [--output-dir <dir>] \
    [--tenant-id <t> --client-id <c> --client-secret <s>]
  ```

  Where `<plugin_dir>` is the flowx plugin root (the directory containing `src/`, `skills/`, and
  `requirements.txt`). `az login` is the simplest auth; suggest the user run it themselves
  (in this session, `! az login`).

The run prints its progress and a summary; on success it reports the timestamped output folder and
log path (venv path), or returns them in the result (MCP path).

## Step 3 — Point the user at the output

Each run writes a timestamped folder under `output_dir`:

```
<output-dir>/YYYY-MM-DD_HHMMSS/
├── overall.csv + .md            # estate-wide rollup
├── ingestion/                   # connectors, copy activities, runtimes, tables, cost
│   ├── ingestion.csv + .md
│   └── details/…
├── orchestration/               # activity types, pipeline runs, dataflows, triggers, cost
│   ├── orchestration.csv + .md
│   └── details/…
└── extraction_YYYYMMDD_HHMMSS.log
```

Surface the folder path and highlight `overall.csv`/`.md` plus the `ingestion/` and
`orchestration/` summaries as the starting points.

## Notes on permissions

The profiler degrades gracefully when access is missing, but coverage depends on Azure roles:

- **Static extraction** — `Reader` on the subscription(s), or `Data Factory Contributor` on
  specific factories; Synapse workspaces need `Synapse Artifact User`; Fabric needs a workspace
  role (Member/Contributor/Admin).
- **Runtime + cost profiling** — additionally `Data Factory Contributor` for pipeline/activity run
  queries and SHIR monitoring, and `Cost Management Reader` for billed costs. Missing cost access
  is reported per subscription and skipped, not fatal.

## Examples

- "Profile our Azure Data Factory estate"
- "Profile just the `prod-adf` factory over the last 30 days"
- "Scan subscription `<id>`, skip Synapse and Fabric"
- "Run the ADF profiler with a service principal"
