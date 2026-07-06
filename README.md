# flowx

ADF to Databricks Lakeflow Jobs translator, delivered as agent skills.

flowx converts Azure Data Factory (ADF) pipeline definitions into Databricks Lakeflow Jobs packaged as Declarative Automation Bundles (DABs). It deterministically translates known activity types and falls back to agentic (LLM-assisted) translation for complex or rare types. flowx runs as a set of [agent skills](skills/) usable from Databricks Genie Code, Claude Code, or any tool that supports the Agent Skills standard.

## Architecture

```
                         flowx Pipeline
                         ==================

  ADF JSON (UC Volumes / Workspace)
        |
        v
  +------------------+
  |  1. DISCOVER     |    Parse ADF ARM/JSON exports
  |  adf_loader.py   | -> Typed AST -> metadata/inventory.json
  +------------------+
        |
        v
  +------------------+
  |  2. CONVERT      |    Registry dispatch + topological sort
  |  engine.py       | -> Pipeline IR (deterministic + agentic gaps)
  +------------------+
        |
        v
  +------------------+
  |  3. PACKAGE      |    IR -> DAB YAML + notebooks + setup scripts
  |  dab_writer.py   | -> Deployable DABs project
  +------------------+
        |
        v
  databricks bundle validate / deploy
```

The phases are exposed two ways: as **skills** the agent runs directly (via a Python virtual
environment locally), and as a single **MCP tool** hosted on a Databricks App (for Genie Code). See
[Running flowx as an MCP server](#running-flowx-as-an-mcp-server).

## Installation

flowx installs in one of two shapes depending on where your agent runs. Full, step-by-step
instructions for both are in the [installation docs](docs/content/docs/installation.mdx); the
summary:

### Databricks Genie Code

The phases run as an MCP server (a Databricks App). Clone the repo into **`/Workspace/Shared`**
(so the app's service principal can read the source), copy `skills/` into your skills folder, then
run the setup skill:

```
@flowx-setup
```

On Databricks, `flowx-setup` deploys the `mcp-flowx` app for you. You can also deploy it directly by
running the **`app/deploy_app.py`** notebook (SDK-based, works on serverless), or `app/deploy.sh`
from a workspace web terminal. Then add the app under Genie Code **Settings → MCP Servers → Add
Server → Custom MCP server**.

### Claude Code (and other local agent harnesses)

flowx is a Claude Code plugin distributed through its marketplace:

```
/plugin marketplace add databricks-solutions/flowx
/plugin install flowx@flowx
```

Run `/reload-plugins`, then set up the local runtime once:

```
/flowx:flowx-setup
```

This provisions a Python virtual environment (via `scripts/bootstrap.sh`) and writes a
`.migration-venv` marker the phase skills read. No `uv` is required for plugin users.

## Usage

Run the end-to-end migration:

```
/flowx:flowx-migrate
```

Or run individual phases:

```
/flowx:flowx-discover    # Parse ADF JSON, produce inventory + complexity report
/flowx:flowx-convert     # Deterministic + agentic translation
/flowx:flowx-package     # Generate DABs project
```

(In Genie Code, invoke the same skills with the `@` prefix, e.g. `@flowx-migrate`.)

## Setup

`/flowx:flowx-setup` keys off the `DATABRICKS_RUNTIME_VERSION` environment variable
(the same signal the rest of the plugin uses to detect Databricks) and prepares one
of two execution paths:

- **Local / Claude Code (virtual environment).** The phases run from the plugin's
  CLI. Setup runs `scripts/bootstrap.sh`, which creates a `.venv`, installs
  `requirements.txt`, and writes the resolved interpreter path to a
  `.migration-venv` marker file that the phase skills read. Optionally, a local
  (stdio) MCP server can be registered to drive the phases through MCP tools
  instead of the CLI.

- **Databricks Genie Code (MCP server, no virtual environment).** The phases run as
  a single `flowx` MCP tool hosted on a Databricks App. Setup runs `app/deploy.sh`,
  which stages a self-contained bundle, syncs it to `/Workspace/Shared/mcp-flowx`,
  and deploys the `mcp-flowx` app. You then grant app/data access and register the
  app under Genie Code **Settings → MCP Servers**. No venv is created on this path.

Run setup once before any other flowx skill, or again whenever the environment is
missing.

## Supported ADF Activity Types

### Deterministic (16 types)

| ADF Activity | Databricks Task | Category |
|---|---|---|
| Copy | Notebook task | Data movement |
| DatabricksNotebook | Notebook task | Compute |
| DatabricksSparkJar | Spark JAR task | Compute |
| DatabricksSparkPython | Spark Python task | Compute |
| ForEach | for_each_task | Control flow |
| IfCondition | if_else_task | Control flow |
| Switch | if_else_task chain | Control flow |
| SetVariable | run_job_task | Control flow |
| AppendVariable | run_job_task | Control flow |
| Filter | Notebook task | Control flow |
| Wait | Notebook task (sleep) | Control flow |
| Lookup | Notebook task | Data access |
| WebActivity | Notebook task | External |
| Delete | Notebook task | Data management |
| ExecutePipeline | run_job_task | Orchestration |
| DatabricksJob | run_job_task | Compute |

### Agentic Fallback (12 types)

Activities with complex semantics, or without a direct Databricks equivalent, are translated by the
agent using LLM-assisted reasoning from the activity's ARM JSON.

| ADF Activity | Strategy |
|---|---|
| ExecuteDataFlow | LLM-assisted (agentic) |
| SqlServerStoredProcedure | LLM-assisted (agentic) |
| AzureFunction | LLM-assisted (agentic) |
| WebHook | LLM-assisted (agentic) |
| Custom | LLM-assisted (agentic) |
| ExecuteSSISPackage | LLM-assisted (agentic) |
| AzureMLExecutePipeline | LLM-assisted (agentic) |
| GetMetadata | LLM-assisted (agentic) |
| Validation | LLM-assisted (agentic) |
| Fail | LLM-assisted (agentic) |
| Script | LLM-assisted (agentic) |
| Until | LLM-assisted (agentic) |

## How It Works

### Phase 1: Discover
Reads ADF JSON definitions from Unity Catalog volumes (or a `/Workspace` Git folder), normalizes ARM template format, parses into typed AST nodes, and classifies each activity as deterministic, agentic, or unsupported. Produces `metadata/inventory.json` and a per-pipeline complexity report at `metadata/profile_report.csv`.

### Phase 2: Convert
Applies deterministic translators via registry dispatch, resolves dependencies through topological sort, and threads immutable `TranslationContext` through control-flow visitors. Agentic gaps are flagged for LLM-assisted translation. Produces Pipeline IR.

### Phase 3: Package
Converts Pipeline IR into a deployable DABs project: `databricks.yml`, per-job YAML resource files, generated Python notebooks, and setup scripts for UC volumes, secrets, and connections.

## Output Format

All three phases write into one shared output directory (default `./flowx_output`):

```
flowx_output/
  databricks.yml              # Bundle configuration (package)
  resources/
    jobs/
      <pipeline_name>.yml     # One job per ADF pipeline
  src/
    notebooks/
      <pipeline_name>/
        <activity_name>.py    # Generated notebooks per activity
    setup/
      create_volumes.py       # UC volume setup
      create_secrets.py       # Secret scope setup
      create_connections.py   # Connection setup
  SETUP.md                    # Setup instructions (package)
  metadata/
    inventory.json            # discover: activity inventory
    profile_report.csv        # discover: per-pipeline complexity report
    <pipeline>.arm.json       # discover: verbatim original ADF/ARM source
    configuration.json        # modify: collected configuration answers
  .work/                      # transient intermediates (translation report, IR, gaps.json); pruned by package
```

## Running flowx as an MCP server

The phases are also packaged as [Model Context Protocol](https://modelcontextprotocol.io) tools (in
[`src/flowx/mcp/`](src/flowx/mcp)) so an agent can invoke them directly instead of shelling out to
the CLI. The server exposes a single `flowx(command, parameters)` tool to stay under host tool
limits. For Databricks Genie Code it runs as a Databricks App; see the [app README](app/README.md)
for deployment (SDK notebook or CLI script) and Genie Code registration.

## Development

```bash
make dev          # Install dependencies (uses uv)
make test         # Run unit tests
make integration  # Run integration tests
make fmt          # Format + lint (ruff + mypy)
make clean        # Remove build artifacts
```

### Prerequisites
- Python 3.12+
- [uv](https://docs.astral.sh/uv/) package manager

These prerequisites are for contributing to the flowx project. Plugin *users* do not need `uv` —
`flowx-setup` provisions the runtime (a pip-based `.venv` locally, or the MCP server on Databricks).

## Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/my-feature`)
3. Follow the [adding a new translator](CLAUDE.md#adding-a-new-deterministic-translator) guide
4. Run `make fmt && make test` before committing
5. Open a pull request
