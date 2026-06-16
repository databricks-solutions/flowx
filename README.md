# flowx

ADF to Databricks Lakeflow Jobs translator via Declarative Automation Bundles.

flowx is a Claude Code plugin that converts Azure Data Factory (ADF) pipeline definitions into Databricks Lakeflow Jobs packaged as Declarative Automation Bundles (DABs). It deterministically translates known activity types and falls back to agentic LLM-assisted translation for complex or rare types.

## Architecture

```
                         flowx Pipeline
                         ==================

  ADF JSON (UC Volumes)
        |
        v
  +------------------+
  |  1. PROFILE      |    Parse ADF ARM/JSON exports
  |  adf_loader.py   | -> Typed AST -> metadata/inventory.json
  +------------------+
        |
        v
  +------------------+
  |  2. TRANSLATE     |    Registry dispatch + topological sort
  |  engine.py       | -> Pipeline IR (deterministic + agentic gaps)
  +------------------+
        |
        v
  +------------------+
  |  3. PREPARE       |    IR -> DAB YAML + notebooks + setup scripts
  |  dab_writer.py   | -> Deployable DABs project
  +------------------+
        |
        v
  databricks bundle validate / deploy
```

## Quick Start

1. Install the plugin in Claude Code:
   ```bash
   claude plugin install ghanse/flowx
   ```

2. Run the end-to-end migration:
   ```
   /flowx:flowx-migrate
   ```

   Or run individual phases:
   ```
   /flowx:flowx-discover     # Parse ADF JSON, produce inventory + complexity report
   /flowx:flowx-convert   # Deterministic + agentic translation
   /flowx:flowx-package     # Generate DABs project
   ```

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

| ADF Activity | Strategy |
|---|---|
| ExecuteDataFlow | LLM-assisted via adf-to-databricks-plugin |
| SqlServerStoredProcedure | LLM-assisted via adf-to-databricks-plugin |
| AzureFunction | LLM-assisted via adf-to-databricks-plugin |
| WebHook | LLM-assisted via adf-to-databricks-plugin |
| Custom | LLM-assisted via adf-to-databricks-plugin |
| ExecuteSSISPackage | LLM-assisted via adf-to-databricks-plugin |
| AzureMLExecutePipeline | LLM-assisted via adf-to-databricks-plugin |
| GetMetadata | LLM-assisted via adf-to-databricks-plugin |
| Validation | LLM-assisted via adf-to-databricks-plugin |
| Fail | LLM-assisted via adf-to-databricks-plugin |
| Script | LLM-assisted via adf-to-databricks-plugin |
| Until | LLM-assisted via adf-to-databricks-plugin |

## How It Works

### Phase 1: Discover
Reads ADF JSON definitions from Unity Catalog volumes, normalizes ARM template format, parses into typed AST nodes, and classifies each activity as deterministic, agentic, or unsupported. Produces `metadata/inventory.json` and a per-pipeline complexity report at `metadata/profile_report.csv`.

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
    profile_report.csv        # profile: per-pipeline complexity report
    <pipeline>.arm.json       # discover: verbatim original ADF/ARM source
    configuration.json        # modify: collected configuration answers
  .work/                      # transient intermediates (translation report, IR, gaps.json); pruned by prepare
```

## Development

```bash
make dev          # Install dependencies
make test         # Run unit tests
make integration  # Run integration tests
make fmt          # Format + lint (ruff + mypy)
make clean        # Remove build artifacts
```

### Prerequisites
- Python 3.12+
- [uv](https://docs.astral.sh/uv/) package manager

## Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/my-feature`)
3. Follow the [adding a new translator](CLAUDE.md#adding-a-new-deterministic-translator) guide
4. Run `make fmt && make test` before committing
5. Open a pull request
