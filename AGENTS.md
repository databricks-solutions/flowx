# AI Agent Guidelines for flowx

## Quick Command Reference

```bash
make dev          # Install dependencies (development; uses uv)
make test         # Unit tests
make integration  # Integration tests (requires ADF fixtures)
make fmt          # Format + lint (ruff + mypy)
make clean        # Remove build artifacts
```

To run the **plugin skills** (discover/convert/package/migrate) without a uv-based dev setup,
bootstrap a self-contained virtual environment with pip via the `setup` skill or directly:

```bash
bash scripts/bootstrap.sh   # creates the venv, pip-installs requirements.txt, writes .migration-venv
# then run plugin code with src/ on PYTHONPATH, using the interpreter from the marker file:
PY="$(cat .migration-venv)"
PYTHONPATH=src "$PY" -m flowx.adapter inputs discover
```

`bootstrap.sh` creates the venv at `/Workspace/Users/<current user>/.migration-skills` when running
under Databricks (Genie Code / notebooks; detected via `DATABRICKS_RUNTIME_VERSION`) and at
`<plugin_dir>/.venv` everywhere else. It writes the resolved interpreter path to
`<plugin_dir>/.migration-venv`; read that marker file rather than hardcoding an interpreter path.

### Databricks Serverless / Genie Code Compatibility

All skills and the bootstrap script are designed to run on **Databricks serverless compute**
(Genie Code, notebook serverless) as well as local machines. Key adaptations:

- **venv:** `bootstrap.sh` creates the venv at `/Workspace/Users/<current user>/.migration-skills`
  on Databricks and `<plugin_dir>/.venv` locally, writing the interpreter path to
  `<plugin_dir>/.migration-venv`. It falls back to `--without-pip` + `get-pip.py` when `ensurepip`
  is unavailable (standard on serverless images).
- **Auth:** `workspace_downloader.py` auto-detects `DATABRICKS_RUNTIME_VERSION` and writes
  `~/.databrickscfg` from `dbruntime.databricks_repl_context` so the SDK can authenticate.
- **CLI:** `databricks bundle validate/deploy` is NOT available on serverless — use the web
  terminal or a local CLI session for those steps.

## Project Overview

flowx is an agent plugin that translates Azure Data Factory (ADF) pipeline definitions into Databricks Lakeflow Jobs via Declarative Automation Bundles (DABs).

## Data Flow

```
ADF JSON -> Parse (AST) -> Classify (Inventory) -> Convert (IR) -> Package (Tasks + Notebooks) -> Bundle (DABs)
```

## Architecture

### Three-Phase Pipeline
All three phases write into one shared `<output_dir>` (default `./flowx_output`):
the DAB bundle at the top level, kept artifacts under `metadata/`, and transient
intermediates under `.work/` (pruned by `package`).

1. **Discover** -- Parse ADF JSON from UC volumes -> typed AST -> `metadata/inventory.json` + `metadata/profile_report.csv` + verbatim `metadata/<pipeline>.arm.json`
2. **Convert** -- Registry dispatch + topological sort -> Pipeline IR (deterministic + agentic gaps); transient report at `.work/translation_report.json`
3. **Package** -- IR -> DAB YAML + generated notebooks + setup scripts; prunes `.work/`

### Key Patterns
- `@dataclass(slots=True, kw_only=True)` for all models
- Immutable `TranslationContext` threaded through visitors
- Registry-based dispatch with match statement for control-flow types
- `TranslationStrategy` enum: DETERMINISTIC > AGENTIC > UNSUPPORTED

## Module Descriptions

| Module | Purpose |
|--------|---------|
| `models/adf_ast.py` | Typed AST nodes for ADF definitions |
| `models/ir.py` | Databricks intermediate representation |
| `models/dab.py` | DAB output schema types |
| `parser/adf_loader.py` | Parses ADF exports, produces `metadata/inventory.json` + `metadata/profile_report.csv` |
| `parser/expression_parser.py` | Translates ADF expressions (@activity, @pipeline, @variables) |
| `translator/engine.py` | Registry dispatch, topological sort, context threading |
| `translator/activity_translators/` | One module per deterministic activity type (16 total) |
| `preparer/workflow_preparer.py` | Orchestrates activity preparers |
| `preparer/code_generator.py` | Notebook code generation for activity types |
| `preparer/activity_preparers/` | One module per activity type |
| `bundler/dab_writer.py` | Generates databricks.yml, job YAML, resources |
| `bundler/notebook_writer.py` | Writes generated notebooks to bundle |
| `bundler/setup_generator.py` | Setup scripts for UC volumes, secrets, connections |
| `reporting/coverage.py` | Builds per-pipeline coverage rows from `metadata/` |
| `reporting/results.py` | Writes per-run coverage to a UC table (run_id/run_date/run_by) via the SDK |
| `reporting/dashboard.py` | Installs + publishes an AI/BI coverage dashboard over the results table |

## Activity Types

### Deterministic Types (16)
Copy, DatabricksNotebook, DatabricksSparkJar, DatabricksSparkPython, ForEach, IfCondition, SetVariable, Lookup, WebActivity, Delete, ExecutePipeline, DatabricksJob, Switch, Wait, Filter, AppendVariable

### Agentic Fallback Types (12)
ExecuteDataFlow, SqlServerStoredProcedure, AzureFunction, WebHook, Custom, ExecuteSSISPackage, AzureMLExecutePipeline, GetMetadata, Validation, Fail, Script, Until

## Testing Standards

- Unit tests in `tests/unit/`, one test file per translator
- Integration tests in `tests/integration/`, require ADF fixture files
- Test fixtures in `tests/resources/json/`
- Run `make test` for unit tests, `make integration` for integration tests
- All translators must have corresponding test coverage

## Code Style Rules

- Python 3.12+, line length 120 characters
- `ruff` for formatting and linting, `mypy` for type checking
- `@dataclass(slots=True, kw_only=True)` for all models
- Never modify TranslationContext in place -- always return a new instance
- Control-flow types (ForEach, IfCondition, Switch, SetVariable, AppendVariable) thread context
- Leaf types return Activity only, control-flow returns (Activity, TranslationContext)
- Use `parse_expression()` for ADF expression translation, return None for unsupported

### Naming, docstrings, and comments

- Spell names out: use unabbreviated variable, parameter, function, and class names
  (`parameter_values` not `params`, `whole_reference` not `whole`). Short loop indices and
  regex match binders (`match`, `item`) are fine.
- Write docstrings in plain, conversational language aimed at both users and maintainers.
  Say what the function does and why in everyday terms; skip jargon and marketing tone.
- Prefer self-documenting code over inline comments. Reserve comments for the non-obvious
  *why* (a workaround, a spec quirk, a subtle ordering constraint) -- not for restating what
  the code already says. Delete comments that narrate self-evident lines.

## Adding a New Deterministic Translator

1. Add IR dataclass to `src/flowx/models/ir.py`
2. Create translator at `src/flowx/translator/activity_translators/<type>.py`
3. Create preparer at `src/flowx/preparer/activity_preparers/<type>.py`
4. Add notebook generator to `src/flowx/preparer/code_generator.py` if needed
5. Register in engine.py (TRANSLATOR_REGISTRY for leaf, match statement for control-flow)
6. Move from AGENTIC_TYPES to DETERMINISTIC_TYPES in adf_loader.py
7. Update activity-mapping.md reference
8. Add test fixtures and unit tests

## MCP server design notes

Rationale behind non-obvious choices in `src/flowx/mcp/server.py` (kept here so the code carries
only one-line pointers):

- **`@mcp.tool(structured_output=False)`** — FastMCP derives an `outputSchema` from a tool's
  `-> dict` return annotation, but Databricks Genie Code's MCP client rejects tools that declare an
  `outputSchema` (a 2025-06-18 spec feature): `tools/list` fails and Genie reports "can't fetch
  tools" even though `initialize` succeeded. Suppressing it returns the dict as JSON text instead,
  which every client accepts.
- **`build_http_app` returns FastMCP's own app** — it is not mounted inside another Starlette app.
  Starlette does not run the lifespan of a *mounted* sub-app, so mounting leaves FastMCP's
  StreamableHTTP session manager uninitialized and every `/mcp` request 500s with "Task group is not
  initialized".
- **DNS-rebinding protection disabled** (`_transport_security`) — behind the Databricks Apps OAuth
  proxy the SDK sees the workspace `Origin` and a proxied `Host: localhost:<port>`, so its Host/Origin
  allowlist misfires (403/421) while adding nothing on top of the proxy's authentication. Browser
  CORS is a separate concern configured via `FLOWX_ALLOWED_ORIGINS`.
