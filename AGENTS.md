# AI Agent Guidelines for Orchestra

## Quick Command Reference

```bash
make dev          # Install dependencies (development; uses uv)
make test         # Unit tests
make integration  # Integration tests (requires ADF fixtures)
make fmt          # Format + lint (ruff + mypy)
make clean        # Remove build artifacts
```

To run the **plugin skills** (ingest/translate/prepare/migrate) without a uv-based dev setup,
bootstrap a self-contained virtual environment with pip via the `setup` skill or directly:

```bash
bash scripts/bootstrap.sh   # creates .venv and pip-installs requirements.txt
# then run plugin code with src/ on PYTHONPATH:
PYTHONPATH=src .venv/bin/python -m orchestra.adapter inputs ingest
```

## Project Overview

Orchestra is an agent plugin that translates Azure Data Factory (ADF) pipeline definitions into Databricks Lakeflow Jobs via Declarative Automation Bundles (DABs).

## Data Flow

```
ADF JSON -> Parse (AST) -> Classify (Inventory) -> Translate (IR) -> Prepare (Tasks + Notebooks) -> Bundle (DABs)
```

## Architecture

### Three-Phase Pipeline
1. **Ingest** -- Parse ADF JSON from UC volumes -> typed AST -> inventory.json
2. **Translate** -- Registry dispatch + topological sort -> Pipeline IR (deterministic + agentic gaps)
3. **Prepare** -- IR -> DAB YAML + generated notebooks + setup scripts

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
| `parser/adf_loader.py` | Parses ADF exports, produces inventory.json |
| `parser/expression_parser.py` | Translates ADF expressions (@activity, @pipeline, @variables) |
| `translator/engine.py` | Registry dispatch, topological sort, context threading |
| `translator/activity_translators/` | One module per deterministic activity type (16 total) |
| `preparer/workflow_preparer.py` | Orchestrates activity preparers |
| `preparer/code_generator.py` | Notebook code generation for activity types |
| `preparer/activity_preparers/` | One module per activity type |
| `bundler/dab_writer.py` | Generates databricks.yml, job YAML, resources |
| `bundler/notebook_writer.py` | Writes generated notebooks to bundle |
| `bundler/setup_generator.py` | Setup scripts for UC volumes, secrets, connections |

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

## Adding a New Deterministic Translator

1. Add IR dataclass to `src/orchestra/models/ir.py`
2. Create translator at `src/orchestra/translator/activity_translators/<type>.py`
3. Create preparer at `src/orchestra/preparer/activity_preparers/<type>.py`
4. Add notebook generator to `src/orchestra/preparer/code_generator.py` if needed
5. Register in engine.py (TRANSLATOR_REGISTRY for leaf, match statement for control-flow)
6. Move from AGENTIC_TYPES to DETERMINISTIC_TYPES in adf_loader.py
7. Update activity-mapping.md reference
8. Add test fixtures and unit tests
