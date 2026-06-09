---
name: ingest
description: >
  Load and parse Azure Data Factory pipeline definitions from Unity Catalog volumes or local directories.
  Produces a typed inventory that classifies every activity as deterministic, agentic, or unsupported.
triggers:
  - "ingest ADF"
  - "load ADF"
  - "parse ADF"
  - "import pipelines"
  - "load pipelines"
  - "parse pipelines"
  - "inventory ADF"
---

# Ingest ADF Pipeline Definitions

Parse Azure Data Factory pipeline, dataset, linked service, and trigger JSON files into a typed AST and produce a classified inventory.

## Context

This is phase 1 of the orchestra migration workflow. It takes raw ADF JSON exports and produces an `inventory.json` file that the `translate` skill consumes. The inventory classifies every ADF activity into one of three strategies:

- **Deterministic** — a built-in translator exists (Copy, DatabricksNotebook, ForEach, IfCondition, etc.)
- **Agentic** — requires LLM-assisted translation via the `adf-to-databricks-plugin` skills (ExecuteDataFlow, Switch, Until, StoredProc, etc.)
- **Unsupported** — no known translation path; requires manual intervention

## Prerequisite — Python environment

This skill runs the plugin's Python code, which depends on third-party packages. Before running
any Python commands, ensure the plugin's virtual environment is bootstrapped. Run the
**`setup`** skill, or directly:

```bash
bash <plugin_dir>/scripts/bootstrap.sh
```

This creates `<plugin_dir>/.venv` and installs dependencies from `requirements.txt`. If Python or 
pip is missing, the script prints a warning telling the user what to install — relay it and stop
until they have installed Python 3.12+ and pip.

Run **every** Python command in this skill with the venv interpreter and `src/` on `PYTHONPATH` 
(use it anywhere a command below shows `python3`):

```bash
export PYTHONPATH="<plugin_dir>/src"
"<plugin_dir>/.venv/bin/python" <plugin_dir>/src/orchestra/parser/adf_loader.py ...
```

## Workflow

Follow these steps in order:

### Step 1 — Determine the ADF source path

Ask the user for the location of their ADF JSON exports. Accept either:
- A Unity Catalog volume path (e.g., `/Volumes/main/default/adf_export`)
- A local directory path (e.g., `./adf_export/` or `/tmp/adf_json/`)

The directory should contain subdirectories or files for:
- `pipeline/` or `pipelines/` — pipeline definition JSON files
- `dataset/` or `datasets/` — dataset definition JSON files (optional)
- `linkedService/` or `linked_services/` — linked service JSON files (optional)
- `trigger/` or `triggers/` — trigger definition JSON files (optional)

### Step 2 — Download from UC volumes if needed

If the source path starts with `/Volumes/`, the files live in a Unity Catalog volume and must be downloaded to a local temp directory first.

Use the `databricks-execution-compute` skill to run the following on the Databricks workspace:

```python
import os, json, shutil, tempfile

volume_path = "<user_provided_volume_path>"
local_dir = tempfile.mkdtemp(prefix="adf_ingest_")

# Copy from volume to local
for root, dirs, files in os.walk(volume_path):
    for f in files:
        if f.endswith(".json"):
            src = os.path.join(root, f)
            rel = os.path.relpath(src, volume_path)
            dst = os.path.join(local_dir, rel)
            os.makedirs(os.path.dirname(dst), exist_ok=True)
            shutil.copy2(src, dst)

print(f"Downloaded ADF files to: {local_dir}")
```

Alternatively, use the Databricks CLI:
```bash
databricks fs cp -r "dbfs:<volume_path>" "<local_temp_dir>" --overwrite
```

Set the working source directory to the local temp path for subsequent steps.

### Step 3 — Run the deterministic parser

Execute the ADF loader to parse all JSON files and produce the inventory:

```bash
python3 <plugin_dir>/src/orchestra/parser/adf_loader.py \
  --source-dir <source_path> \
  --output-dir <output_path>
```

Where:
- `<plugin_dir>` is the root of the orchestra plugin (the directory containing `src/`)
- `<source_path>` is the local directory containing ADF JSON files
- `<output_path>` is where to write the parsed output (default: `./orchestra_output/ingest/`)

This produces:
- `inventory.json` — the classified activity inventory
- `ast/` directory — the typed AST for each pipeline
- `parse_errors.json` — any files that failed to parse

### Step 4 — Read and validate the inventory

Read the generated `inventory.json` file. It has this structure:

```json
{
  "source_dir": "/path/to/adf/json",
  "generated_at": "2026-04-07T12:00:00Z",
  "pipelines": [
    {
      "name": "PipelineName",
      "file": "pipeline/PipelineName.json",
      "activities": [
        {
          "name": "CopyFromBlob",
          "type": "Copy",
          "strategy": "deterministic",
          "translator": "copy.py"
        },
        {
          "name": "RunDataFlow",
          "type": "ExecuteDataFlow",
          "strategy": "agentic",
          "skill": "adf-to-databricks:adf-dataflow-converter"
        }
      ]
    }
  ],
  "summary": {
    "pipeline_count": 12,
    "activity_count": 47,
    "deterministic_count": 35,
    "agentic_count": 10,
    "unsupported_count": 2,
    "coverage_pct": 95.7
  }
}
```

### Step 5 — Present the summary

Display a summary table to the user:

```
ADF Ingestion Summary
=====================
Pipelines parsed:     12
Total activities:     47

Strategy Breakdown:
  Deterministic:      35 (74.5%)
  Agentic:            10 (21.3%)
  Unsupported:         2 ( 4.3%)

Coverage:             95.7%
```

### Step 6 — Detail agentic activities

For activities classified as `agentic`, explain which skill from the `adf-to-databricks-plugin` will handle each:

| Activity | Type | Handling Skill |
|---|---|---|
| RunDataFlow | ExecuteDataFlow | `adf-to-databricks:adf-dataflow-converter` |
| BranchLogic | Switch | `adf-to-databricks:adf-pipeline-converter` |
| ... | ... | ... |

### Step 7 — Warn about unsupported activities

For activities classified as `unsupported`, warn the user clearly:

```
WARNING: The following activities have no automated translation path:
  - Pipeline "ETL_Main" / Activity "RunSSIS" (ExecuteSSISPackage)
    Recommendation: Manual conversion to PySpark notebook required.
```

### Step 8 — Confirm output location

Tell the user where the inventory and AST files were written, and confirm they can proceed to the `translate` phase.

## Examples

- "Ingest my ADF pipelines from /Volumes/main/default/adf_export"
- "Parse ADF definitions from ./tests/resources/json/"
- "Load the ADF pipeline JSON files and show me the inventory"
- "Import pipelines from /tmp/customer_adf_export"

## Output Artifacts

| File | Description |
|---|---|
| `inventory.json` | Classified activity inventory for the translate phase |
| `ast/*.json` | Typed AST for each pipeline |
| `parse_errors.json` | Any files that failed to parse |
