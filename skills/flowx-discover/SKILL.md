---
name: flowx-discover
description: >
  Load and parse Azure Data Factory pipeline definitions from Unity Catalog volumes or local directories.
  Produces a typed inventory that classifies every activity as deterministic, agentic, or unsupported.
triggers:
  - "discover ADF"
  - "load ADF"
  - "parse ADF"
  - "import pipelines"
  - "load pipelines"
  - "parse pipelines"
  - "inventory ADF"
---

# Discover ADF Pipeline Definitions

Parse Azure Data Factory pipeline, dataset, linked service, and trigger JSON files into a typed AST and produce a classified inventory.

## Context

This is phase 1 of the flowx migration workflow. It takes raw ADF JSON exports and produces an `inventory.json` file that the `convert` skill consumes. The inventory classifies every ADF activity into one of three strategies:

- **Deterministic** — a built-in translator exists (Copy, DatabricksNotebook, ForEach, IfCondition, etc.)
- **Agentic** — requires agentic (LLM-assisted) translation by the agent (ExecuteDataFlow, Switch, Until, StoredProc, etc.)
- **Unsupported** — no known translation path; requires manual intervention

## How to run this skill — MCP tool or venv CLI

This phase runs one of two ways; run the **`setup`** skill first if you haven't.

- **MCP tool (Databricks Genie Code, or a local stdio registration) — the only path in Genie Code:**
  call the single **`flowx`** tool with `command="discover"` and run **no** `python3`/`$PY`/`bash`
  commands. The `"$PY" -m …` snippets in the steps below are the **local-CLI fallback only** — ignore
  them on this path.

  The hosted server **cannot read your workspace/volume files**, so pass the ADF JSON **inline** as
  `adf_definitions` — a mapping of relative path → JSON content mirroring the ADF Git-export layout.
  You (the agent) read the ARM JSON files from the source and supply them:

  ```
  flowx(command="discover", parameters={
    "adf_definitions": {
      "pipeline/Foo.json": { ...ARM JSON... },
      "dataset/Bar.json": { ... },
      "linkedService/Baz.json": { ... },
      "trigger/Qux.json": { ... }
    },
    "output_dir": "<output dir>", "pipeline": "<optional>"})
  ```

  For **large factories** (hundreds–thousands of pipelines), don't inline — reference the source
  instead (inline `adf_definitions` is capped at ~5 MB): pass `"adf_volume_path":
  "/Volumes/cat/sch/adf_export"` for a UC Volume (read via the SDK Files API) or
  `"adf_workspace_path": "/Workspace/Shared/adf_export"` for an ADF Git folder in the workspace (read
  via the SDK Workspace API). Locally, where the server can read
  the path, you may instead pass `adf_source_path`. The tool returns the inventory summary
  (pipeline/activity counts by strategy and coverage); use it in place of reading the files directly.

- **venv CLI (local, no MCP server):** ensure the venv exists (`setup` Path B / `bootstrap.sh`), then
  run the commands below with the venv interpreter and `src/` on `PYTHONPATH`. The interpreter path is
  in the marker file `<plugin_dir>/.migration-venv` (use `$PY` anywhere a command shows `python3`):

  ```bash
  export PYTHONPATH="<plugin_dir>/src"
  PY="$(cat <plugin_dir>/.migration-venv)"
  "$PY" -m flowx.adapter discover --adf-source-path <path> --output-dir <dir>
  ```

  If Python or pip is missing, `bootstrap.sh` prints a warning telling the user what to install —
  relay it and stop until they have Python 3.12+ and pip.

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

Run the discover phase via the adapter's unified phase runner (recommended):

```bash
"$PY" -m flowx.adapter discover \
  --adf-source-path <source_path> \
  --output-dir <output_dir> \
  [--pipeline <pipeline_name>]
```

`--adf-source-path` is accepted as an alias of `--source-dir` (it matches the
`adf_source_path` input option). This forwards to, and is equivalent to, running
the loader directly:

```bash
"$PY" -m flowx.parser.adf_loader \
  --source-dir <source_path> --output-dir <output_dir> [--pipeline <pipeline_name>]
```

Where:
- `<plugin_dir>` is the root of the flowx plugin (the directory containing `src/`)
- `<source_path>` is the local directory containing ADF JSON files
- `<output_dir>` is the **single shared migration output directory** used by all three phases
  (default: `./flowx_output`). Discover writes its artifacts into the `metadata/` subfolder.
- `<pipeline_name>` (optional) — when provided, filters to only the named pipeline. When omitted, all pipelines in the source directory are included.

**Always pass `--pipeline` when the user has specified a specific pipeline to migrate.** This ensures the inventory and all downstream phases are scoped to only that pipeline.

This produces, under `<output_dir>/metadata/`:
- `inventory.json` — the classified activity inventory
- `profile_report.csv` — one row per pipeline with a complexity assessment (see Step 4b)
- `<pipeline>.arm.json` — the verbatim original ADF/ARM source for each pipeline (provenance)

### Step 4 — Read and validate the inventory

Read the generated `<output_dir>/metadata/inventory.json` file. It has this structure:

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
          "strategy": "agentic"
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

### Step 4b — Review the complexity report

`<output_dir>/metadata/profile_report.csv` carries one row per pipeline with a migration-complexity
assessment. Columns:

| Column | Meaning |
|---|---|
| `pipeline` | Pipeline name |
| `activities` | Total activities (including nested ForEach/If/Switch children) |
| `datasets` | Distinct datasets the pipeline references |
| `linked_services` | Distinct linked services (activity-level + via referenced datasets) |
| `collapsible_patterns` | Number of motif patterns detected (auto-collapsible during convert) |
| `databricks_native_activities` | Notebook / SparkJar / SparkPython / Job activities (simplest) |
| `control_flow_activities` | ForEach / If / Switch / SetVariable / AppendVariable / Filter / Wait / Until |
| `other_activities` | Everything else — Copy, Web, Lookup, agentic types (hardest) |
| `complexity_score` | Weighted score: native×1 + control×2 + other×3 + datasets + linked_services + collapsible_patterns |
| `complexity_size` | T-shirt size from the score: **S** ≤5, **M** ≤15, **L** ≤30, **XL** >30 |

Use it to set expectations: S/M pipelines are largely deterministic; L/XL pipelines (many "other"
activities, datasets, or linked services) warrant closer review and more agentic translation.

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

For activities classified as `agentic`, explain that each is translated by the agent using LLM-assisted reasoning from the activity's ARM JSON (no built-in deterministic translator exists for these types):

| Activity | Type | Handling |
|---|---|---|
| RunDataFlow | ExecuteDataFlow | Agentic (LLM-assisted) |
| BranchLogic | Switch | Agentic (LLM-assisted) |
| ... | ... | ... |

### Step 7 — Warn about unsupported activities

For activities classified as `unsupported`, warn the user clearly:

```
WARNING: The following activities have no automated translation path:
  - Pipeline "ETL_Main" / Activity "RunSSIS" (ExecuteSSISPackage)
    Recommendation: Manual conversion to PySpark notebook required.
```

### Step 8 — Confirm output location

Tell the user where the metadata files were written (`<output_dir>/metadata/`: inventory.json, profile_report.csv, and the per-pipeline `.arm.json`), summarise the complexity sizes, and confirm they can proceed to the `convert` phase using the same `<output_dir>`.

## Examples

- "Discover my ADF pipelines from /Volumes/main/default/adf_export"
- "Parse ADF definitions from ./tests/resources/json/"
- "Load the ADF pipeline JSON files and show me the inventory"
- "Import pipelines from /tmp/customer_adf_export"
- "Discover only the pl_demo_01 pipeline from /Volumes/main/default/adf_export"

## Output Artifacts

All under the shared `<output_dir>/metadata/` folder:

| File | Description |
|---|---|
| `metadata/inventory.json` | Classified activity inventory for the convert phase |
| `metadata/profile_report.csv` | Per-pipeline complexity report (counts + T-shirt size) |
| `metadata/<pipeline>.arm.json` | Verbatim original ADF/ARM source for each pipeline |
