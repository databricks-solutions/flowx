---
name: translate
description: >
  Translate parsed ADF pipeline AST into Databricks IR (intermediate representation).
  Runs deterministic translators for known activity types, then invokes agentic skills
  from adf-to-databricks-plugin for gaps.
triggers:
  - "translate ADF"
  - "convert ADF"
  - "translate pipelines"
  - "convert pipelines"
  - "run translation"
---

# Translate ADF to Databricks IR

Convert the parsed ADF inventory into Databricks intermediate representation (IR) using deterministic translators for known types and agentic fallback for unknown types.

## Context

This is phase 2 of the orchestra migration workflow. It consumes the `inventory.json` produced by the `ingest` skill and produces a `translation_report.json` that the `prepare` skill uses to generate Databricks Declarative Automation Bundles.

The translation follows a **deterministic-first** strategy:
1. Activities with known, well-defined mappings are translated by built-in Python translators
2. Activities that require interpretation, expression conversion, or lack a Python translator are handled by agent skills from the `adf-to-databricks-plugin`

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

### Step 0 — Gather phase inputs

Run the adapter inputs subcommand so the agent surfaces the free-text
questions the phase needs (inventory path, ADF source dir, output
directory):

```bash
python3 -m orchestra.adapter inputs translate
```

The JSON response carries the prompts and defaults; collect answers
from the user (or fall back to the defaults) and persist them to
`<output_dir>/translate/inputs.json` so later steps and subsequent
phases can read the same values.

### Step 1 — Locate the inventory

Read `inventory.json` from the ingest phase. If the path is not already in conversation context, ask the user:

> Where is the inventory.json from the ingest phase? (default: `./orchestra_output/ingest/inventory.json`)

Validate the file exists and is well-formed.

### Step 2 — Run deterministic translation

Execute the translation engine on all deterministic activities:

```bash
python3 <plugin_dir>/src/orchestra/translator/engine.py \
  --inventory <inventory_path> \
  --source-dir <adf_source_dir> \
  --output-dir <output_dir>
```

Where:
- `<plugin_dir>` is the root of the orchestra plugin
- `<inventory_path>` is the path to `inventory.json`
- `<adf_source_dir>` is the original ADF JSON directory (from the ingest phase)
- `<output_dir>` is the translation output path (default: `./orchestra_output/translate/`)

This produces:
- `translation_report.json` — results for deterministic activities + placeholders for agentic gaps
- `ir/` directory — Databricks IR for each translated activity
- `notebooks/` directory — generated helper notebooks

### Step 3 — Read the translation report

Read `translation_report.json`. It has this structure:

```json
{
  "inventory_path": "/path/to/inventory.json",
  "generated_at": "2026-04-07T12:30:00Z",
  "translations": [
    {
      "pipeline": "ETL_Main",
      "activity": "CopyFromBlob",
      "type": "Copy",
      "strategy": "deterministic",
      "status": "translated",
      "ir": {
        "task_key": "copy_from_blob",
        "task_type": "notebook_task",
        "notebook_path": "notebooks/copy_from_blob.py",
        "parameters": { "source": "abfss://...", "target": "..." }
      }
    },
    {
      "pipeline": "ETL_Main",
      "activity": "TransformData",
      "type": "ExecuteDataFlow",
      "strategy": "agentic",
      "status": "pending",
      "raw_activity_json": { "...": "..." },
      "target_skill": "adf-to-databricks:adf-dataflow-converter"
    }
  ],
  "summary": {
    "total": 47,
    "deterministic_translated": 35,
    "agentic_pending": 10,
    "failed": 2
  }
}
```

### Step 4 — Handle agentic gaps

For each translation with `"status": "pending"` and `"strategy": "agentic"`, invoke the appropriate skill from the `adf-to-databricks-plugin`. Route by activity type:

**ExecuteDataFlow activities:**
Invoke `adf-to-databricks:adf-dataflow-converter` with the raw activity JSON and associated data flow definition. Provide context:
- The raw `typeProperties` from the ADF activity
- The data flow JSON definition (if available in the source directory under `dataflow/`)
- The linked service configurations for source/sink connections
- Target catalog and schema for the SDP pipeline or PySpark notebook output

**Control flow activities (Switch, Until, Wait, Filter, AppendVariable):**
Invoke `adf-to-databricks:adf-pipeline-converter` with the raw activity JSON. Provide context:
- The full pipeline JSON containing the activity
- Any nested activities within the control flow
- Variable definitions from the pipeline
- The desired Databricks task type mapping

**Stored procedures and external calls (SqlServerStoredProcedure, AzureFunction, WebHook, Custom):**
Invoke `adf-to-databricks:adf-pipeline-converter` with the raw activity JSON. Provide context:
- The linked service configuration for the target system
- Connection details and authentication method
- Any parameters or request bodies

**Complex expressions:**
If any activity (deterministic or agentic) contains ADF expressions that the deterministic translator could not resolve, invoke `adf-to-databricks:adf-expression-translator` with:
- The raw expression string (e.g., `@pipeline().parameters.inputPath`)
- The expression context (pipeline parameters, variables, activity outputs)
- The target format (Python f-string, Spark SQL, task parameter reference)

**Trigger definitions:**
Invoke `adf-to-databricks:adf-trigger-converter` with:
- The trigger JSON definition
- The associated pipeline references
- Target: Databricks job schedule configuration (quartz_cron_expression, periodic, or file_arrival)

### Step 5 — Collect agentic results

Each agentic skill invocation produces a translation result. Collect all results into `<output_dir>/agentic_results/`:
- Save each result as `<pipeline>__<activity>.json`
- Include the generated IR, any notebooks, and metadata

### Step 6 — Merge agentic results

Run the merge step to combine deterministic and agentic translations:

```bash
python3 <plugin_dir>/src/orchestra/translator/engine.py \
  --merge-agentic \
  --report <translation_report_path> \
  --agentic-results <agentic_results_dir>
```

This updates `translation_report.json` with the agentic results merged in, changing their status from `pending` to `translated` (or `failed` if the agentic skill could not produce a result).

### Step 6.1 — Gather just-in-time translation preferences

The adapter raises several preference questions plus a chained set for
metadata-driven motifs. Every time the user answers a question whose value 
gates further prompts, re-run `inspect --answers <answers.json>` to surface 
the next batch.

When the metadata-driven flow ends with `metadata_driven_lookup_tool=have`
and the agent has a database tool (Genie, MCP SQL, or a workspace SDK),
run the lookup query directly and write the rows to
`<output_dir>/lookup_values.json`. When the answer is `none`, prompt
the user for a CSV file or comma-separated string and call:

```bash
python3 -m orchestra.adapter materialize-lookup "<csv-or-path>" \
    --out <output_dir>/lookup_values.json
```

Then call `modify` with the lookup values:

```bash
python3 -m orchestra.adapter modify \
    <translation_report_path> \
    <output_dir>/answers.json \
    --lookup-values <output_dir>/lookup_values.json \
    --out <output_dir>/translation_report.stamped.json
```

When no metadata-driven motif is consolidated, `--lookup-values` is omitted.

#### Legacy flow details

Before writing the final report, surface any pipeline-modifier questions the
IR raises (Copy Data paradigm, non-Databricks task compute, Lakeflow Connect
opt-in, Databricks task compute). Use the adapter CLI bridge:

```bash
python3 -m orchestra.adapter inspect <translation_report_path>
```

The command emits JSON:

```json
{
  "pipelines": [
    {
      "pipeline_name": "ETL_Main",
      "questions": [
        {
          "question_id": "copy_activity_paradigm",
          "prompt": "How should Copy Data activities targeting Delta be implemented?",
          "rationale": "...",
          "options": [{"value": "notebook", "label": "...", "description": "..."}, ...],
          "affected_task_keys": ["copy_orders", "copy_customers"],
          "default": "notebook"
        },
        ...
      ]
    }
  ]
}
```

For each question, prompt the user with the rationale, options, and the
task keys it affects. Use the default when the user defers. Collect the
answers into a JSON file (`<output_dir>/answers.json`) shaped like:

```json
{
  "copy_activity_paradigm": "sdp",
  "non_databricks_task_compute": "serverless",
  "use_lakeflow_connectors": "lakeflow_connect"
}
```

Then apply the answers to produce a stamped report the prepare phase consumes:

```bash
python3 -m orchestra.adapter modify \
  <translation_report_path> \
  <output_dir>/answers.json \
  --out <output_dir>/translation_report.stamped.json
```

The prepare phase (next skill) must be pointed at the stamped report.
When no questions are raised, the inspect output is `{"pipelines": [{"pipeline_name": "...", "questions": []}, ...]}` — skip the modify step and pass the original report straight through.

### Step 7 — Present translation summary

Display a summary to the user:

```
Translation Summary
===================
Total activities:           47
Deterministic translated:   35 (74.5%)
Agentic translated:          8 (17.0%)
Failed:                      4 ( 8.5%)

Overall coverage:           91.5%

Failed translations:
  - ETL_Main / RunSSIS (ExecuteSSISPackage) — no translator available
  - ETL_Main / CustomTask (Custom) — agentic skill returned error
  ...

Generated artifacts:
  - translation_report.json
  - ir/ (43 files)
  - notebooks/ (12 files)
```

If coverage is below 100%, explain the options for failed translations:
1. Manual notebook creation for unsupported types
2. Retry agentic translation with additional context
3. Skip the activity and add a placeholder task in the DAB

## Reference

See `references/activity-mapping.md` for the complete mapping between ADF activity types and translation strategies.

## Examples

- "Translate the ADF pipelines"
- "Convert ADF to Databricks"
- "Run the translation on the inventory from the ingest step"
- "Translate the parsed pipelines using deterministic + agentic"

## Output Artifacts

| File | Description |
|---|---|
| `translation_report.json` | Full translation report with IR for all activities |
| `ir/*.json` | Databricks IR for each translated activity |
| `notebooks/*.py` | Generated helper notebooks |
| `agentic_results/*.json` | Raw results from agentic skill invocations |
