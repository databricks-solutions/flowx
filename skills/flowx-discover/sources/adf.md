# Discover — Azure Data Factory

Source guide for `--source adf`. Parse Azure Data Factory pipeline, dataset, linked service, and
trigger JSON files into a typed AST and produce a classified inventory. See the parent `SKILL.md`
for the shared output layout, inventory shape, and how to run a phase.

## Step 1 — Determine the ADF source path

Ask the user for the location of their ADF JSON exports. Accept either:
- A Unity Catalog volume path (e.g. `/Volumes/main/default/adf_export`)
- A local directory path (e.g. `./adf_export/`)

The directory should contain subdirectories or files for:
- `pipeline/` or `pipelines/` — pipeline definition JSON files
- `dataset/` or `datasets/` — dataset definition JSON files (optional)
- `linkedService/` or `linked_services/` — linked service JSON files (optional)
- `trigger/` or `triggers/` — trigger definition JSON files (optional)

On the MCP path the hosted server cannot read your workspace/volume files, so pass the ADF JSON
inline as `adf_definitions` (a mapping of relative path → JSON content), or for large factories
reference the source via `adf_volume_path` / `adf_workspace_path`.

## Step 2 — Download from UC volumes if needed

If the source path starts with `/Volumes/`, copy the files to a local temp directory first (e.g. via
the `databricks-execution-compute` skill or `databricks fs cp -r`), then point discover at the local
path.

## Step 3 — Run the deterministic parser

```bash
"$PY" -m flowx.adapter discover --source adf \
  --adf-source-path <source_path> \
  --output-dir <output_dir> \
  [--pipeline <pipeline_name>]
```

`--adf-source-path` is the ADF alias of `--source-path`; both normalise to `--source-dir`. Always
pass `--pipeline` when the user specified a single pipeline to migrate, so all downstream phases are
scoped to it.

## Step 4 — Read and validate the inventory

Read `<output_dir>/metadata/inventory.json`:

```json
{
  "source": "adf",
  "source_dir": "/path/to/adf/json",
  "pipelines": [
    {
      "name": "PipelineName",
      "activities": [
        {"name": "CopyFromBlob", "type": "Copy", "strategy": "deterministic", "translator": "copy.py"},
        {"name": "RunDataFlow", "type": "ExecuteDataFlow", "strategy": "agentic"}
      ]
    }
  ],
  "summary": {"pipeline_count": 12, "activity_count": 47, "deterministic_count": 35,
              "agentic_count": 10, "unsupported_count": 2, "coverage_pct": 95.7}
}
```

## Step 4b — Review the complexity report

`<output_dir>/metadata/profile_report.csv` has one row per pipeline: `pipeline`, `activities`,
`datasets`, `linked_services`, `collapsible_patterns`, `databricks_native_activities`,
`control_flow_activities`, `other_activities`, `complexity_score`, `complexity_size` (S ≤5, M ≤15,
L ≤30, XL >30). Use it to set expectations: S/M are largely deterministic; L/XL warrant closer
review and more agentic translation.

## Step 5 — Present the summary

```
ADF Profile Summary
===================
Pipelines parsed:     12
Total activities:     47
Strategy Breakdown:
  Deterministic:      35 (74.5%)
  Agentic:            10 (21.3%)
  Unsupported:         2 ( 4.3%)
Coverage:             95.7%
```

## Step 6 — Detail agentic activities

For `agentic` activities, explain that each is translated by the agent using LLM-assisted reasoning
from the activity's ARM JSON (no built-in deterministic translator exists), e.g. `ExecuteDataFlow`,
`Switch`, `Until`, stored procedures.

## Step 7 — Warn about unsupported activities

For `unsupported` activities, warn clearly, e.g. `ExecuteSSISPackage` — recommend manual conversion
to a PySpark notebook.

## Step 8 — Confirm output location

Tell the user where the metadata files were written (`<output_dir>/metadata/`), summarise the
complexity sizes, and confirm they can proceed to `flowx-convert` with the same `<output_dir>`.
