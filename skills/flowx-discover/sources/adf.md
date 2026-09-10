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

Then, after the shared insights step has enriched `inventory.json`, surface the authored judgment so
the user sees *what the factory does*, not just coverage numbers: print the factory `overview`, and
for each `pipeline_insights` entry its `pattern_name` / `intent` and its top `recommended_patterns`
(ranked simplification-first).

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

## Insights — deep-dive & pattern vocabulary

Reference for the shared agentic-insights step (parent `SKILL.md` Step 5, "Author and merge agentic
insights"). Do this deep-dive before authoring insights for any ADF pipeline.

**Deep-dive the ARM.** The inventory is a deterministic skeleton (types, strategy, control edges);
the *why* and *how* — queries, Switch conditions, notebook paths, dataset parameters — live only in
the verbatim ARM. The `metadata/` folder holds one `*.arm.json` file per pipeline; each is a **flat
single-pipeline object** shaped `{"name": "<pipeline>", "properties": {"activities": [...], ...}}`
(no `resources[]` array, no top-level `type`). To inspect a pipeline, **glob `metadata/*.arm.json`
and match on each file's top-level `"name"` field** — do **not** construct a filename from the
pipeline name (names are slugified and lossy, so a built path can miss or collide). Activities are
under `properties.activities` (recurse into nested `ForEach`/`If`/`Switch` bodies). Read the ARM for
any pipeline you write an insight or relationship about.

**ADF constructs → Databricks** — a reference menu, NOT an allowlist; the target side uses current
product names, so reach past it whenever a better or newer fit exists. Flag `simplification_pattern:
true` only on entries that use a distinctive capability, never on the plain-orchestration fallback.

| Pipeline does… | Simplifying target — `simplification_pattern: true` (rank first) | Fallback — `false` |
|---|---|---|
| Extract/Copy from a database (SQL Server, …) | **Lakeflow Connect** managed connector (change-tracking/CDC → Delta) | Auto Loader / JDBC read + `MERGE INTO` |
| Incremental load via watermark | **Lakeflow Declarative Pipelines `AUTO CDC`** | Delta `MERGE INTO` + control table / `dbutils.jobs.taskValues` |
| CDC / SQL Server change tracking | **Lakeflow Connect** or **`AUTO CDC`** | Structured Streaming over the change feed |
| Land + process files | **Auto Loader** (`cloudFiles`, file-notification mode) | — |
| Metadata-driven bulk copy (Lookup→ForEach→Copy) | **Lakeflow Connect** (multi-table) or a parameterized **Lakeflow Jobs** for-each task | — |
| Parent/child `ExecutePipeline` fan-out | **Lakeflow Jobs** for-each task + run-job task + job parameters | — |
| SCD Type 2 (data flow) | **Lakeflow Declarative Pipelines `AUTO CDC`** (SCD Type 2) | — |
| Staged load + stored-proc transform | Spark write to **Delta** + post-load step | — |
| REST API pagination | Python ingestion notebook (requests-based) | Lakeflow Connect SaaS connector if one fits |
| Custom logging / observability tier | **system tables (`system.lakeflow.*`) + native job notifications + AI/BI dashboard** | — |
| Run-state / control tables | Lakeflow job & task run state + `dbutils.jobs.taskValues` | — |
| Clone family (many near-identical pipelines) | one **parameterized Lakeflow Job** invoked N times | — |

**Emit current names, not legacy ones:** Lakeflow Jobs (was Databricks Workflows), Lakeflow
Declarative Pipelines (was Delta Live Tables/DLT), `AUTO CDC` (was `APPLY CHANGES INTO`), Declarative
Automation Bundles (was Databricks Asset Bundles), AI/BI dashboards (was Lakeview), `system.lakeflow`
(was `system.workflow`).

**Then author the insights (shared method).** With this deep-dive and pattern vocabulary in hand,
author and merge the `insights` object by following the source-neutral "Author and merge agentic
insights" step in the parent `SKILL.md`. The insight schema and the authoring method are shared
across sources; only the deep-dive and the construct mappings above are ADF-specific.
