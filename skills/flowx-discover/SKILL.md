---
name: flowx-discover
description: >
  Parse a source orchestrator's pipeline definitions (Azure Data Factory, Apache Airflow) into a
  typed inventory that classifies every task as deterministic, agentic, or unsupported. Phase 1 of
  the flowx migration workflow; routes to a source-specific guide.
triggers:
  - "discover pipelines"
  - "discover ADF"
  - "discover airflow"
  - "load pipelines"
  - "parse pipelines"
  - "import pipelines"
  - "inventory source"
---

# Discover Source Pipeline Definitions

Parse a source orchestrator's definitions into a typed inventory. This is phase 1 of the flowx
migration workflow; it produces `metadata/inventory.json` (consumed by `flowx-convert`) plus a
per-pipeline complexity report at `metadata/profile_report.csv`.

flowx supports more than one **source**, and discovery is source-specific — ADF ships ARM JSON,
Airflow ships Python DAG modules, and the two share no parser. This skill routes to the right
source guide; the shared mechanics (output layout, inventory shape, how to run a phase) live here.

## Step 1 — Identify the source (required)

Ask the user which orchestrator they are migrating **from**, or infer it from the input:

- **Azure Data Factory / Fabric Data Factory** → source `adf` → read `sources/adf.md`
- **Apache Airflow** → source `airflow` → read `sources/airflow.md`

There is no default source. Every phase invocation passes `--source <name>` explicitly.

## Step 2 — Follow the source guide

Read the matching `sources/<source>.md` in this skill directory and follow it. Each guide covers
the source's input layout, the exact discover command, and how to read its inventory.

## How to run a phase — MCP tool or venv CLI

Both paths are the same across sources; only `--source` and the source path differ. Run the
**`setup`** skill first if you haven't.

- **MCP tool (Databricks Genie Code, or a local stdio registration):** call the single **`flowx`**
  tool with `command="discover"` and `parameters` including `"source": "<source>"`. Run **no**
  `python3`/`$PY` commands on this path.

- **venv CLI (local, no MCP server):** ensure the venv exists (`setup` / `bootstrap.sh`), then:

  ```bash
  export PYTHONPATH="<plugin_dir>/src"
  PY="$(cat <plugin_dir>/.migration-venv)"
  "$PY" -m flowx.adapter discover --source <source> --source-path <path> --output-dir <dir> [--pipeline <name>]
  ```

  `--source-path` is the generic flag (each source also accepts its own alias, e.g.
  `--adf-source-path`); both normalise to the phase's `--source-dir`. `--source` is required.

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
  },
  "lineage": {
    "control_edges": [
      {
        "caller_pipeline": "ETL_Main",
        "callee_pipeline": "Load_Dim_Customer",
        "activity_name": "Run Customer Load",
        "wait_on_completion": true
      }
    ],
    "data_edges": [
      {
        "dataset_name": "curated_customer",
        "identity": "abfss://curated/customer",
        "producer_pipeline": "Load_Dim_Customer",
        "producer_activity": "WriteCustomer",
        "consumer_pipeline": "Build_Sales_Mart",
        "consumer_activity": "ReadCustomer",
        "match_kind": "identity",
        "match_key": "abfss://curated/customer"
      }
    ]
  }
}
```

The `lineage` block records the cross-pipeline edges the discover phase
recovered — `control_edges` (one pipeline invokes another via ExecutePipeline;
identified by `activity_name`) and `data_edges` (one pipeline writes a dataset
another reads; identified by `match_key`). Step 5 annotates these edges, so it
depends on this block being present. When a factory has no edges of a kind, its
list is empty (`[]`).

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

### Step 5 — Author and merge agentic insights

The deterministic inventory records *what* each pipeline contains; it cannot
record *what the factory is trying to do* or *how the pipelines relate as a
system*. Author that judgment now and merge it into `inventory.json` under an
`insights` key. This always runs.

1. **Read** the just-written `inventory.json` (`pipelines`, `lineage`, `summary`)
   and `profile_report.csv`. **Then, before authoring, deep-dive the source.**
   The inventory is a deterministic skeleton (types, strategy, control edges); the
   *why* and *how* — queries, Switch conditions, notebook paths, dataset
   parameters — live only in the verbatim ARM. The `metadata/` folder holds one
   `*.arm.json` file per pipeline; each file is a **flat single-pipeline object**
   shaped `{"name": "<pipeline>", "properties": {"activities": [...], ...}}` (no
   `resources[]` array, no top-level `type`). To inspect a pipeline, **glob
   `metadata/*.arm.json` and match on each file's top-level `"name"` field** — do
   **not** construct a filename from the pipeline name (names are slugified and
   lossy, so a built path can miss or collide). The activities are under
   `properties.activities` (recurse into nested `ForEach`/`If`/`Switch` bodies).
   Read the ARM for any pipeline you write an insight or relationship about.
2. **Author** an `insights` object:
   - `overview` — the whole factory as one system, plus the single biggest
     migration steer.
   - `system_recommendation` *(optional; preferred on any multi-pipeline factory)* —
     the **one top-level architectural decision** a migrator must make **before** any
     per-pipeline work, because it *cascades* across pipelines. A per-pipeline card
     alone can't show it: e.g. "adopt a managed connector for the whole extraction
     family" turns the child extractors into connector pipelines, deletes the
     watermark store, **and** empties the fan-out orchestrator all at once. Author
     this **first**, then keep each `pipeline_insights[].recommended_patterns`
     consistent with the branch it recommends. Fields:
     - `headline` — one line naming the decision (e.g. "Managed ingestion collapses
       the extraction factory").
     - `recommended_patterns` — **1–4 whole-system branches**, ranked best-first and
       shaped exactly like a pipeline's (each with `pattern`, `fit`,
       `simplification_pattern`). `[0]` is the recommended branch;
       later entries are the ranked fallbacks. Example: `[0]` = "Adopt **Lakeflow
       Connect** for the whole SQL Server extraction family" (`simplification_pattern:
       true`); `[1]` = "For-each orchestrator + 2 collapsed parameterized jobs"
       (`simplification_pattern: false`).
     - `cascade` — what choosing `[0]` **collapses or eliminates across the system**
       (e.g. "5 child extractors → managed connector pipelines"; "version-watermark
       CSV → gone"; "fan-out orchestrator → near-empty"). This is the payoff the
       reader cannot see from any one pipeline. Omit (or `[]`) when the decision does
       not cascade.
     - `decision_driver` *(optional)* — the gating question that picks the branch
       (e.g. "Is the Lakeflow Connect SQL Server connector GA/approved for this
       source?").
     Use it whenever a **system-wide** capability (a managed connector for a whole
     source, one observability tier, one control layer) would reshape many pipelines
     at once; skip it for a single isolated pipeline.
   - `pipeline_insights[]` — a **sparse, selective** list (per entry an `intent`
     and `recommended_patterns`; optionally `pattern_name`, `databricks_pattern`,
     `conversion_notes`, `risk_if_ignored`).
     Omitting a pipeline is the default and needs no justification — a short,
     high-signal list the reader can trust beats a note on every pipeline.
     - **`risk_if_ignored`** (optional) — a one-line consequence a migrator faces
       if they port this pipeline naively (e.g. "Switch-nested calls are invisible
       in `lineage.control_edges`, so this reads as a leaf"). Use it only when the
       insight carries a genuine migration hazard; otherwise omit.
     - **`recommended_patterns` — the grounded, ranked recommendation.** A list of
       **1–4** Databricks target patterns for this pipeline, ordered **best-first**.
       Author it from a **holistic read of the whole pipeline** — its activities,
       dependencies, datasets, linked-service source types, parameters, and intent —
       **not** from a single pattern label. Each entry is an object:
       - `pattern` — the **named, publicly-documented** Databricks capability (e.g.
         `Lakeflow Connect SQL Server connector`, `Auto Loader`,
         `Lakeflow Declarative Pipelines AUTO CDC`). Name **only** capabilities that
         actually exist; **docs.databricks.com is the reference**. Never invent a name.
       - `fit` — one line: why it fits *this* pipeline / what bespoke logic it replaces.
       - `simplification_pattern` — `true` **only** when the pattern uses a *distinctive*
         Databricks capability that collapses or eliminates a whole legacy pattern:
         a managed connector (**Lakeflow Connect**), declarative CDC (**`AUTO CDC`**),
         **Auto Loader**, or **system tables** replacing a home-grown logging tier.
         Set it `false` for a like-for-like port **and** for plain native building
         blocks that merely re-home the same work — a bare parameterized **Lakeflow
         Job**, a for-each/run-job orchestrator, a plain Delta control table,
         `MERGE INTO`. "Runs on Databricks" is **not** a simplification: almost
         everything you migrate is native, so reserve this flag for the capability
         that makes the old pattern *disappear*. Rank the `true` patterns **first**.

       **Rank simplification-first:** prefer managed ingestion over a hand-rolled
       extract, declarative CDC over custom watermark logic, and collapsing clones
       over N ports — but flag `simplification_pattern: true` only on the entries that
       truly use a distinctive capability, not on the plain-orchestration fallback.
       Keep it to 1–4 (don't pad); **omit the field** when you have no grounded
       recommendation. Note GA/Preview status in `fit` when it affects the decision —
       **verify** a connector's status in the docs/release notes (e.g. the Lakeflow
       Connect SQL Server connector) rather than assuming GA.

       **Recognized-pattern vocabulary — a reference menu, NOT an allowlist.** Common
       ADF→Databricks target patterns with **current** product names. Use it to stay
       grounded and consistent, but reach past it whenever the holistic view calls for a
       better or newer fit:

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

       **Emit current names, not legacy ones:** Lakeflow Jobs (was Databricks
       Workflows), Lakeflow Declarative Pipelines (was Delta Live Tables/DLT), `AUTO CDC`
       (was `APPLY CHANGES INTO`), Declarative Automation Bundles (was Databricks Asset
       Bundles), AI/BI dashboards (was Lakeview), `system.lakeflow` (was
       `system.workflow`).
     - **`databricks_pattern`** (optional) — a one-line **headline** naming the primary
       target architecture. `recommended_patterns[0]` is the structured form of it, so
       omit `databricks_pattern` unless a short prose headline genuinely adds signal.
     - **Include a pipeline only if it meets ANY of these tests:** it anchors a
       reusable *framework* or *pattern* many others depend on (an orchestrator,
       a shared engine/wrapper, a logging/control-table hub); its classification
       or role is *surprising* given its name; or it carries a *risk or caveat* a
       migrator must know before porting it.
     - **Cover distinct roles, not a fixed count.** One representative note per
       notable role/archetype is usually enough — if forty pipelines are near-
       identical wrappers around one engine, note the engine and one representative
       wrapper, not all forty. Scale is set by how many *distinct* roles exist, not
       by pipeline count: on a large factory you will typically flag only a small
       minority. This is a guide, not a quota — include fewer if fewer qualify.
     - Rule of thumb: include a note only if it would **change a reader's decision
       or surprise a domain expert**. When in doubt, omit.
     - **Two whole-factory recommendation patterns** (apply when the evidence is
       there; use `pattern_name` to tag them, record the target as a
       `recommended_patterns` entry with `simplification_pattern: true`, and
       quantify the payoff in `intent` / `conversion_notes`. When either reshapes
       the *whole* system, also surface it as the `system_recommendation`):
       - **Replace, don't transliterate.** When a *whole tier or sub-factory*
         exists only to provide a capability Databricks offers **natively**,
         recommend eliminating it, not re-implementing it as a called job. Common
         generic mappings: a logging/observability tier → system tables
         (`system.lakeflow.*`) + native Lakeflow job notifications + an AI/BI
         dashboard; run-state / control tables → Lakeflow job & task run state and
         `dbutils.jobs.taskValues`; a config-driven Switch/template "engine" with
         no native equivalent → a Python orchestrator driving the Jobs API. **Guard
         against over-firing:** only recommend REPLACE when the tier's *sole*
         purpose is the native capability (e.g. it only logs / only records run
         state). If a pipeline does real domain work alongside the boilerplate,
         migrate it normally — do not tell the reader to delete real logic.
       - **Collapse clone families.** Cluster pipelines by their activity
         *signature* (ordered activity types) and shared child-edge set across the
         whole inventory. Where a family of near-identical pipelines exists, emit
         **one** insight (anchored on a representative pipeline that exists in the
         inventory) that names the family and its count, recommends collapsing the
         N clones into a **single parameterized job invoked N times**, and
         quantifies the win (e.g. "14 `LAAE_ingest_*` pipelines, identical
         `[IfCondition, IfCondition, ExecutePipeline×4]` signature → 1 parameterized
         job"). List the members in `conversion_notes`. This supersedes writing N
         near-duplicate per-pipeline notes.
   - `pipeline_relationships[]` — characterize **how data and control flow
     between the pipelines**, whatever the mechanism. Each relationship carries
     `from_pipeline` and `to_pipeline` (both must be pipeline names that exist in
     the inventory) plus a `lineage_edge`, `relationship_summary`,
     `databricks_pattern`, and `risk_if_ignored`.

     **A `lineage_edge` is one of two tiers. The tier is decided by ONE thing:
     whether the deterministic phase already recorded this coupling as an edge in
     `lineage` — NOT by the coupling's mechanism** (control call, dataset, table
     written in notebook code, ordering dependency, external trigger, …). Do not
     classify by mechanism.

     - **Annotation** (`edge_type` = `control` or `data`) — the coupling is
       *already* an edge in `lineage`; you are adding interpretation to it.
       `edge_identity` is copied **verbatim** from that edge: the `activity_name`
       for a `control_edges` entry, the `match_key` for a `data_edges` entry. Do
       not add `evidence` / `confidence`.
     - **Inferred** (`edge_type` = `inferred`) — a *real* coupling the
       deterministic phase did **not** record as an edge, by any mechanism. Set
       `edge_identity` to a short descriptor of what couples the two pipelines
       (e.g. the shared table/asset, or the nature of the dependency), and **you
       must** supply `evidence` (the concrete ARM observation behind it) and
       `confidence` (`high` / `medium` / `low`). Report only couplings you can
       actually evidence; do not invent them.

     **Decide in order:**
     1. Is this coupling already an edge in `lineage` (a `control_edges` /
        `data_edges` entry)? → **annotation** (`control` / `data`).
     2. Otherwise, is it a real coupling not present in `lineage`? → **inferred**.
     3. If in doubt — the coupling is real but you cannot point to the `lineage`
        edge that names it — classify it **inferred** (never annotate an edge that
        is not there).

     **Inferred covers several sub-cases — do not restrict it to any one:**
     - *Data-in-code:* one pipeline's notebook writes a table another's notebook
       reads (no ADF dataset, so `data_edges` never saw it).
     - *Ordering dependency:* a producer→consumer hand-off expressed only as
       sibling `dependsOn` inside a parent orchestrator, which the deterministic
       phase did not emit as a cross-pipeline edge.
     - *Shared control/config asset, external trigger, message queue,* or any
       other real coupling flowx could not represent.
     - *Near-miss (this is an annotation, not inferred):* pipeline A calls B via
       ExecutePipeline and that call is already a `control_edges` entry — even
       though B then does its real work in a notebook, the coupling itself was
       recorded, so annotate it.
   - **Authoring rules:** reference only pipeline names that exist in the
     inventory; an annotation edge (`control`/`data`) must echo a real lineage
     edge (annotate, don't rediscover); an `inferred` edge must carry non-empty
     `evidence` and a `confidence` level and must not be dressed up as proven
     lineage.
3. **Enrich** — merge the object in:

   - **MCP tool path** (inline dict; the only path in Genie Code):

     ```
     flowx(command="enrich", parameters={
       "output_dir": "<output dir>",
       "insights": { ...authored object... }})
     ```

   - **venv CLI fallback** (write the object to a JSON file first):

     ```bash
     "$PY" -m flowx.adapter enrich --output-dir <dir> --insights-path <file>
     ```

4. **On `ok:false`** the tool did **not** write the file: read `violations`, fix
   the offending pipeline name / lineage edge / field, and call `enrich` again.
   On `ok:true` the `insights` key is now merged into `inventory.json`.

### Step 6 — Present the summary

Display a summary table to the user:

```
ADF Discovery Summary
=====================
Pipelines parsed:     12
Total activities:     47

Strategy Breakdown:
  Deterministic:      35 (74.5%)
  Agentic:            10 (21.3%)
  Unsupported:         2 ( 4.3%)

Coverage:             95.7%
```

Then surface the authored judgment so the user sees *what the factory does*, not
just coverage numbers: print the factory `overview`, and for each
`pipeline_insights` entry its `pattern_name` / `intent` and its top
`recommended_patterns` (ranked simplification-first). Read these back from the
enriched `inventory.json`.

### Step 7 — Detail agentic activities

For activities classified as `agentic`, explain that each is translated by the agent using LLM-assisted reasoning from the activity's ARM JSON (no built-in deterministic translator exists for these types):

| Activity | Type | Handling |
|---|---|---|
| RunDataFlow | ExecuteDataFlow | Agentic (LLM-assisted) |
| BranchLogic | Switch | Agentic (LLM-assisted) |
| ... | ... | ... |

### Step 8 — Warn about unsupported activities

For activities classified as `unsupported`, warn the user clearly:

```
WARNING: The following activities have no automated translation path:
  - Pipeline "ETL_Main" / Activity "RunSSIS" (ExecuteSSISPackage)
    Recommendation: Manual conversion to PySpark notebook required.
```

### Step 9 — Confirm output location

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

## Future considerations

Insights are currently authored in a single pass over the whole factory. For very
large factories, revisit partitioning the authoring across subagents keyed on
lineage clusters (the connected components of the combined control/data-edge
graph), so each subagent reasons about one coherent subsystem. Out of scope for
now — always enrich in one pass.
