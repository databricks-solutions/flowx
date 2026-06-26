---
name: flowx-convert
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

# Convert ADF to Databricks IR

Convert the parsed ADF inventory into Databricks intermediate representation (IR) using deterministic translators for known types and agentic fallback for unknown types.

## Context

This is phase 2 of the flowx migration workflow. It consumes the ADF source (profiled by the `discover` skill) and produces a translation report — a transient intermediate under `<output_dir>/.work/` — that the `package` skill uses to generate Databricks Declarative Automation Bundles. It shares the single migration `<output_dir>` with the other phases.

The translation follows a **deterministic-first** strategy:
1. Activities with known, well-defined mappings are translated by built-in Python translators
2. Activities that require interpretation, expression conversion, or lack a Python translator are handled by agent skills from the `adf-to-databricks-plugin`

## How to run this skill — MCP tools or venv CLI

This phase runs one of two ways; run the **`setup`** skill first if you haven't.

- **MCP tool (Databricks Genie Code, or a local stdio registration) — the only path in Genie Code:**
  call the single **`flowx`** tool (one command per step) and run **no** `python3`/`$PY`/`bash`
  commands. The `"$PY" -m …` snippets in the steps below are the **local-CLI fallback only** — ignore
  them on this path. Map the steps to:

  ```
  flowx(command="convert", parameters={"output_dir": "<dir>", "pipeline": "<optional>"})
  # convert reuses the discovered output_dir on the server; only pass "adf_definitions" (inline ARM
  # JSON) if you are converting without a prior discover on this server.
  flowx(command="inspect", parameters={"report_path": "<dir>/.work/translation_report.json", "answers": [...]})
  flowx(command="apply_answers", parameters={"report_path": "...", "answers": ["id=value", ...], "output_dir": "<dir>", "lookup_csv": "<optional>"})
  flowx(command="merge_agentic", parameters={"report_path": "...", "agentic_results_dir": "<dir>", "output_path": "<optional>"})
  ```

  Use the tool results in place of reading the files directly. `command="merge_agentic"` covers the
  agentic `--merge-agentic` step shown later in this skill.

- **venv CLI (local, no MCP server):** ensure the venv exists (`setup` Path B / `bootstrap.sh`), then
  run the commands below with the venv interpreter (from the marker file `<plugin_dir>/.migration-venv`)
  and `src/` on `PYTHONPATH` (use `$PY` anywhere a command shows `python3`):

  ```bash
  export PYTHONPATH="<plugin_dir>/src"
  PY="$(cat <plugin_dir>/.migration-venv)"
  "$PY" -m flowx.adapter convert --output-dir <dir>
  ```

  If Python or pip is missing, `bootstrap.sh` prints a warning telling the user what to install —
  relay it and stop until they have Python 3.12+ and pip.

## Workflow

Follow these steps in order:

### Step 0 — Gather phase inputs

Run the adapter inputs subcommand so the agent surfaces the free-text
options the phase needs (inventory path, ADF source dir, output
directory):

```bash
"$PY" -m flowx.adapter inputs convert
```

The JSON response carries the prompts and defaults; collect answers from the user
(or fall back to the defaults). Keep them in conversation context — the same shared
`<output_dir>` is used by every phase.

### Step 1 — Locate the inventory

The discover phase wrote `<output_dir>/metadata/inventory.json` (and `profile_report.csv`). If the
shared `<output_dir>` is not already in conversation context, ask the user:

> Which migration output directory did the discover phase use? (default: `./flowx_output`)

Validate `<output_dir>/metadata/inventory.json` exists and is well-formed.

### Step 2 — Run deterministic translation

Execute the translation engine on all deterministic activities:

```bash
# Unified runner (recommended): `"$PY" -m flowx.adapter convert ...`
# forwards to the engine below; --adf-source-path aliases --source-dir.
"$PY" -m flowx.translator.engine \
  --source-dir <adf_source_dir> \
  --output-dir <output_dir> \
  [--pipeline <pipeline_name>]
```

Where:
- `<adf_source_dir>` is the original ADF JSON directory (the same `--source-dir` used by discover)
- `<output_dir>` is the **shared migration output directory** (default: `./flowx_output`) — the
  same one discover used
- `<pipeline_name>` (optional) — when provided, translates only the named pipeline. **Always pass `--pipeline` when the user has specified a specific pipeline to migrate**, matching the value passed to the discover phase.

The translation report and intermediate IR are written to the **transient** `<output_dir>/.work/`
folder (`translation_report.json`, per-pipeline IR, `gaps.json`). These are consumed by the steps
below and the package phase, then pruned — they are not kept artifacts.

### Step 3 — Read the translation report

Read `<output_dir>/.work/translation_report.json`. It has this structure:

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

For each translation with `"status": "pending"` and `"strategy": "agentic"`, invoke the appropriate skill from the `adf-to-databricks-plugin`. Route by activity type.

Every agentic gap in the translation report carries the activity's **full ADF/ARM JSON** under `raw_activity_json` (engine field `raw_definition`), and the generated placeholder notebook embeds the same JSON in a fenced `json` block. This holds for nested activities too — an `Until` inside an `IfCondition` / `Switch` / `ForEach` is reported as its own gap. Always translate from this ARM JSON.

**Until activities (agent-based handler):**
Databricks Lakeflow Jobs have no native repeat-until loop, so translate the `Until` from its ARM JSON into a single Python notebook task implementing a bounded polling loop. From the embedded JSON, read:
- `typeProperties.expression` — the ADF exit condition (e.g. `@or(equals(variables('jobStatus'),'succeeded'), equals(variables('jobStatus'),'failed'))`); convert it into the Python `while not (<condition>):` guard.
- `typeProperties.timeout` — wrap the loop in a wall-clock deadline (`time.monotonic()`), raising on timeout.
- `typeProperties.activities` — the loop body (e.g. a `Wait`, a polling `WebActivity`, a `SetVariable` that captures the next status); translate each child inline so the whole loop runs in one notebook.
Read the loop variables from `dbutils.widgets`, surface the final state as a task value, and write the result over the placeholder notebook's `raise NotImplementedError` cell. If the external `adf-to-databricks:adf-pipeline-converter` skill is installed you may delegate to it with the same ARM JSON; otherwise perform the translation directly.

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

Each resolved agentic gap produces one translation result. Write them into
`<output_dir>/agentic_results/` as one JSON file per activity (the filename is
arbitrary, e.g. `<pipeline>__<activity>.json`). Each file MUST use this schema:

```json
{
  "activity_name": "<the placeholder activity name, exactly as in the report>",
  "pipeline": "<pipeline name>",
  "task": {
    "type": "NotebookActivity",
    "name": "<activity name>",
    "task_key": "<task key>",
    "notebook_path": "/Workspace/.../your_translated_notebook"
  }
}
```

- `activity_name` (required) — matches the `name` of the placeholder task in the
  report (the merge locates it by name, recursing into IfCondition / ForEach /
  Switch containers, so nested gaps like an `Until` are found).
- `pipeline` (optional) — only needed to disambiguate multi-pipeline reports.
- `task` (required) — the replacement IR task. The most portable form is a
  `NotebookActivity` whose `notebook_path` points at a notebook you have written
  to the workspace; the package phase references it directly. `task_key` and
  `depends_on` are inherited from the placeholder when omitted, so dependency
  edges are preserved.

### Step 6 — Merge agentic results

Fold the results into the translation report (placeholders are replaced in place):

```bash
"$PY" -m flowx.translator.engine \
  --merge-agentic \
  --report <output_dir>/.work/translation_report.json \
  --agentic-results <agentic_results_dir>
```

Equivalently via the unified runner: `"$PY" -m flowx.adapter convert --merge-agentic --report <output_dir>/.work/translation_report.json --agentic-results <dir>`. Add `--output <path>` to write a copy instead of overwriting the report. The command exits non-zero if any result could not be matched to a placeholder.

This updates `<output_dir>/.work/translation_report.json` with the agentic results merged in, changing their status from `pending` to `translated` (or `failed` if the agentic skill could not produce a result).

### Step 6.1 — Gather just-in-time translation configuration

Run `inspect` **once** to get the full option schema, then drive the whole question chain yourself —
do **not** re-run `inspect` per follow-up:

```bash
"$PY" -m flowx.adapter inspect <output_dir>/.work/translation_report.json
```

It returns every option the report can raise, each annotated with a `show_when` condition:

```json
{"pipelines": [{"pipeline_name": "...", "options": [
  {"option_id": "notify_destination", "prompt": "...", "rationale": "...",
   "choices": [{"value": "...", "label": "...", "description": "..."}],
   "free_text": false, "default": "keep", "show_when": []},
  {"option_id": "notify_slack_url", "prompt": "...", "free_text": true, "default": "",
   "show_when": [{"option_id": "notify_destination", "in": ["slack"]}]}
]}]}
```

Walk it locally:

1. **Ask an option only when its `show_when` is satisfied** — every clause `{option_id, in:[values]}`
   must match an answer you've already collected (empty `show_when` = always ask). So `notify_slack_url`
   surfaces only after `notify_destination=slack`; the metadata-driven `access`/`size`/`lookup_tool`
   chain surfaces only after `metadata_driven_consolidate=consolidate`, etc. Present each option's
   `prompt`/`rationale` and `choices`; honor the `default`.
2. **Validate each answer** against `choices` (a `free_text` option — empty `choices` — accepts any
   value; blank skips an optional one).
3. **Perform data actions inline** when an answer calls for it — e.g. when
   `metadata_driven_lookup_tool=have`, run the lookup query with your database tool to get the rows.
4. When every applicable option is answered, apply them **in one `modify` call** (Step 6.2) with all
   answers as `--answer OPTION_ID=VALUE` flags. `modify` validates every answer server-side.

**Activity→Notify (`activity_and_notify`) motifs.** When **any** activity (Copy,
Notebook, Lookup, stored procedure, …) is followed by notification Web
activities, the adapter raises `notify_destination`:
`keep` (default) leaves the Web activities to translate directly — nothing is
collapsed. Any other value (`email`, `slack`, `teams`, `pagerduty`, `webhook`)
collapses the pattern: the upstream activity becomes the task and the
notifications become Databricks job-task `on_success`/`on_failure` notifications
routed to that destination (the ADF Web activity URL/body is not used). The schema includes **one
follow-up per Databricks-SDK field** of each destination, each gated by
`show_when: [{notify_destination, in:[<dest>]}]`; ask the chosen destination's fields (required
first) once the user picks it:

| Destination | Chained field options (SDK arg) |
|-------------|---------------------------------|
| `email`     | `notify_email_recipients` (`addresses`, comma-separated) |
| `slack`     | `notify_slack_url` (`url`), `notify_slack_channel_id` (`channel_id`, optional), `notify_slack_oauth_token` (`oauth_token`, optional) |
| `teams`     | `notify_teams_url` (`url`) |
| `pagerduty` | `notify_pagerduty_integration_key` (`integration_key`) |
| `webhook`   | `notify_webhook_url` (`url`), `notify_webhook_username` (`username`, optional), `notify_webhook_password` (`password`, optional) |

All destinations also take an optional `notify_destination_name` and
`notify_events` (both/on_failure/on_success). Optional fields left blank are
omitted so the SDK applies its defaults. For **non-email** destinations, the
`modify` phase creates (or reuses by display name) the Databricks notification
destination via the SDK **as soon as you submit the answers** — it validates the
config immediately and bakes the resolved destination id into the modified report,
so package just wires `webhook_notifications` to that id (no further SDK call).
This requires workspace auth at `modify` time; if creation fails there, the id is
left unresolved and package retries or emits a `notification_destination` setup task.
**Email** needs no destination — it uses raw `email_notifications` and is never
created via the SDK.

When the metadata-driven flow ends with `metadata_driven_lookup_tool=have`
and the agent has a database tool (Genie, MCP SQL, or a workspace SDK),
run the lookup query directly to obtain the rows as CSV. When the answer is
`none`, ask the user for a CSV file path or a literal CSV string. Pass it inline
to `modify` via `--lookup-csv` (no intermediate JSON file):

```bash
"$PY" -m flowx.adapter modify \
    <output_dir>/.work/translation_report.json \
    --output-dir <output_dir> \
    --answer metadata_driven_consolidate=consolidate \
    --answer metadata_driven_access=yes \
    --lookup-csv "<csv-file-path-or-literal-csv-string>"
```

When no metadata-driven motif is consolidated, `--lookup-csv` is omitted. In that default
(non-consolidated) case the motif becomes a Databricks **for-each task** that runs one Spark JDBC
read per source table — its iteration inputs are the resolved lookup rows when available, otherwise a
control-table lookup task seeds them at run time. (Consolidating instead emits one managed Lakeflow
Connect ingestion pipeline.)

#### Legacy flow details

Before writing the final report, surface any pipeline-modifier options the
IR raises (Copy Data paradigm, non-Databricks task compute, Lakeflow Connect
opt-in, Databricks task compute). Use the adapter CLI bridge:

```bash
"$PY" -m flowx.adapter inspect <output_dir>/.work/translation_report.json
```

The command emits JSON:

```json
{
  "pipelines": [
    {
      "pipeline_name": "ETL_Main",
      "options": [
        {
          "option_id": "copy_activity_paradigm",
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

For each option, prompt the user with the rationale, options, and the task keys it
affects. Use the default when the user defers. Then apply the collected answers as
`--answer OPTION_ID=VALUE` flags:

```bash
"$PY" -m flowx.adapter modify \
  <output_dir>/.work/translation_report.json \
  --output-dir <output_dir> \
  --answer copy_activity_paradigm=sdp \
  --answer non_databricks_task_compute=serverless \
  --answer use_lakeflow_connectors=lakeflow_connect
```

`modify` writes two things under the shared `<output_dir>`:
- `.work/translation_report.stamped.json` — the configuration-stamped IR the package phase consumes
- `metadata/configuration.json` — the collected answers, kept as the migration's configuration record

The package phase (next skill) reads the stamped report from `.work/` automatically.
When no options are raised, the inspect output is `{"pipelines": [{"pipeline_name": "...", "options": []},...]}` — skip `modify`; package falls back to the un-stamped report.

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

Generated artifacts (transient, under <output_dir>/.work/):
  - translation_report.json
  - per-pipeline IR (43 files)
  - gaps.json
```

If coverage is below 100%, explain the options for failed translations:
1. Manual notebook creation for unsupported types
2. Retry agentic translation with additional context
3. Skip the activity and add a placeholder task in the DAB

## Reference

See `references/activity-mapping.md` for the complete mapping between ADF activity types and translation strategies.

## Examples

- "Convert the ADF pipelines"
- "Convert ADF to Databricks"
- "Run the translation on the inventory from the profile step"
- "Convert the parsed pipelines using deterministic + agentic"
- "Convert only the pl_demo_01 pipeline"

## Output Artifacts

The convert phase writes only **transient** intermediates, under `<output_dir>/.work/` (consumed
by `modify`/`package`, then pruned — not kept):

| File | Description |
|---|---|
| `.work/translation_report.json` | Full translation report with IR for all activities |
| `.work/<pipeline>.json` | Per-pipeline Databricks IR |
| `.work/gaps.json` | Agentic gaps awaiting skill conversion |
| `.work/translation_report.stamped.json` | Configuration-stamped report (written by `modify`) |
