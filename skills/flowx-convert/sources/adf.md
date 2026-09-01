# Convert — Azure Data Factory

Source guide for `--source adf`. Translate the discovered ADF inventory into Databricks IR using
deterministic translators for known activity types and agentic (LLM-assisted) fallback for the
rest. See the parent `SKILL.md` for how to run the phase, the report contract, and the shared
`inspect`/`modify`/`merge_agentic` commands.

Translation is **deterministic-first**:
1. Activities with known mappings are translated by built-in Python translators.
2. Activities needing interpretation, expression conversion, or lacking a translator are handled by
   agentic translation the agent performs from the ARM JSON.

## Step 1 — Locate the inventory

The discover phase wrote `<output_dir>/metadata/inventory.json`. Confirm the shared `<output_dir>`
(default `./flowx_output`) and that the inventory exists.

## Step 2 — Run deterministic translation

```bash
"$PY" -m flowx.adapter convert --source adf \
  --adf-source-path <adf_source_dir> \
  --output-dir <output_dir> \
  [--pipeline <pipeline_name>]
```

`<adf_source_dir>` is the same ADF JSON directory discover used. Always pass `--pipeline` when the
user scoped to a single pipeline. The report and intermediate IR are written to the transient
`<output_dir>/.work/` folder (`translation_report.json`, per-pipeline IR, `gaps.json`).

## Step 3 — Read the translation report

Read `<output_dir>/.work/translation_report.json`. Each translation entry carries `pipeline`,
`activity`, `type`, `strategy` (`deterministic`/`agentic`), `status`, and either the translated
`ir` or a `raw_activity_json` for pending agentic gaps.

## Step 4 — Handle agentic gaps

For each translation with `"status": "pending"` and `"strategy": "agentic"`, perform LLM-assisted
translation from the activity's **full ADF/ARM JSON** (under `raw_activity_json`; also embedded in
the placeholder notebook's fenced `json` block). Nested gaps (an `Until` inside `IfCondition` /
`Switch` / `ForEach`) are reported individually.

**Until activities:** Lakeflow Jobs have no native repeat-until, so translate the `Until` into one
Python notebook task implementing a bounded polling loop — convert `typeProperties.expression` into
the `while not (<condition>):` guard, wrap in a `time.monotonic()` deadline from
`typeProperties.timeout`, and translate `typeProperties.activities` (the loop body) inline. Read
loop variables from `dbutils.widgets`, surface final state as a task value.

**ExecuteDataFlow:** translate from the raw `typeProperties` + the `dataflow/` JSON definition +
linked-service source/sink connections, into an SDP pipeline or PySpark notebook.

**Control flow (Switch, Until, Wait, Filter, AppendVariable):** translate from the raw activity
JSON, the containing pipeline JSON, nested activities, and variable definitions.

**Stored procedures / external calls (SqlServerStoredProcedure, AzureFunction, WebHook, Custom):**
translate from the raw JSON + the linked-service configuration + connection/auth details.

**Complex expressions:** translate unresolved ADF expressions (e.g. `@pipeline().parameters.x`)
into the target form (Python f-string, Spark SQL, or task-parameter reference).

## Step 5 — Collect agentic results

Write one JSON file per resolved gap into `<output_dir>/agentic_results/`:

```json
{
  "activity_name": "<placeholder activity name, exactly as in the report>",
  "pipeline": "<pipeline name>",
  "task": {"type": "NotebookActivity", "name": "<activity name>", "task_key": "<task key>",
           "notebook_path": "/Workspace/.../your_translated_notebook"}
}
```

`activity_name` (required) is matched by name, recursing into containers. `task_key`/`depends_on`
are inherited from the placeholder when omitted, preserving dependency edges.

## Step 6 — Merge agentic results

```bash
"$PY" -m flowx.adapter convert --merge-agentic \
  --report <output_dir>/.work/translation_report.json \
  --agentic-results <agentic_results_dir>
```

Placeholders are replaced in place, status → `translated`. Exits non-zero if any result can't be
matched. Add `--output <path>` to write a copy instead of overwriting.

## Step 6.1 — Just-in-time translation configuration

Run `inspect` **once** for the full option schema, then drive the whole question chain locally
(don't re-run `inspect` per follow-up):

```bash
"$PY" -m flowx.adapter inspect <output_dir>/.work/translation_report.json
```

Each option carries a `show_when` (a conjunction of `{option_id, in:[values]}` clauses; empty =
always ask). Ask an option only when its `show_when` is satisfied by answers already collected;
present its `prompt`/`rationale`/`choices`; honor the `default`; validate each answer (a `free_text`
option accepts any value). Perform data actions inline when an answer calls for it. Apply everything
in one `modify` call.

**Activity→Notify motifs.** When any activity is followed by notification Web activities, `inspect`
raises `notify_destination`: `keep` (default) translates the Web activities directly; any other
value (`email`/`slack`/`teams`/`pagerduty`/`webhook`) collapses the pattern into Databricks job-task
`on_success`/`on_failure` notifications. Each destination chains follow-up field options (gated by
`show_when`):

| Destination | Chained field options (SDK arg) |
|-------------|---------------------------------|
| `email`     | `notify_email_recipients` (`addresses`, comma-separated) |
| `slack`     | `notify_slack_url` (`url`), `notify_slack_channel_id` (optional), `notify_slack_oauth_token` (optional) |
| `teams`     | `notify_teams_url` (`url`) |
| `pagerduty` | `notify_pagerduty_integration_key` (`integration_key`) |
| `webhook`   | `notify_webhook_url` (`url`), `notify_webhook_username` (optional), `notify_webhook_password` (optional) |

All destinations also take optional `notify_destination_name` and `notify_events`
(both/on_failure/on_success). For non-email destinations, `modify` creates (or reuses by name) the
Databricks notification destination via the SDK when you submit answers, baking the id into the
report; requires workspace auth at `modify` time. Email uses raw `email_notifications`, no SDK call.

**Metadata-driven consolidation.** When the flow ends with `metadata_driven_lookup_tool=have` and
the agent has a database tool (Genie, MCP SQL, workspace SDK), run the lookup query to get the rows
as CSV; when `none`, ask the user for a CSV file/string. Pass it inline to `modify` via
`--lookup-csv`:

```bash
"$PY" -m flowx.adapter modify \
    <output_dir>/.work/translation_report.json \
    --output-dir <output_dir> \
    --answer metadata_driven_consolidate=consolidate \
    --answer metadata_driven_access=yes \
    --lookup-csv "<csv-file-path-or-literal-csv-string>"
```

When no metadata-driven motif is consolidated, `--lookup-csv` is omitted and the motif becomes a
Databricks for-each task running one Spark JDBC read per source table. Consolidating instead emits
one managed Lakeflow Connect ingestion pipeline.

`modify` writes `.work/translation_report.stamped.json` (consumed by package) and
`metadata/configuration.json` (the kept configuration record). When `inspect` raises no options,
skip `modify` — package falls back to the un-stamped report.

## Step 7 — Present translation summary

```
Translation Summary
===================
Total activities:           47
Deterministic translated:   35 (74.5%)
Agentic translated:          8 (17.0%)
Failed:                      4 ( 8.5%)
Overall coverage:           91.5%
```

If coverage is below 100%, explain options for failed translations: manual notebook creation, retry
agentic translation with more context, or skip with a placeholder task. See
`references/activity-mapping.md` for the full ADF activity → strategy mapping.
