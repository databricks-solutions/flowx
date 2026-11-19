# The MCP path (Databricks Genie Code, or a local stdio registration)

Read this from the `flowx-migrate` skill before Step 1 when you run the phases through the `flowx` MCP
tool. In Genie Code this is the only path: the phases run on the deployed `mcp-flowx` app, so there is
no venv, no `bootstrap.sh` and no `.migration-venv`. What every phase shares on this path (the single
`flowx` tool, no Python commands, the temporary server folder and the output paths) is in
[`../../shared/running-flowx.md`](../../shared/running-flowx.md).

## Contents

- [Pass the source by reference](#pass-the-source-by-reference)
- [Run the same steps, one call per phase](#run-the-same-steps-one-call-per-phase)
- [The deterministic-only shortcut](#the-deterministic-only-shortcut)
- [Airflow gaps](#airflow-gaps)

## Pass the source by reference

**The hosted server cannot read files by path**, whether they sit in your local file system, the
workspace or a Databricks volume. Pass the source one of these ways:

- `adf_volume_path`, for an ADF export staged to a UC volume, for example
  `"/Volumes/cat/sch/adf_export"` (the server reads it through the SDK Files API);
- `adf_workspace_path`, for an ADF Git folder already in the workspace, for example
  `"/Workspace/Shared/adf_export"` (read through the SDK Workspace API);
- inline as `adf_definitions`, for small factories only: a mapping of relative path to ARM JSON content
  that mirrors the Git-export layout (`pipeline/...`, `dataset/...`, `linkedService/...`,
  `trigger/...`), which you read and supply yourself.

**Large factories (hundreds to thousands of pipelines): do not inline.** Inline `adf_definitions`
passes through your context window and is capped near 5 MB; use a volume or workspace path. For
Airflow, pass `airflow_source_path` in place of `adf_definitions`.

## Run the same steps, one call per phase

Run the SKILL.md steps in order, one `flowx` call per phase: discover, enrich (optional), convert,
route, fill, configuration options, package. Do not replace them with one `migrate` call: `migrate` is
server code, which can't author the insights or ask the user for the routing decisions, so it runs only
discover, convert and package and never enriches or routes.

Convert reads the source again, so pass the same source key to `discover` and `convert`. With inline
`adf_definitions` that means sending the payload twice; a volume or workspace path sends only the
path. Every other command reuses `output_dir`. Call `inputs` for a phase just before that phase, as
SKILL.md "How to run" describes. On non-ADF sources route records deterministic decisions only;
agentic decisions, `fill_agentic` and `merge_agentic` are ADF only. `package` and `inputs package`
are source-independent.

```
flowx(command="inputs", parameters={"phase": "discover", "source": "adf"})  # just before discover; source req for discover/convert
flowx(command="discover", parameters={"source": "adf", "adf_definitions": {...}, "output_dir": ..., "pipeline": ...})
flowx(command="enrich", parameters={"output_dir": ..., "action": "prepare"})  # returns inventory.json to author against
flowx(command="enrich", parameters={"output_dir": ..., "insights": {...}})  # default: author + record the insights layer (see flowx-enrich)
flowx(command="convert", parameters={"source": "adf", "adf_definitions": {...}, "output_dir": ..., "pipeline": ...})  # same source as discover; deterministic baseline, BEFORE route
flowx(command="route", parameters={"output_dir": ...})  # writes the plan + review page; re-call with the decided "plan": {...} to apply (see flowx-route)
flowx(command="merge_agentic", parameters={"source": "adf", "report_path": ..., "agentic_results_dir": ..., "output_path": ...})  # ADF only, fills convert's own gaps
flowx(command="fill_agentic", parameters={"output_dir": ..., "members": [...], "pipelines": [...]})  # the only fill for a routed-agentic unit
flowx(command="inspect", parameters={"report_path": ...})
flowx(command="apply_answers", parameters={"report_path": ..., "answers": [...], "output_dir": ...})
flowx(command="package", parameters={"output_dir": ..., "catalog": ..., "schema": ...})
flowx(command="record_results", parameters={...}) / flowx(command="install_dashboard", parameters={...})
```

## The deterministic-only shortcut

Only when the user explicitly asks for a deterministic-only pass (no enrich, no routing, as in SKILL.md
Step 4), one `migrate` call runs discover, convert and package and sends the source once:

```
flowx(command="migrate", parameters={
  "source": "adf",
  "adf_definitions": {"pipeline/Foo.json": {...}, "linkedService/Bar.json": {...}, ...},
  "output_dir": ..., "catalog": ..., "schema": ..., "pipeline": "<optional>"})
```

**`migrate` is interactive when configuration options exist.** After translating, if the pipeline
raises any configuration options (for example how to handle an `activity_and_notify` motif,
metadata-driven bulk-copy consolidation, or non-Databricks task compute), it does not package. It
returns the full option schema once: `{"status": "needs_input", "pending_options": [{"pipeline_name",
"options": [{option_id, prompt, rationale, choices, free_text, default, show_when}, ...]}, ...],
"report_path", "output_dir"}`. Walk the whole chain locally, with no round trip per answer (the same
walk applies to `inspect` then `apply_answers` on the step-by-step path):

1. Ask an option only when its `show_when` is satisfied: every clause `{option_id, in:[values]}` must
   match an answer already collected (an empty `show_when` means always ask). So `notify_slack_url`
   comes only after `notify_destination=slack`, and the metadata-driven `access`, `size` and
   `lookup_tool` chain only after `metadata_driven_consolidate=consolidate`. Present each option's
   `prompt`, `rationale` and `choices`, and honor the `default`.
2. Validate each answer against `choices` (a `free_text` option accepts any value) and collect the picks
   as `"option_id=value"` strings. Run any data action inline, for example the lookup query when
   `metadata_driven_lookup_tool=have`.
3. When every applicable option is answered, call `migrate` once with the same parameters plus
   `"answers": ["option_id=value", ...]`. It applies them and packages (`"status": "completed"`).

To accept all defaults and skip the prompts, pass `"interactive": false`. Calling `migrate` again with
`answers` reuses the existing report and does not run discover or convert again.

## Airflow gaps

For an Airflow report with eligible leaf placeholders, use the `flowx-resolve-airflow-gaps` skill between
convert and package. It calls `resolve_agentic` with `action="prepare"`, stages one or more provider
candidates, and applies only the gap fingerprints the user explicitly accepts. Package must then receive
`<output_dir>/.work/translation_report.agentic.json` as `report_path`.
