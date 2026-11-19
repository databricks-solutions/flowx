# The MCP path (Databricks Genie Code, or a local stdio registration)

Read this from the `flowx-migrate` skill before Step 1 when you run the phases through the `flowx` MCP tool.

In Genie Code this is the **only** path — the phases run on the deployed `mcp-flowx` app, so
there is **no venv, no `bootstrap.sh`, and no `.migration-venv`**. Run **no** `python3`/`$PY`/`bash`
commands on this path; the `"$PY" -m …` snippets in the steps below are the **local-CLI fallback
only**. Everything goes through the single **`flowx`** tool, one `command` per step:

The hosted server **cannot read files by path**, whether they sit in your local file system, the
workspace, or a Databricks volume, so pass the source by reference as `adf_volume_path` or
`adf_workspace_path` (the server reads them through the SDK; see below), or, for small factories,
inline as `adf_definitions` (a mapping of relative path → ARM JSON content mirroring
the Git-export layout — `pipeline/…`, `dataset/…`, `linkedService/…`, `trigger/…`), which you read and
supply yourself.

**Run the same Workflow as the CLI path, one `flowx` call per phase**: discover → enrich (optional) →
convert → route → fill → configuration options → package, in the order of the steps below. Do **not**
replace it with one `migrate` call: `migrate` is server code, which can't author the insights or ask
the user for the routing decisions, so it runs only discover → convert → package and never enriches
or routes. Convert reads the source again, so pass the **same source key to `discover` and
`convert`**. With inline `adf_definitions` that means sending the payload twice; a volume or workspace
path (below) sends only the path. Every other command reuses `output_dir`. Ask each phase's inputs
when that phase runs, as Steps 0 and 1 below describe: call `inputs` for a phase just before it, not
for every phase at the start. `source` ("adf" | "airflow") is required for discover/convert and for
`inputs discover`/`inputs convert`; for Airflow, swap `adf_definitions` for `airflow_source_path`.
On non-ADF sources route records deterministic decisions only; agentic decisions and `fill_agentic`
are ADF-only. `merge_agentic` is ADF-only. `package` and `inputs package` are source-independent:

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

**Deterministic-only shortcut: `migrate`.** Only when the user explicitly asks for a deterministic-only
pass (no enrich, no routing, as in Step 4), one `migrate` call runs discover → convert → package and
sends the source once:

```
flowx(command="migrate", parameters={
  "source": "adf",
  "adf_definitions": {"pipeline/Foo.json": {...}, "linkedService/Bar.json": {...}, ...},
  "output_dir": ..., "catalog": ..., "schema": ..., "pipeline": "<optional>"})
```

**`migrate` is interactive when configuration options exist.** After translating, if the pipeline
raises any configuration options (e.g. how to handle an `activity_and_notify` motif, metadata-driven
bulk-copy consolidation, non-Databricks task compute), it **does not package** — it returns the
**full option schema once**: `{"status": "needs_input", "pending_options": [{"pipeline_name",
"options": [{option_id, prompt, rationale, choices, free_text, default, show_when}, …]}, …],
"report_path", "output_dir"}`. You drive the whole chain locally — no per-answer round trip (the same
walk applies to `inspect` → `apply_answers` on the step-by-step path):

1. **Ask an option only when its `show_when` is satisfied** — every clause `{option_id, in:[values]}`
   must match an answer you've already collected (empty `show_when` = always ask). So
   `notify_slack_url` surfaces only after `notify_destination=slack`; the metadata-driven
   `access`/`size`/`lookup_tool` chain only after `metadata_driven_consolidate=consolidate`. Present
   each option's `prompt`/`rationale`/`choices`; honor the `default`.
2. **Validate** each answer against `choices` (a `free_text` option accepts any value); collect picks
   as `"option_id=value"` strings. Run any data action inline (e.g. the lookup query when
   `metadata_driven_lookup_tool=have`).
3. When **every applicable option** is answered, call `migrate` **once** with the same parameters plus
   `"answers": ["option_id=value", …]`. It applies them and packages (`"status": "completed"`).

To accept all defaults and skip the prompts, pass `"interactive": false`. (Re-calling `migrate` with
`answers` reuses the existing report and skips re-running discover/convert.)

> **Large factories (hundreds–thousands of pipelines): do not inline.** Inline `adf_definitions`
> passes through your context window and is capped (~5 MB). Instead point the server at the source by
> reference: either stage the ADF export to a **UC Volume** and pass
> `"adf_volume_path": "/Volumes/cat/sch/adf_export"` (read via the SDK Files API), or pass
> `"adf_workspace_path": "/Workspace/Shared/adf_export"` for an ADF Git folder already in the workspace
> (read via the SDK Workspace API). For output, pass `"output_volume_path": "/Volumes/cat/sch/dab"`
> **or** `"output_workspace_path": "/Workspace/Shared/dab"` so the generated bundle is written to that
> target via the SDK (returned as `bundle_uploaded` instead of inline `bundle`). Grant the
> `mcp-flowx` app's service principal read on the source and write on the output target.

For an Airflow report with eligible leaf placeholders, use the `flowx-resolve-airflow-gaps` skill between convert and package. It calls `resolve_agentic` with `action="prepare"`, stages one or more provider candidates, and applies only the gap fingerprints the user explicitly accepts. Package must then receive `<output_dir>/.work/translation_report.agentic.json` as `report_path`.

The server's `output_dir` is ephemeral and not reachable from your workspace, so **have `migrate`/
`package` write the DAB to the target via the SDK** — pass `"output_volume_path": "/Volumes/…"` or
`"output_workspace_path": "/Workspace/…"` and the bundle is uploaded there (returned as
`bundle_uploaded`). Only when neither is set is the bundle returned inline as `bundle = {"files":
{relpath: text,…}, "truncated": [...]}` (small bundles), which you must then persist yourself. Either
way the user ends up with the DAB to validate and deploy. Each call returns a structured result
(summaries / file trees); use those in place of reading files. Wherever a step below shows `"$PY" -m flowx.adapter <cmd> …`, call
`flowx(command="<cmd>", parameters={...})` instead.

> `databricks bundle validate` / `deploy` of the *generated* bundle is still a user-driven CLI step
> (web terminal / local / CI-CD); present the bundle for review.
