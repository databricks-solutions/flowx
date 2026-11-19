---
name: flowx-migrate
description: >
  End-to-end migration of a source orchestrator's pipelines (Azure Data Factory, Apache Airflow)
  to Databricks Lakeflow Jobs. Orchestrates discover → enrich → convert (deterministic baseline) →
  route → fill → package in sequence.
  Use when the user asks to migrate ADF, Data Factory or Airflow pipelines to Databricks or
  Lakeflow.
---

# Migrate pipelines to Databricks end to end

The top-level skill. It runs the other flowx skills in order (extract if needed, discover, enrich,
convert, route, the fills, configuration, package) against one shared `<output_dir>`, shows a summary
after each phase, and asks the user to confirm before the next. It reads the source's definitions and
writes the deployable bundle plus the kept `metadata/` folder. Each phase's own skill owns how that phase
works; this skill owns the order, the checkpoints, what to ask when, and what to pass on.

## Before you start

- Run the `flowx-setup` skill first if it hasn't run.
- **Scope by source.** Discover, convert and package work for ADF and Airflow, and enrich runs for every
  source. Route and the agentic fills apply to ADF only: route records only `deterministic` decisions for
  an Airflow inventory and refuses an agentic one, and `convert --merge-agentic --source airflow` is
  disabled. Airflow gaps go through the `flowx-resolve-airflow-gaps` skill.
- Ask which orchestrator the user is migrating from, or infer it from the input: Azure Data Factory or
  Fabric Data Factory (`--source adf`), or Apache Airflow (`--source airflow`). There is no default.
  Pass `--source` to discover and convert; package is source-independent. The commands below show ADF;
  for another source swap `--source` and the source path (`--adf-source-path` and
  `--airflow-source-path` are both aliases of `--source-path`).

## How to run

- **MCP (the only path in Genie Code):** the phases run on the deployed `mcp-flowx` app. Before Step 1,
  read [`references/mcp-path.md`](references/mcp-path.md): how to pass the source to the hosted server
  and the `flowx` call for each step. Wherever a step shows `"$PY" -m flowx.adapter <cmd> ...`, call the
  `flowx` command of the same name, with underscores for hyphens (`fill-agentic` is `fill_agentic`,
  `record-results` is `record_results`), except that `modify` is `apply_answers` and
  `convert --merge-agentic` is `merge_agentic`.
- **CLI (local):** run each command with the venv interpreter, for example
  `"$PY" -m flowx.adapter inputs discover --source adf`.
- What is the same for every phase (the `flowx` tool, the venv, a missing Python) is in
  [`../shared/running-flowx.md`](../shared/running-flowx.md).

**Ask each phase's inputs when that phase runs.** The `inputs` command lists a phase's prompts with
their descriptions and defaults. Run it for a phase just before that phase, not for all phases at the
start, so each question comes when the user can answer it:

```bash
"$PY" -m flowx.adapter inputs discover --source adf   # before discover (Step 2); or --source airflow
"$PY" -m flowx.adapter inputs convert --source adf    # before convert (Step 5)
"$PY" -m flowx.adapter inputs package                 # before package (Step 12); source-independent
```

Ask only the prompts the user hasn't answered, accept the defaults for the rest, and pass the values
into that phase's call. Every phase uses the same `<output_dir>` (default `./flowx_output`).

## Steps

### Step 1: Ask the starting inputs

At the start, ask for only these three:

| Input | Description | Required | Default |
|---|---|---|---|
| Source path | ADF: a UC volume path or local directory with the ADF JSON files. Airflow: a DAG `.py` file or directory | Yes | none |
| Output directory | The one shared root for all flowx output (bundle and `metadata/`) | No | `./flowx_output` |
| Flow | The full flow (enrich and a routing review), or a deterministic-only pass | No | full flow |

If the user has no ADF export, run the `flowx-extract-metadata` skill first and use the `discover_path`
it returns as the source path.

Ask in plain words, adapted to the source, for example:

> To migrate your pipelines, I need:
> 1. Where are your pipeline definitions? (for ADF a UC volume path such as `/Volumes/main/default/adf_export` or a local folder with the JSON export, or I can help you get them; for Airflow the DAG file or folder)
> 2. Where should I write the output? (default: `./flowx_output/`)
> 3. The full flow (enrich and a routing review), or a deterministic-only pass? (default: the full flow)

Ask nothing else yet. The other inputs make sense only once the user has seen what discover found: the
convert options (for ADF, how factory global parameters are resolved) at Step 5, and the catalog and
schema, the bundle name and any other `inputs package` option at Step 12.

### Step 2: Discover

Run the `flowx-discover` skill with `--source <source>`, the source path and `--output-dir <output_dir>`.
It writes `metadata/inventory.json` and `metadata/profile_report.csv` (plus `<pipeline>.arm.json` for
ADF). Present the inventory summary:

```
Discover complete
Pipelines parsed:     12
Total activities:     47
  Deterministic:      35 (74.5%)
  Agentic:            10 (21.3%)
  Unsupported:         2 ( 4.3%)
Coverage:             95.7%
```

### Step 3: Confirm before going on

> The discover phase found 47 activities across 12 pipelines. 95.7% have a translation path (74.5% deterministic, 21.3% agentic). 2 activities are unsupported and will need manual handling.
>
> Proceed to the convert phase? (yes/no)

If the user says no, offer to run discover again with a different source directory, to review
`metadata/inventory.json` and `profile_report.csv` for the unsupported activities and pipeline
complexity, or to classify activities by hand before going on.

### Step 4 (optional): Enrich the inventory

Unless the user chose a deterministic-only pass, run the `flowx-enrich` skill. You author the
`insights` (the factory-wide recommendation, each pipeline's intent and recommended Databricks patterns,
and cross-pipeline relationships); `enrich` validates them and merges them into `inventory.json`
without changing any deterministic key. Before recording them, tell the user in a few lines what the
analysis found and ask the open questions whose answers would rule a target pattern in or out (the
skill's "Summarise and ask"); record the answers so route doesn't ask them again. Route reads the
insights to present the agentic option per component.

**For a deterministic-only pass, skip enrich, route and the agentic fills (Steps 4, 7 and 8).** The
deterministic `inventory.json` is complete without them.

### Step 5: Convert to the deterministic baseline

Just before convert, run `inputs convert` and ask the convert options the user hasn't answered; for ADF
that is how factory global parameters are resolved (`--global-parameter-resolution`, default `literal`).
Then run the `flowx-convert` skill with the same `--source`, the same source path discover used, and
the same `<output_dir>`. It writes the deterministic baseline `<output_dir>/.work/translation_report.json`.

Convert runs before route, because route reads and edits this report. If you run convert again after
routing (for example with other options), run route straight after: a fresh report carries no routing
record, so route takes it as the new baseline and re-applies the plan and the agent's stored outputs.
Merges of convert's own gaps made against a different report are kept but not applied (they come back
if convert writes that report again), so merge them again for the new one.
After a package, this is also how you change the conversion (Step 15).

```
Convert complete
Deterministic translated:   35 (74.5%)
Agentic translated:          8 (17.0%)
Failed:                      4 ( 8.5%)
Overall coverage:           91.5%
```

### Step 6: Present the translation details

Show the counts translated deterministically by type, each agentic translation with the skill used,
and each failure with its reason. For a failure, suggest a hand-written notebook, a retry with more
context, or skipping it with a placeholder.

### Step 7: Route the conversion

Skip this step and Step 8 for Airflow (take the deterministic report straight to Step 9 and resolve
Airflow gaps with `flowx-resolve-airflow-gaps`) and for a deterministic-only pass (with no recorded
plan, convert and package behave exactly as before).

Run the `flowx-route` skill on the baseline from Step 5. Route writes its per-component recommendation
and the suggested groupings into `metadata/conversion_plan.json`, every decision pending, and draws them
as `metadata/routing_review.html`. Present the page, ask what the customer wants the conversion to
achieve, qualify the target pattern for any ingestion component (the skill's "Qualify the target
pattern"), record only the customer's decisions, and run route again. Once nothing is pending, route
applies the plan: each routed-agentic unit's tasks become placeholder gaps, and deterministic units
leave the report untouched. A user who wants a straight deterministic migration decides every
component deterministic here, and Step 8 then has nothing to do.

Once every decision is made, route refuses without the Step 5 report ("run the convert phase first").
Running route again is always allowed: it rebuilds the report from the saved baseline, the stored gap
fills, the current plan and the agent's stored outputs, so switching a component back to deterministic
restores convert's output for it.

### Step 8: Fill the routed-agentic gaps

If Step 7 routed any unit agentic, its tasks are now `PlaceholderActivity` nodes with one tagged gap
each, and package refuses until each such unit is filled. Fill before Step 9 so the filled tasks are
configured and packaged. The `flowx-route` skill (Step 3) owns how; in short:

- **`fill-agentic` is the only fill for a routed-agentic unit:**
  `fill-agentic --output-dir <output_dir> --member <a> --member <b> --pipelines-path <file>`, with the
  members exactly matching the routed-agentic component or accepted grouping. The authored pipelines are
  stored in `metadata/agentic_conversion.json`, so a later route re-applies them.
- **Convert's own agentic gaps (ADF only, in pipelines not routed agentic):** author one result JSON per
  gap and run
  `convert --source adf --merge-agentic --report .work/translation_report.json --agentic-results <dir>`.
  `--source adf` is required (convert exits 2 without it). Each result replaces the first task whose
  `name` matches its `activity_name`, so check each name targets the intended gap. Airflow's
  `--merge-agentic` is disabled (exits 2); Airflow per-gap fills use `flowx-resolve-airflow-gaps`.

Run `modify` (Step 9) after the last route, fill-agentic or merge: none of them rewrites modify's
configured report, and package asks you to run modify again when it is out of date.

### Step 9: Ask the translation options

Run `inspect` once for the full option schema, where every option carries a `show_when` condition, and
walk it locally: ask an option only when all its `show_when` clauses match answers already collected;
never run `inspect` again per follow-up. When the user consolidates a metadata-driven motif and you have
a database tool, run the lookup query for the CSV rows; otherwise ask for a CSV file path or a literal
CSV string. Apply everything in one `modify` call (every `--answer OPTION_ID=VALUE`, plus
`--lookup-csv "<csv-or-path>"` when needed, with no intermediate JSON file). The commands and the
options are in [`../shared/translation-options.md`](../shared/translation-options.md).

### Step 10: Confirm before building the bundle

> Translation is 91.5% complete. 4 activities could not be translated automatically.
> Options:
> 1. Proceed to bundle generation (failed activities will get placeholder tasks)
> 2. Retry failed translations with more context
> 3. Stop here and review the translation report
>
> What would you like to do?

### Step 11: Check the workspace files the bundle needs

Run `workspace-paths` on `.work/translation_report.stamped.json` with `--source-dir <source_path>`, and
log in when it returns `needs_auth: true`, as
[`../shared/workspace-files.md`](../shared/workspace-files.md) describes. Pass the resulting
`--profile <name>` to package in Step 12, so package downloads the referenced notebooks into the bundle.

### Step 12: Package

Run `inputs package` and ask the options the user hasn't answered: the catalog and schema, the bundle
name, and any other option it lists. Ask for the catalog and schema as one free-text question: "Which
catalog and schema should the bundle target, as `catalog.schema`? (default `main.default`)". Split the
answer at the dot, strip backticks around either name, and pass them as `--catalog` and `--schema`
(`main` and `default` when not given). If you use a picker, its only way to type a value must be free
text, so a pick always carries the names.

Then run the `flowx-package` skill with the same `<output_dir>`; it reads the stamped report from
`.work/` itself and writes the bundle there. After a successful build it deletes `.work/`, leaving the
bundle and `metadata/`.

### Step 13 (optional): Record coverage and install the dashboard

With workspace auth, offer to record this run's coverage to a Unity Catalog table and to install a
coverage dashboard, as [`../shared/coverage-results.md`](../shared/coverage-results.md) describes. The
`inputs package` answers carry `results_table`, `results_warehouse_id` and `install_dashboard`.

### Step 14: Present the final summary

```
Migration Complete
==================

Source:    /Volumes/main/default/adf_export (12 ADF pipelines)
Output:    ./flowx_output/  (bundle + metadata/)

Coverage:
  Total activities:           47
  Successfully translated:    43 (91.5%)
  Placeholder tasks:           4 ( 8.5%)

Generated Files (under ./flowx_output/):
  databricks.yml
  resources/          (3 job definitions)
  src/notebooks/      (12 notebooks)
  src/setup/          (3 setup scripts)
  SETUP.md
  metadata/           inventory.json, profile_report.csv, <pipeline>.arm.json, configuration.json

Setup Required:
  - Run src/setup/create_volumes.py to create UC volumes
  - Run src/setup/create_secrets.py to configure secrets (review credentials first)
  - Run src/setup/create_connections.py to create external connections

Next Steps:
  1. cd ./flowx_output/
  2. Review generated files, especially notebooks and setup scripts
  3. databricks bundle validate --target dev
  4. Run setup scripts on the target workspace
  5. databricks bundle deploy --target dev
  6. databricks bundle run <job_name> --target dev
  7. Verify job output and promote to staging/prod
```

This shows one bundle at the top of the output folder (one pipeline, or `single` packaging). With
several bundles (the default `per-pipeline` layout, or `per-group`), list each bundle folder with its own
`databricks.yml`, `resources/`, `src/` and `SETUP.md`, point to the top-level `DEPLOY.md`, and replace the
deploy steps with the `flowx-deploy` skill, which deploys callees first and wires the cross-bundle job ids.
List only the setup scripts package actually wrote.

### Step 15: Offer follow-up actions

Offer to validate the bundle (`databricks bundle validate`), deploy to dev
(`databricks bundle deploy --target dev`, or the `flowx-deploy` skill for several bundles), review specific generated files, translate failed
activities again, or export a migration report. To change a decision, an answer or an agent output:
package deleted `.work/`, so run convert (same source and options), route, fill-agentic for a unit whose
output changes, modify if it was used, then package. The plan, the stored fills and modify's answers
carry over from `metadata/`; hand edits to generated bundle files don't. See "After package" in the
`flowx-route` skill.

## Output

Everything goes into the one `<output_dir>`: `metadata/` (discover, enrich, route, modify), the bundle
(`databricks.yml`, `resources/`, `src/`, `SETUP.md`, `DEPLOY.md`) from package, and the transient
`.work/` that package deletes. Each file and the phase that writes it is in
[`../shared/output-folder.md`](../shared/output-folder.md).

## Reference

- [`references/mcp-path.md`](references/mcp-path.md): the MCP path: passing the source to the hosted
  server, the call for each step, and the one-call deterministic `migrate` shortcut.
- [`../shared/translation-options.md`](../shared/translation-options.md): the `inspect` and `modify`
  commands and the options they raise.
- [`references/workflow.md`](references/workflow.md): the workflow phase by phase, with each phase's
  inputs, process and outputs.
