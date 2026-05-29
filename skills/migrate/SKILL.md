---
name: migrate
description: >
  End-to-end migration of Azure Data Factory pipelines to Databricks Lakeflow Jobs.
  Orchestrates ingest, translate, and prepare phases in sequence.
triggers:
  - "migrate ADF"
  - "migrate pipelines"
  - "ADF to Databricks"
  - "migrate to Lakeflow"
  - "ADF migration"
  - "convert ADF to Lakeflow"
  - "migrate data factory"
---

# End-to-End ADF to Databricks Migration

Orchestrate the complete migration of Azure Data Factory pipelines to Databricks Lakeflow Jobs via Declarative Automation Bundles. This skill runs all three phases in sequence: ingest, translate, prepare.

## Context

This is the top-level orchestration skill. It runs the full migration pipeline:

1. **Ingest** — Parse ADF JSON exports into a typed inventory
2. **Translate** — Convert ADF activities to Databricks IR (deterministic + agentic)
3. **Prepare** — Generate Databricks Declarative Automation Bundles for deployment

Each phase builds on the output of the previous phase. The user is shown a summary and asked to confirm before proceeding to the next phase.

## Workflow

Follow these steps in order:

### Step 0 — Gather phase inputs via the adapter

Before invoking ingest, run the adapter inputs subcommand once per
phase so the agent surfaces the matching free-text prompts:

```bash
python3 -m orchestra.adapter inputs ingest
python3 -m orchestra.adapter inputs translate
python3 -m orchestra.adapter inputs prepare
```

Each response carries the questions for that phase plus their
descriptions and defaults.  Collect answers from the user (or accept
the defaults), persist them to `<output_dir>/<phase>/inputs.json`, and
thread the values into the downstream CLI calls.

### Step 1 — Gather inputs

Ask the user for all required inputs upfront:

| Parameter | Description | Required | Default |
|---|---|---|---|
| ADF source path | UC volume path or local directory with ADF JSON files | Yes | — |
| Output directory | Root directory for all orchestra output | No | `./orchestra_output/` |
| Target catalog | Unity Catalog catalog for tables/volumes | No | `main` |
| Target schema | Schema within the catalog | No | `default` |
| Bundle name | Name for the generated DABs project | No | derived from pipelines |

Example prompt:

> To migrate your ADF pipelines, I need:
> 1. Where are your ADF JSON exports? (UC volume path like `/Volumes/main/default/adf_export` or local directory)
> 2. Where should I write the output? (default: `./orchestra_output/`)
> 3. What target catalog and schema? (default: `main.default`)

### Step 2 — Phase 1: Ingest

Invoke the `orchestra:ingest` skill with the ADF source path and output directory set to `<output_dir>/ingest/`.

Wait for the ingest to complete and present the inventory summary:

```
Phase 1: Ingest — Complete
==========================
Pipelines parsed:     12
Total activities:     47
  Deterministic:      35 (74.5%)
  Agentic:            10 (21.3%)
  Unsupported:         2 ( 4.3%)
Coverage:             95.7%
```

### Step 3 — Checkpoint: confirm proceed

Ask the user to review the inventory and confirm before continuing:

> The ingest phase found 47 activities across 12 pipelines. 95.7% have a translation path (74.5% deterministic, 21.3% agentic). 2 activities are unsupported and will need manual handling.
>
> Proceed to the translation phase? (yes/no)

If the user says no, explain the options:
- Re-run ingest with a different source directory
- Review the `inventory.json` to understand unsupported activities
- Manually classify activities before proceeding

If the user says yes, proceed to step 4.

### Step 4 — Phase 2: Translate

Invoke the `orchestra:translate` skill with:
- Inventory path: `<output_dir>/ingest/inventory.json`
- ADF source dir: the original ADF source path
- Output dir: `<output_dir>/translate/`

Wait for the translation to complete and present the summary:

```
Phase 2: Translate — Complete
=============================
Deterministic translated:   35 (74.5%)
Agentic translated:          8 (17.0%)
Failed:                      4 ( 8.5%)
Overall coverage:           91.5%
```

### Step 5 — Present translation details

Show the user:
1. What was translated deterministically (bulk — just counts by type)
2. What was translated via agentic skills (list each with the skill used)
3. What failed and why (list each with the failure reason)

For failures, suggest:
- Manual notebook creation
- Retry with additional context
- Skip and add placeholder

### Step 5.1 — Gather just-in-time translation preferences

Drive the loop multi-pass: re-run `inspect --answers <answers.json>`
after each batch of answers so the adapter can surface chained
metadata-driven prompts.  When the user opts to consolidate a
metadata-driven motif and the agent has a database tool, run the
lookup query directly and persist the rows to
`<output_dir>/translate/lookup_values.json`; otherwise prompt the user
for a CSV file or comma-separated string and run:

```bash
python3 -m orchestra.adapter materialize-lookup "<csv-or-path>" \
    --out <output_dir>/translate/lookup_values.json
```

Pass `--lookup-values` to the modify call when the file exists.

#### Legacy flow details

Before bundle generation, run the adapter inspect CLI on the translation
report to surface any pipeline-modifier questions the IR raises:

```bash
python3 -m orchestra.adapter inspect <output_dir>/translate/translation_report.json
```

For each question in the JSON output, prompt the user with the rationale,
options, and the affected task keys.  Collect answers into
`<output_dir>/translate/answers.json` keyed by `question_id`, then apply
them to a stamped report:

```bash
python3 -m orchestra.adapter modify \
  <output_dir>/translate/translation_report.json \
  <output_dir>/translate/answers.json \
  --out <output_dir>/translate/translation_report.stamped.json
```

Use the stamped report (when produced) as the input to the prepare phase.
When inspect emits no questions for any pipeline, skip modify and use the
original report.

The four questions the adapter raises:

| `question_id` | Allowed values | Default |
|---|---|---|
| `copy_activity_paradigm` | `notebook`, `sdp` | `notebook` |
| `non_databricks_task_compute` | `serverless`, `classic` | `serverless` |
| `use_lakeflow_connectors` | `existing`, `lakeflow_connect` | `existing` |
| `databricks_task_compute` | `existing`, `serverless` | `existing` |

### Step 6 — Checkpoint: confirm proceed to bundle generation

> Translation is 91.5% complete. 4 activities could not be translated automatically.
> Options:
> 1. Proceed to bundle generation (failed activities will get placeholder tasks)
> 2. Retry failed translations with more context
> 3. Stop here and review the translation report
>
> What would you like to do?

### Step 6.5 — Detect workspace artifacts and authenticate

Before invoking the prepare phase, run the adapter's
`workspace-paths` subcommand to detect any absolute workspace paths
the bundle would need to vendor:

```bash
python3 -m orchestra.adapter workspace-paths \
  <output_dir>/translate/translation_report.stamped.json \
  --source-dir <adf_source_path>
```

When the response carries `needs_auth: true`:

1. Confirm the workspace host with the user, defaulting to the first
   entry in `suggested_hosts` (extracted from the Databricks linked
   services in the ADF export).
2. Run `databricks auth login --host <host>` interactively to set up
   a local profile.
3. Pass `--profile <name>` to the prepare invocation in Step 7 so
   orchestra downloads the referenced notebooks and vendors them under
   `bundle/src/notebooks/` with the task references rewritten to the
   relative `../src/notebooks/...` paths.

Skip this step entirely when `needs_auth` is `false`.

### Step 7 — Phase 3: Prepare

Invoke the `orchestra:prepare` skill with:
- Translation report: `<output_dir>/translate/translation_report.stamped.json` if step 5.5 produced one, otherwise `<output_dir>/translate/translation_report.json`
- Output dir: `<output_dir>/dab_output/`
- Catalog: user-specified or `main`
- Schema: user-specified or `default`

### Step 8 — Present final summary

Display the complete migration summary:

```
Migration Complete
==================

Source:    /Volumes/main/default/adf_export (12 ADF pipelines)
Output:    ./orchestra_output/dab_output/

Coverage:
  Total activities:           47
  Successfully translated:    43 (91.5%)
  Placeholder tasks:           4 ( 8.5%)

Generated Files:
  dab_output/
    databricks.yml
    resources/          (3 job definitions)
    src/notebooks/      (12 notebooks)
    setup/              (3 setup scripts)
    tests/              (3 test files)

Setup Required:
  - Run setup/create_volumes.py to create UC volumes
  - Run setup/create_secrets.py to configure secrets (review credentials first)
  - Run setup/register_connections.py to register external connections

Next Steps:
  1. cd ./orchestra_output/dab_output/
  2. Review generated files, especially notebooks and setup scripts
  3. databricks bundle validate --target dev
  4. Run setup scripts on the target workspace
  5. databricks bundle deploy --target dev
  6. databricks bundle run <job_name> --target dev
  7. Verify job output and promote to staging/prod
```

### Step 9 — Offer follow-up actions

Ask if the user wants to:
1. Validate the bundle now (`databricks bundle validate`)
2. Deploy to dev (`databricks bundle deploy --target dev`)
3. Review specific generated files
4. Re-translate any failed activities
5. Export a migration report for documentation

## Reference

See `references/workflow.md` for a detailed description of the three-phase architecture.

## Examples

- "Migrate my ADF pipelines to Databricks"
- "Convert ADF to Lakeflow jobs"
- "ADF to Databricks migration from /Volumes/main/default/adf_export"
- "Migrate data factory pipelines to catalog analytics, schema bronze"
- "Run the full ADF migration workflow"

## Output Artifacts

All artifacts from all three phases are produced under the output directory:

| Directory | Phase | Contents |
|---|---|---|
| `ingest/` | Ingest | `inventory.json`, `ast/`, `parse_errors.json` |
| `translate/` | Translate | `translation_report.json`, `ir/`, `notebooks/`, `agentic_results/` |
| `dab_output/` | Prepare | `databricks.yml`, `resources/`, `src/`, `setup/`, `tests/` |
