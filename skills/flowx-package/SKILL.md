---
name: flowx-package
description: >
  Generate Databricks Declarative Automation Bundles (DABs) from translated IR,
  including job definitions, notebooks, and setup scripts.
  Use after convert when the user asks to package, generate, create or build the bundles (DABs).
---

# Package the translated pipelines into bundles

Phase 3 of the flowx workflow. Package reads the translation report that convert (and route, fill-agentic
and modify, when used) left in `<output_dir>/.work/` and writes a deployable bundle into `<output_dir>`:
`databricks.yml`, job and pipeline YAML under `resources/`, notebooks under `src/notebooks/`, setup
notebooks under `src/setup/`, `SETUP.md`, and a top-level `DEPLOY.md`.

## Before you start

- Run the `flowx-setup` skill first if it hasn't run.
- Convert must have run for this `<output_dir>`. If a routing plan is recorded, package refuses while a
  decision is pending, the plan is stale, or an agentic unit is unfilled; the `flowx-route` skill (Step 4)
  lists each case and its remedy.

## How to run

The common MCP and CLI rules are in [`../shared/running-flowx.md`](../shared/running-flowx.md).

- **MCP:** always pass `output_volume_path` or `output_workspace_path`, so package uploads the bundle
  there and returns `bundle_uploaded`. Without either, it returns small bundles inline as
  `bundle = {"files": {...}, "truncated": [...]}` for you to save.

  ```
  flowx(command="workspace_paths", parameters={"source": "adf", "report_path": "...", "source_dir": "<optional>"})
  flowx(command="package", parameters={"output_dir": "<dir>", "catalog": "<catalog>", "schema": "<schema>",
        "bundle_name": "<optional>", "profile": "<optional>", "download_workspace_files": true,
        "output_volume_path": "<optional /Volumes/...>", "output_workspace_path": "<optional /Workspace/...>"})
  flowx(command="record_results", parameters={"output_dir": "<dir>", "results_table": "catalog.schema.table", "warehouse_id": "<optional>"})
  flowx(command="install_dashboard", parameters={"results_table": "catalog.schema.table", "warehouse_id": "<optional>"})
  ```

- **CLI:** `"$PY" -m flowx.adapter package ...` as in Step 4.

## Steps

### Step 1: Find the translation report

Package reads `<output_dir>/.work/translation_report.stamped.json`, or `translation_report.json` when modify
didn't run; you don't pass a path. If the output folder isn't known yet, ask: "Which migration output
directory should I build the bundle in? (default `./flowx_output`)". Check that the report exists and that
every required translation has status `translated`.

### Step 2: Ask the deployment inputs

| Input | Default |
|---|---|
| Catalog and schema, asked as one free-text answer `catalog.schema` | `main.default` |
| Output directory | `./flowx_output` |
| Bundle name | the first pipeline's name |
| Target environments | `dev, staging, prod` |
| SQL warehouse id, only when the report has SQL tasks | ask |
| Packaging mode: `per-pipeline`, `single` or `per-group` | `per-pipeline` |
| Databricks CLI profile, only when Step 3 needs one | from `~/.databrickscfg` |

Split the catalog and schema answer at the dot, strip backticks, and pass `--catalog` and `--schema`. If
you use a picker, its only way to enter a value must be free text.

Packaging mode decides how a multi-pipeline factory is laid out (all modes are the same for one pipeline):
`per-pipeline` writes one bundle per pipeline and wires cross-bundle Run Pipeline calls at deploy time;
`single` writes one bundle; `per-group` writes one bundle per group of pipelines that call each other, or
per an explicit `--group-by spec --group-spec <path>` mapping. Every mode writes `DEPLOY.md` with a
callees-first deploy order.

### Step 3: Check the workspace files the bundle needs

Run `workspace-paths` and, when it returns `needs_auth: true`, log in and pass `--profile <name>` in
Step 4, as [`../shared/workspace-files.md`](../shared/workspace-files.md) describes. When `needs_auth`
is false, go on to Step 4.

### Step 4: Build the bundle

```bash
"$PY" -m flowx.adapter package --output-dir <output_dir> --catalog <catalog> --schema <schema> \
  [--bundle-name <name>] [--packaging-mode per-pipeline|single|per-group] \
  [--group-by inferred|spec] [--group-spec <path>] [--profile <profile>] \
  [--no-download-workspace-files] [--keep-intermediates]
```

Package downloads the workspace files the report references into the bundle
(`--no-download-workspace-files` keeps the original paths). After a successful build it deletes `.work/`
unless `--keep-intermediates` is set, so route and fill-agentic need a fresh convert afterwards. It
overwrites the files it generates and moves bundle files it no longer writes into
`metadata/archive/<UTC time>/`. To change the conversion after a package, follow "After package" in the
`flowx-route` skill. Details: [`references/bundle-details.md`](references/bundle-details.md).

### Step 5: Show the result and the next steps

Show the generated tree, then explain:

- `src/setup/` notebooks create the Unity Catalog volumes, secret scopes and connections the jobs need.
  Ask the user to review them before running, and to fill in the real secret values.
- `databricks.yml` holds the targets and the variables (catalog, schema, warehouse); each
  `resources/*.yml` is one job with tasks and dependencies mapped from the source.
- Next: `databricks bundle validate --target dev`, then `databricks bundle deploy --target dev`, run a
  job with `databricks bundle run <job_name> --target dev`, then deploy to the other targets.
- With several bundles, point to `DEPLOY.md` and the `flowx-deploy` skill, which deploys them in order
  and wires the cross-bundle job ids.

### Step 6 (optional): Record coverage and install the dashboard

With workspace auth, and when the user gave `results_table` (and `install_dashboard = yes`), run
`record-results` and `install-dashboard` as [`../shared/coverage-results.md`](../shared/coverage-results.md)
describes.

## Output

The folder layout is in [`../shared/output-folder.md`](../shared/output-folder.md). All under `<output_dir>`: `databricks.yml`, `resources/*.yml`, `src/notebooks/`, `src/setup/`, `SETUP.md`
(one per bundle), `DEPLOY.md`, and in `metadata/`: `inventory.json`, `profile_report.csv`,
`<pipeline>.arm.json`, `configuration.json`, `route_audit.json` (only with a routing plan),
`packaged_bundles.json` and `archive/`.

## Reference

- [`references/bundle-details.md`](references/bundle-details.md): archiving, the packaged-bundles record
  and notification destinations.
- [`../shared/running-flowx.md`](../shared/running-flowx.md): the MCP and CLI rules every phase shares.
- [`../shared/workspace-files.md`](../shared/workspace-files.md): the workspace-files check and login.
- [`../shared/coverage-results.md`](../shared/coverage-results.md): the coverage table and dashboard.
- [`../shared/output-folder.md`](../shared/output-folder.md): what each phase writes in `<output_dir>`.
