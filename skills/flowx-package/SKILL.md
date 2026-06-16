---
name: flowx-package
description: >
  Generate Databricks Declarative Automation Bundles (DABs) from translated IR,
  including job definitions, notebooks, and setup scripts.
triggers:
  - "package bundles"
  - "generate DABs"
  - "create bundles"
  - "package deployment"
  - "generate bundles"
  - "build DABs"
---

# Package Databricks Declarative Automation Bundles

Generate deployment-ready Databricks Declarative Automation Bundles (DABs) from the translated intermediate representation, including job definitions, notebooks, and infrastructure setup scripts.

## Context

This is phase 3 of the flowx migration workflow. It consumes the `translation_report.json` produced by the `convert` skill and generates a complete DABs project that can be validated and deployed with the Databricks CLI.

The output is a standard DABs project with:
- `databricks.yml` — the bundle configuration
- `resources/` — job and pipeline YAML definitions
- `src/notebooks/` — generated and helper notebooks
- `setup/` — infrastructure setup scripts (volumes, secrets, connections)

## How to run this skill — MCP tools or venv CLI

This phase runs one of two ways; run the **`setup`** skill first if you haven't.

- **MCP tool (Databricks Genie Code, or a local stdio registration) — the only path in Genie Code:**
  call the single **`flowx`** tool (one command per step) and run **no** `python3`/`$PY`/`bash`
  commands. The `"$PY" -m …` snippets in the steps below are the **local-CLI fallback only** — ignore
  them on this path. Map the steps to:

  ```
  flowx(command="package", parameters={"output_dir": "<dir>", "report_path": "<optional>", "catalog": "<catalog>",
                                            "schema": "<schema>", "bundle_name": "<optional>", "profile": "<optional>",
                                            "download_workspace_files": true,
                                            "output_volume_path": "<optional /Volumes/... target>",
                                            "output_workspace_path": "<optional /Workspace/... target>"})
  flowx(command="workspace_paths", parameters={"report_path": "...", "source_dir": "<optional ADF source>"})
  flowx(command="record_results", parameters={"output_dir": "<dir>", "results_table": "catalog.schema.table", "warehouse_id": "<optional>"})
  flowx(command="install_dashboard", parameters={"results_table": "catalog.schema.table", "warehouse_id": "<optional>"})
  ```

  The server's `output_dir` is ephemeral and not reachable from your workspace, so **have `package`
  write the bundle to the target itself via the SDK** — don't try to persist files yourself. Pass one
  of:
  - `"output_volume_path": "/Volumes/cat/sch/dab"` — uploads the DAB to that UC Volume (SDK Files API).
  - `"output_workspace_path": "/Workspace/Shared/dab"` — uploads it to that workspace folder (SDK
    Workspace API; files written verbatim, not imported as notebooks).

  Either returns `bundle_uploaded = {"output_volume_path"|"output_workspace_path", "files", "count"}`.
  Only when **neither** is given does `package` return the contents inline as
  `bundle = {"files": {relpath: text, …}, "truncated": [...]}` (small bundles only) for you to persist.
  Prefer an output path so the bundle lands durably and large bundles aren't capped. Skip the
  `"$PY" -m …` commands below.

- **venv CLI (local, no MCP server):** ensure the venv exists (`setup` Path B / `bootstrap.sh`), then
  run the commands below with the venv interpreter (from the marker file `<plugin_dir>/.migration-venv`)
  and `src/` on `PYTHONPATH` (use `$PY` anywhere a command shows `python3`):

  ```bash
  export PYTHONPATH="<plugin_dir>/src"
  PY="$(cat <plugin_dir>/.migration-venv)"
  "$PY" -m flowx.adapter package --output-dir <dir> --catalog <catalog> --schema <schema>
  ```

  If Python or pip is missing, `bootstrap.sh` prints a warning telling the user what to install —
  relay it and stop until they have Python 3.12+ and pip.

## Workflow

Follow these steps in order:

### Step 1 — Locate the translation report

The translate/modify phases left the stamped report at `<output_dir>/.work/translation_report.stamped.json`
(or `<output_dir>/.work/translation_report.json` when `modify` was not run). If the shared
`<output_dir>` is not in conversation context, ask the user:

> Which migration output directory should I build the bundle in? (default: `./flowx_output`)

`package` reads the report from `<output_dir>/.work/` automatically — you do not pass a report path.
Validate that a report exists there and that all required translations have status `translated`.

### Step 2 — Gather deployment parameters

Ask the user for the following (provide defaults):

| Parameter | Description | Default |
|---|---|---|
| Target catalog | Unity Catalog catalog for tables/volumes | `main` |
| Target schema | Schema within the catalog | `default` |
| Output directory | Shared migration dir; bundle + `metadata/` are written here | `./flowx_output` |
| Bundle name | Name for the DABs project | derived from first pipeline name |
| Target environments | Deployment targets to configure | `dev, staging, prod` |
| Warehouse ID | SQL warehouse for SQL tasks (optional) | prompt if SQL tasks exist |
| Databricks CLI profile | Profile used to download workspace-resident notebooks / JARs / Python files (`--profile`). Required only when the bundle references absolute workspace paths. | resolved from `~/.databrickscfg` (auto-prompt if multiple) |

### Step 2.5 — Detect workspace artifacts and authenticate

> **Databricks runtime (serverless / cluster):** Authentication is auto-configured
> from the notebook runtime context. The `workspace_downloader` module detects
> `DATABRICKS_RUNTIME_VERSION` in the environment and writes `~/.databrickscfg`
> from `dbruntime.databricks_repl_context` automatically. You can skip the
> interactive `databricks auth login` step — just pass `--profile DEFAULT` (or
> omit `--profile` entirely) and notebook downloading will work.

Before running the bundle writer, check whether the report references
absolute workspace paths (notebooks under `/Shared/`, SparkPython
files, SparkJar libraries) or DBFS paths that the bundle should
download to be self-contained:

```bash
"$PY" -m flowx.adapter workspace-paths \
  <output_dir>/.work/translation_report.stamped.json \
  --source-dir <adf_source_dir>
```

The command emits:

```json
{
  "paths": ["/Shared/team/notebook_a", "/Shared/team/notebook_b"],
  "suggested_hosts": ["https://adb-1234.5.azuredatabricks.net"],
  "needs_auth": true
}
```

When `needs_auth` is `true`:

1. Surface the suggested hosts to the user with `AskUserOption`.  Use
   the first `suggested_hosts` value as the default; allow the user to
   override.  When no host is suggested (no Databricks linked service
   in the export), prompt for the host with no default.
2. Run the interactive Databricks CLI login command and wait for it to
   complete:

   ```bash
   databricks auth login --host <host>
   ```

   This writes a profile into `~/.databrickscfg`.  When the user has
   chosen a specific profile name, append `--profile <name>` to both
   the login and the package invocation below.

3. Pass the resolved profile to step 3 via `--profile <name>` (default
   profile name is `DEFAULT`).  When `needs_auth` is `false` skip steps
   1–2 and omit `--profile` from step 3.

The `paths` list is informational; you can echo it to the user so they
know which notebooks the bundle will download.

### Step 3 — Run bundle generation

Execute the DAB writer:

```bash
# Unified runner (recommended): `"$PY" -m flowx.adapter package ...`
# forwards to dab_writer below.
"$PY" -m flowx.bundler.dab_writer \
  --output-dir <output_dir> \
  --catalog <catalog> \
  --schema <schema> \
  --bundle-name <bundle_name> \
  [--profile <databricks-cli-profile>] \
  [--no-download-workspace-files] \
  [--keep-intermediates]
```

Where:
- `<output_dir>` is the shared migration directory — `package` defaults `--report` to
  `<output_dir>/.work/translation_report.stamped.json` (falling back to the un-stamped report).
  Pass `--report <path>` only to override.
- Other parameters are from step 2
- After a successful build, `package` **prunes the transient `<output_dir>/.work/`** so the
  final tree contains only the bundle and the kept `metadata/` files. Pass `--keep-intermediates`
  to retain `.work/` for debugging.

**Workspace artifact downloading (default: enabled).** When the report references workspace-resident notebooks (`/Shared/...`), DBFS Spark JARs (`dbfs:/...`), or Spark Python files, the preparer downloads them via the Databricks CLI auth so the resulting bundle is self-contained and deployable across environments. Downloaded notebooks are downloaded under `src/notebooks/` and bound to the default `job_cluster` (since they may rely on classic-compute features). The original `notebook_path` in the resource YAML is rewritten to the bundle-relative path `../src/notebooks/<file>.py`.

If no Databricks CLI auth is detected on the host (`~/.databrickscfg` empty AND no `DATABRICKS_CONFIG_PROFILE` / `DATABRICKS_HOST`+`DATABRICKS_TOKEN` env vars), the CLI prints the workspace paths it was about to download and prompts:

```
Workspace downloads are enabled but no Databricks CLI auth was found.
  Looked for profiles in: /Users/<you>/.databrickscfg
  Artifacts to download: /Shared/ETL/transform, …

To authenticate, run one of:
  databricks auth login --host https://<your-workspace>.cloud.databricks.com
  databricks configure --token

Continue with placeholders (downloads will be skipped)? [y/N]:
```

Answering `n` aborts with exit code 2 so the user can authenticate and re-run. Answering `y` continues with placeholder notebooks (legacy in-place workspace paths). In non-interactive sessions the prompt defaults to placeholders.

Use `--no-download-workspace-files` to opt out entirely; the bundle then keeps original workspace paths exactly as in the IR.

### Step 4 — Present the generated file tree

Show the user what was generated:

```
<output_dir>/                         # the shared migration directory
  databricks.yml
  resources/
    etl_main_job.yml
    transform_dlt_pipeline.yml
  src/
    notebooks/
      copy_from_blob.py
      web_activity_call.py
    setup/
      create_volumes.py
      create_secrets.py
      register_connections.py
  SETUP.md
  metadata/                           # kept migration metadata (from discover + modify)
    inventory.json
    profile_report.csv
    <pipeline>.arm.json               # verbatim original ADF/ARM source
    configuration.json                # the collected configuration answers
  # .work/ (transient translation report + IR) is pruned after a successful build
```

### Step 5 — Explain setup tasks

If the `setup/` directory was generated, explain what each script does:

**create_volumes.py** — Creates Unity Catalog volumes required by the migrated jobs. These volumes replace Azure Blob Storage or ADLS references from ADF. Run this once per environment.

**create_secrets.py** — Creates Databricks secret scopes and secrets for connection credentials that were in ADF linked services. Review the secret values and populate them manually or via your secrets management system.

**register_connections.py** — Registers Unity Catalog connections for external data sources (SQL Server, REST APIs, etc.) that were referenced in ADF linked services.

Emphasize that the user should review these scripts before running them, especially `create_secrets.py` which will need actual credential values.

### Step 6 — Explain the generated bundle structure

Briefly describe:
- **databricks.yml** — The root bundle config with workspace, target environments (dev/staging/prod), and variable definitions. Variables are parameterized for environment-specific values (catalog, schema, warehouse).
- **resources/*.yml** — One YAML file per Databricks Lakeflow Job (one per ADF pipeline). Each job contains tasks mapped from ADF activities, with dependencies matching the original ADF dependency chains.
- **src/notebooks/*.py** — Python notebooks for activities that translate to notebook_task. These contain the actual data movement or transformation logic.
- **tests/*.py** — Skeleton test files for validating the migrated jobs.

### Step 7 — Suggest next steps

Present the following next steps:

```
Next Steps
==========
1. Review generated files:
   cd <output_dir>
   cat databricks.yml

2. Validate the bundle:
   databricks bundle validate --target dev

3. Run setup scripts (if generated):
   databricks bundle run setup_volumes --target dev

4. Deploy to dev:
   databricks bundle deploy --target dev

5. Test the deployed jobs:
   databricks bundle run <job_name> --target dev

6. Promote to staging/prod:
   databricks bundle deploy --target staging
   databricks bundle deploy --target prod
```

Recommend running `databricks bundle validate` first to catch any configuration issues before deployment.

### Step 8 — (Optional) Persist coverage results and install a dashboard

This step only applies when running with workspace auth (Genie Code, or a configured
Databricks CLI profile). The `inputs package` options surface three optional prompts:
`results_table`, `results_warehouse_id`, and `install_dashboard`.

**Persist results.** When the user provides a `results_table` (a UC `catalog.schema.table`),
write one migration-coverage row **per pipeline** for this run:

```bash
"$PY" -m flowx.adapter record-results \
  --output-dir <output_dir> \
  --results-table <catalog.schema.table> \
  [--warehouse-id <sql_warehouse_id>]
```

It reads `<output_dir>/metadata/{inventory.json, profile_report.csv}`, creates the table if
needed, and inserts a row per pipeline with the activity/dataset/linked-service counts,
collapsible-pattern count, complexity size, and the deterministic/agentic/unsupported coverage
breakdown. Every row is stamped with a shared **`run_id`** (UUID for this run),
**`run_date`** (`CURRENT_TIMESTAMP()`), and **`run_by`** (`CURRENT_USER()`). The warehouse is
auto-detected (prefers a running serverless warehouse) when `--warehouse-id` is omitted. The
command prints the `run_id` and row count.

**Install the dashboard.** When the user answers `install_dashboard = yes`, create and publish
an AI/BI (Lakeview) coverage dashboard over that table:

```bash
"$PY" -m flowx.adapter install-dashboard \
  --results-table <catalog.schema.table> \
  [--warehouse-id <sql_warehouse_id>] \
  [--dashboard-name "<name>"] [--parent-path "/Workspace/Users/<you>"]
```

It builds the dashboard from a template (KPI counters for pipelines / coverage % /
deterministic-agentic-unsupported activity totals, a complexity-size bar chart, a
coverage-over-runs line, and a per-pipeline coverage table), publishes it, and prints the URL.
Both commands degrade gracefully with an actionable message when workspace auth or a warehouse
is unavailable.

## Examples

- "Package the bundles"
- "Generate DABs for the translated pipelines"
- "Create deployment bundles targeting catalog 'analytics' and schema 'bronze'"
- "Build the DABs project in ./output/my_migration/"

## Output Artifacts

All under the shared `<output_dir>`:

| File | Description |
|---|---|
| `databricks.yml` | Root bundle configuration |
| `resources/*.yml` | Job and pipeline YAML definitions |
| `src/notebooks/*.py` | Generated notebooks |
| `src/setup/*.py` | Infrastructure setup scripts |
| `SETUP.md` | Human-readable setup instructions |
| `metadata/inventory.json` | Activity inventory (from discover) |
| `metadata/profile_report.csv` | Per-pipeline complexity report (from profile) |
| `metadata/<pipeline>.arm.json` | Verbatim original ADF/ARM source (from discover) |
| `metadata/configuration.json` | Collected configuration answers (from modify) |

> **Notification destinations.** When a `activity_and_notify` motif was opted into a Slack/Teams/PagerDuty/Generic-Webhook destination, the destination is created (or reused by display name) via the SDK at **prompt time** (the `modify` phase), and its resolved id is carried in the report; package simply wires that id into the task's `webhook_notifications`. If the report has no pre-resolved id (creation was deferred or failed earlier), package retries the create; failing that — e.g. no workspace auth — a `notification_destination` setup task is emitted in SETUP.md instead and the task ships without notifications. Email destinations use raw `email_notifications` and never create an SDK destination.
