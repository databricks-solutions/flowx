---
name: prepare
description: >
  Generate Databricks Declarative Automation Bundles (DABs) from translated IR,
  including job definitions, notebooks, and setup scripts.
triggers:
  - "prepare bundles"
  - "generate DABs"
  - "create bundles"
  - "prepare deployment"
  - "generate bundles"
  - "build DABs"
---

# Prepare Databricks Declarative Automation Bundles

Generate deployment-ready Databricks Declarative Automation Bundles (DABs) from the translated intermediate representation, including job definitions, notebooks, and infrastructure setup scripts.

## Context

This is phase 3 of the orchestra migration workflow. It consumes the `translation_report.json` produced by the `translate` skill and generates a complete DABs project that can be validated and deployed with the Databricks CLI.

The output is a standard DABs project with:
- `databricks.yml` — the bundle configuration
- `resources/` — job and pipeline YAML definitions
- `src/notebooks/` — generated and helper notebooks
- `setup/` — infrastructure setup scripts (volumes, secrets, connections)

## Prerequisite — Python environment

This skill runs the plugin's Python code, which depends on third-party packages. Before running
any Python commands, ensure the plugin's virtual environment is bootstrapped. Run the
**`setup`** skill, or directly:

```bash
bash <plugin_dir>/scripts/bootstrap.sh
```

This creates `<plugin_dir>/.venv` and installs dependencies from `requirements.txt`. If Python or 
pip is missing, the script prints a warning telling the user what to install — relay it and stop
until they have installed Python 3.12+ and pip.

Run **every** Python command in this skill with the venv interpreter and `src/` on `PYTHONPATH` 
(use it anywhere a command below shows `python3`):

```bash
export PYTHONPATH="<plugin_dir>/src"
"<plugin_dir>/.venv/bin/python" <plugin_dir>/src/orchestra/parser/adf_loader.py ...
```

## Workflow

Follow these steps in order:

### Step 1 — Locate the translation report

Read `translation_report.json` from the translate phase. If the path is not in conversation context, ask the user:

> Where is the translation_report.json from the translate phase? (default: `./orchestra_output/translate/translation_report.json`)

Validate the file exists and all required translations have status `translated`.

### Step 2 — Gather deployment parameters

Ask the user for the following (provide defaults):

| Parameter | Description | Default |
|---|---|---|
| Target catalog | Unity Catalog catalog for tables/volumes | `main` |
| Target schema | Schema within the catalog | `default` |
| Output directory | Where to write the DABs project | `./dab_output/` |
| Bundle name | Name for the DABs project | derived from first pipeline name |
| Target environments | Deployment targets to configure | `dev, staging, prod` |
| Warehouse ID | SQL warehouse for SQL tasks (optional) | prompt if SQL tasks exist |
| Databricks CLI profile | Profile used to download workspace-resident notebooks / JARs / Python files (`--profile`). Required only when the bundle references absolute workspace paths. | resolved from `~/.databrickscfg` (auto-prompt if multiple) |

### Step 2.5 — Detect workspace artifacts and authenticate

Before running the bundle writer, check whether the report references
absolute workspace paths (notebooks under `/Shared/`, SparkPython
files, SparkJar libraries) or DBFS paths that the bundle should
download to be self-contained:

```bash
python3 -m orchestra.adapter workspace-paths \
  <translation_report_path> \
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

1. Surface the suggested hosts to the user with `AskUserQuestion`.  Use
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
   the login and the prepare invocation below.

3. Pass the resolved profile to step 3 via `--profile <name>` (default
   profile name is `DEFAULT`).  When `needs_auth` is `false` skip steps
   1–2 and omit `--profile` from step 3.

The `paths` list is informational; you can echo it to the user so they
know which notebooks the bundle will vendor.

### Step 3 — Run bundle generation

Execute the DAB writer:

```bash
python3 <plugin_dir>/src/orchestra/bundler/dab_writer.py \
  --report <translation_report_path> \
  --output-dir <output_dir> \
  --catalog <catalog> \
  --schema <schema> \
  --bundle-name <bundle_name> \
  [--profile <databricks-cli-profile>] \
  [--no-vendor-workspace-files]
```

Where:
- `<plugin_dir>` is the root of the orchestra plugin
- `<translation_report_path>` is the path to `translation_report.json`
- Other parameters are from step 2

**Workspace artifact vendoring (default: enabled).** When the report references workspace-resident notebooks (`/Shared/...`), DBFS Spark JARs (`dbfs:/...`), or Spark Python files, the preparer downloads them via the Databricks CLI auth so the resulting bundle is self-contained and deployable across environments. Downloaded notebooks are vendored under `src/notebooks/` and bound to the default `job_cluster` (since they may rely on classic-compute features). The original `notebook_path` in the resource YAML is rewritten to the bundle-relative path `../src/notebooks/<file>.py`.

If no Databricks CLI auth is detected on the host (`~/.databrickscfg` empty AND no `DATABRICKS_CONFIG_PROFILE` / `DATABRICKS_HOST`+`DATABRICKS_TOKEN` env vars), the CLI prints the workspace paths it was about to download and prompts:

```
Workspace downloads are enabled but no Databricks CLI auth was found.
  Looked for profiles in: /Users/<you>/.databrickscfg
  Artifacts to vendor: /Shared/ETL/transform, …

To authenticate, run one of:
  databricks auth login --host https://<your-workspace>.cloud.databricks.com
  databricks configure --token

Continue with placeholders (downloads will be skipped)? [y/N]:
```

Answering `n` aborts with exit code 2 so the user can authenticate and re-run. Answering `y` continues with placeholder notebooks (legacy in-place workspace paths). In non-interactive sessions the prompt defaults to placeholders.

Use `--no-vendor-workspace-files` to opt out entirely; the bundle then keeps original workspace paths exactly as in the IR.

### Step 4 — Present the generated file tree

Show the user what was generated:

```
dab_output/
  databricks.yml
  resources/
    etl_main_job.yml
    etl_secondary_job.yml
    transform_dlt_pipeline.yml
  src/
    notebooks/
      copy_from_blob.py
      lookup_config.py
      web_activity_call.py
      set_variable_helper.py
  setup/
    create_volumes.py
    create_secrets.py
    register_connections.py
  tests/
    test_etl_main.py
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

## Examples

- "Prepare the bundles"
- "Generate DABs for the translated pipelines"
- "Create deployment bundles targeting catalog 'analytics' and schema 'bronze'"
- "Build the DABs project in ./output/my_migration/"

## Output Artifacts

| File | Description |
|---|---|
| `databricks.yml` | Root bundle configuration |
| `resources/*.yml` | Job and pipeline YAML definitions |
| `src/notebooks/*.py` | Generated notebooks |
| `setup/*.py` | Infrastructure setup scripts |
| `tests/*.py` | Skeleton test files |
