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

This is phase 3 of the flowx migration workflow. It consumes the `translation_report.json` produced by the `translate` skill and generates a complete DABs project that can be validated and deployed with the Databricks CLI.

The output is a standard DABs project with:
- `databricks.yml` — the bundle configuration
- `resources/` — job and pipeline YAML definitions
- `src/notebooks/` — generated and helper notebooks
- `setup/` — infrastructure setup scripts (volumes, secrets, connections)

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

### Step 3 — Run bundle generation

Execute the DAB writer:

```bash
python3 <plugin_dir>/src/flowx/bundler/dab_writer.py \
  --report <translation_report_path> \
  --output-dir <output_dir> \
  --catalog <catalog> \
  --schema <schema> \
  --bundle-name <bundle_name> \
  --targets <targets>
```

Where:
- `<plugin_dir>` is the root of the flowx plugin
- `<translation_report_path>` is the path to `translation_report.json`
- Other parameters are from step 2

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
