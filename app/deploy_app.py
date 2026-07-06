# Databricks notebook source
# MAGIC %md
# MAGIC # Deploy the flowx MCP server (SDK, no CLI)
# MAGIC
# MAGIC Deploys the flowx MCP server as a Databricks App using the **Python SDK** only, so it runs
# MAGIC directly from a **serverless** Databricks / Genie Code session — where `app/deploy.sh` cannot,
# MAGIC because `databricks apps deploy` / `databricks sync` require a CLI session (web terminal or a
# MAGIC local machine).
# MAGIC
# MAGIC The deployment logic lives in `deploy_helpers.py` (imported below) so it stays importable and
# MAGIC unit-testable. Because the bundle is uploaded through the **Workspace API** (`ImportFormat.RAW`),
# MAGIC there is no `databricks sync` and none of its footguns: no `.gitignore` dropping staged files,
# MAGIC and no stray `databricks.yml` making the CLI abort with "please specify target".

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration
# MAGIC
# MAGIC - **repo_root** *(required)* — the flowx checkout: the directory containing `app/` and
# MAGIC   `src/flowx` (e.g. `/Workspace/Shared/flowx`).
# MAGIC - **app_name** — the app to create/deploy (default `mcp-flowx`; the `mcp-` prefix auto-lists it
# MAGIC   in the AI Playground).
# MAGIC - **source_code_path** — workspace dir the app deploys from. Must be readable by the app's
# MAGIC   service principal, so it defaults under `/Workspace/Shared` — **not** a private
# MAGIC   `/Workspace/Users/<you>` home, which the app SP cannot read.

# COMMAND ----------

dbutils.widgets.text("repo_root", "/Workspace/Shared/flowx", "flowx repo root (contains app/ and src/flowx)")
dbutils.widgets.text("app_name", "mcp-flowx", "App name")
dbutils.widgets.text("source_code_path", "", "Workspace source path (blank = /Workspace/Shared/<app_name>)")

repo_root = dbutils.widgets.get("repo_root").strip()
app_name = dbutils.widgets.get("app_name").strip()
source_code_path = dbutils.widgets.get("source_code_path").strip() or f"/Workspace/Shared/{app_name}"
app_description = "flowx MCP server — ADF to Databricks Lakeflow migration tools"

# COMMAND ----------

import os
import sys

# Make the sibling deploy_helpers module importable from the user-specified checkout.
sys.path.insert(0, os.path.join(repo_root, "app"))

from databricks.sdk import WorkspaceClient

import deploy_helpers

deploy_helpers.validate_repo_root(repo_root)
workspace_client = WorkspaceClient()

print(f"repo_root        : {repo_root}")
print(f"app_name         : {app_name}")
print(f"source_code_path : {source_code_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Stage the self-contained bundle at the workspace source path
# MAGIC
# MAGIC Uploads the app files and a fresh copy of the `flowx` package to `source_code_path`.
# MAGIC `ImportFormat.RAW` lands each file verbatim (not as a notebook) and `overwrite=True` makes
# MAGIC redeploys idempotent.

# COMMAND ----------

flowx_module_count = deploy_helpers.stage_app_bundle(workspace_client, repo_root, source_code_path)
print(f"Staged app files + {flowx_module_count} flowx module files to {source_code_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create the app if needed, then deploy
# MAGIC
# MAGIC `create_and_wait` / `deploy_and_wait` block until the operation reaches a terminal state
# MAGIC (default 20-minute timeout each).

# COMMAND ----------

was_created = deploy_helpers.ensure_app_exists(workspace_client, app_name, app_description)
print(f"App '{app_name}' {'created' if was_created else 'already existed — reusing it'}.")

print(f"Deploying '{app_name}' from {source_code_path} ...")
deployment = deploy_helpers.deploy_app_source(workspace_client, app_name, source_code_path)

deployed_app = workspace_client.apps.get(name=app_name)
app_url = deployed_app.url or ""
deployment_state = deployment.status.state if deployment.status else "unknown"
print(f"Deployment status: {deployment_state}")
print(f"App URL          : {app_url or '(pending — re-run workspace_client.apps.get)'}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next steps to use it in Genie Code

# COMMAND ----------

print(f"""
Deployed app: {app_name}
MCP endpoint: {app_url or '<app-url>'}/mcp
Health check: {app_url or '<app-url>'}/

Next steps:
  1. App access — grant 'Can use' on '{app_name}' to the users / service principals that will
     call it (Apps UI > Permissions).
  2. Data access — grant the app's service principal read/write on the catalogs, schemas, and
     UC volumes the migration touches (and any SQL warehouse used by the reporting tools).
  3. Add it in Genie Code (Agent mode): Settings > MCP Servers > Add Server > Custom MCP server
     > select '{app_name}' > Save. The single 'flowx' tool appears immediately.
  4. If a browser CORS error appears, set the app env var FLOWX_ALLOWED_ORIGINS to your
     workspace URL and redeploy (re-run this notebook).
""")
