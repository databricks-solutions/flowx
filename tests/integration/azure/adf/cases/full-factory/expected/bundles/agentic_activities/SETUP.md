# Setup for bundle `agentic_activities`

Complete every step below before running the bundle. Deployment itself is **not** listed — it is the step that comes *after* everything here.

## Notebooks to author

The following notebooks are stubs that raise `NotImplementedError`. flowx could not download the source (either no workspace path was supplied in ADF, or the path did not resolve against the authenticated workspace). Replace each stub with the real logic.

| Task | Workspace path (ADF) | Stub in bundle | Widgets available |
|---|---|---|---|
| `azure-function` | *(none)* | `src/notebooks/azure-function.py` | *(none)* |
| `azure-ml` | *(none)* | `src/notebooks/azure-ml.py` | *(none)* |
| `custom` | *(none)* | `src/notebooks/custom.py` | *(none)* |
| `data-flow` | *(none)* | `src/notebooks/data-flow.py` | *(none)* |
| `fail` | *(none)* | `src/notebooks/fail.py` | *(none)* |
| `get-metadata` | *(none)* | `src/notebooks/get-metadata.py` | *(none)* |
| `script` | *(none)* | `src/notebooks/script.py` | *(none)* |
| `ssis` | *(none)* | `src/notebooks/ssis.py` | *(none)* |
| `stored-procedure` | *(none)* | `src/notebooks/stored-procedure.py` | *(none)* |
| `until` | *(none)* | `src/notebooks/until.py` | *(none)* |
| `validation` | *(none)* | `src/notebooks/validation.py` | *(none)* |
| `webhook` | *(none)* | `src/notebooks/webhook.py` | *(none)* |

If the workspace path exists in a reachable Databricks workspace, you can have flowx re-ingest it by running `databricks workspace export` and placing the result at the indicated bundle path.

## Manual credential setup

The cluster compute backing the tasks below was authenticated in ADF via a managed identity / CredentialReference that has no direct Databricks equivalent.  flowx defaulted the bundle's default_cluster to `single_user_name: ${workspace.current_user.userName}` so deployment works for the deploying user, but production runs should swap that for a service principal.

| Source | Linked service | ADF authentication | Suggested Databricks setup |
|---|---|---|---|
| `agentic-activities` | `<MSI>` | `Anonymous` | ADF cluster auth has no Databricks equivalent.  The default_cluster runs as ${workspace.current_user.userName}; swap to a service principal via single_user_name or set run_as.service_principal_name on the job. |
