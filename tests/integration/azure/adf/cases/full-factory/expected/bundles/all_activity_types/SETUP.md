# Setup for bundle `all_activity_types`

Complete every step below before running the bundle. Deployment itself is **not** listed — it is the step that comes *after* everything here.

## Secret scopes and values

The generated notebooks read credentials via `dbutils.secrets.get(...)`. You have two equivalent ways to provision the scopes and keys:

**Option A — run `src/setup/create_secrets.py`** in the target workspace. It creates every scope and populates each key with a `PLACEHOLDER` value that you then replace with a real credential.

**Option B — use the CLI directly:**

```bash
databricks secrets create-scope all-activity-types
databricks secrets put-secret all-activity-types jdbc-password
databricks secrets put-secret all-activity-types jdbc-url
databricks secrets put-secret all-activity-types jdbc-user
```

`put-secret` opens an editor by default; pass `--json '{"string_value": "…"}'` for a non-interactive flow.

## Notebooks to author

The following notebooks are stubs that raise `NotImplementedError`. flowx could not download the source (either no workspace path was supplied in ADF, or the path did not resolve against the authenticated workspace). Replace each stub with the real logic.

| Task | Workspace path (ADF) | Stub in bundle | Widgets available |
|---|---|---|---|
| `scripts/integration.py` | *(none)* | `src/scripts/integration.py` | *(none)* |

If the workspace path exists in a reachable Databricks workspace, you can have flowx re-ingest it by running `databricks workspace export` and placing the result at the indicated bundle path.

## Cross-bundle job references

Each row below describes a `run_job_task` that invokes a job **not** defined in this bundle. flowx replaced each external resource reference with a declared bundle variable (`${var.<name>}`). Before validating, deploying, or running the bundle, populate the variable with the numeric job ID the target pipeline was deployed under — either set a `default:` in `databricks.yml` or pass `--var "<name>=<job_id>"` to the bundle command.

| Variable | Target pipeline |
|---|---|
| `child_pipeline_job_id` | `child_pipeline` |

## Unresolved task parameters

The translator left the base_parameters below with empty-string defaults — either the source ADF activity carried a value that couldn't be resolved, or it depended on pipeline state (variables, activity outputs) that doesn't cross the bundle boundary.  Review each entry and either set a real default in the job YAML or pass a value via `databricks bundle run ... --params '{<widget>:<value>}'`.

| Task | Widget |
|---|---|
| `append-variable` | `source_task_key` |
| `copy` | `source_path` |
| `copy` | `target_table` |
| `delete` | `dataset_name` |
| `delete` | `folder_path` |

## Manual credential setup

The cluster compute backing the tasks below was authenticated in ADF via a managed identity / CredentialReference that has no direct Databricks equivalent.  flowx defaulted the bundle's default_cluster to `single_user_name: ${workspace.current_user.userName}` so deployment works for the deploying user, but production runs should swap that for a service principal.

| Source | Linked service | ADF authentication | Suggested Databricks setup |
|---|---|---|---|
| `all-activity-types` | `<MSI>` | `MSI` | ADF cluster auth has no Databricks equivalent.  The default_cluster runs as ${workspace.current_user.userName}; swap to a service principal via single_user_name or set run_as.service_principal_name on the job. |

## Networking

The generated notebooks reach the following endpoints. Confirm the workspace's network profile permits each one before running the bundle. Private endpoints, VNet peering, or storage credentials may be required.

| Type | Target | Notes |
|---|---|---|
| HTTP/S | `https://example.invalid/` | HTTP/S endpoint reached via `requests`. Verify outbound HTTPS is allowed. |
| Database (JDBC) | `all-activity-types` | JDBC database read by the generated notebook. Confirm the workspace has network reach to the database (VNet peering, private endpoint, or firewall allowlist) before running the job. |
