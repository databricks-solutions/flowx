# Setup for bundle `child_pipeline`

Complete every step below before running the bundle. Deployment itself is **not** listed — it is the step that comes *after* everything here.

## Manual credential setup

The cluster compute backing the tasks below was authenticated in ADF via a managed identity / CredentialReference that has no direct Databricks equivalent.  flowx defaulted the bundle's default_cluster to `single_user_name: ${workspace.current_user.userName}` so deployment works for the deploying user, but production runs should swap that for a service principal.

| Source | Linked service | ADF authentication | Suggested Databricks setup |
|---|---|---|---|
| `child-pipeline` | `<MSI>` | `MSI` | ADF cluster auth has no Databricks equivalent.  The default_cluster runs as ${workspace.current_user.userName}; swap to a service principal via single_user_name or set run_as.service_principal_name on the job. |
