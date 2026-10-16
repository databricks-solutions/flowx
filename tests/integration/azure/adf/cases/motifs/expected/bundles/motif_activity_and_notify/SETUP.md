# Setup for bundle `motif_activity_and_notify`

Complete every step below before running the bundle. Deployment itself is **not** listed — it is the step that comes *after* everything here.

## Unresolved task parameters

The translator left the base_parameters below with empty-string defaults — either the source ADF activity carried a value that couldn't be resolved, or it depended on pipeline state (variables, activity outputs) that doesn't cross the bundle boundary.  Review each entry and either set a real default in the job YAML or pass a value via `databricks bundle run ... --params '{<widget>:<value>}'`.

| Task | Widget |
|---|---|
| `notify` | `body` |

## Networking

The generated notebooks reach the following endpoints. Confirm the workspace's network profile permits each one before running the bundle. Private endpoints, VNet peering, or storage credentials may be required.

| Type | Target | Notes |
|---|---|---|
| HTTP/S | `https://example.invalid/` | HTTP/S endpoint reached via `requests`. Verify outbound HTTPS is allowed. |
