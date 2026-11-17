# Setup for bundle `motif_metadata_bulk_copy`

Complete every step below before running the bundle. Deployment itself is **not** listed — it is the step that comes *after* everything here.

## Secret scopes and values

The generated notebooks read credentials via `dbutils.secrets.get(...)`. You have two equivalent ways to provision the scopes and keys:

**Option A — run `src/setup/create_secrets.py`** in the target workspace. It creates every scope and populates each key with a `PLACEHOLDER` value that you then replace with a real credential.

**Option B — use the CLI directly:**

```bash
databricks secrets create-scope lookup-metadata
databricks secrets put-secret lookup-metadata jdbc-password
databricks secrets put-secret lookup-metadata jdbc-url
databricks secrets put-secret lookup-metadata jdbc-user
databricks secrets create-scope motif_metadata_driven_bulk_copy
databricks secrets put-secret motif_metadata_driven_bulk_copy jdbc-password
databricks secrets put-secret motif_metadata_driven_bulk_copy jdbc-url
databricks secrets put-secret motif_metadata_driven_bulk_copy jdbc-user
```

`put-secret` opens an editor by default; pass `--json '{"string_value": "…"}'` for a non-interactive flow.

## Networking

The generated notebooks reach the following endpoints. Confirm the workspace's network profile permits each one before running the bundle. Private endpoints, VNet peering, or storage credentials may be required.

| Type | Target | Notes |
|---|---|---|
| Database (JDBC) | `lookup-metadata` | JDBC database read by the generated notebook. Confirm the workspace has network reach to the database (VNet peering, private endpoint, or firewall allowlist) before running the job. |
| Database (JDBC) | `motif_metadata_driven_bulk_copy` | JDBC database read by the generated notebook. Confirm the workspace has network reach to the database (VNet peering, private endpoint, or firewall allowlist) before running the job. |
