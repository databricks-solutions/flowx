# Setup for bundle `motif_rest_api_pagination`

Complete every step below before running the bundle. Deployment itself is **not** listed — it is the step that comes *after* everything here.

## Secret scopes and values

The generated notebooks read credentials via `dbutils.secrets.get(...)`. You have two equivalent ways to provision the scopes and keys:

**Option A — run `src/setup/create_secrets.py`** in the target workspace. It creates every scope and populates each key with a `PLACEHOLDER` value that you then replace with a real credential.

**Option B — use the CLI directly:**

```bash
databricks secrets create-scope rest_api
databricks secrets put-secret rest_api token
```

`put-secret` opens an editor by default; pass `--json '{"string_value": "…"}'` for a non-interactive flow.

## Unresolved task parameters

The translator left the base_parameters below with empty-string defaults — either the source ADF activity carried a value that couldn't be resolved, or it depended on pipeline state (variables, activity outputs) that doesn't cross the bundle boundary.  Review each entry and either set a real default in the job YAML or pass a value via `databricks bundle run ... --params '{<widget>:<value>}'`.

| Task | Widget |
|---|---|
| `motif_rest_api_pagination` | `api_url` |
| `motif_rest_api_pagination` | `target_table` |
