# Setup for bundle `motif_incremental_watermark`

Complete every step below before running the bundle. Deployment itself is **not** listed — it is the step that comes *after* everything here.

## Unresolved task parameters

The translator left the base_parameters below with empty-string defaults — either the source ADF activity carried a value that couldn't be resolved, or it depended on pipeline state (variables, activity outputs) that doesn't cross the bundle boundary.  Review each entry and either set a real default in the job YAML or pass a value via `databricks bundle run ... --params '{<widget>:<value>}'`.

| Task | Widget |
|---|---|
| `motif_incremental_load_watermark` | `source_path` |
| `motif_incremental_load_watermark` | `target_table` |
