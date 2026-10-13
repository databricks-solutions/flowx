# Setup for bundle `complex_orchestration`

Complete every step below before running the bundle. Deployment itself is **not** listed — it is the step that comes *after* everything here.

## Notebooks to author

The following notebooks are stubs that raise `NotImplementedError`. flowx could not download the source (either no workspace path was supplied in ADF, or the path did not resolve against the authenticated workspace). Replace each stub with the real logic.

| Task | Workspace path (ADF) | Stub in bundle | Widgets available |
|---|---|---|---|
| `mixed-script` | *(none)* | `src/notebooks/mixed-script.py` | *(none)* |

If the workspace path exists in a reachable Databricks workspace, you can have flowx re-ingest it by running `databricks workspace export` and placing the result at the indicated bundle path.

## Cross-bundle job references

Each row below describes a `run_job_task` that invokes a job **not** defined in this bundle. flowx replaced each external resource reference with a declared bundle variable (`${var.<name>}`). Before validating, deploying, or running the bundle, populate the variable with the numeric job ID the target pipeline was deployed under — either set a `default:` in `databricks.yml` or pass `--var "<name>=<job_id>"` to the bundle command.

| Variable | Target pipeline |
|---|---|
| `child_pipeline_job_id` | `child_pipeline` |
