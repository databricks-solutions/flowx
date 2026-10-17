# Deploy

This migration produced **1 bundle(s)** with packaging mode `per-pipeline`.

All pipelines are packaged into a **single bundle** at the migration output root. There is no cross-bundle ordering *within* this migration, so deploy it directly:

```bash
databricks bundle validate -t dev
databricks bundle deploy -t dev
```

Pipelines in this bundle: agentic_activities.

> **Note:** if this bundle references a pipeline outside the migration, that reference is a `${var.<callee>_job_id}` bundle variable with no default — `databricks bundle deploy` fails on the unset variable, and `flowx deploy` errors with `MissingDependencyError` unless you pass `--allow-missing-deps`. See `SETUP.md` to supply the job id(s).
