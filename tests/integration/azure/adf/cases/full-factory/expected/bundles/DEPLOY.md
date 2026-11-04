# Deploy

This migration produced **8 bundle(s)** with packaging mode `per-pipeline`.

## Suggested deploy order

Deploy callees before their callers so each caller's `${var.<callee>}` job-id reference can be resolved. The automated deployer does this for you (see below); this order is for manual deploys.

1. `agentic_activities/`
2. `child_pipeline/`
3. `all_activity_types/` — depends on: child_pipeline
4. `complex_orchestration/` — depends on: child_pipeline
5. `motif_activity_and_notify/`
6. `motif_incremental_watermark/`
7. `motif_metadata_bulk_copy/`
8. `motif_rest_api_pagination/`

## Bundles

### `agentic_activities/`

Pipelines: agentic_activities

### `child_pipeline/`

Pipelines: child_pipeline

### `all_activity_types/`

Pipelines: all_activity_types

Cross-bundle dependencies (`${var.…}`): child_pipeline

### `complex_orchestration/`

Pipelines: complex_orchestration

Cross-bundle dependencies (`${var.…}`): child_pipeline

### `motif_activity_and_notify/`

Pipelines: motif_activity_and_notify

### `motif_incremental_watermark/`

Pipelines: motif_incremental_watermark

### `motif_metadata_bulk_copy/`

Pipelines: motif_metadata_bulk_copy

### `motif_rest_api_pagination/`

Pipelines: motif_rest_api_pagination

## Automated ordered deploy

Rather than deploying each bundle by hand, use the ordered deployer, which discovers these bundles, deploys callees first, and injects each callee's deployed job id into its callers automatically:

```bash
python -m flowx.adapter deploy --output-dir . --target dev
```

> Local CLI only — `databricks bundle deploy`/`summary` are unavailable on Databricks serverless / Genie Code. Run this from a local CLI session or the web terminal.
