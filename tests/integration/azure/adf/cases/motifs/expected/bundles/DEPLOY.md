# Deploy

This migration produced **4 bundle(s)** with packaging mode `per-pipeline`.

## Suggested deploy order

Deploy callees before their callers so each caller's `${var.<callee>}` job-id reference can be resolved. The automated deployer does this for you (see below); this order is for manual deploys.

1. `motif_activity_and_notify/`
2. `motif_incremental_watermark/`
3. `motif_metadata_bulk_copy/`
4. `motif_rest_api_pagination/`

## Bundles

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
