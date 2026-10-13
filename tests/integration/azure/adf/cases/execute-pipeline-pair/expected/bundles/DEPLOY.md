# Deploy

This migration produced **2 bundle(s)** with packaging mode `per-pipeline`.

## Suggested deploy order

Deploy callees before their callers so each caller's `${var.<callee>}` job-id reference can be resolved. The automated deployer does this for you (see below); this order is for manual deploys.

1. `child_pipeline/`
2. `complex_orchestration/` — depends on: child_pipeline

## Bundles

### `child_pipeline/`

Pipelines: child_pipeline

### `complex_orchestration/`

Pipelines: complex_orchestration

Cross-bundle dependencies (`${var.…}`): child_pipeline

## Automated ordered deploy

Rather than deploying each bundle by hand, use the ordered deployer, which discovers these bundles, deploys callees first, and injects each callee's deployed job id into its callers automatically:

```bash
python -m flowx.adapter deploy --output-dir . --target dev
```

> Local CLI only — `databricks bundle deploy`/`summary` are unavailable on Databricks serverless / Genie Code. Run this from a local CLI session or the web terminal.
