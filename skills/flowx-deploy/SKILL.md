---
name: flowx-deploy
description: >
  Deploy the per-pipeline Databricks Asset Bundles from a multi-pipeline flowx
  migration in dependency order, resolving cross-bundle job ids automatically.
  Local CLI only.
triggers:
  - "deploy bundles"
  - "deploy in dependency order"
  - "ordered deploy"
  - "deploy flowx bundles"
  - "deploy multi pipeline migration"
---

# Deploy per-pipeline flowx bundles in dependency order

Deploy every bundle produced by a multi-pipeline migration, in the right order, wiring cross-bundle
`ExecutePipeline` references automatically.

## Context

flowx emits **one bundle per ADF pipeline** under the output directory (`<output_dir>/<pipeline>/`).
When pipeline A calls pipeline B via `ExecutePipeline`, the generated `run_job_task` in A's bundle
references B — a job that lives in B's *own* bundle. flowx rewrites that out-of-bundle reference to
`${var.<B>}` and declares a matching bundle variable, so each bundle is deploy-valid on its own; but
the operator otherwise has to find B's numeric job id and pass it to A by hand.

The package phase writes a top-level `DEPLOY.md` describing the bundle layout, cross-bundle
dependencies, and the suggested callees-first deploy order. This skill is the **automated** form of
those instructions — read `DEPLOY.md` for the human-readable version.

This skill automates that:

1. Discovers the bundles under the output directory (any immediate subdirectory with a
   `databricks.yml`) — no manifest needed.
2. Reads each bundle's job resource keys and its `${var.<callee>}` cross-bundle dependencies straight
   from the generated `resources/*.yml`.
3. Topologically sorts them (callees first) — a cyclic call graph is rejected with a clear error.
4. Deploys each bundle with `databricks bundle deploy`.
5. After each deploy, reads the deployed job id from `databricks bundle summary -o json` and injects
   it into callers via `--var "<callee>=<id>"`.

Because it captures and injects the **numeric job id** (not a name), dev-mode `[dev <user>]` job-name
prefixes are irrelevant — it works identically for `dev` and `prod` targets.

## Prerequisites

- An output directory with the per-pipeline bundle subdirectories (from a multi-pipeline migration).
- A working local `databricks` CLI with a configured profile / auth for the target workspace.

> **Not available on Databricks serverless / Genie Code.** `databricks bundle deploy` and
> `bundle summary` do not run on serverless compute, so this is a **local venv-CLI** (or web-terminal)
> step only.

## How to run

Use the venv interpreter from the marker file (`<plugin_dir>/.migration-venv`) with `src/` on
`PYTHONPATH`:

```bash
export PYTHONPATH="<plugin_dir>/src"
PY="$(cat <plugin_dir>/.migration-venv)"

# 1. Preview the deploy order and per-bundle commands without deploying:
"$PY" -m flowx.adapter deploy --output-dir <output_dir> --target dev --dry-run

# 2. Deploy for real:
"$PY" -m flowx.adapter deploy --output-dir <output_dir> --target dev [--profile <profile>]
```

Flags:

- `--output-dir` — directory holding the per-pipeline bundle subdirectories (default `./flowx_output`).
- `--target` — bundle target to deploy (default `dev`).
- `--profile` — Databricks CLI profile used for both `deploy` and `summary`.
- `--dry-run` — print the dependency order and each `databricks bundle deploy …` command (with
  `<callee>=<captured at deploy time>` placeholders), without deploying.
- `--allow-missing-deps` — continue when a bundle references a callee that isn't present under the
  output dir; that dependency's `--var` is skipped and must be set manually (see the bundle's
  `SETUP.md`). Without this flag, a missing dependency is a hard error.

## Behavior and failure handling

- **Ordering:** callees always deploy before their callers. The order is deterministic.
- **Deploy failure:** if any bundle's `databricks bundle deploy` fails, deployment stops immediately;
  dependents are not deployed. The failing bundle and its stderr are printed.
- **Job-id capture:** ids are read from `bundle summary -o json` at `.resources.jobs.<key>.id`. A
  resource without a deployed job id (e.g. a Lakeflow pipeline resource) is skipped — no empty `--var`.
- **Cycles:** a cyclic call graph cannot be ordered; the command errors out.
