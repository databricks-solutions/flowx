---
name: flowx-deploy
description: >
  Deploy the per-pipeline Declarative Automation Bundles from a multi-pipeline flowx
  migration in dependency order, resolving cross-bundle job ids automatically.
  Local CLI only.
  Use when the user asks to deploy the flowx bundles, or to deploy a multi-pipeline migration in
  dependency order.
---

# Deploy the flowx bundles in dependency order

After package, local CLI only. With several bundles (`per-pipeline` or `per-group` packaging), a
pipeline A that calls pipeline B through Run Pipeline (`ExecutePipeline`) gets a `run_job_task` that
points at a job in B's own bundle. flowx writes that reference as `${var.<B>_job_id}` with a matching bundle
variable, so each bundle is valid on its own, but someone has to pass B's numeric job id to A. This skill
does it: it deploys every bundle under `<output_dir>`, callees first, and wires each deployed job id into
its callers. The top-level `DEPLOY.md` package writes is the same order for a human to follow.

## Before you start

- An output folder with the bundle subfolders from a multi-pipeline migration.
- A working local `databricks` CLI with a configured profile for the target workspace.
- **Not on Databricks serverless or Genie Code:** `databricks bundle deploy` and `bundle summary` don't
  run there, so this is a local CLI (or web terminal) step only.

## How to run

CLI only, with the venv from `flowx-setup` (rules in [`../shared/running-flowx.md`](../shared/running-flowx.md)):

```bash
"$PY" -m flowx.adapter deploy --output-dir <output_dir> --target dev --dry-run      # preview
"$PY" -m flowx.adapter deploy --output-dir <output_dir> --target dev [--profile <profile>]
```

| Flag | Meaning |
|---|---|
| `--output-dir` | The folder holding the bundle subfolders (default `./flowx_output`) |
| `--target` | The bundle target to deploy (default `dev`) |
| `--profile` | The Databricks CLI profile for both `deploy` and `summary` |
| `--dry-run` | Print the order and each `databricks bundle deploy ...` command, with `<callee>=<captured at deploy time>` placeholders, without deploying |
| `--allow-missing-deps` | Go on when a bundle calls a callee that isn't under the output folder; that `--var` is skipped and must be set by hand (see the bundle's `SETUP.md`). Without it, a missing callee is an error |

## Steps

### Step 1: Preview the order

Run with `--dry-run` and show the user the order. The command finds the bundles itself (any immediate
subfolder with a `databricks.yml`, no manifest needed), reads each bundle's job keys and its
`${var.<callee>_job_id}` references from `resources/*.yml`, and sorts them callees first. The order is
deterministic. A cyclic call graph can't be ordered, and the command stops with an error.

### Step 2: Deploy

Run without `--dry-run`. Each bundle is deployed with `databricks bundle deploy`; then the deployed job id
is read from `databricks bundle summary -o json` at `.resources.jobs.<key>.id` and passed to its callers
as `--var "<callee>_job_id=<id>"`. A resource with no deployed job id (for example a Lakeflow pipeline) is
skipped, so no empty `--var` is passed. Because it passes the numeric id, not a name, the `[dev <user>]`
name prefix of dev mode doesn't matter: it works the same for `dev` and `prod`.

### Step 3: Report the result

If a bundle's deploy fails, deployment stops at once and its dependents are not deployed; the failing
bundle and its error output are printed. Tell the user which bundle failed and what was deployed before
it.

## Output

The bundles deployed to the target workspace, in order. Nothing is written to `<output_dir>`.

## Reference

- `DEPLOY.md` in `<output_dir>`: the bundle layout, the cross-bundle calls and the suggested order.
- [`../shared/running-flowx.md`](../shared/running-flowx.md): running a phase.
