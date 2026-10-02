# Discover — AWS Glue Workflows

Source guide for `--source glue`. Parse exported Glue Workflow graphs into a classified inventory.
See the parent `SKILL.md` for the shared output layout, inventory shape, and how to run a phase.

## How it works

flowx reads each workflow's graph from an `aws glue get-workflow --include-graph` export (either the
bare workflow object or a `{"Workflow": {...}}` payload). Nothing is executed and no AWS credentials
are used. The graph has three node kinds:

- **Job** and **crawler** nodes become tasks. This increment classifies both as **agentic**: the
  workflow export names a job but does not carry its script, so each is emitted as a placeholder for
  LLM-assisted translation in the convert phase (Glue ETL → Spark, crawler → Auto Loader / Unity
  Catalog setup).
- **Trigger** nodes carry the orchestration and are translated **deterministically** — they do not
  become tasks. A scheduled trigger sets the Lakeflow Job schedule; a conditional trigger becomes
  dependency edges between tasks; an on-demand trigger leaves its actions as manual roots.

Because job and crawler bodies are deferred to the convert phase, deterministic *task* coverage is 0%
this increment; the deterministic value is the schedule and the dependency graph.

## Step 1 — Determine the source path

Ask the user for either a single workflow `.json` file or a directory of exports (scanned
recursively). Export with `aws glue get-workflow --name <workflow> --include-graph`.

## Step 2 — Run the parser

```bash
"$PY" -m flowx.adapter discover --source glue \
  --glue-source-path <path_to_json_or_dir> \
  --output-dir <output_dir> \
  [--pipeline <workflow_name>]
```

`--glue-source-path` is the alias of `--source-path`; both normalise to `--source-dir`. Pass
`--pipeline <name>` to scope to a single workflow.

## Step 3 — Read and validate the inventory

Read `<output_dir>/metadata/inventory.json` (`"source": "glue"`). Each pipeline entry lists its job
and crawler nodes with a `strategy`. `metadata/profile_report.csv` carries one row per workflow.

## Step 4 — Present the summary

```
Glue Workflows Discover Summary
===============================
Workflows:          1
Total nodes:        4
  Deterministic:    0
  Agentic:          4
Translation path:   100.0%
Deterministic:      0.0%
```

## Step 5 — Detail agentic nodes

For each job, note it will be emitted as a placeholder for the convert phase to fill with a ported
Spark script. For each crawler, note it maps to Auto Loader schema inference or a Unity Catalog
setup step.

## Coverage notes

Deterministic today: scheduled triggers (cron → Quartz Job schedule), conditional triggers
(predicate → `depends_on` edges with a `run_if` outcome), and on-demand triggers (manual roots). Job
and crawler bodies become placeholders. Deferred this increment: EventBridge-driven (`EVENT`)
triggers, deterministic GlueContext/DynamicFrame rewrites, and multi-trigger fan-in where two
triggers target one node with conflicting predicates. See
[`../../flowx-convert/sources/glue.md`](../../flowx-convert/sources/glue.md) and the repo's
`design/glue-ir-mapping.md`.
