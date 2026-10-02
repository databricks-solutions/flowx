# Discover — AWS Step Functions

Source guide for `--source stepfunctions`. Parse exported Step Functions state machines into a
classified inventory. See the parent `SKILL.md` for the shared output layout, inventory shape, and
how to run a phase.

## How it works

flowx reads each state machine's **Amazon States Language (ASL)** JSON — either a bare ASL document
(`{"StartAt": ..., "States": {...}}`) or an AWS `describe-state-machine` payload that wraps the ASL
under a `definition` key. Nothing is executed and no AWS credentials are used. Each state is
classified:

- **Deterministic** — control flow and well-known structure that maps to a flowx IR node without an
  LLM: `Choice` → condition, `Map` → for-each, `Parallel` → concurrent branches, `Wait`, `Pass`, and
  a `Task` that starts a nested state machine → run-job.
- **Agentic** — a `Task` that runs a Lambda handler, a Glue job, or a service integration (SNS, SQS,
  DynamoDB, Batch, ECS, EMR, SageMaker). Its code body has no deterministic mapping, so it is emitted
  as a placeholder for LLM-assisted translation in the convert phase.

## Step 1 — Determine the source path

Ask the user for either a single state machine `.json` file or a directory of exported definitions
(scanned recursively). Export with `aws stepfunctions describe-state-machine` or by copying the ASL.

## Step 2 — Run the parser

```bash
"$PY" -m flowx.adapter discover --source stepfunctions \
  --stepfunctions-source-path <path_to_json_or_dir> \
  --output-dir <output_dir> \
  [--pipeline <state_machine_name>]
```

`--stepfunctions-source-path` is the alias of `--source-path`; both normalise to `--source-dir`.
Pass `--pipeline <name>` to scope to a single state machine.

## Step 3 — Read and validate the inventory

Read `<output_dir>/metadata/inventory.json` (`"source": "stepfunctions"`). Each pipeline entry lists
its states with a `strategy`. `metadata/profile_report.csv` carries one row per state machine.

## Step 4 — Present the summary

```
Step Functions Discover Summary
===============================
State machines:     1
Total states:       7
  Deterministic:    5
  Agentic:          2
Translation path:   100.0%
Deterministic:      71.4%
```

## Step 5 — Detail agentic states

For `agentic` states, name the service the `Task` invokes (Lambda / Glue / SNS / ...) and note it will
be emitted as a placeholder for the convert phase to fill via LLM-assisted translation.

## Coverage notes

Deterministic today: `Choice` (single and cascaded rules → condition), `Map` → for-each, `Parallel` →
concurrent branches, `Wait`, `Pass`, and nested-state-machine `Task` → run-job. `Task` states for
Lambda/Glue/service integrations become placeholders. Deferred this increment: EventBridge schedule
ingestion (schedules live outside the ASL), `Retry`/`Catch` error edges, JSONPath input/output
rewriting, and irreducible or cyclic state graphs (routed to a placeholder). See
[`../../flowx-convert/sources/stepfunctions.md`](../../flowx-convert/sources/stepfunctions.md) and the
repo's `design/stepfunctions-ir-mapping.md` for the full mapping and deferrals.
