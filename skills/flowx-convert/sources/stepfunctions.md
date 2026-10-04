# Convert — AWS Step Functions

Source guide for `--source stepfunctions`. Translate exported state machines into the shared flowx
Pipeline IR. See the parent `SKILL.md` for the shared report shape and how to run a phase.

## How it works

The convert phase parses each state machine and translates it into the source-neutral Pipeline IR,
then serialises it to `.work/translation_report.json` — the same report the ADF and Airflow sources
emit, so the package phase consumes it unchanged. Placeholders (agentic states) are also written to
`.work/gaps.json` for LLM-assisted resolution.

A state machine becomes one Lakeflow Job; each ASL state becomes a task or a dependency edge.
Transitions (`Next`) become `depends_on` edges; a `Choice` becomes a nested condition whose branches
reconverge at the computed join state.

## State → IR mapping

| ASL state | Strategy | IR activity |
|---|---|---|
| `Task` — nested state machine (`states:startExecution`) | Deterministic | `RunJobActivity` |
| `Task` — Glue workflow (`glue:startWorkflowRun`, literal `Name`) | Deterministic | `RunJobActivity` |
| `Task` — Lambda / single Glue job / service integration | Agentic | `PlaceholderActivity` |
| `Choice` (single or cascaded rules) | Deterministic | `IfConditionActivity` |
| `Map` | Deterministic | `ForEachActivity` |
| `Parallel` | Deterministic | concurrent task branches |
| `Wait` | Deterministic | `WaitActivity` |
| `Pass` (with `Result`) | Deterministic | `SetVariableActivity` |
| `Pass` (routing only) | — | dropped (dependency passes through) |
| `Succeed` / `Fail` | Deterministic | terminal (no task) |
| `Retry` | Deterministic | `max_retries` + `min_retry_interval_millis` on the task |
| `Catch` | Deterministic | sibling task with `depends_on` outcome `ALL_FAILED` |

## Run it

```bash
"$PY" -m flowx.adapter convert --source stepfunctions \
  --stepfunctions-source-path <path_to_json_or_dir> \
  --output-dir <output_dir> \
  [--pipeline <state_machine_name>]
```

Then run `flowx-package` against the same `<output_dir>`.

## JSONPath input/output

A Task's `Parameters` block is rewritten into the task's parameters: keys ending in `.$` are JSONPath
references into the state input. A reference to a field a prior state produced via its `ResultPath`
becomes a task value, `{{tasks.<producer>.values.<field>}}`; a reference that traces to the
state-machine input becomes a job parameter, `{{job.parameters.<field>}}`, declared on the job. Static
keys pass through as literals. Context-object (`$$`), intrinsic (`States.*`), bracket-notation, and
whole-state (`$`) references are left verbatim with a note. `InputPath` / `OutputPath` /
`ResultSelector` are not yet applied (resolution assumes the default `$`); nested result paths resolve
to the top-level task value with a note to index it in the notebook. See
`design/stepfunctions-jsonpath.md`.

## Composing with Glue Workflows

A `Task` state that starts a Glue workflow (`glue:startWorkflowRun` with a literal `Name`) becomes a
`run_job_task` targeting `${resources.jobs.<workflow>.id}` — the job the `glue` source converts that
workflow into. To get the Step Functions job and the Glue job in **one** bundle so the reference
resolves, convert each source, combine the reports, then package with `--single-bundle`:

```bash
"$PY" -m flowx.adapter convert --source stepfunctions --stepfunctions-source-path ./sfn --output-dir ./out
"$PY" -m flowx.adapter convert --source glue --glue-source-path ./glue --output-dir ./out_glue
"$PY" -m flowx.adapter combine \
  --report ./out/.work/translation_report.json \
  --report ./out_glue/.work/translation_report.json \
  --out ./out/.work/combined.json
"$PY" -m flowx.adapter package --report ./out/.work/combined.json --output-dir ./out --single-bundle
```

Without `--single-bundle`, each pipeline is packaged as its own bundle and the cross-job reference is
rewritten to a `${var.<workflow>_job_id}` bundle variable you populate at deploy instead. A single
Glue *job* started via `glue:startJobRun` (not a workflow) stays an agentic placeholder — there is no
converted job to run. See `design/aws-composition.md`.

## Deferred this increment

- **EventBridge schedules** — schedules are EventBridge rules outside the ASL; the Job schedule is
  not populated yet.
- **`Retry` / `Catch`** — error-handling edges are not mapped to task retries / failure dependencies.
- **Job schedule** — Step Functions state machines carry no schedule in their ASL. Schedules live in
  EventBridge Scheduler or EventBridge Rules as separate AWS resources. The converted Lakeflow Job
  will have no schedule; configure it manually after migration using the cron expression from the
  EventBridge resource that was triggering the state machine.
- **`BackoffRate`** — noted but not mapped; retries use a fixed interval.
- **Catch handlers that rejoin the main path** — the catch handler region is translated and wired
  correctly, but the rejoined state's `depends_on` is not automatically extended with the handler's
  exit; manual wiring may be needed at the rejoin point.
- **Compound / `*Path` / `Is*` choice rules** — kept verbatim in the condition's left operand with an
  `expr` operator for review (the branch structure is still correct).
- **Irreducible / cyclic graphs** — routed to a placeholder rather than mistranslated.
- **Glue / Lambda bodies** — surfaced as agentic gaps; deterministic GlueContext/DynamicFrame rewrites
  are a later increment.

See `design/stepfunctions-ir-mapping.md` for the rationale.
