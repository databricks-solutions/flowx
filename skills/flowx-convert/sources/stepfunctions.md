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
| `Task` — Lambda / Glue / service integration | Agentic | `PlaceholderActivity` |
| `Choice` (single or cascaded rules) | Deterministic | `IfConditionActivity` |
| `Map` | Deterministic | `ForEachActivity` |
| `Parallel` | Deterministic | concurrent task branches |
| `Wait` | Deterministic | `WaitActivity` |
| `Pass` (with `Result`) | Deterministic | `SetVariableActivity` |
| `Pass` (routing only) | — | dropped (dependency passes through) |
| `Succeed` / `Fail` | Deterministic | terminal (no task) |

## Run it

```bash
"$PY" -m flowx.adapter convert --source stepfunctions \
  --stepfunctions-source-path <path_to_json_or_dir> \
  --output-dir <output_dir> \
  [--pipeline <state_machine_name>]
```

Then run `flowx-package` against the same `<output_dir>`.

## Deferred this increment

- **EventBridge schedules** — schedules are EventBridge rules outside the ASL; the Job schedule is
  not populated yet.
- **`Retry` / `Catch`** — error-handling edges are not mapped to task retries / failure dependencies.
- **JSONPath I/O** — `InputPath` / `Parameters` / `ResultPath` / `ResultSelector` / `OutputPath` are
  not rewritten into task parameters and task values.
- **Compound / `*Path` / `Is*` choice rules** — kept verbatim in the condition's left operand with an
  `expr` operator for review (the branch structure is still correct).
- **Irreducible / cyclic graphs** — routed to a placeholder rather than mistranslated.
- **Glue / Lambda bodies** — surfaced as agentic gaps; deterministic GlueContext/DynamicFrame rewrites
  are a later increment.

See `design/stepfunctions-ir-mapping.md` for the rationale.
