# Glue Workflows → flowx IR mapping

Design notes for the `glue` source. Captures how a Glue Workflow graph maps onto flowx's IR and what
the first increment defers. Companion to `stepfunctions-ir-mapping.md`; the two are separate sources
(PRD Open Question 1), sharing only `flowx/sources/inventory.py`.

## The shape

A Glue Workflow is a graph of three node kinds: **jobs**, **crawlers**, and **triggers**. The graph
also ships an `Edges` list, but it is denormalised — the authoritative orchestration lives on each
**trigger** object (`Predicate` = upstream state, `Actions` = downstream nodes, `Schedule` = cron).
The translator reads triggers and ignores `Edges`.

## Approach: nodes are tasks, triggers are wiring

- **Jobs and crawlers → tasks.** Each becomes one `PlaceholderActivity`. The workflow export names a
  job but does not carry its script (that lives in a separate `get-job` response's S3
  `ScriptLocation`), so there is nothing to translate deterministically yet — the body is an agentic
  gap for the convert phase. Crawlers likewise become placeholders (Auto Loader / Unity Catalog
  setup). This is honest: deterministic *task* coverage is 0% until bodies are handled, but the
  schedule and dependency graph are translated deterministically.
- **Triggers → schedule + edges.** A `SCHEDULED` trigger sets `Pipeline.schedule`; `ON_DEMAND` leaves
  its actions as manual roots; `CONDITIONAL` adds a `depends_on` edge from each predicate condition's
  node to each action node.

## Predicate → `run_if`

A conditional trigger's predicate reduces to a single Databricks `run_if`, stamped as the edge
`outcome`. `run_if_from_adf_outcomes` (workflow_preparer.py) passes DAB `run_if` constants straight
through, so the glue source reuses the Airflow outcome path with no bundler change:

| Predicate | `run_if` |
|---|---|
| all `SUCCEEDED`, `AND` | `None` (default `ALL_SUCCESS`) |
| `SUCCEEDED`, `ANY` | `AT_LEAST_ONE_SUCCESS` |
| all `FAILED`, `AND` | `ALL_FAILED` |
| all `FAILED`, `ANY` | `AT_LEAST_ONE_FAILED` |
| mixed / `TIMEOUT` / `STOPPED` / `CANCELLED` | `ALL_DONE` |

The last row is lossy: Databricks gates only on success or failure, so a Glue `TIMEOUT`/`STOPPED`
condition can only be approximated as "run regardless".

## Cron → Quartz

Glue cron is 6-field (`minute hour day-of-month month day-of-week year`) wrapped in `cron(...)` and
shares Quartz's `?` day convention, so the Quartz expression is the AWS fields with a `0` seconds
field prefixed: `cron(0 7 * * ? *)` → `0 0 7 * * ? *`. Glue schedules are always UTC.

## Deferred (next increments)

- **Job / crawler bodies.** Agentic placeholders for now (PRD Open Question 2, Option 1). Rule-based
  GlueContext/DynamicFrame → Spark and crawler → Auto Loader are a later increment.
- **`EVENT` triggers.** EventBridge-driven starts are not mapped.
- **Multi-trigger fan-in.** When two conditional triggers target one node with different predicates,
  the edges are merged and the preparer's first `run_if` wins; noted in `not_translatable`.
- **Bundler preflight.** As with `stepfunctions`, `glue` had to be added to the `tags.source`
  allowlist in `dab_writer.py` — the package phase is not quite source-agnostic (PRD inaccuracy).
