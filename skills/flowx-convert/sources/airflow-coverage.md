# Airflow → DABs — coverage and follow-ups

What the `--source airflow` path converts today, and what it does **not** yet handle. This is a
verified inventory against the parser (`src/flowx/sources/airflow/`), not an aspirational roadmap.
Use it to set expectations before a migration and to prioritize follow-up work.

The Airflow parser is a **static AST walk** — it reads DAG modules with `ast.parse`, never installs
Airflow, and never executes a DAG. Anything the static walk can't see, it can't convert.

## Supported today

| Construct | Result |
| --- | --- |
| `PythonOperator` (classic) | Notebook task; callable `def` preserved, transitive helpers/constants/non-Airflow imports carried, `op_args`/`op_kwargs` passed as JSON widgets, return value via `dbutils.jobs.taskValues.set`. |
| `PythonVirtualenvOperator` / `ExternalPythonOperator` | Notebook task with a `%pip install` cell for `requirements`. |
| `BranchPythonOperator` / `ShortCircuitOperator` | Failing placeholder + review gap (runtime branch selection can't be lowered statically). |
| `BashOperator` / `SSHOperator` | `%sh` notebook; a single unchained `spark-submit` invocation is lifted only when every option arity is known. |
| `SparkSubmitOperator` | Spark JAR or Python task. |
| Databricks provider operators (`DatabricksSubmitRun*`, `DatabricksRunNow*`, `DatabricksNotebookOperator`) | Notebook / run-job tasks. |
| SQL operators (`DatabricksSql*`, `SQLExecuteQueryOperator`, `PostgresOperator`, `MySqlOperator`, `HiveOperator`, `DatabricksCopyIntoOperator`) | `sql_task` (SqlActivity); Jinja values → `:name`, identifier positions → `IDENTIFIER(:name)`, with `sql_task.parameters`. |
| `TriggerDagRunOperator` | `run_job_task` referencing the target DAG by sanitized job name. |
| `EmailOperator` | Placeholder recommending job-level email notifications. |
| dbt CLI operators (`DbtRun/Test/Seed/Snapshot/Build/Deps`) and Cosmos `DbtDag` / `DbtTaskGroup` | Single `DbtFactoryActivity`, **static explosion** (default) or **PyDABs** (`--dbt-mode pydabs`); see [dbt factory](#dbt-factory-mode). |
| **TaskFlow API** (`@dag`, `@task`, `@task.virtualenv`) | Canonical, aliased, and qualified Airflow decorators are resolved statically. Each `@task` invocation → a task; implicit XCom data flow (`transform(extract())`) → a notebook that reads upstream return values via `dbutils.jobs.taskValues.get`, calls the function, and publishes its own. `@task.branch` / `@task.short_circuit`, or a callable reading task context/XCom, route to a placeholder + gap. |
| File sensors (`S3KeySensor`, `GCSObjectExistenceSensor`, `FileSensor`, `HdfsSensor`, `WebHdfsSensor`) | With no schedule, a root sensor whose descendants cover every non-sensor task → `file_arrival` trigger; otherwise a `dbutils.fs` polling notebook task. |
| Table/SQL sensors (`DatabricksPartitionSensor`, `DatabricksSqlSensor`, `DatabricksSQLStatementsSensor`, `SqlSensor`) | With no schedule, a root literal-table sensor whose descendants cover every non-sensor task → `table_update` trigger; otherwise a `spark.sql` polling notebook task. |
| `ExternalTaskSensor` | Placeholder explaining logical-run-aware migration options; polling the latest Databricks job run is not equivalent to Airflow's matching logical run. |
| `HttpSensor` / `PythonSensor` / `DateTimeSensor` | Polling notebook tasks for absolute HTTP URLs, callable polls, and wait-until. Relative HTTP endpoints and Python callables reading task context route to placeholders. |
| Time sensors (`TimeSensor`, `TimeDeltaSensor`) | Placeholder; their per-run wait semantics are not silently folded into or removed from the job schedule. |
| `DummyOperator` / `EmptyOperator` | Dropped, downstream dependencies rewired. |
| `.expand()` on `@task` | `for_each_task` when exactly one mapped argument is a literal list and no `.partial()` arguments are present; other forms route to a placeholder + gap. |
| Classic operator `.partial().expand()` / `.expand()` | `for_each_task` containing a linked failing placeholder until every mapped and fixed argument can be proven bound into the inner Databricks task. |
| Dependencies | `>>` / `<<` chains (incl. list/tuple fan-out and inline TaskFlow calls) and `set_upstream` / `set_downstream`. |
| **TaskGroups** (context-manager `with TaskGroup(...)`) | Static nesting → task-key namespacing (`group__subgroup__task`); group-level edges (`group_a >> group_b`, `task >> group`) expand to leaf→root edges between member tasks. |
| **`@task_group`** (decorator form) | Placeholder + gap with dependency edges preserved; a decorator group is a sub-pipeline flowx doesn't lower deterministically. |
| Schedule | Cron `schedule_interval` → Quartz (Unix DOW 0–6 → Quartz 1–7); exact sub-hour `timedelta` → Quartz, longer intervals → periodic, `@continuous` → continuous mode. |
| `trigger_rule` | Exact supported rules map to `run_if`; `none_failed_min_one_success` maps to `NONE_FAILED` with the all-skipped delta recorded. Rules without an equivalent become linked placeholders. |
| Job parameters | `params={...}` / `Param(default=...)` → job parameters with defaults; `{{ params.x }}` / `{{ var.value.x }}` / `{{ dag_run.conf['x'] }}` → `{{job.parameters.x}}`. |
| `Variable.get` in a callable | Rewritten to `dbutils.widgets.get`; a callable using an Airflow `Connection` object routes to a placeholder because one secret string cannot preserve the object API. |
| Multiple DAGs | Every DAG, including multiple declarations and repeated static `@dag` factory invocations in one Python file, becomes a sibling job in one shared Airflow bundle so `TriggerDagRunOperator` resource references resolve. Narrow classic factories shaped as one DAG declaration followed by `return dag` are expanded with statically bindable arguments. |

Any operator not listed becomes a `PlaceholderActivity` **and** a `gaps.json` entry carrying the
operator's raw source for review. The legacy `merge_agentic` command is disabled for Airflow;
eligible one-task leaf gaps may use the fingerprint-bound `flowx-resolve-airflow-gaps` workflow. The
safe fallback is a flagged, failing task rather than a silent omission. Callables that read Airflow task context
(`**context` / `ti`) or XCom, and runtime-branching decorators, take the same route rather than
emitting code that fails at runtime.

The resolver consumes the pinned `airflow-to-dabs` v0.2.1 Flowx provider profile. It receives one
flowx-produced gap envelope and cannot express graph or task-policy changes. Accepted `resolved`
candidates contribute to mechanically validated code-attached coverage, but remain agentic and do
not increase deterministic coverage. `needs_input`, `deferred`, and unreviewed candidates remain
linked failing placeholders.

## Not yet supported

These are absent but fail safely — routed to a linked placeholder notebook that raises
`NotImplementedError`, explicitly excluded, or rejected by reconciliation — or are deliberate scope
decisions.

- **Full TaskGroup expansion** — a `@task_group` invocation (mapped `pair.expand(...)` or plain
  `pair(...)`) and `TaskGroup.partial().expand()` aren't lowered into their member tasks. They route
  to a placeholder + gap with dependency edges preserved.
- **Dynamic operator construction** — operators created inside comprehensions are not statically
  expanded. Helper factories are supported only when their body is an optional docstring followed
  by one statically bindable `return RecognizedOperator(...)`; other forms fail reconciliation and
  block package output.
- **Dynamic DAG factories** — classic DAG factories outside the documented single-declaration shape,
  non-literal factory arguments, and non-literal `dag_id` overrides fail reconciliation and block
  package output rather than emitting a filename-derived empty Job.
- **Sensors beyond the mapped families** (`S3PrefixSensor`, custom sensors, etc.) → placeholder + gap.
  A file sensor with a non-literal path, or a table/SQL sensor with no literal `sql` / `table_name`,
  also falls back to a placeholder.
- **Dynamic dbt configuration.** Project/profile paths, selectors, excludes, vars, and full-refresh
  flags must be statically visible. Missing project, profile, or manifest inputs produce a failing
  setup-required placeholder rather than a partially deployable dbt job.

## dbt factory mode

Two front-ends feed a single `DbtFactoryActivity`: Cosmos `DbtDag` / `DbtTaskGroup`, and a chain of
dbt CLI operators (collapsed into one factory at the first dbt task's position). Select the render
mode with `--dbt-mode {static,pydabs}` on the convert phase (default `static`).

- **Static explosion (default).** Emits an inner job with one `notebook_task` per exploded dbt node
  (dependency-wired from a pruned `manifest.json`), a shared `run_dbt_command.py` runner notebook that
  invokes the dbt CLI with pinned task libraries, and a `run_job_task` hop from the parent. The manifest
  is read at package time; the available project, profile, and manifest files are copied into `src/`.
- **PyDABs (`--dbt-mode pydabs`).** Emits a `resources/<key>_dbt_job.py` hook (plus a
  `resources/__init__.py` package marker) at the bundle root, registers it under `databricks.yml`
  `python.resources`, generates a pinned uv `pyproject.toml` plus the dbt-factory-compatible runner,
  and copies the project/profile/manifest inputs. `bundle deploy` runs the hook to build the dbt job.
  A source `--select` restriction falls back to static explosion so the generated per-node commands
  can preserve dbt selector intersection semantics.

## Priority for remaining follow-ups

1. **Full TaskGroup expansion** — lower a `@task_group` / `TaskGroup.partial().expand()` into its
   member tasks (a for-each over the group when mapped) instead of a placeholder.
2. **Additional sensor families** — as demand warrants; unmapped sensors route to a placeholder today.
