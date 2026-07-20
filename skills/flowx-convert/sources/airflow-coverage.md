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
| `BranchPythonOperator` / `ShortCircuitOperator` | Placeholder routed to the agentic-gap round (runtime branch selection can't be lowered statically). |
| `BashOperator` / `SSHOperator` | `%sh` notebook; a wrapped `spark-submit` is lifted to a Spark JAR/Python task. |
| `SparkSubmitOperator` | Spark JAR or Python task. |
| Databricks provider operators (`DatabricksSubmitRun*`, `DatabricksRunNow*`, `DatabricksNotebookOperator`) | Notebook / run-job tasks. |
| SQL operators (`DatabricksSql*`, `SQLExecuteQueryOperator`, `PostgresOperator`, `MySqlOperator`, `HiveOperator`, `DatabricksCopyIntoOperator`) | `sql_task` (SqlActivity); Jinja → `:name` markers + `sql_task.parameters`. |
| `TriggerDagRunOperator` | `run_job_task` referencing the target DAG by sanitized job name. |
| `EmailOperator` | Placeholder recommending job-level email notifications. |
| dbt CLI operators (`DbtRun/Test/Seed/Snapshot/Build/Deps`) and Cosmos `DbtDag` / `DbtTaskGroup` | Single `DbtFactoryActivity`, **static explosion** (default) or **PyDABs** (`--dbt-mode pydabs`); see [dbt factory](#dbt-factory-mode). |
| **TaskFlow API** (`@dag`, `@task`, `@task.virtualenv`) | Each `@task` invocation → a task; implicit XCom data flow (`transform(extract())`) → a notebook that reads upstream return values via `dbutils.jobs.taskValues.get`, calls the function, and publishes its own. `@task.branch` / `@task.short_circuit`, or a callable reading task context/XCom, route to a placeholder + gap. |
| File sensors (`S3KeySensor`, `GCSObjectExistenceSensor`, `FileSensor`, `HdfsSensor`, `WebHdfsSensor`) | Root sensor with no schedule → `file_arrival` trigger; otherwise a `dbutils.fs` polling notebook task. |
| Table/SQL sensors (`DatabricksPartitionSensor`, `DatabricksSqlSensor`, `DatabricksSQLStatementsSensor`, `SqlSensor`) | Root sensor naming a literal table with no schedule → `table_update` trigger; otherwise a `spark.sql` polling notebook task. |
| `ExternalTaskSensor` | Cross-DAG wait: a notebook polling the upstream DAG's Databricks job run state (referenced by sanitized job name). |
| `HttpSensor` / `PythonSensor` / `DateTimeSensor` | Polling notebook tasks (`requests` poll / callable poll / wait-until). A `PythonSensor` callable reading task context routes to a placeholder. |
| Time sensors (`TimeSensor`, `TimeDeltaSensor`) | Absorbed into the schedule (start-of-DAG delay); dropped with dependency rewiring. |
| `DummyOperator` / `EmptyOperator` | Dropped, downstream dependencies rewired. |
| `.expand()` on an operator | `for_each_task`. |
| Dependencies | `>>` / `<<` chains (incl. list/tuple fan-out and inline TaskFlow calls) and `set_upstream` / `set_downstream`. |
| **TaskGroups** | Static nesting → task-key namespacing (`group__subgroup__task`); group-level edges (`group_a >> group_b`, `task >> group`) expand to leaf→root edges between member tasks. |
| Schedule | Cron `schedule_interval` → Quartz (Unix DOW 0–6 → Quartz 1–7); `timedelta` → periodic. |
| `trigger_rule` | DAB `run_if` constant per edge (`ALL_DONE`, `ALL_FAILED`, `AT_LEAST_ONE_SUCCESS`, `NONE_FAILED`, …). |
| Job parameters | `params={...}` / `Param(default=...)` → job parameters with defaults; `{{ params.x }}` / `{{ var.value.x }}` / `{{ dag_run.conf['x'] }}` → `{{job.parameters.x}}`. |
| `Variable.get` / `BaseHook.get_connection` in a callable | Rewritten to `dbutils.widgets.get` / `dbutils.secrets.get`. |

Any operator not listed becomes a `PlaceholderActivity` **and** a `gaps.json` entry carrying the
operator's raw source, for LLM-assisted translation in the agentic-gap round. That is the safe
fallback: a flagged manual task, not a silent omission. Callables that read Airflow task context
(`**context` / `ti`) or XCom, and runtime-branching decorators, take the same route rather than
emitting code that fails at runtime.

## Not yet supported

These are absent but fail safely — routed to a placeholder + `gaps.json`, or simply not exploded — or
are deliberate scope decisions.

- **Dynamic TaskGroup mapping** (`.expand()` on a `@task_group` or `TaskGroup.partial().expand()`).
  `.expand()` is only recognized on operator/`@task` calls; a mapped *group's* fan-out is lost.
- **Sensors beyond the mapped families** (`S3PrefixSensor`, custom sensors, etc.) → placeholder + gap.
  A file sensor with a non-literal path, or a table/SQL sensor with no literal `sql` / `table_name`,
  also falls back to a placeholder.
- **Shared multi-DAG bundle** *(by design, for now)*. Converting a directory of N DAGs produces **N
  independent bundles**, one per DAG in its own subdirectory — not a single bundle with N jobs.
  Cross-DAG job references (`TriggerDagRunOperator`, `ExternalTaskSensor`) reference the sibling job by
  sanitized name; that job lives in a separate bundle, so both bundles must be deployed to the same
  workspace for the reference to resolve.
- **Scaffolding for dbt static mode.** Static explosion assumes the dbt project, `manifest.json`, and
  profiles already ship in the bundle. No `pyproject.toml` / `Makefile` / `profiles.yml` scaffolding is
  generated (PyDABs mode surfaces the setup steps in SETUP.md instead).

## dbt factory mode

Two front-ends feed a single `DbtFactoryActivity`: Cosmos `DbtDag` / `DbtTaskGroup`, and a chain of
dbt CLI operators (collapsed into one factory at the first dbt task's position). Select the render
mode with `--dbt-mode {static,pydabs}` on the convert phase (default `static`).

- **Static explosion (default).** Emits an inner job with one `notebook_task` per exploded dbt node
  (dependency-wired from a pruned `manifest.json`), a shared `run_dbt_command.py` runner notebook that
  shells out to the dbt CLI, and a `run_job_task` hop from the parent. The manifest is read at package
  time. **Assumes** the dbt project + `manifest.json` + profiles already exist in the bundle.
- **PyDABs (`--dbt-mode pydabs`).** Emits a `resources/<key>_dbt_job.py` hook (plus a
  `resources/__init__.py` package marker) at the bundle root, registers it under `databricks.yml`
  `python.resources`, and surfaces the setup steps in SETUP.md (install `databricks-dbt-factory`,
  ensure `manifest.json` exists). `bundle deploy` runs the hook to build the dbt job from the live
  manifest.

## Priority for remaining follow-ups

1. **Dynamic TaskGroup mapping** — expand a mapped `@task_group` / `TaskGroup.partial().expand()` into
   a for-each over the group's tasks (today the fan-out is lost).
2. **Shared multi-DAG bundle** — a single bundle with N jobs so cross-DAG references resolve within one
   deploy (currently one bundle per DAG, by design).
3. **Additional sensor families** — as demand warrants; unmapped sensors route to a placeholder today.
