# Airflow → DABs — coverage and follow-ups

What the `--source airflow` path converts today, and what it does **not** yet handle. This is a
verified inventory against the parser (`src/flowx/sources/airflow/`), not an aspirational roadmap.
Use it to set expectations before a migration and to prioritize follow-up work.

The Airflow parser is a **static AST walk** — it reads DAG modules with `ast.parse`, never installs
Airflow, and never executes a DAG. Anything the static walk can't see, it can't convert.

## Supported today

| Construct | Result |
| --- | --- |
| Airflow authoring versions | Airflow 3 `airflow.sdk` and `airflow.providers.standard` imports, modern Airflow 2 imports, and strong Airflow 1.10 legacy operator/sensor imports are resolved statically without importing Airflow. |
| `PythonOperator` (classic) | Notebook task; callable `def` preserved, transitive helpers/constants/non-Airflow imports carried, `op_args`/`op_kwargs` passed as JSON widgets, return value via `dbutils.jobs.taskValues.set`. |
| `PythonVirtualenvOperator` / `ExternalPythonOperator` | Notebook task with a `%pip install` cell for `requirements`. |
| `BranchPythonOperator` / `ShortCircuitOperator` | Failing placeholder + review gap (runtime branch selection can't be lowered statically). |
| `BashOperator` / `SSHOperator` | `%sh` notebook; a single unchained `spark-submit` invocation is lifted only when every option arity is known. |
| `SparkSubmitOperator` | Spark JAR or Python task. |
| Dataproc / Managed Spark (`DataprocSubmitJobOperator`, `DataprocCreateBatchOperator`, and the `ManagedSpark*` aliases) | Routed by the nested payload key, never the class: PySpark → Spark Python task, JVM Spark → Spark JAR task, inline Spark SQL → `sql_task`. Cluster lifecycle tasks and paired sensors are absorbed into Jobs compute when provably safe; see [Dataproc](#dataproc-and-managed-spark). |
| Databricks provider operators (`DatabricksSubmitRun*`, `DatabricksRunNow*`, `DatabricksNotebookOperator`) | Notebook / run-job tasks. |
| SQL operators (`DatabricksSql*`, `SQLExecuteQueryOperator`, `PostgresOperator`, `MySqlOperator`, `HiveOperator`, `DatabricksCopyIntoOperator`) | `sql_task` (SqlActivity); Jinja values → `:name`, identifier positions → `IDENTIFIER(:name)`, with `sql_task.parameters`. |
| `TriggerDagRunOperator` | `run_job_task` referencing the target DAG by sanitized job name. |
| `EmailOperator` | Placeholder recommending job-level email notifications. |
| dbt CLI operators (`DbtRun/Test/Seed/Snapshot/Build/Deps`) and Cosmos `DbtDag` / `DbtTaskGroup` | Single `DbtFactoryActivity`, **static explosion** (default) or **PyDABs** (`--dbt-mode pydabs`); see [dbt factory](#dbt-factory-mode). |
| **TaskFlow API** (`@dag`, `@task`, `@task.virtualenv`) | Canonical, aliased, and qualified Airflow decorators are resolved statically. Each synchronous `@task` invocation → a task; implicit XCom data flow (`transform(extract())`) → a notebook that reads upstream return values via `dbutils.jobs.taskValues.get`, calls the function, and publishes its own. Native async `@task` callables, `@task.branch` / `@task.short_circuit`, or a callable reading task context/XCom route to a linked placeholder + agentic leaf gap. |
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
| Schedule | Cron `schedule_interval` → Quartz (Unix DOW 0–6 → Quartz 1–7); a `timedelta` whose length divides an hour or a day, with a literal `start_date`, → a Quartz cron anchored to `start_date` so runs fire on Airflow's interval boundaries (a one-day interval in the DAG timezone, unless its local start time is skipped or repeated by daylight saving; shorter intervals in UTC); other `timedelta` schedules → periodic, whose phase follows deployment time, so their interval macros become gaps. `@continuous` → continuous mode. Airflow 3 Asset/Dataset lists and uniform `&` / `|` expressions map to `ALL_UPDATED` / `ANY_UPDATED` table triggers when each asset declares `extra={"databricks_table": "catalog.schema.table"}` or an `x-databricks-table:` URI. |
| `trigger_rule` | Exact supported rules map to `run_if`; `none_failed_min_one_success` and its legacy `none_failed_or_skipped` spelling map to `NONE_FAILED` with the all-skipped delta recorded. Rules without an equivalent become linked placeholders. |
| Job parameters | `params={...}` / `Param(default=...)` → job parameters with defaults. User `params.x` keeps the name `x`; `var.value.x`, `dag_run.conf['x']`, and `run_id` use collision-free `__flowx_airflow_*` bindings. User parameter names beginning with `__flowx_` become explicit gaps. |
| Interval macros | `ds`, `ds_nodash`, `ts`, `ts_nodash`, `logical_date` / `execution_date`, `data_interval_start` / `data_interval_end`, and (Airflow 2 data-interval timetables only) `prev_ds` / `next_ds` are rendered as Airflow renders them, in UTC, by a generated first task `__flowx_airflow_dates`; consumers read `{{tasks.__flowx_airflow_dates.values.<macro>}}`. Airflow 2 cron / preset and `timedelta` schedules (and an explicit `CronDataIntervalTimetable`) use the previous schedule tick as the logical date; an Airflow 3 raw cron uses the fire time (assuming the default `create_cron_data_intervals = False`, disclosed); manual and triggered-job runs use the trigger time. The Airflow version comes from source (`schedule_interval`, `airflow.operators.*`, `airflow.utils.dates` → 2; `airflow.sdk`, `airflow.providers.standard` → 3). An undeterminable version or timetable, an event-triggered job, `macros.*`, and Airflow 3 `prev_ds` / `next_ds` become gaps. A native backfill overrides `__flowx_airflow_trigger_time` with `{{backfill.iso_datetime}}` and sets `__flowx_airflow_trigger_type` to `periodic`; `__flowx_airflow_logical_date` replays one exact partition. |
| Job policy | Static positive `dagrun_timeout` → Job `timeout_seconds`; static failure recipients → Job `email_notifications.on_failure`. Explicitly disabled `depends_on_past`, retry/failure email, SLA callback, auto-pause, and empty environment settings are recorded as intentional no-ops. |
| `Variable.get` in a callable | `Variable.get('literal_name')` is rewritten to a collision-free `__flowx_airflow_variable_*` widget. Dynamic keys, Airflow defaults/deserialization options, other Airflow runtime imports, and Airflow `Connection` objects route to placeholders rather than emitting notebooks that require Airflow. |
| Multiple DAGs | Every DAG, including multiple declarations and repeated static `@dag` factory invocations in one Python file, becomes a sibling job in one shared Airflow bundle so `TriggerDagRunOperator` resource references resolve. Narrow classic factories shaped as one DAG declaration followed by `return dag` are expanded with statically bindable arguments. |

Any operator not listed becomes a `PlaceholderActivity` **and** a `gaps.json` entry carrying the
operator's raw source for review. The legacy `merge_agentic` command is disabled for Airflow; eligible one-task leaf gaps may use the fingerprint-bound `flowx-resolve-airflow-gaps` workflow. The
safe fallback is a flagged, failing task rather than a silent omission. Callables that read Airflow task context
(`**context` / `ti`) or XCom, and runtime-branching decorators, take the same route rather than
emitting code that fails at runtime.

The resolver consumes the pinned `airflow-to-dabs` Flowx provider profile. It receives one flowx-produced gap envelope and cannot express graph or task-policy changes. Accepted `resolved` candidates contribute to mechanically validated code-attached coverage, but remain agentic and do not increase deterministic coverage. `needs_input`, `deferred`, and unreviewed candidates remain linked failing placeholders.

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
- **Unresolved Airflow 3 schedules** — `AssetOrTimeSchedule`, mixed Asset boolean expressions, custom timetables, and Assets without explicit Databricks table metadata become `AirflowSourceSemantics` gaps. Job-level trigger and schedule changes are outside the leaf-only agentic contract.
- **Ambiguous Airflow 1.10 schedule defaults** — assigned DAGs and legacy imports are supported, but a DAG using strong 1.10 syntax that omits `schedule_interval` becomes an `AirflowSourceSemantics` gap because historical default schedule and catchup behavior cannot be inferred safely from source alone.
- **Cross-run and operational policy** — active `depends_on_past`, `max_consecutive_failed_dag_runs`, `sla_miss_callback`, retry-email events, dynamic `dagrun_timeout`, and non-empty `default_args.env` have no exact leaf-only Jobs mapping. Each becomes a source-semantics placeholder with a setting-specific remediation message; failure recipients and any independently representable Job timeout remain preserved.
- **Unsafe inline template contexts** — SQL Jinja embedded in a string, quoted identifier, typed literal, or adjacent identifier fragment and shell Jinja in a non-expanding quoted heredoc, ANSI-C quote, or escaped position route to a placeholder instead of emitting a value with changed lexical semantics.
- **Sensors beyond the mapped families** (`S3PrefixSensor`, custom sensors, etc.) → placeholder + gap.
  A file sensor with a non-literal path, or a table/SQL sensor with no literal `sql` / `table_name`,
  also falls back to a placeholder.
- **Dynamic dbt configuration.** Project/profile paths, selectors, excludes, vars, and full-refresh
  flags must be statically visible. Selectors, excludes, and vars are rendered by static explosion
  only; in `--dbt-mode pydabs` they force a static fallback. Missing project, profile, or manifest
  inputs produce a failing setup-required placeholder rather than a partially deployable dbt job.

## Dataproc and Managed Spark

flowx recognizes the Google provider's Dataproc operators and sensors, and the `ManagedSpark*` names
that alias them, and applies one set of rules to both names. A job or batch payload must resolve
statically to exactly one engine key; its value decides the task:

| Payload | Result |
| --- | --- |
| `pyspark_job` / `pyspark_batch` | `spark_python_task`; `args` → `parameters`, `jar_file_uris` → task libraries. |
| `spark_job` / `spark_batch` | `spark_jar_task`; `main_class` → `main_class_name`, main and dependency JARs → task libraries. |
| `spark_sql_job` with an inline `query_list` | `sql_task`, with Jinja values bound as named parameters. |
| `spark_r_*`, `pyspark_notebook_batch`, `hive_job`, `hadoop_job`, `pig_job`, `flink_job`, `presto_job`, `trino_job` | Failing placeholder + gap with engine-specific migration guidance. |

These fail closed rather than guessing a mapping:
- zero or several engine keys, or a payload that isn't static;
- a query file, auxiliary `python_file_uris` / `file_uris` / `archive_uris`, or a missing `main_class`;
- a `file://` Dataproc node path, or a templated artifact location;
- a non-Spark job property, `cancel_on_kill=False`, or any argument without declared semantics.

`spark_submit_task` is never emitted.

**Compute.** The job's `placement.cluster_name` links it to its `DataprocCreateClusterOperator`. From
that cluster, flowx carries over:
- `worker_config.num_instances` as `num_workers`, when there are no secondary workers (zero workers
  becomes a single-node cluster);
- `spark:`-prefixed software properties, as `spark_conf`.

The job's own `spark.*` properties are added to the same `spark_conf`, and every Spark workload binds
to the default job cluster. A job placed by `cluster_labels`, or on a cluster this DAG does not create,
also runs there, and that compute change is reported. Other property prefixes (`yarn:`, `hdfs:`, `mapred:`, `dataproc:`, …) are
removed and reported. Machine types and image versions are reported but never mapped, so the node
type and Databricks Runtime remain bundle variables.

Secondary workers, init actions, autoscaling policies, a Dataproc Metastore, GKE placement, and
optional components block absorption.

**Collapse.** Cluster create, delete, start, and stop tasks are removed, with dependencies rewired, only
when all of these hold:
- every job placed on the cluster migrates deterministically;
- nothing else in the DAG reads a removed task's output through XCom;
- no task updates, scales, or diagnoses the cluster;
- removing a task with a non-default trigger rule would not change when its downstream tasks run.

A `DataprocJobSensor` is removed when its `dataproc_job_id` is the `xcom_pull` of an asynchronous
submission that nothing else reads. A `DataprocBatchSensor` is removed when its `batch_id` matches the
creating operator's `batch_id`. Either sensor must also be downstream of the workload it waits on and
set no `retries` / `retry_delay` of its own. A static `timeout` or `execution_timeout` becomes the
workload task's `timeout_seconds`, which cancels the workload when it expires.

Every removal is recorded in the transformation ledger. The changed retry, teardown, and wait
behavior is reported as a gap finding, as are the GCS artifacts the job's Databricks identity must
be able to read. Tasks that cannot be removed stay as failing placeholders with the reason attached.
Workflow templates, cluster update, scale, and diagnose, batch control, and cancel-operation tasks
always stay as placeholders.

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
  A source selector, exclusion, or `--vars` restriction falls back to static explosion: the factory
  owns resource selection and parse context, so it rejects those options in the per-task dbt
  commands. Static explosion applies them to the generated per-node commands instead.

## Priority for remaining follow-ups

1. **Full TaskGroup expansion** — lower a `@task_group` / `TaskGroup.partial().expand()` into its
   member tasks (a for-each over the group when mapped) instead of a placeholder.
2. **Additional sensor families** — as demand warrants; unmapped sensors route to a placeholder today.
