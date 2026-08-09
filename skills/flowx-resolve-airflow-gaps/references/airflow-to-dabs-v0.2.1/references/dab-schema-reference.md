# Databricks Asset Bundles YAML Schema Reference

Condensed reference for generating DABs configuration files. Covers all task types, triggers, clusters, and job-level configuration supported as of Jan 2026.

---

## Top-Level Structure: `databricks.yml`

```yaml
bundle:
  name: <bundle-name>

include:
  - resources/*.yml

variables:
  spark_version:
    description: Spark runtime version
    default: "<SPARK_VERSION>"
  node_type_id:
    description: Cluster node type
    default: "<NODE_TYPE_ID>"
  warehouse_id:
    description: SQL warehouse ID for SQL tasks
    default: ""

targets:
  dev:
    mode: development
    workspace:
      host: ${var.dev_workspace_url}
  prod:
    mode: production
    workspace:
      host: ${var.prod_workspace_url}
    run_as:
      service_principal_name: ${var.service_principal}
```

---

## Python-Defined Resources (PyDABs)

Resources can also be defined in Python instead of YAML via a top-level `python:` block in `databricks.yml`. Used by dbt factory mode (see `references/operator-mapping.md`) to generate one task per dbt object at deploy time.

```yaml
python:
  venv_path: .venv                                # venv with databricks-bundles installed
  resources:
    - "resources.<module_name>:load_resources"    # one module:function entry per generator
```

The referenced function is called by the Databricks CLI during both `bundle validate` and `bundle deploy`:

```python
from databricks.bundles.core import Bundle, Resources
from databricks.bundles.jobs import Job

def load_resources(bundle: Bundle) -> Resources:
    resources = Resources()
    resources.add_job("<job_key>", Job.from_dict({...}))   # dict uses Jobs API fields
    return resources
```

Rules:

- `python:` coexists with `include: - resources/*.yml`. YAML jobs and Python-registered jobs share one resources namespace, so YAML can reference a Python-registered job (e.g. `job_id: ${resources.jobs.<job_key>.id}` in a `run_job_task`).
- `load_resources` runs on every `bundle validate` too — any deploy-time file writers inside it must be idempotent.
- Relative paths (e.g. `notebook_path`) in Python-defined jobs resolve against the bundle root.
- Requires the venv at `venv_path` to exist with `databricks-bundles` installed before running `validate`/`deploy` (`uv sync --dev` with the generated `pyproject.toml`).

---

## Job Resource Definition

Defined in `resources/*.yml` files, included by `databricks.yml`.

```yaml
resources:
  jobs:
    <job-key>:
      name: <human-readable-name>
      description: <optional description>
      tags:
        team: data-engineering
        source: airflow-migration
      max_concurrent_runs: 1
      timeout_seconds: 3600

      # Schedule (see Schedule section below)
      schedule:
        quartz_cron_expression: "0 0 8 * * ?"
        timezone_id: "America/New_York"
        pause_status: UNPAUSED

      # OR Trigger (see Trigger section below)
      trigger:
        file_arrival:
          url: <unity-catalog-external-location-or-volume-url>

      # Email notifications (job-level)
      email_notifications:
        on_start:
          - "team@example.com"
        on_success:
          - "team@example.com"
        on_failure:
          - "oncall@example.com"

      # Job parameters (accessible by all tasks)
      parameters:
        # For an Airflow {{ ds }} that is a logical/partition date on a SCHEDULED job, default to the
        # scheduled trigger time (correct on normal runs); a native Databricks backfill overrides it
        # with {{backfill.iso_date}}. Use {{job.start_time.iso_date}} for wall-clock "today" semantics
        # or an event-triggered job (trigger.time is unreliable there — see schedule-trigger-mapping.md).
        - name: run_date
          default: "{{job.trigger.time.iso_date}}"
        - name: env
          default: "dev"

      # Shared cluster definitions
      job_clusters:
        - job_cluster_key: shared-cluster
          new_cluster:
            spark_version: ${var.spark_version}
            node_type_id: ${var.node_type_id}
            num_workers: 2
            spark_conf:
              spark.sql.shuffle.partitions: "200"
            spark_env_vars:
              ENV: "{{job.parameters.env}}"

      # Task list
      tasks:
        - task_key: <task-key>
          # ... task definition (see Task Types below)
```

---

## Task Types

Each task must have exactly one task type field (e.g., `notebook_task`, `sql_task`). All tasks share these common fields:

### Common Task Fields

```yaml
- task_key: <unique-identifier>            # Required. 1-100 chars, [a-zA-Z0-9_-]
  description: <optional description>
  depends_on:                              # Optional dependency list
    - task_key: <upstream-task-key>
      outcome: "true"                      # Only for condition_task dependencies
  timeout_seconds: 3600                    # 0 = no timeout
  run_if: ALL_SUCCESS                      # ALL_SUCCESS | ALL_DONE | NONE_FAILED | AT_LEAST_ONE_SUCCESS | ALL_FAILED | AT_LEAST_ONE_FAILED
  # Cluster (one of):
  job_cluster_key: shared-cluster          # Reference to job_clusters entry
  existing_cluster_id: "1234-567890-abc"   # Use existing cluster
  new_cluster:                             # Create new cluster for this task
    spark_version: ${var.spark_version}
    node_type_id: ${var.node_type_id}
    num_workers: 2
  # Notifications (task-level)
  email_notifications:
    on_start: []
    on_success: []
    on_failure: []
```

---

### notebook_task

Runs a Databricks notebook (.py, .ipynb, .sql, .r, .scala).

```yaml
- task_key: my_notebook
  notebook_task:
    notebook_path: ../src/my_notebook.py        # Required. Relative to config file.
    source: WORKSPACE                           # WORKSPACE (default) or GIT
    base_parameters:                            # Optional key-value params
      param1: "value1"
      param2: "{{job.parameters.env}}"
    warehouse_id: ${var.warehouse_id}           # Optional. For SQL-only notebooks.
```

---

### spark_python_task

Runs a Python file on a Spark cluster.

```yaml
- task_key: my_python_script
  spark_python_task:
    python_file: ../src/my_script.py            # Required. Path to .py file.
    source: WORKSPACE
    parameters:                                 # Optional positional args
      - "--date"
      - "{{job.parameters.run_date}}"
```

---

### python_wheel_task

Runs an entry point from a Python wheel package.

```yaml
- task_key: my_wheel_task
  python_wheel_task:
    entry_point: run                            # Required. Function or class name.
    package_name: my_package                    # Required. Package name.
    named_parameters:                           # Optional keyword args (OR parameters, not both)
      env: "prod"
      date: "{{job.parameters.run_date}}"
  libraries:
    - whl: ../dist/my_package-*.whl
```

---

### spark_jar_task

Runs a main class from a JAR file.

```yaml
- task_key: my_jar_task
  spark_jar_task:
    main_class_name: com.example.Main           # Required. Fully-qualified class name.
    parameters:                                 # Optional positional args
      - "--input"
      - "/data/input"
  libraries:
    - jar: /Volumes/main/default/jars/app.jar
```

---

### sql_task

Runs a SQL query, SQL file, or refreshes a SQL alert/dashboard.

```yaml
# SQL file
- task_key: my_sql_file
  sql_task:
    warehouse_id: ${var.warehouse_id}           # Required.
    file:
      path: ../src/query.sql                    # Path to .sql file
      source: WORKSPACE
    parameters:
      run_date: "{{job.parameters.run_date}}"

# SQL query (by ID)
- task_key: my_sql_query
  sql_task:
    warehouse_id: ${var.warehouse_id}
    query:
      query_id: "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"

# SQL alert
- task_key: my_sql_alert
  sql_task:
    warehouse_id: ${var.warehouse_id}
    alert:
      alert_id: "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
```

---

### pipeline_task

Triggers a Lakeflow Declarative Pipeline update (a DLT/declarative pipeline, or a Lakeflow Connect
managed-ingestion pipeline — see below).

```yaml
- task_key: my_pipeline
  pipeline_task:
    pipeline_id: ${resources.pipelines.my_pipeline.id}   # Required. Bundle ref or pipeline ID.
    full_refresh: false                                    # Optional. Default false.
```

---

### Managed-ingestion pipelines (Lakeflow Connect)

A Lakeflow Connect ingestion pipeline is a `resources.pipelines.<name>` entry carrying an
`ingestion_definition`. See `references/lakeflow-connect.md` for when to choose Connect over a Jobs
task. The schema's `ingestion_definition` description warns it should not be mixed with a normal DLT
pipeline's `libraries` settings; note, however, that current query-based-ingestion examples do set
`catalog`/`target` alongside `ingestion_definition` — follow the field combination in the current docs /
`databricks bundle schema` for your CLI version rather than assuming a blanket incompatibility.

**Combined ingestion (primary/canonical)** — one pipeline, `connection_name` on the ingestion
definition (SaaS, files, query-based DB, and CDC via `connection_name`; add `connector_type` when the
source supports both query-based and CDC):

```yaml
resources:
  pipelines:
    salesforce_ingest:
      name: salesforce_ingest
      ingestion_definition:
        connection_name: ${var.salesforce_connection}   # UC connection (created out-of-band)
        objects:
          - table:
              source_schema: salesforce
              source_table: opportunity
              destination_catalog: ${var.catalog}
              destination_schema: ${var.schema}
```

**Foreign-catalog ingestion (query-based, for federated sources — Snowflake/BigQuery/Redshift/Synapse)**
— set `ingest_from_uc_foreign_catalog: true` and reference the source by `source_catalog/schema/table`
(no `connection_name` / gateway on the ingestion definition):

```yaml
resources:
  pipelines:
    snowflake_ingest:
      name: snowflake_ingest
      ingestion_definition:
        ingest_from_uc_foreign_catalog: true
        objects:
          - table:
              source_catalog: ${var.snowflake_foreign_catalog}   # a UC foreign catalog (below)
              source_schema: public
              source_table: orders
              destination_catalog: ${var.catalog}
              destination_schema: ${var.schema}
```

The **foreign catalog** is a bundle resource (`resources.catalogs`), created from a UC connection. It
requires `bundle.engine: direct` — "defining catalogs is only supported if you are using the direct
deployment engine." A foreign catalog needs `connection_name` **plus source-specific `options`** (e.g.
`options: { database: '<db>' }` for Snowflake/PostgreSQL/Redshift per CREATE FOREIGN CATALOG); a
`connection_name`-only catalog can pass schema validation but fail at deploy. **Reference an existing
foreign catalog by default; only create one when the bundle should own it.**

```yaml
bundle:
  name: snowflake-ingest
  engine: direct                     # required to define catalogs in a bundle

resources:
  catalogs:
    snowflake_fc:
      name: ${var.snowflake_foreign_catalog}
      connection_name: ${var.snowflake_connection}
      options:
        database: ${var.snowflake_database}   # source-specific; confirm required options per source
```

> **UC connections are NOT bundle resources.** Create the connection out-of-band (`CREATE CONNECTION`
> / UI) and reference it by name. Record it as a prerequisite in `MIGRATION_NOTES.md` (name, auth,
> networking).

**Gateway CDC (Private Preview — requires enrollment).** Log-based CDC for a database source uses a
**separate** `gateway_definition` pipeline plus an ingestion pipeline joined by `ingestion_gateway_id`
(`gateway_definition` and `ingestion_definition` are never on the same pipeline). The bundle schema
marks `gateway_definition` `[Private Preview]` / `doNotSuggest` — generate this path **only** with
connector-specific verification and confirmed workspace Private-Preview enrollment; it is not the
default. Prefer combined CDC (`connection_name` + `connector_type`) where the connector supports it.

**Orchestration.** A **triggered** ingestion pipeline is driven by a `pipeline_task` at the original
dependency position. A **continuous** pipeline (streaming connectors like Kafka/RabbitMQ, or any
connector documented continuous-only) is not `pipeline_task`-driven — run it standalone and have the
downstream job depend on a job-level `trigger.table_update` on its destination table. Run mode is
per-connector; confirm it, don't assume.

---

### dbt_task

Runs dbt commands.

```yaml
- task_key: my_dbt_task
  dbt_task:
    commands:                                   # Required. Up to 10 commands.
      - "dbt deps"
      - "dbt seed"
      - "dbt run"
      - "dbt test"
    project_directory: ../dbt/my_project        # Optional. Defaults to repo root.
    warehouse_id: ${var.warehouse_id}           # Optional. Omit profiles_directory when set.
    # profiles_directory: ../dbt/profiles       # Optional. Use only when warehouse_id is omitted.
    catalog: main                               # Optional. Requires warehouse_id.
    schema: transforms                          # Optional.
  libraries:
    - pypi:
        package: "dbt-databricks>=1.0.0,<2.0.0"
```

A single `dbt_task` runs the whole invocation as one opaque task. For one task per dbt model/seed/snapshot/test (per-model observability and retries), use dbt factory mode instead — see the dbt conversion decision point in `references/operator-mapping.md`.

---

### run_job_task

Triggers another Databricks job.

```yaml
- task_key: trigger_downstream
  run_job_task:
    job_id: ${resources.jobs.downstream-job.id}  # Required. Job ID or substitution.
    job_parameters:                               # Optional.
      env: "prod"
```

**Nesting limit:** Run Job tasks may nest at most **3 levels deep** (a job runs a job runs a
job); Databricks rejects deeper nesting and circular dependencies. A `for_each_task` whose body
is a `run_job_task` (the mapped-task-group pattern) consumes one of those levels — budget the
remaining depth accordingly.

**Concurrency of the target job:** The target job's own `max_concurrent_runs` (default **1**)
gates how many of its runs proceed at once. When a job is triggered repeatedly — e.g. a
`for_each_task` with `concurrency > 1` whose body is a `run_job_task` — raise the target job's
`max_concurrent_runs` to at least that concurrency (and account for **overlapping parent runs**),
otherwise excess triggers serialize. Also set `queue: { enabled: true }` explicitly on the target
job: a bundle/API-defined job does **not** inherit the UI's default-on queueing, so without it
excess concurrent triggers are **skipped** rather than queued (queued runs wait up to 48 h).

```yaml
resources:
  jobs:
    region_pipeline_job:
      name: region_pipeline
      max_concurrent_runs: 8          # ≥ the driving for_each concurrency (+ overlapping parents)
      queue:
        enabled: true                 # bundle jobs don't inherit UI default-on queueing
      # ... tasks ...
```

---

### condition_task

If/else conditional logic. Does not require a cluster.

```yaml
- task_key: check_condition
  condition_task:
    left: "{{job.parameters.env}}"              # Required. String, dynamic ref, or task value.
    op: EQUAL_TO                                # Required. See operators below.
    right: "prod"                               # Required.

# Operators: EQUAL_TO, NOT_EQUAL, GREATER_THAN, GREATER_THAN_OR_EQUAL, LESS_THAN, LESS_THAN_OR_EQUAL

# Downstream tasks use outcome:
- task_key: prod_task
  depends_on:
    - task_key: check_condition
      outcome: "true"
  notebook_task:
    notebook_path: ../src/prod.py

- task_key: dev_task
  depends_on:
    - task_key: check_condition
      outcome: "false"
  notebook_task:
    notebook_path: ../src/dev.py
```

---

### for_each_task

Iterates a **single** nested task over an array of inputs.

```yaml
- task_key: process_all
  for_each_task:
    inputs: "{{tasks.generate_list.values.items}}"  # Required. JSON array or ref (see forms below).
    concurrency: 5                                   # Optional. Max parallel iterations. Default 1.
    task:                                            # Required. Exactly ONE nested task definition.
      task_key: process_item
      notebook_task:
        notebook_path: ../src/process_item.py
        base_parameters:
          item: "{{input}}"                          # Whole element. Use {{input.field}} for a field.
```

**Nested task — exactly one.** `for_each_task.task` holds a single task, not a subgraph, and it
**cannot** be another `for_each_task`. It may be any standard task type, including a
`run_job_task` — to fan a *multi-step subgraph* out over a collection, make the nested task a
`run_job_task` pointing at a child job that contains the subgraph (see `run_job_task` above for
the concurrency/nesting rules that pattern requires).

**Iteration reference.** Inside the nested task, `{{input}}` is the current element and
`{{input.<key>}}` is a field of an object element. Use them in the nested task's parameter values
(`notebook_task.base_parameters`, `run_job_task.job_parameters`, task `parameters`).

**`inputs` forms and size limits** (all must be JSON-serializable — choose the transport by size):

| Form | Max size |
|---|---|
| JSON-array literal, e.g. `'["a","b"]'` or `'[{"t":"x"}]'` | 5,000 characters |
| Task-value ref `{{tasks.<key>.values.<name>}}` (array produced upstream) | 48 KiB |
| Job-parameter ref `{{job.parameters.<name>}}` | 10,000 characters |

**`concurrency`** defaults to **1** (sequential). Set it to restore parallel fan-out; when the
body is a `run_job_task`, also raise the child job's `max_concurrent_runs` and enable its queue
(see `run_job_task`).

**No cross-iteration outputs.** A task outside the `for_each_task` can depend on the for-each task
as a whole, but cannot read the individual iterations' task values. To consume per-iteration
results downstream, have each iteration persist its result (e.g. write to a table/volume) and add
a separate aggregation task that reads those **persisted** results — not the original input array.

---

### dashboard_task

Refreshes a Lakeview dashboard.

```yaml
- task_key: refresh_dashboard
  dashboard_task:
    dashboard_id: "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"  # Required.
    warehouse_id: ${var.warehouse_id}                       # Optional.
```

---

### clean_rooms_notebook_task

Runs a notebook inside a Databricks Clean Room.

```yaml
- task_key: clean_room_analysis
  clean_rooms_notebook_task:
    clean_room_name: "partner-clean-room"       # Required.
    notebook_name: "shared_analysis"             # Required.
```

---

## Schedule Configuration

Time-based scheduling using Quartz cron expressions (6-7 fields).

```yaml
schedule:
  quartz_cron_expression: "0 0 8 * * ?"     # Required. Seconds Minutes Hours DayOfMonth Month DayOfWeek [Year]
  timezone_id: "America/New_York"             # Required.
  pause_status: UNPAUSED                      # PAUSED or UNPAUSED
```

**Quartz cron field order:** `Seconds Minutes Hours DayOfMonth Month DayOfWeek [Year]`

Use `?` for DayOfMonth or DayOfWeek when the other is specified. This differs from standard 5-field Unix cron.

---

## Trigger Configuration

Event-driven triggers (mutually exclusive with `schedule`).

### File Arrival

```yaml
trigger:
  file_arrival:
    url: "s3://bucket/path/"                             # Required. UC external location or volume URL.
    min_time_between_triggers_seconds: 60                # Optional.
    wait_after_last_change_seconds: 60                   # Optional. Minimum allowed is 60.
```

### Table Update

```yaml
trigger:
  table_update:
    condition: ANY_UPDATED                               # ANY_UPDATED or ALL_UPDATED
    table_names:                                         # Required. List of UC table names.
      - "main.silver.transactions"
      - "main.silver.customers"
    min_time_between_triggers_seconds: 300               # Optional.
    wait_after_last_change_seconds: 60                   # Optional.
```

### Continuous

Use continuous mode for always-on execution semantics (`@continuous` in Airflow).

```yaml
continuous:
  pause_status: UNPAUSED
```

For periodic event triggers, use:

```yaml
trigger:
  periodic:
    interval: 1
    unit: HOURS                                          # HOURS, DAYS, WEEKS
```

---

## Cluster Configuration

Three ways to assign compute to a task:

### New Cluster (per-task)

```yaml
new_cluster:
  spark_version: ${var.spark_version}
  node_type_id: ${var.node_type_id}
  num_workers: 2                                # Fixed size
  # OR autoscale:
  autoscale:
    min_workers: 1
    max_workers: 8
  spark_conf:
    spark.sql.shuffle.partitions: "200"
  spark_env_vars:
    ENV: "prod"
  data_security_mode: SINGLE_USER               # For Unity Catalog
```

### Job Cluster (shared across tasks in same job)

```yaml
# Defined at job level:
job_clusters:
  - job_cluster_key: shared-cluster
    new_cluster:
      spark_version: ${var.spark_version}
      node_type_id: ${var.node_type_id}
      num_workers: 2

# Referenced in task:
- task_key: my_task
  job_cluster_key: shared-cluster
```

### Existing Cluster

```yaml
- task_key: my_task
  existing_cluster_id: "1234-567890-abcdef12"
```

---

### Serverless Environments

Serverless notebook tasks omit all cluster fields (`job_cluster_key`, `new_cluster`, `existing_cluster_id`). Referencing a job-level environment via `environment_key` is OPTIONAL — use it to pin dependencies; without it the task runs on the default serverless environment.

```yaml
resources:
  jobs:
    <job-key>:
      environments:
        - environment_key: Default
          spec:
            # Either a pre-built base-environment file synced with the bundle
            # (built once; tasks skip per-run pip installs):
            base_environment: ${workspace.file_path}/dbt_serverless_env.yaml
            # OR inline dependencies (mutually exclusive with base_environment):
            # environment_version: "5"
            # dependencies:
            #   - dbt-databricks==1.12.2
            #   - dbt-core==1.11.12    # pin dbt-core too (see note below)
      tasks:
        - task_key: my_task
          environment_key: Default
          notebook_task:
            notebook_path: ../src/my_task.py
```

The base-environment file itself contains the same spec fields:

```yaml
environment_version: "5"        # serverless environment version
dependencies:
  - dbt-databricks==1.12.2
  - dbt-core==1.11.12           # pin dbt-core too, not just the adapter
```

For dbt factory mode, pin **both** `dbt-databricks` and `dbt-core` to the exact versions in the bundle venv. `dbt-databricks` alone allows a `dbt-core` range, but the factory glue imports the local `dbt-core` for its selector-exactness check — the runtime environment must resolve the identical `dbt-core` for that guarantee to hold.

---

## Variable Substitutions

DABs supports dynamic substitutions using `${}` syntax:

| Pattern | Description |
|---|---|
| `${var.<name>}` | Bundle variable |
| `${resources.jobs.<key>.id}` | Job ID from another resource in the bundle |
| `${resources.pipelines.<key>.id}` | Pipeline ID from the bundle |
| `${workspace.root_path}` | Workspace root path for the bundle |
| `${bundle.name}` | Bundle name |

---

## Dynamic Value References (in task parameters)

Used within task parameter values using `{{}}` syntax:

| Pattern | Description |
|---|---|
| `{{job.parameters.<name>}}` | Job-level parameter |
| `{{job.run_id}}` | Current run ID |
| `{{job.start_time.iso_date}}` | Actual execution start date, UTC (YYYY-MM-DD). Wall-clock — drifts with queue delay/retries. |
| `{{job.trigger.time.iso_date}}` | Scheduled trigger date, UTC (rounded to the minute for cron). The right default for a logical/partition `{{ ds }}` on a scheduled job — correct on normal runs, where `start_time` would drift. Other parts: `iso_datetime`, `year`, `month`, `day`, `timestamp_ms`. |
| `{{backfill.iso_date}}` | Start of the time range for a native [backfill](https://docs.databricks.com/aws/en/jobs/backfill-jobs) run — the logical date being replayed. Set by the backfill UI as a per-run override of a date/time job parameter. Also `iso_datetime`, `timestamp_ms`, `year`, `month`, `day`. |
| `{{tasks.<key>.values.<name>}}` | Task value set by upstream task via `dbutils.jobs.taskValues.set()` |
| `{{input}}` | Current element inside a `for_each_task` nested task |
| `{{input.<key>}}` | A field of the current element (when iterating objects) |
| `{{job.repair_count}}` | Number of repair attempts |
