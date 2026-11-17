# Dataproc migration

Use this reference when an Airflow DAG imports Dataproc operators or sensors from the Google provider or submits jobs to Google Cloud Managed Service for Apache Spark (formerly Dataproc). The migration target is native Databricks Lakeflow Jobs unless the user explicitly chooses to retain Dataproc.

## Recognition

Recognize both public naming surfaces without treating either as deprecated:

- Classes in `airflow.providers.google.cloud.operators.dataproc`, including `DataprocSubmitJobOperator`, `DataprocCreateBatchOperator`, cluster lifecycle operators, workflow-template operators, batch control operators, and `DataprocCancelOperationOperator`.
- Rebranding aliases in `airflow.providers.google.cloud.operators.managed_spark`, including `ManagedSparkSubmitJobOperator`, `ManagedSparkCreateBatchOperator`, and the corresponding cluster/workflow/control `ManagedSpark*Operator` names. These aliases point to the Dataproc implementations in provider releases that expose the module; verify the installed provider rather than assuming a version from the DAG text.
- `DataprocJobSensor` and `DataprocBatchSensor` in `airflow.providers.google.cloud.sensors.dataproc`.
- Dynamic imports, local wrappers, and subclasses whose `execute()` delegates to one of these operators or to the Dataproc hook. Surface an unresolved wrapper instead of dropping it.

There is no single `DataprocOperator` mapping. `DataprocSubmitJobOperator` and `DataprocCreateBatchOperator` are envelopes around multiple engines, and the nested job or batch discriminator determines the Databricks target.

## Inventory contract

Capture these values before mapping:

- Operator class and import path, including whether it uses a `Dataproc*` name or `ManagedSpark*` alias.
- `project_id`, `region`, `gcp_conn_id`, `impersonation_chain`, labels, request ID, timeout, retry policy, `asynchronous`, `deferrable`, `cancel_on_kill`, and polling interval.
- Complete `job` or `batch` payload after resolving local constants, dictionary composition, and Jinja that can be resolved statically.
- The one-of discriminator and its body: `pyspark_job`, `spark_job`, `spark_sql_job`, `spark_r_job`, `hive_job`, `hadoop_job`, `pig_job`, `flink_job`, `presto_job`, `trino_job`, `pyspark_batch`, `pyspark_notebook_batch`, `spark_batch`, `spark_sql_batch`, or `spark_r_batch`.
- Main source/JAR/class/query; arguments; Spark properties; Python files; JARs; archives; ordinary files; Maven packages; environment; runtime version; service account; network; staging bucket; and history-server settings.
- `cluster_config`, `virtual_cluster_config`, autoscaling policy body or URI, machine types, worker counts, secondary/preemptible workers, image version, init actions, optional components, disks, accelerators, service account, network, and GKE placement.
- Workflow-template body, ordered jobs, `prerequisite_step_ids`, managed or existing cluster placement, and parameter declarations/values.
- Every XCom consumer of a returned job ID, batch object, cluster ID, operation ID, status, diagnostics URI, or metadata.
- Sensor target ID, target/failure states, poke interval, timeout, soft-fail behavior, and whether it monitors a submission in the same DAG.

If the payload or template body is constructed at runtime and cannot be resolved, do not guess an executable mapping. Require the resolved payload or retain the external orchestration with a manual-review note.

## Decision flow

1. Determine whether the Dataproc workload is migrating to Databricks or intentionally remaining on Google Cloud. Default to native migration for an Airflow-to-Databricks conversion; retaining Dataproc requires an explicit user decision or a workload that cannot yet move.
2. Resolve exactly one job or batch discriminator. Zero or multiple discriminators are a validation error.
3. Select the native Lakeflow task from the payload table below.
4. Resolve source artifacts, dependencies, data paths, authentication, compute constraints, and downstream output contracts.
5. Collapse cluster/batch lifecycle and wait sensors only after proving their IDs and outputs have no remaining consumers.
6. Rewire dependencies across every removed lifecycle task and document the changed retry/cancellation envelope.

## Payload mapping

| Dataproc payload | Databricks target | Field mapping | Fail-closed conditions |
|---|---|---|---|
| `pyspark_job` / `pyspark_batch` | `spark_python_task` | Main Python URI → `python_file`; `args` → `parameters`; compatible Spark properties → compute `spark_conf` | Missing source, unsupported auxiliary files/archives, unresolved packages, or Dataproc-only runtime behavior |
| `pyspark_notebook_batch` | `notebook_task` | Import the resolved notebook source into the bundle; translate resolvable arguments to named `base_parameters`/widgets | Missing notebook source, positional arguments without a verified name mapping, unsupported notebook format, or unresolved dependencies |
| `spark_job` / `spark_batch` | `spark_jar_task` | Main class → `main_class_name`; arguments → `parameters`; main/dependency JARs → `libraries` | Missing main class/JAR, incompatible JAR packaging, or unsupported artifact location |
| `spark_sql_job` / `spark_sql_batch` | `sql_task` for SQL Warehouse-compatible SQL; otherwise `notebook_task` | Inline query or query file → extracted `.sql`; parameters use named markers | Spark session configuration, JAR/UDF dependencies, unsupported commands, or unresolved query source |
| `spark_r_job` / `spark_r_batch` | R `notebook_task` on classic compute | R source → R notebook; arguments → widgets/base parameters | Missing source, unresolved R packages, environment assumptions, or no supported classic compute |
| `hive_job` | `sql_task` or `notebook_task` | Translate HiveQL to Spark SQL; translate metastore objects to Unity Catalog | Hive-specific SerDes/UDFs/scripts, unresolved metastore names, or incompatible commands |
| `hadoop_job` | Rewrite to PySpark/JVM Spark, then use `spark_python_task`/`spark_jar_task` | Preserve inputs, outputs, and job arguments in the rewritten workload | Never treat a MapReduce JAR as a Spark JAR without a verified Spark entry point |
| `pig_job` | Rewrite to Spark SQL or PySpark | Preserve dataflow and UDF semantics | No Pig runtime exists on Databricks |
| `flink_job` | Redesign as Spark Structured Streaming or a Lakeflow Spark Declarative Pipeline when appropriate | Preserve source, sink, checkpoint, state, watermarks, and delivery guarantees | No direct Flink task type |
| `presto_job` / `trino_job` | Rewrite compatible SQL for `sql_task`, or retain externally through an SDK notebook | Preserve catalogs, session properties, and result/output contract | Presto/Trino are not Lakehouse Federation sources and are not Spark jobs |

Never emit `spark_submit_task`. Databricks marks the task type deprecated and pending removal, disallows it for new use cases, and directs JVM workloads to JAR tasks and R workloads to notebooks.

A `spark_python_task` must reference a plain Python script. Do not add `# Databricks notebook source` as the first line: Databricks classifies that workspace asset as a notebook, while `spark_python_task.python_file` requires a Python file. Pass converted Airflow arguments through the task's `parameters` list and read them with `argparse` or `sys.argv`.

### PySpark example

Airflow:

```python
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateClusterOperator,
    DataprocDeleteClusterOperator,
    DataprocSubmitJobOperator,
)
from airflow.providers.google.cloud.sensors.dataproc import DataprocJobSensor

create_cluster = DataprocCreateClusterOperator(
    task_id="create_cluster",
    project_id=PROJECT_ID,
    region=REGION,
    cluster_name=CLUSTER_NAME,
    cluster_config={
        "worker_config": {"num_instances": 4, "machine_type_uri": "n2-standard-8"},
        "software_config": {
            "properties": {
                "spark:spark.sql.adaptive.enabled": "true",
                "yarn:yarn.nodemanager.resource.memory-mb": "28672",
            }
        },
    },
)

submit_events = DataprocSubmitJobOperator(
    task_id="submit_events",
    project_id=PROJECT_ID,
    region=REGION,
    asynchronous=True,
    job={
        "placement": {"cluster_name": CLUSTER_NAME},
        "pyspark_job": {
            "main_python_file_uri": "gs://dataproc-artifacts/jobs/transform_events.py",
            "args": ["--run-date", "{{ ds }}"],
        },
    },
)

wait_for_events = DataprocJobSensor(
    task_id="wait_for_events",
    project_id=PROJECT_ID,
    region=REGION,
    dataproc_job_id="{{ ti.xcom_pull(task_ids='submit_events') }}",
)

delete_cluster = DataprocDeleteClusterOperator(
    task_id="delete_cluster",
    project_id=PROJECT_ID,
    region=REGION,
    cluster_name=CLUSTER_NAME,
    trigger_rule="all_done",
)

create_cluster >> submit_events >> wait_for_events >> delete_cluster
```

DABs resource after the source file is copied into the bundle:

```yaml
resources:
  jobs:
    dataproc_events_job:
      name: dataproc-events
      parameters:
        - name: run_date
          default: ""
        - name: trigger_date
          default: "{{job.trigger.time.iso_date}}"
      job_clusters:
        - job_cluster_key: events_compute
          new_cluster:
            spark_version: ${var.spark_version}
            node_type_id: ${var.node_type_id}
            num_workers: 4
            spark_conf:
              spark.sql.adaptive.enabled: "true"
      tasks:
        - task_key: transform_events
          job_cluster_key: events_compute
          spark_python_task:
            python_file: ../src/transform_events.py
            parameters:
              - --run-date
              - "{{job.parameters.run_date}}"
              - --trigger-date
              - "{{job.parameters.trigger_date}}"
```

This example interprets `{{ ds }}` with Airflow 2 scheduled data-interval semantics. The script uses `run_date` when an exact partition date is explicitly supplied and otherwise derives the previous calendar day from `trigger_date`. Native backfill must leave `run_date` empty and override `trigger_date` with `{{backfill.iso_date}}`, so replayed and scheduled runs apply the same offset; an Airflow 3 raw-cron DAG may instead use the trigger date directly. The create, sensor, and delete tasks disappear only because the cluster/job IDs are not otherwise consumed. The Spark property is preserved; the YARN property is removed and disclosed. `n2-standard-8` is not guessed into a Databricks node type: `${var.node_type_id}` remains required until the user selects a workspace-supported GCP node type. Deleting the Dataproc cluster under `all_done` has no direct task because Jobs compute terminates with the run; document any teardown-status difference.

### JVM Spark example

For a `spark_job` or `spark_batch`, emit a JAR task and attach the JAR as a library:

```yaml
- task_key: aggregate_events
  job_cluster_key: events_compute
  spark_jar_task:
    main_class_name: com.example.events.AggregateEvents
    parameters:
      - --run-date
      - "{{job.parameters.run_date}}"
  libraries:
    - jar: /Volumes/main/artifacts/jars/events-assembly.jar
```

Do not assume a Dataproc system JAR or `file:///usr/lib/...` exists on Databricks. A GCS JAR can be preserved only when the selected Databricks compute and identity support that location; otherwise copy it to a Unity Catalog volume or use a verified Maven coordinate.

## Source and auxiliary artifacts

- A PySpark main file already stored at an accessible `gs://` URI can remain there on Databricks on Google Cloud. If the customer supplies the source repository, prefer copying the source into the bundle for versioned deployment. If neither source nor an accessible URI is available, use a required variable with no default or `<REQUIRED_PYSPARK_SOURCE_URI>` and record the blocker.
- `python_file_uris`, `.zip`/`.egg` files, `archive_uris`, and `file_uris` are not automatically equivalent to Jobs libraries. Package Python code as a wheel when practical; map archives/files only after identifying how the application reads them. Flag unresolved artifact side effects.
- Map Maven packages to `libraries[].maven` only when coordinates and repositories are explicit. Map Python packages to a serverless environment or task library only when exact requirements are known. Never infer versions.
- Preserve ordinary job arguments exactly after converting Airflow Jinja to job parameters/dynamic value references.
- Extract inline Spark SQL/HiveQL into committed `.sql` files and use named parameter markers. Do not place dynamic value references directly inside SQL files.

## Cluster lifecycle and compute

Dataproc cluster operators describe infrastructure; Lakeflow tasks describe workload execution. Prefer serverless when the workload and required dependencies/configuration are supported. Otherwise create Jobs compute and absorb compatible cluster settings.

R is not supported on serverless compute. Every migrated `spark_r_job` or `spark_r_batch` must therefore use classic Jobs compute through `job_cluster_key`, `new_cluster`, or `existing_cluster_id`.

Serverless supports only a documented subset of Spark configuration properties. Preserve a Dataproc Spark property on serverless only when it appears in the Databricks serverless allowlist; otherwise use classic Jobs compute or flag the property for redesign. Never silently copy an unsupported property or silently drop one that affects behavior.

| Dataproc setting | Databricks handling |
|---|---|
| `worker_config.num_instances` with no secondary workers | May map to `num_workers` when static |
| `secondary_worker_config`, preemptible workers, driver pools | No exact topology mapping; require a compute design and document cost/resilience differences |
| `worker_config.machine_type_uri`, `master_config.machine_type_uri` | Never copy or guess; require an explicit workspace-supported `node_type_id` mapping |
| `software_config.image_version` / batch runtime version | Require an explicit supported Databricks Runtime mapping; do not derive it from the numeric Dataproc image |
| `software_config.properties` keys prefixed `spark:` | Strip the `spark:` namespace and place compatible `spark.*` keys in `spark_conf` |
| Job/batch `properties` containing `spark.*` | Place compatible properties in compute `spark_conf`; task-level conflicts require review |
| `yarn:`, `hdfs:`, `mapred:`, `dataproc:` properties | Remove or redesign; always list active settings in `MIGRATION_NOTES.md` |
| Autoscaling policy body | Map only resolved min/max worker behavior; a policy URI alone is insufficient |
| Init actions | Convert deliberately to libraries, environment dependencies, init scripts, or application code; do not copy blindly |
| Optional components | Classify each component; Hive/Spark may be rewritten, while Flink/Trino/Presto require the payload rules above |
| GKE virtual cluster | No literal compute mapping; redesign on Databricks compute |
| Disks, accelerators, network, internal IP, CMEK | Treat as workspace/compute/security design inputs, not portable task fields |
| Cluster labels | May become job tags only when they are organizational metadata; do not imply lifecycle equivalence |

Remove create/start/update/stop/delete/diagnose tasks only when all of these are true:

- The cluster exists only to run workloads being migrated in the same graph.
- No downstream or cross-DAG consumer uses its ID, state, diagnostics, labels, or metadata.
- No task mutates the cluster between jobs in a way that changes workload semantics.
- Every predecessor and successor edge can be rewired without changing trigger-rule behavior.
- Setup/teardown and retry-envelope changes are recorded.

Otherwise retain the external action explicitly or stop for a design decision.

## Asynchronous jobs, sensors, retries, and cancellation

Collapse an asynchronous job submission plus `DataprocJobSensor` only when `dataproc_job_id` resolves to the submitted job ID and that ID has no other consumer. For batches, `DataprocCreateBatchOperator` returns the whole batch as a dictionary; collapse a paired `DataprocBatchSensor` only when its `batch_id` matches the create operator's `batch_id` argument, or a verified ID extracted from that returned batch object, and no other task consumes the object or ID. `deferrable=True` and Airflow reschedule mode are scheduler optimizations and do not require a Databricks poller after native migration.

Native task retries rerun the Databricks workload. This may differ from retrying only the Dataproc submit call or reattaching to an already-running job. Record the old and new retry boundaries and repeated-side-effect risk. If the Airflow workflow uses a stable request ID, resumable job ID, `cancel_on_kill`, a separate cancellation task, or metadata-driven branching, preserve the behavior explicitly or flag it.

When Dataproc remains external, use a notebook with the Google Cloud SDK and:

- Store the submitted job/batch ID before polling and reuse it after retry.
- Preserve request ID/idempotency and never submit a duplicate external job merely because the notebook retried.
- Preserve wait/no-wait behavior, polling interval, timeout, target/failure states, soft-fail gating, cancellation, and job result/output transport.
- Keep Google credentials out of source. Use the customer-approved workload identity/service-account mechanism or secrets, and document the prerequisite.

## Workflow templates

For `DataprocInstantiateInlineWorkflowTemplateOperator` or a stored template whose full body is available:

1. Expand every ordered job into a Lakeflow task using its payload discriminator.
2. Map `prerequisite_step_ids` to `depends_on`.
3. Convert template parameters to job parameters and update all field references.
4. Absorb a template-managed cluster into shared Jobs compute only after applying the compute rules above.
5. Preserve per-step retries/timeouts and disclose any collapsed lifecycle/retry envelope.

`DataprocCreateWorkflowTemplateOperator` without instantiation is control-plane infrastructure, not workload execution. Omit it only when the created object is unused after migration; otherwise flag or retain it. A stored template name/ID without the template body is not enough to generate executable Lakeflow tasks.

## GCS data access and identity

Preserving a `gs://` string does not prove access. For every GCS source, JAR, auxiliary artifact, input, output, checkpoint, or staging path:

- State which Databricks identity reads or writes it.
- Prefer Unity Catalog external locations and storage credentials for governed data access where applicable.
- Verify the selected compute supports the artifact location and access mode.
- Preserve path semantics exactly; do not rewrite buckets or prefixes from names alone.
- Record paths that must be copied to a volume or workspace file.

Do not inline `gcp_conn_id`, service-account keys, impersonation credentials, or Airflow connection secrets. Dataproc service accounts and impersonation chains are not automatic Databricks identity mappings.

## Required MIGRATION_NOTES entries

For every Dataproc-bearing DAG, record:

- Operator names/import generation and the resolved job/batch discriminator for every workload.
- Native task selection and why SQL used `sql_task` versus a notebook.
- Every removed cluster, batch, sensor, workflow-template, diagnostics, or cancellation task and the rewired edges.
- Old and new retry, timeout, cancellation, setup/teardown, and rerun boundaries.
- Spark properties preserved; YARN/HDFS/MapReduce/Dataproc properties removed or redesigned.
- Runtime, node type, autoscaling, secondary/preemptible worker, init-action, optional-component, GKE, disk, accelerator, network, and CMEK decisions.
- Main and auxiliary artifact locations, packaging changes, and missing source blockers.
- GCS data/artifact access and identity prerequisites.
- Workflow-template expansion and any unavailable stored template body.
- Returned job/batch/cluster/operation IDs or metadata that had consumers.
- Airflow version and intended data window for every date-sensitive `{{ ds }}`/logical-date argument, including any previous-interval offset.
- Unsupported engines and the chosen rewrite or retained-external plan.

## Validation checklist

- Every submit/create-batch payload resolves to exactly one discriminator.
- Every migrated payload has exactly one task type field.
- No generated YAML contains `spark_submit_task`.
- Every `pyspark_notebook_batch` resolves to a notebook source and a verified named-parameter mapping.
- Every R notebook task uses classic compute, and every preserved Spark property is supported by the selected compute mode.
- PySpark sources, JARs/classes, SQL, and R sources resolve to real bundle files or explicit accessible URIs; unresolved locations have no guessed default.
- Dataproc `file:///` and system paths are not carried into Databricks unless the generated compute deliberately creates them.
- All removed lifecycle/sensor/template tasks have their predecessors and successors rewired.
- Asynchronous submission and sensor pairs are collapsed together or retained together.
- No consumed Dataproc ID/status/metadata disappears silently.
- Every active non-Spark property is removed, redesigned, or documented.
- Required runtime/node/warehouse/path variables have values before deploy; validation failure from an unassigned required variable is reported, not "fixed" with a guessed default.
- GCS access and identity are explicit for every retained path.
- `databricks bundle validate -t <target>` succeeds after required variables are supplied, or the missing values are reported exactly.

## Authoritative documentation

- [Airflow Google provider: Dataproc operators](https://airflow.apache.org/docs/apache-airflow-providers-google/stable/operators/cloud/dataproc.html)
- [Airflow Google provider API: Dataproc operators](https://airflow.apache.org/docs/apache-airflow-providers-google/stable/_api/airflow/providers/google/cloud/operators/dataproc/index.html)
- [Google Cloud Dataproc Jobs REST resource](https://cloud.google.com/dataproc/docs/reference/rest/v1/projects.regions.jobs)
- [Google Cloud Serverless for Apache Spark Batch REST resource](https://cloud.google.com/dataproc-serverless/docs/reference/rest/v1/projects.locations.batches)
- [Databricks: Python script task for jobs](https://docs.databricks.com/gcp/en/jobs/tasks/python-script)
- [Databricks: JAR task for jobs](https://docs.databricks.com/gcp/en/jobs/tasks/jar)
- [Databricks: Spark Submit deprecation and migration](https://docs.databricks.com/gcp/en/jobs/tasks/spark-submit)
- [Databricks: Serverless compute limitations](https://docs.databricks.com/gcp/en/compute/serverless/limitations)
- [Databricks: Spark properties for serverless notebooks and jobs](https://docs.databricks.com/gcp/en/spark/conf#serverless)
