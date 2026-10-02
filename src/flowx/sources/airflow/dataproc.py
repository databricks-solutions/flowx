"""Static translation rules for Google Cloud Dataproc and Managed Spark operators.

A Dataproc job or batch is an envelope around several engines, so the nested one-of payload key,
not the operator class, decides the Databricks task. This module resolves those payloads and the
cluster settings that accompany them into flowx IR, and explains in plain words why anything it
cannot translate exactly must be migrated by hand. It never imports Airflow or Google libraries.
"""

from __future__ import annotations

import ast
from dataclasses import dataclass, field
from typing import Any

from flowx.models.ir import Activity, SparkJarActivity, SparkPythonActivity, SqlActivity

_OPERATOR_SUFFIXES = (
    "CreateClusterOperator",
    "DeleteClusterOperator",
    "StartClusterOperator",
    "StopClusterOperator",
    "UpdateClusterOperator",
    "ScaleClusterOperator",
    "DiagnoseClusterOperator",
    "SubmitJobOperator",
    "CreateBatchOperator",
    "DeleteBatchOperator",
    "GetBatchOperator",
    "ListBatchesOperator",
    "CancelOperationOperator",
    "CreateWorkflowTemplateOperator",
    "InstantiateWorkflowTemplateOperator",
    "InstantiateInlineWorkflowTemplateOperator",
)

# The Managed Spark names are plain aliases of the Dataproc implementations, so every rule below is
# written once against the Dataproc name.
MANAGED_SPARK_ALIASES: dict[str, str] = {f"ManagedSpark{suffix}": f"Dataproc{suffix}" for suffix in _OPERATOR_SUFFIXES}
DATAPROC_OPERATORS: frozenset[str] = frozenset(f"Dataproc{suffix}" for suffix in _OPERATOR_SUFFIXES)
DATAPROC_SENSORS: frozenset[str] = frozenset({"DataprocJobSensor", "DataprocBatchSensor"})
ALL_DATAPROC_CONSTRUCTS: frozenset[str] = DATAPROC_OPERATORS | DATAPROC_SENSORS | frozenset(MANAGED_SPARK_ALIASES)

SUBMIT_JOB = "DataprocSubmitJobOperator"
CREATE_BATCH = "DataprocCreateBatchOperator"
CREATE_CLUSTER = "DataprocCreateClusterOperator"
DELETE_CLUSTER = "DataprocDeleteClusterOperator"
START_CLUSTER = "DataprocStartClusterOperator"
STOP_CLUSTER = "DataprocStopClusterOperator"
JOB_SENSOR = "DataprocJobSensor"
BATCH_SENSOR = "DataprocBatchSensor"
WORKLOAD_OPERATORS: frozenset[str] = frozenset({SUBMIT_JOB, CREATE_BATCH})

_JOB_DISCRIMINATORS = (
    "pyspark_job",
    "spark_job",
    "spark_sql_job",
    "spark_r_job",
    "hive_job",
    "hadoop_job",
    "pig_job",
    "flink_job",
    "presto_job",
    "trino_job",
)
_BATCH_DISCRIMINATORS = (
    "pyspark_batch",
    "pyspark_notebook_batch",
    "spark_batch",
    "spark_sql_batch",
    "spark_r_batch",
)

_UNSUPPORTED_ENGINE_GUIDANCE: dict[str, str] = {
    "spark_r_job": (
        "R workloads run as an R notebook_task on classic Jobs compute (R is not supported on serverless); "
        "import the resolved R source as a notebook and bind classic compute."
    ),
    "spark_r_batch": (
        "R workloads run as an R notebook_task on classic Jobs compute (R is not supported on serverless); "
        "import the resolved R source as a notebook and bind classic compute."
    ),
    "pyspark_notebook_batch": (
        "Import the notebook source into the bundle as a notebook_task and map its arguments to named "
        "parameters only after verifying the notebook reads them by name."
    ),
    "hive_job": (
        "Translate the HiveQL to Spark SQL and its metastore objects to Unity Catalog, then run it as a "
        "sql_task or notebook_task."
    ),
    "hadoop_job": (
        "MapReduce has no Databricks runtime. Rewrite the workload as PySpark or JVM Spark; a MapReduce JAR "
        "is not a Spark JAR without a verified Spark entry point."
    ),
    "pig_job": "Pig has no Databricks runtime. Rewrite the dataflow as Spark SQL or PySpark.",
    "flink_job": (
        "There is no Flink task type. Redesign the job as Spark Structured Streaming or a Lakeflow Spark "
        "Declarative Pipeline that preserves its sources, sinks, state, watermarks, and delivery guarantees."
    ),
    "presto_job": (
        "Presto is not a Spark engine. Rewrite compatible SQL for a sql_task, or keep the query on Presto "
        "through an SDK notebook."
    ),
    "trino_job": (
        "Trino is not a Spark engine. Rewrite compatible SQL for a sql_task, or keep the query on Trino "
        "through an SDK notebook."
    ),
}

# Fields each translatable payload may carry. Anything else has semantics flowx does not map.
_PAYLOAD_FIELDS: dict[str, frozenset[str]] = {
    "pyspark_job": frozenset(
        {"main_python_file_uri", "args", "python_file_uris", "jar_file_uris", "file_uris", "archive_uris", "properties"}
    ),
    "pyspark_batch": frozenset(
        {"main_python_file_uri", "args", "python_file_uris", "jar_file_uris", "file_uris", "archive_uris"}
    ),
    "spark_job": frozenset(
        {"main_jar_file_uri", "main_class", "args", "jar_file_uris", "file_uris", "archive_uris", "properties"}
    ),
    "spark_batch": frozenset({"main_jar_file_uri", "main_class", "args", "jar_file_uris", "file_uris", "archive_uris"}),
    "spark_sql_job": frozenset({"query_file_uri", "query_list", "script_variables", "properties", "jar_file_uris"}),
    "spark_sql_batch": frozenset({"query_file_uri", "query_variables", "jar_file_uris"}),
}
_JOB_ENVELOPE_FIELDS = frozenset({"placement", "labels"})
_BATCH_ENVELOPE_FIELDS = frozenset({"runtime_config", "environment_config", "labels"})
_BATCH_RUNTIME_FIELDS = frozenset({"version", "properties"})

# Cluster features that change the workload's environment enough to need a compute design decision.
_BLOCKING_CLUSTER_FIELDS: dict[str, str] = {
    "secondary_worker_config": "secondary or preemptible workers have no exact Databricks topology",
    "initialization_actions": "init actions must be converted deliberately to libraries or init scripts",
    "autoscaling_config": "an autoscaling policy reference does not carry resolved worker bounds",
    "metastore_config": "a Dataproc Metastore changes how tables resolve",
    "gke_cluster_config": "a GKE virtual cluster has no literal Databricks compute mapping",
    "auxiliary_node_groups": "auxiliary node groups have no Databricks compute mapping",
}


@dataclass(slots=True, kw_only=True)
class WorkloadTranslation:
    """The outcome of translating one Dataproc job or batch submission.

    Attributes:
        activity: The Databricks activity when the payload translates exactly, otherwise ``None``.
        reason: Why the payload needs manual migration when ``activity`` is ``None``.
        discriminator: The resolved one-of payload key, when exactly one was present.
        cluster_name: The Dataproc cluster the job is placed on, used to find its create operator.
        placement: A description of the Dataproc cluster the job targets, when its payload sets ``placement``.
        spark_conf: Spark properties from the payload that belong on the task's compute.
        dropped_properties: Dataproc-only properties removed from the payload.
        artifact_uris: Remote source and library locations the Databricks identity must be able to read.
        not_mapped: Payload settings recorded for review because Databricks has no equivalent field.
        validation_error: Why the payload is malformed (it does not name exactly one engine), which blocks
            packaging rather than routing the task to manual migration.
    """

    activity: Activity | None = None
    reason: str | None = None
    discriminator: str | None = None
    cluster_name: str | None = None
    placement: str | None = None
    spark_conf: dict[str, str] = field(default_factory=dict)
    dropped_properties: list[str] = field(default_factory=list)
    artifact_uris: list[str] = field(default_factory=list)
    not_mapped: list[str] = field(default_factory=list)
    validation_error: str | None = None


@dataclass(slots=True, kw_only=True)
class ClusterTranslation:
    """Settings from a DataprocCreateClusterOperator's ``cluster_config`` that fit Jobs compute.

    Attributes:
        blocking_reason: Why the cluster cannot be absorbed into Jobs compute, or ``None``.
        num_workers: A static worker count, when the topology maps exactly.
        spark_conf: ``spark:``-prefixed software properties with the namespace removed.
        dropped_properties: Non-Spark software properties removed from the cluster.
        not_mapped: Machine types, image versions, and other design inputs left to the bundle.
    """

    blocking_reason: str | None = None
    num_workers: int | None = None
    spark_conf: dict[str, str] = field(default_factory=dict)
    dropped_properties: list[str] = field(default_factory=list)
    not_mapped: list[str] = field(default_factory=list)


def canonical_name(operator: str) -> str:
    """Returns the Dataproc name for a Managed Spark alias, or the name unchanged."""
    return MANAGED_SPARK_ALIASES.get(operator, operator)


def static_value(node: ast.expr | None) -> Any:
    """Evaluates a literal AST node, returning ``None`` when the value is not known statically."""
    if node is None:
        return None
    try:
        return ast.literal_eval(node)
    except (ValueError, SyntaxError):
        return None


def _is_templated(value: str) -> bool:
    return "{{" in value or "{%" in value


def _string_list(value: Any) -> list[str] | None:
    if value is None:
        return []
    if isinstance(value, (list, tuple)) and all(isinstance(item, str) for item in value):
        return list(value)
    return None


def _split_spark_properties(properties: Any) -> tuple[dict[str, str], list[str], str | None]:
    """Separates Spark properties that belong on Databricks compute from Dataproc-only ones.

    Returns the Spark configuration, the removed Dataproc-only property names, and an error message
    when a property is not a plain ``spark.*`` setting.
    """
    if properties is None:
        return {}, [], None
    if not isinstance(properties, dict):
        return {}, [], "its properties are not a static dictionary"
    spark_conf: dict[str, str] = {}
    dropped: list[str] = []
    for key, value in properties.items():
        if not isinstance(key, str) or not key.startswith("spark."):
            return {}, [], f"property {key!r} is not a Spark property and has no Databricks equivalent"
        if key.startswith("spark.dataproc."):
            dropped.append(key)
            continue
        spark_conf[key] = str(value)
    return spark_conf, sorted(dropped), None


def _resolve_discriminator(payload: dict[str, Any], allowed: tuple[str, ...]) -> tuple[str | None, str | None]:
    present = [key for key in (*_JOB_DISCRIMINATORS, *_BATCH_DISCRIMINATORS) if key in payload]
    if len(present) != 1:
        found = ", ".join(present) if present else "none"
        return None, f"the payload must name exactly one engine; found {found}"
    if present[0] not in allowed:
        return None, f"engine {present[0]!r} is not valid for this operator"
    return present[0], None


def _envelope_error(payload: dict[str, Any], discriminator: str, envelope_fields: frozenset[str]) -> str | None:
    extra = sorted(set(payload) - envelope_fields - {discriminator})
    return f"its payload sets {', '.join(extra)}, which flowx does not map" if extra else None


def translate_workload(operator: str, task_id: str, task_key: str, kwargs: dict[str, ast.expr]) -> WorkloadTranslation:
    """Translates a Dataproc job submission or batch creation into a Databricks activity.

    The nested engine key decides the task: PySpark becomes a Python script task, JVM Spark a JAR
    task, and inline Spark SQL a SQL task. Everything else is returned with a reason so the caller
    emits a failing placeholder instead of guessing.
    """
    canonical = canonical_name(operator)
    is_batch = canonical == CREATE_BATCH
    payload_key = "batch" if is_batch else "job"
    payload = static_value(kwargs.get(payload_key))
    if not isinstance(payload, dict):
        return WorkloadTranslation(
            reason=f"The {payload_key} payload is not statically resolvable; supply the resolved payload."
        )
    if static_value(kwargs.get("cancel_on_kill")) is False:
        return WorkloadTranslation(
            reason="cancel_on_kill=False keeps the Dataproc job running after the task is killed; Databricks "
            "cancels the workload with its run."
        )

    discriminator, error = _resolve_discriminator(payload, _BATCH_DISCRIMINATORS if is_batch else _JOB_DISCRIMINATORS)
    if discriminator is None:
        message = f"The Dataproc {payload_key} payload is invalid: {error}."
        return WorkloadTranslation(reason=message, validation_error=message)
    result = WorkloadTranslation(discriminator=discriminator)
    placement = payload.get("placement") if not is_batch else None
    if isinstance(placement, dict):
        if isinstance(placement.get("cluster_name"), str):
            result.cluster_name = placement["cluster_name"]
            result.placement = f"cluster {result.cluster_name!r}"
        elif placement.get("cluster_labels"):
            result.placement = "a cluster selected by cluster_labels"
        else:
            result.placement = "a cluster that the payload does not name"
    if discriminator in _UNSUPPORTED_ENGINE_GUIDANCE:
        result.reason = (
            f"Dataproc {discriminator} needs manual migration. {_UNSUPPORTED_ENGINE_GUIDANCE[discriminator]}"
        )
        return result

    envelope_error = _envelope_error(
        payload, discriminator, _BATCH_ENVELOPE_FIELDS if is_batch else _JOB_ENVELOPE_FIELDS
    )
    if envelope_error:
        result.reason = f"The Dataproc {payload_key} cannot be translated exactly: {envelope_error}."
        return result

    body = payload[discriminator]
    if not isinstance(body, dict):
        result.reason = f"The {discriminator} body is not a static dictionary."
        return result
    unknown = sorted(set(body) - _PAYLOAD_FIELDS[discriminator])
    if unknown:
        result.reason = f"The {discriminator} sets {', '.join(unknown)}, which flowx does not map."
        return result

    properties: Any = body.get("properties")
    if is_batch:
        runtime = payload.get("runtime_config") or {}
        if not isinstance(runtime, dict):
            result.reason = "The batch runtime_config is not a static dictionary."
            return result
        unknown_runtime = sorted(set(runtime) - _BATCH_RUNTIME_FIELDS)
        if unknown_runtime:
            result.reason = f"The batch runtime_config sets {', '.join(unknown_runtime)}, which flowx does not map."
            return result
        if "version" in runtime:
            result.not_mapped.append(f"runtime version {runtime['version']!r}")
        properties = runtime.get("properties")
        environment = payload.get("environment_config") or {}
        if isinstance(environment, dict) and environment.get("peripherals_config"):
            result.reason = (
                "The batch uses peripherals_config (metastore or history server), which changes how it runs."
            )
            return result
        if isinstance(environment, dict):
            execution = environment.get("execution_config") or {}
            if isinstance(execution, dict):
                result.not_mapped.extend(f"execution_config.{name}" for name in sorted(execution))

    spark_conf, dropped, property_error = _split_spark_properties(properties)
    if property_error:
        result.reason = f"The {discriminator} cannot be translated exactly: {property_error}."
        return result
    result.spark_conf = spark_conf
    result.dropped_properties = dropped

    arguments = _string_list(body.get("args"))
    if arguments is None:
        result.reason = f"The {discriminator} args are not a static list of strings."
        return result
    jar_uris = _string_list(body.get("jar_file_uris"))
    if jar_uris is None:
        result.reason = f"The {discriminator} jar_file_uris are not a static list of strings."
        return result
    for auxiliary in ("python_file_uris", "file_uris", "archive_uris"):
        if body.get(auxiliary):
            result.reason = (
                f"The {discriminator} ships {auxiliary}, which are not equivalent to Databricks task libraries; "
                "package them (for example as a wheel) after confirming how the application reads them."
            )
            return result

    description = f"Migrated from Airflow {operator} ({discriminator})."
    if discriminator in ("pyspark_job", "pyspark_batch"):
        main_file = body.get("main_python_file_uri")
        location_error = _location_error(main_file, "main_python_file_uri", jar_uris)
        if location_error or not isinstance(main_file, str):
            result.reason = f"The {discriminator} cannot be translated: {location_error}."
            return result
        result.artifact_uris = [main_file, *jar_uris]
        result.activity = SparkPythonActivity(
            name=task_id,
            task_key=task_key,
            description=description,
            python_file=main_file,
            parameters=arguments or None,
            libraries=[{"jar": uri} for uri in jar_uris] or None,
            keep_remote_artifacts=True,
        )
        return result

    if discriminator in ("spark_job", "spark_batch"):
        main_class = body.get("main_class")
        if not isinstance(main_class, str) or not main_class:
            result.reason = (
                f"The {discriminator} names no main_class; a Databricks JAR task needs a verified entry point "
                "rather than the JAR manifest."
            )
            return result
        library_uris = [*jar_uris]
        main_jar = body.get("main_jar_file_uri")
        if isinstance(main_jar, str):
            library_uris.insert(0, main_jar)
        if not library_uris:
            result.reason = (
                f"The {discriminator} loads {main_class} from the Dataproc classpath; supply the JAR that "
                "provides it, because Dataproc system JARs do not exist on Databricks."
            )
            return result
        location_error = _location_error(None, None, library_uris) or (
            "main_class is templated" if _is_templated(main_class) else None
        )
        if location_error:
            result.reason = f"The {discriminator} cannot be translated: {location_error}."
            return result
        result.artifact_uris = library_uris
        result.activity = SparkJarActivity(
            name=task_id,
            task_key=task_key,
            description=description,
            main_class_name=main_class,
            parameters=arguments or None,
            libraries=[{"jar": uri} for uri in library_uris],
            keep_remote_artifacts=True,
        )
        return result

    query_list = body.get("query_list")
    queries = query_list.get("queries") if isinstance(query_list, dict) else None
    if body.get("query_file_uri") or not isinstance(queries, list) or not queries:
        result.reason = (
            f"The {discriminator} reads its SQL from a query file flowx cannot read at conversion time; "
            "commit the query to the bundle."
        )
        return result
    if spark_conf or dropped or jar_uris or body.get("script_variables"):
        result.reason = (
            f"The {discriminator} depends on Spark configuration, JARs, or script variables, so it is not "
            "SQL-warehouse compatible; run it from a notebook_task."
        )
        return result
    if not all(isinstance(query, str) for query in queries):
        result.reason = f"The {discriminator} queries are not static strings."
        return result
    result.activity = SqlActivity(
        name=task_id,
        task_key=task_key,
        description=description,
        sql=";\n".join(query.strip().rstrip(";") for query in queries) + ";",
    )
    return result


def _location_error(main_file: Any, main_label: str | None, library_uris: list[str]) -> str | None:
    """Returns why an artifact location cannot be carried to Databricks, or ``None`` when it can."""
    if main_label is not None:
        if not isinstance(main_file, str) or not main_file:
            return f"{main_label} is missing"
        if _is_templated(main_file):
            return f"{main_label} is templated"
        if main_file.startswith("file://"):
            return f"{main_label} {main_file!r} is a Dataproc node path that does not exist on Databricks"
    for uri in library_uris:
        if _is_templated(uri):
            return f"JAR location {uri!r} is templated"
        if uri.startswith("file://"):
            return f"JAR location {uri!r} is a Dataproc node path that does not exist on Databricks"
    return None


def translate_cluster_config(kwargs: dict[str, ast.expr]) -> ClusterTranslation:
    """Maps the parts of a Dataproc cluster definition that have an exact Jobs compute equivalent."""
    if kwargs.get("virtual_cluster_config") is not None:
        return ClusterTranslation(blocking_reason="a GKE virtual cluster has no literal Databricks compute mapping")
    config = static_value(kwargs.get("cluster_config"))
    if kwargs.get("cluster_config") is None:
        config = {}
    if not isinstance(config, dict):
        return ClusterTranslation(blocking_reason="its cluster_config is not statically resolvable")
    for name, reason in _BLOCKING_CLUSTER_FIELDS.items():
        if config.get(name):
            return ClusterTranslation(blocking_reason=reason)

    result = ClusterTranslation()
    worker = config.get("worker_config") or {}
    if isinstance(worker, dict):
        instances = worker.get("num_instances")
        if isinstance(instances, int) and not isinstance(instances, bool):
            result.num_workers = instances
        result.not_mapped.extend(f"worker_config.{name}" for name in sorted(worker) if name != "num_instances")
    if config.get("master_config"):
        result.not_mapped.append("master_config")
    software = config.get("software_config") or {}
    if isinstance(software, dict):
        if software.get("optional_components"):
            return ClusterTranslation(blocking_reason="optional components must be classified before migration")
        if "image_version" in software:
            result.not_mapped.append(f"software_config.image_version {software['image_version']!r}")
        properties = software.get("properties") or {}
        if not isinstance(properties, dict):
            return ClusterTranslation(blocking_reason="its software properties are not a static dictionary")
        for key, value in properties.items():
            if isinstance(key, str) and key.startswith("spark:"):
                spark_key = key.removeprefix("spark:")
                if spark_key.startswith("spark.dataproc."):
                    result.dropped_properties.append(key)
                else:
                    result.spark_conf[spark_key] = str(value)
            else:
                result.dropped_properties.append(str(key))
        result.dropped_properties.sort()
    result.not_mapped.extend(
        name for name in sorted(config) if name not in ("worker_config", "master_config", "software_config")
    )
    return result
