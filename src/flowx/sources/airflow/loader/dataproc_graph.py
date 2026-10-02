"""Graph-level Dataproc analysis: which lifecycle and wait tasks Jobs compute absorbs.

A Dataproc DAG usually wraps its real workloads in infrastructure tasks: create a cluster, submit a
job, wait for it with a sensor, then delete the cluster. On Databricks the job cluster starts and
stops with the run and a task waits for its own workload, so those infrastructure tasks can go away.
They are removed only when that is provably safe: every job on the cluster migrates, nothing else
reads the removed task's output, and rewiring the edges does not change trigger-rule behavior.
Anything short of that stays in the graph as a failing placeholder with the reason attached.
"""

from __future__ import annotations

import ast
import re
from dataclasses import dataclass, field
from typing import Any

from flowx.sources.airflow import dataproc, templating
from flowx.sources.airflow import operators as ops

_XCOM_PULL = re.compile(r"xcom_pull\((?P<arguments>[^()]*)\)")
_JOB_ID_FROM_XCOM = re.compile(
    r"^\{\{\s*(?:ti|task_instance)\.xcom_pull\(\s*(?:task_ids\s*=\s*)?['\"](?P<task_id>[^'\"]+)['\"]\s*\)\s*\}\}$"
)
_CLUSTER_ACTIONS = frozenset(
    {dataproc.CREATE_CLUSTER, dataproc.DELETE_CLUSTER, dataproc.START_CLUSTER, dataproc.STOP_CLUSTER}
)
_CLUSTER_MUTATIONS = frozenset(
    {"DataprocUpdateClusterOperator", "DataprocScaleClusterOperator", "DataprocDiagnoseClusterOperator"}
)


@dataclass(slots=True, kw_only=True)
class DataprocPlan:
    """What the loader should do with a DAG's Dataproc tasks.

    Attributes:
        dropped: Capture ids of lifecycle and sensor tasks removed and rewired around.
        proofs: Transformation-ledger entries explaining every removal.
        clusters: Jobs compute settings for each migrated workload, keyed by capture id.
        retained_reasons: Why each Dataproc task that stays in the graph needs manual migration.
        disclosures: ``(capture_id, code, message)`` findings describing changed behavior.
        validation_failures: ``(capture_id, code, message)`` findings for malformed payloads, which fail
            reconciliation and block packaging.
    """

    dropped: set[str] = field(default_factory=set)
    proofs: list[dict[str, Any]] = field(default_factory=list)
    clusters: dict[str, dict[str, Any]] = field(default_factory=dict)
    retained_reasons: dict[str, str] = field(default_factory=dict)
    disclosures: list[tuple[str, str, str]] = field(default_factory=list)
    validation_failures: list[tuple[str, str, str]] = field(default_factory=list)


def _references_task(text: str, task_id: str) -> bool:
    quoted = re.compile(rf"['\"]{re.escape(task_id)}['\"]")
    return any(quoted.search(match.group("arguments")) for match in _XCOM_PULL.finditer(text))


def _references_output(text: str, variable: str) -> bool:
    return bool(re.search(rf"\b{re.escape(variable)}\.output\b|XComArg\(\s*{re.escape(variable)}\b", text))


def _consumers(
    variable: str,
    task_id: str,
    operator_texts: dict[str, str],
    function_texts: dict[str, str],
) -> set[str]:
    """Returns every task or function that reads *variable*'s output through XCom."""
    found = {
        other
        for other, text in operator_texts.items()
        if other != variable and (_references_task(text, task_id) or _references_output(text, variable))
    }
    found |= {
        f"function:{name}"
        for name, text in function_texts.items()
        if _references_task(text, task_id) or _references_output(text, variable)
    }
    return found


_DATAPROC_CLIENTS = re.compile(
    r"\b(?:DataprocHook|ClusterControllerClient|JobControllerClient|BatchControllerClient)\b"
)


def _cluster_users(
    cluster_name: str,
    candidates: dict[str, str],
    function_texts: dict[str, str],
) -> set[str]:
    """Returns every candidate task that names the cluster or drives Dataproc through a client.

    A candidate's text includes the bodies of module functions it references by name, so a
    ``python_callable`` that submits work to the cluster through the Dataproc hook still counts.
    """
    name_pattern = re.compile(rf"(?<![\w-]){re.escape(cluster_name)}(?![\w-])")
    users: set[str] = set()
    for task_id, text in candidates.items():
        called = " ".join(
            body
            for function_name, body in function_texts.items()
            if re.search(rf"\b{re.escape(function_name)}\b", text)
        )
        combined = f"{text} {called}"
        if name_pattern.search(combined) or _DATAPROC_CLIENTS.search(combined):
            users.add(task_id)
    return users


def _downstreams(variable: str, upstreams: dict[str, list[str]]) -> set[str]:
    return {other for other, parents in upstreams.items() if variable in parents}


def _removal_blocker(
    variable: str,
    operator: str,
    kwargs: dict[str, ast.expr],
    upstreams: dict[str, list[str]],
) -> str | None:
    """Returns why removing this task would change behavior, or ``None`` when removal is safe."""
    unconsumed = ops.unconsumed_kwargs(operator, kwargs)
    if unconsumed:
        return f"argument(s) {', '.join(sorted(unconsumed))} have no Databricks equivalent"
    rule = templating.trigger_rule_mapping(kwargs).rule
    if rule != "all_success" and _downstreams(variable, upstreams):
        return f"its trigger_rule {rule!r} gates downstream tasks, so rewiring around it would change when they run"
    return None


def plan_dataproc(
    operators: dict[str, tuple[str, str, dict[str, ast.expr]]],
    upstreams: dict[str, list[str]],
    functions: dict[str, ast.FunctionDef],
) -> DataprocPlan:
    """Decides which Dataproc tasks Jobs compute absorbs and what compute each workload runs on."""
    plan = DataprocPlan()
    dataproc_vars = {
        variable: (task_id, dataproc.canonical_name(operator), operator, kwargs)
        for variable, (task_id, operator, kwargs) in operators.items()
        if operator in dataproc.ALL_DATAPROC_CONSTRUCTS
    }
    if not dataproc_vars:
        return plan

    operator_texts = {
        variable: " ".join(ast.unparse(value) for value in kwargs.values())
        for variable, (_task_id, _operator, kwargs) in operators.items()
    }
    function_texts = {name: ast.unparse(definition) for name, definition in functions.items()}
    workloads = {
        variable: dataproc.translate_workload(operator, task_id, "", kwargs)
        for variable, (task_id, canonical, operator, kwargs) in dataproc_vars.items()
        if canonical in dataproc.WORKLOAD_OPERATORS
    }
    plan.validation_failures.extend(
        (variable, "dataproc_payload_engine_invalid", translation.validation_error)
        for variable, translation in workloads.items()
        if translation.validation_error is not None
    )
    collapsed_by_workload: dict[str, list[str]] = {variable: [] for variable in workloads}
    absorbed_workloads: set[str] = set()
    teardown_rules: dict[str, str] = {}

    def retain(variable: str, reason: str) -> None:
        plan.retained_reasons.setdefault(variable, reason)

    def cluster_name_of(kwargs: dict[str, ast.expr]) -> str | None:
        name = dataproc.static_value(kwargs.get("cluster_name"))
        return name if isinstance(name, str) else None

    groups: dict[str, list[str]] = {}
    for variable, (_task_id, canonical, _operator, kwargs) in dataproc_vars.items():
        if canonical in _CLUSTER_ACTIONS or canonical in _CLUSTER_MUTATIONS:
            name = cluster_name_of(kwargs)
            if name is None:
                retain(variable, f"{canonical} targets a cluster whose name is not static")
                continue
            groups.setdefault(name, []).append(variable)

    for name, members in sorted(groups.items()):
        creates = [member for member in members if dataproc_vars[member][1] == dataproc.CREATE_CLUSTER]
        cluster_workloads = sorted(variable for variable, result in workloads.items() if result.cluster_name == name)
        blocker: str | None = None
        cluster: dataproc.ClusterTranslation | None = None
        mutation = next((member for member in members if dataproc_vars[member][1] in _CLUSTER_MUTATIONS), None)
        if len(creates) != 1:
            blocker = "the DAG does not create this cluster exactly once, so it may be managed elsewhere"
        elif mutation is not None:
            blocker = f"task {dataproc_vars[mutation][0]!r} modifies the cluster between jobs"
        elif not cluster_workloads:
            blocker = "no job in this DAG is placed on the cluster, so its purpose is unknown"
        else:
            unmigrated = [variable for variable in cluster_workloads if workloads[variable].activity is None]
            if unmigrated:
                blocker = (
                    f"job {dataproc_vars[unmigrated[0]][0]!r} on this cluster needs manual migration and may "
                    "still need the cluster"
                )
        if blocker is None:
            cluster = dataproc.translate_cluster_config(dataproc_vars[creates[0]][3])
            blocker = cluster.blocking_reason
        if blocker is None:
            candidates = {
                operators[variable][0]: text
                for variable, text in operator_texts.items()
                if variable not in members and variable not in cluster_workloads
            }
            users = _cluster_users(name, candidates, function_texts)
            if users:
                blocker = f"task(s) {', '.join(sorted(users))} still use the cluster outside a migrated Dataproc job"
        if blocker is None:
            for member in members:
                task_id, _canonical, operator, kwargs = dataproc_vars[member]
                consumers = _consumers(member, task_id, operator_texts, function_texts)
                if consumers:
                    blocker = f"task {task_id!r} is read by {', '.join(sorted(consumers))}"
                    break
                member_blocker = _removal_blocker(member, operator, kwargs, upstreams)
                if member_blocker:
                    blocker = f"task {task_id!r}: {member_blocker}"
                    break
        if blocker is not None or cluster is None:
            for member in members:
                retain(
                    member,
                    f"Dataproc cluster {name!r} was not absorbed into Jobs compute: {blocker}. Keep the action "
                    "on Google Cloud, or design the Databricks compute and remove it deliberately.",
                )
            continue

        plan.dropped.update(members)
        absorbed_workloads.update(cluster_workloads)
        for member in members:
            rule = templating.trigger_rule_mapping(dataproc_vars[member][3]).rule
            if dataproc_vars[member][1] == dataproc.DELETE_CLUSTER and rule != "all_success":
                teardown_rules[member] = rule
        for workload in cluster_workloads:
            collapsed_by_workload[workload].extend(members)
            compute: dict[str, Any] = {}
            if cluster.num_workers is not None:
                compute["num_workers"] = cluster.num_workers
            spark_conf = {**cluster.spark_conf, **workloads[workload].spark_conf}
            if spark_conf:
                compute["spark_conf"] = spark_conf
            if compute:
                plan.clusters[workload] = {**compute, "_bind_default_cluster": True}
            if cluster.dropped_properties or cluster.not_mapped:
                details = [
                    *(f"property {item} removed" for item in cluster.dropped_properties),
                    *(f"{item} not mapped" for item in cluster.not_mapped),
                ]
                plan.disclosures.append(
                    (
                        workload,
                        "dataproc_cluster_settings_not_mapped",
                        f"Dataproc cluster {name!r} settings without a Jobs compute equivalent: {'; '.join(details)}. "
                        "Choose the node type and Databricks Runtime explicitly and redesign any removed "
                        "setting that affects behavior.",
                    )
                )
        plan.proofs.append(
            {
                "code": "dataproc_cluster_absorbed",
                "cluster_name": name,
                "capture_ids": sorted(members),
                "workload_capture_ids": cluster_workloads,
                "num_workers": cluster.num_workers,
                "spark_conf": dict(sorted(cluster.spark_conf.items())),
                "dropped_properties": list(cluster.dropped_properties),
                "not_mapped": list(cluster.not_mapped),
            }
        )

    for variable, result in workloads.items():
        if result.activity is None or variable in plan.clusters or not result.spark_conf:
            continue
        plan.clusters[variable] = {"spark_conf": dict(result.spark_conf), "_bind_default_cluster": True}

    paired_workloads: dict[str, list[str]] = {}
    sensor_targets: dict[str, str] = {}
    task_to_variable = {task_id: variable for variable, (task_id, _c, _o, _k) in dataproc_vars.items()}
    for variable, (_task_id, canonical, _operator, kwargs) in dataproc_vars.items():
        if canonical == dataproc.JOB_SENSOR:
            reference = dataproc.static_value(kwargs.get("dataproc_job_id"))
            match = _JOB_ID_FROM_XCOM.match(reference) if isinstance(reference, str) else None
            target = task_to_variable.get(match.group("task_id")) if match else None
            if target is None or dataproc_vars[target][1] != dataproc.SUBMIT_JOB:
                retain(
                    variable,
                    "DataprocJobSensor does not wait on a job submitted in this DAG (its dataproc_job_id is not "
                    "an xcom_pull of a submission task); keep the wait or translate it manually.",
                )
                continue
        elif canonical == dataproc.BATCH_SENSOR:
            batch_id = dataproc.static_value(kwargs.get("batch_id"))
            target = next(
                (
                    other
                    for other, (_t, other_canonical, _o, other_kwargs) in dataproc_vars.items()
                    if other_canonical == dataproc.CREATE_BATCH
                    and isinstance(batch_id, str)
                    and dataproc.static_value(other_kwargs.get("batch_id")) == batch_id
                ),
                None,
            )
            if target is None:
                retain(
                    variable,
                    "DataprocBatchSensor's batch_id does not match the batch_id of a batch created in this DAG; "
                    "keep the wait or translate it manually.",
                )
                continue
        else:
            continue
        sensor_targets[variable] = target
        paired_workloads.setdefault(target, []).append(variable)

    for sensor, target in sorted(sensor_targets.items()):
        task_id, canonical, operator, kwargs = dataproc_vars[sensor]
        target_task_id, target_canonical, _target_operator, target_kwargs = dataproc_vars[target]
        blocker = None
        if workloads[target].activity is None:
            blocker = f"the workload {target_task_id!r} it waits on needs manual migration"
        elif len(paired_workloads[target]) != 1:
            blocker = f"more than one sensor waits on {target_task_id!r}"
        elif (
            target_canonical == dataproc.SUBMIT_JOB
            and dataproc.static_value(target_kwargs.get("asynchronous")) is not True
        ):
            blocker = f"{target_task_id!r} is not submitted asynchronously, so the sensor does not pair with it"
        else:
            allowed = {sensor} if target_canonical == dataproc.SUBMIT_JOB else set()
            other_consumers = _consumers(target, target_task_id, operator_texts, function_texts) - allowed
            if other_consumers:
                blocker = f"the output of {target_task_id!r} is also read by {', '.join(sorted(other_consumers))}"
            elif _consumers(sensor, task_id, operator_texts, function_texts):
                blocker = "another task reads the sensor's output"
            else:
                blocker = _removal_blocker(sensor, operator, kwargs, upstreams)
        if blocker is not None:
            retain(sensor, f"{canonical} was not collapsed into native task completion: {blocker}.")
            continue
        plan.dropped.add(sensor)
        collapsed_by_workload[target].append(sensor)
        plan.proofs.append(
            {
                "code": "dataproc_sensor_collapsed",
                "capture_id": sensor,
                "paired_capture_id": target,
                "pairing": "dataproc_job_id" if canonical == dataproc.JOB_SENSOR else "batch_id",
            }
        )

    for variable, result in sorted(workloads.items()):
        if result.activity is None:
            continue
        task_id, canonical, _operator, kwargs = dataproc_vars[variable]
        if result.placement is not None and variable not in absorbed_workloads:
            plan.disclosures.append(
                (
                    variable,
                    "dataproc_placement_not_migrated",
                    f"Task {task_id!r} was placed on {result.placement}, which is not a cluster this DAG creates "
                    "and flowx absorbed into Jobs compute. It now runs on the bundle's default job cluster; size "
                    "that cluster to match the Dataproc compute it replaces.",
                )
            )
        if result.artifact_uris:
            plan.disclosures.append(
                (
                    variable,
                    "dataproc_artifacts_require_access",
                    f"Task {task_id!r} runs code from {', '.join(result.artifact_uris)}. Copy the source into "
                    "the bundle, or grant the job's Databricks identity access to these locations; Dataproc "
                    "service accounts and impersonation chains do not carry over.",
                )
            )
        if result.dropped_properties or result.not_mapped:
            details = [
                *(f"property {item} removed" for item in result.dropped_properties),
                *(f"{item} not mapped" for item in result.not_mapped),
            ]
            plan.disclosures.append(
                (
                    variable,
                    "dataproc_settings_not_mapped",
                    f"Task {task_id!r} payload settings without a Databricks equivalent: {'; '.join(details)}.",
                )
            )
        removed = sorted(dataproc_vars[member][0] for member in collapsed_by_workload[variable])
        asynchronous = dataproc.static_value(kwargs.get("asynchronous")) is True
        waited = any(member in sensor_targets for member in collapsed_by_workload[variable])
        notes: list[str] = []
        if removed:
            notes.append(f"removed and rewired around {', '.join(removed)}")
        for member in collapsed_by_workload[variable]:
            if member in teardown_rules:
                notes.append(
                    f"teardown {dataproc_vars[member][0]!r} ran under trigger_rule {teardown_rules[member]!r}; "
                    "the job cluster now terminates with the run"
                )
        if canonical == dataproc.SUBMIT_JOB and asynchronous and not waited:
            notes.append("the asynchronous submission had no paired sensor, so downstream tasks now wait for it")
        if notes:
            plan.disclosures.append(
                (
                    variable,
                    "dataproc_execution_envelope_changed",
                    f"Task {task_id!r} now runs natively on Databricks: {'; '.join(notes)}. Task retries rerun the "
                    "workload instead of resubmitting to Dataproc, and timeouts and cancellation follow the task.",
                )
            )

    distinct_compute = {repr(sorted(compute.items())) for compute in plan.clusters.values()}
    if len(distinct_compute) > 1:
        first = sorted(plan.clusters)[0]
        plan.disclosures.append(
            (
                first,
                "dataproc_shared_job_cluster",
                "Dataproc workloads in this DAG used different compute settings, but the bundle runs them on one "
                "shared job cluster; split the job clusters before deploying.",
            )
        )
    return plan
