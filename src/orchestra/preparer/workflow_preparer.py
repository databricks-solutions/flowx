"""Converts a translated Pipeline IR into a PreparedWorkflow."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from orchestra.models.dab import DabNotebook, SecretInstruction, SetupTask
from orchestra.models.ir import (
    Activity,
    AppendVariableActivity,
    CopyActivity,
    DeleteActivity,
    ExecutePipelineActivity,
    FilterActivity,
    ForEachActivity,
    IfConditionActivity,
    LookupActivity,
    MotifActivity,
    NotebookActivity,
    Pipeline,
    PlaceholderActivity,
    RunJobActivity,
    SetVariableActivity,
    SparkJarActivity,
    SparkPythonActivity,
    SwitchActivity,
    UnsupportedActivity,
    WaitActivity,
    WebActivity,
)


@dataclass(slots=True, kw_only=True)
class PreparedActivity:
    """Result of preparing a single activity for DAB deployment."""

    task: dict[str, Any]
    extra_tasks: list[dict[str, Any]] = field(default_factory=list)
    notebooks: list[DabNotebook] = field(default_factory=list)
    secrets: list[SecretInstruction] = field(default_factory=list)
    setup_tasks: list[SetupTask] = field(default_factory=list)
    inner_workflows: list[PreparedWorkflow] = field(default_factory=list)
    # Switch renames its first case from ``<activity>`` to
    # ``<activity>_case_<value>``; ``prepare_workflow`` reads this map to
    # rewrite ``depends_on`` edges that referenced the original key.
    task_key_remap: dict[str, str] = field(default_factory=dict)
    # Lakeflow pipeline resources (e.g. Lakeflow Connect managed
    # ingestion pipelines) the bundle writer emits under
    # ``resources/pipelines/<resource_key>.yml``.  Each entry is a dict
    # with ``resource_key`` and ``definition`` keys.
    pipeline_resources: list[dict[str, Any]] = field(default_factory=list)


@dataclass(slots=True, kw_only=True)
class PreparedWorkflow:
    """A fully prepared workflow ready for DAB bundle generation."""

    name: str
    tasks: list[dict[str, Any]]
    notebooks: list[DabNotebook]
    secrets: list[SecretInstruction]
    setup_tasks: list[SetupTask]
    inner_workflows: list[PreparedWorkflow] = field(default_factory=list)
    parameters: list[dict[str, Any]] = field(default_factory=list)
    cluster_hints: list[dict[str, Any]] = field(default_factory=list)
    pipeline_resources: list[dict[str, Any]] = field(default_factory=list)


def run_if_from_adf_outcomes(outcomes: list[str | None]) -> str | None:
    """Maps a set of ADF dependency-edge outcomes to a single DAB ``run_if``."""
    normalised = [outcome for outcome in outcomes if outcome]
    if not normalised:
        return None
    if any(outcome in ("Completed", "Skipped") for outcome in normalised):
        return "ALL_DONE"
    if any(outcome == "Failed" for outcome in normalised):
        return "AT_LEAST_ONE_FAILED"
    return None


def build_common_task_fields(activity: Activity) -> dict[str, Any]:
    """Builds the task-level fields shared by every DAB task type.

    Returns:
        A dict with ``task_key``, ``depends_on``, ``timeout_seconds``, and
        retry fields populated from the activity.
    """
    task: dict[str, Any] = {"task_key": activity.task_key}

    if activity.depends_on:
        task["depends_on"] = [{"task_key": dep.task_key} for dep in activity.depends_on]
        run_if = run_if_from_adf_outcomes([dep.outcome for dep in activity.depends_on])
        if run_if:
            task["run_if"] = run_if

    if activity.timeout_seconds is not None and activity.timeout_seconds > 0:
        task["timeout_seconds"] = activity.timeout_seconds

    if activity.max_retries is not None and activity.max_retries > 0:
        task["retry_on_timeout"] = True
        task["max_retries"] = activity.max_retries
        if activity.min_retry_interval_millis is not None:
            task["min_retry_interval_millis"] = activity.min_retry_interval_millis

    if activity.description:
        task["description"] = activity.description

    return task


def prepare_activity(
    activity: Activity,
    *,
    scope: str = "",
    variable_task_keys: dict[str, str] | None = None,
) -> PreparedActivity:
    """Dispatches to the appropriate activity preparer based on activity type."""
    from orchestra.preparer.activity_preparers import (
        append_variable,
        copy,
        databricks_job,
        delete,
        execute_pipeline,
        filter,
        for_each,
        if_condition,
        lookup,
        motif,
        notebook,
        set_variable,
        spark_jar,
        spark_python,
        switch,
        wait,
        web_activity,
    )

    dispatch: dict[type, Any] = {
        NotebookActivity: notebook.prepare,
        SparkJarActivity: spark_jar.prepare,
        SparkPythonActivity: spark_python.prepare,
        CopyActivity: copy.prepare,
        LookupActivity: lookup.prepare,
        WebActivity: web_activity.prepare,
        DeleteActivity: delete.prepare,
        SetVariableActivity: set_variable.prepare,
        FilterActivity: filter.prepare,
        AppendVariableActivity: append_variable.prepare,
        ForEachActivity: for_each.prepare,
        IfConditionActivity: if_condition.prepare,
        ExecutePipelineActivity: execute_pipeline.prepare,
        RunJobActivity: databricks_job.prepare,
        SwitchActivity: switch.prepare,
        WaitActivity: wait.prepare,
        MotifActivity: motif.prepare,
    }

    preparer_fn = dispatch.get(type(activity))
    if preparer_fn is None:
        if isinstance(activity, (PlaceholderActivity, UnsupportedActivity)):
            prepared = _prepare_placeholder(activity)
        else:
            raise ValueError(
                f"No preparer registered for activity type {type(activity).__name__} (task_key={activity.task_key!r})"
            )
    elif type(activity) is NotebookActivity:
        prepared = preparer_fn(activity, scope=scope, variable_task_keys=variable_task_keys)
    elif type(activity) is AppendVariableActivity:
        prepared = preparer_fn(activity, scope=scope, variable_task_keys=variable_task_keys)
    else:
        prepared = preparer_fn(activity, scope=scope)

    prepared.task = _stamp_compute_mode(prepared.task, activity.compute_mode)
    return prepared


def _stamp_compute_mode(task: dict[str, Any], compute_mode: str | None) -> dict[str, Any]:
    """Returns a new task dict carrying a private compute-mode marker.

    Args:
        task: Task dict produced by a per-type preparer.  Not mutated.
        compute_mode: Value from ``Activity.compute_mode``.  When ``None``
            the marker is omitted and the original *task* is returned
            unchanged.

    Returns:
        A new dict shallow-copied from *task* with the ``_compute_mode``
        marker attached, or *task* itself when no marker applies.
    """
    if not compute_mode:
        return task
    return {**task, "_compute_mode": compute_mode}


def _prepare_placeholder(activity: Activity) -> PreparedActivity:
    """Returns a PreparedActivity with a stub notebook for an unsupported activity."""
    task = build_common_task_fields(activity)

    if isinstance(activity, PlaceholderActivity):
        comment = activity.comment or "This activity requires manual implementation."
        original_type = activity.original_type
    elif isinstance(activity, UnsupportedActivity):
        comment = activity.reason or "This activity type is not supported."
        original_type = activity.original_type
    else:
        comment = "Unknown activity type."
        original_type = type(activity).__name__

    notebook_name = f"{activity.task_key}.py"
    notebook_path = f"notebooks/{notebook_name}"

    content = (
        "# Databricks notebook source\n"
        "# MAGIC %md\n"
        f"# MAGIC # Placeholder: {activity.name}\n"
        "# MAGIC\n"
        f"# MAGIC Original ADF activity type: **{original_type}**\n"
        "# MAGIC\n"
        f"# MAGIC {comment}\n"
        "\n# COMMAND ----------\n\n"
        f"raise NotImplementedError(\"Activity '{activity.name}' ({original_type}) requires manual implementation.\")\n"
    )

    task["notebook_task"] = {
        "notebook_path": f"../src/{notebook_path}",
    }

    notebook = DabNotebook(
        relative_path=notebook_path,
        content=content,
    )

    return PreparedActivity(task=task, notebooks=[notebook])


@dataclass(frozen=True, slots=True)
class PreparedArtifacts:
    """Immutable accumulator for the artifact lists a workflow collects."""

    notebooks: tuple[DabNotebook, ...] = ()
    secrets: tuple[SecretInstruction, ...] = ()
    setup_tasks: tuple[SetupTask, ...] = ()
    inner_workflows: tuple[PreparedWorkflow, ...] = ()
    pipeline_resources: tuple[dict[str, Any], ...] = ()


def merge_prepared_artifacts(
    artifacts: PreparedArtifacts,
    prepared: PreparedActivity,
) -> PreparedArtifacts:
    """Return a new :class:`PreparedArtifacts` extended with *prepared*'s artifacts."""
    return PreparedArtifacts(
        notebooks=artifacts.notebooks + tuple(prepared.notebooks),
        secrets=artifacts.secrets + tuple(prepared.secrets),
        setup_tasks=artifacts.setup_tasks + tuple(prepared.setup_tasks),
        inner_workflows=artifacts.inner_workflows + tuple(prepared.inner_workflows),
        pipeline_resources=artifacts.pipeline_resources + tuple(prepared.pipeline_resources),
    )


def prepare_workflow(pipeline: Pipeline) -> PreparedWorkflow:
    """Converts a Pipeline IR into a PreparedWorkflow ready for the DAB bundle writer."""
    all_tasks: list[dict[str, Any]] = []
    artifacts = PreparedArtifacts()
    cluster_hints: list[dict[str, Any]] = []
    task_key_remap: dict[str, str] = {}

    scope = pipeline.name
    # Updated in pipeline-declaration order so AppendVariable sees the
    # most-recent prior writer of each variable.
    variable_task_keys_map: dict[str, str] = {}

    for activity in pipeline.tasks:
        prepared = prepare_activity(activity, scope=scope, variable_task_keys=variable_task_keys_map)
        all_tasks.append(prepared.task)
        all_tasks.extend(prepared.extra_tasks)
        artifacts = merge_prepared_artifacts(artifacts, prepared)
        task_key_remap.update(prepared.task_key_remap)
        if activity.cluster:
            cluster_hints.append(dict(activity.cluster))

        if isinstance(activity, (SetVariableActivity, AppendVariableActivity)):
            variable_task_keys_map[activity.variable_name] = activity.task_key

    if task_key_remap:
        for task in all_tasks:
            for dep in task.get("depends_on", []) or []:
                original_key = dep.get("task_key")
                if original_key in task_key_remap:
                    dep["task_key"] = task_key_remap[original_key]

    seen_secrets: set[tuple[str, str]] = set()
    unique_secrets: list[SecretInstruction] = []
    for secret in artifacts.secrets:
        secret_id = (secret.scope, secret.key)
        if secret_id not in seen_secrets:
            seen_secrets.add(secret_id)
            unique_secrets.append(secret)

    return PreparedWorkflow(
        name=pipeline.name,
        tasks=all_tasks,
        notebooks=list(artifacts.notebooks),
        secrets=unique_secrets,
        setup_tasks=_dedupe_setup_tasks(artifacts.setup_tasks),
        inner_workflows=list(artifacts.inner_workflows),
        cluster_hints=cluster_hints,
        pipeline_resources=list(artifacts.pipeline_resources),
    )


def _dedupe_setup_tasks(setup_tasks: tuple[SetupTask, ...]) -> list[SetupTask]:
    """Returns the setup-task list with duplicates collapsed by identifying config.

    Args:
        setup_tasks: Setup tasks aggregated across every prepared
            activity in the workflow.

    Returns:
        List with at most one entry per logical resource: connection
        tasks dedupe by ``connection_name``; volume tasks dedupe by
        ``volume_name``.  Other task types are kept as-is so the
        downstream setup-notebook generator still sees them.
    """
    seen: set[tuple[str, str]] = set()
    unique: list[SetupTask] = []
    for task in setup_tasks:
        key = _setup_task_dedupe_key(task)
        if key is None:
            unique.append(task)
            continue
        if key in seen:
            continue
        seen.add(key)
        unique.append(task)
    return unique


def _setup_task_dedupe_key(task: SetupTask) -> tuple[str, str] | None:
    """Returns the identity tuple used to dedupe a :class:`SetupTask`.

    Args:
        task: A setup task collected from a prepared activity.

    Returns:
        A ``(type, identifier)`` tuple for known setup task types, or
        ``None`` when the task type does not have a stable identity
        (which preserves the original behaviour of emitting each
        occurrence).
    """
    config = task.config or {}
    if task.type == "connection":
        name = config.get("connection_name")
        return (task.type, str(name)) if name else None
    if task.type == "volume":
        name = config.get("volume_name")
        return (task.type, str(name)) if name else None
    return None
