"""Converts a translated Pipeline IR into a PreparedWorkflow."""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass, field
from typing import Any

from orchestra.models.dab import DabNotebook, ParameterApproximation, SecretInstruction, SetupTask
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
    parameter_approximations: list[ParameterApproximation] = field(default_factory=list)


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
    parameter_approximations: list[ParameterApproximation] = field(default_factory=list)
    # C-10 (SCHED-001): serialised schedule / trigger spec the bundler
    # renders as ``schedule:`` / ``trigger:`` on the emitted DAB job.
    schedule: dict[str, Any] | None = None


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

    if activity.existing_cluster_id:
        task["existing_cluster_id"] = activity.existing_cluster_id

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
    elif type(activity) is ForEachActivity:
        # C-06 (VAREX-004): inner-job parameter collector needs the parent's
        # variable -> setter mapping so @variables('X') references inside the
        # ForEach body route through {{tasks.X.values.Y}} rather than an
        # undeclared {{job.parameters.X}}.
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
    parameter_approximations: tuple[ParameterApproximation, ...] = ()


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
        parameter_approximations=artifacts.parameter_approximations + tuple(prepared.parameter_approximations),
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
        # C-04 (NB-ITER2-4 / LSC2-001): walk into IfCondition / Switch /
        # ForEach branches so the workflow's cluster_hints aggregation
        # picks up cluster config on activities nested inside compound
        # activities.  Without this the default Standard_DS3_v2 / 15.4.x
        # fallback ships even when the inner notebook has an explicit LS.
        for nested_activity in _iter_activity_with_descendants(activity):
            if nested_activity.cluster:
                cluster_hints.append(dict(nested_activity.cluster))

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

    # VAREX3-003: emit a manual_variable_rollup SetupTask whenever a sibling
    # IfCondition / Switch / SetVariable reads a variable that is only
    # mutated inside a ForEach inner job.  ADF semantics treat the post-
    # ForEach read as "latest committed value" but that value is unreachable
    # across the run_job_task boundary in DAB.  Surfacing the warning lets
    # the user add a roll-up notebook before the dependent activity runs.
    cross_scope_rollups = _detect_cross_foreach_variable_reads(pipeline.tasks)
    setup_tasks_out = _dedupe_setup_tasks(artifacts.setup_tasks)
    setup_tasks_out.extend(cross_scope_rollups)
    # C-36 (SCHED4-001): emit a manual_schedule_time_of_day SetupTask
    # whenever the trigger.periodic schedule carries hours/minutes/weekDays
    # that the periodic primitive can't encode.  SETUP.md picks it up so
    # the user can manually add the time-of-day to the cron expression.
    if pipeline.schedule and pipeline.schedule.get("time_of_day_note"):
        setup_tasks_out.append(
            SetupTask(
                type="manual_schedule_time_of_day",
                config={
                    "pipeline": pipeline.name,
                    "frequency": pipeline.schedule.get("unit", ""),
                    "interval": pipeline.schedule.get("interval", ""),
                    "time_of_day_note": pipeline.schedule.get("time_of_day_note"),
                },
            )
        )

    # C-39 (LSC4-004): when any cluster hint references an ADF
    # authentication mode that has no direct Databricks equivalent (MSI,
    # CredentialReference) the bundle's default_cluster silently uses
    # ``single_user_name: ${workspace.current_user.userName}``.  Surface
    # a manual_credential SetupTask so SETUP.md flags the substitution.
    seen_auth: set[tuple[str, str]] = set()
    for hint in cluster_hints:
        auth = hint.get("_adf_authentication") or ""
        cred = hint.get("_adf_credential_reference") or ""
        if not auth and not cred:
            continue
        key = (str(auth), str(cred))
        if key in seen_auth:
            continue
        seen_auth.add(key)
        setup_tasks_out.append(
            SetupTask(
                type="manual_credential",
                config={
                    "source": pipeline.name,
                    "linked_service": cred or "<MSI>",
                    "authentication": auth or "CredentialReference",
                    "note": (
                        "ADF cluster auth has no Databricks equivalent.  The "
                        "default_cluster runs as ${workspace.current_user.userName}; "
                        "swap to a service principal via single_user_name or "
                        "set run_as.service_principal_name on the job."
                    ),
                },
            )
        )

    return PreparedWorkflow(
        name=pipeline.name,
        tasks=all_tasks,
        notebooks=list(artifacts.notebooks),
        secrets=unique_secrets,
        setup_tasks=setup_tasks_out,
        inner_workflows=list(artifacts.inner_workflows),
        cluster_hints=cluster_hints,
        pipeline_resources=list(artifacts.pipeline_resources),
        parameter_approximations=list(artifacts.parameter_approximations),
        schedule=pipeline.schedule,
    )


def _detect_cross_foreach_variable_reads(activities: list[Activity]) -> list[SetupTask]:
    """Returns SetupTasks for variables mutated inside a ForEach but read outside.

    VAREX3-003: when a SetVariable for `X` lives only inside a ForEach
    inner-job, a sibling task reading @variables('X') gets the stale init
    value (post-ForEach reads cannot cross the run_job_task boundary in
    DAB).  Surfacing this as a manual_variable_rollup SetupTask gives the
    user a documented workaround (add a roll-up notebook that copies the
    final value to a parent-scope task value).
    """
    import re

    var_ref_pattern = re.compile(r"@?variables\(\s*'([^']+)'\s*\)", re.IGNORECASE)

    # Index variable -> set of ForEach activity names that contain the setter
    # so we can name the parent in the warning message.
    var_set_inside_foreach: dict[str, list[str]] = {}
    for activity in activities:
        if isinstance(activity, ForEachActivity):
            for inner in activity.inner_activities:
                if isinstance(inner, SetVariableActivity):
                    var_set_inside_foreach.setdefault(inner.variable_name, []).append(activity.task_key)

    if not var_set_inside_foreach:
        return []

    # Identify variables that are also set OUTSIDE any ForEach -- those are
    # not cross-scope dangers because the parent always has a fresh setter
    # to point at.
    set_outside: set[str] = set()
    for activity in activities:
        if isinstance(activity, SetVariableActivity):
            set_outside.add(activity.variable_name)

    dangerous_vars = {name: parents for name, parents in var_set_inside_foreach.items() if name not in set_outside}
    if not dangerous_vars:
        return []

    # Now find sibling reads of those variables.  Walk every top-level
    # activity that is NOT the originating ForEach and collect refs.
    flagged: dict[str, str] = {}  # variable -> parent task_key

    def _read_refs(text: str) -> set[str]:
        if not isinstance(text, str):
            return set()
        return {m.group(1) for m in var_ref_pattern.finditer(text)}

    def _walk_activity_strings(activity: Activity) -> Iterable[str]:
        # Yield every string-like field the variable might appear in.
        if isinstance(activity, IfConditionActivity):
            yield activity.left or ""
            yield activity.right or ""
        if isinstance(activity, SwitchActivity):
            yield activity.on_expression or ""
        if isinstance(activity, SetVariableActivity):
            yield activity.variable_value or ""
        if isinstance(activity, NotebookActivity):
            for value in (activity.base_parameters or {}).values():
                if isinstance(value, str):
                    yield value
        if isinstance(activity, WebActivity):
            yield activity.url or ""
            if isinstance(activity.body, str):
                yield activity.body

    for activity in activities:
        # Skip ForEach themselves (siblings only).
        if isinstance(activity, ForEachActivity):
            continue
        for text in _walk_activity_strings(activity):
            for var_name in _read_refs(text):
                if var_name in dangerous_vars and var_name not in flagged:
                    flagged[var_name] = dangerous_vars[var_name][0]

    return [
        SetupTask(
            type="manual_variable_rollup",
            config={
                "variable_name": var_name,
                "parent_foreach": parent_key,
                "message": (
                    f"Variable '{var_name}' is mutated inside ForEach "
                    f"'{parent_key}' but read in a sibling task.  Task "
                    f"values cannot cross run_job_task boundaries; add a "
                    f"roll-up notebook that copies the final value to a "
                    f"parent-scope task value before the sibling runs."
                ),
            },
        )
        for var_name, parent_key in sorted(flagged.items())
    ]


def _iter_activity_with_descendants(activity: Activity) -> Iterable[Activity]:
    """Yields *activity* and every nested branch activity (BFS).

    C-04 (NB-ITER2-4 / LSC2-001): IfCondition / Switch / ForEach activities
    nest sub-activities in branch fields (``if_true_activities``,
    ``if_false_activities``, ``inner_activities``, ``cases[].activities``,
    ``default_activities``).  ``prepare_workflow`` previously only saw the
    top-level tasks, so cluster hints carried by a deeply nested
    NotebookActivity were dropped.
    """
    queue: list[Activity] = [activity]
    while queue:
        current = queue.pop(0)
        yield current
        for attr in ("inner_activities", "if_true_activities", "if_false_activities"):
            nested = getattr(current, attr, None) or []
            queue.extend(nested)
        if isinstance(current, SwitchActivity):
            for case_item in current.cases:
                queue.extend(case_item.activities)
            queue.extend(current.default_activities)


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
