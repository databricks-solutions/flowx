"""Preparer for NotebookActivity -> notebook_task dict."""

from __future__ import annotations

from typing import TYPE_CHECKING

from orchestra.models.dab import DabNotebook, ParameterApproximation, SetupTask
from orchestra.models.ir import TranslationContext
from orchestra.parser.expression_parser import resolve_expression, resolve_interpolated_string
from orchestra.preparer.activity_preparers.naming import notebook_filename, workspace_notebook_filename
from orchestra.preparer.workflow_preparer import PreparedActivity, build_common_task_fields
from orchestra.preparer.workspace_downloader import download_notebook, workspace_downloads_enabled

if TYPE_CHECKING:
    from orchestra.models.ir import NotebookActivity


def _notebook_placeholder(original_path: str, activity_name: str, filename: str) -> str:
    """Return placeholder notebook content with manual-export instructions."""
    return (
        "# Databricks notebook source\n"
        "# MAGIC %md\n"
        f"# MAGIC # {activity_name}\n"
        "# MAGIC\n"
        f"# MAGIC **Source workspace path**: `{original_path}`\n"
        "# MAGIC\n"
        f"# MAGIC This notebook was referenced by ADF pipeline activity `{activity_name}`.\n"
        "# MAGIC Replace this placeholder with the actual notebook content.\n"
        "# MAGIC\n"
        "# MAGIC Export from workspace:\n"
        "# MAGIC ```\n"
        f'# MAGIC databricks workspace export "{original_path}" --format SOURCE -o src/notebooks/{filename}\n'
        "# MAGIC ```\n"
        "\n# COMMAND ----------\n\n"
        "# TODO: Replace this placeholder with the exported notebook content\n"
        "raise NotImplementedError(\n"
        f'    f"Export notebook from workspace: {original_path}"\n'
        ")\n"
    )


def _resolve_base_parameters(
    params: dict[str, str],
    *,
    variable_task_keys: dict[str, str] | None = None,
    existing_notebook: bool = False,
) -> dict[str, str]:
    """Resolves ADF expressions in ``base_parameters`` to DAB-compatible values.

    For *existing* notebooks (absolute workspace paths) we keep ``notebook_code``
    parameters as their raw original expression so the downstream
    ``_extract_manual_parameters_from_existing_notebook_tasks`` scanner can
    pick them up and surface them in SETUP.md -- orchestra cannot patch the
    notebook body, so the user has to compute the value in-line themselves.

    For bundle-generated notebooks the embedded notebook body owns the
    runtime computation (via ``required_parameters``), so ``notebook_code``
    parameters are dropped here.
    """
    context = TranslationContext()
    resolved: dict[str, str] = {}
    for key, value in params.items():
        result = resolve_expression(value, context, variable_task_keys=variable_task_keys)
        if result is not None and result.kind in ("literal", "dab_ref"):
            resolved[key] = result.value
            continue
        if result is not None and result.kind == "notebook_code":
            if existing_notebook:
                resolved[key] = _raw_expression(value)
            continue
        if isinstance(value, dict) and value.get("type") == "Expression" and "value" in value:
            resolved[key] = str(value["value"])
        else:
            resolved[key] = str(value)
    return resolved


def _raw_expression(value: object) -> str:
    """Returns the original ADF expression text for an unresolved parameter."""
    if isinstance(value, dict) and value.get("type") == "Expression" and "value" in value:
        return str(value["value"])
    return str(value)


def _resolve_notebook_path(path: str) -> str:
    """Resolves any ADF expression embedded in a notebook workspace path."""
    context = TranslationContext()
    if "@{" in path:
        return resolve_interpolated_string(path, context)
    if path.startswith("@"):
        result = resolve_expression(path, context)
        if result is not None and result.kind in ("dab_ref", "literal"):
            return result.value
    return path


_DISPATCH_STUB_WIDGET = "notebook_path"


def _dispatch_stub_notebook(activity: NotebookActivity, filename: str) -> str:
    """Returns the body of a dynamic-dispatch stub notebook.

    C-28 (NB-ITER4-001): when the ADF ``notebookPath`` is a runtime
    expression the translator couldn't reduce (e.g. ``@trim(json(...))``),
    the bundle ships a stub that reads ``notebook_path`` from a widget and
    calls ``dbutils.notebook.run()`` to dispatch to whatever the workflow
    resolved at runtime.  The base_parameters dict ferries the rest of the
    widgets through.
    """
    expression = activity.notebook_path_expression or "(unspecified)"
    return (
        "# Databricks notebook source\n"
        "# MAGIC %md\n"
        f"# MAGIC # Dispatch stub: {activity.name}\n"
        "# MAGIC\n"
        "# MAGIC The ADF activity's `notebookPath` is a runtime expression that\n"
        "# MAGIC orchestra could not resolve at translation time.\n"
        "# MAGIC\n"
        f"# MAGIC **Original expression**: `{expression}`\n"
        "# MAGIC\n"
        "# MAGIC This stub reads the resolved notebook path from the\n"
        f"# MAGIC `{_DISPATCH_STUB_WIDGET}` widget and dispatches via\n"
        "# MAGIC `dbutils.notebook.run`.  See SETUP.md → *Dynamic notebook dispatch*.\n"
        "\n# COMMAND ----------\n\n"
        f"dbutils.widgets.text('{_DISPATCH_STUB_WIDGET}', '')\n"
        f"target_notebook = dbutils.widgets.get('{_DISPATCH_STUB_WIDGET}')\n"
        "if not target_notebook:\n"
        "    raise ValueError(\n"
        f'        "Dispatch stub for activity {activity.name!r} requires a runtime "\n'
        f"        \"value for the '{_DISPATCH_STUB_WIDGET}' widget.  See SETUP.md.\"\n"
        "    )\n"
        "\n"
        "# Forward every other widget through to the resolved notebook so it\n"
        "# receives the same base_parameters the workflow declared.\n"
        f"_passthrough_widgets = [w for w in dbutils.widgets.getAll() if w != '{_DISPATCH_STUB_WIDGET}']\n"
        "arguments = {name: dbutils.widgets.get(name) for name in _passthrough_widgets}\n"
        "\n"
        "dbutils.notebook.run(target_notebook, timeout_seconds=0, arguments=arguments)\n"
    )


def prepare(
    activity: NotebookActivity,
    *,
    scope: str = "",
    variable_task_keys: dict[str, str] | None = None,
) -> PreparedActivity:
    """Converts a NotebookActivity into a DAB notebook_task definition."""
    # C-28 (NB-ITER4-001): dynamic notebookPath -> emit a dispatch stub.
    if activity.notebook_path_unresolved:
        return _prepare_dispatch_stub(activity, variable_task_keys=variable_task_keys)

    resolved_path = _resolve_notebook_path(activity.notebook_path)
    task = build_common_task_fields(activity)
    is_existing_notebook = resolved_path.startswith("/")

    base_parameters: dict[str, str] | None = None
    if activity.base_parameters:
        base_parameters = _resolve_base_parameters(
            dict(activity.base_parameters),
            variable_task_keys=variable_task_keys,
            existing_notebook=is_existing_notebook,
        )

    approximations = [
        ParameterApproximation(
            task_key=activity.task_key,
            widget_name=entry["widget_name"],
            raw_expression=entry["raw_expression"],
            replacement=entry["replacement"],
            note=entry["note"],
        )
        for entry in activity.parameter_approximations
    ]

    if is_existing_notebook:
        downloaded = download_notebook(resolved_path) if workspace_downloads_enabled() else None
        if downloaded is not None:
            filename = workspace_notebook_filename(resolved_path) or notebook_filename(activity.task_key, activity.name)
            notebook_relative_path = f"notebooks/{filename}"
            task["notebook_task"] = {"notebook_path": f"../src/{notebook_relative_path}"}
            if base_parameters is not None:
                task["notebook_task"]["base_parameters"] = base_parameters
            if activity.compute_mode != "serverless" and not task.get("existing_cluster_id"):
                task["job_cluster_key"] = "default_cluster"
            if activity.libraries:
                task["libraries"] = activity.libraries
            notebooks = [DabNotebook(relative_path=notebook_relative_path, content=downloaded)]
            return PreparedActivity(
                task=task,
                notebooks=notebooks,
                parameter_approximations=approximations,
                setup_tasks=_unresolved_library_setup_tasks(activity),
            )

        task["notebook_task"] = {"notebook_path": resolved_path}
        if base_parameters is not None:
            task["notebook_task"]["base_parameters"] = base_parameters
        if activity.libraries:
            task["libraries"] = activity.libraries
        return PreparedActivity(
            task=task,
            parameter_approximations=approximations,
            setup_tasks=_unresolved_library_setup_tasks(activity),
        )

    placeholder_filename = notebook_filename(activity.task_key, activity.name)
    notebook_relative_path = f"notebooks/{placeholder_filename}"
    content = download_notebook(resolved_path) or _notebook_placeholder(
        resolved_path, activity.name, placeholder_filename
    )

    task["notebook_task"] = {"notebook_path": f"../src/{notebook_relative_path}"}
    if base_parameters is not None:
        task["notebook_task"]["base_parameters"] = base_parameters
    if activity.libraries:
        task["libraries"] = activity.libraries

    notebooks = [DabNotebook(relative_path=notebook_relative_path, content=content)]
    setup_tasks = _unresolved_library_setup_tasks(activity)
    return PreparedActivity(
        task=task,
        notebooks=notebooks,
        parameter_approximations=approximations,
        setup_tasks=setup_tasks,
    )


def _unresolved_library_setup_tasks(activity: NotebookActivity) -> list[SetupTask]:
    """Builds ``unresolved_library`` setup tasks for SETUP.md.

    C-30 (NB-ITER4-003): library entries whose jar/whl path didn't resolve
    surface as a SETUP.md section so the user can fix the missing identifier
    rather than discovering the failure when the cluster tries to install
    a file called ``@concat(...)`` at job-run time.
    """
    tasks: list[SetupTask] = []
    for entry in activity.unresolved_libraries:
        tasks.append(
            SetupTask(
                type="unresolved_library",
                config={
                    "task_key": activity.task_key,
                    "library_type": entry.get("type", ""),
                    "expression": entry.get("expression", ""),
                    "missing": list(entry.get("missing") or []),
                },
            )
        )
    return tasks


def _prepare_dispatch_stub(
    activity: NotebookActivity,
    *,
    variable_task_keys: dict[str, str] | None = None,
) -> PreparedActivity:
    """Builds the bundle artifacts for a dynamic-notebookPath activity.

    C-28 (NB-ITER4-001): emits a dispatch-stub notebook (reads
    ``notebook_path`` widget and ``dbutils.notebook.run()``s it), a
    SetupTask of kind ``dynamic_notebook_dispatch`` for SETUP.md, and
    threads the original base_parameters through.
    """
    task = build_common_task_fields(activity)
    filename = notebook_filename(activity.task_key, activity.name)
    notebook_relative_path = f"notebooks/{filename}"

    base_parameters: dict[str, str] = {}
    base_parameters[_DISPATCH_STUB_WIDGET] = ""
    if activity.base_parameters:
        for key, value in _resolve_base_parameters(
            dict(activity.base_parameters),
            variable_task_keys=variable_task_keys,
            existing_notebook=False,
        ).items():
            base_parameters.setdefault(key, value)

    content = _dispatch_stub_notebook(activity, filename)

    task["notebook_task"] = {
        "notebook_path": f"../src/{notebook_relative_path}",
        "base_parameters": base_parameters,
    }
    if activity.libraries:
        task["libraries"] = activity.libraries

    setup_tasks: list[SetupTask] = [
        SetupTask(
            type="dynamic_notebook_dispatch",
            config={
                "task_key": activity.task_key,
                "activity_name": activity.name,
                "expression": activity.notebook_path_expression or "",
                "widget_name": _DISPATCH_STUB_WIDGET,
            },
        )
    ]
    setup_tasks.extend(_unresolved_library_setup_tasks(activity))

    approximations = [
        ParameterApproximation(
            task_key=activity.task_key,
            widget_name=entry["widget_name"],
            raw_expression=entry["raw_expression"],
            replacement=entry["replacement"],
            note=entry["note"],
        )
        for entry in activity.parameter_approximations
    ]

    notebooks = [DabNotebook(relative_path=notebook_relative_path, content=content)]
    return PreparedActivity(
        task=task,
        notebooks=notebooks,
        setup_tasks=setup_tasks,
        parameter_approximations=approximations,
    )
