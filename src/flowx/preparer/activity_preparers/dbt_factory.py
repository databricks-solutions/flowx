"""Preparer for DbtFactoryActivity -> a dbt job wired via run_job_task.

Renders a dbt workload two ways from the same exploded node list:

- ``static`` (default): one notebook task per dbt node in an inner job, wired
  from the parent via a ``run_job_task`` hop.  Every dbt task is a real DAB
  task, so flowx's coverage / validate / REPORT.csv see them.
- ``pydabs`` (opt-in): a PyDABs hook module the bundle loads at deploy time,
  which calls ``databricks-dbt-factory`` to build the dbt job from the live
  manifest.  The parent still gets the ``run_job_task`` hop.
"""

from __future__ import annotations

import json
import shlex
from pathlib import Path
from typing import TYPE_CHECKING, Any

from flowx.dbt.manifest import explode_manifest
from flowx.models.dab import DabNotebook, SetupTask
from flowx.preparer.workflow_preparer import (
    PreparedActivity,
    PreparedWorkflow,
    build_common_task_fields,
)
from flowx.utils import normalize_task_key

if TYPE_CHECKING:
    from flowx.models.ir import DbtFactoryActivity

_RUNNER_RELATIVE_PATH = "notebooks/run_dbt_command.py"
_DBT_PROJECT_RELATIVE_PATH = "dbt_project"
_DBT_PROFILES_RELATIVE_PATH = "dbt_profiles"
_EXCLUDED_DBT_PATH_PARTS = {".git", ".venv", "__pycache__", "logs", "target"}


def _tree_artifacts(source_root: Path, destination_root: str) -> list[DabNotebook]:
    """Returns bundle artifacts for files beneath a local source directory."""
    if not source_root.is_dir():
        return []
    return [
        DabNotebook(
            relative_path=(Path(destination_root) / source.relative_to(source_root)).as_posix(),
            binary_content=source.read_bytes(),
        )
        for source in sorted(source_root.rglob("*"))
        if source.is_file()
        and not source.is_symlink()
        and not (_EXCLUDED_DBT_PATH_PARTS & set(source.relative_to(source_root).parts))
    ]


def _dbt_source_artifacts(activity: DbtFactoryActivity) -> list[DabNotebook]:
    """Returns deployable project, profile, and manifest files available on the local filesystem."""
    project_dir = Path(activity.project_dir).expanduser()
    artifacts = (
        _tree_artifacts(project_dir, _DBT_PROJECT_RELATIVE_PATH) if (project_dir / "dbt_project.yml").is_file() else []
    )

    profiles_dir = Path(activity.profiles_dir).expanduser()
    if (profiles_dir / "profiles.yml").is_file():
        artifacts.extend(_tree_artifacts(profiles_dir, _DBT_PROFILES_RELATIVE_PATH))

    manifest_path = Path(activity.manifest_path).expanduser() if activity.manifest_path else None
    if manifest_path and manifest_path.is_file():
        artifacts.append(
            DabNotebook(
                relative_path=f"{_DBT_PROJECT_RELATIVE_PATH}/target/manifest.json",
                binary_content=manifest_path.read_bytes(),
            )
        )
    return artifacts


def _pydabs_pyproject_source() -> str:
    """Returns the uv project required by generated PyDABs hooks."""
    return (
        "[project]\n"
        'name = "flowx-dbt-bundle"\n'
        'version = "0.1.0"\n'
        'requires-python = ">=3.10"\n'
        "dependencies = [\n"
        '    "databricks-bundles>=1.0.0,<2.0.0",\n'
        '    "databricks-dbt-factory==0.2.1",\n'
        '    "dbt-databricks==1.12.2",\n'
        '    "dbt-core==1.11.12",\n'
        "]\n"
    )


def _pydabs_options_by_resource_type(activity: DbtFactoryActivity) -> dict[str, str]:
    """Returns shell-safe dbt options for each generated task-factory type."""
    common = ["--target", activity.target]
    for selector in activity.exclude_selectors:
        common.extend(("--exclude", selector))
    if activity.variables is not None:
        variables = json.dumps(activity.variables) if isinstance(activity.variables, dict) else activity.variables
        common.extend(("--vars", variables))
    options: dict[str, str] = {}
    for resource_type in activity.resource_types or ["model", "seed", "snapshot", "test"]:
        tokens = [*common]
        if activity.full_refresh and resource_type in {"model", "seed"}:
            tokens.append("--full-refresh")
        options[resource_type] = shlex.join(tokens)
    return options


def _nodes_from_activity(activity: DbtFactoryActivity) -> list[dict[str, Any]]:
    """Returns the exploded dbt node specs for *activity*.

    Uses the front-end-supplied ``nodes`` when present; otherwise reads and
    explodes ``manifest_path``.  Each spec has ``task_key``, ``command``,
    ``selector``, and ``depends_on`` (task keys).
    """
    if activity.nodes:
        if not activity.resource_types:
            return activity.nodes
        command_types = {"run": "model", "seed": "seed", "snapshot": "snapshot", "test": "test"}
        selected = []
        for node in activity.nodes:
            command = node.get("command")
            resource_type = command_types.get(command) if isinstance(command, str) else None
            if resource_type is not None and resource_type in activity.resource_types:
                selected.append(node)
        selected_keys = {node["task_key"] for node in selected}
        return [
            {**node, "depends_on": [key for key in node.get("depends_on") or [] if key in selected_keys]}
            for node in selected
        ]
    if activity.manifest_path:
        manifest = json.loads(Path(activity.manifest_path).read_text(encoding="utf-8"))
        return [
            {
                "task_key": node.task_key,
                "command": node.command,
                "selector": node.selector,
                "depends_on": node.depends_on,
            }
            for node in explode_manifest(manifest, resource_types=set(activity.resource_types) or None)
        ]
    return []


def _runner_notebook_source() -> str:
    """Returns the owned dbt-runner notebook body.

    The runner reads its command / selector / target / project-dir from
    widgets and shells out to the dbt CLI, so one notebook serves every dbt
    node task.
    """
    return (
        "# Databricks notebook source\n"
        "# Owned dbt-command runner for flowx dbt-factory (static) mode.\n"
        "# One task per dbt node passes its command + fqn: selector as widgets.\n\n"
        "import json\n"
        "import os\n"
        "import subprocess\n\n"
        "dbutils.widgets.text('dbt_command', 'run')\n"
        "dbutils.widgets.text('dbt_select', '')\n"
        "dbutils.widgets.text('dbt_selectors', '[]')\n"
        "dbutils.widgets.text('dbt_exclude', '[]')\n"
        "dbutils.widgets.text('dbt_vars', '')\n"
        "dbutils.widgets.text('dbt_full_refresh', 'false')\n"
        "dbutils.widgets.text('dbt_target', 'dev')\n"
        "dbutils.widgets.text('dbt_project_dir', '.')\n"
        "dbutils.widgets.text('dbt_profiles_dir', 'dbt_profiles')\n\n"
        "command = dbutils.widgets.get('dbt_command')\n"
        "select = dbutils.widgets.get('dbt_select')\n"
        "selectors = json.loads(dbutils.widgets.get('dbt_selectors'))\n"
        "exclude = json.loads(dbutils.widgets.get('dbt_exclude'))\n"
        "variables = dbutils.widgets.get('dbt_vars')\n"
        "full_refresh = dbutils.widgets.get('dbt_full_refresh').lower() == 'true'\n"
        "target = dbutils.widgets.get('dbt_target')\n"
        "project_dir = dbutils.widgets.get('dbt_project_dir')\n"
        "profiles_dir = dbutils.widgets.get('dbt_profiles_dir')\n\n"
        "context = dbutils.notebook.entry_point.getDbutils().notebook().getContext()\n"
        "notebook_dir = os.path.dirname('/Workspace' + context.notebookPath().get())\n"
        "if not os.path.isabs(project_dir):\n"
        "    project_dir = os.path.normpath(os.path.join(notebook_dir, project_dir))\n"
        "if not os.path.isabs(profiles_dir):\n"
        "    profiles_dir = os.path.normpath(os.path.join(notebook_dir, profiles_dir))\n\n"
        "argv = ['dbt', command, '--target', target, '--project-dir', project_dir,\n"
        "        '--profiles-dir', profiles_dir]\n"
        "if select:\n"
        "    selected_nodes = [f'{select},{selector}' for selector in selectors] if selectors else [select]\n"
        "    argv += ['--select', *selected_nodes]\n"
        "    if command == 'test':\n"
        "        # Pin test selection to the node itself; don't pull in indirectly-selected tests.\n"
        "        argv += ['--indirect-selection', 'empty']\n\n"
        "if exclude:\n"
        "    argv += ['--exclude', *exclude]\n"
        "if variables:\n"
        "    argv += ['--vars', variables]\n"
        "if full_refresh and command in {'run', 'seed'}:\n"
        "    argv.append('--full-refresh')\n\n"
        "print('running:', ' '.join(argv))\n"
        "result = subprocess.run(argv, check=False)\n"
        "if result.returncode != 0:\n"
        "    raise RuntimeError(f'dbt {command} failed with exit code {result.returncode}')\n"
    )


def _pydabs_runner_notebook_source() -> str:
    """Returns the notebook contract expected by databricks-dbt-factory notebook tasks."""
    return (
        "# Databricks notebook source\n\n"
        "import json\n"
        "import os\n"
        "import shlex\n\n"
        "from dbt.cli.main import dbtRunner\n\n"
        "dbutils.widgets.text('dbt_commands', '')\n"
        "dbutils.widgets.text('project_directory', '')\n"
        "dbutils.widgets.text('profiles_directory', '')\n\n"
        "commands = json.loads(dbutils.widgets.get('dbt_commands'))\n"
        "project_directory = dbutils.widgets.get('project_directory')\n"
        "profiles_directory = dbutils.widgets.get('profiles_directory')\n"
        "context = dbutils.notebook.entry_point.getDbutils().notebook().getContext()\n"
        "os.environ['DBT_ACCESS_TOKEN'] = context.apiToken().get()\n"
        "os.environ['DBT_HOST'] = context.apiUrl().get()\n\n"
        "if project_directory:\n"
        "    notebook_dir = os.path.dirname('/Workspace' + context.notebookPath().get())\n"
        "    project_path = (\n"
        "        project_directory\n"
        "        if os.path.isabs(project_directory)\n"
        "        else os.path.normpath(os.path.join(notebook_dir, project_directory))\n"
        "    )\n"
        "    os.chdir(project_path)\n\n"
        "runner = dbtRunner()\n"
        "for command in commands:\n"
        "    command = command.strip()\n"
        "    if command.startswith('dbt '):\n"
        "        command = command[4:]\n"
        "    arguments = shlex.split(command)\n"
        "    if profiles_directory:\n"
        "        arguments.extend(['--profiles-dir', profiles_directory])\n"
        "    result = runner.invoke(arguments)\n"
        "    if not result.success:\n"
        "        detail = result.exception or result.result or '(no further details)'\n"
        "        raise RuntimeError(f\"dbt command failed: dbt {' '.join(arguments)}\\n{detail}\")\n"
    )


def _node_task(node: dict[str, Any], activity: DbtFactoryActivity) -> dict[str, Any]:
    """Builds one inner-job notebook task for a dbt node."""
    task: dict[str, Any] = {
        "task_key": node["task_key"],
        "libraries": [
            {"pypi": {"package": "dbt-databricks==1.12.2"}},
            {"pypi": {"package": "dbt-core==1.11.12"}},
        ],
        "notebook_task": {
            "notebook_path": f"../src/{_RUNNER_RELATIVE_PATH}",
            "base_parameters": {
                "dbt_command": node["command"],
                "dbt_select": node["selector"],
                "dbt_selectors": json.dumps(activity.selectors),
                "dbt_exclude": json.dumps(activity.exclude_selectors),
                "dbt_vars": (
                    json.dumps(activity.variables) if isinstance(activity.variables, dict) else activity.variables or ""
                ),
                "dbt_full_refresh": str(activity.full_refresh).lower(),
                "dbt_target": activity.target,
                "dbt_project_dir": f"../{_DBT_PROJECT_RELATIVE_PATH}",
                "dbt_profiles_dir": f"../{_DBT_PROFILES_RELATIVE_PATH}",
            },
        },
    }
    depends_on = [{"task_key": key} for key in node.get("depends_on") or []]
    if depends_on:
        task["depends_on"] = depends_on
    return task


def _prepare_static(activity: DbtFactoryActivity, nodes: list[dict[str, Any]]) -> PreparedActivity:
    """Static renderer: inner job of per-node tasks + a run_job_task hop."""
    parent_task = build_common_task_fields(activity)

    inner_job_name = f"{activity.task_key}_dbt"
    inner_tasks = [_node_task(node, activity) for node in nodes]
    notebooks = [
        DabNotebook(relative_path=_RUNNER_RELATIVE_PATH, content=_runner_notebook_source()),
        *_dbt_source_artifacts(activity),
    ]

    inner_workflow = PreparedWorkflow(
        name=inner_job_name,
        tasks=inner_tasks,
        notebooks=notebooks,
        secrets=[],
        setup_tasks=[],
    )

    inner_job_key = normalize_task_key(inner_job_name)
    parent_task["run_job_task"] = {"job_id": f"${{resources.jobs.{inner_job_key}.id}}"}

    return PreparedActivity(task=parent_task, inner_workflows=[inner_workflow])


def _prepare_missing_inputs(activity: DbtFactoryActivity, missing_inputs: list[str]) -> PreparedActivity:
    """Returns a failing placeholder task when required local dbt inputs are unavailable."""
    relative_path = f"notebooks/{activity.task_key}_dbt_setup_required.py"
    missing = ", ".join(missing_inputs)
    content = (
        "# Databricks notebook source\n"
        f"# dbt project inputs required for migrated task {activity.task_key}.\n\n"
        f"raise RuntimeError({f'Missing dbt project input(s): {missing}. Add them and re-run flowx package.'!r})\n"
    )
    task = build_common_task_fields(activity)
    task["notebook_task"] = {"notebook_path": f"../src/{relative_path}"}
    return PreparedActivity(task=task, notebooks=[DabNotebook(relative_path=relative_path, content=content)])


def _missing_dbt_inputs(activity: DbtFactoryActivity, *, require_manifest: bool = True) -> list[str]:
    """Returns required dbt inputs that are not available to copy into the bundle."""
    project_dir = Path(activity.project_dir).expanduser()
    profiles_dir = Path(activity.profiles_dir).expanduser()
    manifest_path = Path(activity.manifest_path).expanduser() if activity.manifest_path else None
    missing: list[str] = []
    if not (project_dir / "dbt_project.yml").is_file():
        missing.append(f"dbt project at {project_dir}")
    if not (profiles_dir / "profiles.yml").is_file():
        missing.append(f"profiles.yml under {profiles_dir}")
    if require_manifest and (manifest_path is None or not manifest_path.is_file()):
        missing.append(f"manifest at {manifest_path or '<manifest.json>'}")
    return missing


def _pydabs_hook_source(activity: DbtFactoryActivity) -> str:
    """Returns the PyDABs hook module body for deploy-time dbt-factory generation."""
    resource_types = activity.resource_types or ["model", "seed", "snapshot", "test"]
    dbt_options = _pydabs_options_by_resource_type(activity)
    return (
        '"""PyDABs hook: build the dbt job from the live manifest at deploy time."""\n\n'
        "from databricks.bundles.core import Bundle, Resources\n"
        "from databricks.bundles.jobs import Job\n"
        "from databricks_dbt_factory.DbtFactory import DbtFactory\n"
        "from databricks_dbt_factory.DbtTask import DbtTaskOptions, TaskType\n"
        "from databricks_dbt_factory.SpecsHandler import SpecsHandler\n"
        "from databricks_dbt_factory.TaskFactory import (\n"
        "    DbtDependencyResolver,\n"
        "    ModelTaskFactory,\n"
        "    SeedTaskFactory,\n"
        "    SnapshotTaskFactory,\n"
        "    TestTaskFactory,\n"
        ")\n\n"
        f"MANIFEST_PATH = {'src/dbt_project/target/manifest.json'!r}\n"
        f"PROJECT_DIR = {'../dbt_project'!r}\n"
        f"PROFILES_DIR = {'../dbt_profiles'!r}\n"
        f"RESOURCE_TYPES = {resource_types!r}\n\n"
        f"DBT_OPTIONS = {dbt_options!r}\n\n"
        "def _task_factories():\n"
        "    resolver = DbtDependencyResolver()\n"
        "    options = DbtTaskOptions(\n"
        "        task_type=TaskType.NOTEBOOK,\n"
        "        environment_key='Default',\n"
        "        notebook_path='src/notebooks/run_dbt_command.py',\n"
        "        project_directory=PROJECT_DIR,\n"
        "        profiles_directory=PROFILES_DIR,\n"
        "    )\n"
        "    factory_classes = {\n"
        "        'model': ModelTaskFactory,\n"
        "        'seed': SeedTaskFactory,\n"
        "        'snapshot': SnapshotTaskFactory,\n"
        "        'test': TestTaskFactory,\n"
        "    }\n"
        "    return {\n"
        "        name: factory_classes[name](resolver, options, DBT_OPTIONS[name])\n"
        "        for name in RESOURCE_TYPES\n"
        "    }\n\n"
        "def load_resources(bundle: Bundle) -> Resources:\n"
        "    manifest = SpecsHandler.read_dbt_manifest(MANIFEST_PATH)\n"
        "    task_factories = _task_factories()\n"
        "    resources = Resources()\n"
        "    factory = DbtFactory(SpecsHandler(), task_factories, bundle_tests=False)\n"
        "    tasks = factory.create_tasks(manifest)\n"
        "    environment = {\n"
        "        'environment_key': 'Default',\n"
        "        'spec': {\n"
        "            'environment_version': '4',\n"
        "            'dependencies': ['dbt-databricks==1.12.2', 'dbt-core==1.11.12'],\n"
        "        },\n"
        "    }\n"
        f"    resources.add_job({normalize_task_key(activity.task_key + '_dbt')!r}, Job(\n"
        f"        name={normalize_task_key(activity.task_key + '_dbt')!r}, tasks=tasks, environments=[environment]\n"
        "    ))\n"
        "    return resources\n"
    )


def _prepare_pydabs(activity: DbtFactoryActivity) -> PreparedActivity:
    """PyDABs renderer: emit the hook module + a run_job_task hop.

    The dbt job is defined by the hook at deploy time, so no inner workflow is
    emitted here.  A SetupTask records that databricks.yml needs a
    ``python.resources`` entry pointing at the hook.
    """
    parent_task = build_common_task_fields(activity)
    inner_job_key = normalize_task_key(f"{activity.task_key}_dbt")
    parent_task["run_job_task"] = {"job_id": f"${{resources.jobs.{inner_job_key}.id}}"}

    hook_relative_path = f"resources/{activity.task_key}_dbt_job.py"
    hook_notebook = DabNotebook(relative_path=hook_relative_path, content=_pydabs_hook_source(activity))
    runner_notebook = DabNotebook(relative_path=_RUNNER_RELATIVE_PATH, content=_pydabs_runner_notebook_source())
    pyproject = DabNotebook(relative_path="pyproject.toml", content=_pydabs_pyproject_source())
    # `resources` must be an importable package for `python.resources: resources.<mod>` to resolve.
    package_marker = DabNotebook(relative_path="resources/__init__.py", content="")
    source_artifacts = _dbt_source_artifacts(activity)
    setup_task = SetupTask(
        type="pydabs_dbt_factory",
        config={
            "hook_module": f"resources.{activity.task_key}_dbt_job",
            "job_key": inner_job_key,
            "manifest_path": "src/dbt_project/target/manifest.json",
            "note": (
                "dbt-factory PyDABs mode: databricks.yml registers "
                f"`resources.{activity.task_key}_dbt_job:load_resources`; run `uv sync` before bundle commands."
            ),
        },
    )
    return PreparedActivity(
        task=parent_task,
        notebooks=[hook_notebook, package_marker, runner_notebook, pyproject, *source_artifacts],
        setup_tasks=[setup_task],
    )


def prepare(activity: DbtFactoryActivity, *, scope: str = "") -> PreparedActivity:
    """Converts a DbtFactoryActivity into DAB tasks per its render mode."""
    if activity.resource_types == ["dependency"]:
        missing_inputs = _missing_dbt_inputs(activity, require_manifest=False)
        if missing_inputs:
            return _prepare_missing_inputs(activity, missing_inputs)
        dependency_node = {"task_key": "dbt_deps", "command": "deps", "selector": "", "depends_on": []}
        return _prepare_static(activity, [dependency_node])
    if activity.render_mode == "pydabs" or not activity.nodes:
        missing_inputs = _missing_dbt_inputs(activity)
        if missing_inputs:
            return _prepare_missing_inputs(activity, missing_inputs)
    if activity.render_mode == "pydabs":
        if activity.selectors:
            return _prepare_static(activity, _nodes_from_activity(activity))
        return _prepare_pydabs(activity)
    nodes = _nodes_from_activity(activity)
    return _prepare_static(activity, nodes)
