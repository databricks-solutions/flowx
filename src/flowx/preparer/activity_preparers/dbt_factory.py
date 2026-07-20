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


def _nodes_from_activity(activity: DbtFactoryActivity) -> list[dict[str, Any]]:
    """Returns the exploded dbt node specs for *activity*.

    Uses the front-end-supplied ``nodes`` when present; otherwise reads and
    explodes ``manifest_path``.  Each spec has ``task_key``, ``command``,
    ``selector``, and ``depends_on`` (task keys).
    """
    if activity.nodes:
        return activity.nodes
    if activity.manifest_path:
        manifest = json.loads(Path(activity.manifest_path).read_text(encoding="utf-8"))
        return [
            {
                "task_key": node.task_key,
                "command": node.command,
                "selector": node.selector,
                "depends_on": node.depends_on,
            }
            for node in explode_manifest(manifest)
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
        "import subprocess\n\n"
        "dbutils.widgets.text('dbt_command', 'run')\n"
        "dbutils.widgets.text('dbt_select', '')\n"
        "dbutils.widgets.text('dbt_target', 'dev')\n"
        "dbutils.widgets.text('dbt_project_dir', '.')\n"
        "dbutils.widgets.text('dbt_profiles_dir', 'dbt_profiles')\n\n"
        "command = dbutils.widgets.get('dbt_command')\n"
        "select = dbutils.widgets.get('dbt_select')\n"
        "target = dbutils.widgets.get('dbt_target')\n"
        "project_dir = dbutils.widgets.get('dbt_project_dir')\n"
        "profiles_dir = dbutils.widgets.get('dbt_profiles_dir')\n\n"
        "argv = ['dbt', command, '--target', target, '--project-dir', project_dir,\n"
        "        '--profiles-dir', profiles_dir]\n"
        "if select:\n"
        "    argv += ['--select', select]\n"
        "    if command == 'test':\n"
        "        # Pin test selection to the node itself; don't pull in indirectly-selected tests.\n"
        "        argv += ['--indirect-selection', 'empty']\n\n"
        "print('running:', ' '.join(argv))\n"
        "result = subprocess.run(argv, check=False)\n"
        "if result.returncode != 0:\n"
        "    raise RuntimeError(f'dbt {command} failed with exit code {result.returncode}')\n"
    )


def _node_task(node: dict[str, Any], activity: DbtFactoryActivity) -> dict[str, Any]:
    """Builds one inner-job notebook task for a dbt node."""
    task: dict[str, Any] = {
        "task_key": node["task_key"],
        "notebook_task": {
            "notebook_path": f"../src/{_RUNNER_RELATIVE_PATH}",
            "base_parameters": {
                "dbt_command": node["command"],
                "dbt_select": node["selector"],
                "dbt_target": activity.target,
                "dbt_project_dir": activity.project_dir,
                "dbt_profiles_dir": activity.profiles_dir,
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
    runner_notebook = DabNotebook(relative_path=_RUNNER_RELATIVE_PATH, content=_runner_notebook_source())

    inner_workflow = PreparedWorkflow(
        name=inner_job_name,
        tasks=inner_tasks,
        notebooks=[runner_notebook],
        secrets=[],
        setup_tasks=[],
    )

    inner_job_key = normalize_task_key(inner_job_name)
    parent_task["run_job_task"] = {"job_id": f"${{resources.jobs.{inner_job_key}.id}}"}

    return PreparedActivity(task=parent_task, inner_workflows=[inner_workflow])


def _pydabs_hook_source(activity: DbtFactoryActivity) -> str:
    """Returns the PyDABs hook module body for deploy-time dbt-factory generation."""
    return (
        '"""PyDABs hook: build the dbt job from the live manifest at deploy time."""\n\n'
        "from databricks.bundles.core import Bundle, Resources\n"
        "from databricks_dbt_factory.DbtFactory import DbtFactory\n"
        "from databricks_dbt_factory.Utils import read_dbt_manifest\n\n"
        f"MANIFEST_PATH = {activity.manifest_path or 'target/manifest.json'!r}\n"
        f"PROJECT_DIR = {activity.project_dir!r}\n"
        f"PROFILES_DIR = {activity.profiles_dir!r}\n\n"
        "def load_resources(bundle: Bundle) -> Resources:\n"
        "    resources = Resources()\n"
        "    factory = DbtFactory()\n"
        "    tasks = factory.create_tasks(read_dbt_manifest(MANIFEST_PATH))\n"
        f"    resources.add_job({normalize_task_key(activity.task_key + '_dbt')!r}, {{'tasks': tasks}})\n"
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
    # `resources` must be an importable package for `python.resources: resources.<mod>` to resolve.
    package_marker = DabNotebook(relative_path="resources/__init__.py", content="")
    setup_task = SetupTask(
        type="pydabs_dbt_factory",
        config={
            "hook_module": f"resources.{activity.task_key}_dbt_job",
            "job_key": inner_job_key,
            "manifest_path": activity.manifest_path or "target/manifest.json",
            "note": (
                "dbt-factory PyDABs mode: add a `python.resources` entry to databricks.yml pointing at "
                f"`resources.{activity.task_key}_dbt_job:load_resources`, and `pip install databricks-dbt-factory`."
            ),
        },
    )
    return PreparedActivity(task=parent_task, notebooks=[hook_notebook, package_marker], setup_tasks=[setup_task])


def prepare(activity: DbtFactoryActivity, *, scope: str = "") -> PreparedActivity:
    """Converts a DbtFactoryActivity into DAB tasks per its render mode."""
    if activity.render_mode == "pydabs":
        return _prepare_pydabs(activity)
    nodes = _nodes_from_activity(activity)
    return _prepare_static(activity, nodes)
