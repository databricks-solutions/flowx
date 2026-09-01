"""Unit tests for the DbtFactoryActivity preparer (static + pydabs modes)."""

from __future__ import annotations

import json

import pytest

from flowx.models.ir import DbtFactoryActivity, Dependency, NotebookActivity, Pipeline
from flowx.preparer.workflow_preparer import prepare_activity, prepare_workflow

_NODES = [
    {"task_key": "seed_codes", "command": "seed", "selector": "fqn:p.codes", "depends_on": []},
    {"task_key": "model_stg", "command": "run", "selector": "fqn:p.staging.stg", "depends_on": []},
    {
        "task_key": "model_fct",
        "command": "run",
        "selector": "fqn:p.marts.fct",
        "depends_on": ["model_stg", "seed_codes"],
    },
    {"task_key": "test_stg", "command": "test", "selector": "fqn:p.staging.t", "depends_on": ["model_stg"]},
]


def _dbt_activity(**overrides):
    kwargs = dict(
        name="dbt_transform",
        task_key="dbt_transform",
        project_dir=".",
        profiles_dir="dbt_profiles",
        target="dev",
        nodes=_NODES,
        render_mode="static",
    )
    kwargs.update(overrides)
    return DbtFactoryActivity(**kwargs)


def _pydabs_activity(tmp_path, **overrides):
    project = tmp_path / "dbt-source"
    profiles = tmp_path / "dbt-profiles"
    (project / "target").mkdir(parents=True)
    profiles.mkdir(parents=True)
    (project / "dbt_project.yml").write_text("name: demo\nprofile: demo\n")
    (project / "target" / "manifest.json").write_text(json.dumps({"nodes": {}}))
    (profiles / "profiles.yml").write_text("demo:\n  target: dev\n  outputs: {}\n")
    kwargs = dict(
        nodes=[],
        render_mode="pydabs",
        project_dir=str(project),
        profiles_dir=str(profiles),
        manifest_path=str(project / "target" / "manifest.json"),
    )
    kwargs.update(overrides)
    return _dbt_activity(**kwargs)


def test_static_parent_task_is_run_job_hop():
    prepared = prepare_activity(_dbt_activity())
    assert "run_job_task" in prepared.task
    assert prepared.task["run_job_task"]["job_id"] == "${resources.jobs.dbt_transform_dbt.id}"


def test_static_emits_inner_job_with_one_task_per_node():
    prepared = prepare_activity(_dbt_activity())
    assert len(prepared.inner_workflows) == 1
    inner = prepared.inner_workflows[0]
    task_keys = {t["task_key"] for t in inner.tasks}
    assert task_keys == {"seed_codes", "model_stg", "model_fct", "test_stg"}


def test_static_filters_preloaded_nodes_to_command_resource_types():
    prepared = prepare_activity(_dbt_activity(resource_types=["model"]))
    assert {task["task_key"] for task in prepared.inner_workflows[0].tasks} == {"model_stg", "model_fct"}


def test_static_preserves_node_dependencies():
    prepared = prepare_activity(_dbt_activity())
    inner = prepared.inner_workflows[0]
    fct = next(t for t in inner.tasks if t["task_key"] == "model_fct")
    deps = {d["task_key"] for d in fct["depends_on"]}
    assert deps == {"model_stg", "seed_codes"}


def test_static_node_task_carries_command_and_selector():
    prepared = prepare_activity(_dbt_activity())
    inner = prepared.inner_workflows[0]
    test_task = next(t for t in inner.tasks if t["task_key"] == "test_stg")
    params = test_task["notebook_task"]["base_parameters"]
    assert params["dbt_command"] == "test"
    assert params["dbt_select"] == "fqn:p.staging.t"
    assert params["dbt_target"] == "dev"
    packages = {library["pypi"]["package"] for library in test_task["libraries"]}
    assert packages == {"dbt-databricks==1.12.2", "dbt-core==1.11.12"}


def test_static_node_task_carries_source_dbt_options():
    prepared = prepare_activity(
        _dbt_activity(
            selectors=["tag:daily"],
            exclude_selectors=["tag:slow"],
            variables={"region": "west"},
            full_refresh=True,
        )
    )
    model_task = next(t for t in prepared.inner_workflows[0].tasks if t["task_key"] == "model_stg")
    params = model_task["notebook_task"]["base_parameters"]

    assert params["dbt_selectors"] == '["tag:daily"]'
    assert params["dbt_exclude"] == '["tag:slow"]'
    assert params["dbt_vars"] == '{"region": "west"}'
    assert params["dbt_full_refresh"] == "true"
    runner = next(
        notebook for notebook in prepared.inner_workflows[0].notebooks if "run_dbt_command" in notebook.relative_path
    )
    assert "--exclude" in runner.content
    assert "--vars" in runner.content
    assert "--full-refresh" in runner.content


def test_static_emits_single_shared_runner_notebook():
    prepared = prepare_activity(_dbt_activity())
    inner = prepared.inner_workflows[0]
    runner_paths = [nb.relative_path for nb in inner.notebooks]
    assert runner_paths == ["notebooks/run_dbt_command.py"]
    # Every node task points at the one runner.
    for task in inner.tasks:
        assert task["notebook_task"]["notebook_path"] == "../src/notebooks/run_dbt_command.py"


def test_static_parent_hop_keeps_upstream_dependency():
    activity = _dbt_activity(depends_on=[Dependency(task_key="ingest")])
    prepared = prepare_activity(activity)
    assert prepared.task["depends_on"] == [{"task_key": "ingest"}]


def test_static_missing_manifest_emits_manual_placeholder_instead_of_crashing(tmp_path):
    prepared = prepare_activity(
        _dbt_activity(nodes=[], manifest_path=str(tmp_path / "missing" / "manifest.json"), resource_types=["model"])
    )

    assert prepared.inner_workflows == []
    assert "notebook_task" in prepared.task
    assert "manifest" in prepared.notebooks[0].content


def test_pydabs_missing_local_inputs_emits_setup_placeholder(tmp_path):
    prepared = prepare_activity(
        _dbt_activity(
            nodes=[],
            render_mode="pydabs",
            project_dir=str(tmp_path / "missing-project"),
            profiles_dir=str(tmp_path / "missing-profiles"),
            manifest_path=str(tmp_path / "missing-manifest.json"),
        )
    )

    assert prepared.inner_workflows == []
    assert "notebook_task" in prepared.task
    assert not any(notebook.relative_path.startswith("resources/") for notebook in prepared.notebooks)
    assert "dbt project" in prepared.notebooks[0].content


def test_pydabs_emits_hook_module_and_no_inner_job(tmp_path):
    prepared = prepare_activity(_pydabs_activity(tmp_path))
    assert prepared.inner_workflows == []
    hook_paths = {nb.relative_path for nb in prepared.notebooks}
    # The hook module plus a resources/ package marker so `python.resources` can import it.
    assert {
        "resources/dbt_transform_dbt_job.py",
        "resources/__init__.py",
        "notebooks/run_dbt_command.py",
        "pyproject.toml",
    } <= hook_paths
    hook = next(nb for nb in prepared.notebooks if nb.relative_path.endswith("_dbt_job.py"))
    assert "load_resources" in hook.content
    assert "from databricks_dbt_factory.Utils import read_dbt_manifest" in hook.content
    assert "DbtFactory(task_factories" in hook.content
    # The supported factory API exposes the manifest reader as a module-level Utils function.
    assert "SpecsHandler" not in hook.content
    assert "read_dbt_manifest(MANIFEST_PATH)" in hook.content
    runner = next(nb for nb in prepared.notebooks if nb.relative_path == "notebooks/run_dbt_command.py")
    assert "dbt_commands" in runner.content
    assert "dbt_commands parameter is required" in runner.content
    assert "project_directory" in runner.content
    assert "profiles_directory" in runner.content
    assert "urlparse" in runner.content
    assert "DBT_TARGET_PATH" in runner.content
    assert "partial_parse.msgpack" in runner.content
    assert "shutil.rmtree" in runner.content
    compile(runner.content, runner.relative_path, "exec")
    assert "run_job_task" in prepared.task


@pytest.mark.parametrize(
    "reserved_options",
    [
        {"selectors": ["tag:daily"]},
        {"exclude_selectors": ["tag:slow"]},
        {"variables": {"region": "west"}},
    ],
)
def test_pydabs_reserved_factory_options_fall_back_to_static(tmp_path, reserved_options):
    prepared = prepare_activity(_pydabs_activity(tmp_path, nodes=_NODES, **reserved_options))

    assert len(prepared.inner_workflows) == 1
    assert {task["task_key"] for task in prepared.inner_workflows[0].tasks} == {
        "seed_codes",
        "model_stg",
        "model_fct",
        "test_stg",
    }
    assert not any(task.type == "pydabs_dbt_factory" for task in prepared.setup_tasks)


def test_pydabs_keeps_supported_target_and_full_refresh_options(tmp_path):
    prepared = prepare_activity(
        _pydabs_activity(tmp_path, target="prod", full_refresh=True, resource_types=["model", "test"])
    )

    hook = next(notebook for notebook in prepared.notebooks if notebook.relative_path.endswith("_dbt_job.py"))
    assert "'model': '--target prod --full-refresh'" in hook.content
    assert "'test': '--target prod'" in hook.content
    assert "--exclude" not in hook.content
    assert "--vars" not in hook.content


def test_pydabs_records_setup_task(tmp_path):
    prepared = prepare_activity(_pydabs_activity(tmp_path))
    setup_types = {t.type for t in prepared.setup_tasks}
    assert "pydabs_dbt_factory" in setup_types


def test_full_pipeline_wires_two_jobs():
    pipeline = Pipeline(
        name="orders",
        tasks=[
            NotebookActivity(
                name="ingest",
                task_key="ingest",
                notebook_path="notebooks/ingest.py",
                generated_source="# Databricks notebook source\nprint('x')\n",
            ),
            _dbt_activity(depends_on=[Dependency(task_key="ingest")]),
        ],
    )
    wf = prepare_workflow(pipeline)
    parent_keys = {t["task_key"] for t in wf.tasks}
    assert parent_keys == {"ingest", "dbt_transform"}
    assert len(wf.inner_workflows) == 1
    assert wf.inner_workflows[0].name == "dbt_transform_dbt"


def test_survives_json_report_round_trip():
    # The convert->package phase boundary serialises the IR to translation_report.json.
    # DbtFactoryActivity must serialise and rehydrate without losing its node list.
    from flowx.bundler.dab_writer import pipeline_dict_to_ir
    from flowx.ir_serde import pipeline_to_dict
    from flowx.models.ir import DbtFactoryActivity

    pipeline = Pipeline(
        name="orders",
        tasks=[
            _dbt_activity(
                resource_types=["model"],
                selectors=["tag:daily"],
                exclude_selectors=["tag:slow"],
                variables={"region": "west"},
                full_refresh=True,
            )
        ],
    )
    rehydrated, _ = pipeline_dict_to_ir(pipeline_to_dict(pipeline))
    dbt = rehydrated.tasks[0]
    assert isinstance(dbt, DbtFactoryActivity)
    assert dbt.render_mode == "static"
    assert {n["task_key"] for n in dbt.nodes} == {"seed_codes", "model_stg", "model_fct", "test_stg"}
    assert dbt.resource_types == ["model"]
    assert dbt.selectors == ["tag:daily"]
    assert dbt.exclude_selectors == ["tag:slow"]
    assert dbt.variables == {"region": "west"}
    assert dbt.full_refresh is True


def test_pydabs_bundle_wires_python_resources_and_setup(tmp_path):
    # End-to-end: PyDABs mode must register the hook under databricks.yml python.resources, write the
    # hook + package marker to the bundle root (not src/), and surface the setup steps in SETUP.md.
    import yaml

    from flowx.bundler.dab_writer import write_bundle

    pipeline = Pipeline(
        name="orders",
        tasks=[
            NotebookActivity(
                name="ingest",
                task_key="ingest",
                notebook_path="notebooks/ingest.py",
                generated_source="# Databricks notebook source\nprint('x')\n",
            ),
            _pydabs_activity(
                tmp_path,
                depends_on=[Dependency(task_key="ingest")],
            ),
        ],
    )
    write_bundle(prepare_workflow(pipeline), tmp_path)

    databricks_yml = yaml.safe_load((tmp_path / "databricks.yml").read_text())
    assert databricks_yml["python"]["resources"] == ["resources.dbt_transform_dbt_job:load_resources"]
    assert databricks_yml["python"]["venv_path"] == ".venv"
    # Hook + package marker live at the bundle root so `resources.<mod>` imports resolve.
    assert (tmp_path / "resources" / "dbt_transform_dbt_job.py").exists()
    assert (tmp_path / "resources" / "__init__.py").exists()
    assert not (tmp_path / "src" / "resources").exists()
    pyproject = (tmp_path / "pyproject.toml").read_text()
    assert 'requires-python = ">=3.10,<3.13"' in pyproject
    assert "databricks-dbt-factory==0.3.3" in pyproject
    assert "dbt-databricks==1.12.2" in pyproject
    setup = (tmp_path / "SETUP.md").read_text()
    assert "dbt factory (PyDABs mode)" in setup
    assert "databricks-dbt-factory" in setup
    assert "uv sync" in setup


def test_pydabs_copies_available_dbt_project_into_bundle(tmp_path):
    from flowx.bundler.dab_writer import write_bundle

    project = tmp_path / "project"
    (project / "models").mkdir(parents=True)
    (project / "dbt_project.yml").write_text("name: demo\nprofile: demo\n")
    (project / "models" / "orders.sql").write_text("select 1\n")
    (project / "target").mkdir()
    (project / "target" / "manifest.json").write_text(json.dumps({"nodes": {}}))
    (project / "target" / "partial_parse.msgpack").write_bytes(b"prebuilt-dbt-graph")
    profiles = tmp_path / "profiles"
    profiles.mkdir()
    (profiles / "profiles.yml").write_text("demo:\n  target: dev\n  outputs: {}\n")
    output = tmp_path / "bundle"
    pipeline = Pipeline(
        name="orders",
        tasks=[
            _dbt_activity(
                render_mode="pydabs",
                project_dir=str(project),
                profiles_dir=str(profiles),
                manifest_path=str(project / "target" / "manifest.json"),
            )
        ],
    )

    write_bundle(prepare_workflow(pipeline), output)

    assert (output / "src" / "dbt_project" / "dbt_project.yml").exists()
    assert (output / "src" / "dbt_project" / "models" / "orders.sql").exists()
    assert (output / "src" / "dbt_project" / "target" / "partial_parse.msgpack").read_bytes() == b"prebuilt-dbt-graph"
