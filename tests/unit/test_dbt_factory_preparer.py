"""Unit tests for the DbtFactoryActivity preparer (static + pydabs modes)."""

from __future__ import annotations

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


def test_pydabs_emits_hook_module_and_no_inner_job():
    prepared = prepare_activity(_dbt_activity(render_mode="pydabs", manifest_path="target/manifest.json"))
    assert prepared.inner_workflows == []
    hook_paths = [nb.relative_path for nb in prepared.notebooks]
    assert hook_paths == ["resources/dbt_transform_dbt_job.py"]
    assert "load_resources" in prepared.notebooks[0].content
    assert "run_job_task" in prepared.task


def test_pydabs_records_setup_task():
    prepared = prepare_activity(_dbt_activity(render_mode="pydabs"))
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

    pipeline = Pipeline(name="orders", tasks=[_dbt_activity()])
    rehydrated, _ = pipeline_dict_to_ir(pipeline_to_dict(pipeline))
    dbt = rehydrated.tasks[0]
    assert isinstance(dbt, DbtFactoryActivity)
    assert dbt.render_mode == "static"
    assert {n["task_key"] for n in dbt.nodes} == {"seed_codes", "model_stg", "model_fct", "test_stg"}
