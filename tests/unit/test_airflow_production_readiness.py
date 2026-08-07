"""Regression coverage for the Airflow source-audit findings."""

from pathlib import Path

from flowx.models.ir import NotebookActivity
from flowx.sources.airflow.loader import load_airflow_dag, load_airflow_dags

_REPROS = Path(__file__).parents[1] / "resources" / "airflow" / "review_repros"


def _dependencies(pipeline) -> dict[str, list[str]]:
    return {
        task.task_key: sorted(dependency.task_key for dependency in (task.depends_on or []))
        for task in pipeline.tasks
    }


def test_assigned_dag_preserves_configuration_and_tasks() -> None:
    pipeline = load_airflow_dag(_REPROS / "a1_assigned_dag.py")

    assert pipeline.name == "legacy_etl"
    assert pipeline.schedule == {
        "kind": "schedule",
        "quartz_cron_expression": "0 0 3 ? * *",
        "timezone_id": "UTC",
        "pause_status": "UNPAUSED",
    }
    assert pipeline.tags["airflow_catchup"] == "true"
    assert {task.task_key for task in pipeline.tasks} == {"extract", "load"}
    assert _dependencies(pipeline)["load"] == ["extract"]
    assert next(task for task in pipeline.tasks if task.task_key == "extract").max_retries == 5


def test_task_key_collisions_allocate_distinct_keys_without_losing_edges() -> None:
    pipeline = load_airflow_dag(_REPROS / "a2_task_key_collision.py")

    assert [task.task_key for task in pipeline.tasks] == ["load_data", "load_data__2", "final"]
    assert _dependencies(pipeline)["final"] == ["load_data", "load_data__2"]


def test_bounded_loops_preserve_generated_tasks_and_edges() -> None:
    pipeline = load_airflow_dag(_REPROS / "t1_loop.py")

    assert [task.task_key for task in pipeline.tasks] == ["load_us", "load_eu", "load_apac"]
    assert _dependencies(pipeline) == {
        "load_us": [],
        "load_eu": ["load_us"],
        "load_apac": ["load_eu"],
    }


def test_aliases_chain_cross_downstream_and_single_return_factories_are_captured() -> None:
    alias_pipeline = load_airflow_dag(_REPROS / "t5_alias.py")
    chain_pipeline = load_airflow_dag(_REPROS / "t6_chain.py")
    helper_pipeline = load_airflow_dag(_REPROS / "t8_helperfn.py")

    assert {task.task_key for task in alias_pipeline.tasks} == {"aliased", "py"}
    assert _dependencies(chain_pipeline) == {
        "a": [],
        "b": ["a"],
        "c": ["a", "b"],
        "d": ["a", "b"],
    }
    assert [task.task_key for task in helper_pipeline.tasks] == ["first", "second"]
    assert _dependencies(helper_pipeline)["second"] == ["first"]


def test_module_callable_wins_over_unrelated_nested_definitions() -> None:
    pipeline = load_airflow_dag(_REPROS / "t19_fncollide.py")
    task = pipeline.tasks[0]

    assert isinstance(task, NotebookActivity)
    assert "CORRECT_BODY" in (task.generated_source or "")
    assert "WRONG_BODY" not in (task.generated_source or "")


def test_literal_dag_factory_loop_and_multiple_assigned_dags_remain_distinct() -> None:
    generated = load_airflow_dags(_REPROS / "t12_globals.py")
    assigned = load_airflow_dags(_REPROS / "t32_multiassigned.py")

    assert [pipeline.name for pipeline in generated] == ["etl_alpha", "etl_beta"]
    assert [pipeline.name for pipeline in assigned] == ["team_a_etl", "team_b_etl"]
    assert all([task.task_key for task in pipeline.tasks] == ["extract", "load"] for pipeline in assigned)
