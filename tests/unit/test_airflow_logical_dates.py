"""Tests for Airflow logical-date fidelity: timetable classification and the generated date resolver."""

from __future__ import annotations

import ast
import textwrap
from pathlib import Path
from typing import Any

import pytest

from flowx.models.ir import NotebookActivity, Pipeline, PlaceholderActivity, SqlActivity
from flowx.sources.airflow import logical_date_runtime as runtime
from flowx.sources.airflow.loader import load_airflow_dag, logical_dates

RESOLVER = "__flowx_airflow_dates"


def _resolve(**arguments: Any) -> dict[str, str]:
    arguments.setdefault("trigger_type", "periodic")
    return runtime.resolve(**arguments)


def _load(tmp_path: Path, source: str) -> Pipeline:
    path = tmp_path / "dag.py"
    path.write_text(textwrap.dedent(source), encoding="utf-8")
    return load_airflow_dag(path)


def _by_key(pipeline: Pipeline) -> dict[str, Any]:
    return {task.task_key: task for task in pipeline.tasks}


def _codes(pipeline: Pipeline) -> set[str]:
    return {finding["code"] for finding in pipeline.not_translatable}


def _generated_namespace(semantics: logical_dates.LogicalDateSemantics) -> dict[str, Any]:
    namespace: dict[str, Any] = {"__name__": "generated_resolver"}
    exec(compile(logical_dates.resolver_source(semantics), "resolver", "exec"), namespace)
    return namespace


# --------------------------------------------------------------------------------------
# Run-time arithmetic: the peer-specified semantics table
# --------------------------------------------------------------------------------------


def test_airflow2_cron_ds_is_the_previous_tick() -> None:
    values = _resolve(semantics="data_interval_cron", cron="0 2 * * *", trigger_time="2026-01-02T02:00:00Z")
    assert values["ds"] == "2026-01-01"
    assert values["data_interval_start"] == "2026-01-01T02:00:00+00:00"
    assert values["data_interval_end"] == "2026-01-02T02:00:00+00:00"


def test_airflow2_daily_preset_is_the_previous_midnight() -> None:
    values = _resolve(semantics="data_interval_cron", cron="@daily", trigger_time="2026-01-02T00:00:00Z")
    assert values["ts"] == "2026-01-01T00:00:00+00:00"


def test_airflow2_hourly_shifts_one_hour_and_keeps_the_date_until_midnight() -> None:
    values = _resolve(semantics="data_interval_cron", cron="@hourly", trigger_time="2026-01-02T02:00:00Z")
    assert values["ts"] == "2026-01-02T01:00:00+00:00"
    assert values["ds"] == "2026-01-02"
    after_midnight = _resolve(semantics="data_interval_cron", cron="@hourly", trigger_time="2026-01-02T00:00:00Z")
    assert after_midnight["ds"] == "2026-01-01"


def test_irregular_cron_monday_run_shifts_to_the_previous_friday_tick() -> None:
    values = _resolve(semantics="data_interval_cron", cron="0 2 * * 1-5", trigger_time="2026-01-05T02:00:00Z")
    assert values["ds"] == "2026-01-02"


def test_timedelta_schedule_shifts_one_interval() -> None:
    values = _resolve(semantics="data_interval_delta", delta_seconds=6 * 3600, trigger_time="2026-01-02T02:00:00Z")
    assert values["ts"] == "2026-01-01T20:00:00+00:00"
    assert values["data_interval_end"] == "2026-01-02T02:00:00+00:00"


def test_airflow3_raw_cron_keeps_the_fire_time() -> None:
    values = _resolve(semantics="trigger_cron", cron="0 2 * * *", trigger_time="2026-01-02T02:00:00Z")
    assert values["ds"] == "2026-01-02"
    assert values["data_interval_start"] == values["data_interval_end"] == "2026-01-02T02:00:00+00:00"


@pytest.mark.parametrize("trigger_type", ["one_time", "run_job_task"])
def test_manual_runs_keep_the_trigger_time(trigger_type: str) -> None:
    values = _resolve(
        semantics="data_interval_cron",
        cron="0 2 * * *",
        trigger_time="2026-01-02T15:30:00Z",
        trigger_type=trigger_type,
    )
    assert values["ts"] == "2026-01-02T15:30:00+00:00"
    # Airflow's infer_manual_data_interval: the last complete period before the trigger.
    assert values["data_interval_start"] == "2026-01-01T02:00:00+00:00"
    assert values["data_interval_end"] == "2026-01-02T02:00:00+00:00"


def test_logical_date_override_wins_without_a_shift() -> None:
    values = _resolve(
        semantics="data_interval_cron",
        cron="0 2 * * *",
        trigger_time="2026-01-02T02:00:00Z",
        override="2025-12-25T02:00:00Z",
    )
    assert values["ds"] == "2025-12-25"
    assert values["data_interval_end"] == "2025-12-26T02:00:00+00:00"


def test_trigger_time_drift_still_aligns_to_its_tick() -> None:
    values = _resolve(semantics="data_interval_cron", cron="0 2 * * *", trigger_time="2026-01-02T02:00:03.250Z")
    assert values["ds"] == "2026-01-01"
    assert values["data_interval_end"] == "2026-01-02T02:00:00+00:00"


def test_day_of_month_and_day_of_week_match_either_when_both_restricted() -> None:
    # Tuesday Jan 13 is a tick through its day of month; the previous tick is Friday Jan 9.
    values = _resolve(semantics="data_interval_cron", cron="0 0 13 * FRI", trigger_time="2026-01-13T00:00:00Z")
    assert values["ds"] == "2026-01-09"


def test_named_months_and_days() -> None:
    # Monday 2026-03-02 is a tick; the tick before it skips the weekend back to Friday 2026-02-27.
    values = _resolve(semantics="data_interval_cron", cron="0 6 * JAN-MAR MON-FRI", trigger_time="2026-03-02T06:00:00Z")
    assert values["ds"] == "2026-02-27"
    # A trigger outside the named months aligns to the last in-schedule tick (Tuesday 2026-03-31).
    april = _resolve(semantics="data_interval_cron", cron="0 6 * JAN-MAR MON-FRI", trigger_time="2026-04-01T06:00:00Z")
    assert april["data_interval_end"] == "2026-03-31T06:00:00+00:00"


def test_daylight_saving_gap_skips_the_nonexistent_local_tick() -> None:
    # 02:30 on 2026-03-08 does not exist in New York; the tick before the 03-09 run is 03-07 02:30 EST.
    values = _resolve(
        semantics="data_interval_cron",
        cron="30 2 * * *",
        zone_name="America/New_York",
        trigger_time="2026-03-09T06:30:00Z",
    )
    assert values["data_interval_start"] == "2026-03-07T07:30:00+00:00"


def test_cron_ticks_use_the_dag_timezone_and_render_in_utc() -> None:
    values = _resolve(
        semantics="data_interval_cron",
        cron="0 2 * * *",
        zone_name="America/Los_Angeles",
        trigger_time="2026-01-02T10:00:00Z",
        publish_neighbors=True,
    )
    assert values == {
        "ds": "2026-01-01",
        "ds_nodash": "20260101",
        "ts": "2026-01-01T10:00:00+00:00",
        "ts_nodash": "20260101T100000",
        "logical_date": "2026-01-01T10:00:00+00:00",
        "execution_date": "2026-01-01T10:00:00+00:00",
        "data_interval_start": "2026-01-01T10:00:00+00:00",
        "data_interval_end": "2026-01-02T10:00:00+00:00",
        "prev_ds": "2025-12-31",
        "next_ds": "2026-01-02",
        "prev_ds_nodash": "20251231",
        "next_ds_nodash": "20260102",
    }


def test_unreachable_cron_fails_loudly() -> None:
    with pytest.raises(RuntimeError, match="no tick"):
        _resolve(semantics="data_interval_cron", cron="0 0 31 2 *", trigger_time="2026-01-02T00:00:00Z")


def test_neighbors_require_a_data_interval_timetable() -> None:
    with pytest.raises(ValueError, match="prev_ds"):
        _resolve(
            semantics="trigger_cron", cron="0 2 * * *", trigger_time="2026-01-02T02:00:00Z", publish_neighbors=True
        )


# --------------------------------------------------------------------------------------
# The generated notebook embeds the same arithmetic
# --------------------------------------------------------------------------------------


def test_generated_resolver_runs_without_dbutils_and_matches_the_runtime() -> None:
    semantics = logical_dates.LogicalDateSemantics(
        kind=runtime.DATA_INTERVAL_CRON, generation="2", cron="0 2 * * 1-5", timezone="UTC"
    )
    namespace = _generated_namespace(semantics)
    values = namespace["resolve"](
        trigger_time="2026-01-05T02:00:00Z", trigger_type="periodic", override="", **namespace["_TIMETABLE"]
    )
    assert values["ds"] == "2026-01-02"
    assert values["prev_ds"] == "2026-01-01"
    source = logical_dates.resolver_source(semantics)
    assert source.startswith("# Databricks notebook source\n")
    ast.parse(source)


def test_generated_resolver_publishes_every_value_as_a_task_value() -> None:
    published: dict[str, str] = {}

    class _Widgets:
        values = {"trigger_time": "2026-01-02T02:00:00Z", "trigger_type": "periodic", "logical_date_override": ""}

        def text(self, name: str, default: str) -> None:
            pass

        def get(self, name: str) -> str:
            return self.values[name]

    class _TaskValues:
        def set(self, *, key: str, value: str) -> None:
            published[key] = value

    class _Jobs:
        taskValues = _TaskValues()

    class _Dbutils:
        widgets = _Widgets()
        jobs = _Jobs()

    semantics = logical_dates.LogicalDateSemantics(kind=runtime.DATA_INTERVAL_CRON, generation="3", cron="0 2 * * *")
    namespace: dict[str, Any] = {"__name__": "generated_resolver", "dbutils": _Dbutils()}
    exec(compile(logical_dates.resolver_source(semantics), "resolver", "exec"), namespace)

    assert published["ds"] == "2026-01-01"
    assert "prev_ds" not in published  # Airflow 3 removed prev_ds / next_ds


# --------------------------------------------------------------------------------------
# Classification from DAG source
# --------------------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("source", "expected"),
    [
        ("from airflow import DAG\nDAG(dag_id='d', schedule_interval='@daily')\n", "2"),
        ("from airflow.operators.python import PythonOperator\n", "2"),
        ("from airflow.utils.dates import days_ago\n", "2"),
        ("from airflow.sdk import DAG\n", "3"),
        ("from airflow.providers.standard.operators.python import PythonOperator\n", "3"),
        ("from airflow.operators.python_operator import PythonOperator\n", "1.10"),
        ("from airflow.sdk import DAG\nfrom airflow.operators.bash import BashOperator\n", "unknown"),
        ("from airflow import DAG\n", "unknown"),
    ],
)
def test_airflow_generation_signals(source: str, expected: str) -> None:
    module = ast.parse(source)
    dag_kwargs = {"schedule_interval": ast.Constant("@daily")} if "schedule_interval" in source else {}
    assert logical_dates.airflow_generation(module, dag_kwargs)[0] == expected


def test_resolver_is_emitted_for_sql_and_notebook_consumers_and_reconciles(tmp_path: Path) -> None:
    pipeline = _load(
        tmp_path,
        """
        from airflow import DAG
        from airflow.providers.databricks.operators.databricks import DatabricksNotebookOperator
        from airflow.providers.databricks.operators.databricks_sql import DatabricksSqlOperator
        with DAG(dag_id="d", schedule_interval="0 2 * * 1-5", timezone="Europe/Madrid") as dag:
            q = DatabricksSqlOperator(task_id="q", sql="SELECT * FROM t WHERE d = '{{ ds }}'")
            nb = DatabricksNotebookOperator(task_id="nb", notebook_path="/x", notebook_params={"p": "{{ ts_nodash }}"})
            q >> nb
        """,
    )
    tasks = _by_key(pipeline)
    assert pipeline.reconciliation_status == "verified"
    resolver = tasks[RESOLVER]
    assert isinstance(resolver, NotebookActivity)
    assert "'cron': '0 2 * * 1-5'" in (resolver.generated_source or "")
    assert "'zone_name': 'Europe/Madrid'" in (resolver.generated_source or "")
    sql = tasks["q"]
    assert isinstance(sql, SqlActivity)
    assert sql.parameters == {"__flowx_airflow_date_ds": "{{tasks.__flowx_airflow_dates.values.ds}}"}
    assert [dependency.task_key for dependency in sql.depends_on or []] == [RESOLVER]
    notebook = tasks["nb"]
    assert notebook.base_parameters == {"p": "{{tasks.__flowx_airflow_dates.values.ts_nodash}}"}
    assert [dependency.task_key for dependency in notebook.depends_on or []] == ["q"]
    proof = next(item for item in pipeline.audit["transformations"] if item["code"] == "logical_date_resolver_emitted")
    assert proof["semantics"] == "data_interval_cron"
    assert proof["consumer_task_keys"] == ["nb", "q"]
    assert proof["attached_root_task_keys"] == ["q"]
    assert proof["emitted_edges"] == [[RESOLVER, "q"]]
    assert {parameter["name"] for parameter in pipeline.parameters or []} == {
        "__flowx_airflow_trigger_time",
        "__flowx_airflow_trigger_type",
        "__flowx_airflow_logical_date",
    }


def test_dag_without_interval_macros_gets_no_resolver(tmp_path: Path) -> None:
    pipeline = _load(
        tmp_path,
        """
        from airflow import DAG
        from airflow.providers.databricks.operators.databricks import DatabricksNotebookOperator
        with DAG(dag_id="d", schedule="0 2 * * *") as dag:
            nb = DatabricksNotebookOperator(task_id="nb", notebook_path="/x")
        """,
    )
    assert RESOLVER not in _by_key(pipeline)
    assert not pipeline.parameters


def test_undeterminable_airflow_version_turns_consumers_into_gaps(tmp_path: Path) -> None:
    pipeline = _load(
        tmp_path,
        """
        from airflow import DAG
        from airflow.providers.databricks.operators.databricks import DatabricksNotebookOperator
        with DAG(dag_id="d", schedule="0 2 * * *") as dag:
            nb = DatabricksNotebookOperator(task_id="nb", notebook_path="/x", notebook_params={"p": "{{ ds }}"})
        """,
    )
    tasks = _by_key(pipeline)
    assert isinstance(tasks["nb"], PlaceholderActivity)
    assert "Airflow version cannot be determined" in tasks["nb"].comment
    assert RESOLVER not in tasks
    assert "airflow_logical_date_semantics_undeterminable" in _codes(pipeline)


def test_airflow3_raw_cron_uses_the_fire_time_and_discloses_the_assumption(tmp_path: Path) -> None:
    pipeline = _load(
        tmp_path,
        """
        from airflow.sdk import DAG
        from airflow.providers.databricks.operators.databricks import DatabricksNotebookOperator
        with DAG(dag_id="d", schedule="0 2 * * *") as dag:
            nb = DatabricksNotebookOperator(task_id="nb", notebook_path="/x", notebook_params={"p": "{{ ds }}"})
        """,
    )
    resolver = _by_key(pipeline)[RESOLVER]
    assert "'semantics': 'trigger_cron'" in (resolver.generated_source or "")
    assert "'publish_neighbors': False" in (resolver.generated_source or "")
    assert "airflow3_cron_data_intervals_assumed_false" in _codes(pipeline)


def test_airflow3_explicit_data_interval_timetable_shifts_to_the_previous_tick() -> None:
    semantics = logical_dates.classify(
        ast.parse("from airflow.sdk import DAG\n"),
        dag_kwargs={"schedule": ast.parse("CronDataIntervalTimetable('0 2 * * *', timezone='UTC')").body[0].value},  # type: ignore[attr-defined]
        schedule_node=ast.parse("CronDataIntervalTimetable('0 2 * * *', timezone='UTC')").body[0].value,  # type: ignore[attr-defined]
        schedule_interval=None,
        timezone=None,
        schedule=None,
    )
    assert (semantics.kind, semantics.cron, semantics.generation) == ("data_interval_cron", "0 2 * * *", "3")
    namespace = _generated_namespace(semantics)
    values = namespace["resolve"](
        trigger_time="2026-01-02T02:00:00Z", trigger_type="periodic", override="", **namespace["_TIMETABLE"]
    )
    assert values["ds"] == "2026-01-01"


def test_airflow3_prev_ds_is_a_gap(tmp_path: Path) -> None:
    pipeline = _load(
        tmp_path,
        """
        from airflow.sdk import DAG
        from airflow.providers.databricks.operators.databricks import DatabricksNotebookOperator
        with DAG(dag_id="d", schedule="0 2 * * *") as dag:
            nb = DatabricksNotebookOperator(task_id="nb", notebook_path="/x", notebook_params={"p": "{{ prev_ds }}"})
        """,
    )
    task = _by_key(pipeline)["nb"]
    assert isinstance(task, PlaceholderActivity)
    assert "prev_ds exist only for Airflow 2" in task.comment


def test_airflow2_default_schedule_resolves_with_a_daily_delta(tmp_path: Path) -> None:
    pipeline = _load(
        tmp_path,
        """
        from airflow import DAG
        from airflow.operators.bash import BashOperator
        with DAG(dag_id="d") as dag:
            run = BashOperator(task_id="run", bash_command="echo {{ ds_nodash }}")
        """,
    )
    resolver = _by_key(pipeline)[RESOLVER]
    assert "'semantics': 'data_interval_delta'" in (resolver.generated_source or "")
    assert "'delta_seconds': 86400" in (resolver.generated_source or "")
    assert "airflow2_default_schedule_assumed" in _codes(pipeline)


@pytest.mark.parametrize(
    ("trigger_rule", "run_if"),
    [
        ("all_failed", "ALL_FAILED"),
        ("one_success", "AT_LEAST_ONE_SUCCESS"),
        ("none_failed", "NONE_FAILED"),
        ("all_done", "ALL_DONE"),
        ("one_failed", "AT_LEAST_ONE_FAILED"),
    ],
)
def test_resolver_never_joins_a_consumer_trigger_rule(tmp_path: Path, trigger_rule: str, run_if: str) -> None:
    pipeline = _load(
        tmp_path,
        f"""
        from airflow import DAG
        from airflow.operators.bash import BashOperator
        with DAG(dag_id="d", schedule_interval="@daily") as dag:
            work = BashOperator(task_id="work", bash_command="true")
            handler = BashOperator(task_id="handler", bash_command="echo {{{{ ds }}}}", trigger_rule="{trigger_rule}")
            work >> handler
        """,
    )
    from flowx.preparer.workflow_preparer import prepare_workflow

    assert pipeline.reconciliation_status == "verified"
    prepared = {task["task_key"]: task for task in prepare_workflow(pipeline).tasks}
    assert prepared["handler"]["depends_on"] == [{"task_key": "work"}]
    assert prepared["handler"]["run_if"] == run_if
    assert prepared["work"]["depends_on"] == [{"task_key": RESOLVER}]
    assert "run_if" not in prepared["work"]


def test_root_consumer_depends_directly_on_the_resolver(tmp_path: Path) -> None:
    pipeline = _load(
        tmp_path,
        """
        from airflow import DAG
        from airflow.operators.bash import BashOperator
        with DAG(dag_id="d", schedule_interval="@daily") as dag:
            first = BashOperator(task_id="first", bash_command="echo {{ ds }}", trigger_rule="all_failed")
            second = BashOperator(task_id="second", bash_command="true")
            first >> second
        """,
    )
    from flowx.preparer.workflow_preparer import prepare_workflow

    prepared = {task["task_key"]: task for task in prepare_workflow(pipeline).tasks}
    assert prepared["first"]["depends_on"] == [{"task_key": RESOLVER}]
    assert "run_if" not in prepared["first"]
    assert prepared["second"]["depends_on"] == [{"task_key": "first"}]


def test_roots_that_feed_no_consumer_do_not_wait_for_the_resolver(tmp_path: Path) -> None:
    pipeline = _load(
        tmp_path,
        """
        from airflow import DAG
        from airflow.operators.bash import BashOperator
        with DAG(dag_id="d", schedule_interval="@daily") as dag:
            independent = BashOperator(task_id="independent", bash_command="true")
            upstream = BashOperator(task_id="upstream", bash_command="true")
            consumer = BashOperator(task_id="consumer", bash_command="echo {{ ds }}")
            upstream >> consumer
        """,
    )
    tasks = _by_key(pipeline)
    assert not tasks["independent"].depends_on
    assert [dependency.task_key for dependency in tasks["upstream"].depends_on or []] == [RESOLVER]
