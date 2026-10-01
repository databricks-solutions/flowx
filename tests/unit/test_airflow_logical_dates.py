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


def test_daylight_saving_gap_moves_the_nonexistent_tick_forward() -> None:
    # 02:30 on 2026-03-08 does not exist in New York; Airflow moves that tick to 03:30 EDT (07:30Z), so
    # the run firing 03-09 02:30 EDT covers the interval starting at the moved tick.
    values = _resolve(
        semantics="data_interval_cron",
        cron="30 2 * * *",
        zone_name="America/New_York",
        trigger_time="2026-03-09T06:30:00Z",
    )
    assert values["data_interval_start"] == "2026-03-08T07:30:00+00:00"
    assert values["ds"] == "2026-03-08"


@pytest.mark.parametrize("march_eighth_fire", ["2026-03-08T10:00:00Z", "2026-03-08T09:00:00Z"])
def test_spring_forward_runs_get_consecutive_partitions(march_eighth_fire: str) -> None:
    fires = ["2026-03-07T10:00:00Z", march_eighth_fire, "2026-03-09T09:00:00Z", "2026-03-10T09:00:00Z"]
    runs = [
        _resolve(semantics="data_interval_cron", cron="0 2 * * *", zone_name="America/Los_Angeles", trigger_time=fire)
        for fire in fires
    ]
    assert [run["ds"] for run in runs] == ["2026-03-06", "2026-03-07", "2026-03-08", "2026-03-09"]
    for earlier, later in zip(runs, runs[1:], strict=False):
        assert earlier["data_interval_end"] == later["data_interval_start"]


def test_fall_back_hourly_intervals_are_contiguous_and_one_hour_long() -> None:
    runs = [
        _resolve(
            semantics="data_interval_cron",
            cron="0 * * * *",
            zone_name="America/New_York",
            trigger_time=f"2026-11-01T{hour:02d}:00:00Z",
        )
        for hour in range(3, 10)
    ]
    for earlier, later in zip(runs, runs[1:], strict=False):
        assert earlier["data_interval_end"] == later["data_interval_start"]
    for run in runs:
        start = runtime.parse_instant(run["data_interval_start"])
        end = runtime.parse_instant(run["data_interval_end"])
        assert (end - start).total_seconds() == 3600


@pytest.mark.parametrize(
    ("expression", "days_unrestricted", "weekdays_unrestricted"),
    [
        ("0 0 * * *", True, True),
        ("0 0 */2 * *", False, True),
        ("0 0 * * */2", True, False),
        ("0 0 */2 * 1", False, False),
        ("0 0 1-31 * 1", False, False),
        ("0 0 1-31 * *", True, True),
    ],
)
def test_day_fields_follow_croniter_restriction_rules(
    expression: str, days_unrestricted: bool, weekdays_unrestricted: bool
) -> None:
    schedule = runtime.CronSchedule(expression)
    assert schedule.days_unrestricted is days_unrestricted
    assert schedule.weekdays_unrestricted is weekdays_unrestricted


def test_restricted_step_day_of_month_ors_with_day_of_week() -> None:
    from datetime import datetime

    schedule = runtime.CronSchedule("0 0 */2 * 1")
    assert schedule.day_matches(datetime(2026, 1, 3))  # odd day of month (a Saturday)
    assert schedule.day_matches(datetime(2026, 1, 12))  # even day of month, but a Monday
    assert not schedule.day_matches(datetime(2026, 1, 6))  # even day of month, a Tuesday


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


def _classify_timedelta(imports: str) -> logical_dates.LogicalDateSemantics:
    node = ast.parse("timedelta(hours=6)").body[0].value  # type: ignore[attr-defined]
    return logical_dates.classify(
        ast.parse(imports),
        dag_kwargs={"schedule": node},
        schedule_node=node,
        schedule_interval=None,
        timezone=None,
        schedule=None,
    )


def test_airflow2_timedelta_schedule_uses_data_intervals() -> None:
    semantics = _classify_timedelta("from airflow.operators.bash import BashOperator\n")
    assert (semantics.kind, semantics.delta_seconds) == ("data_interval_delta", 21600)
    assert not semantics.disclosures


def test_airflow3_timedelta_schedule_uses_the_fire_time_and_discloses_the_assumption() -> None:
    semantics = _classify_timedelta("from airflow.sdk import DAG\n")
    assert (semantics.kind, semantics.delta_seconds) == ("trigger_delta", 21600)
    assert [code for code, _ in semantics.disclosures] == ["airflow3_delta_data_intervals_assumed_false"]
    namespace = _generated_namespace(semantics)
    values = namespace["resolve"](
        trigger_time="2026-01-02T06:00:00Z", trigger_type="periodic", override="", **namespace["_TIMETABLE"]
    )
    assert values["ts"] == "2026-01-02T06:00:00+00:00"
    assert values["data_interval_start"] == values["data_interval_end"] == "2026-01-02T06:00:00+00:00"


def test_timedelta_schedule_with_an_unknown_airflow_version_is_undeterminable() -> None:
    semantics = _classify_timedelta("from airflow import DAG\n")
    assert semantics.kind == logical_dates.UNDETERMINABLE
    assert not semantics.resolvable


def test_airflow3_raw_cron_aligns_a_late_trigger_to_its_tick() -> None:
    values = _resolve(semantics="trigger_cron", cron="0 0 * * *", trigger_time="2026-01-01T00:00:03.250Z")
    assert values["ts"] == "2026-01-01T00:00:00+00:00"
    assert values["ts_nodash"] == "20260101T000000"
    assert values["logical_date"] == values["data_interval_start"] == values["data_interval_end"] == values["ts"]


def test_manual_trigger_cron_run_keeps_its_trigger_time_to_the_second() -> None:
    values = _resolve(
        semantics="trigger_cron", cron="0 0 * * *", trigger_time="2026-01-01T15:30:12.345Z", trigger_type="one_time"
    )
    assert values["ts"] == "2026-01-01T15:30:12+00:00"


@pytest.mark.parametrize("semantics", ["data_interval_delta", "trigger_delta"])
def test_delta_runs_align_to_their_anchor(semantics: str) -> None:
    values = _resolve(
        semantics=semantics,
        delta_seconds=21600,
        anchor_time="2026-01-01T00:00:00Z",
        trigger_time="2026-01-02T06:00:04.900Z",
    )
    expected_end = "2026-01-02T06:00:00+00:00"
    assert values["data_interval_end"] == expected_end
    if semantics == "trigger_delta":
        assert values["ts"] == expected_end
    else:
        assert values["ts"] == values["data_interval_start"] == "2026-01-02T00:00:00+00:00"


def test_no_rendered_timestamp_carries_sub_second_precision() -> None:
    for semantics in ("data_interval_cron", "trigger_cron", "manual_only"):
        values = _resolve(
            semantics=semantics, cron="0 * * * *", trigger_time="2026-01-01T05:00:00.999Z", trigger_type="one_time"
        )
        assert all("." not in value for value in values.values())


def _delta_dag(delta: str, start_date: str, *, uses_date: bool = True) -> str:
    command = "echo {{ ds }}" if uses_date else "echo done"
    return f"""
        from datetime import datetime, timedelta

        import pendulum
        from airflow import DAG
        from airflow.operators.bash import BashOperator

        with DAG(dag_id="delta_dag", schedule_interval={delta}, start_date={start_date}) as dag:
            BashOperator(task_id="work", bash_command="{command}")
    """


@pytest.mark.parametrize(
    ("delta", "start_date", "expected"),
    [
        ("timedelta(hours=6)", "datetime(2026, 1, 1)", ("0 0 0/6 * * ?", "UTC")),
        (
            "timedelta(hours=6)",
            'pendulum.datetime(2026, 1, 1, 1, 30, tz="America/Los_Angeles")',
            ("0 30 3/6 * * ?", "UTC"),
        ),
        (
            "timedelta(days=1)",
            'pendulum.datetime(2026, 1, 1, 5, tz="America/Los_Angeles")',
            ("0 0 5 * * ?", "America/Los_Angeles"),
        ),
        ("timedelta(minutes=15)", "datetime(2026, 1, 1, 0, 7)", ("0 7/15 * * * ?", "UTC")),
    ],
)
def test_day_dividing_timedelta_fires_on_airflow_interval_boundaries(
    tmp_path: Path, delta: str, start_date: str, expected: tuple[str, str]
) -> None:
    pipeline = _load(tmp_path, _delta_dag(delta, start_date, uses_date=False))

    assert pipeline.schedule is not None
    assert (pipeline.schedule["quartz_cron_expression"], pipeline.schedule["timezone_id"]) == expected


@pytest.mark.parametrize(
    ("delta", "start_date"),
    [
        ("timedelta(days=2)", "datetime(2026, 1, 1)"),
        ("timedelta(hours=6)", "pendulum.today()"),
        ("timedelta(days=1)", 'pendulum.datetime(2026, 1, 1, 2, 30, tz="America/Los_Angeles")'),
    ],
)
def test_timedelta_without_a_fixed_phase_stays_periodic(tmp_path: Path, delta: str, start_date: str) -> None:
    pipeline = _load(tmp_path, _delta_dag(delta, start_date, uses_date=False))

    assert pipeline.schedule is not None and pipeline.schedule["kind"] == "periodic"


def test_dates_on_a_periodic_timedelta_schedule_are_undeterminable(tmp_path: Path) -> None:
    pipeline = _load(tmp_path, _delta_dag("timedelta(days=2)", "datetime(2026, 1, 1)"))

    assert RESOLVER not in _by_key(pipeline)
    assert isinstance(_by_key(pipeline)["work"], PlaceholderActivity)
    assert "airflow_logical_date_semantics_undeterminable" in _codes(pipeline)


def test_anchored_timedelta_resolver_carries_its_start_date(tmp_path: Path) -> None:
    pipeline = _load(tmp_path, _delta_dag("timedelta(hours=6)", "datetime(2026, 1, 1, 0, 0)"))

    resolver = _by_key(pipeline)[RESOLVER]
    assert isinstance(resolver, NotebookActivity)
    assert "'anchor_time': '2026-01-01T00:00:00+00:00'" in (resolver.generated_source or "")


def test_daily_delta_intervals_stay_contiguous_across_daylight_saving() -> None:
    anchor = "2026-01-01T13:00:00+00:00"
    fires = ["2026-03-07T13:00:00Z", "2026-03-08T12:00:00Z", "2026-03-09T12:00:00Z"]
    resolved = [
        _resolve(
            semantics=runtime.DATA_INTERVAL_DELTA,
            delta_seconds=86400,
            zone_name="America/Los_Angeles",
            anchor_time=anchor,
            trigger_time=fire,
        )
        for fire in fires
    ]

    assert [values["ds"] for values in resolved] == ["2026-03-06", "2026-03-07", "2026-03-08"]
    assert [values["data_interval_end"] for values in resolved] == [
        "2026-03-07T13:00:00+00:00",
        "2026-03-08T12:00:00+00:00",
        "2026-03-09T12:00:00+00:00",
    ]
    for earlier, later in zip(resolved, resolved[1:], strict=False):
        assert earlier["data_interval_end"] == later["data_interval_start"]


def test_shared_preparer_does_not_depend_on_the_airflow_front_end() -> None:
    import subprocess
    import sys

    probe = (
        "import sys, flowx.preparer.workflow_preparer\n"
        "assert 'flowx.sources.airflow.templating' not in sys.modules, 'preparer imported Airflow templating'\n"
    )
    result = subprocess.run([sys.executable, "-c", probe], capture_output=True, text=True, check=False)

    assert result.returncode == 0, result.stderr


def test_backfill_setup_follows_the_resolver_tag() -> None:
    from flowx.models.ir import WaitActivity
    from flowx.preparer.workflow_preparer import prepare_workflow

    pipeline = Pipeline(
        name="tagged",
        tags={"source": "airflow", "airflow_logical_date_resolver": "true"},
        tasks=[WaitActivity(name="w", task_key="w", wait_time_seconds=1)],
    )

    backfills = [task for task in prepare_workflow(pipeline).setup_tasks if task.type == "airflow_backfill"]

    assert [task.config["date_resolver"] for task in backfills] == [True]


def test_resolver_pipelines_carry_the_resolver_tag(tmp_path: Path) -> None:
    pipeline = _load(tmp_path, _delta_dag("timedelta(hours=6)", "datetime(2026, 1, 1)"))

    assert pipeline.tags.get("airflow_logical_date_resolver") == "true"


def test_try_except_version_shim_is_an_unknown_generation() -> None:
    from flowx.sources.airflow.loader import ast_utils

    module = ast.parse(
        "from airflow.operators.bash import BashOperator\n"
        "try:\n    from airflow.sdk import DAG\nexcept ImportError:\n    from airflow import DAG\n"
    )

    assert ast_utils.airflow_generation(module)[0] == "unknown"
    assert logical_dates.airflow_generation is ast_utils.airflow_generation


def test_logical_date_values_scan_nested_strings_without_copying(monkeypatch: pytest.MonkeyPatch) -> None:
    import dataclasses

    from flowx.models.ir import ForEachActivity

    def forbid_copy(*_arguments: object, **_keywords: object) -> None:
        raise AssertionError("logical_date_values deep-copied the activity")

    nested = NotebookActivity(
        name="inner",
        task_key="inner",
        notebook_path="inner.py",
        base_parameters={"{{tasks.__flowx_airflow_dates.values.ts}}": "{{tasks.__flowx_airflow_dates.values.ds}}"},
    )
    loop = ForEachActivity(name="loop", task_key="loop", items_expression="[1]", inner_activities=[nested])
    monkeypatch.setattr(dataclasses, "asdict", forbid_copy)

    assert logical_dates.logical_date_values(loop) == {"ds", "ts"}
