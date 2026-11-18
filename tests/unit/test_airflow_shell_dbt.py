"""Tests for dbt commands run from an Airflow BashOperator becoming native Databricks dbt tasks."""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from flowx.bundler.dab_writer import pipeline_dict_to_ir, write_bundle
from flowx.ir_serde import pipeline_to_dict
from flowx.models.ir import DbtCommandActivity, NotebookActivity, Pipeline, PlaceholderActivity
from flowx.preparer.workflow_preparer import prepare_workflow
from flowx.sources.airflow import shell_dbt
from flowx.sources.airflow.loader import load_airflow_dag


@pytest.mark.parametrize(
    ("command", "working_directory", "commands", "project_dir"),
    [
        ("$DBT_PATH snapshot --select snp", "/srv/proj", ["dbt snapshot --select snp"], "/srv/proj"),
        ("${DBT_BIN} run-operation merge_dim", "/srv/proj", ["dbt run-operation merge_dim"], "/srv/proj"),
        ("/opt/venv/bin/dbt build", "/srv/proj", ["dbt build"], "/srv/proj"),
        ("cd /opt/proj && dbt deps && dbt run", None, ["dbt deps", "dbt run"], "/opt/proj"),
        ("cd proj\ndbt run", "/srv", ["dbt run"], "/srv/proj"),
        ("dbt run --project-dir=../other", "/srv/proj", ["dbt run"], "/srv/other"),
        ("dbt run --vars '{\"day\": 1}'", "/p", ["dbt run --vars '{\"day\": 1}'"], "/p"),
        ("dbt test", None, ["dbt test"], None),
    ],
)
def test_dbt_only_commands_are_recognized(
    command: str, working_directory: str | None, commands: list[str], project_dir: str | None
) -> None:
    parsed = shell_dbt.parse(command, working_directory)

    assert parsed is not None
    assert parsed.commands == commands
    assert parsed.project_dir == project_dir


def test_profile_options_are_dropped_and_reported() -> None:
    parsed = shell_dbt.parse("dbt run --target prod --profiles-dir ~/.dbt --select a", "/p")

    assert parsed is not None
    assert parsed.commands == ["dbt run --select a"]
    assert parsed.dropped_options == ["--target prod", "--profiles-dir ~/.dbt"]


@pytest.mark.parametrize(
    "command",
    [
        "dbt run; dbt test",
        "dbt run\ndbt test",
        "dbt run || true",
        "dbt run | tee run.log",
        "dbt run > run.log",
        "dbt run &",
        "dbt run --select $MODEL",
        "dbt run --select {{ params.model }}",
        "dbt run $(cat args)",
        "source venv/bin/activate && dbt run",
        "dbt frobnicate",
        "dbt run && echo done",
        "echo hi",
    ],
)
def test_commands_doing_more_than_dbt_are_not_recognized(command: str) -> None:
    assert shell_dbt.parse(command, "/p") is None


def _load(tmp_path: Path, task: str) -> Pipeline:
    dag_path = tmp_path / "dag.py"
    dag_path.write_text(
        "from airflow import DAG\n"
        "from airflow.operators.bash import BashOperator\n"
        "with DAG(dag_id='d') as dag:\n"
        f"    t = {task}\n",
        encoding="utf-8",
    )
    return load_airflow_dag(dag_path)


def _task(pipeline: Pipeline):
    return next(task for task in pipeline.tasks if task.task_key == "t")


def test_bash_dbt_task_becomes_a_dbt_task_with_cwd_as_its_project(tmp_path: Path) -> None:
    pipeline = _load(
        tmp_path, "BashOperator(task_id='t', bash_command='$DBT_PATH run --select dim', cwd='/srv/proj', retries=2)"
    )

    task = _task(pipeline)
    assert isinstance(task, DbtCommandActivity)
    assert task.commands == ["dbt run --select dim"]
    assert task.project_dir == "/srv/proj"
    assert task.max_retries == 2
    assert pipeline.reconciliation_status == "verified"
    disclosure = next(
        item for item in pipeline.audit["transformations"] if item["code"] == "shell_dbt_lowered_to_dbt_task"
    )
    assert disclosure["executable"] == "$DBT_PATH"
    arguments = next(
        item
        for item in pipeline.audit["transformations"]
        if item["code"] == "operator_arguments_classified" and item["capture_id"] == "t"
    )["arguments"]
    assert {"name": "cwd", "status": "consumed", "rationale": "operator_adapter"} in arguments


def test_cwd_stays_a_gap_for_a_command_that_is_not_dbt(tmp_path: Path) -> None:
    pipeline = _load(tmp_path, "BashOperator(task_id='t', bash_command='make all', cwd='/srv/proj')")

    task = _task(pipeline)
    assert isinstance(task, PlaceholderActivity)
    assert "cwd" in task.comment


def test_dbt_without_a_known_project_is_a_gap(tmp_path: Path) -> None:
    pipeline = _load(tmp_path, "BashOperator(task_id='t', bash_command='dbt run')")

    task = _task(pipeline)
    assert isinstance(task, PlaceholderActivity)
    assert "cannot tell which dbt project" in task.comment
    assert task.raw_definition["dbt_commands"] == ["dbt run"]


def test_dbt_with_a_dynamic_cwd_is_not_lowered(tmp_path: Path) -> None:
    pipeline = _load(tmp_path, "BashOperator(task_id='t', bash_command='dbt run', cwd=PROJECT_DIR)")

    assert isinstance(_task(pipeline), PlaceholderActivity)


def test_dbt_with_other_unrepresented_arguments_is_a_gap(tmp_path: Path) -> None:
    pipeline = _load(tmp_path, "BashOperator(task_id='t', bash_command='dbt run', cwd='/p', env={'A': '1'})")

    task = _task(pipeline)
    assert isinstance(task, PlaceholderActivity)
    assert not any(item["code"] == "shell_dbt_lowered_to_dbt_task" for item in pipeline.audit["transformations"])


def _bundle(tmp_path: Path, activity: DbtCommandActivity) -> tuple[dict, Path]:
    workflow = prepare_workflow(Pipeline(name="p", tasks=[activity]))
    out = tmp_path / "bundle"
    write_bundle(workflow, out, catalog="main", schema="a")
    job = yaml.safe_load(next((out / "resources").glob("*.yml")).read_text())
    return next(iter(job["resources"]["jobs"].values())), out


def _project(tmp_path: Path, *files: str) -> Path:
    project = tmp_path / "intelsat_ca"
    (project / "models").mkdir(parents=True)
    (project / "dbt_project.yml").write_text("name: intelsat_ca\n", encoding="utf-8")
    (project / "models" / "dim.sql").write_text("select 1\n", encoding="utf-8")
    for name in files:
        (project / name).write_text("packages: []\n", encoding="utf-8")
    return project


def test_dbt_task_runs_on_the_warehouse_with_the_project_in_the_bundle(tmp_path: Path) -> None:
    project = _project(tmp_path)
    job, out = _bundle(
        tmp_path, DbtCommandActivity(name="t", task_key="t", commands=["dbt run"], project_dir=str(project))
    )

    task = job["tasks"][0]
    assert task["dbt_task"] == {
        "project_directory": "../src/dbt_projects/intelsat_ca",
        "commands": ["dbt run"],
        "warehouse_id": "${var.warehouse_id}",
        "catalog": "${var.catalog}",
        "schema": "${var.schema}",
        "source": "WORKSPACE",
    }
    assert task["job_cluster_key"] == "default_cluster"
    assert (out / "src" / "dbt_projects" / "intelsat_ca" / "models" / "dim.sql").is_file()
    assert "warehouse_id" in yaml.safe_load((out / "databricks.yml").read_text())["variables"]


def test_project_with_packages_installs_them_first(tmp_path: Path) -> None:
    project = _project(tmp_path, "packages.yml")
    job, _ = _bundle(
        tmp_path, DbtCommandActivity(name="t", task_key="t", commands=["dbt run"], project_dir=str(project))
    )

    assert job["tasks"][0]["dbt_task"]["commands"] == ["dbt deps", "dbt run"]


def test_missing_project_becomes_a_failing_setup_task(tmp_path: Path) -> None:
    job, out = _bundle(
        tmp_path,
        DbtCommandActivity(name="t", task_key="t", commands=["dbt run"], project_dir="/home/ec2-user/airflow/proj"),
    )

    task = job["tasks"][0]
    assert "dbt_task" not in task
    notebook = out / "src" / "notebooks" / "t_dbt_project_required.py"
    assert "Copy it to src/dbt_projects/proj" in notebook.read_text()


def test_dbt_command_activity_round_trips_through_the_report() -> None:
    pipeline = Pipeline(
        name="p",
        tasks=[
            NotebookActivity(name="a", task_key="a", notebook_path="notebooks/a.py"),
            DbtCommandActivity(name="t", task_key="t", commands=["dbt deps", "dbt run"], project_dir="/srv/proj"),
        ],
    )

    rehydrated, _ = pipeline_dict_to_ir(pipeline_to_dict(pipeline))

    task = rehydrated.tasks[1]
    assert isinstance(task, DbtCommandActivity)
    assert task.commands == ["dbt deps", "dbt run"]
    assert task.project_dir == "/srv/proj"
