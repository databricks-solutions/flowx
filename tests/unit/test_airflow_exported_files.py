"""Unit tests for packaging files a DAG names by their Airflow-machine path from the export."""

from __future__ import annotations

from pathlib import Path

import pytest

from flowx.models.ir import PlaceholderActivity, SparkPythonActivity
from flowx.preparer.workflow_preparer import prepare_workflow
from flowx.sources.airflow.loader import load_airflow_dags


@pytest.mark.parametrize(
    "task",
    [
        "BashOperator(task_id='job', bash_command='spark-submit /home/jdoe/jobs/reprocess.py --ff-days 3')",
        "SparkSubmitOperator(task_id='job', application='/home/jdoe/jobs/reprocess.py', "
        "application_args=['--ff-days', '3'])",
    ],
)
def test_spark_submit_script_from_the_export_is_packaged(tmp_path: Path, task: str) -> None:
    script = tmp_path / "export" / "home" / "jdoe" / "jobs" / "reprocess.py"
    script.parent.mkdir(parents=True)
    script.write_text("import argparse\nprint('reprocess')\n", encoding="utf-8")

    pipeline = _load(tmp_path, task)
    activity = pipeline.tasks[0]

    assert isinstance(activity, SparkPythonActivity)
    assert activity.generated_source == "import argparse\nprint('reprocess')\n"
    assert activity.parameters == ["--ff-days", "3"]
    codes = [item["code"] for item in pipeline.audit["transformations"]]
    assert "spark_submit_application_packaged" in codes


def test_spark_submit_date_macro_stays_one_argument(tmp_path: Path) -> None:
    script = tmp_path / "export" / "home" / "jdoe" / "jobs" / "summary.py"
    script.parent.mkdir(parents=True)
    script.write_text("print('summary')\n", encoding="utf-8")

    command = "spark-submit /home/jdoe/jobs/summary.py --run-date {{ ds }}"
    pipeline = _load(tmp_path, f"BashOperator(task_id='job', bash_command='{command}')")
    activity = next(task for task in pipeline.tasks if task.task_key == "job")

    assert isinstance(activity, SparkPythonActivity)
    assert activity.parameters is not None and len(activity.parameters) == 2
    assert activity.parameters[0] == "--run-date"
    assert "{{ ds }}" not in activity.parameters[1] and activity.parameters[1].startswith("{{tasks.")


def test_braces_in_a_packaged_script_are_not_treated_as_templates(tmp_path: Path) -> None:
    # Airflow renders the command, never the script, so {{ }} text inside the script is just code or comments.
    script = tmp_path / "export" / "home" / "jdoe" / "jobs" / "summary.py"
    script.parent.mkdir(parents=True)
    script.write_text("# The date comes from Airflow's {{ ds }}.\nprint(f'{{literal braces}}')\n", encoding="utf-8")

    pipeline = _load(tmp_path, "BashOperator(task_id='job', bash_command='spark-submit /home/jdoe/jobs/summary.py')")

    assert isinstance(pipeline.tasks[0], SparkPythonActivity)
    assert "{{ ds }}" in pipeline.tasks[0].generated_source


@pytest.mark.parametrize(
    "task",
    [
        "BashOperator(task_id='job', bash_command='spark-submit /opt/jobs/etl.py')",
        "SparkSubmitOperator(task_id='job', application='/opt/jobs/etl.py')",
    ],
)
def test_spark_submit_script_missing_from_the_export_is_a_gap(tmp_path: Path, task: str) -> None:
    pipeline = _load(tmp_path, task)
    activity = pipeline.tasks[0]

    assert isinstance(activity, PlaceholderActivity)
    assert "'/opt/jobs/etl.py'" in activity.comment
    assert "was not found" in activity.comment
    assert "Copy the script into the folder discover reads, then convert again" in activity.comment


def test_spark_submit_script_matching_several_export_files_is_a_gap(tmp_path: Path) -> None:
    for owner in ("alice", "bob"):
        script = tmp_path / "export" / owner / "jobs" / "etl.py"
        script.parent.mkdir(parents=True)
        script.write_text("print(1)\n", encoding="utf-8")

    pipeline = _load(tmp_path, "SparkSubmitOperator(task_id='job', application='/home/carol/etl.py')")

    assert isinstance(pipeline.tasks[0], PlaceholderActivity)
    assert "matches 2 files" in pipeline.tasks[0].comment


@pytest.mark.parametrize("application", ["dbfs:/jobs/etl.py", "s3://bucket/etl.py"])
def test_remote_and_dbfs_spark_submit_scripts_keep_their_handling(tmp_path: Path, application: str) -> None:
    pipeline = _load(tmp_path, f"SparkSubmitOperator(task_id='job', application='{application}')")
    activity = pipeline.tasks[0]

    assert isinstance(activity, SparkPythonActivity)
    assert activity.python_file == application
    assert activity.generated_source is None


def test_databricks_paths_are_left_to_package(tmp_path: Path) -> None:
    script = tmp_path / "export" / "Workspace" / "jobs" / "etl.py"
    script.parent.mkdir(parents=True)
    script.write_text("print(1)\n", encoding="utf-8")

    pipeline = _load(tmp_path, "BashOperator(task_id='job', bash_command='spark-submit /Workspace/jobs/etl.py')")

    assert pipeline.tasks[0].generated_source is None


def _write_script(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def _packaged(prepared) -> tuple[dict[str, str], dict[str, str]]:
    """Returns each task's python_file path (under src/) and the notebook content at each packaged path."""
    python_files = {
        task["task_key"]: task["spark_python_task"]["python_file"].removeprefix("../src/") for task in prepared.tasks
    }
    contents = {notebook.relative_path: notebook.content for notebook in prepared.notebooks}
    return python_files, contents


def test_different_scripts_with_one_file_name_package_to_distinct_files(tmp_path: Path) -> None:
    _write_script(tmp_path / "export" / "home" / "alice" / "jobs" / "load.py", "print('alice')\n")
    _write_script(tmp_path / "export" / "home" / "bob" / "jobs" / "load.py", "print('bob')\n")
    pipeline = _load_dag(
        tmp_path,
        "BashOperator(task_id='load_alice', bash_command='spark-submit /home/alice/jobs/load.py')",
        "BashOperator(task_id='load_bob', bash_command='spark-submit /home/bob/jobs/load.py')",
    )

    prepared = prepare_workflow(pipeline)

    python_files, contents = _packaged(prepared)
    assert python_files == {
        "load_alice": "scripts/load_alice/load.py",
        "load_bob": "scripts/load_bob/load.py",
    }
    assert contents[python_files["load_alice"]] == "print('alice')\n"
    assert contents[python_files["load_bob"]] == "print('bob')\n"
    assert len(prepared.notebooks) == 2


def test_dbfs_scripts_with_one_file_name_package_to_distinct_files(tmp_path: Path) -> None:
    # Databricks paths are placeholders when downloads are off; each placeholder names its own source path.
    first, second = "dbfs:/a/load.py", "dbfs:/b/load.py"
    pipeline = _load_dag(
        tmp_path,
        f"SparkSubmitOperator(task_id='load_first', application='{first}')",
        f"SparkSubmitOperator(task_id='load_second', application='{second}')",
    )

    prepared = prepare_workflow(pipeline)

    python_files, contents = _packaged(prepared)
    assert python_files["load_first"] != python_files["load_second"]
    assert first in contents[python_files["load_first"]]
    assert second in contents[python_files["load_second"]]
    assert second not in contents[python_files["load_first"]]


def test_tasks_running_one_script_share_one_packaged_file(tmp_path: Path) -> None:
    _write_script(tmp_path / "export" / "home" / "alice" / "jobs" / "load.py", "print('load')\n")
    pipeline = _load_dag(
        tmp_path,
        "BashOperator(task_id='worker_1', bash_command='spark-submit /home/alice/jobs/load.py --part 1')",
        "BashOperator(task_id='worker_2', bash_command='spark-submit /home/alice/jobs/load.py --part 2')",
    )

    prepared = prepare_workflow(pipeline)

    python_files, contents = _packaged(prepared)
    assert python_files == {"worker_1": "scripts/load.py", "worker_2": "scripts/load.py"}
    assert list(contents) == ["scripts/load.py"]


def test_script_paths_do_not_depend_on_task_order(tmp_path: Path) -> None:
    _write_script(tmp_path / "export" / "home" / "alice" / "jobs" / "load.py", "print('alice')\n")
    _write_script(tmp_path / "export" / "home" / "bob" / "jobs" / "load.py", "print('bob')\n")
    alice = "BashOperator(task_id='load_alice', bash_command='spark-submit /home/alice/jobs/load.py')"
    bob = "BashOperator(task_id='load_bob', bash_command='spark-submit /home/bob/jobs/load.py')"

    def packaged_paths(*tasks: str) -> dict[str, str]:
        prepared = prepare_workflow(_load_dag(tmp_path, *tasks))
        return _packaged(prepared)[0]

    assert packaged_paths(alice, bob) == packaged_paths(bob, alice)


def _load_dag(tmp_path: Path, *tasks: str):
    root = tmp_path / "export"
    dag_path = root / "dags" / "dag.py"
    dag_path.parent.mkdir(parents=True, exist_ok=True)
    body = "".join(f"    {task}\n" for task in tasks)
    dag_path.write_text(
        "from airflow import DAG\n"
        "from airflow.operators.bash import BashOperator\n"
        "from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator\n"
        "with DAG(dag_id='d', schedule_interval='15 * * * *', catchup=False) as dag:\n"
        f"{body}",
        encoding="utf-8",
    )
    pipelines = load_airflow_dags(dag_path, source_root=root)
    assert len(pipelines) == 1
    return pipelines[0]


def _load(tmp_path: Path, task: str):
    root = tmp_path / "export"
    dag_path = root / "dags" / "dag.py"
    dag_path.parent.mkdir(parents=True, exist_ok=True)
    dag_path.write_text(
        "from airflow import DAG\n"
        "from airflow.operators.bash import BashOperator\n"
        "from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator\n"
        "with DAG(dag_id='d', schedule_interval='15 * * * *', catchup=False) as dag:\n"
        f"    {task}\n",
        encoding="utf-8",
    )
    pipelines = load_airflow_dags(dag_path, source_root=root)
    assert len(pipelines) == 1
    return pipelines[0]
