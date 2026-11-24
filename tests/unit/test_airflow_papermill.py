"""Unit tests for Airflow PapermillOperator -> Databricks notebook task."""

from __future__ import annotations

import json
from pathlib import Path

from flowx.models.ir import NotebookActivity, PlaceholderActivity
from flowx.sources.airflow import papermill
from flowx.sources.airflow.loader import load_airflow_dags

_IMPORTS = (
    "from datetime import timedelta\n"
    "from pathlib import Path\n"
    "from airflow.models import DAG\n"
    "from airflow.providers.papermill.operators.papermill import PapermillOperator\n"
)


def test_jupyterhub_tutorial_dag_becomes_a_packaged_notebook_task(tmp_path: Path) -> None:
    # The notebook path is built from module constants and points at the JupyterHub machine, so the
    # notebook is found by name under the folder discover reads.
    _write_notebook(
        tmp_path / "export" / "analyst" / "demos" / "scheduled_job.ipynb",
        [_markdown("# Scheduled job"), _code("df = spark.sql('select 1')\ndf.write.saveAsTable('sandbox.t')")],
    )
    pipeline = _load_one(
        tmp_path,
        _IMPORTS + "username = 'analyst'\n"
        "home_dir = Path('/home', username)\n"
        "outputs_dir = Path(home_dir, 'outputs')\n"
        "with DAG(dag_id=f'{username}_job', schedule_interval=timedelta(hours=1), catchup=False) as dag:\n"
        "    PapermillOperator(\n"
        "        task_id='run_notebook',\n"
        "        input_nb=str(Path(home_dir, 'demos/scheduled_job.ipynb')),\n"
        "        output_nb=str(Path(outputs_dir, 'scheduled_job.ipynb')),\n"
        "    )\n",
    )
    task = _by_key(pipeline)["run_notebook"]

    assert isinstance(task, NotebookActivity)
    assert task.base_parameters is None
    assert task.generated_source.startswith("# Databricks notebook source\n")
    assert "# MAGIC %md\n# MAGIC # Scheduled job" in task.generated_source
    assert "df.write.saveAsTable('sandbox.t')" in task.generated_source
    compile(task.generated_source, "notebook.py", "exec")
    disclosure = _disclosure(pipeline, "papermill_notebook_converted")
    assert disclosure["input_nb"] == "/home/analyst/demos/scheduled_job.ipynb"
    assert disclosure["output_nb"] == "/home/analyst/outputs/scheduled_job.ipynb"


def test_parameters_become_base_parameters_and_widgets_stay_task_local(tmp_path: Path) -> None:
    _write_notebook(
        tmp_path / "dags" / "report.ipynb",
        [_code("run_date = None\nlimit = 5\nregion = 'us'", tags=["parameters"]), _code("print(run_date, limit)")],
    )
    pipeline = _load_one(
        tmp_path,
        _IMPORTS + "with DAG(dag_id='report', schedule_interval='0 6 * * *') as dag:\n"
        "    PapermillOperator(task_id='report', input_nb='report.ipynb', output_nb='out.ipynb',\n"
        "        parameters={'run_date': '{{ ds }}', 'limit': 10, 'owner': 'analytics'})\n",
        dag_folder="dags",
    )
    task = _by_key(pipeline)["report"]

    assert isinstance(task, NotebookActivity)
    assert task.base_parameters is not None
    assert task.base_parameters["limit"] == "10"
    assert task.base_parameters["owner"] == "analytics"
    assert "{{ ds }}" not in task.base_parameters["run_date"]
    source = task.generated_source
    # The DAG passes a date string, so run_date is read as text even though the notebook default is None.
    assert "run_date = _papermill_parameter('run_date', 'None', as_text=True)" in source
    assert "limit = _papermill_parameter('limit', '5', as_text=False)" in source
    assert "region = _papermill_parameter('region', 'us', as_text=True)" in source
    assert "owner = _papermill_parameter('owner', '', as_text=True)" in source
    compile(source, "notebook.py", "exec")
    declared = {parameter["name"] for parameter in pipeline.parameters or []}
    assert not {"run_date", "limit", "region", "owner"} & declared


def test_missing_notebook_becomes_a_placeholder(tmp_path: Path) -> None:
    pipeline = _load_one(
        tmp_path,
        _IMPORTS + "with DAG(dag_id='gone') as dag:\n"
        "    PapermillOperator(task_id='gone', input_nb='/home/u/missing.ipynb', output_nb='/tmp/o.ipynb')\n",
    )
    task = _by_key(pipeline)["gone"]

    assert isinstance(task, PlaceholderActivity)
    assert "was not found" in task.comment


def test_dynamic_input_path_becomes_a_placeholder(tmp_path: Path) -> None:
    pipeline = _load_one(
        tmp_path,
        _IMPORTS + "import os\n"
        "with DAG(dag_id='dynamic') as dag:\n"
        "    PapermillOperator(task_id='dynamic', input_nb=os.environ['NB'], output_nb='/tmp/o.ipynb')\n",
    )

    assert isinstance(_by_key(pipeline)["dynamic"], PlaceholderActivity)


def test_unsupported_magic_becomes_a_placeholder(tmp_path: Path) -> None:
    _write_notebook(tmp_path / "spark_magic.ipynb", [_code("%%configure -f\n{'conf': {}}")])
    pipeline = _load_one(
        tmp_path,
        _IMPORTS + "with DAG(dag_id='magic') as dag:\n"
        "    PapermillOperator(task_id='magic', input_nb='spark_magic.ipynb', output_nb='o.ipynb')\n",
    )
    task = _by_key(pipeline)["magic"]

    assert isinstance(task, PlaceholderActivity)
    assert "%%configure" in task.comment


def test_non_python_kernel_becomes_a_placeholder(tmp_path: Path) -> None:
    _write_notebook(tmp_path / "r.ipynb", [_code("print(1)")])
    pipeline = _load_one(
        tmp_path,
        _IMPORTS + "with DAG(dag_id='r') as dag:\n"
        "    PapermillOperator(task_id='r', input_nb='r.ipynb', output_nb='o.ipynb', kernel_name='ir')\n",
    )

    assert isinstance(_by_key(pipeline)["r"], PlaceholderActivity)


def test_convert_notebook_handles_magics_pip_and_injected_cells() -> None:
    document = _document(
        [
            _code("%matplotlib inline\n!pip install tqdm\nimport tqdm"),
            _code("%%sql\nselect 1"),
            _code("%%time\nresult = 1 + 1"),
            _code("query = '''\n%s rows\n!important\n'''\nprint(query)"),
            _code("run_date = '2024-01-01'", tags=["injected-parameters"]),
        ]
    )

    converted = papermill.convert_notebook(document, task_id="t", notebook_name="n.ipynb", passed_parameters={})

    assert "# MAGIC %pip install tqdm" in converted.source
    assert "%matplotlib" not in converted.source
    assert "# MAGIC %sql\n# MAGIC select 1" in converted.source
    assert "%%time" not in converted.source and "result = 1 + 1" in converted.source
    assert "%s rows\n!important" in converted.source
    assert "2024-01-01" not in converted.source
    compile(converted.source, "notebook.py", "exec")


def test_parameters_without_a_parameters_cell_are_injected_at_the_top() -> None:
    document = _document([_code("print(region)")])

    converted = papermill.convert_notebook(
        document, task_id="t", notebook_name="n.ipynb", passed_parameters={"region": "eu"}
    )

    assert converted.parameter_names == ["region"]
    body = converted.source.split("# COMMAND ----------")
    assert "region = _papermill_parameter('region', '', as_text=True)" in body[0]
    assert "print(region)" in body[1]


def test_locate_notebook_prefers_the_longest_matching_folder_path(tmp_path: Path) -> None:
    for owner in ("alice", "bob"):
        _write_notebook(tmp_path / owner / "etl" / "daily.ipynb", [_code("print(1)")])

    found, _ = papermill.locate_notebook("/home/bob/etl/daily.ipynb", (), tmp_path)
    ambiguous, detail = papermill.locate_notebook("/home/carol/daily.ipynb", (), tmp_path)

    assert found == tmp_path / "bob" / "etl" / "daily.ipynb"
    assert ambiguous is None and "matches 2 files" in detail


def test_locate_notebook_rejects_remote_paths(tmp_path: Path) -> None:
    found, detail = papermill.locate_notebook("s3://bucket/n.ipynb", (), tmp_path)

    assert found is None and "remote s3://" in detail


def _load_one(root: Path, dag_source: str, *, dag_folder: str = ".") -> object:
    dag_path = root / dag_folder / "dag.py"
    dag_path.parent.mkdir(parents=True, exist_ok=True)
    dag_path.write_text(dag_source, encoding="utf-8")
    pipelines = load_airflow_dags(dag_path, source_root=root)
    assert len(pipelines) == 1
    return pipelines[0]


def _by_key(pipeline: object) -> dict[str, object]:
    return {task.task_key: task for task in pipeline.tasks}  # type: ignore[attr-defined]


def _disclosure(pipeline: object, code: str) -> dict[str, object]:
    transformations = pipeline.audit.get("transformations", [])  # type: ignore[attr-defined]
    return next(item for item in transformations if item.get("code") == code)


def _code(source: str, *, tags: list[str] | None = None) -> dict[str, object]:
    return {"cell_type": "code", "metadata": {"tags": tags or []}, "source": source, "outputs": []}


def _markdown(source: str) -> dict[str, object]:
    return {"cell_type": "markdown", "metadata": {}, "source": source}


def _document(cells: list[dict[str, object]]) -> dict[str, object]:
    return {
        "nbformat": 4,
        "nbformat_minor": 5,
        "metadata": {"kernelspec": {"name": "python3", "language": "python"}},
        "cells": cells,
    }


def _write_notebook(path: Path, cells: list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(_document(cells)), encoding="utf-8")
