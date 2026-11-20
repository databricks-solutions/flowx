"""Tests for attaching a DAG's imported project code (custom operators, callables, shared defaults) to its gaps."""

from __future__ import annotations

from pathlib import Path

from flowx.models.ir import Pipeline, PlaceholderActivity
from flowx.sources.airflow.loader import load_pipelines

_DAG = (
    "from airflow import DAG\n"
    "from airflow.operators.python import PythonOperator\n"
    "from libs.operators import CopyOperator\n"
    "from libs.queries import LOAD_QUERY\n"
    "from scripts.checks import run_checks\n"
    "{imports}"
    "with DAG(dag_id='d'{dag_arguments}) as dag:\n"
    "    copy = CopyOperator(task_id='copy', query=LOAD_QUERY)\n"
    "    check = PythonOperator(task_id='check', python_callable=run_checks)\n"
    "    copy >> check\n"
)


def _write(root: Path, files: dict[str, str]) -> None:
    for name, content in files.items():
        path = root / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(content, encoding="utf-8")


def _load(tmp_path: Path, files: dict[str, str], *, imports: str = "", dag_arguments: str = "") -> Pipeline:
    _write(tmp_path, {"dags/team/dag.py": _DAG.format(imports=imports, dag_arguments=dag_arguments), **files})
    return load_pipelines(tmp_path / "dags")[0]


def _sources(pipeline: Pipeline, task_key: str) -> dict[str, dict]:
    task = next(task for task in pipeline.tasks if task.task_key == task_key)
    assert isinstance(task, PlaceholderActivity)
    return {record["name"]: record for record in task.raw_definition.get("imported_sources", [])}


_PROJECT = {
    "dags/libs/__init__.py": "",
    "dags/libs/operators.py": (
        "from airflow.models import BaseOperator\n"
        "\n"
        "class CopyOperator(BaseOperator):\n"
        "    def execute(self, context):\n"
        "        return 1\n"
        "\n"
        "class Other:\n"
        "    pass\n"
    ),
    "dags/libs/queries.py": "LOAD_QUERY = 'SELECT 1'\nOTHER = 'SELECT 2'\n",
    "dags/scripts/checks.py": "def run_checks():\n    print('ok')\n",
}


def test_custom_operator_gap_carries_the_operator_class_and_the_query_it_imports(tmp_path: Path) -> None:
    sources = _sources(_load(tmp_path, _PROJECT), "copy")

    operator = sources["CopyOperator"]
    assert operator["import"] == "from libs.operators import CopyOperator"
    assert operator["resolved_path"] == str(tmp_path / "dags" / "libs" / "operators.py")
    assert operator["content"].startswith("class CopyOperator(BaseOperator):")
    assert "class Other" not in operator["content"]
    assert sources["LOAD_QUERY"]["content"] == "LOAD_QUERY = 'SELECT 1'"


def test_imported_callable_gap_carries_the_function(tmp_path: Path) -> None:
    sources = _sources(_load(tmp_path, _PROJECT), "check")

    assert sources["run_checks"]["content"] == "def run_checks():\n    print('ok')"


def test_missing_project_module_is_recorded_as_unresolved(tmp_path: Path) -> None:
    sources = _sources(_load(tmp_path, {}), "copy")

    assert "was not found under" in sources["CopyOperator"]["unresolved"]
    assert "content" not in sources["CopyOperator"]


def test_airflow_and_standard_library_imports_are_not_listed(tmp_path: Path) -> None:
    sources = _sources(_load(tmp_path, _PROJECT), "check")

    assert set(sources) == {"run_checks"}


def test_code_outside_the_discover_folder_is_not_read(tmp_path: Path) -> None:
    _write(tmp_path, {"libs/operators.py": "class CopyOperator:\n    pass\n"})

    sources = _sources(_load(tmp_path, {}), "copy")

    assert "content" not in sources["CopyOperator"]


def test_symlink_leading_outside_the_project_is_not_read(tmp_path: Path) -> None:
    (tmp_path / "secret.py").write_text("def run_checks():\n    pass\n", encoding="utf-8")
    (tmp_path / "dags" / "scripts").mkdir(parents=True)
    (tmp_path / "dags" / "scripts" / "checks.py").symlink_to(tmp_path / "secret.py")

    sources = _sources(_load(tmp_path, {}), "check")

    assert "symbolic link that leads outside" in sources["run_checks"]["unresolved"]


def test_unreadable_shared_default_args_gap_carries_its_definition(tmp_path: Path) -> None:
    pipeline = _load(
        tmp_path,
        {**_PROJECT, "dags/libs/defaults.py": "default_args = {'retries': 2}\n"},
        imports="from libs.defaults import default_args\n",
        dag_arguments=", default_args=default_args",
    )

    finding = next(
        item
        for item in pipeline.not_translatable
        if item["code"] == "unsupported_dag_setting" and item["details"]["name"] == "default_args"
    )
    assert finding["details"]["imported_sources"][0]["content"] == "default_args = {'retries': 2}"


def test_relative_import_resolves_from_the_dag_folder(tmp_path: Path) -> None:
    _write(
        tmp_path,
        {
            "dags/team/dag.py": (
                "from airflow import DAG\n"
                "from .helpers import CopyOperator\n"
                "with DAG(dag_id='d') as dag:\n"
                "    copy = CopyOperator(task_id='copy')\n"
            ),
            "dags/team/helpers.py": "class CopyOperator:\n    pass\n",
        },
    )

    sources = _sources(load_pipelines(tmp_path / "dags")[0], "copy")

    assert sources["CopyOperator"]["content"] == "class CopyOperator:\n    pass"


def test_name_re_exported_by_a_package_is_followed(tmp_path: Path) -> None:
    _write(
        tmp_path,
        {
            "dags/team/dag.py": (
                "from airflow import DAG\n"
                "from libs import CopyOperator\n"
                "with DAG(dag_id='d') as dag:\n"
                "    copy = CopyOperator(task_id='copy')\n"
            ),
            "dags/libs/__init__.py": "from .custom import CopyOperator\n",
            "dags/libs/custom.py": "class CopyOperator:\n    pass\n",
        },
    )

    sources = _sources(load_pipelines(tmp_path / "dags")[0], "copy")

    assert sources["CopyOperator"]["resolved_path"] == str(tmp_path / "dags" / "libs" / "custom.py")
    assert sources["CopyOperator"]["content"] == "class CopyOperator:\n    pass"


def test_module_file_too_large_to_read_is_recorded(tmp_path: Path) -> None:
    sources = _sources(
        _load(tmp_path, {**_PROJECT, "dags/libs/operators.py": "class CopyOperator:\n    pass\n" + "#" * 1_100_000}),
        "copy",
    )

    assert sources["CopyOperator"]["unresolved"] == "the module file is too large to read"
