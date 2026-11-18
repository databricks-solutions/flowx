"""Tests for files that build Airflow DAGs at import time in ways flowx cannot follow."""

from __future__ import annotations

from pathlib import Path

import pytest

from flowx.sources.airflow.loader import discover_dags, load_pipelines


@pytest.mark.parametrize(
    "source",
    [
        "import dagfactory\n"
        "dag_factory = dagfactory.DagFactory('/opt/airflow/dags/config.yml')\n"
        "dag_factory.generate_dags(globals())\n",
        "from dagfactory import load_yaml_dags\nload_yaml_dags(globals_dict=globals())\n",
        "from gusty import create_dag\ndag = create_dag('/opt/airflow/dags/jobs')\n",
        "from configs import build_all\nfor name, built in build_all().items():\n    globals()[name] = built\n",
        "from configs import build_all\nglobals().update(build_all())\n",
        "from configs import register\nregister(globals())\n",
    ],
)
def test_generated_dag_file_is_reported_as_a_failed_source(tmp_path: Path, source: str) -> None:
    (tmp_path / "generated.py").write_text(source, encoding="utf-8")

    pipelines = load_pipelines(tmp_path)

    assert [pipeline.name for pipeline in pipelines] == ["generated"]
    assert pipelines[0].reconciliation_status == "failed"
    assert pipelines[0].not_translatable[0]["code"] == "unsupported_dag_generator"


def test_helper_module_without_dags_is_not_reported(tmp_path: Path) -> None:
    (tmp_path / "helpers.py").write_text(
        "from airflow.models import BaseOperator\n\nclass CopyOperator(BaseOperator):\n    pass\n",
        encoding="utf-8",
    )

    assert discover_dags(tmp_path) == []


def test_factory_registered_through_globals_still_expands(tmp_path: Path) -> None:
    (tmp_path / "dags.py").write_text(
        "from airflow import DAG\n"
        "from airflow.operators.bash import BashOperator\n"
        "def create_dag(name):\n"
        "    with DAG(dag_id=name) as dag:\n"
        "        BashOperator(task_id='t', bash_command='echo hi')\n"
        "    return dag\n"
        "for name in ['a', 'b']:\n"
        "    globals()[f'dag_{name}'] = create_dag(name)\n",
        encoding="utf-8",
    )

    pipelines = load_pipelines(tmp_path)

    assert sorted(pipeline.name for pipeline in pipelines) == ["a", "b"]
    assert {pipeline.reconciliation_status for pipeline in pipelines} == {"verified"}


def test_helper_that_only_reads_globals_is_not_reported(tmp_path: Path) -> None:
    (tmp_path / "registry.py").write_text("def lookup(name):\n    return globals().get(name)\n", encoding="utf-8")

    assert discover_dags(tmp_path) == []


def test_import_only_helper_is_not_reported(tmp_path: Path) -> None:
    (tmp_path / "helper.py").write_text("import dagfactory\n", encoding="utf-8")

    assert discover_dags(tmp_path) == []


@pytest.mark.parametrize(
    "generation",
    [
        "from configs import build_all\nglobals().update(build_all())\n",
        "import dagfactory\ndagfactory.DagFactory('/opt/airflow/dags/config.yml').generate_dags(globals())\n",
    ],
)
def test_generated_dags_next_to_a_static_dag_are_reported(tmp_path: Path, generation: str) -> None:
    (tmp_path / "mixed.py").write_text(
        "from airflow import DAG\n"
        "from airflow.operators.bash import BashOperator\n"
        "with DAG(dag_id='static') as dag:\n"
        "    BashOperator(task_id='t', bash_command='echo hi')\n" + generation,
        encoding="utf-8",
    )

    pipelines = load_pipelines(tmp_path)

    assert [(pipeline.name, pipeline.reconciliation_status) for pipeline in pipelines] == [
        ("static", "verified"),
        ("mixed__generated_dags", "failed"),
    ]
