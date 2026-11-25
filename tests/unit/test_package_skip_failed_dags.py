"""Tests for packaging the Airflow DAGs that passed reconciliation while leaving out the ones that failed."""

from __future__ import annotations

import json
from pathlib import Path

import pytest
import yaml

from flowx.bundler.dab_writer import main as package_main
from flowx.skipped_dags import without_skipped_dag_references
from flowx.sources.airflow.convert import main as convert_main
from flowx.sources.airflow.discover import main as discover_main

_GOOD = (
    "from datetime import datetime\n"
    "from airflow import DAG\n"
    "from airflow.operators.bash import BashOperator\n"
    "with DAG('good', start_date=datetime(2024, 1, 1), schedule='@daily', max_active_runs=1) as dag:\n"
    "    BashOperator(task_id='hi', bash_command='echo hi')\n"
)
_BAD = (
    "from airflow import DAG\n"
    "from airflow.operators.bash import BashOperator\n"
    "with DAG('bad') as dag:\n"
    "    tasks = [BashOperator(task_id=f't{i}', bash_command='echo hi') for i in range(2)]\n"
)
_CALLER = (
    "from datetime import datetime\n"
    "from airflow import DAG\n"
    "from airflow.operators.trigger_dagrun import TriggerDagRunOperator\n"
    "with DAG('caller', start_date=datetime(2024, 1, 1), schedule='@daily', max_active_runs=1) as dag:\n"
    "    TriggerDagRunOperator(task_id='kick', trigger_dag_id='bad')\n"
)


def _convert(tmp_path: Path, files: dict[str, str]) -> Path:
    dags = tmp_path / "dags"
    dags.mkdir()
    for name, content in files.items():
        (dags / name).write_text(content, encoding="utf-8")
    output = tmp_path / "out"
    discover_main(["--source-dir", str(dags), "--output-dir", str(output)])
    convert_main(["--source-dir", str(dags), "--output-dir", str(output)])
    return output


def _package(output: Path, *flags: str) -> int:
    return package_main(["--output-dir", str(output), "--no-download-workspace-files", "--keep-intermediates", *flags])


def _job(output: Path, name: str) -> dict:
    document = yaml.safe_load((output / "resources" / f"{name}.yml").read_text(encoding="utf-8"))
    return document["resources"]["jobs"][name]


def test_a_failed_dag_still_stops_package_by_default(tmp_path: Path) -> None:
    output = _convert(tmp_path, {"good.py": _GOOD, "bad.py": _BAD})

    assert _package(output) == 1
    assert not (output / "resources").exists()


def test_skip_failed_dags_packages_the_others(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    output = _convert(tmp_path, {"good.py": _GOOD, "bad.py": _BAD})

    assert _package(output, "--skip-failed-dags") == 0

    assert sorted(path.stem for path in (output / "resources").glob("*.yml")) == ["good"]
    assert _job(output, "good")["schedule"]["pause_status"] == "UNPAUSED"
    assert "left out 1 Airflow DAG(s) that failed reconciliation" in capsys.readouterr().err


def test_task_triggering_a_left_out_dag_becomes_a_placeholder_and_pauses_its_job(tmp_path: Path) -> None:
    output = _convert(tmp_path, {"good.py": _GOOD, "bad.py": _BAD, "caller.py": _CALLER})

    assert _package(output, "--skip-failed-dags", "--allow-unresolved-gaps") == 0

    job = _job(output, "caller")
    task = next(task for task in job["tasks"] if task["task_key"] == "kick")
    assert "run_job_task" not in task
    notebook = (output / task["notebook_task"]["notebook_path"].removeprefix("../")).read_text(encoding="utf-8")
    assert "Airflow DAG 'bad' failed reconciliation and was left out of this bundle" in notebook
    assert job["schedule"]["pause_status"] == "PAUSED"


def test_skip_failed_dags_exits_zero_when_nothing_failed(tmp_path: Path) -> None:
    output = _convert(tmp_path, {"good.py": _GOOD})

    assert _package(output, "--skip-failed-dags") == 0


def test_skip_failed_dags_does_not_hide_a_malformed_report(tmp_path: Path) -> None:
    output = tmp_path / "out"
    (output / ".work").mkdir(parents=True)
    (output / ".work" / "translation_report.json").write_text('{"pipelines": "not a list"}', encoding="utf-8")

    assert _package(output, "--skip-failed-dags") == 1


def test_only_failed_dags_leaves_nothing_to_package(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    output = _convert(tmp_path, {"bad.py": _BAD})

    assert _package(output, "--skip-failed-dags") == 1
    assert not (output / "resources").exists()
    assert "every Airflow DAG failed reconciliation" in capsys.readouterr().err


def test_setup_lists_the_left_out_dags(tmp_path: Path) -> None:
    output = _convert(tmp_path, {"good.py": _GOOD, "bad.py": _BAD})

    _package(output, "--skip-failed-dags")

    setup = (output / "SETUP.md").read_text(encoding="utf-8")
    assert "## Left-out DAGs" in setup
    assert "- `bad`" in setup


def test_run_now_task_with_a_left_out_dag_name_is_kept() -> None:
    pipeline = {
        "name": "caller",
        "tasks": [
            {"type": "RunJobActivity", "name": "bad", "task_key": "bad", "job_name": "bad", "existing_job_id": "123"}
        ],
    }

    assert without_skipped_dag_references(pipeline, frozenset({"bad"})) is pipeline


def test_mcp_package_returns_the_bundle_when_dags_were_left_out(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    pytest.importorskip("mcp")
    from flowx.mcp import runner, server

    output = _convert(tmp_path, {"good.py": _GOOD, "bad.py": _BAD})

    def run_package(arguments: list[str]) -> runner.AdapterResult:
        returncode = _package(output, *[str(argument) for argument in arguments[3:]])
        return runner.AdapterResult(command=arguments, returncode=returncode, stdout="", stderr="")

    monkeypatch.setattr(runner, "run_adapter", run_package)

    result = server._cmd_package({"output_dir": str(output), "skip_failed_dags": True})

    assert result["ok"] is True
    assert result["status"] == "packaged_without_failed_dags"
    assert result["left_out_dags"] == ["bad"]
    assert "bundle" in result


_FACTORY = (
    "from datetime import datetime\n"
    "from airflow import DAG\n"
    "for name in ['a', 'b']:\n"
    "    globals()[name] = DAG(name, start_date=datetime(2024, 1, 1), schedule=None)\n"
)
_GENERATED_CALLER = _CALLER.replace("trigger_dag_id='bad'", "trigger_dag_id='a'")


def test_trigger_of_a_dag_a_left_out_generator_may_build_becomes_a_placeholder(tmp_path: Path) -> None:
    output = _convert(tmp_path, {"good.py": _GOOD, "factory.py": _FACTORY, "caller.py": _GENERATED_CALLER})

    assert _package(output, "--skip-failed-dags", "--allow-unresolved-gaps") == 0

    job = _job(output, "caller")
    assert all("run_job_task" not in task for task in job["tasks"])
    assert job["schedule"]["pause_status"] == "PAUSED"
    setup = (output / "SETUP.md").read_text(encoding="utf-8")
    assert setup.count("- `factory`") == 1


def test_replaced_trigger_is_recorded_like_an_excluded_dag_reference() -> None:
    pipeline = {
        "name": "caller",
        "reconciliation_status": "verified_with_reviewed_resolutions",
        "schedule": {"kind": "schedule", "pause_status": "UNPAUSED"},
        "audit": {"deterministic_count": 2, "agentic_count": 0, "transformations": []},
        "tasks": [{"type": "RunJobActivity", "name": "kick", "task_key": "kick", "job_name": "bad"}],
    }

    updated = without_skipped_dag_references(pipeline, frozenset({"bad"}))

    assert updated["reconciliation_status"] == "verified_with_gaps"
    assert updated["audit"]["deterministic_count"] == 1
    assert updated["audit"]["agentic_count"] == 1
    assert [item["code"] for item in updated["audit"]["transformations"]] == [
        "schedule_paused_until_migration_completes"
    ]


def test_trigger_of_a_left_out_dag_needs_acknowledgment_like_an_excluded_one(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    output = _convert(tmp_path, {"good.py": _GOOD, "bad.py": _BAD, "caller.py": _CALLER})

    assert _package(output, "--skip-failed-dags") == 1
    assert not (output / "resources").exists()
    assert "--allow-unresolved-gaps" in capsys.readouterr().err


def test_trigger_placeholders_of_left_out_dags_count_as_unreviewed(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    output = _convert(tmp_path, {"good.py": _GOOD, "bad.py": _BAD, "caller.py": _CALLER})
    capsys.readouterr()

    assert _package(output, "--skip-failed-dags", "--allow-unresolved-gaps") == 0

    line = next(line for line in capsys.readouterr().out.splitlines() if line.startswith("Airflow gap review: "))
    assert json.loads(line.removeprefix("Airflow gap review: "))["unreviewed_count"] == 1
