"""Tests for the fingerprint-bound Airflow agentic resolution workflow."""

from __future__ import annotations

import hashlib
import json
import shutil
from pathlib import Path

import pytest

import flowx.agentic as agentic_contract
from flowx.adapter.__main__ import main as adapter_main
from flowx.agentic import AgenticContractError, _validate_candidate, summarize_persisted_agentic_resolutions
from flowx.bundler.dab_writer import main as package_main
from flowx.reporting.coverage import build_coverage_rows
from flowx.sources.airflow.convert import main as airflow_convert
from flowx.sources.airflow.loader import load_airflow_dag


def _write_source(tmp_path: Path, *, two_tasks: bool = False) -> Path:
    source = tmp_path / "dag.py"
    second = (
        "    second = KubernetesPodOperator(task_id='second', image='python:3.12')\n    pod >> second\n"
        if two_tasks
        else ""
    )
    source.write_text(
        "from airflow import DAG\n"
        "with DAG(dag_id='agentic') as dag:\n"
        "    pod = KubernetesPodOperator(task_id='pod', image='python:3.11', retries=2)\n"
        f"{second}",
        encoding="utf-8",
    )
    return source


def _prepare(tmp_path: Path, *, two_tasks: bool = False) -> tuple[Path, Path, dict]:
    source = _write_source(tmp_path, two_tasks=two_tasks)
    output = tmp_path / "output"
    assert airflow_convert(["--source-dir", str(source), "--output-dir", str(output)]) == 0
    report = output / ".work" / "translation_report.json"
    original = report.read_bytes()

    assert (
        adapter_main(
            [
                "resolve-agentic",
                "prepare",
                "--source",
                "airflow",
                "--source-path",
                str(source),
                "--report",
                str(report),
                "--output-dir",
                str(output),
            ]
        )
        == 0
    )
    assert report.read_bytes() == original
    gaps = json.loads((output / ".work" / "agentic" / "gaps.json").read_text(encoding="utf-8"))
    return source, output, gaps


def _prepare_source(source: Path, output: Path) -> list[dict]:
    assert airflow_convert(["--source-dir", str(source), "--output-dir", str(output)]) == 0
    report = output / ".work" / "translation_report.json"
    assert (
        adapter_main(
            [
                "resolve-agentic",
                "prepare",
                "--source",
                "airflow",
                "--source-path",
                str(source),
                "--report",
                str(report),
                "--output-dir",
                str(output),
            ]
        )
        == 0
    )
    return json.loads((output / ".work" / "agentic" / "gaps.json").read_text(encoding="utf-8"))


def _candidate(gap: dict, *, source: str = "print('Migrated from Airflow')\n", status: str = "resolved") -> dict:
    if not source.startswith("# Databricks notebook source\n"):
        source = "# Databricks notebook source\n" + source
    generated_file = {
        "path": "task.py",
        "language": "python",
        "content": source,
        "sha256": hashlib.sha256(source.encode("utf-8")).hexdigest(),
    }
    dispositions = [
        {
            "name": argument["name"],
            "disposition": "preserved_by_flowx" if argument["preserved_by_flowx"] else "consumed",
            "rationale": "Flowx preserves task policy." if argument["preserved_by_flowx"] else "Used by notebook code.",
        }
        for argument in gap["arguments"]
    ]
    candidate = {
        "contract_version": "1",
        "gap_id": gap["gap_id"],
        "status": status,
        "baseline_report_sha256": gap["baseline_report_sha256"],
        "source_sha256": gap["source_sha256"],
        "provider": {
            "name": "airflow-to-dabs",
            "version": "0.2.0",
            "repository": "https://github.com/park-peter/airflow-to-dabs",
        },
        "model": {"name": "test-model"},
        "argument_disposition": dispositions,
        "prerequisites": [],
        "warnings": [],
        "semantic_deltas": [],
    }
    if status == "resolved":
        candidate["replacement"] = {"kind": "notebook", "file": "task.py"}
        candidate["generated_files"] = [generated_file]
    else:
        candidate["reason"] = "More deployment information is required."
    return candidate


def _stage(output: Path, candidate: dict, *, name: str = "candidate.json", replace: bool = False) -> int:
    candidate_path = output / name
    candidate_path.write_text(json.dumps(candidate, indent=2), encoding="utf-8")
    args = [
        "resolve-agentic",
        "stage",
        "--source",
        "airflow",
        "--output-dir",
        str(output),
        "--candidate",
        str(candidate_path),
    ]
    if replace:
        args.append("--replace")
    return adapter_main(args)


def _review_manifest(output: Path) -> Path:
    index = json.loads((output / ".work" / "agentic" / "candidate_index.json").read_text(encoding="utf-8"))
    expected = [
        {"gap_id": gap_id, "sha256": entry["sha256"], "status": entry["status"]}
        for gap_id, entry in sorted(index.items())
    ]
    matches = []
    for path in (output / ".work" / "agentic" / "review_manifests").glob("*.json"):
        if json.loads(path.read_text(encoding="utf-8"))["candidates"] == expected:
            matches.append(path)
    assert len(matches) == 1
    return matches[0]


def _load_tasks(report: Path) -> dict[str, dict]:
    payload = json.loads(report.read_text(encoding="utf-8"))
    pipeline = payload["pipelines"][0] if "pipelines" in payload else payload
    return {task["task_key"]: task for task in pipeline["tasks"]}


def test_prepare_writes_versioned_fingerprint_bound_gap_without_changing_report(tmp_path: Path):
    source, output, gaps = _prepare(tmp_path)

    assert len(gaps) == 1
    gap = gaps[0]
    assert gap["contract_version"] == "1"
    assert gap["gap_id"]
    assert gap["pipeline_name"] == "agentic"
    assert gap["task_key"] == "pod"
    assert gap["operator"] == "KubernetesPodOperator"
    assert gap["source_sha256"] == hashlib.sha256(source.read_bytes()).hexdigest()
    assert {argument["name"] for argument in gap["arguments"]} == {"task_id", "image", "retries"}
    assert (output / ".work" / "agentic" / "baseline.json").exists()
    assert (output / ".work" / "agentic" / "source" / "dag.py").read_bytes() == source.read_bytes()


def test_gap_fingerprints_use_each_placeholder_own_source_span(tmp_path: Path) -> None:
    source = tmp_path / "interleaved.py"
    source.write_text(
        "from airflow import DAG\n"
        "from airflow.operators.bash import BashOperator\n"
        "from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator\n"
        "\n"
        "\n"
        "\n"
        "\n"
        "\n"
        "\n"
        "with DAG(dag_id='interleaved') as dag:\n"
        "    a_ok = BashOperator(task_id='a_ok', bash_command='echo a')\n"
        "    b_ok = BashOperator(task_id='b_ok', bash_command='echo b')\n"
        "    c_gap = KubernetesPodOperator(task_id='c_gap', image='python:3.12')\n"
        "    d_ok = BashOperator(task_id='d_ok', bash_command='echo d')\n"
        "    e_gap = KubernetesPodOperator(task_id='e_gap', image='python:3.12')\n",
        encoding="utf-8",
    )

    pipeline = load_airflow_dag(source)
    findings = {
        item["details"]["source_task_id"]: item
        for item in pipeline.not_translatable
        if item["code"] == "operator_placeholder"
    }

    assert {task_id: finding["line"] for task_id, finding in findings.items()} == {"c_gap": 13, "e_gap": 15}
    assert findings["c_gap"]["fingerprint"] != findings["e_gap"]["fingerprint"]


@pytest.mark.parametrize(
    ("source_text", "expected"),
    [
        (
            "from airflow import DAG\n"
            "with DAG(dag_id='collision') as dag:\n"
            "    first = KubernetesPodOperator(task_id='load.data', image='python:3.12')\n"
            "    second = KubernetesPodOperator(task_id='load_data', image='python:3.12')\n",
            [("load_data", "load.data", 3), ("load_data__2", "load_data", 4)],
        ),
        (
            "from airflow import DAG\n"
            "with DAG(dag_id='mapped') as dag:\n"
            "    pod = KubernetesPodOperator.partial(task_id='pod', image='python:3.12').expand(env=['a'])\n",
            [("pod_iteration", "pod", 3)],
        ),
        (
            "from airflow import DAG\n"
            "with DAG(dag_id='bare') as dag:\n"
            "    KubernetesPodOperator(task_id='bare_pod', image='python:3.12')\n",
            [("bare_pod", "bare_pod", 3)],
        ),
        (
            "from airflow import DAG\n"
            "def make(task_id):\n"
            "    return KubernetesPodOperator(task_id=task_id, image='python:3.12')\n"
            "with DAG(dag_id='helper') as dag:\n"
            "    pod = make('helper_pod')\n",
            [("helper_pod", "helper_pod", 5)],
        ),
        (
            "from airflow.decorators import dag, task\n"
            "@task.branch\n"
            "def choose():\n"
            "    return 'next'\n"
            "@dag(dag_id='taskflow')\n"
            "def workflow():\n"
            "    choose()\n"
            "workflow()\n",
            [("choose", "choose", 7)],
        ),
        (
            "from airflow.decorators import dag, task_group\n"
            "@task_group\n"
            "def grouped():\n"
            "    pass\n"
            "@dag(dag_id='task_group')\n"
            "def workflow():\n"
            "    grouped()\n"
            "workflow()\n",
            [("grouped_tg1", "grouped", 7)],
        ),
    ],
    ids=("collision-safe-keys", "classic-mapped-inner", "bare-operator", "helper-factory", "taskflow", "task-group"),
)
def test_placeholder_findings_bind_capture_identity_to_source(
    tmp_path: Path,
    source_text: str,
    expected: list[tuple[str, str, int]],
) -> None:
    source = tmp_path / "dag.py"
    source.write_text(source_text, encoding="utf-8")

    pipeline = load_airflow_dag(source)
    findings = [item for item in pipeline.not_translatable if item["code"] == "operator_placeholder"]

    assert [
        (item["details"]["task_key"], item["details"]["source_task_id"], item["line"]) for item in findings
    ] == expected
    assert all(item["details"]["capture_id"] for item in findings)


def test_source_expanded_placeholders_have_unique_gap_fingerprints(tmp_path: Path) -> None:
    source = tmp_path / "loop.py"
    source.write_text(
        "from airflow import DAG\n"
        "with DAG(dag_id='loop') as dag:\n"
        "    for index in range(2):\n"
        "        KubernetesPodOperator(task_id=f'pod_{index}', image='python:3.12')\n",
        encoding="utf-8",
    )

    pipeline = load_airflow_dag(source)
    findings = [item for item in pipeline.not_translatable if item["code"] == "operator_placeholder"]
    gaps = _prepare_source(source, tmp_path / "output")

    assert [item["details"]["source_task_id"] for item in findings] == ["pod_0", "pod_1"]
    assert len({item["fingerprint"] for item in findings}) == 2
    assert len({gap["gap_id"] for gap in gaps}) == 2


def test_gap_fingerprint_is_stable_when_an_unrelated_source_gap_changes_task_path(tmp_path: Path) -> None:
    source = tmp_path / "stable.py"
    source.write_text(
        "from airflow import DAG\n"
        "with DAG(dag_id='stable') as dag:\n"
        "    KubernetesPodOperator(task_id='pod', image='python:3.12')\n",
        encoding="utf-8",
    )
    original = next(
        item for item in load_airflow_dag(source).not_translatable if item["code"] == "operator_placeholder"
    )

    source.write_text(
        "from airflow import DAG\n"
        "with DAG(dag_id='stable', max_active_runs=1) as dag:\n"
        "    KubernetesPodOperator(task_id='pod', image='python:3.12')\n",
        encoding="utf-8",
    )
    changed = next(item for item in load_airflow_dag(source).not_translatable if item["code"] == "operator_placeholder")

    assert original["details"]["task_path"] == ["tasks", 0]
    assert changed["details"]["task_path"] == ["tasks", 1]
    assert changed["fingerprint"] == original["fingerprint"]


def test_nested_and_top_level_task_key_collision_keeps_gap_identity_distinct(tmp_path: Path) -> None:
    source = tmp_path / "nested_collision.py"
    source.write_text(
        "from airflow import DAG\n"
        "with DAG(dag_id='nested_collision') as dag:\n"
        "    top = KubernetesPodOperator(task_id='pod_iteration', image='python:3.12')\n"
        "    mapped = KubernetesPodOperator.partial(task_id='pod', image='python:3.12').expand(env=['prod'])\n",
        encoding="utf-8",
    )

    pipeline = load_airflow_dag(source)
    findings = [item for item in pipeline.not_translatable if item["code"] == "operator_placeholder"]
    gaps = _prepare_source(source, tmp_path / "output")

    assert [item["details"]["source_task_id"] for item in findings] == ["pod_iteration", "pod"]
    assert len({item["fingerprint"] for item in findings}) == 2
    assert len({tuple(gap["task_path"]) for gap in gaps}) == 2
    assert {gap["capture_identity"] for gap in gaps} == {"top", "mapped"}


def test_gap_envelope_carries_bound_helper_arguments(tmp_path: Path) -> None:
    source = tmp_path / "helper.py"
    source.write_text(
        "from airflow import DAG\n"
        "def make(task_id, image):\n"
        "    return KubernetesPodOperator(task_id=task_id, image=image)\n"
        "with DAG(dag_id='helper') as dag:\n"
        "    pod = make('helper_pod', 'python:3.12')\n",
        encoding="utf-8",
    )

    gap = _prepare_source(source, tmp_path / "output")[0]
    arguments = {item["name"]: item for item in gap["arguments"]}

    assert arguments["task_id"]["source_expression"] == "'helper_pod'"
    assert arguments["image"]["source_expression"] == "'python:3.12'"


def test_gap_envelope_carries_statically_bound_operator_arguments(tmp_path: Path) -> None:
    source = tmp_path / "constants.py"
    source.write_text(
        "from airflow import DAG\n"
        "IMAGE = 'python:3.12'\n"
        "with DAG(dag_id='constants') as dag:\n"
        "    pod = KubernetesPodOperator(task_id='pod', image=IMAGE)\n",
        encoding="utf-8",
    )

    gap = _prepare_source(source, tmp_path / "output")[0]
    arguments = {item["name"]: item for item in gap["arguments"]}

    assert arguments["image"]["source_expression"] == "'python:3.12'"
    assert "image=IMAGE" in gap["raw_definition"]["source"]


def test_taskflow_gap_arguments_come_from_invocation_not_callable_body(tmp_path: Path) -> None:
    source = tmp_path / "taskflow.py"
    source.write_text(
        "from airflow.decorators import dag, task\n"
        "@task.branch\n"
        "def choose(value):\n"
        "    print(value)\n"
        "    return 'next'\n"
        "@dag(dag_id='taskflow')\n"
        "def workflow():\n"
        "    choose('selected')\n"
        "workflow()\n",
        encoding="utf-8",
    )

    gap = _prepare_source(source, tmp_path / "output")[0]

    assert gap["arguments"] == [{"name": "$arg0", "source_expression": "'selected'", "preserved_by_flowx": False}]


def test_only_actually_preserved_policy_arguments_are_flowx_owned(tmp_path: Path) -> None:
    source = tmp_path / "policy.py"
    source.write_text(
        "from airflow import DAG\n"
        "from airflow.operators.bash import BashOperator\n"
        "with DAG(dag_id='policy') as dag:\n"
        "    BashOperator(task_id='work', bash_command='echo hi', retries=2, pool='critical', trigger_rule='always')\n",
        encoding="utf-8",
    )

    gap = _prepare_source(source, tmp_path / "output")[0]
    arguments = {item["name"]: item["preserved_by_flowx"] for item in gap["arguments"]}

    assert arguments == {
        "task_id": True,
        "bash_command": False,
        "retries": True,
        "pool": False,
        "trigger_rule": False,
    }


def test_classic_mapped_placeholder_preserves_retry_policy_claimed_by_flowx(tmp_path: Path) -> None:
    source = tmp_path / "mapped_policy.py"
    source.write_text(
        "from airflow import DAG\n"
        "with DAG(dag_id='mapped_policy') as dag:\n"
        "    pod = KubernetesPodOperator.partial(task_id='pod', retries=2).expand(image=['a', 'b'])\n",
        encoding="utf-8",
    )

    pipeline = load_airflow_dag(source)
    inner = pipeline.tasks[0].inner_activities[0]
    gap = _prepare_source(source, tmp_path / "output")[0]
    arguments = {item["name"]: item for item in gap["arguments"]}

    assert inner.max_retries == 2
    assert arguments["retries"]["preserved_by_flowx"] is True


def test_unlowered_dynamic_retry_policy_is_not_claimed_as_preserved(tmp_path: Path) -> None:
    source = tmp_path / "dynamic_policy.py"
    source.write_text(
        "from airflow import DAG\n"
        "with DAG(dag_id='dynamic_policy') as dag:\n"
        "    pod = KubernetesPodOperator(task_id='pod', image='python:3.12', retries=get_retries())\n",
        encoding="utf-8",
    )

    gap = _prepare_source(source, tmp_path / "output")[0]
    arguments = {item["name"]: item for item in gap["arguments"]}

    assert arguments["retries"]["preserved_by_flowx"] is False


def test_resolve_agentic_is_explicitly_airflow_only(tmp_path: Path, capsys):
    exit_code = adapter_main(
        [
            "resolve-agentic",
            "prepare",
            "--source",
            "adf",
            "--source-path",
            str(tmp_path),
            "--report",
            str(tmp_path / "report.json"),
            "--output-dir",
            str(tmp_path / "output"),
        ]
    )

    assert exit_code == 2
    assert "not enabled for ADF; ADF uses the legacy merge path" in capsys.readouterr().err


def test_stage_rejects_graph_identity_fields(tmp_path: Path, capsys):
    _, output, gaps = _prepare(tmp_path)
    candidate = _candidate(gaps[0])
    candidate["replacement"]["task_key"] = "HIJACKED"

    assert _stage(output, candidate) == 1
    assert "replacement contains unsupported fields" in capsys.readouterr().err
    assert not list((output / ".work" / "agentic" / "candidates").glob("*.json"))


def test_stage_rejects_airflow_import_but_allows_airflow_in_comments(tmp_path: Path, capsys):
    _, output, gaps = _prepare(tmp_path)
    bad = _candidate(gaps[0], source="# Airflow provenance\nfrom airflow import DAG\n")

    assert _stage(output, bad) == 1
    assert "must not import Airflow" in capsys.readouterr().err

    good = _candidate(gaps[0], source="# Migrated from Airflow\nprint('ok')\n")
    assert _stage(output, good) == 0


def test_stage_requires_complete_argument_disposition_and_ignored_rationale(tmp_path: Path, capsys):
    _, output, gaps = _prepare(tmp_path)
    missing = _candidate(gaps[0])
    missing["argument_disposition"].pop()

    assert _stage(output, missing) == 1
    assert "argument_disposition must cover every source argument" in capsys.readouterr().err

    ignored = _candidate(gaps[0])
    ignored["argument_disposition"][1] = {
        "name": ignored["argument_disposition"][1]["name"],
        "disposition": "ignored",
        "rationale": "",
    }
    assert _stage(output, ignored) == 1
    assert "ignored argument requires a rationale" in capsys.readouterr().err


def test_stage_rejects_unresolved_jinja_provider_drift_and_file_hash_mismatch(tmp_path: Path, capsys):
    _, output, gaps = _prepare(tmp_path)

    unresolved = _candidate(gaps[0], source="print('{{ ds }}')\n")
    assert _stage(output, unresolved) == 1
    assert "unresolved Airflow Jinja" in capsys.readouterr().err

    wrong_provider = _candidate(gaps[0])
    wrong_provider["provider"]["version"] = "0.1.0"
    assert _stage(output, wrong_provider) == 1
    assert "pinned airflow-to-dabs v0.2.0" in capsys.readouterr().err

    bad_hash = _candidate(gaps[0])
    bad_hash["generated_files"][0]["sha256"] = "0" * 64
    assert _stage(output, bad_hash) == 1
    assert "sha256 does not match" in capsys.readouterr().err


def test_stage_rejects_python_script_without_databricks_notebook_marker(tmp_path: Path, capsys) -> None:
    _, output, gaps = _prepare(tmp_path)
    candidate = _candidate(gaps[0], source="print('plain script')\n")
    content = "print('plain script')\n"
    candidate["generated_files"][0]["content"] = content
    candidate["generated_files"][0]["sha256"] = hashlib.sha256(content.encode("utf-8")).hexdigest()

    assert _stage(output, candidate) == 1
    assert "Databricks notebook source marker" in capsys.readouterr().err


def test_stage_rejects_duplicate_candidates_for_one_gap(tmp_path: Path, capsys) -> None:
    _, output, gaps = _prepare(tmp_path)
    first = tmp_path / "first.json"
    second = tmp_path / "second.json"
    first.write_text(json.dumps(_candidate(gaps[0], source="print('first')\n")), encoding="utf-8")
    second.write_text(json.dumps(_candidate(gaps[0], source="print('second')\n")), encoding="utf-8")

    exit_code = adapter_main(
        [
            "resolve-agentic",
            "stage",
            "--source",
            "airflow",
            "--output-dir",
            str(output),
            "--candidate",
            str(first),
            "--candidate",
            str(second),
        ]
    )

    assert exit_code == 1
    assert "duplicate candidate" in capsys.readouterr().err.lower()
    assert not list((output / ".work" / "agentic" / "candidates").iterdir())


def test_stage_requires_dynamic_references_in_task_parameters_not_source_files(tmp_path: Path, capsys) -> None:
    _, output, gaps = _prepare(tmp_path)
    source = "print('{{job.parameters.env}}')\n"
    candidate = _candidate(gaps[0], source=source)
    candidate["replacement"]["base_parameters"] = {"env": "{{job.parameters.env}}"}

    assert _stage(output, candidate) == 1
    assert "cannot contain Databricks dynamic references" in capsys.readouterr().err

    valid = _candidate(gaps[0], source="print(dbutils.widgets.get('env'))\n")
    valid["replacement"]["base_parameters"] = {"env": "{{job.parameters.env}}"}
    assert _stage(output, valid) == 0


def test_stage_does_not_misclassify_airflow_input_names_as_dynamic_references(tmp_path: Path, capsys) -> None:
    _, output, gaps = _prepare(tmp_path)
    candidate = _candidate(gaps[0])
    candidate["replacement"]["base_parameters"] = {"path": "{{ input_file }}"}

    assert _stage(output, candidate) == 1
    assert "unresolved Airflow Jinja" in capsys.readouterr().err


def test_pinned_v020_provider_fixtures_satisfy_the_flowx_contract() -> None:
    root = Path(__file__).parents[2] / "skills" / "flowx-resolve-airflow-gaps" / "references" / "airflow-to-dabs-v0.2.0"
    provider = json.loads((root / "provider.json").read_text(encoding="utf-8"))

    assert provider["provider"]["version"] == "0.2.0"
    for outcome in ("notebook", "sql", "needs-input", "deferred"):
        gap = json.loads((root / "fixtures" / f"gap-{outcome}.json").read_text(encoding="utf-8"))
        candidate = json.loads((root / "fixtures" / f"resolution-{outcome}.json").read_text(encoding="utf-8"))
        manifest = {"baseline_report_sha256": gap["baseline_report_sha256"]}

        resolution = _validate_candidate(candidate, gap_by_id={gap["gap_id"]: gap}, manifest=manifest)

        assert resolution.gap["gap_id"] == gap["gap_id"]


def test_apply_rebuilds_from_baseline_and_preserves_graph_policy(tmp_path: Path):
    _, output, gaps = _prepare(tmp_path)
    assert _stage(output, _candidate(gaps[0])) == 0
    baseline_report = output / ".work" / "agentic" / "baseline.json"
    baseline_bytes = baseline_report.read_bytes()
    baseline_task = _load_tasks(baseline_report)["pod"]

    assert (
        adapter_main(
            [
                "resolve-agentic",
                "apply",
                "--source",
                "airflow",
                "--output-dir",
                str(output),
                "--accept-gap",
                gaps[0]["gap_id"],
            ]
        )
        == 0
    )

    applied_report = output / ".work" / "translation_report.agentic.json"
    applied = json.loads(applied_report.read_text(encoding="utf-8"))
    applied_task = _load_tasks(applied_report)["pod"]
    for field in ("name", "task_key", "depends_on", "max_retries", "timeout_seconds", "min_retry_interval_millis"):
        assert applied_task.get(field) == baseline_task.get(field)
    assert applied_task["type"] == "NotebookActivity"
    assert "Migrated from Airflow" in applied_task["generated_source"]
    assert applied["reconciliation_status"] == "verified_with_reviewed_resolutions"
    assert applied["audit"]["agentic_resolution"]["validation_status"] == "verified"
    assert baseline_report.read_bytes() == baseline_bytes
    assert (output / "metadata" / "agentic" / "accepted_resolutions.json").exists()


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("task_key", "HIJACKED"),
        ("depends_on", [{"task_key": "missing"}]),
        ("max_retries", 99),
    ],
)
def test_post_apply_proof_rejects_graph_or_policy_mutation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    field: str,
    value: object,
) -> None:
    _, output, gaps = _prepare(tmp_path)
    assert _stage(output, _candidate(gaps[0])) == 0
    original = agentic_contract._build_replacement

    def mutated_replacement(placeholder: dict, candidate: dict) -> dict:
        task = original(placeholder, candidate)
        task[field] = value
        return task

    monkeypatch.setattr(agentic_contract, "_build_replacement", mutated_replacement)

    exit_code = adapter_main(
        [
            "resolve-agentic",
            "apply",
            "--source",
            "airflow",
            "--output-dir",
            str(output),
            "--accept-gap",
            gaps[0]["gap_id"],
        ]
    )

    assert exit_code == 1
    assert "changed task identity, dependencies, or policy" in capsys.readouterr().err
    assert not (output / ".work" / "translation_report.agentic.json").exists()


def test_apply_supports_sql_leaf_payload(tmp_path: Path):
    _, output, gaps = _prepare(tmp_path)
    candidate = _candidate(gaps[0])
    sql = "SELECT 1 AS resolved\n"
    candidate["replacement"] = {"kind": "sql", "file": "task.sql", "parameters": {}}
    candidate["generated_files"] = [
        {
            "path": "task.sql",
            "language": "sql",
            "content": sql,
            "sha256": hashlib.sha256(sql.encode("utf-8")).hexdigest(),
        }
    ]
    assert _stage(output, candidate) == 0

    assert (
        adapter_main(
            [
                "resolve-agentic",
                "apply",
                "--source",
                "airflow",
                "--output-dir",
                str(output),
                "--accept-gap",
                gaps[0]["gap_id"],
            ]
        )
        == 0
    )

    task = _load_tasks(output / ".work" / "translation_report.agentic.json")["pod"]
    assert task["type"] == "SqlActivity"
    assert task["sql"] == sql
    assert task["warehouse_ref"] == "${var.warehouse_id}"
    report = output / ".work" / "translation_report.agentic.json"
    assert (
        package_main(
            [
                "--report",
                str(report),
                "--output-dir",
                str(output),
                "--no-download-workspace-files",
            ]
        )
        == 0
    )
    resource = (output / "resources" / "agentic.yml").read_text(encoding="utf-8")
    assert "sql_task:" in resource
    assert "../src/sql/pod.sql" in resource
    assert (output / "src" / "sql" / "pod.sql").read_text(encoding="utf-8") == sql


def test_nested_for_each_resolution_preserves_enclosing_control_flow(tmp_path: Path):
    fixture = Path(__file__).resolve().parents[1] / "resources" / "airflow" / "review_repros" / "a8_classic_mapping.py"
    source = tmp_path / "a8_classic_mapping.py"
    source.write_bytes(fixture.read_bytes())
    output = tmp_path / "output"
    assert airflow_convert(["--source-dir", str(source), "--output-dir", str(output)]) == 0
    report = output / ".work" / "translation_report.json"
    assert (
        adapter_main(
            [
                "resolve-agentic",
                "prepare",
                "--source",
                "airflow",
                "--source-path",
                str(source),
                "--report",
                str(report),
                "--output-dir",
                str(output),
            ]
        )
        == 0
    )
    gaps = json.loads((output / ".work" / "agentic" / "gaps.json").read_text(encoding="utf-8"))
    assert len(gaps) == 1
    assert _stage(output, _candidate(gaps[0], source="print(dbutils.widgets.get('env'))\n")) == 0
    baseline = json.loads((output / ".work" / "agentic" / "baseline.json").read_text(encoding="utf-8"))
    baseline_outer = baseline["tasks"][0]

    assert (
        adapter_main(
            [
                "resolve-agentic",
                "apply",
                "--source",
                "airflow",
                "--output-dir",
                str(output),
                "--accept-gap",
                gaps[0]["gap_id"],
            ]
        )
        == 0
    )

    applied = json.loads((output / ".work" / "translation_report.agentic.json").read_text(encoding="utf-8"))
    applied_outer = applied["tasks"][0]
    assert applied_outer["type"] == baseline_outer["type"] == "ForEachActivity"
    assert applied_outer["task_key"] == baseline_outer["task_key"]
    assert applied_outer["items_expression"] == baseline_outer["items_expression"]
    assert applied_outer["inner_activities"][0]["type"] == "NotebookActivity"


def test_apply_rejects_staged_candidate_tampering(tmp_path: Path, capsys):
    _, output, gaps = _prepare(tmp_path)
    assert _stage(output, _candidate(gaps[0])) == 0
    staged = output / ".work" / "agentic" / "candidates" / f"{gaps[0]['gap_id']}.json"
    payload = json.loads(staged.read_text(encoding="utf-8"))
    payload["replacement"]["kind"] = "sql"
    staged.write_text(json.dumps(payload), encoding="utf-8")

    exit_code = adapter_main(
        [
            "resolve-agentic",
            "apply",
            "--source",
            "airflow",
            "--output-dir",
            str(output),
            "--accept-gap",
            gaps[0]["gap_id"],
        ]
    )

    assert exit_code == 1
    assert "staged candidate was modified after validation" in capsys.readouterr().err
    assert not (output / ".work" / "translation_report.agentic.json").exists()


def test_apply_rejects_malformed_candidate_index(tmp_path: Path, capsys):
    _, output, gaps = _prepare(tmp_path)
    index = output / ".work" / "agentic" / "candidate_index.json"
    index.write_text(
        json.dumps({"../../outside": {"sha256": "0" * 64, "status": "resolved"}}),
        encoding="utf-8",
    )

    exit_code = adapter_main(
        [
            "resolve-agentic",
            "apply",
            "--source",
            "airflow",
            "--output-dir",
            str(output),
            "--accept-gap",
            gaps[0]["gap_id"],
        ]
    )

    assert exit_code == 1
    assert "Candidate index contains an unknown gap_id" in capsys.readouterr().err
    assert not (output / ".work" / "translation_report.agentic.json").exists()


def test_apply_rejects_live_source_changes(tmp_path: Path, capsys):
    source, output, gaps = _prepare(tmp_path)
    assert _stage(output, _candidate(gaps[0])) == 0
    source.write_text(source.read_text(encoding="utf-8") + "# changed\n", encoding="utf-8")

    exit_code = adapter_main(
        [
            "resolve-agentic",
            "apply",
            "--source",
            "airflow",
            "--output-dir",
            str(output),
            "--accept-gap",
            gaps[0]["gap_id"],
        ]
    )

    assert exit_code == 1
    assert "source changed since prepare; re-run prepare" in capsys.readouterr().err


def test_reduced_allowlist_restores_unaccepted_placeholder_and_reset_restores_all(tmp_path: Path):
    _, output, gaps = _prepare(tmp_path, two_tasks=True)
    for index, gap in enumerate(gaps):
        assert _stage(output, _candidate(gap), name=f"candidate-{index}.json") == 0

    assert (
        adapter_main(
            [
                "resolve-agentic",
                "apply",
                "--source",
                "airflow",
                "--output-dir",
                str(output),
                "--accept-all",
                "--review-manifest",
                str(_review_manifest(output)),
            ]
        )
        == 0
    )
    applied_report = output / ".work" / "translation_report.agentic.json"
    assert {task["type"] for task in _load_tasks(applied_report).values()} == {"NotebookActivity"}

    assert (
        adapter_main(
            [
                "resolve-agentic",
                "apply",
                "--source",
                "airflow",
                "--output-dir",
                str(output),
                "--accept-gap",
                gaps[0]["gap_id"],
            ]
        )
        == 0
    )
    types = {task_key: task["type"] for task_key, task in _load_tasks(applied_report).items()}
    assert types[gaps[0]["task_key"]] == "NotebookActivity"
    assert types[gaps[1]["task_key"]] == "PlaceholderActivity"

    assert (
        adapter_main(["resolve-agentic", "apply", "--source", "airflow", "--output-dir", str(output), "--reset"]) == 0
    )
    assert {task["type"] for task in _load_tasks(applied_report).values()} == {"PlaceholderActivity"}


def test_accept_all_requires_an_exact_prior_review_manifest(tmp_path: Path, capsys) -> None:
    _, output, gaps = _prepare(tmp_path)
    assert _stage(output, _candidate(gaps[0])) == 0

    assert (
        adapter_main(["resolve-agentic", "apply", "--source", "airflow", "--output-dir", str(output), "--accept-all"])
        == 1
    )
    assert "require --review-manifest" in capsys.readouterr().err

    stale = _review_manifest(output)
    changed = _candidate(gaps[0], source="print('replacement')")
    assert _stage(output, changed) == 1
    assert "use --replace" in capsys.readouterr().err
    assert _stage(output, changed, replace=True) == 0

    assert (
        adapter_main(
            [
                "resolve-agentic",
                "apply",
                "--source",
                "airflow",
                "--output-dir",
                str(output),
                "--accept-all",
                "--review-manifest",
                str(stale),
            ]
        )
        == 1
    )
    assert "does not exactly match" in capsys.readouterr().err


def test_review_complete_declines_exact_staged_set_and_leaves_unstaged_gaps_unreviewed(tmp_path: Path) -> None:
    _, output, gaps = _prepare(tmp_path, two_tasks=True)
    assert _stage(output, _candidate(gaps[0])) == 0

    assert (
        adapter_main(
            [
                "resolve-agentic",
                "apply",
                "--source",
                "airflow",
                "--output-dir",
                str(output),
                "--review-complete",
                "--review-manifest",
                str(_review_manifest(output)),
            ]
        )
        == 0
    )

    report = output / ".work" / "translation_report.agentic.json"
    assert {task["type"] for task in _load_tasks(report).values()} == {"PlaceholderActivity"}
    evidence = output / "metadata" / "agentic"
    decisions = json.loads((evidence / "review_decisions.json").read_text(encoding="utf-8"))["decisions"]
    assert decisions == [
        {
            "gap_id": gaps[0]["gap_id"],
            "candidate_sha256": decisions[0]["candidate_sha256"],
            "decision": "declined",
        }
    ]
    outcomes = summarize_persisted_agentic_resolutions(evidence)["pipelines"]["agentic"]
    assert outcomes == {"resolved": 0, "needs_input": 0, "deferred": 0, "declined": 1, "unreviewed": 1}


def test_reset_uses_durable_baseline_after_source_change_and_work_pruning(tmp_path: Path) -> None:
    source, output, gaps = _prepare(tmp_path)
    assert _stage(output, _candidate(gaps[0])) == 0
    assert (
        adapter_main(
            [
                "resolve-agentic",
                "apply",
                "--source",
                "airflow",
                "--output-dir",
                str(output),
                "--accept-gap",
                gaps[0]["gap_id"],
            ]
        )
        == 0
    )
    source.write_text(source.read_text(encoding="utf-8") + "# changed\n", encoding="utf-8")
    shutil.rmtree(output / ".work")

    assert (
        adapter_main(["resolve-agentic", "apply", "--source", "airflow", "--output-dir", str(output), "--reset"]) == 0
    )

    report = output / ".work" / "translation_report.agentic.json"
    assert _load_tasks(report)["pod"]["type"] == "PlaceholderActivity"
    assert (output / "metadata" / "agentic" / "source" / "dag.py").exists()


@pytest.mark.parametrize("status", ["needs_input", "deferred"])
def test_unresolved_outcome_is_terminal_and_keeps_the_linked_placeholder(tmp_path: Path, status: str):
    _, output, gaps = _prepare(tmp_path)
    assert _stage(output, _candidate(gaps[0], status=status)) == 0

    assert (
        adapter_main(
            [
                "resolve-agentic",
                "apply",
                "--source",
                "airflow",
                "--output-dir",
                str(output),
                "--accept-gap",
                gaps[0]["gap_id"],
            ]
        )
        == 0
    )

    report = output / ".work" / "translation_report.agentic.json"
    assert _load_tasks(report)["pod"]["type"] == "PlaceholderActivity"
    payload = json.loads(report.read_text(encoding="utf-8"))
    finding = next(item for item in payload["not_translatable"] if item["fingerprint"] == gaps[0]["gap_id"])
    assert finding["resolution"]["status"] == status


def test_package_accepts_only_flowx_verified_agentic_report(tmp_path: Path):
    _, output, gaps = _prepare(tmp_path)
    assert _stage(output, _candidate(gaps[0])) == 0
    assert (
        adapter_main(
            [
                "resolve-agentic",
                "apply",
                "--source",
                "airflow",
                "--output-dir",
                str(output),
                "--accept-gap",
                gaps[0]["gap_id"],
            ]
        )
        == 0
    )

    report = output / ".work" / "translation_report.agentic.json"
    assert (
        package_main(
            [
                "--report",
                str(report),
                "--output-dir",
                str(output),
                "--no-download-workspace-files",
            ]
        )
        == 0
    )
    assert not (output / ".work").exists()
    assert (output / "metadata" / "agentic" / "accepted_resolutions.json").exists()


def test_package_replays_terminal_needs_input_evidence(tmp_path: Path):
    _, output, gaps = _prepare(tmp_path)
    assert _stage(output, _candidate(gaps[0], status="needs_input")) == 0
    assert (
        adapter_main(
            [
                "resolve-agentic",
                "apply",
                "--source",
                "airflow",
                "--output-dir",
                str(output),
                "--accept-gap",
                gaps[0]["gap_id"],
            ]
        )
        == 0
    )
    report = output / ".work" / "translation_report.agentic.json"
    payload = json.loads(report.read_text(encoding="utf-8"))
    assert payload["reconciliation_status"] == "verified_with_gaps"
    accepted = output / "metadata" / "agentic" / "accepted_resolutions.json"
    accepted.write_text(json.dumps({"contract_version": "1", "candidates": []}), encoding="utf-8")
    bundle = tmp_path / "bundle"

    assert package_main(["--report", str(report), "--output-dir", str(bundle)]) == 1
    assert not (bundle / "databricks.yml").exists()


def test_package_rejects_agentic_report_tampering_before_bundle_writes(tmp_path: Path):
    _, output, gaps = _prepare(tmp_path)
    assert _stage(output, _candidate(gaps[0])) == 0
    assert (
        adapter_main(
            [
                "resolve-agentic",
                "apply",
                "--source",
                "airflow",
                "--output-dir",
                str(output),
                "--accept-gap",
                gaps[0]["gap_id"],
            ]
        )
        == 0
    )
    report = output / ".work" / "translation_report.agentic.json"
    payload = json.loads(report.read_text(encoding="utf-8"))
    payload["tasks"][0]["task_key"] = "HIJACKED"
    report.write_text(json.dumps(payload), encoding="utf-8")
    bundle = tmp_path / "bundle"

    assert package_main(["--report", str(report), "--output-dir", str(bundle)]) == 1
    assert not (bundle / "databricks.yml").exists()


def test_reviewed_resolution_evidence_drives_honest_runnable_coverage(tmp_path: Path) -> None:
    _, output, gaps = _prepare(tmp_path, two_tasks=True)
    assert _stage(output, _candidate(gaps[0]), name="resolved.json") == 0
    assert _stage(output, _candidate(gaps[1], status="needs_input"), name="needs-input.json") == 0
    assert (
        adapter_main(
            [
                "resolve-agentic",
                "apply",
                "--source",
                "airflow",
                "--output-dir",
                str(output),
                "--accept-gap",
                gaps[0]["gap_id"],
                "--accept-gap",
                gaps[1]["gap_id"],
            ]
        )
        == 0
    )
    metadata = output / "metadata"
    (metadata / "inventory.json").write_text(
        json.dumps(
            {
                "source": "airflow",
                "pipelines": [
                    {
                        "name": "agentic",
                        "activities": [],
                        "audited_activity_count": 2,
                        "deterministic_count": 0,
                        "agentic_count": 2,
                        "failed_count": 0,
                        "excluded_count": 0,
                        "reconciliation_status": "verified_with_gaps",
                        "migration_status": "included",
                        "findings": [],
                    }
                ],
            }
        ),
        encoding="utf-8",
    )

    summary = summarize_persisted_agentic_resolutions(metadata / "agentic")
    row = build_coverage_rows(metadata)[0]

    assert summary == {
        "provider_version": "0.2.0",
        "pipelines": {"agentic": {"resolved": 1, "needs_input": 1, "deferred": 0, "declined": 0, "unreviewed": 0}},
    }
    assert row["coverage_pct"] == 100.0
    assert row["deterministic_coverage_pct"] == 0.0
    assert row["runnable_coverage_pct"] == 50.0
    assert row["unresolved_agentic_activities"] == 1
    assert row["agentic_provider_version"] == "0.2.0"
    assert row["reconciliation_status"] == "verified_with_reviewed_resolutions"


def test_reporting_keeps_non_resolver_source_gaps_unreviewed(tmp_path: Path) -> None:
    source = tmp_path / "mixed_gaps.py"
    source.write_text(
        "from airflow import DAG\n"
        "with DAG(dag_id='mixed_gaps') as dag:\n"
        "    pod = KubernetesPodOperator(task_id='pod', image='python:3.12')\n"
        "    for item in runtime_values:\n"
        "        KubernetesPodOperator(task_id=f'dynamic_{item}', image='python:3.12')\n",
        encoding="utf-8",
    )
    output = tmp_path / "output"
    gaps = _prepare_source(source, output)
    assert len(gaps) == 1
    assert _stage(output, _candidate(gaps[0])) == 0
    assert (
        adapter_main(
            [
                "resolve-agentic",
                "apply",
                "--source",
                "airflow",
                "--output-dir",
                str(output),
                "--accept-gap",
                gaps[0]["gap_id"],
            ]
        )
        == 0
    )
    baseline = json.loads((output / ".work" / "agentic" / "baseline.json").read_text(encoding="utf-8"))
    metadata = output / "metadata"
    (metadata / "inventory.json").write_text(
        json.dumps(
            {
                "source": "airflow",
                "pipelines": [
                    {
                        "name": "mixed_gaps",
                        "activities": [],
                        "audited_activity_count": baseline["audit"]["audited_activity_count"],
                        "deterministic_count": baseline["audit"]["deterministic_count"],
                        "agentic_count": baseline["audit"]["agentic_count"],
                        "failed_count": baseline["audit"]["failed_count"],
                        "excluded_count": 0,
                        "reconciliation_status": baseline["reconciliation_status"],
                        "migration_status": "included",
                        "findings": baseline["not_translatable"],
                    }
                ],
            }
        ),
        encoding="utf-8",
    )

    row = build_coverage_rows(metadata)[0]

    assert json.loads(row["agentic_resolution_outcomes"]) == {
        "resolved": 1,
        "needs_input": 0,
        "deferred": 0,
        "declined": 0,
        "unreviewed": 1,
    }
    assert row["unresolved_agentic_activities"] == 1


def test_reporting_rejects_duplicate_hash_valid_agentic_evidence(tmp_path: Path) -> None:
    _, output, gaps = _prepare(tmp_path)
    assert _stage(output, _candidate(gaps[0])) == 0
    assert (
        adapter_main(
            [
                "resolve-agentic",
                "apply",
                "--source",
                "airflow",
                "--output-dir",
                str(output),
                "--accept-gap",
                gaps[0]["gap_id"],
            ]
        )
        == 0
    )
    evidence = output / "metadata" / "agentic"
    duplicated_gaps = [gaps[0], gaps[0]]
    gaps_bytes = (json.dumps(duplicated_gaps, sort_keys=True, separators=(",", ":")) + "\n").encode()
    (evidence / "gaps.json").write_bytes(gaps_bytes)
    manifest_path = evidence / "manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["gaps_sha256"] = hashlib.sha256(gaps_bytes).hexdigest()
    manifest_path.write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")

    with pytest.raises(AgenticContractError, match="duplicate persisted gap_id"):
        summarize_persisted_agentic_resolutions(evidence)
