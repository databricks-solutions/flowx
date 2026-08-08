"""Tests for Airflow source auditing, exclusions, and package preflight."""

from __future__ import annotations

import ast
import json
from pathlib import Path

import pytest

from flowx import ir_serde
from flowx.bundler import dab_writer
from flowx.models.ir import Dependency, Pipeline, PlaceholderActivity
from flowx.sources.airflow import audit
from flowx.sources.airflow import loader as airflow_loader

_SIMPLE_DAG = (
    "from airflow import DAG\n"
    "from airflow.operators.bash import BashOperator\n"
    "with DAG(dag_id='audited', schedule='@daily') as dag:\n"
    "    first = BashOperator(task_id='first', bash_command='echo first')\n"
    "    second = BashOperator(task_id='second', bash_command='echo second')\n"
    "    first >> second\n"
)


def _candidate(kind: str, code: str) -> audit.AuditCandidate:
    return audit.AuditCandidate(kind=kind, code=code, line=1, column=0, occurrence=1)


def test_finding_fingerprint_uses_relative_file_full_span_and_code() -> None:
    first = audit.finding(
        source_file="dags/example.py",
        code="argument_loss",
        severity="failed",
        message="lost",
        candidate=audit.AuditCandidate(
            kind="argument",
            code="argument_loss",
            line=4,
            column=2,
            end_line=4,
            end_column=12,
            occurrence=1,
        ),
    )
    second = audit.finding(
        source_file="dags/example.py",
        code="argument_loss",
        severity="failed",
        message="lost",
        candidate=audit.AuditCandidate(
            kind="argument",
            code="argument_loss",
            line=4,
            column=2,
            end_line=5,
            end_column=12,
            occurrence=1,
        ),
    )

    assert first["source_file"] == "dags/example.py"
    assert first["end_line"] == 4
    assert first["end_column"] == 12
    assert first["fingerprint"] != second["fingerprint"]


@pytest.mark.parametrize("mutation", ["task", "edge", "setting", "argument"])
def test_source_capture_mutations_fail_reconciliation(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, mutation: str
) -> None:
    dag_path = tmp_path / "audited.py"
    dag_path.write_text(_SIMPLE_DAG, encoding="utf-8")

    if mutation in {"task", "edge"}:
        original_audit = audit.audit_module

        def mutate(module: ast.Module, *, target_dag_variable: str | None = None) -> audit.SourceAudit:
            result = original_audit(module, target_dag_variable=target_dag_variable)
            if mutation == "task":
                result.tasks.append(_candidate("task", "removed_capture_task"))
            else:
                result.edges.append(_candidate("edge", "removed_capture_edge"))
            return result

        monkeypatch.setattr(audit, "audit_module", mutate)
    elif mutation == "setting":
        original_apply = airflow_loader._DagVisitor._apply_dag_kwargs

        def drop_setting(self, kwargs):
            original_apply(self, kwargs)
            self.captured_dag_settings.discard("schedule")

        monkeypatch.setattr(airflow_loader._DagVisitor, "_apply_dag_kwargs", drop_setting)
    else:
        original_register = airflow_loader._DagVisitor._register_operator_call

        def drop_argument(self, node, var, *, binding=None):
            registered = original_register(self, node, var, binding=binding)
            if registered and self.operators[var][0] == "first":
                self.operators[var][2].pop("bash_command", None)
            return registered

        monkeypatch.setattr(airflow_loader._DagVisitor, "_register_operator_call", drop_argument)

    pipeline = airflow_loader.load_airflow_dag(dag_path)

    assert pipeline.reconciliation_status == "failed"
    assert any(finding["severity"] == "failed" for finding in pipeline.not_translatable)
    assert pipeline.audit["failed_count"] == (1 if mutation in {"task", "argument"} else 0)


def test_failed_report_blocks_package_before_bundle_writes(tmp_path: Path) -> None:
    report = tmp_path / "failed.json"
    output = tmp_path / "bundle"
    pipeline = Pipeline(
        name="failed",
        tags={"source": "airflow"},
        reconciliation_status="failed",
        not_translatable=[
            {
                "code": "task_capture_mismatch",
                "severity": "failed",
                "message": "one source task was not captured",
            }
        ],
        audit={
            "source_file": "failed.py",
            "audited_activity_count": 1,
            "transformations": [],
        },
    )
    report.write_text(json.dumps(ir_serde.pipeline_to_dict(pipeline)), encoding="utf-8")

    exit_code = dab_writer.main(
        [
            "--report",
            str(report),
            "--output-dir",
            str(output),
            "--no-download-workspace-files",
            "--keep-intermediates",
        ]
    )

    assert exit_code == 1
    assert not (output / "databricks.yml").exists()
    assert not (output / "resources").exists()
    assert not (output / "src").exists()


def test_captured_task_removed_from_ir_fails_reconciliation(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    dag_path = tmp_path / "audited.py"
    dag_path.write_text(_SIMPLE_DAG, encoding="utf-8")
    original_reconcile = airflow_loader._reconcile_pipeline

    def remove_emitted_task(pipeline, **kwargs):
        pipeline.tasks.pop()
        return original_reconcile(pipeline, **kwargs)

    monkeypatch.setattr(airflow_loader, "_reconcile_pipeline", remove_emitted_task)

    pipeline = airflow_loader.load_airflow_dag(dag_path)

    assert pipeline.reconciliation_status == "failed"
    finding = next(item for item in pipeline.not_translatable if item["code"] == "captured_task_not_emitted")
    assert finding["details"]["task_keys"] == ["second"]


@pytest.mark.parametrize(
    ("body", "expected_failed"),
    [
        (
            "    head = BashOperator(task_id='head', bash_command='echo head')\n"
            "    fanout = [BashOperator(task_id=f'work_{i}', bash_command='echo work') for i in range(3)]\n",
            1,
        ),
        (
            "    first, second = (\n"
            "        BashOperator(task_id='first', bash_command='echo first'),\n"
            "        BashOperator(task_id='second', bash_command='echo second'),\n"
            "    )\n",
            2,
        ),
    ],
)
def test_unclaimed_dag_task_construction_fails_closed(tmp_path: Path, body: str, expected_failed: int) -> None:
    dag_path = tmp_path / "unclaimed.py"
    dag_path.write_text(
        "from airflow import DAG\n"
        "from airflow.operators.bash import BashOperator\n"
        "with DAG(dag_id='unclaimed') as dag:\n" + body,
        encoding="utf-8",
    )

    pipeline = airflow_loader.load_airflow_dag(dag_path)

    assert pipeline.reconciliation_status == "failed"
    assert pipeline.audit["failed_count"] == expected_failed
    assert pipeline.audit["audited_activity_count"] >= expected_failed
    assert any(item["code"] == "unclaimed_dag_task" for item in pipeline.not_translatable)


def test_source_edge_identity_mismatch_fails_reconciliation(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    dag_path = tmp_path / "rewired.py"
    dag_path.write_text(_SIMPLE_DAG, encoding="utf-8")
    original_add_edges = airflow_loader._DagVisitor._add_edges

    def reverse_edge(self, upstreams, downstreams, node):
        return original_add_edges(self, downstreams, upstreams, node)

    monkeypatch.setattr(airflow_loader._DagVisitor, "_add_edges", reverse_edge)

    pipeline = airflow_loader.load_airflow_dag(dag_path)

    assert pipeline.reconciliation_status == "failed"
    finding = next(item for item in pipeline.not_translatable if item["code"] == "edge_identity_mismatch")
    assert finding["details"]["audited_edges"] == [["first", "second"]]
    assert finding["details"]["captured_edges"] == [["second", "first"]]


def test_captured_edge_removed_from_ir_fails_reconciliation(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    dag_path = tmp_path / "missing_ir_edge.py"
    dag_path.write_text(_SIMPLE_DAG, encoding="utf-8")
    original_reconcile = airflow_loader._reconcile_pipeline

    def remove_emitted_edge(pipeline, **kwargs):
        pipeline.tasks[-1].depends_on = None
        return original_reconcile(pipeline, **kwargs)

    monkeypatch.setattr(airflow_loader, "_reconcile_pipeline", remove_emitted_edge)

    pipeline = airflow_loader.load_airflow_dag(dag_path)

    assert pipeline.reconciliation_status == "failed"
    finding = next(item for item in pipeline.not_translatable if item["code"] == "captured_edge_not_emitted")
    assert finding["details"]["missing_edges"] == [["first", "second"]]


def test_helper_capture_does_not_depend_on_independent_auditor_classification(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repro = Path(__file__).parents[1] / "resources" / "airflow" / "review_repros" / "t8_helperfn.py"
    original_audit = audit.audit_module

    def omit_helper_candidates(module: ast.Module, *, target_dag_variable: str | None = None) -> audit.SourceAudit:
        result = original_audit(module, target_dag_variable=target_dag_variable)
        result.tasks = [candidate for candidate in result.tasks if candidate.code != "helper_factory_task"]
        return result

    monkeypatch.setattr(audit, "audit_module", omit_helper_candidates)

    pipeline = airflow_loader.load_airflow_dag(repro)

    assert pipeline.reconciliation_status == "verified"
    assert pipeline.audit["audited_activity_count"] == 2


def test_removing_real_helper_capture_is_detected(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    dag_path = tmp_path / "helper.py"
    dag_path.write_text(
        "from airflow import DAG\n"
        "from airflow.operators.bash import BashOperator\n"
        "def make(task_id):\n"
        "    return BashOperator(task_id=task_id, bash_command='echo work')\n"
        "with DAG(dag_id='helper') as dag:\n"
        "    work = make('work')\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(airflow_loader._DagVisitor, "_register_helper_factory_call", lambda *args, **kwargs: False)

    pipeline = airflow_loader.load_airflow_dag(dag_path)

    assert pipeline.reconciliation_status == "failed"
    assert any(item["code"] == "unclaimed_dag_task" for item in pipeline.not_translatable)


def test_dynamic_helper_statement_cannot_escape_both_capture_passes(tmp_path: Path) -> None:
    dag_path = tmp_path / "dynamic_helper.py"
    dag_path.write_text(
        "from airflow import DAG\n"
        "from airflow.operators.bash import BashOperator\n"
        "def make(task_id):\n"
        "    command = 'echo work'\n"
        "    return BashOperator(task_id=task_id, bash_command=command)\n"
        "with DAG(dag_id='dynamic_helper') as dag:\n"
        "    work = make('work')\n",
        encoding="utf-8",
    )

    pipeline = airflow_loader.load_airflow_dag(dag_path)

    assert pipeline.reconciliation_status == "failed"
    assert pipeline.audit["failed_count"] == 1
    assert any(item["code"] == "unclaimed_dag_statement" for item in pipeline.not_translatable)


def test_assigned_dag_dynamic_helper_call_fails_closed(tmp_path: Path) -> None:
    dag_path = tmp_path / "assigned_dynamic_helper.py"
    dag_path.write_text(
        "from airflow import DAG\n"
        "from airflow.operators.bash import BashOperator\n"
        "dag = DAG(dag_id='assigned_dynamic_helper')\n"
        "def make(task_id):\n"
        "    command = 'echo work'\n"
        "    return BashOperator(task_id=task_id, bash_command=command, dag=dag)\n"
        "work = make('work')\n",
        encoding="utf-8",
    )

    pipeline = airflow_loader.load_airflow_dag(dag_path)

    assert pipeline.reconciliation_status == "failed"
    assert pipeline.audit["failed_count"] == 1
    assert any(item["code"] == "unclaimed_dag_task" for item in pipeline.not_translatable)


def test_uninvoked_module_helper_body_does_not_create_a_task(tmp_path: Path) -> None:
    dag_path = tmp_path / "dormant_helper.py"
    dag_path.write_text(
        "from airflow import DAG\n"
        "from airflow.operators.bash import BashOperator\n"
        "dag = DAG(dag_id='dormant_helper')\n"
        "def dormant():\n"
        "    task = BashOperator(task_id='dormant', bash_command='echo dormant', dag=dag)\n"
        "    return task\n",
        encoding="utf-8",
    )

    pipeline = airflow_loader.load_airflow_dag(dag_path)

    assert pipeline.reconciliation_status == "verified"
    assert pipeline.tasks == []
    assert pipeline.audit["audited_activity_count"] == 0


def test_unresolved_construct_is_classified_in_coverage(tmp_path: Path) -> None:
    dag_path = tmp_path / "dynamic.py"
    dag_path.write_text(
        "from airflow import DAG\n"
        "from airflow.operators.bash import BashOperator\n"
        "with DAG(dag_id='dynamic') as dag:\n"
        "    stable = BashOperator(task_id='stable', bash_command='echo stable')\n"
        "    for item in runtime_values:\n"
        "        BashOperator(task_id=f'work_{item}', bash_command='echo work')\n",
        encoding="utf-8",
    )

    pipeline = airflow_loader.load_airflow_dag(dag_path)

    assert pipeline.reconciliation_status == "verified_with_gaps"
    assert pipeline.audit["audited_activity_count"] == 2
    assert pipeline.audit["deterministic_count"] == 1
    assert pipeline.audit["agentic_count"] == 1


def test_bundle_invariant_failure_is_preflighted_before_destination_writes(tmp_path: Path) -> None:
    report = tmp_path / "dangling.json"
    output = tmp_path / "bundle"
    pipeline = Pipeline(
        name="parent",
        tags={"source": "airflow"},
        reconciliation_status="verified",
        audit={
            "source_file": "parent.py",
            "audited_activity_count": 1,
            "transformations": [],
        },
        tasks=[
            PlaceholderActivity(
                name="dangling",
                task_key="dangling",
                original_type="Test",
                depends_on=[Dependency(task_key="missing")],
            )
        ],
    )
    report.write_text(json.dumps(ir_serde.pipeline_to_dict(pipeline)), encoding="utf-8")

    exit_code = dab_writer.main(
        [
            "--report",
            str(report),
            "--output-dir",
            str(output),
            "--no-download-workspace-files",
            "--keep-intermediates",
        ]
    )

    assert exit_code == 1
    assert not (output / "databricks.yml").exists()


def test_excluded_dag_stays_audited_and_included_reference_becomes_placeholder(tmp_path: Path) -> None:
    (tmp_path / "caller.py").write_text(
        "from airflow import DAG\n"
        "from airflow.operators.trigger_dagrun import TriggerDagRunOperator\n"
        "with DAG(dag_id='caller') as dag:\n"
        "    trigger = TriggerDagRunOperator(task_id='trigger', trigger_dag_id='target')\n",
        encoding="utf-8",
    )
    (tmp_path / "target.py").write_text(
        "from airflow import DAG\n"
        "from airflow.operators.bash import BashOperator\n"
        "with DAG(dag_id='target') as dag:\n"
        "    work = BashOperator(task_id='work', bash_command='echo work')\n",
        encoding="utf-8",
    )

    pipelines = airflow_loader.load_pipelines(tmp_path, exclude_dags={"target"})
    by_name = {pipeline.name: pipeline for pipeline in pipelines}

    assert by_name["target"].migration_status == "excluded"
    assert by_name["target"].audit["audited_activity_count"] == 1
    assert by_name["target"].audit["excluded_count"] == 1
    assert isinstance(by_name["caller"].tasks[0], PlaceholderActivity)
    assert by_name["caller"].tasks[0].raw_definition == {"excluded_dag": "target"}
    assert by_name["caller"].reconciliation_status == "verified_with_gaps"


@pytest.mark.parametrize(
    ("filename", "dag_import", "decorator"),
    [
        ("aliased.py", "from airflow.decorators import dag as workflow", "workflow"),
        ("qualified.py", "import airflow.sdk", "airflow.sdk.dag"),
    ],
)
def test_discovery_resolves_aliased_and_qualified_dag_decorators(
    tmp_path: Path,
    filename: str,
    dag_import: str,
    decorator: str,
) -> None:
    dag_path = tmp_path / filename
    dag_id = dag_path.stem
    dag_path.write_text(
        f"{dag_import}\n"
        "from airflow.operators.bash import BashOperator\n"
        f"@{decorator}(dag_id='{dag_id}')\n"
        "def build():\n"
        "    BashOperator(task_id='work', bash_command='echo work')\n"
        "build()\n",
        encoding="utf-8",
    )

    assert airflow_loader.discover_dags(dag_path) == [dag_path]
    pipeline = airflow_loader.load_pipelines(dag_path)[0]
    assert pipeline.name == dag_id
    assert pipeline.reconciliation_status == "verified"
    assert [task.task_key for task in pipeline.tasks] == ["work"]


def test_mixed_directory_does_not_hide_an_aliased_dag(tmp_path: Path) -> None:
    canonical = tmp_path / "canonical.py"
    canonical.write_text(
        "from airflow import DAG\n"
        "from airflow.operators.bash import BashOperator\n"
        "with DAG(dag_id='canonical') as dag:\n"
        "    BashOperator(task_id='work', bash_command='echo canonical')\n",
        encoding="utf-8",
    )
    aliased = tmp_path / "aliased.py"
    aliased.write_text(
        "from airflow.decorators import dag as workflow\n"
        "from airflow.operators.bash import BashOperator\n"
        "@workflow(dag_id='aliased')\n"
        "def build():\n"
        "    BashOperator(task_id='work', bash_command='echo aliased')\n"
        "build()\n",
        encoding="utf-8",
    )

    assert airflow_loader.discover_dags(tmp_path) == [aliased, canonical]
    assert {pipeline.name for pipeline in airflow_loader.load_pipelines(tmp_path)} == {"aliased", "canonical"}


def test_taskflow_alias_is_captured_inside_a_recognized_dag(tmp_path: Path) -> None:
    dag_path = tmp_path / "task_alias.py"
    dag_path.write_text(
        "from airflow.decorators import dag, task as step\n"
        "@step\n"
        "def work():\n"
        "    return 1\n"
        "@dag(dag_id='task_alias')\n"
        "def build():\n"
        "    work()\n"
        "build()\n",
        encoding="utf-8",
    )

    pipeline = airflow_loader.load_pipelines(dag_path)[0]

    assert pipeline.reconciliation_status == "verified"
    assert [task.task_key for task in pipeline.tasks] == ["work"]


def test_static_classic_dag_factory_preserves_dag_identity_and_tasks(tmp_path: Path) -> None:
    dag_path = tmp_path / "classic_factory.py"
    dag_path.write_text(
        "from airflow import DAG\n"
        "from airflow.operators.bash import BashOperator\n"
        "def make_dag(dag_id, message='default'):\n"
        "    with DAG(dag_id=dag_id) as dag:\n"
        "        BashOperator(task_id='work', bash_command=f'echo {message}')\n"
        "    return dag\n"
        "factory_dag = make_dag('factory_dag', message='hello')\n",
        encoding="utf-8",
    )

    pipeline = airflow_loader.load_pipelines(dag_path)[0]

    assert pipeline.name == "factory_dag"
    assert pipeline.reconciliation_status == "verified"
    assert [task.task_key for task in pipeline.tasks] == ["work"]
    assert "echo hello" in (pipeline.tasks[0].generated_source or "")


def test_decorated_dag_factory_override_emits_each_invocation(tmp_path: Path) -> None:
    dag_path = tmp_path / "decorated_factory.py"
    dag_path.write_text(
        "from airflow.decorators import dag\n"
        "from airflow.operators.bash import BashOperator\n"
        "@dag\n"
        "def build(message='default'):\n"
        "    BashOperator(task_id='work', bash_command=f'echo {message}')\n"
        "first = build.override(dag_id='first')('one')\n"
        "second = build.override(dag_id='second')('two')\n",
        encoding="utf-8",
    )

    pipelines = airflow_loader.load_pipelines(dag_path)

    assert [pipeline.name for pipeline in pipelines] == ["first", "second"]
    assert all(pipeline.reconciliation_status == "verified" for pipeline in pipelines)
    assert "echo one" in (pipelines[0].tasks[0].generated_source or "")
    assert "echo two" in (pipelines[1].tasks[0].generated_source or "")


def test_dynamic_dag_factory_fails_closed_instead_of_emitting_verified_empty_ir(tmp_path: Path) -> None:
    dag_path = tmp_path / "dynamic_factory.py"
    dag_path.write_text(
        "from airflow import DAG\n"
        "from airflow.operators.bash import BashOperator\n"
        "def make_dag(dag_id):\n"
        "    with DAG(dag_id=dag_id) as dag:\n"
        "        BashOperator(task_id='work', bash_command='echo work')\n"
        "    return dag\n"
        "factory_dag = make_dag(runtime_dag_id())\n",
        encoding="utf-8",
    )

    pipeline = airflow_loader.load_pipelines(dag_path)[0]

    assert pipeline.name == "factory_dag"
    assert pipeline.reconciliation_status == "failed"
    assert pipeline.audit["failed_count"] == 1
    assert any(finding["code"] == "unsupported_dag_factory" for finding in pipeline.not_translatable)
