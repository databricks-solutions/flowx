"""Tests for Step Functions JSONPath I/O rewriting into task parameters and task values."""

from __future__ import annotations

from pathlib import Path

from flowx.models.ir import PlaceholderActivity
from flowx.sources.stepfunctions.jsonpath import resolve_parameters, result_field
from flowx.sources.stepfunctions.loader import load_pipelines

_FIXTURE = Path(__file__).parent.parent / "resources" / "stepfunctions" / "ingest_pipeline.json"


def test_result_field_reads_result_path_leaf() -> None:
    """ResultPath $.extract registers the field 'extract'; $ / null / absent register nothing."""
    assert result_field({"ResultPath": "$.extract"}) == "extract"
    assert result_field({"ResultPath": "$"}) is None
    assert result_field({"ResultPath": None}) is None
    assert result_field({}) is None


def test_resolve_parameters_literals_job_params_and_task_values() -> None:
    """A .$ ref resolves to a task value when produced upstream, else a declared job parameter."""
    producers = {"extract": "extract"}
    job_parameters: dict[str, str] = {}
    notes: list[dict] = []
    resolved = resolve_parameters(
        {"manifest.$": "$.extract.manifestPath", "targetTable.$": "$.targetTable", "mode": "full"},
        producers,
        job_parameters,
        notes,
        "Load",
    )
    assert resolved["mode"] == "full"
    assert resolved["manifest"] == "{{tasks.extract.values.extract}}"
    assert resolved["targetTable"] == "{{job.parameters.targetTable}}"
    assert job_parameters == {"targetTable": ""}


def test_context_and_intrinsic_refs_left_verbatim() -> None:
    """Context-object and intrinsic references are not resolved and are noted."""
    notes: list[dict] = []
    resolved = resolve_parameters({"id.$": "$$.Execution.Id", "ts.$": "States.Format('{}', $.x)"}, {}, {}, notes, "S")
    assert resolved["id"] == "$$.Execution.Id"
    assert resolved["ts"] == "States.Format('{}', $.x)"
    assert len(notes) == 2


def test_pipeline_declares_job_parameters_from_machine_input() -> None:
    """Machine-input references across the state machine become declared job parameters."""
    pipeline = load_pipelines(_FIXTURE)[0]
    names = {parameter["name"] for parameter in (pipeline.parameters or [])}
    assert names == {"sourceTable", "runDate", "targetTable"}


def test_task_base_parameters_wire_input_and_upstream_result() -> None:
    """Extract reads machine input; Load reads Extract's result as a task value."""
    pipeline = load_pipelines(_FIXTURE)[0]
    by_name = {task.name: task for task in pipeline.tasks}

    extract = by_name["Extract"]
    assert isinstance(extract, PlaceholderActivity)
    assert extract.base_parameters == {
        "sourceTable": "{{job.parameters.sourceTable}}",
        "runDate": "{{job.parameters.runDate}}",
        "mode": "full",
    }

    load = by_name["Load"]
    assert load.base_parameters["targetTable"] == "{{job.parameters.targetTable}}"
    assert load.base_parameters["manifest"] == "{{tasks.extract.values.extract}}"
