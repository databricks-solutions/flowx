"""Tests for engine --merge-agentic (folding agent results into a translation report)."""

from __future__ import annotations

import json
from pathlib import Path

from flowx.translator.engine import merge_agentic_results


def _write(path: Path, obj: object) -> None:
    path.write_text(json.dumps(obj), encoding="utf-8")


def test_merge_replaces_nested_placeholder_and_preserves_edges(tmp_path: Path):
    report = tmp_path / "translation_report.json"
    _write(
        report,
        {
            "name": "p",
            "tasks": [
                {
                    "name": "Gate",
                    "type": "IfConditionActivity",
                    "task_key": "gate",
                    "if_true_activities": [
                        {
                            "name": "Wait",
                            "type": "PlaceholderActivity",
                            "task_key": "wait",
                            "original_type": "Until",
                            "depends_on": [{"task_key": "upstream", "outcome": "Succeeded"}],
                        },
                    ],
                },
            ],
        },
    )
    results = tmp_path / "agentic_results"
    results.mkdir()
    _write(
        results / "wait.json",
        {
            "activity_name": "Wait",
            "task": {
                "type": "NotebookActivity",
                "name": "Wait",
                "task_key": "wait",
                "notebook_path": "/Workspace/Shared/until_wait",
            },
        },
    )

    merged, unmatched = merge_agentic_results(report, results)
    assert (merged, unmatched) == (1, 0)
    out = json.loads(report.read_text())
    task = out["tasks"][0]["if_true_activities"][0]
    assert task["type"] == "NotebookActivity"
    assert task["notebook_path"] == "/Workspace/Shared/until_wait"
    # depends_on carried over from the placeholder
    assert task["depends_on"] == [{"task_key": "upstream", "outcome": "Succeeded"}]


def test_merge_unmatched_when_activity_absent(tmp_path: Path):
    report = tmp_path / "r.json"
    _write(report, {"name": "p", "tasks": [{"name": "A", "type": "NotebookActivity", "task_key": "a"}]})
    results = tmp_path / "res"
    results.mkdir()
    _write(results / "x.json", {"activity_name": "Nope", "task": {"type": "NotebookActivity", "name": "Nope"}})

    merged, unmatched = merge_agentic_results(report, results)
    assert (merged, unmatched) == (0, 1)


def test_merge_multi_pipeline_disambiguates_by_name(tmp_path: Path):
    report = tmp_path / "r.json"
    _write(
        report,
        {
            "pipelines": [
                {"name": "p1", "tasks": [{"name": "U", "type": "PlaceholderActivity", "task_key": "u1"}]},
                {"name": "p2", "tasks": [{"name": "U", "type": "PlaceholderActivity", "task_key": "u2"}]},
            ]
        },
    )
    results = tmp_path / "res"
    results.mkdir()
    _write(
        results / "u.json",
        {
            "pipeline": "p2",
            "activity_name": "U",
            "task": {"type": "NotebookActivity", "name": "U", "task_key": "u2", "notebook_path": "/x"},
        },
    )
    merged, unmatched = merge_agentic_results(report, results)
    assert (merged, unmatched) == (1, 0)
    out = json.loads(report.read_text())
    assert out["pipelines"][0]["tasks"][0]["type"] == "PlaceholderActivity"  # p1 untouched
    assert out["pipelines"][1]["tasks"][0]["type"] == "NotebookActivity"  # p2 merged
