"""Tests that the package phase runs bundle invariants (Tier-0) over its output."""

from __future__ import annotations

import json
import tempfile
from pathlib import Path

from flowx.bundler.dab_writer import main as package_main


def _run_package(report: dict) -> int:
    with tempfile.TemporaryDirectory() as tmp:
        out = Path(tmp)
        work = out / ".work"
        work.mkdir(parents=True)
        (work / "translation_report.json").write_text(json.dumps(report), encoding="utf-8")
        return package_main(["--output-dir", str(out)])


def _notebook_task(name: str, task_key: str) -> dict:
    return {
        "name": name,
        "task_key": task_key,
        "type": "NotebookActivity",
        "notebook_path": f"notebooks/{name}.py",
        "generated_source": "# Databricks notebook source\nprint('x')\n",
    }


def test_package_passes_invariants_for_clean_bundle():
    report = {"name": "clean", "tasks": [_notebook_task("a", "a"), _notebook_task("b", "b")]}
    assert _run_package(report) == 0


def test_package_fails_on_duplicate_task_key():
    # Two tasks sharing a task_key -> duplicate_task_key violation -> non-zero exit.
    report = {"name": "bad", "tasks": [_notebook_task("a", "dup"), _notebook_task("b", "dup")]}
    assert _run_package(report) == 1


def test_package_loads_multi_pipeline_report():
    # A {"pipelines": [...]} report (emitted for multi-DAG conversion) must package all pipelines,
    # not silently produce "no pipelines found". Guards the P0 multi-DAG load crash.
    from flowx.bundler.dab_writer import _load_report

    report = {
        "pipelines": [
            {"name": "first", "tasks": [_notebook_task("x", "x")]},
            {"name": "second", "tasks": [_notebook_task("y", "y")]},
        ]
    }
    with tempfile.TemporaryDirectory() as tmp:
        out = Path(tmp)
        work = out / ".work"
        work.mkdir(parents=True)
        report_path = work / "translation_report.json"
        report_path.write_text(json.dumps(report), encoding="utf-8")
        workflows = _load_report(report_path)
        assert [w.name for w in workflows] == ["first", "second"]
        assert package_main(["--output-dir", str(out)]) == 0
