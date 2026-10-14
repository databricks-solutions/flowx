"""Validate that agentic replacements preserve placeholder structure."""

from __future__ import annotations

import ast
import json
import sys
from pathlib import Path
from typing import Any

VALID_TYPES = {
    "AppendVariableActivity",
    "CopyActivity",
    "DbtFactoryActivity",
    "DeleteActivity",
    "ExecutePipelineActivity",
    "FilterActivity",
    "ForEachActivity",
    "IfConditionActivity",
    "LookupActivity",
    "MotifActivity",
    "NotebookActivity",
    "RunJobActivity",
    "SetVariableActivity",
    "SparkJarActivity",
    "SparkPythonActivity",
    "SqlActivity",
    "SwitchActivity",
    "WaitActivity",
    "WebActivity",
}


def _tasks(value: Any):
    if isinstance(value, dict):
        if isinstance(value.get("name"), str) and isinstance(value.get("type"), str):
            yield value
        for child in value.values():
            yield from _tasks(child)
    elif isinstance(value, list):
        for child in value:
            yield from _tasks(child)


def _dependencies(value: Any) -> list[Any]:
    dependencies = value or []
    if not isinstance(dependencies, list):
        return [dependencies]
    return sorted(dependencies, key=lambda dependency: json.dumps(dependency, sort_keys=True))


def validate(expected: list[dict[str, Any]], report: dict[str, Any]) -> list[str]:
    """Return every structural failure without exposing generated source."""
    failures: list[str] = []
    pipelines = report.get("pipelines", [report])
    by_pipeline = {
        pipeline["name"]: {task["name"]: task for task in _tasks(pipeline.get("tasks", []))} for pipeline in pipelines
    }
    for item in expected:
        pipeline_name = item["pipeline"]
        activity_name = item["activity_name"]
        label = f"{pipeline_name}/{activity_name}"
        task = by_pipeline.get(pipeline_name, {}).get(activity_name)
        if task is None:
            failures.append(f"{label}: task: expected present, actual missing")
            continue

        actual_type = task.get("type")
        if actual_type not in VALID_TYPES:
            failures.append(f"{label}: type: expected valid task type, actual {actual_type!r}")

        expected_key = item.get("task_key")
        actual_key = task.get("task_key")
        if actual_key != expected_key:
            failures.append(f"{label}: task_key: expected {expected_key!r}, actual {actual_key!r}")

        expected_dependencies = _dependencies(item.get("depends_on"))
        actual_dependencies = _dependencies(task.get("depends_on"))
        if actual_dependencies != expected_dependencies:
            failures.append(f"{label}: depends_on: expected {expected_dependencies!r}, actual {actual_dependencies!r}")

        if actual_type == "NotebookActivity":
            source = task.get("generated_source")
            if not isinstance(source, str) or not source.strip():
                failures.append(f"{label}: generated_source: expected non-empty valid Python, actual empty")
            else:
                try:
                    ast.parse(source)
                except SyntaxError:
                    failures.append(f"{label}: generated_source: expected valid Python, actual invalid")
    return failures


def main() -> int:
    """Validate the expected structure file against a merged report."""
    expected = json.loads(Path(sys.argv[1]).read_text(encoding="utf-8"))
    report = json.loads(Path(sys.argv[2]).read_text(encoding="utf-8"))
    failures = validate(expected, report)
    if failures:
        print("agentic structure validation failed:", file=sys.stderr)
        for failure in failures:
            print(f"- {failure}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
