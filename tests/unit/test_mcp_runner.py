"""Tests for hosted MCP source materialization."""

from __future__ import annotations

from pathlib import Path

import pytest

from flowx.mcp import runner


def test_materialize_airflow_definitions_writes_nested_python_files() -> None:
    source = runner.materialize_airflow_definitions(
        {
            "daily.py": "from airflow import DAG\n",
            "team/hourly.py": "from airflow.decorators import dag\n",
        }
    )
    root = Path(source)
    try:
        assert (root / "daily.py").read_text(encoding="utf-8") == "from airflow import DAG\n"
        assert (root / "team" / "hourly.py").read_text(encoding="utf-8") == ("from airflow.decorators import dag\n")
    finally:
        runner.cleanup_materialized(source)


@pytest.mark.parametrize(
    ("definitions", "message"),
    [
        ({}, "empty"),
        ({"../escape.py": "pass\n"}, "unsafe path"),
        ({"README.md": "not a DAG"}, "must end in .py"),
        ({"dag.py": {"not": "source"}}, "must be a string"),
        ({"team/./dag.py": "pass\n", "team/dag.py": "print('duplicate')\n"}, "duplicate path"),
    ],
)
def test_materialize_airflow_definitions_rejects_unsafe_payloads(definitions: dict[str, object], message: str) -> None:
    with pytest.raises(ValueError, match=message):
        runner.materialize_airflow_definitions(definitions)  # type: ignore[arg-type]


def test_materialize_airflow_definitions_enforces_utf8_byte_limit(monkeypatch) -> None:
    monkeypatch.setattr(runner, "MAX_INLINE_BYTES", 4)

    with pytest.raises(ValueError, match="limit 4"):
        runner.materialize_airflow_definitions({"dag.py": "ééé"})
