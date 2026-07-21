"""Tests that the MCP dispatcher threads --source to the adapter for both sources.

The adapter requires --source for discover/convert, so every MCP command must pass it.
"""

from __future__ import annotations

from pathlib import Path

import pytest

pytest.importorskip("mcp")

from flowx.mcp import runner, server  # noqa: E402


class _FakeResult:
    ok = True
    stdout = ""
    stderr = ""
    returncode = 0

    def as_dict(self) -> dict[str, object]:
        return {"returncode": 0, "stdout": "", "stderr": ""}


@pytest.fixture
def captured(monkeypatch):
    """Records the full argv of every adapter invocation."""
    calls: list[list[str]] = []

    def fake_run_adapter(args, **_kwargs):
        calls.append([str(a) for a in args])
        return _FakeResult()

    monkeypatch.setattr(runner, "run_adapter", fake_run_adapter)
    monkeypatch.setattr(runner, "summarize_inventory", lambda out: {})
    monkeypatch.setattr(runner, "summarize_translation", lambda out: {})
    return calls


def _argv(calls: list[list[str]], subcommand: str) -> list[str]:
    return next(argv for argv in calls if argv and argv[0] == subcommand)


def test_discover_defaults_to_adf_source(captured, tmp_path: Path):
    server._cmd_discover({"adf_source_path": str(tmp_path), "output_dir": str(tmp_path / "o")})
    argv = _argv(captured, "discover")
    assert "--source" in argv and argv[argv.index("--source") + 1] == "adf"
    assert "--source-path" in argv


def test_discover_routes_airflow_source(captured, tmp_path: Path):
    server._cmd_discover({"source": "airflow", "airflow_source_path": str(tmp_path), "output_dir": str(tmp_path / "o")})
    argv = _argv(captured, "discover")
    assert argv[argv.index("--source") + 1] == "airflow"
    assert argv[argv.index("--source-path") + 1] == str(tmp_path)


def test_convert_threads_source(captured, tmp_path: Path):
    server._cmd_convert({"source": "airflow", "airflow_source_path": str(tmp_path), "output_dir": str(tmp_path)})
    argv = _argv(captured, "convert")
    assert argv[argv.index("--source") + 1] == "airflow"


def test_merge_agentic_threads_source(captured):
    server._cmd_merge_agentic(
        {
            "source": "adf",
            "report_path": "/tmp/report.json",
            "agentic_results_dir": "/tmp/results",
        }
    )
    argv = _argv(captured, "convert")
    assert argv[argv.index("--source") + 1] == "adf"


def test_inputs_threads_source(captured):
    server._cmd_inputs({"phase": "discover", "source": "airflow"})
    argv = _argv(captured, "inputs")
    assert argv[argv.index("--source") + 1] == "airflow"


def test_discover_missing_source_path_errors_clearly(captured, tmp_path: Path):
    result = server._cmd_discover({"source": "airflow", "output_dir": str(tmp_path)})
    assert result["ok"] is False
    assert "airflow" in result["error"]
