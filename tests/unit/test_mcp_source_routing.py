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


def test_discover_threads_explicit_adf_source(captured, tmp_path: Path):
    server._cmd_discover({"source": "adf", "adf_source_path": str(tmp_path), "output_dir": str(tmp_path / "o")})
    argv = _argv(captured, "discover")
    assert "--source" in argv and argv[argv.index("--source") + 1] == "adf"
    assert "--source-path" in argv


def test_discover_requires_source(tmp_path: Path):
    # No default source: a command with no 'source' raises KeyError, which the dispatcher (below)
    # converts into a clear "Missing required parameter 'source'" error instead of assuming adf.
    with pytest.raises(KeyError):
        server._cmd_discover({"adf_source_path": str(tmp_path), "output_dir": str(tmp_path / "o")})


def test_dispatcher_reports_missing_source_clearly(tmp_path: Path):
    handler = server._COMMANDS["discover"]
    try:
        result = handler({"adf_source_path": str(tmp_path), "output_dir": str(tmp_path / "o")})
    except KeyError as missing:
        result = {"ok": False, "error": f"Missing required parameter {missing} for command 'discover'."}
    assert result["ok"] is False
    assert "source" in result["error"].lower()


def test_discover_routes_airflow_source(captured, tmp_path: Path):
    server._cmd_discover({"source": "airflow", "airflow_source_path": str(tmp_path), "output_dir": str(tmp_path / "o")})
    argv = _argv(captured, "discover")
    assert argv[argv.index("--source") + 1] == "airflow"
    assert argv[argv.index("--source-path") + 1] == str(tmp_path)


def test_airflow_exclusions_are_forwarded_as_repeatable_flags(captured, tmp_path: Path):
    parameters = {
        "source": "airflow",
        "airflow_source_path": str(tmp_path),
        "output_dir": str(tmp_path / "o"),
        "exclude_dag": ["legacy", "experimental"],
    }

    server._cmd_discover(parameters)
    server._cmd_convert(parameters)

    for command in ("discover", "convert"):
        argv = _argv(captured, command)
        exclusions = [argv[index + 1] for index, value in enumerate(argv) if value == "--exclude-dag"]
        assert exclusions == ["legacy", "experimental"]


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


def test_merge_agentic_rejects_airflow_without_invoking_adapter(captured):
    result = server._cmd_merge_agentic(
        {
            "source": "airflow",
            "report_path": "/tmp/report.json",
            "agentic_results_dir": "/tmp/results",
        }
    )

    assert result == {
        "ok": False,
        "error": "Airflow agentic merge is disabled; use the fingerprint-bound resolve_agentic workflow.",
    }
    assert captured == []


def test_resolve_agentic_prepare_routes_airflow_contract(captured):
    result = server._cmd_resolve_agentic(
        {
            "source": "airflow",
            "action": "prepare",
            "airflow_source_path": "/tmp/dags",
            "report_path": "/tmp/out/.work/translation_report.json",
            "output_dir": "/tmp/out",
        }
    )

    argv = _argv(captured, "resolve-agentic")
    assert argv[:4] == ["resolve-agentic", "prepare", "--source", "airflow"]
    assert argv[argv.index("--source-path") + 1] == "/tmp/dags"
    assert argv[argv.index("--report") + 1] == "/tmp/out/.work/translation_report.json"
    assert result["ok"] is True


def test_resolve_agentic_stage_materializes_inline_candidate(captured):
    result = server._cmd_resolve_agentic(
        {
            "source": "airflow",
            "action": "stage",
            "output_dir": "/tmp/out",
            "candidates": [{"gap_id": "abc"}],
        }
    )

    argv = _argv(captured, "resolve-agentic")
    assert "--candidate" in argv
    assert result["ok"] is True


def test_resolve_agentic_rejects_adf_without_invoking_adapter(captured):
    result = server._cmd_resolve_agentic({"source": "adf", "action": "prepare", "output_dir": "/tmp/out"})

    assert result == {
        "ok": False,
        "error": "resolve_agentic is not enabled for ADF; ADF uses the legacy merge path.",
    }
    assert captured == []


def test_inputs_threads_source(captured):
    server._cmd_inputs({"phase": "discover", "source": "airflow"})
    argv = _argv(captured, "inputs")
    assert argv[argv.index("--source") + 1] == "airflow"


def test_inputs_package_is_source_independent(captured):
    # package prompts don't vary by source, so `inputs package` must not require (or pass) --source.
    server._cmd_inputs({"phase": "package"})
    argv = _argv(captured, "inputs")
    assert "--source" not in argv


def test_workspace_paths_forwards_airflow_source_path(captured, tmp_path: Path):
    server._cmd_workspace_paths(
        {"source": "airflow", "report_path": "/tmp/report.json", "airflow_source_path": str(tmp_path)}
    )
    argv = _argv(captured, "workspace-paths")
    assert argv[argv.index("--source") + 1] == "airflow"
    assert argv[argv.index("--source-dir") + 1] == str(tmp_path)


def test_discover_missing_source_path_errors_clearly(captured, tmp_path: Path):
    result = server._cmd_discover({"source": "airflow", "output_dir": str(tmp_path)})
    assert result["ok"] is False
    assert "airflow" in result["error"]


def test_source_name_rejects_non_string(captured):
    # A malformed non-string source must raise (ValueError), not silently coerce 123 -> "123".
    with pytest.raises(ValueError, match="must be a string"):
        server._source_name({"source": 123})
