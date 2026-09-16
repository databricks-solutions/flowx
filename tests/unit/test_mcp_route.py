"""Tests that the reshaped MCP ``route`` / ``fill_agentic`` commands forward to the adapter CLI.

``route`` is one command: no plan => the adapter emits the recommendation; a plan (inline or path)
=> the adapter records + edits via ``--plan-path``. ``fill_agentic`` performs the cross-pipeline
combine fill.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

pytest.importorskip("mcp")

from flowx.mcp import runner, server  # noqa: E402


class _Result:
    ok = True
    returncode = 0

    def __init__(self, stdout: str) -> None:
        self.stdout = stdout
        self.stderr = ""

    def as_dict(self) -> dict[str, Any]:
        return {"returncode": 0, "stdout": self.stdout, "stderr": ""}


@pytest.fixture
def captured(monkeypatch):
    calls: list[list[str]] = []

    def fake_run_adapter(args, **_kwargs):
        calls.append([str(a) for a in args])
        if args and args[0] == "route" and "--plan-path" not in [str(a) for a in args]:
            return _Result(json.dumps({"components": [{"component_id": "component-1"}], "default_plan": {}}))
        return _Result(json.dumps({"ok": True, "violations": [], "components": 1, "edit": {"agentic_pipelines": []}}))

    monkeypatch.setattr(runner, "run_adapter", fake_run_adapter)
    return calls


def test_route_without_a_plan_emits_the_recommendation(captured, tmp_path: Path) -> None:
    out = server._cmd_route({"output_dir": str(tmp_path)})
    assert out["ok"] is True
    assert out["result"]["components"][0]["component_id"] == "component-1"
    argv = captured[0]
    assert argv[0] == "route" and "--plan-path" not in argv
    assert "--output-dir" in argv


def test_route_inline_plan_is_staged_and_forwarded_as_plan_path(captured, tmp_path: Path) -> None:
    plan = {"components": [{"component_id": "component-1", "members": ["a"], "decision": "agentic"}]}
    out = server._cmd_route({"output_dir": str(tmp_path), "plan": plan})
    assert out["ok"] is True
    argv = captured[0]
    assert argv[0] == "route"
    assert "--plan-path" in argv  # inline plan staged to a real temp file


def test_route_forwards_an_explicit_plan_path(captured, tmp_path: Path) -> None:
    plan_path = tmp_path / "plan.json"
    plan_path.write_text("{}", encoding="utf-8")
    server._cmd_route({"output_dir": str(tmp_path), "plan_path": str(plan_path)})
    argv = captured[0]
    assert argv[argv.index("--plan-path") + 1] == str(plan_path)


def test_route_rejects_both_plan_and_plan_path(tmp_path: Path) -> None:
    out = server._cmd_route({"output_dir": str(tmp_path), "plan": {}, "plan_path": "x.json"})
    assert out["ok"] is False and "at most one" in out["error"]


def test_route_forwards_source_so_convert_can_be_triggered(captured, tmp_path: Path) -> None:
    # Parity with the CLI: MCP route forwards source + source-path so the CLI can trigger convert when
    # the report is absent. Recommend path (no plan).
    server._cmd_route({"output_dir": str(tmp_path), "source": "adf", "adf_source_path": "/tmp/adf"})
    argv = captured[0]
    assert argv[argv.index("--source") + 1] == "adf"
    assert argv[argv.index("--source-path") + 1] == "/tmp/adf"


def test_route_forwards_source_on_the_record_path(captured, tmp_path: Path) -> None:
    plan = {"components": [{"component_id": "component-1", "members": ["a"], "decision": "agentic"}]}
    server._cmd_route({"output_dir": str(tmp_path), "plan": plan, "source": "adf", "adf_source_path": "/tmp/adf"})
    argv = captured[0]
    assert "--plan-path" in argv
    assert argv[argv.index("--source") + 1] == "adf"
    assert argv[argv.index("--source-path") + 1] == "/tmp/adf"


def test_route_without_source_forwards_no_source_flags(captured, tmp_path: Path) -> None:
    server._cmd_route({"output_dir": str(tmp_path)})
    assert "--source" not in captured[0]


def test_route_registered_in_command_map() -> None:
    assert "route" in server._COMMANDS


# --------------------------------------------------------------------------- #
# fill_agentic (combine).
# --------------------------------------------------------------------------- #


@pytest.fixture
def captured_fill(monkeypatch):
    calls: list[list[str]] = []

    def fake_run_adapter(args, **_kwargs):
        calls.append([str(a) for a in args])
        return _Result(json.dumps({"ok": True, "violations": [], "pipelines": 1}))

    monkeypatch.setattr(runner, "run_adapter", fake_run_adapter)
    return calls


def test_fill_agentic_inline_pipelines_are_staged(captured_fill, tmp_path: Path) -> None:
    out = server._cmd_fill_agentic(
        {"output_dir": str(tmp_path), "members": ["parent", "child"], "pipelines": [{"name": "lfc", "tasks": []}]}
    )
    assert out["ok"] is True
    argv = captured_fill[0]
    assert argv[0] == "fill-agentic" and argv[1] == "combine"
    assert argv[argv.index("--members") + 1] == "parent,child"
    assert "--pipelines-path" in argv


def test_fill_agentic_forwards_members_string_and_path(captured_fill, tmp_path: Path) -> None:
    pipelines_path = tmp_path / "authored.json"
    pipelines_path.write_text("[]", encoding="utf-8")
    server._cmd_fill_agentic({"output_dir": str(tmp_path), "members": "a,b", "pipelines_path": str(pipelines_path)})
    argv = captured_fill[0]
    assert argv[argv.index("--members") + 1] == "a,b"
    assert argv[argv.index("--pipelines-path") + 1] == str(pipelines_path)


def test_fill_agentic_requires_members(tmp_path: Path) -> None:
    out = server._cmd_fill_agentic({"output_dir": str(tmp_path), "pipelines": []})
    assert out["ok"] is False and "members" in out["error"]


def test_fill_agentic_requires_exactly_one_pipelines_source(tmp_path: Path) -> None:
    both = server._cmd_fill_agentic(
        {"output_dir": str(tmp_path), "members": ["a"], "pipelines": [], "pipelines_path": "x.json"}
    )
    neither = server._cmd_fill_agentic({"output_dir": str(tmp_path), "members": ["a"]})
    assert both["ok"] is False and "exactly one" in both["error"]
    assert neither["ok"] is False and "exactly one" in neither["error"]


def test_fill_agentic_registered_in_command_map() -> None:
    assert "fill_agentic" in server._COMMANDS
