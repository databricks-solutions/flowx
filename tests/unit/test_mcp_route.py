"""Tests that the MCP ``route`` command forwards to the adapter CLI correctly (routing, #77)."""

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
        if args and str(args[1]) == "recommend":
            return _Result(json.dumps({"components": [{"component_id": "component-1"}], "default_plan": {}}))
        return _Result(json.dumps({"ok": True, "violations": [], "components": 1}))

    monkeypatch.setattr(runner, "run_adapter", fake_run_adapter)
    return calls


def test_route_recommend_forwards_to_adapter(captured, tmp_path: Path) -> None:
    out = server._cmd_route({"output_dir": str(tmp_path), "action": "recommend"})
    assert out["ok"] is True
    assert out["result"]["components"][0]["component_id"] == "component-1"
    argv = captured[0]
    assert argv[0] == "route" and argv[1] == "recommend"
    assert "--output-dir" in argv


def test_route_defaults_to_recommend(captured, tmp_path: Path) -> None:
    server._cmd_route({"output_dir": str(tmp_path)})
    assert captured[0][1] == "recommend"


def test_route_record_inline_plan_is_staged_to_a_file(captured, tmp_path: Path) -> None:
    plan = {"components": [{"component_id": "component-1", "members": ["a"], "decision": "deterministic"}]}
    out = server._cmd_route({"output_dir": str(tmp_path), "action": "record", "plan": plan})
    assert out["ok"] is True
    argv = captured[0]
    assert argv[0] == "route" and argv[1] == "record"
    # The handler forwarded a real --plan-path (the staged temp file) to the CLI.
    assert "--plan-path" in argv


def test_route_record_forwards_plan_path(captured, tmp_path: Path) -> None:
    plan_path = tmp_path / "plan.json"
    plan_path.write_text("{}", encoding="utf-8")
    server._cmd_route({"output_dir": str(tmp_path), "action": "record", "plan_path": str(plan_path)})
    argv = captured[0]
    assert argv[1] == "record"
    assert argv[argv.index("--plan-path") + 1] == str(plan_path)


def test_route_record_requires_a_plan(tmp_path: Path) -> None:
    both = server._cmd_route({"output_dir": str(tmp_path), "action": "record", "plan": {}, "plan_path": "x.json"})
    neither = server._cmd_route({"output_dir": str(tmp_path), "action": "record"})
    assert both["ok"] is False and "exactly one" in both["error"]
    assert neither["ok"] is False and "exactly one" in neither["error"]


def test_route_rejects_unknown_action(tmp_path: Path) -> None:
    out = server._cmd_route({"output_dir": str(tmp_path), "action": "sideways"})
    assert out["ok"] is False and "action" in out["error"]


def test_route_registered_in_command_map() -> None:
    assert "route" in server._COMMANDS
