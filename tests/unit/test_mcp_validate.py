"""Tests for the MCP ``validate`` command."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

pytest.importorskip("mcp")

from flowx.mcp import runner, server  # noqa: E402


def test_mcp_validate_stages_inline_reviews_for_the_cli(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[list[str]] = []

    def fake_run_adapter(args: list[Any]) -> runner.AdapterResult:
        arguments = [str(arg) for arg in args]
        calls.append(arguments)
        staged = json.loads(Path(arguments[arguments.index("--reviews-path") + 1]).read_text())
        return runner.AdapterResult(
            command=arguments, returncode=0, stdout=json.dumps({"ok": True, **staged}), stderr=""
        )

    monkeypatch.setattr(runner, "run_adapter", fake_run_adapter)
    result = server._cmd_validate({"action": "record", "output_dir": "out", "reviews": {"reviewer": "m"}})
    assert result["ok"] is True
    assert result["result"]["reviewer"] == "m"
    assert calls[0][:4] == ["validate", "record", "--output-dir", "out"]
    assert server._cmd_validate({"action": "record"}) == {"ok": False, "error": "validate record requires 'reviews'."}
