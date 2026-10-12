"""Tests for the MCP ``profile`` command: argv dispatch, report readback, and the missing-extra path.

Skipped where the optional ``mcp`` dependency is absent.
"""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import pytest

pytest.importorskip("mcp")

from flowx.mcp import runner, server  # noqa: E402

_INSTALL_HINT = "Install it with:  pip install 'azure-identity>=1.17'"


class _FakeResult:
    def __init__(self, *, returncode: int = 0, stderr: str = "") -> None:
        self.returncode = returncode
        self.ok = returncode == 0
        self.stdout = ""
        self.stderr = stderr

    def as_dict(self) -> dict[str, object]:
        return {"ok": self.ok, "returncode": self.returncode, "stdout": "", "stderr": self.stderr}


@pytest.fixture
def stub_adapter(monkeypatch):
    """Replaces the adapter subprocess with a configurable fake; records each argv."""
    state: dict[str, object] = {"calls": [], "result": _FakeResult()}

    def fake_run_adapter(args, **_kwargs):
        state["calls"].append([str(a) for a in args])
        return state["result"]

    monkeypatch.setattr(runner, "run_adapter", fake_run_adapter)
    return state


def test_profile_is_a_registered_command():
    assert "profile" in server._COMMANDS


def test_profile_forwards_source_and_scope_flags(stub_adapter, tmp_path: Path):
    server._cmd_profile(
        {
            "source": "adf",
            "output_dir": str(tmp_path),
            "days": 30,
            "subscription_id": "sub",
            "resource_group": "rg",
            "factory_name": "f",
        }
    )
    assert stub_adapter["calls"] == [
        [
            "profile",
            "--source",
            "adf",
            "--output-dir",
            str(tmp_path),
            "--days",
            "30",
            "--subscription-id",
            "sub",
            "--resource-group",
            "rg",
            "--factory-name",
            "f",
        ]
    ]


def test_profile_returns_the_report_on_success(stub_adapter, tmp_path: Path):
    tco = tmp_path / "metadata" / "tco"
    tco.mkdir(parents=True)
    (tco / "cost_comparison.md").write_text("# ADF cost")
    result = server._cmd_profile({"source": "adf", "output_dir": str(tmp_path)})
    assert result["ok"] is True
    assert result["report_md"] == "# ADF cost"


def test_profile_missing_extra_returns_exit_2_and_install_hint(stub_adapter, tmp_path: Path):
    # A stale report from an earlier run must not be returned alongside the failure.
    tco = tmp_path / "metadata" / "tco"
    tco.mkdir(parents=True)
    (tco / "cost_comparison.md").write_text("stale")
    stub_adapter["result"] = _FakeResult(returncode=2, stderr=_INSTALL_HINT)
    result = server._cmd_profile({"source": "adf", "output_dir": str(tmp_path)})
    assert result["ok"] is False
    assert result["process"]["returncode"] == 2
    assert _INSTALL_HINT in result["process"]["stderr"]
    assert "report_md" not in result


def test_profile_requires_source(stub_adapter):
    with pytest.raises(KeyError):
        server._cmd_profile({})
    assert stub_adapter["calls"] == []


def test_importing_mcp_server_does_not_import_azure():
    probe = "import sys, flowx.mcp.server; print(any(m.split('.')[0] == 'azure' for m in sys.modules))"
    completed = subprocess.run([sys.executable, "-c", probe], capture_output=True, text=True, check=True)
    assert completed.stdout.strip() == "False"
