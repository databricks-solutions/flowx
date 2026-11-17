"""Tests for the MCP ``profile`` command: argv dispatch, report readback, and the missing-extra path.

Skipped where the optional ``mcp`` dependency is absent.
"""

from __future__ import annotations

import os
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
    (tco / "cost_comparison.csv").write_text("level,factory,est_total\n")
    result = server._cmd_profile({"source": "adf", "output_dir": str(tmp_path)})
    assert result["ok"] is True
    assert result["report_md"] == "# ADF cost"
    assert result["report_csv"] == "level,factory,est_total\n"


@pytest.mark.parametrize(
    ("written", "missing"),
    [
        (["cost_comparison.csv"], ["cost_comparison.md"]),
        (["cost_comparison.md"], ["cost_comparison.csv"]),
        ([], ["cost_comparison.md", "cost_comparison.csv"]),
    ],
)
def test_profile_fails_when_a_report_is_missing_after_exit_0(
    stub_adapter, tmp_path: Path, written: list[str], missing: list[str]
):
    tco = tmp_path / "metadata" / "tco"
    tco.mkdir(parents=True)
    for name in written:
        (tco / name).write_text("report")
    result = server._cmd_profile({"source": "adf", "output_dir": str(tmp_path)})
    assert result["ok"] is False
    assert result["process"]["returncode"] == 0
    assert all(str(tco / name) in result["error"] for name in missing)
    assert not any(str(tco / name) in result["error"] for name in written)
    assert "report_md" not in result
    assert "report_csv" not in result


def test_profile_missing_extra_returns_exit_2_and_install_hint(stub_adapter, tmp_path: Path):
    # A stale report from an earlier run must not be returned alongside the failure.
    tco = tmp_path / "metadata" / "tco"
    tco.mkdir(parents=True)
    (tco / "cost_comparison.md").write_text("stale")
    (tco / "cost_comparison.csv").write_text("stale")
    stub_adapter["result"] = _FakeResult(returncode=2, stderr=_INSTALL_HINT)
    result = server._cmd_profile({"source": "adf", "output_dir": str(tmp_path)})
    assert result["ok"] is False
    assert result["process"]["returncode"] == 2
    assert _INSTALL_HINT in result["process"]["stderr"]
    assert "report_md" not in result
    assert "report_csv" not in result


_AZURE_BLOCKER = """
import sys


class _BlockAzure:
    def find_spec(self, name, path=None, target=None):
        if name == "azure" or name.startswith("azure."):
            raise ModuleNotFoundError(f"No module named {name!r}", name=name)
        return None


sys.meta_path.insert(0, _BlockAzure())
"""


def test_profile_without_azure_installed_exits_2_with_the_install_hint(tmp_path: Path, monkeypatch):
    blocker = tmp_path / "blocker"
    blocker.mkdir()
    (blocker / "sitecustomize.py").write_text(_AZURE_BLOCKER)
    src = Path(__file__).parents[2] / "src"
    monkeypatch.setenv("PYTHONPATH", os.pathsep.join([str(blocker), str(src), os.environ.get("PYTHONPATH", "")]))
    result = server._cmd_profile({"source": "adf", "output_dir": str(tmp_path / "out")})
    assert result["ok"] is False
    assert result["process"]["returncode"] == 2
    assert _INSTALL_HINT in result["process"]["stderr"]
    assert "Traceback" not in result["process"]["stderr"]
    assert "report_md" not in result


def test_run_adapter_returns_a_failure_on_timeout(monkeypatch):
    def fake_run(command, **kwargs):
        raise subprocess.TimeoutExpired(command, kwargs["timeout"], output=b"partial out", stderr=b"partial err")

    monkeypatch.setattr(subprocess, "run", fake_run)
    result = runner.run_adapter(["profile"], timeout=5)
    assert result.ok is False
    assert result.returncode is None
    assert result.stdout == "partial out"
    assert result.stderr == "partial err\nTimed out after 5 s (FLOWX_MCP_TIMEOUT)."
    assert result.as_dict()["returncode"] is None


def test_run_adapter_timeout_without_output(monkeypatch):
    def fake_run(command, **kwargs):
        raise subprocess.TimeoutExpired(command, kwargs["timeout"])

    monkeypatch.setattr(subprocess, "run", fake_run)
    result = runner.run_adapter(["inspect"], timeout=7)
    assert result.stdout == ""
    assert result.stderr == "Timed out after 7 s (FLOWX_MCP_TIMEOUT)."


def test_profile_timeout_returns_the_hint_and_no_report(monkeypatch, tmp_path: Path):
    tco = tmp_path / "metadata" / "tco"
    tco.mkdir(parents=True)
    (tco / "cost_comparison.md").write_text("stale")
    (tco / "cost_comparison.csv").write_text("stale")

    def fake_run(command, **kwargs):
        raise subprocess.TimeoutExpired(command, kwargs["timeout"])

    monkeypatch.setattr(subprocess, "run", fake_run)
    result = server._cmd_profile({"source": "adf", "output_dir": str(tmp_path)})
    assert result["ok"] is False
    assert result["process"]["returncode"] is None
    assert "Timed out after" in result["process"]["stderr"]
    assert "resource_group" in result["process"]["stderr"]
    assert "notebook" in result["process"]["stderr"]
    assert "report_md" not in result
    assert "report_csv" not in result


def test_profile_requires_source(stub_adapter):
    with pytest.raises(KeyError):
        server._cmd_profile({})
    assert stub_adapter["calls"] == []


def test_importing_mcp_server_does_not_import_azure():
    probe = "import sys, flowx.mcp.server; print(any(m.split('.')[0] == 'azure' for m in sys.modules))"
    completed = subprocess.run([sys.executable, "-c", probe], capture_output=True, text=True, check=True)
    assert completed.stdout.strip() == "False"
