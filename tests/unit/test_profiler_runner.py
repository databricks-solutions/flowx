"""The runner wires credential -> scan -> report and returns an exit code."""

from __future__ import annotations

from pathlib import Path

import pytest

from flowx.sources.adf.profiler import runner
from flowx.sources.adf.profiler.models import PipelineCost, ProfileResult


def _scanner_returning(result: ProfileResult, seen: dict | None = None):
    class FakeScanner:
        def __init__(self, **kwargs):
            if seen is not None:
                seen["init"] = kwargs

        def scan(self, **kwargs):
            if seen is not None:
                seen["scan"] = kwargs
            return result

    return FakeScanner


@pytest.fixture(autouse=True)
def fake_credential(monkeypatch):
    calls: list[dict] = []

    def fake_get_credential(**kwargs):
        calls.append(kwargs)
        return object()

    monkeypatch.setattr(runner, "get_credential", fake_get_credential)
    return calls


def test_main_runs_scan_and_writes_report(tmp_path, monkeypatch, capsys):
    result = ProfileResult(
        region="eastus",
        pricing_source="test",
        days=90,
        total_pipeline_runs=1,
        pipeline_costs=[
            PipelineCost(
                factory_name="f",
                pipeline_name="p",
                total_runs=1,
                orchestration_cost=0.001,
                data_movement_cost=0.0,
                pipeline_activity_cost=0.0,
                external_cost=0.0,
                total_cost=0.001,
            )
        ],
    )
    monkeypatch.setattr(runner, "AdfScanner", _scanner_returning(result))

    code = runner.main(["--output-dir", str(tmp_path), "--days", "90"])
    assert code == 0
    report_path = Path(tmp_path) / "metadata" / "tco" / "cost_comparison.md"
    assert report_path.exists()
    assert str(report_path) in capsys.readouterr().out


def test_main_forwards_filters_and_credentials(tmp_path, monkeypatch, fake_credential):
    seen: dict = {}
    empty = ProfileResult(region="eastus", pricing_source="test", days=7, total_pipeline_runs=0)
    monkeypatch.setattr(runner, "AdfScanner", _scanner_returning(empty, seen))
    argv = ["--output-dir", str(tmp_path), "--days", "7", "--subscription-id", "SUB", "--resource-group", "RG"]
    argv += ["--factory-name", "f", "--tenant-id", "t", "--client-id", "c", "--client-secret", "s"]
    assert runner.main(argv) == 0
    assert fake_credential == [{"tenant_id": "t", "client_id": "c", "client_secret": "s"}]
    assert seen["init"]["days"] == 7
    assert seen["scan"] == {"subscription_id": "SUB", "resource_group": "RG", "factory_name": "f"}


def test_main_accepts_and_ignores_source_dir(tmp_path, monkeypatch):
    empty = ProfileResult(region="eastus", pricing_source="test", days=90, total_pipeline_runs=0)
    monkeypatch.setattr(runner, "AdfScanner", _scanner_returning(empty))
    assert runner.main(["--source-dir", "/ignored", "--output-dir", str(tmp_path)]) == 0


def test_main_does_not_pass_pricing_to_scanner(tmp_path, monkeypatch):
    seen: dict = {}
    empty = ProfileResult(region="westeurope", pricing_source="default list rates", days=90, total_pipeline_runs=0)
    monkeypatch.setattr(runner, "AdfScanner", _scanner_returning(empty, seen))
    runner.main(["--output-dir", str(tmp_path)])
    assert seen["init"].get("pricing") is None


def test_main_prints_permission_warnings(tmp_path, monkeypatch, capsys):
    result = ProfileResult(
        region="eastus",
        pricing_source="test",
        days=90,
        total_pipeline_runs=0,
        permission_warnings=["f: Cannot query pipeline runs (need Data Factory Contributor)"],
    )
    monkeypatch.setattr(runner, "AdfScanner", _scanner_returning(result))
    assert runner.main(["--output-dir", str(tmp_path)]) == 0
    assert "Data Factory Contributor" in capsys.readouterr().err


def test_main_reports_auth_failure_with_setup_guidance(tmp_path, monkeypatch, capsys):
    class FailingScanner:
        def __init__(self, **_kwargs):
            pass

        def scan(self, **_kwargs):
            raise runner.ProfileAuthenticationError("no credential found")

    monkeypatch.setattr(runner, "AdfScanner", FailingScanner)
    code = runner.main(["--output-dir", str(tmp_path)])
    assert code == 2
    message = capsys.readouterr().err
    assert "az login" in message
    assert "service principal" in message.lower()
    assert not (tmp_path / "metadata" / "tco").exists()


def test_main_reports_missing_extra(tmp_path, monkeypatch, capsys):
    def missing(**_kwargs):
        raise runner.MissingProfileDependencyError("Install it with:  pip install -e '.[profile]'")

    monkeypatch.setattr(runner, "get_credential", missing)
    assert runner.main(["--output-dir", str(tmp_path)]) == 2
    assert ".[profile]" in capsys.readouterr().err


def test_main_turns_unexpected_errors_into_one_line_guidance(tmp_path, monkeypatch, capsys):
    class UnreachableScanner:
        def __init__(self, **_kwargs):
            pass

        def scan(self, **_kwargs):
            raise ConnectionError("Failed to establish a new connection: management.azure.com")

    monkeypatch.setattr(runner, "AdfScanner", UnreachableScanner)
    assert runner.main(["--output-dir", str(tmp_path)]) == 1
    message = capsys.readouterr().err
    assert "management.azure.com" in message
    assert "run the profile locally" in message
    assert "Traceback" not in message
