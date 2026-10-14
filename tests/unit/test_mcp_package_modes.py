"""Tests that the MCP ``package`` and ``migrate`` commands honour the bundle packaging mode.

The CLI's ``package --packaging-mode/--group-by/--group-spec`` decides how a multi-pipeline migration
is laid out into bundles. These tests package the same converted state through MCP and through the
CLI and check both write the same files, and that a bad value is refused the same way. Skipped where
the optional ``mcp`` dependency is absent.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

pytest.importorskip("mcp")

from flowx.bundler.dab_writer import main as package_main  # noqa: E402
from flowx.mcp import runner, server  # noqa: E402


def _write_converted_state(output_dir: Path) -> None:
    """Writes a translation report with three pipelines: 'caller' runs 'callee'; 'solo' stands alone.

    The three modes lay this out differently: three bundles (per-pipeline), one bundle (single), and
    two bundles (per-group, since caller and callee are connected).
    """
    work = output_dir / ".work"
    work.mkdir(parents=True)
    wait = {"type": "WaitActivity", "name": "Pause", "task_key": "pause", "wait_time_seconds": 5}
    report = {
        "translations": [
            {
                "pipeline": "caller",
                "status": "translated",
                "ir": {
                    "type": "ExecutePipelineActivity",
                    "name": "Run Callee",
                    "task_key": "run_callee",
                    "pipeline_name": "callee",
                },
            },
            {"pipeline": "callee", "status": "translated", "ir": wait},
            {"pipeline": "solo", "status": "translated", "ir": wait},
        ]
    }
    (work / "translation_report.json").write_text(json.dumps(report))


def _tree(root: Path) -> dict[str, str]:
    """Every file under *root* as ``{relative path: text}``."""
    return {str(path.relative_to(root)): path.read_text() for path in sorted(root.rglob("*")) if path.is_file()}


def _cli_flags(parameters: dict[str, str]) -> list[str]:
    flags: list[str] = []
    for key, value in parameters.items():
        flags += [f"--{key.replace('_', '-')}", value]
    return flags


def _package_both_ways(tmp_path: Path, parameters: dict[str, str]) -> tuple[dict[str, Any], Path, Path]:
    """Packages identical converted state via MCP and via the CLI; returns the MCP result and both dirs."""
    mcp_dir, cli_dir = tmp_path / "mcp", tmp_path / "cli"
    _write_converted_state(mcp_dir)
    _write_converted_state(cli_dir)
    result = server._cmd_package({"output_dir": str(mcp_dir), "download_workspace_files": False, **parameters})
    exit_code = package_main(["--output-dir", str(cli_dir), "--no-download-workspace-files", *_cli_flags(parameters)])
    assert exit_code == 0
    return result, mcp_dir, cli_dir


@pytest.fixture
def no_bundle_upload(monkeypatch):
    """Keeps the MCP result small: the bundle contents are compared on disk, not inline."""
    monkeypatch.setattr(runner, "read_tree", lambda out: {"files": {}, "truncated": []})


@pytest.mark.parametrize(
    ("parameters", "expected_bundle_files"),
    [
        ({}, {"caller/databricks.yml", "callee/databricks.yml", "solo/databricks.yml"}),
        ({"packaging_mode": "per-pipeline"}, {"caller/databricks.yml", "callee/databricks.yml", "solo/databricks.yml"}),
        ({"packaging_mode": "single"}, {"databricks.yml"}),
        ({"packaging_mode": "per-group"}, {"callee/databricks.yml", "solo/databricks.yml"}),
        ({"packaging_mode": "per-group", "group_by": "inferred"}, {"callee/databricks.yml", "solo/databricks.yml"}),
    ],
)
def test_mcp_package_writes_the_cli_layout(no_bundle_upload, tmp_path: Path, parameters, expected_bundle_files):
    result, mcp_dir, cli_dir = _package_both_ways(tmp_path, parameters)

    assert result["ok"], result["process"]
    assert _tree(mcp_dir) == _tree(cli_dir)
    assert {name for name in _tree(mcp_dir) if name.endswith("databricks.yml")} == expected_bundle_files


def test_mcp_package_honours_an_explicit_group_spec(no_bundle_upload, tmp_path: Path):
    spec_path = tmp_path / "groups.json"
    spec_path.write_text(json.dumps({"team_a": ["caller", "solo"], "team_b": ["callee"]}))

    result, mcp_dir, cli_dir = _package_both_ways(
        tmp_path, {"packaging_mode": "per-group", "group_by": "spec", "group_spec": str(spec_path)}
    )

    assert result["ok"], result["process"]
    assert _tree(mcp_dir) == _tree(cli_dir)
    assert (mcp_dir / "team_a" / "databricks.yml").exists()
    assert (mcp_dir / "team_b" / "databricks.yml").exists()


@pytest.mark.parametrize("parameters", [{"packaging_mode": "per-group"}, {}])
def test_mcp_package_uses_a_group_spec_sent_without_group_by(no_bundle_upload, tmp_path: Path, parameters):
    spec_path = tmp_path / "groups.json"
    spec_path.write_text(json.dumps({"team_a": ["caller", "solo"], "team_b": ["callee"]}))
    mcp_dir, cli_dir = tmp_path / "mcp", tmp_path / "cli"
    _write_converted_state(mcp_dir)
    _write_converted_state(cli_dir)

    result = server._cmd_package(
        {"output_dir": str(mcp_dir), "download_workspace_files": False, "group_spec": str(spec_path), **parameters}
    )
    exit_code = package_main(
        [
            "--output-dir",
            str(cli_dir),
            "--no-download-workspace-files",
            *_cli_flags({"packaging_mode": "per-group", "group_by": "spec", "group_spec": str(spec_path)}),
        ]
    )

    assert exit_code == 0
    assert result["ok"], result["process"]
    assert _tree(mcp_dir) == _tree(cli_dir)
    assert {name for name in _tree(mcp_dir) if name.endswith("databricks.yml")} == {
        "team_a/databricks.yml",
        "team_b/databricks.yml",
    }


@pytest.mark.parametrize(
    "parameters",
    [{"packaging_mode": "bogus"}, {"packaging_mode": "per-group", "group_by": "bogus"}],
)
def test_mcp_package_refuses_a_bad_value_like_the_cli(no_bundle_upload, tmp_path: Path, capsys, parameters):
    mcp_dir, cli_dir = tmp_path / "mcp", tmp_path / "cli"
    _write_converted_state(mcp_dir)
    _write_converted_state(cli_dir)

    mcp_result = server._cmd_package({"output_dir": str(mcp_dir), **parameters})
    with pytest.raises(SystemExit) as cli_exit:
        package_main(["--output-dir", str(cli_dir), *_cli_flags(parameters)])

    assert mcp_result["ok"] is False
    assert mcp_result["process"]["returncode"] == cli_exit.value.code == 2
    assert "invalid choice: 'bogus'" in mcp_result["process"]["stderr"]
    assert "invalid choice: 'bogus'" in capsys.readouterr().err
    assert not (mcp_dir / "databricks.yml").exists() and not list(mcp_dir.glob("*/databricks.yml"))


class _FakeResult:
    ok = True
    stdout = ""
    stderr = ""
    returncode = 0

    def as_dict(self) -> dict[str, object]:
        return {"returncode": 0, "stdout": "", "stderr": ""}


@pytest.mark.parametrize(
    ("parameters", "expected_flags"),
    [
        ({}, []),
        ({"packaging_mode": "single"}, ["--packaging-mode", "single"]),
        (
            {"packaging_mode": "per-group", "group_by": "spec", "group_spec": "/tmp/groups.json"},
            ["--packaging-mode", "per-group", "--group-by", "spec", "--group-spec", "/tmp/groups.json"],
        ),
        (
            {"group_spec": "/tmp/groups.json"},
            ["--packaging-mode", "per-group", "--group-by", "spec", "--group-spec", "/tmp/groups.json"],
        ),
    ],
)
def test_migrate_forwards_the_packaging_mode(monkeypatch, tmp_path: Path, parameters, expected_flags):
    calls: list[list[str]] = []

    def fake_run_adapter(args, **_kwargs):
        calls.append([str(arg) for arg in args])
        return _FakeResult()

    monkeypatch.setattr(runner, "run_adapter", fake_run_adapter)
    monkeypatch.setattr(runner, "summarize_inventory", lambda out: {})
    monkeypatch.setattr(runner, "summarize_translation", lambda out: {})
    monkeypatch.setattr(runner, "list_tree", lambda out: [])
    monkeypatch.setattr(runner, "read_tree", lambda out: {"files": {}, "truncated": []})

    result = server._cmd_migrate(
        {
            "source": "adf",
            "adf_source_path": str(tmp_path / "adf"),
            "output_dir": str(tmp_path / "out"),
            "interactive": False,
            **parameters,
        }
    )

    assert result["status"] == "completed"
    package_argv = next(argv for argv in calls if argv[0] == "package")
    assert package_argv[package_argv.index("--schema") + 2 :] == expected_flags
