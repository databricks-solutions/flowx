"""Tests that the MCP ``package`` and ``migrate`` commands honour the bundle packaging mode.

The CLI's ``package --packaging-mode/--group-by/--group-spec`` decides how a multi-pipeline migration
is laid out into bundles. These tests package the same converted state through MCP and through the
CLI and check both write the same files, and that a bad value is refused the same way. Skipped where
the optional ``mcp`` dependency is absent.
"""

from __future__ import annotations

import asyncio
import json
from collections.abc import Callable
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


def _write_airflow_state(output_dir: Path) -> None:
    """Writes a translation report with two Airflow DAGs, which package always puts in one combined bundle."""
    work = output_dir / ".work"
    work.mkdir(parents=True)
    notebook = {
        "name": "extract",
        "task_key": "extract",
        "type": "NotebookActivity",
        "notebook_path": "notebooks/extract.py",
        "generated_source": "# Databricks notebook source\nprint('x')\n",
    }
    report = {
        "pipelines": [
            {
                "name": dag,
                "tags": {"source": "airflow"},
                "reconciliation_status": "verified",
                "audit": {"source_file": f"{dag}.py", "audited_activity_count": 1, "transformations": []},
                "tasks": [notebook],
            }
            for dag in ("extract_dag", "load_dag")
        ]
    }
    (work / "translation_report.json").write_text(json.dumps(report))


def _write_group_spec(directory: Path) -> Path:
    """Writes a spec that groups 'caller' with 'solo' and puts 'callee' on its own."""
    spec_path = directory / "groups.json"
    spec_path.write_text(json.dumps({"team_a": ["caller", "solo"], "team_b": ["callee"]}))
    return spec_path


def _call_flowx(command: str, parameters: dict[str, Any]) -> dict[str, Any]:
    """Calls the ``flowx`` tool through the MCP server, as an agent does, and decodes its JSON reply."""
    (reply,) = asyncio.run(server.build_server().call_tool("flowx", {"command": command, "parameters": parameters}))
    return json.loads(reply.text)


def _inputs_package_defaults() -> dict[str, Any]:
    """The answers a hosted agent starts from: every ``inputs package`` option at its default."""
    return {
        option["option_id"]: option["default"]
        for option in server._cmd_inputs({"phase": "package"})["inputs"]["options"]
    }


def _tree(root: Path) -> dict[str, str]:
    """Every file under *root* as ``{relative path: text}``."""
    return {str(path.relative_to(root)): path.read_text() for path in sorted(root.rglob("*")) if path.is_file()}


def _cli_flags(parameters: dict[str, Any]) -> list[str]:
    flags: list[str] = []
    for key, value in parameters.items():
        flags += [f"--{key.replace('_', '-')}", str(value)]
    return flags


def _package_both_ways(
    tmp_path: Path,
    parameters: dict[str, str],
    cli_parameters: dict[str, str] | None = None,
    *,
    write_state: Callable[[Path], None] = _write_converted_state,
) -> tuple[dict[str, Any], Path, Path]:
    """Packages identical converted state via MCP and via the CLI; returns the MCP result and both dirs.

    The CLI gets *cli_parameters* when given (the flags the MCP call should amount to), else *parameters*.
    """
    mcp_dir, cli_dir = tmp_path / "mcp", tmp_path / "cli"
    write_state(mcp_dir)
    write_state(cli_dir)
    result = server._cmd_package({"output_dir": str(mcp_dir), "download_workspace_files": False, **parameters})
    cli_flags = _cli_flags(parameters if cli_parameters is None else cli_parameters)
    exit_code = package_main(["--output-dir", str(cli_dir), "--no-download-workspace-files", *cli_flags])
    assert exit_code == 0
    return result, mcp_dir, cli_dir


def _has_bundle(root: Path) -> bool:
    return (root / "databricks.yml").exists() or bool(list(root.glob("*/databricks.yml")))


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
    spec_path = _write_group_spec(tmp_path)

    result, mcp_dir, cli_dir = _package_both_ways(
        tmp_path, {"packaging_mode": "per-group", "group_by": "spec", "group_spec": str(spec_path)}
    )

    assert result["ok"], result["process"]
    assert _tree(mcp_dir) == _tree(cli_dir)
    assert (mcp_dir / "team_a" / "databricks.yml").exists()
    assert (mcp_dir / "team_b" / "databricks.yml").exists()


@pytest.mark.parametrize("parameters", [{"packaging_mode": "per-group"}, {"group_by": "spec"}, {}])
def test_mcp_package_uses_a_group_spec_with_its_layout_left_unset(no_bundle_upload, tmp_path: Path, parameters):
    spec_path = _write_group_spec(tmp_path)

    result, mcp_dir, cli_dir = _package_both_ways(
        tmp_path,
        {"group_spec": str(spec_path), **parameters},
        {"packaging_mode": "per-group", "group_by": "spec", "group_spec": str(spec_path)},
    )

    assert result["ok"], result["process"]
    assert _tree(mcp_dir) == _tree(cli_dir)
    assert {name for name in _tree(mcp_dir) if name.endswith("databricks.yml")} == {
        "team_a/databricks.yml",
        "team_b/databricks.yml",
    }


_SPEC_CONFLICTS = [
    ({"packaging_mode": "per-pipeline"}, "packaging_mode 'per-pipeline'"),
    ({"packaging_mode": "single"}, "packaging_mode 'single'"),
    ({"group_by": "inferred"}, "group_by 'inferred'"),
    ({"packaging_mode": "per-group", "group_by": "inferred"}, "group_by 'inferred'"),
    ({"packaging_mode": ""}, "packaging_mode ''"),
]


@pytest.mark.parametrize(("parameters", "conflict"), _SPEC_CONFLICTS)
def test_mcp_package_refuses_a_group_spec_with_a_contradicting_layout(tmp_path: Path, parameters, conflict):
    spec_path = _write_group_spec(tmp_path)
    output_dir = tmp_path / "out"
    _write_converted_state(output_dir)

    result = _call_flowx("package", {"output_dir": str(output_dir), "group_spec": str(spec_path), **parameters})

    assert result["ok"] is False
    assert repr(str(spec_path)) in result["error"] and conflict in result["error"]
    assert not _has_bundle(output_dir)


def test_inputs_package_answers_with_a_spec_package_by_that_spec(no_bundle_upload, tmp_path: Path):
    """The hosted flow: answer ``inputs package`` as its group_spec text says, then package with those answers."""
    answers = _inputs_package_defaults()
    spec_path = _write_group_spec(tmp_path)
    answers.update(packaging_mode="per-group", group_spec=str(spec_path))

    result, mcp_dir, cli_dir = _package_both_ways(
        tmp_path,
        {key: answers[key] for key in ("catalog", "schema", "packaging_mode", "group_spec")},
        {"packaging_mode": "per-group", "group_by": "spec", "group_spec": str(spec_path)},
    )

    assert result["ok"], result["process"]
    assert _tree(mcp_dir) == _tree(cli_dir)
    assert {name for name in _tree(mcp_dir) if name.endswith("databricks.yml")} == {
        "team_a/databricks.yml",
        "team_b/databricks.yml",
    }


def test_inputs_package_default_mode_with_a_spec_is_refused_not_ignored(tmp_path: Path):
    answers = _inputs_package_defaults()
    spec_path = _write_group_spec(tmp_path)
    output_dir = tmp_path / "out"
    _write_converted_state(output_dir)

    result = _call_flowx(
        "package",
        {"output_dir": str(output_dir), "packaging_mode": answers["packaging_mode"], "group_spec": str(spec_path)},
    )

    assert result["ok"] is False
    assert f"packaging_mode {answers['packaging_mode']!r}" in result["error"]
    assert not _has_bundle(output_dir)


@pytest.mark.parametrize(
    "parameters",
    [
        {"packaging_mode": "per-pipeline"},
        {"packaging_mode": "per-group", "group_by": "inferred"},
        {"group_spec": "/nonexistent/flowx/groups.json"},
    ],
)
def test_mcp_package_writes_one_combined_bundle_for_a_multi_dag_airflow_report(
    no_bundle_upload, tmp_path: Path, parameters
):
    result, mcp_dir, cli_dir = _package_both_ways(
        tmp_path, {"bundle_name": "airflow_suite", **parameters}, write_state=_write_airflow_state
    )

    assert result["ok"], result["process"]
    assert _tree(mcp_dir) == _tree(cli_dir)
    assert {name for name in _tree(mcp_dir) if name.endswith("databricks.yml")} == {"databricks.yml"}


def test_mcp_package_still_refuses_a_bad_value_for_a_multi_dag_airflow_report(no_bundle_upload, tmp_path: Path):
    output_dir = tmp_path / "out"
    _write_airflow_state(output_dir)

    result = server._cmd_package({"output_dir": str(output_dir), "packaging_mode": "bogus"})

    assert result["ok"] is False
    assert result["process"]["returncode"] == 2
    assert "invalid choice: 'bogus'" in result["process"]["stderr"]
    assert not _has_bundle(output_dir)


@pytest.mark.parametrize(
    ("parameters", "cli_parameters"),
    [
        ({"group_spec": ""}, {}),
        ({"packaging_mode": "per-group", "group_spec": ""}, {"packaging_mode": "per-group"}),
    ],
)
def test_mcp_package_treats_a_blank_group_spec_as_unset(no_bundle_upload, tmp_path: Path, parameters, cli_parameters):
    result, mcp_dir, cli_dir = _package_both_ways(tmp_path, parameters, cli_parameters)

    assert result["ok"], result["process"]
    assert _tree(mcp_dir) == _tree(cli_dir)


@pytest.mark.parametrize(
    ("parameters", "bad_value"),
    [
        ({"packaging_mode": "bogus"}, "bogus"),
        ({"packaging_mode": "per-group", "group_by": "bogus"}, "bogus"),
        ({"packaging_mode": ""}, ""),
        ({"packaging_mode": "per-group", "group_by": ""}, ""),
        ({"packaging_mode": 0}, "0"),
        ({"packaging_mode": False}, "False"),
        ({"packaging_mode": "per-group", "group_by": False}, "False"),
    ],
)
def test_mcp_package_refuses_a_bad_value_like_the_cli(no_bundle_upload, tmp_path: Path, capsys, parameters, bad_value):
    mcp_dir, cli_dir = tmp_path / "mcp", tmp_path / "cli"
    _write_converted_state(mcp_dir)
    _write_converted_state(cli_dir)

    mcp_result = server._cmd_package({"output_dir": str(mcp_dir), **parameters})
    with pytest.raises(SystemExit) as cli_exit:
        package_main(["--output-dir", str(cli_dir), *_cli_flags(parameters)])

    assert mcp_result["ok"] is False
    assert mcp_result["process"]["returncode"] == cli_exit.value.code == 2
    assert f"invalid choice: '{bad_value}'" in mcp_result["process"]["stderr"]
    assert f"invalid choice: '{bad_value}'" in capsys.readouterr().err
    assert not _has_bundle(mcp_dir)


_MISSING_SPEC_PATH = "/nonexistent/flowx/groups.json"


@pytest.mark.parametrize(
    ("parameters", "message"),
    [
        (
            {"packaging_mode": "per-group", "group_by": "spec"},
            "per-group with --group-by spec requires a --group-spec file.",
        ),
        ({"group_spec": _MISSING_SPEC_PATH}, f"group spec file not found: {_MISSING_SPEC_PATH}"),
        ({"packaging_mode": "per-group", "group_spec": {"team_a": ["caller"]}}, "group spec file not found"),
    ],
)
def test_mcp_package_refuses_an_unusable_group_spec_like_the_cli(
    no_bundle_upload, tmp_path: Path, capsys, parameters, message
):
    mcp_dir, cli_dir = tmp_path / "mcp", tmp_path / "cli"
    _write_converted_state(mcp_dir)
    _write_converted_state(cli_dir)

    mcp_result = server._cmd_package({"output_dir": str(mcp_dir), **parameters})
    exit_code = package_main(["--output-dir", str(cli_dir), *_cli_flags(parameters)])

    assert mcp_result["ok"] is False
    assert mcp_result["process"]["returncode"] == exit_code == 1
    assert message in mcp_result["process"]["stderr"]
    assert message in capsys.readouterr().err
    assert not _has_bundle(mcp_dir)


def test_mcp_package_refuses_a_malformed_group_spec_like_the_cli(no_bundle_upload, tmp_path: Path):
    spec_path = tmp_path / "groups.yaml"
    spec_path.write_text("- caller\n- solo\n")
    parameters = {"packaging_mode": "per-group", "group_spec": str(spec_path)}
    mcp_dir, cli_dir = tmp_path / "mcp", tmp_path / "cli"
    _write_converted_state(mcp_dir)
    _write_converted_state(cli_dir)

    mcp_result = server._cmd_package({"output_dir": str(mcp_dir), **parameters})
    with pytest.raises(ValueError, match="must be a mapping, got list"):
        package_main(["--output-dir", str(cli_dir), *_cli_flags(parameters)])

    assert mcp_result["ok"] is False
    assert mcp_result["process"]["returncode"] == 1
    assert "must be a mapping, got list" in mcp_result["process"]["stderr"]
    assert not _has_bundle(mcp_dir)


class _FakeResult:
    ok = True
    stdout = ""
    stderr = ""
    returncode = 0

    def as_dict(self) -> dict[str, object]:
        return {"returncode": 0, "stdout": "", "stderr": ""}


def _migrate(output_dir: Path, parameters: dict[str, Any]) -> dict[str, Any]:
    return server._cmd_migrate(
        {
            "source": "adf",
            "adf_source_path": str(output_dir.parent / "adf"),
            "output_dir": str(output_dir),
            "interactive": False,
            **parameters,
        }
    )


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
        (
            {"group_by": "spec", "group_spec": "/tmp/groups.json"},
            ["--packaging-mode", "per-group", "--group-by", "spec", "--group-spec", "/tmp/groups.json"],
        ),
        ({"group_spec": ""}, []),
        ({"packaging_mode": "per-group", "group_spec": ""}, ["--packaging-mode", "per-group"]),
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

    result = _migrate(tmp_path / "out", parameters)

    assert result["status"] == "completed"
    package_argv = next(argv for argv in calls if argv[0] == "package")
    assert package_argv[package_argv.index("--schema") + 2 :] == expected_flags


@pytest.mark.parametrize(
    ("parameters", "returncode", "message"),
    [
        ({"packaging_mode": ""}, 2, "invalid choice: ''"),
        ({"packaging_mode": "per-group", "group_by": ""}, 2, "invalid choice: ''"),
        ({"packaging_mode": "per-group", "group_by": "spec"}, 1, "requires a --group-spec file"),
        ({"group_spec": _MISSING_SPEC_PATH}, 1, "group spec file not found"),
    ],
)
def test_migrate_refuses_a_bad_packaging_value_like_package(
    monkeypatch, tmp_path: Path, parameters, returncode, message
):
    output_dir = tmp_path / "out"
    _write_converted_state(output_dir)
    real_run_adapter = runner.run_adapter

    def run_adapter(args, **kwargs):
        """Skips discover and convert (their output is already on disk) and runs package for real."""
        return real_run_adapter(args, **kwargs) if args[0] == "package" else _FakeResult()

    monkeypatch.setattr(runner, "run_adapter", run_adapter)

    result = _migrate(output_dir, parameters)

    assert result["ok"] is False
    assert (result["status"], result["failed_phase"]) == ("failed", "package")
    assert result["steps"]["package"]["process"]["returncode"] == returncode
    assert message in result["steps"]["package"]["process"]["stderr"]
    assert not _has_bundle(output_dir)


@pytest.mark.parametrize(("parameters", "conflict"), _SPEC_CONFLICTS)
def test_migrate_refuses_a_group_spec_with_a_contradicting_layout_before_discover(
    monkeypatch, tmp_path: Path, parameters, conflict
):
    spec_path = _write_group_spec(tmp_path)
    calls: list[list[Any]] = []

    def fake_run_adapter(args, **_kwargs):
        calls.append(args)
        return _FakeResult()

    monkeypatch.setattr(runner, "run_adapter", fake_run_adapter)

    result = _call_flowx(
        "migrate",
        {
            "source": "adf",
            "adf_source_path": str(tmp_path / "adf"),
            "output_dir": str(tmp_path / "out"),
            "interactive": False,
            "group_spec": str(spec_path),
            **parameters,
        },
    )

    assert result["ok"] is False
    assert repr(str(spec_path)) in result["error"] and conflict in result["error"]
    assert calls == []
