"""Tests that the MCP ``package`` and ``migrate`` commands honour the bundle packaging mode.

The CLI's ``package --packaging-mode/--group-by/--group-spec`` decides how a multi-pipeline migration
is laid out into bundles. These tests package the same converted state through MCP and through the
CLI and check both write the same files, that a bad value is refused the same way, and that a
combination of values the CLI would quietly ignore is refused before anything runs. Skipped where the
optional ``mcp`` dependency is absent.
"""

from __future__ import annotations

import asyncio
import itertools
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


def test_inputs_package_defaults_package_like_the_cli(no_bundle_upload, tmp_path: Path):
    """The hosted flow: keep every ``inputs package`` default and package with those answers."""
    answers = _inputs_package_defaults()

    result, mcp_dir, cli_dir = _package_both_ways(
        tmp_path, {key: answers[key] for key in ("catalog", "schema", "packaging_mode", "group_spec")}, {}
    )

    assert result["ok"], result["process"]
    assert _tree(mcp_dir) == _tree(cli_dir)


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
    ("parameters", "bad_value"),
    [
        ({"packaging_mode": "bogus"}, "bogus"),
        ({"packaging_mode": "per-group", "group_by": "bogus"}, "bogus"),
        ({"packaging_mode": ""}, ""),
        ({"packaging_mode": "per-group", "group_by": ""}, ""),
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
        ({"group_spec": _MISSING_SPEC_PATH}, f"group spec file not found: {_MISSING_SPEC_PATH}"),
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


@pytest.fixture
def adapter_calls(monkeypatch) -> list[list[str]]:
    """Turns every adapter run into a successful no-op and records its argv, so nothing is packaged."""
    calls: list[list[str]] = []

    def fake_run_adapter(args, **_kwargs):
        calls.append([str(arg) for arg in args])
        return _FakeResult()

    monkeypatch.setattr(runner, "run_adapter", fake_run_adapter)
    monkeypatch.setattr(runner, "summarize_inventory", lambda out: {})
    monkeypatch.setattr(runner, "summarize_translation", lambda out: {})
    monkeypatch.setattr(runner, "list_tree", lambda out: [])
    monkeypatch.setattr(runner, "read_tree", lambda out: {"files": {}, "truncated": []})
    return calls


def _parameters_for(command: str, output_dir: Path, packaging: dict[str, Any]) -> dict[str, Any]:
    """The fewest parameters *command* needs to write to *output_dir*, plus the given packaging keys."""
    parameters = {"output_dir": str(output_dir), **packaging}
    if command == "migrate":
        parameters.update(source="adf", adf_source_path=str(output_dir.parent / "adf"), interactive=False)
    return parameters


def _migrate(output_dir: Path, parameters: dict[str, Any]) -> dict[str, Any]:
    return server._cmd_migrate(_parameters_for("migrate", output_dir, parameters))


def _layout_parameters(layout: tuple[Any, Any, Any]) -> dict[str, Any]:
    """The packaging keys a ``(packaging_mode, group_by, group_spec)`` layout sends; ``None`` leaves a key out."""
    keys = ("packaging_mode", "group_by", "group_spec")
    return {key: value for key, value in zip(keys, layout, strict=True) if value is not None}


_GROUP_SPEC = "/tmp/groups.json"
_SPEC_LAYOUT = ["--packaging-mode", "per-group", "--group-by", "spec", "--group-spec", _GROUP_SPEC]
_FORWARDED_LAYOUTS = {
    (None, None, None): [],
    ("per-pipeline", None, None): ["--packaging-mode", "per-pipeline"],
    ("single", None, None): ["--packaging-mode", "single"],
    ("per-group", None, None): ["--packaging-mode", "per-group"],
    ("per-group", "inferred", None): ["--packaging-mode", "per-group", "--group-by", "inferred"],
    (None, None, ""): [],
    ("per-pipeline", None, ""): ["--packaging-mode", "per-pipeline"],
    ("single", None, ""): ["--packaging-mode", "single"],
    ("per-group", None, ""): ["--packaging-mode", "per-group"],
    ("per-group", "inferred", ""): ["--packaging-mode", "per-group", "--group-by", "inferred"],
    (None, None, _GROUP_SPEC): _SPEC_LAYOUT,
    (None, "spec", _GROUP_SPEC): _SPEC_LAYOUT,
    ("per-group", None, _GROUP_SPEC): _SPEC_LAYOUT,
    ("per-group", "spec", _GROUP_SPEC): _SPEC_LAYOUT,
}
_REFUSED_LAYOUTS = [
    layout
    for layout in itertools.product(
        [None, "per-pipeline", "single", "per-group"], [None, "inferred", "spec"], [None, "", _GROUP_SPEC]
    )
    if layout not in _FORWARDED_LAYOUTS
] + [("", None, _GROUP_SPEC)]


@pytest.mark.parametrize("command", ["package", "migrate"])
@pytest.mark.parametrize(("layout", "expected_flags"), list(_FORWARDED_LAYOUTS.items()), ids=str)
def test_a_layout_that_fits_is_forwarded_verbatim(adapter_calls, tmp_path: Path, command, layout, expected_flags):
    result = _call_flowx(command, _parameters_for(command, tmp_path / "out", _layout_parameters(layout)))

    assert result["ok"], result
    package_argv = next(argv for argv in adapter_calls if argv[0] == "package")
    assert package_argv[package_argv.index("--schema") + 2 :] == expected_flags


@pytest.fixture
def migrate_with_options(adapter_calls, monkeypatch) -> list[list[str]]:
    """Like ``adapter_calls``, but ``inspect`` reports one unanswered option so a first migrate returns needs_input."""
    recording_run_adapter = runner.run_adapter
    pending = {"pipelines": [{"pipeline_name": "caller", "options": [{"option_id": "option", "show_when": []}]}]}

    def run_adapter(args, **kwargs):
        result = recording_run_adapter(args, **kwargs)
        if args[0] == "inspect":
            result = _FakeResult()
            result.stdout = json.dumps(pending)
        return result

    monkeypatch.setattr(runner, "run_adapter", run_adapter)
    return adapter_calls


def _first_migrate_call(calls: list[list[str]], output_dir: Path, packaging: dict[str, Any]) -> dict[str, Any]:
    """The first, answerless migrate call with a non-default layout; it stops at needs_input.

    A report is already on disk, so checking that discover and convert still ran proves an answerless
    call never resumes from it. *calls* is cleared afterwards so the caller sees only the next call's runs.
    """
    _write_converted_state(output_dir)
    first = _call_flowx(
        "migrate",
        {
            "source": "adf",
            "adf_source_path": str(output_dir.parent / "adf"),
            "output_dir": str(output_dir),
            **packaging,
        },
    )
    assert first["status"] == "needs_input", first
    assert [argv[0] for argv in calls] == ["discover", "convert", "inspect"]
    calls.clear()
    return first


_LAYOUTS_SENT_FIRST = [("single", None, None), ("per-group", "inferred", None), (None, None, _GROUP_SPEC)]


@pytest.mark.parametrize(
    ("first_layout", "re_call_layout"),
    list(itertools.product(_LAYOUTS_SENT_FIRST, [(None, None, None), *_LAYOUTS_SENT_FIRST])),
    ids=str,
)
def test_a_migrate_answers_re_call_packages_with_its_own_layout(
    migrate_with_options, tmp_path: Path, first_layout, re_call_layout
):
    """The layout sent on the first call is not remembered: only the answers re-call's own keys set it."""
    output_dir = tmp_path / "out"
    _first_migrate_call(migrate_with_options, output_dir, _layout_parameters(first_layout))

    result = _call_flowx(
        "migrate",
        {
            "source": "adf",
            "output_dir": str(output_dir),
            "answers": ["option=value"],
            **_layout_parameters(re_call_layout),
        },
    )

    assert result["ok"], result
    assert [argv[0] for argv in migrate_with_options] == ["modify", "package"]
    package_argv = migrate_with_options[-1]
    assert package_argv[package_argv.index("--schema") + 2 :] == _FORWARDED_LAYOUTS[re_call_layout]


def test_a_later_interactive_false_call_is_a_fresh_run_that_needs_the_source_again(
    migrate_with_options, tmp_path: Path
):
    """Only an answers re-call reuses the report; interactive=false after needs_input starts over from discover."""
    output_dir = tmp_path / "out"
    packaging = {"packaging_mode": "single"}
    _first_migrate_call(migrate_with_options, output_dir, packaging)

    without_source = _call_flowx(
        "migrate", {"source": "adf", "output_dir": str(output_dir), "interactive": False, **packaging}
    )
    assert without_source["ok"] is False
    assert "Provide a source path" in without_source["error"]
    assert migrate_with_options == []

    with_source = _call_flowx("migrate", _parameters_for("migrate", output_dir, packaging))
    assert with_source["ok"], with_source
    assert [argv[0] for argv in migrate_with_options] == ["discover", "convert", "package"]
    assert migrate_with_options[-1][-2:] == ["--packaging-mode", "single"]


@pytest.mark.parametrize("command", ["package", "migrate"])
@pytest.mark.parametrize("layout", _REFUSED_LAYOUTS, ids=str)
def test_a_layout_that_does_not_fit_is_refused_before_anything_runs(adapter_calls, tmp_path: Path, command, layout):
    packaging = _layout_parameters(layout)

    result = _call_flowx(command, _parameters_for(command, tmp_path / "out", packaging))

    assert result["ok"] is False
    for key, value in packaging.items():
        assert f"{key} {value!r}" in result["error"]
    assert adapter_calls == []


@pytest.mark.parametrize("command", ["package", "migrate"])
@pytest.mark.parametrize(
    ("key", "packaging"),
    [
        ("packaging_mode", {"packaging_mode": 0}),
        ("packaging_mode", {"packaging_mode": False}),
        ("group_by", {"packaging_mode": "per-group", "group_by": False}),
        ("group_spec", {"group_spec": False}),
        ("group_spec", {"group_spec": 0}),
        ("group_spec", {"packaging_mode": "per-group", "group_spec": {"team_a": ["caller"]}}),
    ],
    ids=str,
)
def test_a_packaging_value_that_is_not_a_string_is_refused_before_anything_runs(
    adapter_calls, tmp_path: Path, command, key, packaging
):
    """A non-string value would reach the CLI stringified (``False`` as the spec path ``"False"``), so it is refused."""
    result = _call_flowx(command, _parameters_for(command, tmp_path / "out", packaging))

    assert result["ok"] is False
    assert f"{key} must be a string, got {packaging[key]!r}" in result["error"]
    assert adapter_calls == []


@pytest.mark.parametrize(
    ("parameters", "returncode", "message"),
    [
        ({"packaging_mode": ""}, 2, "invalid choice: ''"),
        ({"packaging_mode": "per-group", "group_by": ""}, 2, "invalid choice: ''"),
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
