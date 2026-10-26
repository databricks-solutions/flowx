"""Tests that the MCP ``route`` / ``fill_agentic`` commands forward to the adapter CLI.

``route`` with no plan lets the adapter write the recommendation into ``conversion_plan.json``; a
plan (inline or path) is forwarded via ``--plan-path``. Either way the recorded plan and the routing
review page come back to the caller. ``fill_agentic`` fills a routed-agentic unit.
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
        argv = [str(a) for a in args]
        calls.append(argv)
        # A successful route always writes the plan and the review page; keep any a test wrote first.
        metadata = Path(argv[argv.index("--output-dir") + 1]) / "metadata"
        metadata.mkdir(parents=True, exist_ok=True)
        for name, text in (("conversion_plan.json", '{"components": []}'), ("routing_review.html", "<html></html>")):
            if not (metadata / name).exists():
                (metadata / name).write_text(text, encoding="utf-8")
        if args and args[0] == "route" and "--plan-path" not in argv:
            return _Result(json.dumps({"ok": True, "violations": [], "pending": ["component-1"], "applied": False}))
        return _Result(json.dumps({"ok": True, "violations": [], "components": 1, "edit": {"agentic_pipelines": []}}))

    monkeypatch.setattr(runner, "run_adapter", fake_run_adapter)
    return calls


def test_route_without_a_plan_forwards_no_plan_path(captured, tmp_path: Path) -> None:
    out = server._cmd_route({"output_dir": str(tmp_path)})
    assert out["ok"] is True
    argv = captured[0]
    assert argv[0] == "route" and "--plan-path" not in argv
    assert "--output-dir" in argv


def test_route_returns_the_recorded_plan_and_review_page_inline(captured, tmp_path: Path) -> None:
    metadata = tmp_path / "metadata"
    metadata.mkdir()
    (metadata / "conversion_plan.json").write_text('{"components": []}', encoding="utf-8")
    (metadata / "routing_review.html").write_text("<html></html>", encoding="utf-8")
    (metadata / "inventory.json").write_text("{}", encoding="utf-8")

    out = server._cmd_route({"output_dir": str(tmp_path)})

    assert sorted(out["bundle"]["files"]) == ["conversion_plan.json", "routing_review.html"]


@pytest.mark.parametrize("oversized", ["conversion_plan.json", "routing_review.html"])
def test_route_fails_when_the_plan_or_review_page_is_too_large_to_return_inline(
    captured, tmp_path: Path, oversized: str
) -> None:
    """The inline bundle drops a file over its size cap, and the agent needs both to edit the plan and show the page."""
    metadata = tmp_path / "metadata"
    metadata.mkdir()
    (metadata / "conversion_plan.json").write_text('{"components": []}', encoding="utf-8")
    (metadata / "routing_review.html").write_text("<html></html>", encoding="utf-8")
    (metadata / oversized).write_text("x" * 2_100_000, encoding="utf-8")

    out = server._cmd_route({"output_dir": str(tmp_path)})

    assert out["ok"] is False
    assert "bundle" not in out
    assert f"{oversized} (2100000 bytes) is too large to return inline" in out["error"]
    assert "output_volume_path" in out["error"] and "output_workspace_path" in out["error"]
    assert out["result"]["ok"] is True


def test_route_fails_when_a_successful_route_left_no_review_page(tmp_path: Path, monkeypatch) -> None:
    """A file removed after route wrote it (a discover in between) is not delivered, so route must not say ok."""

    def fake_run_adapter(args, **_kwargs):
        metadata = tmp_path / "metadata"
        metadata.mkdir(exist_ok=True)
        (metadata / "conversion_plan.json").write_text('{"components": []}', encoding="utf-8")
        return _Result(json.dumps({"ok": True, "violations": [], "pending": ["component-1"], "applied": False}))

    monkeypatch.setattr(runner, "run_adapter", fake_run_adapter)

    out = server._cmd_route({"output_dir": str(tmp_path)})

    assert out["ok"] is False
    assert "bundle" not in out
    assert "routing_review.html is missing" in out["error"]


def test_route_fails_cleanly_when_a_file_goes_missing_while_it_is_copied(captured, tmp_path: Path, monkeypatch) -> None:
    """A discover can move a file between route's success and the copy; that is a missing file, not a crash."""
    real_copy = server.shutil.copy

    def copy_after_discover_moved_the_page(source, destination):
        if Path(source).name == "routing_review.html":
            Path(source).unlink()
        return real_copy(source, destination)

    monkeypatch.setattr(server.shutil, "copy", copy_after_discover_moved_the_page)

    out = server._cmd_route({"output_dir": str(tmp_path)})

    assert out["ok"] is False
    assert "routing_review.html is missing" in out["error"]


def test_route_that_failed_still_returns_the_plan_already_recorded(tmp_path: Path, monkeypatch) -> None:
    metadata = tmp_path / "metadata"
    metadata.mkdir()
    (metadata / "conversion_plan.json").write_text('{"components": []}', encoding="utf-8")

    def fake_run_adapter(args, **_kwargs):
        return _Result(json.dumps({"ok": False, "violations": ["a plan problem"], "components": 0}))

    monkeypatch.setattr(runner, "run_adapter", fake_run_adapter)

    out = server._cmd_route({"output_dir": str(tmp_path), "plan": {"components": []}})

    assert out["ok"] is False
    assert "error" not in out
    assert list(out["bundle"]["files"]) == ["conversion_plan.json"]


@pytest.mark.parametrize("oversized", ["conversion_plan.json", "routing_review.html"])
def test_route_that_failed_returns_both_files_as_they_are_even_when_one_is_too_large(
    tmp_path: Path, monkeypatch, oversized: str
) -> None:
    metadata = tmp_path / "metadata"
    metadata.mkdir()
    (metadata / "conversion_plan.json").write_text('{"components": []}', encoding="utf-8")
    (metadata / "routing_review.html").write_text("<html></html>", encoding="utf-8")
    (metadata / oversized).write_text("x" * 2_100_000, encoding="utf-8")
    monkeypatch.setattr(
        runner, "run_adapter", lambda args, **_: _Result(json.dumps({"ok": False, "violations": ["x"]}))
    )

    out = server._cmd_route({"output_dir": str(tmp_path)})

    assert out["ok"] is False
    assert "error" not in out
    assert out["bundle"]["truncated"] == [oversized]


def test_route_says_a_file_it_cannot_read_as_text_is_unreadable_not_too_large(captured, tmp_path: Path) -> None:
    metadata = tmp_path / "metadata"
    metadata.mkdir()
    (metadata / "conversion_plan.json").write_bytes(b"\xff")

    out = server._cmd_route({"output_dir": str(tmp_path)})

    assert out["ok"] is False
    assert "conversion_plan.json is not UTF-8 text" in out["error"]
    assert "too large" not in out["error"]


def test_route_reports_why_a_file_could_not_be_copied(captured, tmp_path: Path, monkeypatch) -> None:
    real_copy = server.shutil.copy

    def copy_without_permission(source, destination):
        if Path(source).name == "conversion_plan.json":
            raise PermissionError("permission denied")
        return real_copy(source, destination)

    monkeypatch.setattr(server.shutil, "copy", copy_without_permission)

    out = server._cmd_route({"output_dir": str(tmp_path)})

    assert out["ok"] is False
    assert "conversion_plan.json could not be copied (permission denied)" in out["error"]
    assert "missing" not in out["error"]


@pytest.mark.parametrize("upload_key", ["output_volume_path", "output_workspace_path"])
@pytest.mark.parametrize("broken", ["missing_page", "plan_not_text"])
def test_a_successful_route_uploads_nothing_unless_both_files_are_sound(
    captured, tmp_path: Path, monkeypatch, upload_key: str, broken: str
) -> None:
    """An upload of a new plan beside a stale page, or of a plan that is not text, would look like a good delivery."""
    metadata = tmp_path / "metadata"
    metadata.mkdir()
    if broken == "plan_not_text":
        (metadata / "conversion_plan.json").write_bytes(b"\xff")
    uploaded: list[str] = []

    def fake_upload(local_root: Path, destination: str) -> dict[str, Any]:
        uploaded.extend(path.name for path in local_root.rglob("*"))
        return {upload_key: destination, "files": uploaded, "count": len(uploaded)}

    monkeypatch.setattr(runner, "upload_tree_to_volume", fake_upload)
    monkeypatch.setattr(runner, "upload_tree_to_workspace", fake_upload)
    real_run = runner.run_adapter

    def route_that_leaves_no_page(args, **kwargs):
        result = real_run(args, **kwargs)
        if broken == "missing_page":
            (metadata / "routing_review.html").unlink()
        return result

    monkeypatch.setattr(runner, "run_adapter", route_that_leaves_no_page)

    out = server._cmd_route({"output_dir": str(tmp_path), upload_key: "/Volumes/main/default/flowx"})

    assert out["ok"] is False
    assert uploaded == []
    assert "bundle_uploaded" not in out


def test_route_uploads_an_oversized_plan_and_review_page(captured, tmp_path: Path, monkeypatch) -> None:
    metadata = tmp_path / "metadata"
    metadata.mkdir()
    (metadata / "conversion_plan.json").write_text("x" * 2_100_000, encoding="utf-8")
    (metadata / "routing_review.html").write_text("<html></html>", encoding="utf-8")
    uploaded: list[str] = []

    def fake_upload(local_root: Path, volume_path: str) -> dict[str, Any]:
        uploaded.extend(sorted(path.name for path in local_root.rglob("*")))
        return {"output_volume_path": volume_path, "files": uploaded, "count": len(uploaded)}

    monkeypatch.setattr(runner, "upload_tree_to_volume", fake_upload)

    out = server._cmd_route({"output_dir": str(tmp_path), "output_volume_path": "/Volumes/main/default/flowx"})

    assert out["ok"] is True
    assert uploaded == ["conversion_plan.json", "routing_review.html"]


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


def test_route_never_forwards_a_source_so_it_cannot_run_convert(captured, tmp_path: Path) -> None:
    server._cmd_route({"output_dir": str(tmp_path), "source": "adf", "adf_source_path": "/tmp/adf"})
    assert "--source" not in captured[0] and "--source-path" not in captured[0]


def test_route_registered_in_command_map() -> None:
    assert "route" in server._COMMANDS


# --------------------------------------------------------------------------- #
# fill_agentic.
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
    assert argv[0] == "fill-agentic" and argv[1] == "--output-dir"
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


@pytest.mark.parametrize("members", [[None], ["parent", 7], ["parent", ""], 5], ids=["null", "number", "empty", "int"])
def test_fill_agentic_refuses_members_that_are_not_pipeline_names(captured_fill, tmp_path: Path, members) -> None:
    """``[None]`` used to reach the CLI as ``--members None`` and could match a pipeline named "None"."""
    out = server._cmd_fill_agentic({"output_dir": str(tmp_path), "members": members, "pipelines": []})

    assert out["ok"] is False and "members" in out["error"]
    assert captured_fill == []


def test_fill_agentic_requires_exactly_one_pipelines_source(tmp_path: Path) -> None:
    both = server._cmd_fill_agentic(
        {"output_dir": str(tmp_path), "members": ["a"], "pipelines": [], "pipelines_path": "x.json"}
    )
    neither = server._cmd_fill_agentic({"output_dir": str(tmp_path), "members": ["a"]})
    assert both["ok"] is False and "exactly one" in both["error"]
    assert neither["ok"] is False and "exactly one" in neither["error"]


def test_fill_agentic_registered_in_command_map() -> None:
    assert "fill_agentic" in server._COMMANDS
