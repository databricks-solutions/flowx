"""Tests for the MCP server's agent-driven interactive ``migrate`` flow.

The server returns the full option schema once (``needs_input``); the agent walks the chain locally
and re-calls ``migrate`` once with the complete answers, which applies and packages. These tests
guard that one-shot contract. Skipped where the optional ``mcp`` dependency is absent.
"""

from __future__ import annotations

import contextlib
import io
import json
from pathlib import Path

import pytest

pytest.importorskip("mcp")

from flowx.adapter.__main__ import main as adapter_main  # noqa: E402
from flowx.mcp import runner, server  # noqa: E402


class _FakeResult:
    def __init__(self, *, ok: bool = True, stdout: str = "", stderr: str = "") -> None:
        self.ok = ok
        self.stdout = stdout
        self.stderr = stderr
        self.returncode = 0 if ok else 1

    def as_dict(self) -> dict[str, object]:
        return {"returncode": self.returncode, "stdout": self.stdout, "stderr": self.stderr}


_SCHEMA = {
    "pipelines": [
        {
            "pipeline_name": "p",
            "options": [
                {
                    "option_id": "notify_destination",
                    "prompt": "Route notifications?",
                    "rationale": "...",
                    "choices": [{"value": "keep", "label": "Keep", "description": ""}],
                    "free_text": False,
                    "default": "keep",
                    "affected_task_keys": ["load"],
                    "show_when": [],
                },
                {
                    "option_id": "notify_slack_url",
                    "prompt": "Slack URL?",
                    "rationale": "...",
                    "choices": [],
                    "free_text": True,
                    "default": "",
                    "affected_task_keys": ["load"],
                    "show_when": [{"option_id": "notify_destination", "in": ["slack"]}],
                },
            ],
        }
    ]
}


@pytest.fixture
def stub_adapter(monkeypatch):
    """Stubs the adapter subprocess + artifact readers; records which subcommands ran."""
    calls: list[str] = []

    def fake_run_adapter(args):
        calls.append(args[0])
        if args[0] == "inspect":
            return _FakeResult(stdout=json.dumps(_SCHEMA))
        return _FakeResult()

    monkeypatch.setattr(runner, "run_adapter", fake_run_adapter)
    monkeypatch.setattr(runner, "summarize_inventory", lambda out: {"pipeline_count": 1})
    monkeypatch.setattr(runner, "summarize_translation", lambda out: {"translated": 1})
    monkeypatch.setattr(runner, "list_tree", lambda out: ["databricks.yml"])
    monkeypatch.setattr(runner, "read_tree", lambda out: {"files": {}, "truncated": []})
    return calls


def test_first_call_returns_full_schema_without_packaging(stub_adapter, tmp_path: Path):
    result = server._cmd_migrate(
        {"source": "adf", "adf_source_path": str(tmp_path / "adf"), "output_dir": str(tmp_path / "out")}
    )
    assert result["status"] == "needs_input"
    # The whole tree (including the conditional slack follow-up) is returned up front.
    option_ids = {o["option_id"] for pipe in result["pending_options"] for o in pipe["options"]}
    assert {"notify_destination", "notify_slack_url"} <= option_ids
    all_options = [o for pipe in result["pending_options"] for o in pipe["options"]]
    slack = next(o for o in all_options if o["option_id"] == "notify_slack_url")
    assert slack["show_when"] == [{"option_id": "notify_destination", "in": ["slack"]}]
    # discover + convert ran, but NOT package (we paused for input).
    assert stub_adapter == ["discover", "convert", "inspect"]


def test_resume_with_answers_applies_and_packages_once(stub_adapter, tmp_path: Path):
    out = tmp_path / "out"
    (out / ".work").mkdir(parents=True)
    (out / ".work" / "translation_report.json").write_text("{}")  # prior convert output -> resume path

    result = server._cmd_migrate(
        {
            "source": "adf",
            "adf_source_path": str(tmp_path / "adf"),
            "output_dir": str(out),
            "answers": ["notify_destination=slack", "notify_slack_url=https://hooks.slack.com/x"],
        }
    )
    assert result["status"] == "completed"
    # Resume skips discover/convert and does not re-inspect; it applies the answers then packages.
    assert stub_adapter == ["modify", "package"]
    assert "apply_answers" in result["steps"] and "package" in result["steps"]


def test_interactive_false_skips_prompt_and_packages(stub_adapter, tmp_path: Path):
    result = server._cmd_migrate(
        {
            "source": "adf",
            "adf_source_path": str(tmp_path / "adf"),
            "output_dir": str(tmp_path / "out"),
            "interactive": False,
        }
    )
    assert result["status"] == "completed"
    assert stub_adapter == ["discover", "convert", "package"]  # no inspect, no pause


def _run_discover_for_real_and_package_a_bundle(monkeypatch: pytest.MonkeyPatch) -> None:
    """Run discover through the real adapter; package stands in by writing a small current bundle."""

    def run_adapter(args: list[object], **_: object) -> runner.AdapterResult:
        command = [str(arg) for arg in args]
        if command[0] == "package":
            out = Path(command[command.index("--output-dir") + 1])
            for relative in ("databricks.yml", "resources/job.yml", "src/notebook.py"):
                (out / relative).parent.mkdir(parents=True, exist_ok=True)
                (out / relative).write_text(f"# {relative}\n", encoding="utf-8")
        if command[0] != "discover":
            return runner.AdapterResult(command=command, returncode=0, stdout="", stderr="")
        stdout, stderr = io.StringIO(), io.StringIO()
        with contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
            returncode = adapter_main(command)
        return runner.AdapterResult(command=command, returncode=returncode, stdout=stdout.getvalue(), stderr="")

    monkeypatch.setattr(runner, "run_adapter", run_adapter)


def test_migrate_over_an_earlier_run_reports_the_files_discover_archived(
    monkeypatch: pytest.MonkeyPatch, fixtures_dir: Path, tmp_path: Path
) -> None:
    """Hosted migrate re-runs discover for real, which archives the earlier outputs; the result summarises them."""
    _run_discover_for_real_and_package_a_bundle(monkeypatch)
    out = tmp_path / "out"
    parameters = {"source": "adf", "adf_source_path": str(fixtures_dir), "output_dir": str(out), "interactive": False}

    first = server._cmd_migrate(parameters)
    assert first["status"] == "completed"
    assert "archived" not in first["steps"]["discover"]
    (out / "metadata" / "conversion_plan.json").write_text('{"components": []}', encoding="utf-8")
    inventory_before = (out / "metadata" / "inventory.json").read_bytes()

    second = server._cmd_migrate(parameters)

    archived = second["steps"]["discover"]["archived"]
    record = json.loads((out / archived["record"]).read_text(encoding="utf-8"))
    assert archived["archive_dir"] == Path(archived["record"]).parent.as_posix()
    assert archived["archive_dir"].startswith("metadata/archive/")
    assert archived["archived_at"] == record["archived_at"]
    assert archived["moved_count"] == len(record["moved"])
    assert archived["record"] in archived["note"]
    moved = {entry["from"]: entry["to"] for entry in archived["moved"]}
    assert (out / moved["metadata/conversion_plan.json"]).read_text(encoding="utf-8") == '{"components": []}'
    assert (out / moved["metadata/inventory.json"]).read_bytes() == inventory_before
    assert not (out / "metadata" / "conversion_plan.json").exists()


def test_the_archive_summary_lists_at_most_fifty_moved_files(
    monkeypatch: pytest.MonkeyPatch, fixtures_dir: Path, tmp_path: Path
) -> None:
    """A large earlier run is counted in full, but only the first fifty moves are listed in the tool result."""
    _run_discover_for_real_and_package_a_bundle(monkeypatch)
    out = tmp_path / "out"
    parameters = {"source": "adf", "adf_source_path": str(fixtures_dir), "output_dir": str(out), "interactive": False}
    assert server._cmd_migrate(parameters)["status"] == "completed"
    for index in range(60):
        (out / "metadata" / "extra").mkdir(exist_ok=True)
        (out / "metadata" / "extra" / f"{index:02d}.txt").write_text(str(index), encoding="utf-8")

    archived = server._cmd_migrate(parameters)["steps"]["discover"]["archived"]

    record = json.loads((out / archived["record"]).read_text(encoding="utf-8"))
    assert archived["moved_count"] == len(record["moved"]) > 60
    assert archived["moved"] == [{"from": entry["from"], "to": entry["to"]} for entry in record["moved"][:50]]


def test_after_a_rerun_the_current_bundle_is_delivered_and_listed_before_archived_files(
    monkeypatch: pytest.MonkeyPatch, fixtures_dir: Path, tmp_path: Path
) -> None:
    """Archived runs sort last, so the delivery limits are spent on the current bundle, not on an earlier run."""
    _run_discover_for_real_and_package_a_bundle(monkeypatch)
    out = tmp_path / "out"
    parameters = {"source": "adf", "adf_source_path": str(fixtures_dir), "output_dir": str(out), "interactive": False}
    assert server._cmd_migrate(parameters)["status"] == "completed"

    package = server._cmd_migrate(parameters)["steps"]["package"]

    for delivered in (list(package["bundle"]["files"]), package["bundle_files"]):
        archived = [index for index, path in enumerate(delivered) if path.startswith("metadata/archive/")]
        current = [index for index, path in enumerate(delivered) if not path.startswith("metadata/archive/")]
        assert archived and {"databricks.yml", "resources/job.yml", "src/notebook.py"} <= set(delivered)
        assert max(current) < min(archived)
