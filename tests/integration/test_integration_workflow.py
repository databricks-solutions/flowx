"""Offline contracts for the live-integration workflow."""

from __future__ import annotations

import re
import shutil
import subprocess
from pathlib import Path

import pytest

WORKFLOW = Path(__file__).parents[2] / ".github" / "workflows" / "integration.yml"


def test_claude_prompt_uses_stdin_and_scoped_edit_permissions() -> None:
    """Variadic permission flags cannot consume the prompt or grant broad writes."""
    workflow = WORKFLOW.read_text(encoding="utf-8")
    invocation = re.search(r"if ! claude -p \\\n(?P<flags>.*?)<<CLAUDE_PROMPT", workflow, re.DOTALL)
    assert invocation, "Claude must receive its prompt from the CLAUDE_PROMPT heredoc"
    flags = invocation.group("flags")
    assert "--permission-mode dontAsk" in flags
    assert "acceptEdits" not in workflow
    assert '--tools="Read,Edit,Write,Glob,Grep"' in flags
    assert '--allowedTools="Read,Edit(agentic_results/**),Glob,Grep"' in flags
    assert '--disallowedTools="Read(' in flags
    assert "Write(agentic_results/**)" not in workflow
    assert "--allowedTools " not in flags
    assert "--disallowedTools " not in flags
    assert "CLAUDE_PROMPT\n          then" in workflow
    assert "Bash" not in flags


def test_agentic_prompt_and_coverage_share_expected_task_source() -> None:
    """The prompt and exact-coverage gate use one PlaceholderActivity list."""
    workflow = WORKFLOW.read_text(encoding="utf-8")
    assert 'expected_tasks_file="${RUNNER_TEMP}/expected-agentic-tasks.json"' in workflow
    assert 'select(.type? == "PlaceholderActivity")' in workflow
    assert '$(cat "${expected_tasks_file}")' in workflow
    assert "jq -c '[.[] | [.pipeline, .activity_name]] | sort' \"${expected_tasks_file}\"" in workflow
    assert "strategy is" not in workflow
    assert 'status is "pending"' not in workflow
    assert "generated_source field" in workflow
    assert '.task.generated_source | (type == "string" and length > 0)' in workflow
    assert ".task.notebook_path" not in workflow


def test_agentic_guard_covers_ignored_files() -> None:
    """Claude confinement compares all non-result worktree file content."""
    workflow = WORKFLOW.read_text(encoding="utf-8")
    assert "pre-claude-worktree.sha256" in workflow
    assert "post-claude-worktree.sha256" in workflow
    assert "if: always() && steps.agentic.outputs.configured == 'true'" in workflow
    assert workflow.count("snapshot_worktree()") == 1
    assert workflow.count('source "${RUNNER_TEMP}/snapshot-worktree.sh"') == 2
    assert "including ignored files" in workflow


def test_snapshot_helper_handles_links_and_detects_changes(tmp_path: Path) -> None:
    """The runner's snapshot detects content, entry, mode, and link changes."""
    find = shutil.which("find")
    if find is None:
        pytest.skip("find is unavailable")
    probe = subprocess.run(
        [find, ".", "-maxdepth", "0", "-printf", ""],
        cwd=tmp_path,
        capture_output=True,
        check=False,
    )
    if probe.returncode != 0:
        pytest.skip("GNU find -printf is unavailable")

    workflow = WORKFLOW.read_text(encoding="utf-8")
    match = re.search(
        r"<<'SNAPSHOT_HELPER'\n(?P<body>.*?)\n\s*SNAPSHOT_HELPER",
        workflow,
        re.DOTALL,
    )
    assert match, "snapshot helper must be embedded once in the workflow"
    helper = tmp_path / "snapshot-worktree.sh"
    helper.write_text(re.sub(r"^ {10}", "", match.group("body"), flags=re.MULTILINE) + "\n")

    (tmp_path / "regular.txt").write_text("before", encoding="utf-8")
    (tmp_path / "directory").mkdir()
    (tmp_path / "other-directory").mkdir()
    (tmp_path / "directory-link").symlink_to("directory", target_is_directory=True)
    (tmp_path / "dangling-link").symlink_to("missing", target_is_directory=True)

    def snapshot() -> str:
        result = subprocess.run(
            ["/bin/bash", "-c", f'source "{helper}"; snapshot_worktree'],
            cwd=tmp_path,
            capture_output=True,
            check=True,
            text=True,
        )
        return result.stdout

    baseline = snapshot()
    assert "directory-link -> directory" in baseline
    assert "dangling-link -> missing" in baseline

    (tmp_path / "regular.txt").write_text("after", encoding="utf-8")
    assert snapshot() != baseline
    (tmp_path / "regular.txt").write_text("before", encoding="utf-8")

    ignored_dir = tmp_path / ".venv"
    ignored_dir.mkdir()
    (ignored_dir / "new.py").write_text("pass", encoding="utf-8")
    assert snapshot() != baseline
    shutil.rmtree(ignored_dir)

    (tmp_path / "regular.txt").chmod(0o700)
    assert snapshot() != baseline
    (tmp_path / "regular.txt").chmod(0o644)

    (tmp_path / "directory-link").unlink()
    (tmp_path / "directory-link").symlink_to("other-directory", target_is_directory=True)
    assert snapshot() != baseline


def test_agentic_results_reject_symlinks_before_jq() -> None:
    """Merge inputs cannot redirect reads outside the allowed result directory."""
    workflow = WORKFLOW.read_text(encoding="utf-8")
    symlink_check = workflow.index("find agentic_results -type l -print -quit")
    first_result_read = workflow.index("result_files=(agentic_results/*.json)")
    assert symlink_check < first_result_read


def test_configured_agentic_failures_are_enforced() -> None:
    """Only missing authentication may skip agentic resolution successfully."""
    workflow = WORKFLOW.read_text(encoding="utf-8")
    assert 'echo "configured=true" >> "${GITHUB_OUTPUT}"' in workflow
    assert 'fail_agentic "Claude Code did not complete successfully"' in workflow
    assert "id: agentic-merge" in workflow
    assert "continue-on-error: true" in workflow
    assert "Enforce configured agentic resolution" in workflow
    assert "if: always() && steps.agentic.outputs.configured == 'true'" in workflow
    assert (
        workflow.index("Validate bundles with Databricks CLI")
        < workflow.index("Enforce configured agentic resolution")
        < workflow.index("Destroy ephemeral Azure resources")
    )
