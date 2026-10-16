"""Offline contracts for the live-integration workflow."""

from __future__ import annotations

import json
import os
import re
import shutil
import subprocess
import sys
from pathlib import Path

import pytest
import yaml

WORKFLOW = Path(__file__).parents[2] / ".github" / "workflows" / "integration.yml"
PUSH_WORKFLOW = WORKFLOW.with_name("push.yml")
SCRIPTS = WORKFLOW.parents[1] / "scripts"
AGENTIC_STRUCTURE_CHECK = SCRIPTS / "check-agentic-structure.py"
RESOLVE_AGENTIC = SCRIPTS / "resolve-agentic-gaps.sh"
SNAPSHOT_TREE = SCRIPTS / "snapshot-tree.sh"
VALIDATE_BUNDLES = SCRIPTS / "validate-bundles.sh"
WORKFLOW_AND_SCRIPTS = "\n".join(
    [WORKFLOW.read_text(encoding="utf-8"), *(path.read_text(encoding="utf-8") for path in sorted(SCRIPTS.iterdir()))]
)


def test_claude_runs_with_restricted_tools_outside_the_checkout() -> None:
    """Claude reads its prompt from stdin, has no Bash, edits only agentic_results/, and loads a plugin copy."""
    script = RESOLVE_AGENTIC.read_text(encoding="utf-8")
    invocation = re.search(r"if ! claude -p \\\n(?P<flags>.*?)<<CLAUDE_PROMPT", script, re.DOTALL)
    assert invocation, "Claude must receive its prompt from the CLAUDE_PROMPT heredoc"
    flags = invocation.group("flags")
    assert "--permission-mode dontAsk" in flags
    assert '--allowedTools="Read,Edit(agentic_results/**),Glob,Grep"' in flags
    assert "Bash" not in flags
    assert '--plugin-dir "${RUNNER_TEMP}/flowx-plugin"' in script


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

    helper = tmp_path / "snapshot-worktree.sh"
    helper.write_text(SNAPSHOT_TREE.read_text(encoding="utf-8"), encoding="utf-8")

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
    script = RESOLVE_AGENTIC.read_text(encoding="utf-8")
    symlink_check = script.index("find agentic_results -type l -print -quit")
    first_result_read = script.index("result_files=(agentic_results/*.json)")
    assert symlink_check < first_result_read


@pytest.mark.parametrize("depends_on", [None, []], ids=["missing", "empty"])
def test_agentic_structure_accepts_empty_dependency_representations(tmp_path: Path, depends_on) -> None:
    """Null, missing, and empty dependency lists represent the same graph edges."""
    expected = [{"pipeline": "pipeline", "activity_name": "activity", "task_key": "activity", "depends_on": None}]
    task = {"name": "activity", "task_key": "activity", "type": "WaitActivity"}
    if depends_on is not None:
        task["depends_on"] = depends_on
    report = {"name": "pipeline", "tasks": [task]}
    expected_path = tmp_path / "expected.json"
    report_path = tmp_path / "report.json"
    expected_path.write_text(json.dumps(expected), encoding="utf-8")
    report_path.write_text(json.dumps(report), encoding="utf-8")

    result = subprocess.run(
        [sys.executable, str(AGENTIC_STRUCTURE_CHECK), str(expected_path), str(report_path)],
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stderr


def test_agentic_structure_treats_dependency_edges_as_order_independent(tmp_path: Path) -> None:
    """IR DAG edges are a set even though their serialized representation is a list."""
    dependencies = [{"task_key": "first"}, {"task_key": "second", "outcome": "failed"}]
    expected = [
        {"pipeline": "pipeline", "activity_name": "activity", "task_key": "activity", "depends_on": dependencies}
    ]
    report = {
        "name": "pipeline",
        "tasks": [
            {"name": "activity", "task_key": "activity", "type": "WaitActivity", "depends_on": dependencies[::-1]}
        ],
    }
    expected_path = tmp_path / "expected.json"
    report_path = tmp_path / "report.json"
    expected_path.write_text(json.dumps(expected), encoding="utf-8")
    report_path.write_text(json.dumps(report), encoding="utf-8")

    result = subprocess.run(
        [sys.executable, str(AGENTIC_STRUCTURE_CHECK), str(expected_path), str(report_path)],
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stderr


def test_agentic_structure_reports_all_failures_without_source(tmp_path: Path) -> None:
    """Every bad replacement is reported together without printing generated code."""
    names = ["dependency", "key", "type", "empty-source", "invalid-source", "missing"]
    expected = [
        {
            "pipeline": "pipeline",
            "activity_name": name,
            "task_key": name,
            "depends_on": [{"task_key": "upstream"}] if name == "dependency" else [],
        }
        for name in names
    ]
    report = {
        "name": "pipeline",
        "tasks": [
            {"name": "dependency", "task_key": "dependency", "type": "WaitActivity", "depends_on": []},
            {"name": "key", "task_key": "changed", "type": "WaitActivity"},
            {"name": "type", "task_key": "type", "type": "UnknownActivity"},
            {"name": "empty-source", "task_key": "empty-source", "type": "NotebookActivity", "generated_source": ""},
            {
                "name": "invalid-source",
                "task_key": "invalid-source",
                "type": "NotebookActivity",
                "generated_source": "SECRET_INVALID_SOURCE(",
            },
        ],
    }
    expected_path = tmp_path / "expected.json"
    report_path = tmp_path / "report.json"
    expected_path.write_text(json.dumps(expected), encoding="utf-8")
    report_path.write_text(json.dumps(report), encoding="utf-8")

    result = subprocess.run(
        [sys.executable, str(AGENTIC_STRUCTURE_CHECK), str(expected_path), str(report_path)],
        capture_output=True,
        text=True,
    )
    assert result.returncode == 1
    for fragment in (
        "pipeline/dependency: depends_on:",
        "pipeline/key: task_key:",
        "pipeline/type: type:",
        "pipeline/empty-source: generated_source:",
        "pipeline/invalid-source: generated_source:",
        "pipeline/missing: task: expected present, actual missing",
    ):
        assert fragment in result.stderr
    assert "SECRET_INVALID_SOURCE" not in result.stderr


def test_required_bundle_variable_parser_uses_real_yaml_shapes(tmp_path: Path) -> None:
    """The embedded parser handles defaults, target values, and absent variables."""
    workflow = VALIDATE_BUNDLES.read_text(encoding="utf-8")
    match = re.search(
        r'"\$\{python_bin\}" - "\$\{bundle\}" 2>/dev/null <<\'PY\'\n(?P<body>.*?)\n\s*PY',
        workflow,
        re.DOTALL,
    )
    assert match, "required-variable parser must remain extractable"
    parser = re.sub(r"^ {10}", "", match.group("body"), flags=re.MULTILINE)

    fixtures = {
        "mixed.yml": (
            "variables:\n"
            "  with_default:\n"
            "    default: present\n"
            "  child_pipeline_job_id:\n"
            "    description: required\n"
            "  supplied_by_target:\n"
            "    description: target override\n"
            "targets:\n"
            "  dev:\n"
            "    variables:\n"
            "      supplied_by_target: configured\n",
            ["child_pipeline_job_id"],
        ),
        "missing.yml": ("bundle:\n  name: no-variables\n", []),
        "null.yml": ("variables: null\n", []),
    }
    for filename, (content, expected) in fixtures.items():
        bundle = tmp_path / filename
        bundle.write_text(content, encoding="utf-8")
        result = subprocess.run(
            [sys.executable, "-", str(bundle)],
            input=parser,
            capture_output=True,
            check=True,
            text=True,
        )
        assert result.stdout.splitlines() == expected


def test_bundle_parser_failure_fails_validation_step(tmp_path: Path) -> None:
    """A broken venv parser is reported and cannot silently omit --var flags."""
    broken_python = tmp_path / "broken-python"
    broken_python.write_text("#!/bin/sh\nexit 9\n", encoding="utf-8")
    broken_python.chmod(0o700)
    (tmp_path / ".migration-venv").write_text(f"{broken_python}\n", encoding="utf-8")
    bundle_dir = tmp_path / "output" / "broken-bundle"
    bundle_dir.mkdir(parents=True)
    (bundle_dir / "databricks.yml").write_text("variables: null\n", encoding="utf-8")

    result = subprocess.run(
        ["/bin/bash", str(VALIDATE_BUNDLES)],
        cwd=tmp_path,
        env={**os.environ, "FLOWX_OUTPUT": str(tmp_path / "output")},
        capture_output=True,
        check=False,
        text=True,
    )
    assert result.returncode != 0
    assert "Could not parse required variables for bundle broken-bundle." in result.stdout


def test_post_claude_steps_use_runner_temp_scripts() -> None:
    """Claude cannot alter scripts executed by later privileged or always-run steps."""
    workflow = WORKFLOW.read_text(encoding="utf-8")
    post_claude = workflow.split("- name: Report unexpected agentic-step failure", 1)[1]
    assert ".github/scripts/" not in post_claude
    referenced = re.findall(r'run: \'"\$\{CI_SCRIPTS\}/([^"/]+)"\'', post_claude)
    assert referenced == [
        "report-agentic-failure.sh",
        "verify-claude-worktree.sh",
        "merge-agentic-results.sh",
        "validate-agentic-structure.sh",
        "package-integration-results.sh",
        "check-databricks-credentials.sh",
        "validate-bundles.sh",
        "enforce-integration-results.sh",
        "teardown-integration.sh",
    ]


def test_workflow_script_references_are_executable() -> None:
    """Every workflow script reference resolves to an executable checked-in file."""
    workflows = WORKFLOW.read_text(encoding="utf-8") + PUSH_WORKFLOW.read_text(encoding="utf-8")
    names = set(re.findall(r"(?:\.github/scripts/|\$\{CI_SCRIPTS\}/)([A-Za-z0-9_.-]+)", workflows))
    names.update(re.findall(r"\$\{CI_SCRIPTS\}/([A-Za-z0-9_.-]+)", WORKFLOW_AND_SCRIPTS))
    assert names
    for name in names:
        path = SCRIPTS / name
        assert path.is_file(), name
        assert os.access(path, os.X_OK), name


def test_ci_script_bootstrap_is_job_local_and_precedes_uses() -> None:
    """Each job using the trusted copy must create it immediately after checkout."""
    workflow = yaml.safe_load(WORKFLOW.read_text(encoding="utf-8"))
    for job_name, job in workflow["jobs"].items():
        steps = job.get("steps", [])
        trusted_uses = [index for index, step in enumerate(steps) if "${CI_SCRIPTS}" in step.get("run", "")]
        direct_uses = [index for index, step in enumerate(steps) if ".github/scripts/" in step.get("run", "")]
        for index in direct_uses:
            assert steps[index].get("name") == "Copy trusted CI scripts outside the checkout", job_name
        if not trusted_uses:
            continue
        checkout = next(
            index for index, step in enumerate(steps) if str(step.get("uses", "")).startswith("actions/checkout@")
        )
        bootstrap = next(
            index
            for index, step in enumerate(steps)
            if step.get("name") == "Copy trusted CI scripts outside the checkout"
        )
        assert bootstrap == checkout + 1, job_name
        assert bootstrap < min(trusted_uses), job_name
