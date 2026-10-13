"""Offline contracts for the live-integration workflow."""

from __future__ import annotations

import os
import re
import shutil
import subprocess
import sys
from pathlib import Path

import pytest
import yaml

WORKFLOW = Path(__file__).parents[2] / ".github" / "workflows" / "integration.yml"


def test_claude_prompt_uses_stdin_and_scoped_edit_permissions() -> None:
    """Variadic permission flags cannot consume the prompt or grant broad writes."""
    workflow = WORKFLOW.read_text(encoding="utf-8")
    invocation = re.search(r"if ! claude -p \\\n(?P<flags>.*?)<<CLAUDE_PROMPT", workflow, re.DOTALL)
    assert invocation, "Claude must receive its prompt from the CLAUDE_PROMPT heredoc"
    flags = invocation.group("flags")
    assert "--permission-mode dontAsk" in flags
    assert "acceptEdits" not in workflow
    assert '--tools="Read,Edit,Write,Glob,Grep,Skill"' in flags
    assert '--allowedTools="Read,Edit(agentic_results/**),Glob,Grep"' in flags
    assert '--disallowedTools="Read(' in flags
    assert "Write(agentic_results/**)" not in workflow
    assert "--allowedTools " not in flags
    assert "--disallowedTools " not in flags
    assert "CLAUDE_PROMPT\n          then" in workflow
    assert "Bash" not in flags


def test_claude_plugin_is_copied_outside_the_checkout() -> None:
    """dontAsk can edit results only when the plugin is outside the working tree."""
    workflow = WORKFLOW.read_text(encoding="utf-8")
    assert "--plugin-dir ." not in workflow
    assert '--plugin-dir "${RUNNER_TEMP}/flowx-plugin"' in workflow
    assert 'cp -R .claude-plugin skills "${plugin_dir}/"' in workflow
    assert 'chmod -R a-w "${plugin_dir}"' in workflow
    assert workflow.index('cp -R .claude-plugin skills "${plugin_dir}/"') < workflow.index(
        'snapshot_worktree > "${worktree_snapshot}"'
    )


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
    assert "Enforce live integration results" in workflow
    assert "if: always()" in workflow
    assert 'if [[ "${AGENTIC_CONFIGURED}" != true ]]' in workflow
    assert (
        workflow.index("Validate bundles with Databricks CLI")
        < workflow.index("Enforce live integration results")
        < workflow.index("Destroy ephemeral Azure resources")
    )


def test_live_export_includes_dataflows() -> None:
    """Claude receives referenced mapping-data-flow definitions from ADF."""
    workflow = WORKFLOW.read_text(encoding="utf-8")
    assert '"${export_dir}"/{pipelines,datasets,linked_services,dataflows,integration_runtimes,triggers}' in workflow
    assert '"dataflows:dataflows"' in workflow
    assert '"integrationruntimes:integration_runtimes"' in workflow


def test_live_workflow_runs_expected_output_cases_and_not_legacy_quality_script() -> None:
    """One deployment feeds the same case suite used by offline CI."""
    workflow = WORKFLOW.read_text(encoding="utf-8")
    assert "Compare live ADF cases with expected outputs" in workflow
    assert "id: adf-cases" in workflow
    assert "ADF_CASES_OUTCOME: ${{ steps.adf-cases.outcome }}" in workflow
    case_step = workflow.split("- name: Compare live ADF cases with expected outputs", 1)[1].split("- name:", 1)[0]
    assert "continue-on-error: true" in case_step
    assert "PYTHONPATH=src uv run pytest tests/integration/test_adf_cases.py -v" in workflow
    assert "ADF_CASE_SOURCE_DIR=${export_dir}" in workflow
    assert "assert_quality.py" not in workflow
    assert workflow.index("Validate bundles with Databricks CLI") < workflow.index("Enforce live integration results")


def test_merged_agentic_outputs_receive_structural_case_validation() -> None:
    """Merged placeholders and every case bundle are structurally checked."""
    workflow = WORKFLOW.read_text(encoding="utf-8")
    assert "expected-agentic-structure.json" in workflow
    assert "id: agentic-structure" in workflow
    assert "STRUCTURE_OUTCOME: ${{ steps.agentic-structure.outcome }}" in workflow
    assert 'assert task["type"] in valid_types' in workflow
    assert 'if task["type"] == "NotebookActivity":' in workflow
    assert "ast.parse(source)" in workflow
    assert 'cases_root.glob("*/case.json")' in workflow
    assert "check_bundle_dir(bundle_dir)" in workflow
    assert 'bundle_by_pipeline[job["name"]] = bundle_dir' in workflow


def test_bundle_validation_supplies_required_variables_and_collects_failures() -> None:
    """Validate-only runs substitute required inputs and report every bad bundle."""
    workflow = WORKFLOW.read_text(encoding="utf-8")
    assert 'python_bin="$(cat .migration-venv)"' in workflow
    assert "realpath" not in workflow
    assert 'if ! required_output="$(' in workflow
    assert "Could not parse required variables for bundle %s" in workflow
    assert "name not in target_variables" in workflow
    assert 'validate_args+=(--var "${variable}=${placeholder}")' in workflow
    assert '[[ "${variable}" == *_job_id ]] && placeholder=0' in workflow
    assert 'failed_bundles+=("${bundle_name}")' in workflow
    assert "Bundle validation failed for: %s" in workflow


def test_required_bundle_variable_parser_uses_real_yaml_shapes(tmp_path: Path) -> None:
    """The embedded parser handles defaults, target values, and absent variables."""
    workflow = WORKFLOW.read_text(encoding="utf-8")
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
    workflow = yaml.safe_load(WORKFLOW.read_text(encoding="utf-8"))
    step = next(
        step
        for step in workflow["jobs"]["integration"]["steps"]
        if step.get("name") == "Validate bundles with Databricks CLI"
    )
    broken_python = tmp_path / "broken-python"
    broken_python.write_text("#!/bin/sh\nexit 9\n", encoding="utf-8")
    broken_python.chmod(0o700)
    (tmp_path / ".migration-venv").write_text(f"{broken_python}\n", encoding="utf-8")
    bundle_dir = tmp_path / "output" / "broken-bundle"
    bundle_dir.mkdir(parents=True)
    (bundle_dir / "databricks.yml").write_text("variables: null\n", encoding="utf-8")

    result = subprocess.run(
        ["/bin/bash", "-c", step["run"]],
        cwd=tmp_path,
        env={**os.environ, "FLOWX_OUTPUT": str(tmp_path / "output")},
        capture_output=True,
        check=False,
        text=True,
    )
    assert result.returncode != 0
    assert "Could not parse required variables for bundle broken-bundle." in result.stdout
