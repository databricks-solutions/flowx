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
VERIFY_CLAUDE = SCRIPTS / "verify-claude-worktree.sh"
EXPORT_ADF = SCRIPTS / "export-adf.sh"
PACKAGE_RESULTS = SCRIPTS / "package-integration-results.sh"
VALIDATE_BUNDLES = SCRIPTS / "validate-bundles.sh"
ENFORCE_RESULTS = SCRIPTS / "enforce-integration-results.sh"
WORKFLOW_AND_SCRIPTS = "\n".join(
    [WORKFLOW.read_text(encoding="utf-8"), *(path.read_text(encoding="utf-8") for path in sorted(SCRIPTS.iterdir()))]
)


def test_claude_prompt_uses_stdin_and_scoped_edit_permissions() -> None:
    """Variadic permission flags cannot consume the prompt or grant broad writes."""
    script = RESOLVE_AGENTIC.read_text(encoding="utf-8")
    invocation = re.search(r"if ! claude -p \\\n(?P<flags>.*?)<<CLAUDE_PROMPT", script, re.DOTALL)
    assert invocation, "Claude must receive its prompt from the CLAUDE_PROMPT heredoc"
    flags = invocation.group("flags")
    assert "--permission-mode dontAsk" in flags
    assert "acceptEdits" not in WORKFLOW_AND_SCRIPTS
    assert '--tools="Read,Edit,Write,Glob,Grep,Skill"' in flags
    assert '--allowedTools="Read,Edit(agentic_results/**),Glob,Grep"' in flags
    assert '--disallowedTools="Read(' in flags
    assert "Write(agentic_results/**)" not in WORKFLOW_AND_SCRIPTS
    assert "--allowedTools " not in flags
    assert "--disallowedTools " not in flags
    assert "CLAUDE_PROMPT\nthen" in script
    assert "Bash" not in flags


def test_claude_plugin_is_copied_outside_the_checkout() -> None:
    """dontAsk can edit results only when the plugin is outside the working tree."""
    script = RESOLVE_AGENTIC.read_text(encoding="utf-8")
    assert "--plugin-dir ." not in WORKFLOW_AND_SCRIPTS
    assert '--plugin-dir "${RUNNER_TEMP}/flowx-plugin"' in script
    assert 'cp -R .claude-plugin skills "${plugin_dir}/"' in script
    assert 'chmod -R a-w "${plugin_dir}"' in script
    assert script.index('cp -R .claude-plugin skills "${plugin_dir}/"') < script.index(
        'snapshot_worktree > "${worktree_snapshot}"'
    )


def test_agentic_prompt_and_coverage_share_expected_task_source() -> None:
    """The prompt and exact-coverage gate use one PlaceholderActivity list."""
    script = RESOLVE_AGENTIC.read_text(encoding="utf-8")
    assert 'expected_tasks_file="${RUNNER_TEMP}/expected-agentic-tasks.json"' in script
    assert 'select(.type? == "PlaceholderActivity")' in script
    assert '$(cat "${expected_tasks_file}")' in script
    assert "jq -c '[.[] | [.pipeline, .activity_name]] | sort' \"${expected_tasks_file}\"" in script
    assert "strategy is" not in script
    assert 'status is "pending"' not in script
    assert "generated_source field" in script
    assert '.task.generated_source | (type == "string" and length > 0)' in script
    assert ".task.notebook_path" not in script


def test_agentic_guard_covers_ignored_files() -> None:
    """Claude confinement compares all non-result worktree file content."""
    workflow = WORKFLOW.read_text(encoding="utf-8")
    scripts = RESOLVE_AGENTIC.read_text() + VERIFY_CLAUDE.read_text()
    assert "pre-claude-worktree.sha256" in scripts
    assert "post-claude-worktree.sha256" in scripts
    assert "if: always() && steps.agentic.outputs.configured == 'true'" in workflow
    assert SNAPSHOT_TREE.read_text().count("snapshot_worktree()") == 1
    assert scripts.count('source "${RUNNER_TEMP}/snapshot-worktree.sh"') == 2
    assert "including ignored files" in scripts


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


def test_configured_agentic_failures_are_enforced() -> None:
    """Only missing authentication may skip agentic resolution successfully."""
    workflow = WORKFLOW.read_text(encoding="utf-8")
    assert 'echo "configured=true" >> "${GITHUB_OUTPUT}"' in RESOLVE_AGENTIC.read_text()
    assert 'fail_agentic "Claude Code did not complete successfully"' in RESOLVE_AGENTIC.read_text()
    assert "id: agentic-merge" in workflow
    assert "continue-on-error: true" in workflow
    assert "Enforce live integration results" in workflow
    assert "if: always()" in workflow
    assert 'if [[ "${AGENTIC_CONFIGURED}" != true ]]' in ENFORCE_RESULTS.read_text()
    assert (
        workflow.index("Validate bundles with Databricks CLI")
        < workflow.index("Enforce live integration results")
        < workflow.index("Destroy ephemeral Azure resources")
    )


def test_live_export_includes_dataflows() -> None:
    """Claude receives referenced mapping-data-flow definitions from ADF."""
    script = EXPORT_ADF.read_text(encoding="utf-8")
    assert '"${export_dir}"/{pipelines,datasets,linked_services,dataflows,integration_runtimes,triggers}' in script
    assert '"dataflows:dataflows"' in script
    assert '"integrationruntimes:integration_runtimes"' in script


def test_live_workflow_runs_expected_output_cases_and_not_legacy_quality_script() -> None:
    """One deployment feeds the same case suite used by offline CI."""
    workflow = WORKFLOW.read_text(encoding="utf-8")
    assert "Compare live ADF cases with expected outputs" in workflow
    assert "id: adf-cases" in workflow
    assert "ADF_CASES_OUTCOME: ${{ steps.adf-cases.outcome }}" in workflow
    case_step = workflow.split("- name: Compare live ADF cases with expected outputs", 1)[1].split("- name:", 1)[0]
    assert "continue-on-error: true" in case_step
    assert "PYTHONPATH=src uv run pytest tests/integration/test_adf_cases.py -v --tb=short" in workflow
    assert "ADF_CASE_SOURCE_DIR=${export_dir}" in EXPORT_ADF.read_text()
    assert "assert_quality.py" not in workflow
    assert workflow.index("Validate bundles with Databricks CLI") < workflow.index("Enforce live integration results")


def test_merged_agentic_outputs_receive_structural_case_validation() -> None:
    """Merged placeholders and every case bundle are structurally checked."""
    workflow = WORKFLOW.read_text(encoding="utf-8")
    assert "expected-agentic-structure.json" in RESOLVE_AGENTIC.read_text()
    assert "id: agentic-structure" in workflow
    assert "STRUCTURE_OUTCOME: ${{ steps.agentic-structure.outcome }}" in workflow
    assert "depends_on: (.depends_on // [])" in RESOLVE_AGENTIC.read_text()
    assert "${CI_SCRIPTS}/check-agentic-structure.py" in (SCRIPTS / "validate-agentic-structure.sh").read_text()
    package = PACKAGE_RESULTS.read_text()
    assert 'cases_root.glob("*/case.json")' in package
    assert "check_bundle_dir(bundle_dir)" in package
    assert 'bundle_by_pipeline[job["name"]] = bundle_dir' in package


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


def test_bundle_validation_supplies_required_variables_and_collects_failures() -> None:
    """Validate-only runs substitute required inputs and report every bad bundle."""
    script = VALIDATE_BUNDLES.read_text(encoding="utf-8")
    assert 'python_bin="$(cat .migration-venv)"' in script
    assert "realpath" not in script
    assert 'if ! required_output="$(' in script
    assert "Could not parse required variables for bundle %s" in script
    assert "name not in target_variables" in script
    assert 'validate_args+=(--var "${variable}=${placeholder}")' in script
    assert '[[ "${variable}" == *_job_id ]] && placeholder=0' in script
    assert 'failed_bundles+=("${bundle_name}")' in script
    assert "Bundle validation failed for: %s" in script


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
