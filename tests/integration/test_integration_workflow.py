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
ALL_WORKFLOWS = sorted(WORKFLOW.parent.glob("*.yml"))
SCRIPTS = WORKFLOW.parents[1] / "scripts"
AGENTIC_STRUCTURE_CHECK = SCRIPTS / "check-agentic-structure.py"
RESOLVE_AGENTIC = SCRIPTS / "resolve-agentic-gaps.sh"
SNAPSHOT_TREE = SCRIPTS / "snapshot-tree.sh"
VALIDATE_BUNDLES = SCRIPTS / "validate-bundles.sh"
INSTALL_PINNED_TOOL = SCRIPTS / "install-pinned-tool.sh"
WORKFLOW_AND_SCRIPTS = "\n".join(
    [WORKFLOW.read_text(encoding="utf-8"), *(path.read_text(encoding="utf-8") for path in sorted(SCRIPTS.iterdir()))]
)


def test_claude_runs_with_restricted_tools_outside_the_checkout(tmp_path: Path) -> None:
    """Claude runs restricted in dontAsk mode with no Bash and a deny list, and loads a read-only plugin copy.

    The copy is made in RUNNER_TEMP and made read-only before the worktree snapshot, and acceptEdits is never used.
    """
    flags, prompt = _claude_invocation(tmp_path)
    assert "--restricted" in flags
    assert flags[flags.index("--permission-mode") + 1] == "dontAsk"
    assert flags[flags.index("--tools") + 1] == "Read,Edit,Write,Glob,Grep,Skill"
    assert flags[flags.index("--disallowedTools") + 1]
    plugin_dir = str(tmp_path / "temp" / "flowx-plugin")
    assert flags[flags.index("--plugin-dir") + 1] == plugin_dir
    assert _values(flags, "--add-dir") == [str(tmp_path / "temp" / "adf-export"), plugin_dir]
    assert "Bash" not in " ".join(flags)
    assert "acceptEdits" not in WORKFLOW_AND_SCRIPTS
    assert "PlaceholderActivity" in prompt
    script = RESOLVE_AGENTIC.read_text(encoding="utf-8")
    steps = [
        'plugin_dir="${RUNNER_TEMP}/flowx-plugin"',
        'cp -R .claude-plugin skills "${plugin_dir}/"',
        'chmod -R a-w "${plugin_dir}"',
        'snapshot_worktree > "${worktree_snapshot}"',
    ]
    assert all(step in script for step in steps)
    assert [script.index(step) for step in steps] == sorted(script.index(step) for step in steps)


def test_claude_reads_and_searches_only_its_inputs(tmp_path: Path) -> None:
    """Read, Glob, and Grep share one scoped allow list, and credential caches, /proc, and .git are denied."""
    flags, _ = _claude_invocation(tmp_path)
    allowed = _rules(flags[flags.index("--allowedTools") + 1])
    denied = _rules(flags[flags.index("--disallowedTools") + 1])
    allowed_dirs = {
        "./flowx_output/**",
        f"/{tmp_path}/temp/adf-export/**",
        f"/{tmp_path}/temp/flowx-plugin/**",
        "./agentic_results/**",
    }
    for tool in ("Read", "Glob", "Grep"):
        assert {path for name, path in allowed if name == tool} == allowed_dirs, tool
    assert ("Edit", "agentic_results/**") in allowed
    assert all(path for _, path in allowed), "every allowed tool must be scoped to a path"
    for path in ("//proc/**", "~/.azure/**", "~/.config/**", "./.git/**", f"/{tmp_path}/state/**"):
        assert ("Read", path) in denied


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


def test_agentic_structure_does_not_echo_values_claude_wrote(tmp_path: Path) -> None:
    """Failure messages name the field, never the unexpected value, so hostile output can't reach the log."""
    hostile = "::warning::HOSTILE_VALUE"
    expected = [{"pipeline": "pipeline", "activity_name": "activity", "task_key": "activity", "depends_on": []}]
    task = {"name": "activity", "type": hostile, "task_key": hostile, "depends_on": [{"task_key": hostile}]}
    expected_path = tmp_path / "expected.json"
    report_path = tmp_path / "report.json"
    expected_path.write_text(json.dumps(expected), encoding="utf-8")
    report_path.write_text(json.dumps({"name": "pipeline", "tasks": [task]}), encoding="utf-8")

    result = subprocess.run(
        [sys.executable, str(AGENTIC_STRUCTURE_CHECK), str(expected_path), str(report_path)],
        capture_output=True,
        text=True,
    )
    assert result.returncode == 1
    for field in ("type:", "task_key:", "depends_on:"):
        assert f"pipeline/activity: {field}" in result.stderr
    assert "HOSTILE_VALUE" not in result.stderr


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
    (tmp_path / ".venv" / "bin").mkdir(parents=True)
    (tmp_path / ".venv" / "bin" / "python").symlink_to(broken_python)
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
    steps = _live_steps()
    after_claude = steps[_step_index(steps, "Resolve agentic gaps with Claude Code") + 1 :]
    runs = [step["run"] for step in after_claude if "run" in step]
    assert not any(".github/scripts/" in run for run in runs)
    assert [re.search(r"\$\{CI_SCRIPTS\}/([^\"]+)", run).group(1) for run in runs] == [
        "report-agentic-failure.sh",
        "verify-claude-worktree.sh",
        "merge-agentic-results.sh",
        "validate-agentic-structure.sh",
        "package-integration-results.sh",
        "validate-bundles.sh",
        "enforce-integration-results.sh",
        "teardown-integration.sh",
    ]


def test_every_checkout_disables_persisted_credentials() -> None:
    """No checkout leaves a GitHub token in .git/config for later steps, or Claude, to find."""
    checkouts = [
        step
        for path in ALL_WORKFLOWS
        for job in yaml.safe_load(path.read_text(encoding="utf-8"))["jobs"].values()
        for step in job.get("steps", [])
        if str(step.get("uses", "")).startswith("actions/checkout@")
    ]
    assert len(checkouts) >= 5
    assert all(step.get("with", {}).get("persist-credentials") is False for step in checkouts)


def test_live_runs_queue_instead_of_overlapping() -> None:
    """All runs share one resource group, so the live job uses one fixed group and never cancels a deploy."""
    workflow = yaml.safe_load(WORKFLOW.read_text(encoding="utf-8"))
    assert "concurrency" not in workflow
    concurrency = workflow["jobs"]["integration"]["concurrency"]
    assert concurrency == {"group": "flowx-live-integration", "cancel-in-progress": False}


def test_installs_happen_before_azure_login_from_pinned_sources() -> None:
    """Packages and tools come from lock files or pinned checksums, all before Azure credentials exist."""
    steps = _live_steps()
    login = next(index for index, step in enumerate(steps) if str(step.get("uses", "")).startswith("azure/login@"))
    installs = [
        _step_index(steps, "Install flowx from the lock file"),
        _step_index(steps, "Install Terraform"),
        _step_index(steps, "Install Claude Code"),
        _step_index(steps, "Install Databricks CLI"),
    ]
    assert max(installs) < login
    assert steps[installs[0]]["run"] == "uv sync --frozen"
    assert not any(str(step.get("uses", "")).startswith(("hashicorp/", "databricks/")) for step in steps)
    for step in steps:
        assert not re.search(r"bootstrap\.sh|pip install|npm install", step.get("run", ""))
    assert "npm ci --ignore-scripts" in (SCRIPTS / "install-claude-code.sh").read_text(encoding="utf-8")
    assert "sha256sum --check --strict" in INSTALL_PINNED_TOOL.read_text(encoding="utf-8")


def test_azure_cli_is_signed_out_before_claude_and_teardown_always_runs() -> None:
    """Claude never runs while the Azure CLI token cache exists, and destroy runs even after failures."""
    steps = _live_steps()
    export = _step_index(steps, "Export deployed ADF definitions")
    sign_out = _step_index(steps, "Sign out of Azure CLI before Claude runs")
    claude = _step_index(steps, "Resolve agentic gaps with Claude Code")
    assert export < sign_out < claude
    assert steps[sign_out]["if"] == "always()"
    assert 'rm -rf "${HOME}/.azure"' in (SCRIPTS / "sign-out-azure-cli.sh").read_text(encoding="utf-8")
    teardown = steps[_step_index(steps, "Destroy ephemeral Azure resources")]
    assert teardown["if"] == "always()"
    assert teardown is steps[-1]


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
            assert steps[index].get("name") == "Copy CI scripts out of Claude's reach", job_name
        if not trusted_uses:
            continue
        checkout = next(
            index for index, step in enumerate(steps) if str(step.get("uses", "")).startswith("actions/checkout@")
        )
        bootstrap = next(
            index for index, step in enumerate(steps) if step.get("name") == "Copy CI scripts out of Claude's reach"
        )
        assert bootstrap == checkout + 1, job_name
        assert bootstrap < min(trusted_uses), job_name


def _live_steps() -> list[dict]:
    """Return the steps of the live integration job."""
    return yaml.safe_load(WORKFLOW.read_text(encoding="utf-8"))["jobs"]["integration"]["steps"]


def _step_index(steps: list[dict], name: str) -> int:
    """Return the position of the step with this exact name."""
    return next(index for index, step in enumerate(steps) if step.get("name") == name)


def _claude_invocation(tmp_path: Path) -> tuple[list[str], str]:
    """Run resolve-agentic-gaps.sh with a stub Claude CLI and return the arguments and prompt it received."""
    if shutil.which("jq") is None:
        pytest.skip("jq is required for the Claude invocation test")
    root = WORKFLOW.parents[2]
    workspace = tmp_path / "workspace"
    (workspace / ".github" / "scripts").mkdir(parents=True)
    # The real snapshot needs GNU find and isn't what this checks.
    (workspace / ".github" / "scripts" / "snapshot-tree.sh").write_text(
        "snapshot_worktree() { :; }\n", encoding="utf-8"
    )
    for directory in (".claude-plugin", "skills"):
        shutil.copytree(root / directory, workspace / directory)
    (workspace / "flowx_output" / ".work").mkdir(parents=True)
    report = {"name": "pipeline", "tasks": [{"name": "gap", "type": "PlaceholderActivity", "task_key": "gap"}]}
    (workspace / "flowx_output" / ".work" / "translation_report.json").write_text(json.dumps(report), encoding="utf-8")
    (tmp_path / "temp" / "adf-export").mkdir(parents=True)
    for directory in ("state", "data"):
        (tmp_path / directory).mkdir()
    arguments_log = tmp_path / "claude-arguments.json"
    prompt_log = tmp_path / "claude-prompt"
    claude = tmp_path / "claude"
    claude.write_text(
        f"""#!{sys.executable}
import json, sys
open({str(arguments_log)!r}, "w").write(json.dumps(sys.argv[1:]))
open({str(prompt_log)!r}, "w").write(sys.stdin.read())
""",
        encoding="utf-8",
    )
    claude.chmod(0o755)
    environment = {
        **os.environ,
        "CLAUDE_CODE_OAUTH_TOKEN": "stub",
        "CLAUDE_BIN": str(claude),
        "FLOWX_OUTPUT": "flowx_output",
        "ADF_EXPORT_DIR": str(tmp_path / "temp" / "adf-export"),
        "RUNNER_TEMP": str(tmp_path / "temp"),
        "GITHUB_WORKSPACE": str(workspace),
        "GITHUB_OUTPUT": str(tmp_path / "github-output"),
        "GITHUB_STEP_SUMMARY": str(tmp_path / "summary"),
        "TF_STATE_DIR": str(tmp_path / "state"),
        "TF_DATA_DIR": str(tmp_path / "data"),
    }
    try:
        subprocess.run(["bash", str(RESOLVE_AGENTIC)], cwd=workspace, env=environment, capture_output=True, check=False)
    finally:
        for directory in ("state", "data"):
            (tmp_path / directory).chmod(0o700)
        subprocess.run(["chmod", "-R", "u+w", str(tmp_path / "temp")], check=False)
    flags = [flag for argument in json.loads(arguments_log.read_text()) for flag in argument.split("=", 1)]
    return flags, prompt_log.read_text()


def _values(flags: list[str], name: str) -> list[str]:
    """Return every value passed for a repeated command-line flag."""
    return [flags[index + 1] for index, flag in enumerate(flags) if flag == name]


def _rules(rule_list: str) -> set[tuple[str, str]]:
    """Split a comma-separated permission list such as "Read(./a/**),Grep" into (tool, path) pairs."""
    rules = set()
    for rule in rule_list.split(","):
        match = re.fullmatch(r"(?P<tool>\w+)(?:\((?P<path>.*)\))?", rule)
        assert match, rule
        rules.add((match.group("tool"), match.group("path") or ""))
    return rules
