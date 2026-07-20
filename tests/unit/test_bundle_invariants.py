"""Unit tests for bundle structural-invariant checks (guard #2)."""

from __future__ import annotations

from flowx.validate.bundle_invariants import check_bundle_dir, check_job, check_resource_text, format_result


def _codes(findings) -> set[str]:
    return {f.code for f in findings}


def test_clean_job_has_no_findings():
    job = {
        "name": "p",
        "parameters": [{"name": "region", "default": "us"}],
        "tasks": [
            {
                "task_key": "a",
                "notebook_task": {"notebook_path": "/n", "base_parameters": {"region": "{{job.parameters.region}}"}},
            },
            {"task_key": "b", "depends_on": [{"task_key": "a"}], "notebook_task": {"notebook_path": "/n"}},
        ],
    }
    assert check_job("p", job) == []


def test_duplicate_job_parameter_flagged():
    job = {"parameters": [{"name": "region", "default": "us"}, {"name": "region", "default": "us"}], "tasks": []}
    assert "duplicate_job_parameter" in _codes(check_job("p", job))


def test_duplicate_task_key_flagged():
    job = {"tasks": [{"task_key": "a"}, {"task_key": "a"}]}
    assert "duplicate_task_key" in _codes(check_job("p", job))


def test_undeclared_job_parameter_reference_flagged():
    job = {
        "parameters": [{"name": "region"}],
        "tasks": [{"task_key": "a", "notebook_task": {"base_parameters": {"env": "{{job.parameters.env}}"}}}],
    }
    codes = _codes(check_job("p", job))
    assert "undeclared_job_parameter" in codes  # env is referenced but not declared


def test_dangling_depends_on_flagged():
    job = {"tasks": [{"task_key": "a", "depends_on": [{"task_key": "ghost"}]}]}
    assert "dangling_depends_on" in _codes(check_job("p", job))


def test_yaml_anchor_smell_flagged():
    # The exact shape PyYAML emits when the same object is in a list twice.
    text = (
        "resources:\n  jobs:\n    p:\n      name: p\n      tasks: []\n"
        "      parameters:\n      - &id001\n        name: region\n        default: us\n      - *id001\n"
    )
    findings = check_resource_text(text, filename="p.yml")
    codes = _codes(findings)
    assert "yaml_anchor" in codes
    # and the parsed structure also trips the duplicate-parameter invariant
    assert "duplicate_job_parameter" in codes


def test_dependency_cycle_flagged():
    job = {
        "tasks": [
            {"task_key": "a", "depends_on": [{"task_key": "b"}]},
            {"task_key": "b", "depends_on": [{"task_key": "a"}]},
        ]
    }
    assert "dependency_cycle" in _codes(check_job("p", job))


def test_acyclic_chain_has_no_cycle_finding():
    job = {
        "tasks": [
            {"task_key": "a"},
            {"task_key": "b", "depends_on": [{"task_key": "a"}]},
            {"task_key": "c", "depends_on": [{"task_key": "b"}]},
        ]
    }
    assert "dependency_cycle" not in _codes(check_job("p", job))


def test_bundle_job_reference_can_target_job_in_another_resource_file(tmp_path):
    resources = tmp_path / "resources"
    resources.mkdir()
    (resources / "parent.yml").write_text(
        "resources:\n"
        "  jobs:\n"
        "    parent:\n"
        "      tasks:\n"
        "        - task_key: call_child\n"
        "          run_job_task:\n"
        "            job_id: ${resources.jobs.child.id}\n",
        encoding="utf-8",
    )
    (resources / "child.yml").write_text(
        "resources:\n  jobs:\n    child:\n      tasks: []\n",
        encoding="utf-8",
    )

    result = check_bundle_dir(tmp_path)

    assert "dangling_run_job_reference" not in _codes(result.findings)


def test_bundle_job_reference_to_unknown_resource_is_flagged(tmp_path):
    resources = tmp_path / "resources"
    resources.mkdir()
    (resources / "parent.yml").write_text(
        "resources:\n"
        "  jobs:\n"
        "    parent:\n"
        "      tasks:\n"
        "        - task_key: call_missing\n"
        "          run_job_task:\n"
        "            job_id: ${resources.jobs.missing.id}\n",
        encoding="utf-8",
    )

    result = check_bundle_dir(tmp_path)
    finding = next(finding for finding in result.findings if finding.code == "dangling_run_job_reference")

    assert finding.severity == "warning"
    assert "parent.yml" in finding.location
    assert "call_missing" in finding.location
    assert "dangling_run_job_reference" in format_result(result)
