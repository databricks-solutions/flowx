"""Unit tests for bundle structural-invariant checks (guard #2)."""

from __future__ import annotations

from flowx.validate.bundle_invariants import check_job, check_resource_text


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
