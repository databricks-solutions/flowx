"""Unit tests for the ordered multi-bundle deployer.

The deploy/summary subprocess (``databricks bundle …``) is always mocked — these tests never touch a
real workspace.
"""

from __future__ import annotations

import json
import subprocess

import pytest
import yaml

from flowx.bundler import deployer
from flowx.bundler.deployer import (
    AmbiguousLayoutError,
    CycleError,
    MissingDependencyError,
    _build_graph,
    _discover_bundles,
    _topo_sort,
    run,
)


def _make_bundle(root, bundle_dir, *, jobs, deps=None):
    """Writes a minimal per-pipeline bundle: databricks.yml + one resource YAML.

    Args:
        jobs: resource keys (job names) this bundle defines.
        deps: resource keys of sibling jobs to reference via ${var.<dep>} in a run_job_task.
    """
    bdir = root / bundle_dir
    (bdir / "resources").mkdir(parents=True)
    (bdir / "databricks.yml").write_text("bundle:\n  name: " + bundle_dir + "\n")
    resources = {"resources": {"jobs": {}}}
    for job in jobs:
        tasks = []
        for dep in deps or []:
            tasks.append({"task_key": f"call_{dep}", "run_job_task": {"job_id": f"${{var.{dep}}}"}})
        resources["resources"]["jobs"][job] = {"name": job, "tasks": tasks}
    (bdir / "resources" / f"{jobs[0]}.yml").write_text(yaml.safe_dump(resources))


# ---------------------------------------------------------------------------
# Discovery
# ---------------------------------------------------------------------------


class TestDiscovery:
    def test_reads_jobs_and_var_dependencies(self, tmp_path):
        _make_bundle(tmp_path, "a", jobs=["a"], deps=["b"])
        _make_bundle(tmp_path, "b", jobs=["b"])
        bundles = {bundle.bundle_dir: bundle for bundle in _discover_bundles(tmp_path)}
        assert set(bundles) == {"a", "b"}
        assert bundles["a"].resource_keys == {"a"}
        assert bundles["a"].depends_on == {"b"}
        assert bundles["b"].depends_on == set()

    def test_ignores_non_bundle_dirs(self, tmp_path):
        _make_bundle(tmp_path, "a", jobs=["a"])
        (tmp_path / "metadata").mkdir()  # not a bundle (no databricks.yml)
        (tmp_path / "deploy_manifest.json").write_text("{}")  # stray file
        assert {b.bundle_dir for b in _discover_bundles(tmp_path)} == {"a"}

    def test_inner_job_keys_collected(self, tmp_path):
        # A bundle whose resource file defines a parent + an inner ForEach job.
        _make_bundle(tmp_path, "b", jobs=["b", "b_foreach_body"])
        (bundle,) = _discover_bundles(tmp_path)
        assert bundle.resource_keys == {"b", "b_foreach_body"}

    def test_single_mode_root_bundle_discovered(self, tmp_path):
        # single-mode / single-pipeline layout: the sole bundle sits at output_dir root, not a subdir.
        _make_bundle(tmp_path, ".", jobs=["combined", "other"])
        (bundle,) = _discover_bundles(tmp_path)
        assert bundle.bundle_dir == "."
        assert bundle.resource_keys == {"combined", "other"}
        assert bundle.depends_on == set()


# ---------------------------------------------------------------------------
# Graph + topological sort
# ---------------------------------------------------------------------------


class TestTopoSort:
    def test_dependency_first_order(self, tmp_path):
        _make_bundle(tmp_path, "a", jobs=["a"], deps=["b"])
        _make_bundle(tmp_path, "b", jobs=["b"], deps=["c"])
        _make_bundle(tmp_path, "c", jobs=["c"])
        assert _topo_sort(_build_graph(_discover_bundles(tmp_path))) == ["c", "b", "a"]

    def test_inner_key_resolves_to_owning_bundle(self, tmp_path):
        # a depends on b_inner, an inner job of bundle b.
        _make_bundle(tmp_path, "a", jobs=["a"], deps=["b_inner"])
        _make_bundle(tmp_path, "b", jobs=["b", "b_inner"])
        assert _topo_sort(_build_graph(_discover_bundles(tmp_path))) == ["b", "a"]

    def test_cycle_raises(self, tmp_path):
        _make_bundle(tmp_path, "a", jobs=["a"], deps=["b"])
        _make_bundle(tmp_path, "b", jobs=["b"], deps=["a"])
        with pytest.raises(CycleError):
            _topo_sort(_build_graph(_discover_bundles(tmp_path)))

    def test_missing_dependency_raises(self, tmp_path):
        _make_bundle(tmp_path, "a", jobs=["a"], deps=["ghost"])
        with pytest.raises(MissingDependencyError):
            _build_graph(_discover_bundles(tmp_path))

    def test_missing_dependency_allowed(self, tmp_path):
        _make_bundle(tmp_path, "a", jobs=["a"], deps=["ghost"])
        graph = _build_graph(_discover_bundles(tmp_path), allow_missing_deps=True)
        assert graph == {"a": []}


# ---------------------------------------------------------------------------
# run(): ordering, --var injection, dry-run, failure handling
# ---------------------------------------------------------------------------


class _FakeCli:
    """Records commands and returns canned deploy/summary results.

    ``summary_ids`` maps a bundle_dir (cwd name) -> {resource_key: job_id} to synthesize the
    ``bundle summary`` JSON. ``fail_on`` names a bundle_dir whose deploy returns non-zero.
    """

    def __init__(self, summary_ids=None, fail_on=None):
        self.summary_ids = summary_ids or {}
        self.fail_on = fail_on
        self.commands = []

    def __call__(self, cmd, *, cwd):
        self.commands.append((cmd, str(cwd)))
        bundle_dir = str(cwd).rsplit("/", 1)[-1]
        if "summary" in cmd:
            jobs = {key: {"id": jid} for key, jid in self.summary_ids.get(bundle_dir, {}).items()}
            return subprocess.CompletedProcess(cmd, 0, stdout=json.dumps({"resources": {"jobs": jobs}}), stderr="")
        if self.fail_on == bundle_dir:
            return subprocess.CompletedProcess(cmd, 1, stdout="", stderr=f"boom in {bundle_dir}")
        return subprocess.CompletedProcess(cmd, 0, stdout="", stderr="")


def test_run_deploys_in_order_and_injects_captured_id(tmp_path, monkeypatch):
    _make_bundle(tmp_path, "parent", jobs=["parent"], deps=["child"])
    _make_bundle(tmp_path, "child", jobs=["child"])
    fake = _FakeCli(summary_ids={"child": {"child": 123}})
    monkeypatch.setattr(deployer, "_run_cli", fake)

    assert run(tmp_path, target="dev", profile="myprofile") == 0

    deploy_cwds = [cwd for cmd, cwd in fake.commands if "deploy" in cmd]
    # child deploys before parent.
    assert deploy_cwds.index(str(tmp_path / "child")) < deploy_cwds.index(str(tmp_path / "parent"))

    parent_cmd = next(cmd for cmd, cwd in fake.commands if "deploy" in cmd and cwd.endswith("parent"))
    assert "--var" in parent_cmd and "child=123" in parent_cmd
    assert "-p" in parent_cmd and "myprofile" in parent_cmd
    assert "-t" in parent_cmd and "dev" in parent_cmd
    # child (no deps) gets no --var.
    child_cmd = next(cmd for cmd, cwd in fake.commands if "deploy" in cmd and cwd.endswith("child"))
    assert "--var" not in child_cmd


def test_summary_receives_dependency_vars(tmp_path, monkeypatch):
    """`bundle summary` for a dependent must re-pass its --var or it errors on the unset variable."""
    _make_bundle(tmp_path, "parent", jobs=["parent"], deps=["child"])
    _make_bundle(tmp_path, "child", jobs=["child"])
    fake = _FakeCli(summary_ids={"child": {"child": 55}, "parent": {"parent": 66}})
    monkeypatch.setattr(deployer, "_run_cli", fake)

    assert run(tmp_path) == 0
    parent_summary = next(cmd for cmd, cwd in fake.commands if "summary" in cmd and cwd.endswith("parent"))
    assert "--var" in parent_summary and "child=55" in parent_summary


def test_dry_run_makes_no_calls(tmp_path, monkeypatch):
    _make_bundle(tmp_path, "parent", jobs=["parent"], deps=["child"])
    _make_bundle(tmp_path, "child", jobs=["child"])
    fake = _FakeCli()
    monkeypatch.setattr(deployer, "_run_cli", fake)

    assert run(tmp_path, dry_run=True) == 0
    assert fake.commands == []


def test_deploy_failure_stops_before_dependents(tmp_path, monkeypatch):
    _make_bundle(tmp_path, "parent", jobs=["parent"], deps=["child"])
    _make_bundle(tmp_path, "child", jobs=["child"])
    fake = _FakeCli(fail_on="child")  # child deploys first and fails
    monkeypatch.setattr(deployer, "_run_cli", fake)

    assert run(tmp_path) == 1
    deployed = [cwd for cmd, cwd in fake.commands if "deploy" in cmd]
    assert not any(cwd.endswith("parent") for cwd in deployed)


def test_uncaptured_dependency_id_blocks_caller_with_clear_error(tmp_path, monkeypatch, capsys):
    """If a callee's job id isn't captured, the caller is blocked with an actionable message."""
    _make_bundle(tmp_path, "parent", jobs=["parent"], deps=["child"])
    _make_bundle(tmp_path, "child", jobs=["child"])
    # child deploys fine but its summary yields no id (transient summary failure) -> capture miss.
    fake = _FakeCli(summary_ids={})
    monkeypatch.setattr(deployer, "_run_cli", fake)

    assert run(tmp_path) == 1
    err = capsys.readouterr().err
    assert "BLOCKED" in err
    assert "child" in err
    # parent's deploy was never attempted.
    assert not any("deploy" in cmd and cwd.endswith("parent") for cmd, cwd in fake.commands)


def test_empty_output_dir_returns_error(tmp_path):
    assert run(tmp_path) == 1


def test_cycle_returns_error(tmp_path, monkeypatch):
    _make_bundle(tmp_path, "a", jobs=["a"], deps=["b"])
    _make_bundle(tmp_path, "b", jobs=["b"], deps=["a"])
    monkeypatch.setattr(deployer, "_run_cli", _FakeCli())
    assert run(tmp_path) == 1


class TestAmbiguousLayout:
    """A root databricks.yml *and* subdirectory bundles both present (a mode-switch on one output dir,
    since package never clears the dir) must be refused, not silently resolved to one layout."""

    def test_root_and_subdir_bundles_raise(self, tmp_path):
        # A stale single-mode bundle at the root, then a per-pipeline re-package writing subdir bundles.
        (tmp_path / "databricks.yml").write_text("bundle:\n  name: stale_single\n")
        _make_bundle(tmp_path, "a", jobs=["a"])
        _make_bundle(tmp_path, "b", jobs=["b"])
        with pytest.raises(AmbiguousLayoutError) as excinfo:
            _discover_bundles(tmp_path)
        # The message names both stale subdirs so the operator knows what to clear.
        assert "a" in str(excinfo.value) and "b" in str(excinfo.value)

    def test_run_reports_ambiguous_layout_and_deploys_nothing(self, tmp_path, monkeypatch):
        (tmp_path / "databricks.yml").write_text("bundle:\n  name: stale_single\n")
        _make_bundle(tmp_path, "a", jobs=["a"])
        fake = _FakeCli()
        monkeypatch.setattr(deployer, "_run_cli", fake)
        assert run(tmp_path) == 1
        # No deploy was attempted — the ambiguity is caught before any CLI call.
        assert fake.commands == []

    def test_root_only_still_deploys_directly(self, tmp_path):
        # Only a root bundle (clean single-mode dir) is unambiguous and returns the '.' bundle.
        (tmp_path / "databricks.yml").write_text("bundle:\n  name: only_single\n")
        (tmp_path / "resources").mkdir()
        (tmp_path / "resources" / "j.yml").write_text(
            yaml.safe_dump({"resources": {"jobs": {"j": {"name": "j", "tasks": []}}}})
        )
        bundles = _discover_bundles(tmp_path)
        assert [b.bundle_dir for b in bundles] == ["."]
