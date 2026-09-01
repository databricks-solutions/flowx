"""Unit tests for configurable bundle packaging modes (single / per-pipeline / per-group)."""

from __future__ import annotations

import json

import pytest
import yaml

from flowx.bundler.dab_writer import (
    MalformedReportError,
    _group_workflows,
    _load_group_spec,
    _load_report,
    _prefixed_notebook_relative_path,
    write_bundle_group,
)
from flowx.models.ir import (
    CopyActivity,
    ExecutePipelineActivity,
    ForEachActivity,
    NotebookActivity,
    Pipeline,
    SparkPythonActivity,
    WaitActivity,
)
from flowx.preparer.workflow_preparer import prepare_workflow


def _workflow(name: str, calls: list[str] | None = None):
    tasks: list = [WaitActivity(name="wait", task_key="wait", wait_time_seconds=1)]
    for index, callee in enumerate(calls or []):
        tasks.append(ExecutePipelineActivity(name=f"call_{index}", task_key=f"call_{index}", pipeline_name=callee))
    return prepare_workflow(Pipeline(name=name, tasks=tasks))


class TestGroupWorkflows:
    def test_per_pipeline_one_group_each(self):
        wfs = [_workflow("a"), _workflow("b")]
        groups = _group_workflows(wfs, mode="per-pipeline")
        assert sorted(name for name, _ in groups) == ["a", "b"]
        assert all(len(members) == 1 for _, members in groups)

    def test_single_folds_all_into_one(self):
        wfs = [_workflow("a"), _workflow("b")]
        groups = _group_workflows(wfs, mode="single")
        assert len(groups) == 1
        name, members = groups[0]
        assert name == "flowx_bundle"
        assert {m.name for m in members} == {"a", "b"}

    def test_single_uses_bundle_name_when_given(self):
        groups = _group_workflows([_workflow("a"), _workflow("b")], mode="single", bundle_name="my_bundle")
        assert groups[0][0] == "my_bundle"

    def test_per_group_inferred_groups_by_call_graph(self):
        # a->b are connected (a calls b, so b is the callee root); c is standalone.
        wfs = [_workflow("a", ["b"]), _workflow("b"), _workflow("c")]
        groups = _group_workflows(wfs, mode="per-group", group_by="inferred")
        members_by_name = {name: sorted(m.name for m in members) for name, members in groups}
        # Bundle is named after the callee root 'b' (topo-first), not the alphabetically-first 'a'.
        assert members_by_name == {"b": ["a", "b"], "c": ["c"]}

    def test_per_group_inferred_names_bundle_after_callee_root_not_alphabetical(self):
        # 'z_root' is the callee (deploys first) but sorts last; it must still name the bundle,
        # proving the name follows the dependency graph rather than alphabetical order.
        wfs = [_workflow("a_caller", ["z_root"]), _workflow("z_root")]
        groups = _group_workflows(wfs, mode="per-group", group_by="inferred")
        assert len(groups) == 1
        assert groups[0][0] == "z_root"

    def test_per_group_spec_honors_mapping(self):
        wfs = [_workflow("a"), _workflow("b"), _workflow("c")]
        groups = _group_workflows(
            wfs,
            mode="per-group",
            group_by="spec",
            group_spec={"a": "grp1", "b": "grp1", "c": "grp2"},
        )
        members_by_name = {name: sorted(m.name for m in members) for name, members in groups}
        assert members_by_name == {"grp1": ["a", "b"], "grp2": ["c"]}

    def test_single_pipeline_collapses_regardless_of_mode(self):
        groups = _group_workflows([_workflow("solo")], mode="per-group")
        assert len(groups) == 1
        assert groups[0][1][0].name == "solo"

    def test_colliding_normalized_keys_raise_not_silently_drop(self):
        # "Load Sales" and "load-sales" both normalize to "load_sales" -> must fail loudly, since
        # keeping only one would ship a bundle set silently missing a pipeline.
        wfs = [_workflow("Load Sales"), _workflow("load-sales")]
        with pytest.raises(ValueError, match="collide on normalized key"):
            _group_workflows(wfs, mode="per-pipeline")


class TestLoadGroupSpec:
    def test_flat_pipeline_to_group_map(self, tmp_path):
        spec = tmp_path / "spec.yml"
        spec.write_text(yaml.dump({"Pipeline A": "grp1", "pipeline_b": "grp1"}))
        assert _load_group_spec(spec) == {"pipeline_a": "grp1", "pipeline_b": "grp1"}

    def test_group_to_pipelines_map(self, tmp_path):
        spec = tmp_path / "spec.json"
        spec.write_text('{"grp1": ["Pipeline A", "pipeline_b"], "grp2": ["c"]}')
        assert _load_group_spec(spec) == {"pipeline_a": "grp1", "pipeline_b": "grp1", "c": "grp2"}


class TestWriteBundleGroup:
    def test_single_bundle_holds_multiple_job_resources(self, tmp_path):
        wfs = [_workflow("alpha"), _workflow("beta")]
        write_bundle_group(wfs, tmp_path, bundle_name="combined")
        resources = {p.name for p in (tmp_path / "resources").glob("*.yml")}
        assert {"alpha.yml", "beta.yml"} <= resources
        databricks_yml = yaml.safe_load((tmp_path / "databricks.yml").read_text())
        assert databricks_yml["bundle"]["name"] == "combined"
        # One consolidated SETUP.md for the whole bundle.
        assert (tmp_path / "SETUP.md").exists()

    def test_intra_group_call_stays_direct_ref(self, tmp_path):
        # alpha calls beta; both in the same bundle -> keep ${resources.jobs.beta.id}, no ${var}.
        wfs = [_workflow("alpha", ["beta"]), _workflow("beta")]
        write_bundle_group(wfs, tmp_path, bundle_name="combined")
        alpha_yaml = (tmp_path / "resources" / "alpha.yml").read_text()
        assert "${resources.jobs.beta.id}" in alpha_yaml
        assert "${var.beta}" not in alpha_yaml
        databricks_yml = yaml.safe_load((tmp_path / "databricks.yml").read_text())
        assert "beta" not in databricks_yml.get("variables", {})

    def test_cross_group_call_becomes_var(self, tmp_path):
        # alpha calls gamma, which is NOT in this bundle -> rewrite to ${var.gamma}.
        wfs = [_workflow("alpha", ["gamma"])]
        write_bundle_group(wfs, tmp_path, bundle_name="alpha_only")
        alpha_yaml = (tmp_path / "resources" / "alpha.yml").read_text()
        assert "${var.gamma}" in alpha_yaml
        databricks_yml = yaml.safe_load((tmp_path / "databricks.yml").read_text())
        assert "gamma" in databricks_yml["variables"]


def _copy_workflow(name: str, source_type: str):
    """A one-Copy-activity pipeline; the activity name is shared so notebook paths would collide."""
    return prepare_workflow(
        Pipeline(
            name=name,
            tasks=[
                CopyActivity(name="Copy Data", task_key="copy_data", source_type=source_type, sink_type="DeltaSink")
            ],
        )
    )


def _foreach_subjob_workflow(name: str):
    """A pipeline whose ForEach escalates to an inner sub-job (two children), keyed 'loop_inner_tasks'."""
    foreach = ForEachActivity(
        name="Loop",
        task_key="loop",
        items_expression="@pipeline().parameters.arr",
        inner_activities=[
            NotebookActivity(name="One", task_key="one", notebook_path="/Shared/one"),
            NotebookActivity(name="Two", task_key="two", notebook_path="/Shared/two"),
        ],
    )
    return prepare_workflow(Pipeline(name=name, tasks=[foreach]))


class TestMultiPipelineArtifactCollisions:
    """Regression: co-locating pipelines in one bundle must not let same-named artifacts collide."""

    def test_notebooks_namespaced_per_pipeline(self, tmp_path):
        # Two pipelines, each a Copy named "Copy Data" -> same notebook filename, different content.
        write_bundle_group(
            [_copy_workflow("pipe_a", "AzureSqlSource"), _copy_workflow("pipe_b", "BlobSource")],
            tmp_path,
            bundle_name="combined",
        )
        notebooks = {str(p.relative_to(tmp_path / "src")) for p in (tmp_path / "src").rglob("*.py")}
        # Each pipeline's copy notebook lands under its own subdirectory — no overwrite.
        assert "notebooks/pipe_a/copy_data.py" in notebooks
        assert "notebooks/pipe_b/copy_data.py" in notebooks
        # And each job references its OWN notebook.
        assert "../src/notebooks/pipe_a/copy_data.py" in (tmp_path / "resources" / "pipe_a.yml").read_text()
        assert "../src/notebooks/pipe_b/copy_data.py" in (tmp_path / "resources" / "pipe_b.yml").read_text()

    def test_inner_foreach_job_keys_namespaced_per_pipeline(self, tmp_path):
        write_bundle_group(
            [_foreach_subjob_workflow("pipe_a"), _foreach_subjob_workflow("pipe_b")],
            tmp_path,
            bundle_name="combined",
        )
        resources = {p.name for p in (tmp_path / "resources").glob("*.yml")}
        # Both inner sub-jobs survive under distinct, pipeline-prefixed keys.
        assert "pipe_a_loop_inner_tasks.yml" in resources
        assert "pipe_b_loop_inner_tasks.yml" in resources
        # Each parent's run_job_task points at its own inner job.
        assert "${resources.jobs.pipe_a_loop_inner_tasks.id}" in (tmp_path / "resources" / "pipe_a.yml").read_text()
        assert "${resources.jobs.pipe_b_loop_inner_tasks.id}" in (tmp_path / "resources" / "pipe_b.yml").read_text()

    def test_namespacing_rewrites_self_referential_paths_in_notebook_body(self, tmp_path):
        # A Spark-Python placeholder body references its own bundle path (`... src/scripts/foo.py`).
        # After namespacing, that in-body path must match the notebook's new location, or an operator
        # following the download hint would write the script where the task no longer looks.
        def spark_wf(name: str):
            return prepare_workflow(
                Pipeline(
                    name=name,
                    tasks=[SparkPythonActivity(name="Run", task_key="run", python_file="dbfs:/scripts/foo.py")],
                )
            )

        write_bundle_group([spark_wf("pipe_a"), spark_wf("pipe_b")], tmp_path, bundle_name="combined")
        body = (tmp_path / "src" / "scripts" / "pipe_a" / "foo.py").read_text()
        # The download hint points at the namespaced path, not the un-prefixed one.
        assert "src/scripts/pipe_a/foo.py" in body
        assert "src/scripts/foo.py" not in body
        # And the task's python_file points at the same namespaced path.
        assert "../src/scripts/pipe_a/foo.py" in (tmp_path / "resources" / "pipe_a.yml").read_text()

    def test_single_pipeline_bundle_paths_unchanged(self, tmp_path):
        # Namespacing must NOT fire for a one-pipeline bundle (per-pipeline output stays identical).
        write_bundle_group([_copy_workflow("solo", "AzureSqlSource")], tmp_path, bundle_name="solo")
        notebooks = {str(p.relative_to(tmp_path / "src")) for p in (tmp_path / "src").rglob("*.py")}
        assert "notebooks/copy_data.py" in notebooks
        assert not any("/solo/" in n for n in notebooks)


def _write_two_pipeline_report(tmp_path):
    """Writes an aggregated translation report with two pipelines: 'caller' calls 'callee'."""
    import json

    work = tmp_path / ".work"
    work.mkdir()
    report = {
        "translations": [
            {
                "pipeline": "caller",
                "status": "translated",
                "ir": {
                    "type": "ExecutePipelineActivity",
                    "name": "Run Callee",
                    "task_key": "run_callee",
                    "pipeline_name": "callee",
                },
            },
            {
                "pipeline": "callee",
                "status": "translated",
                "ir": {
                    "type": "WaitActivity",
                    "name": "Pause",
                    "task_key": "pause",
                    "wait_time_seconds": 5,
                },
            },
        ]
    }
    (work / "translation_report.json").write_text(json.dumps(report))


class TestPackageMainModes:
    def _run(self, tmp_path, *extra):
        from flowx.bundler.dab_writer import main

        return main(
            [
                "--output-dir",
                str(tmp_path),
                "--no-download-workspace-files",
                "--keep-intermediates",
                *extra,
            ]
        )

    def test_per_pipeline_writes_a_bundle_dir_each_plus_deploy_md(self, tmp_path):
        _write_two_pipeline_report(tmp_path)
        assert self._run(tmp_path, "--packaging-mode", "per-pipeline") == 0
        assert (tmp_path / "caller" / "databricks.yml").exists()
        assert (tmp_path / "callee" / "databricks.yml").exists()
        deploy_md = (tmp_path / "DEPLOY.md").read_text()
        # callee deploys before caller.
        assert deploy_md.index("`callee/`") < deploy_md.index("`caller/`")

    def test_single_mode_one_bundle_at_root(self, tmp_path):
        _write_two_pipeline_report(tmp_path)
        assert self._run(tmp_path, "--packaging-mode", "single") == 0
        assert (tmp_path / "databricks.yml").exists()
        resources = {p.name for p in (tmp_path / "resources").glob("*.yml")}
        assert {"caller.yml", "callee.yml"} <= resources
        # Intra-bundle call stays a direct ref.
        assert "${resources.jobs.callee.id}" in (tmp_path / "resources" / "caller.yml").read_text()
        assert "single bundle" in (tmp_path / "DEPLOY.md").read_text()
        # bundle.name is the group name (flowx_bundle), NOT the first pipeline — so the dev workspace
        # path is .bundle/flowx_bundle/dev, not .bundle/caller/dev for a bundle holding many pipelines.
        databricks_yml = yaml.safe_load((tmp_path / "databricks.yml").read_text())
        assert databricks_yml["bundle"]["name"] == "flowx_bundle"

    def test_writer_valueerror_surfaces_cleanly(self, tmp_path, monkeypatch, capsys):
        """A ValueError from write_bundle_group (e.g. the inner-key collision guard) must be reported as
        a clean 'Error: ...' + exit 1, not escape main() as a traceback."""
        import flowx.bundler.dab_writer as dab_writer

        _write_two_pipeline_report(tmp_path)

        def _boom(*_args, **_kwargs):
            raise ValueError("simulated collision")

        monkeypatch.setattr(dab_writer, "write_bundle_group", _boom)
        assert self._run(tmp_path, "--packaging-mode", "per-pipeline") == 1
        assert "Error: simulated collision" in capsys.readouterr().err

    def test_per_group_inferred_colocates_connected_pipelines(self, tmp_path):
        _write_two_pipeline_report(tmp_path)
        assert self._run(tmp_path, "--packaging-mode", "per-group") == 0
        # caller + callee are connected -> a single component, so one bundle at the output root.
        resources = {p.name for p in (tmp_path / "resources").glob("*.yml")}
        assert {"caller.yml", "callee.yml"} <= resources
        assert "single bundle" in (tmp_path / "DEPLOY.md").read_text()

    def test_per_group_inferred_separates_disconnected_pipelines(self, tmp_path):
        import json

        work = tmp_path / ".work"
        work.mkdir()
        # Two independent pipelines with no Run Pipeline edge between them -> two bundles.
        report = {
            "translations": [
                {
                    "pipeline": "solo_one",
                    "status": "translated",
                    "ir": {"type": "WaitActivity", "name": "W", "task_key": "w", "wait_time_seconds": 1},
                },
                {
                    "pipeline": "solo_two",
                    "status": "translated",
                    "ir": {"type": "WaitActivity", "name": "W", "task_key": "w", "wait_time_seconds": 1},
                },
            ]
        }
        (work / "translation_report.json").write_text(json.dumps(report))
        assert self._run(tmp_path, "--packaging-mode", "per-group") == 0
        assert (tmp_path / "solo_one" / "databricks.yml").exists()
        assert (tmp_path / "solo_two" / "databricks.yml").exists()


class TestLoadReportPipelinesShape:
    """The convert/modify ``{"pipelines": [...]}`` report shape (real flowx output)."""

    def test_load_report_handles_pipelines_format(self, tmp_path):
        """``_load_report`` accepts the ``{"pipelines": [...]}`` aggregated report.

        Regression: ``convert``/``modify`` serialize multi-pipeline reports under a top-level
        ``"pipelines"`` key, but ``_load_report`` only understood the single-pipeline and legacy
        ``"translations"`` shapes and silently returned ``[]`` for this one — so ``package`` aborted
        with "No translated pipelines found" for any real multi-pipeline factory.
        """
        report = {
            "pipelines": [
                {
                    "name": "pipeline_a",
                    "tasks": [
                        {"type": "WaitActivity", "name": "Pause", "task_key": "pause", "wait_time_seconds": 5},
                    ],
                },
                {
                    "name": "pipeline_b",
                    "tasks": [
                        {"type": "NotebookActivity", "name": "Run NB", "task_key": "run_nb", "notebook_path": "/x"},
                    ],
                },
            ]
        }
        report_path = tmp_path / "translation_report.json"
        report_path.write_text(json.dumps(report))

        workflows = _load_report(report_path)
        assert {wf.name for wf in workflows} == {"pipeline_a", "pipeline_b"}

    def test_load_report_aborts_on_malformed_pipelines_entry(self, tmp_path):
        """A malformed ``"pipelines"`` entry aborts loading instead of being silently dropped."""
        report = {
            "pipelines": [
                {
                    "name": "pipeline_ok",
                    "tasks": [
                        {"type": "WaitActivity", "name": "Pause", "task_key": "pause", "wait_time_seconds": 5},
                    ],
                },
                {"name": "no_tasks_here"},  # missing "tasks"
                {"tasks": []},  # missing "name"
                "not-even-a-dict",  # not a dict at all
            ]
        }
        report_path = tmp_path / "translation_report.json"
        report_path.write_text(json.dumps(report))

        with pytest.raises(MalformedReportError) as exc_info:
            _load_report(report_path)
        message = str(exc_info.value)
        assert "no_tasks_here" in message
        assert "index 2" in message
        assert "index 3" in message

    def test_package_main_returns_3_on_malformed_pipelines_entry(self, tmp_path):
        """``package`` aborts with exit code 3 when a pipeline entry is malformed."""
        from flowx.bundler.dab_writer import main as dab_main

        report = {
            "pipelines": [
                {
                    "name": "pipeline_ok",
                    "tasks": [
                        {"type": "WaitActivity", "name": "Pause", "task_key": "pause", "wait_time_seconds": 5},
                    ],
                },
                {"name": "no_tasks_here"},
            ]
        }
        report_path = tmp_path / "translation_report.json"
        report_path.write_text(json.dumps(report))

        exit_code = dab_main(
            ["--report", str(report_path), "--output-dir", str(tmp_path / "out"), "--no-download-workspace-files"]
        )
        assert exit_code == 3


class TestSyntheticParameterDefault:
    """A job parameter with no ADF default gets default="" (DAB-valid) but is surfaced in SETUP.md."""

    def test_missing_default_emits_empty_and_lists_in_setup(self, tmp_path):
        wf = prepare_workflow(
            Pipeline(
                name="needs_param",
                tasks=[WaitActivity(name="w", task_key="w", wait_time_seconds=1)],
                parameters=[{"name": "target_table"}],  # no "default"
            )
        )
        write_bundle_group([wf], tmp_path, bundle_name="needs_param")
        # default:"" is emitted so `bundle validate/deploy` accepts the parameter.
        doc = yaml.safe_load((tmp_path / "resources" / "needs_param.yml").read_text())
        assert doc["resources"]["jobs"]["needs_param"]["parameters"] == [{"name": "target_table", "default": ""}]
        # ...and it is surfaced in SETUP.md (not silently substituted, and not a separate file).
        setup_md = (tmp_path / "SETUP.md").read_text()
        assert "Job parameters without an ADF default" in setup_md
        assert "target_table" in setup_md

    def test_present_default_not_listed_in_setup(self, tmp_path):
        wf = prepare_workflow(
            Pipeline(
                name="has_param",
                tasks=[WaitActivity(name="w", task_key="w", wait_time_seconds=1)],
                parameters=[{"name": "region", "default": "eu"}],
            )
        )
        write_bundle_group([wf], tmp_path, bundle_name="has_param")
        assert "Job parameters without an ADF default" not in (tmp_path / "SETUP.md").read_text()


class TestWriterDoesNotMutateCaller:
    """write_bundle_group must not mutate the PreparedWorkflows it is handed. It rewrites
    ${resources.jobs.X.id} -> ${var.X} in place internally; if that leaked back to the caller it would
    erase the Run Pipeline edges pipeline_graph reads, silently breaking grouping / DEPLOY.md order for
    any caller that builds the graph after writing."""

    def test_run_pipeline_graph_survives_writing(self, tmp_path):
        from flowx.bundler.pipeline_graph import build_pipeline_dependencies

        # 'a' calls 'b'; write them as separate per-pipeline bundles (so b is cross-bundle for a).
        wfs = [_workflow("a", ["b"]), _workflow("b")]
        write_bundle_group([wfs[0]], tmp_path / "a", bundle_name="a")
        write_bundle_group([wfs[1]], tmp_path / "b", bundle_name="b")

        # Graph built AFTER writing must still see the a->b edge (writer worked on copies).
        deps = build_pipeline_dependencies(wfs)
        assert deps == {"a": {"b"}, "b": set()}

    def test_caller_task_dicts_unchanged(self, tmp_path):
        wf = _workflow("caller", ["callee"])
        before = [dict(t) for t in wf.tasks]
        write_bundle_group([wf], tmp_path, bundle_name="caller")
        # The caller's own task dicts are untouched — no ${var.X} rewrite bled back.
        assert wf.tasks == before


class TestPrefixedNotebookRelativePath:
    """Every notebook this codebase emits is under a category dir (notebooks/, lib/, src/…), so the
    slashed path is the real case and must be idempotent (double-apply is a no-op). The function must
    NOT special-case head==prefix, which would silently skip namespacing a genuine path whose top
    segment equals the pipeline key (e.g. a pipeline literally named 'notebooks')."""

    def test_slashed_path_prefixed_once(self):
        assert _prefixed_notebook_relative_path("notebooks/x.py", "pre") == "notebooks/pre/x.py"

    def test_slashed_path_idempotent(self):
        once = _prefixed_notebook_relative_path("notebooks/x.py", "pre")
        assert _prefixed_notebook_relative_path(once, "pre") == once

    def test_genuine_path_with_head_equal_to_prefix_is_still_namespaced(self):
        # A pipeline named 'notebooks' -> prefix 'notebooks'; its path notebooks/x.py must still be
        # namespaced to notebooks/notebooks/x.py, not skipped because head == prefix.
        assert _prefixed_notebook_relative_path("notebooks/x.py", "notebooks") == "notebooks/notebooks/x.py"

    def test_slashless_path_prefixed_once(self):
        assert _prefixed_notebook_relative_path("x.py", "pre") == "pre/x.py"
