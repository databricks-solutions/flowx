"""Unit tests for DEPLOY.md rendering."""

from __future__ import annotations

from flowx.bundler.deploy_writer import render_deploy_md


class TestRenderDeployMd:
    def test_single_bundle_has_no_ordering_section(self):
        md = render_deploy_md(
            [("flowx_bundle", ["a", "b"])],
            {"a": {"b"}, "b": set()},
            single_bundle=True,
            packaging_mode="single",
        )
        assert "single bundle" in md
        assert "Suggested deploy order" not in md
        assert "databricks bundle deploy -t dev" in md

    def test_multi_bundle_orders_callees_first(self):
        groups = [("caller", ["caller"]), ("callee", ["callee"])]
        deps = {"caller": {"callee"}, "callee": set()}
        md = render_deploy_md(groups, deps, single_bundle=False, packaging_mode="per-pipeline")
        assert md.index("`callee/`") < md.index("`caller/`")
        # The caller lists its cross-bundle dependency.
        assert "depends on: callee" in md

    def test_order_is_dependency_not_alphabetical(self):
        # 'aaa_root' sorts first but is the callee of 'zzz_leaf'; it must still deploy first, proving
        # the order follows the dependency graph rather than the (alphabetical) group order.
        groups = [("zzz_leaf", ["zzz_leaf"]), ("aaa_root", ["aaa_root"])]
        deps = {"zzz_leaf": {"aaa_root"}, "aaa_root": set()}
        md = render_deploy_md(groups, deps, single_bundle=False, packaging_mode="per-pipeline")
        assert md.index("`aaa_root/`") < md.index("`zzz_leaf/`")

    def test_multi_bundle_points_at_automated_deployer(self):
        md = render_deploy_md(
            [("a", ["a"]), ("b", ["b"])],
            {"a": {"b"}, "b": set()},
            single_bundle=False,
            packaging_mode="per-pipeline",
        )
        assert "python -m flowx.adapter deploy" in md
        assert "serverless" in md

    def test_cycle_is_flagged(self):
        md = render_deploy_md(
            [("a", ["a"]), ("b", ["b"])],
            {"a": {"b"}, "b": {"a"}},
            single_bundle=False,
            packaging_mode="per-pipeline",
        )
        assert "cyclic" in md.lower()

    def test_grouped_bundle_lists_all_member_pipelines(self):
        groups = [("grp", ["p1", "p2"]), ("other", ["other"])]
        deps = {"p1": {"p2"}, "p2": {"other"}, "other": set()}
        md = render_deploy_md(groups, deps, single_bundle=False, packaging_mode="per-group")
        assert "p1, p2" in md
        # p1->p2 is intra-bundle (both in grp) so grp only depends on 'other'.
        assert "depends on: other" in md
