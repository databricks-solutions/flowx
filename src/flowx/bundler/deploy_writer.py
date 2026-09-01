"""Renders the top-level ``DEPLOY.md`` describing bundle layout and deploy order.

A multi-pipeline migration can emit several bundles that must be deployed in dependency order: a
callee pipeline's job must exist before a caller's ``run_job_task`` can reference its numeric id.
``DEPLOY.md`` is the human-readable companion to the automated
:mod:`flowx.bundler.deployer` (``flowx.adapter deploy``): it lists each bundle, the pipelines it
contains, the cross-bundle ``${var.X}`` dependencies it has, and a suggested callees-first deploy
order — collapsed from the pipeline-level Run Pipeline graph to bundle granularity.

This module only *renders* text; it reads no filesystem state and shells out to nothing, so it works
identically on local CLI and Databricks serverless.
"""

from __future__ import annotations

from flowx.bundler.pipeline_graph import PipelineCycleError, topo_order


def _bundle_of_pipeline(groups: list[tuple[str, list[str]]]) -> dict[str, str]:
    """Returns a ``pipeline_key -> bundle_dir`` map from ``(bundle_dir, [pipeline_keys])`` groups."""
    mapping: dict[str, str] = {}
    for bundle_dir, pipeline_keys in groups:
        for key in pipeline_keys:
            mapping[key] = bundle_dir
    return mapping


def _bundle_dependencies(
    groups: list[tuple[str, list[str]]],
    pipeline_deps: dict[str, set[str]],
) -> dict[str, set[str]]:
    """Collapses the pipeline-level dep graph to a ``bundle_dir -> {dependency bundle_dirs}`` graph.

    A bundle depends on another when any of its pipelines calls a pipeline that lives in the other
    bundle. Intra-bundle calls and calls to pipelines outside the migration are dropped.
    """
    bundle_of = _bundle_of_pipeline(groups)
    graph: dict[str, set[str]] = {bundle_dir: set() for bundle_dir, _ in groups}
    for caller_pipeline, callees in pipeline_deps.items():
        caller_bundle = bundle_of.get(caller_pipeline)
        if caller_bundle is None:
            continue
        for callee_pipeline in callees:
            callee_bundle = bundle_of.get(callee_pipeline)
            if callee_bundle is None or callee_bundle == caller_bundle:
                continue
            graph[caller_bundle].add(callee_bundle)
    return graph


def _bundle_deploy_order(bundle_graph: dict[str, set[str]]) -> tuple[list[str], bool]:
    """Returns ``(ordered_bundle_dirs, ok)``. Callees first; ``ok`` is False on a cyclic graph.

    Falls back to a stable sorted order when the graph is cyclic so DEPLOY.md still renders (the
    cycle is called out in the prose).
    """
    try:
        return topo_order({node: set(deps) for node, deps in bundle_graph.items()}), True
    except PipelineCycleError:
        return sorted(bundle_graph), False


def render_deploy_md(
    groups: list[tuple[str, list[str]]],
    pipeline_deps: dict[str, set[str]],
    *,
    single_bundle: bool,
    packaging_mode: str = "per-pipeline",
) -> str:
    """Renders ``DEPLOY.md`` for a packaging run.

    Args:
        groups: ``(bundle_dir_name, [pipeline_keys])`` for every bundle written, in write order.
        pipeline_deps: ``pipeline_key -> {callee pipeline keys}`` from
            :func:`flowx.bundler.pipeline_graph.build_pipeline_dependencies`.
        single_bundle: True when the run produced one bundle at the output root (no subdirectory).
        packaging_mode: The ``--packaging-mode`` used, surfaced for context.

    Returns:
        The full Markdown document.
    """
    bundle_graph = _bundle_dependencies(groups, pipeline_deps)
    order, acyclic = _bundle_deploy_order(bundle_graph)

    lines: list[str] = [
        "# Deploy",
        "",
        f"This migration produced **{len(groups)} bundle(s)** with packaging mode `{packaging_mode}`.",
        "",
    ]

    if single_bundle:
        bundle_dir, pipeline_keys = groups[0]
        lines += [
            "All pipelines are packaged into a **single bundle** at the migration output root. Deploy it "
            "directly — there is no cross-bundle ordering to worry about:",
            "",
            "```bash",
            "databricks bundle validate -t dev",
            "databricks bundle deploy -t dev",
            "```",
            "",
            f"Pipelines in this bundle: {', '.join(sorted(pipeline_keys))}.",
            "",
        ]
        return "\n".join(lines)

    if not acyclic:
        lines += [
            "> **Warning:** the Run Pipeline call graph between these bundles is **cyclic**, so no valid "
            "deploy order exists. The list below is sorted by name, not by dependency. Break the cycle "
            "(or package the cyclic pipelines into one bundle with `--packaging-mode single`/`per-group`) "
            "before deploying.",
            "",
        ]

    lines += [
        "## Suggested deploy order",
        "",
        "Deploy callees before their callers so each caller's `${var.<callee>}` job-id reference can be "
        "resolved. The automated deployer does this for you (see below); this order is for manual "
        "deploys.",
        "",
    ]
    for position, bundle_dir in enumerate(order, start=1):
        deps = sorted(bundle_graph.get(bundle_dir, set()))
        suffix = f" — depends on: {', '.join(deps)}" if deps else ""
        lines.append(f"{position}. `{bundle_dir}/`{suffix}")
    lines.append("")

    lines += ["## Bundles", ""]
    pipelines_by_bundle = dict(groups)
    for bundle_dir in order:
        pipeline_keys = pipelines_by_bundle.get(bundle_dir, [])
        lines.append(f"### `{bundle_dir}/`")
        lines.append("")
        lines.append(f"Pipelines: {', '.join(sorted(pipeline_keys))}")
        deps = sorted(bundle_graph.get(bundle_dir, set()))
        if deps:
            lines.append("")
            lines.append(f"Cross-bundle dependencies (`${{var.…}}`): {', '.join(deps)}")
        lines.append("")

    lines += [
        "## Automated ordered deploy",
        "",
        "Rather than deploying each bundle by hand, use the ordered deployer, which discovers these "
        "bundles, deploys callees first, and injects each callee's deployed job id into its callers "
        "automatically:",
        "",
        "```bash",
        "python -m flowx.adapter deploy --output-dir . --target dev",
        "```",
        "",
        "> Local CLI only — `databricks bundle deploy`/`summary` are unavailable on Databricks "
        "serverless / Genie Code. Run this from a local CLI session or the web terminal.",
        "",
    ]
    return "\n".join(lines)
