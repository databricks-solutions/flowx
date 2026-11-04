"""Pipeline-level Run Pipeline (ExecutePipeline) dependency graph.

An ADF ``ExecutePipeline`` activity is prepared as a ``run_job_task`` whose ``job_id`` is
``${resources.jobs.<callee>.id}`` (see :func:`flowx.preparer.activity_preparers.execute_pipeline`),
where ``<callee>`` is the normalized name of the called pipeline. This module reads those refs back
off the prepared workflow task trees to reconstruct the caller -> callee graph *before* any bundle
is written.

The graph is the single source of truth for two packaging decisions:

- **Grouping** (``per-group`` inferred mode): pipelines that call one another belong in the same
  bundle. :func:`connected_components` returns those clusters.
- **Deploy order** (top-level ``DEPLOY.md``): :func:`topo_order` returns callees-before-callers so
  the generated ``DEPLOY.md`` can suggest the order the operator (or ``flowx.adapter deploy``)
  should deploy in.

Keys throughout are the normalized pipeline resource keys (:func:`flowx.utils.to_lowercase_key`),
matching the job resource keys emitted into ``resources/*.yml`` and the ``${resources.jobs.X.id}`` /
``${var.X}`` refs, so they line up 1:1 with what :mod:`flowx.bundler.deployer` discovers at deploy
time.
"""

from __future__ import annotations

import re
from typing import TYPE_CHECKING, Any

from flowx.utils import to_lowercase_key

if TYPE_CHECKING:
    from flowx.preparer.workflow_preparer import PreparedWorkflow

# The ref shape execute_pipeline.prepare emits: ${resources.jobs.<callee>.id}. Used here to read the
# Run Pipeline call graph off the prepared task trees *before* dab_writer rewrites cross-bundle refs
# to ${var.<callee>_job_id} (see _rewrite_cross_bundle_job_references).
CROSS_BUNDLE_JOB_ID_REF = re.compile(r"\$\{resources\.jobs\.([^.]+)\.id\}")


class PipelineCycleError(Exception):
    """Raised when the Run Pipeline dependency graph contains a cycle (cannot be ordered)."""


def _iter_run_job_targets(tasks: list[dict[str, Any]]) -> list[str]:
    """Returns the callee key of every ``run_job_task`` in *tasks*, descending into ForEach bodies."""
    targets: list[str] = []

    def visit(task: dict[str, Any]) -> None:
        run_job = task.get("run_job_task")
        if isinstance(run_job, dict):
            match = CROSS_BUNDLE_JOB_ID_REF.fullmatch(str(run_job.get("job_id", "")))
            if match:
                targets.append(match.group(1))
        for_each = task.get("for_each_task")
        if isinstance(for_each, dict) and isinstance(for_each.get("task"), dict):
            visit(for_each["task"])

    for task in tasks:
        visit(task)
    return targets


def _workflow_own_keys(workflow: PreparedWorkflow) -> set[str]:
    """Returns the job resource keys a workflow owns: its own key plus every inner ForEach job key."""
    keys = {to_lowercase_key(workflow.name)}
    keys.update(to_lowercase_key(inner.name) for inner in workflow.inner_workflows)
    return keys


def build_pipeline_dependencies(workflows: list[PreparedWorkflow]) -> dict[str, set[str]]:
    """Builds a ``pipeline_key -> {callee pipeline keys}`` map from Run Pipeline refs.

    Scans each workflow's task tree (and its inner ForEach jobs') for ``run_job_task`` refs of the
    form ``${resources.jobs.X.id}`` and records ``X`` as a dependency of the *owning* pipeline. Refs
    to a workflow's own keys (its inner ForEach jobs) are self-edges and dropped — they are within
    one bundle and never affect grouping or deploy order. Refs to keys no workflow provides are kept
    (an ExecutePipeline to a pipeline outside this migration); callers decide how to treat them.

    Returns:
        A dict with one entry per workflow (keyed by its normalized name), whose value is the set of
        other pipeline keys it calls. Every workflow appears as a key, even with no dependencies.
    """
    key_by_workflow = {to_lowercase_key(wf.name): wf for wf in workflows}
    deps: dict[str, set[str]] = {key: set() for key in key_by_workflow}

    for workflow in workflows:
        owner = to_lowercase_key(workflow.name)
        own_keys = _workflow_own_keys(workflow)
        callees = list(_iter_run_job_targets(workflow.tasks))
        for inner in workflow.inner_workflows:
            callees.extend(_iter_run_job_targets(inner.tasks))
        for callee in callees:
            if callee in own_keys:
                continue
            deps[owner].add(callee)
    return deps


def connected_components(deps: dict[str, set[str]]) -> list[list[str]]:
    """Returns clusters of pipelines connected (in either direction) by Run Pipeline calls.

    Dependencies are treated as **undirected** edges: a pipeline and everything it transitively
    calls or is called by land in one component. Only keys present in *deps* participate; callee
    keys not in *deps* (calls to pipelines outside the migration) are ignored for grouping.

    Returns:
        A list of components, each a sorted list of pipeline keys. Components are ordered by their
        smallest member so the result is deterministic.
    """
    nodes = set(deps)
    adjacency: dict[str, set[str]] = {node: set() for node in nodes}
    for node, callees in deps.items():
        for callee in callees:
            if callee not in nodes:
                continue
            adjacency[node].add(callee)
            adjacency[callee].add(node)

    seen: set[str] = set()
    components: list[list[str]] = []
    for start in sorted(nodes):
        if start in seen:
            continue
        stack = [start]
        component: set[str] = set()
        while stack:
            node = stack.pop()
            if node in component:
                continue
            component.add(node)
            stack.extend(adjacency[node] - component)
        seen |= component
        components.append(sorted(component))

    components.sort(key=lambda members: members[0])
    return components


def kahn_topo_sort(graph: dict[str, set[str]]) -> tuple[list[str], list[str]]:
    """Topologically sorts *graph* (node -> the nodes it depends on), dependencies first.

    Edges to nodes absent from *graph* are ignored. Ready nodes are drained in sorted order so the
    result is deterministic.

    Returns:
        An ``(ordered, unordered)`` tuple. ``unordered`` is the sorted list of nodes a cycle left
        unplaced; it is empty when the graph is acyclic.
    """
    nodes = set(graph)
    in_degree = {node: len({dep for dep in deps if dep in nodes}) for node, deps in graph.items()}
    dependents: dict[str, list[str]] = {node: [] for node in nodes}
    for node, deps in graph.items():
        for dep in deps:
            if dep in nodes:
                dependents[dep].append(node)

    queue = sorted(node for node, degree in in_degree.items() if degree == 0)
    ordered: list[str] = []
    while queue:
        node = queue.pop(0)
        ordered.append(node)
        for dependent in sorted(dependents[node]):
            in_degree[dependent] -= 1
            if in_degree[dependent] == 0:
                queue.append(dependent)
        queue.sort()

    unordered = sorted(node for node in nodes if node not in ordered)
    return ordered, unordered


def topo_order(deps: dict[str, set[str]]) -> list[str]:
    """Returns pipeline keys callees-first (a called pipeline precedes its caller).

    Raises:
        PipelineCycleError: when the Run Pipeline graph has a cycle (cyclic factories cannot be
            ordered into a deploy sequence).
    """
    ordered, unordered = kahn_topo_sort(deps)
    if unordered:
        raise PipelineCycleError(
            "Cyclic Run Pipeline dependency between: " + ", ".join(unordered) + ". "
            "A cyclic call graph cannot be ordered into a deploy sequence."
        )
    return ordered
