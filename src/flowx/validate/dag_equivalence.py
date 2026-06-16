"""Motif-aware DAG equivalence check (Tier-0 static validation).

flowx translates an Azure Data Factory pipeline into a Databricks IR
pipeline.  The two top-level dependency DAGs are *not* expected to be
identical, because motif collapsing rewrites the graph: each detected motif
contracts its matched activity set ``S`` into a single
:class:`~flowx.models.ir.MotifActivity`, dropping the edges internal to
``S`` and rewiring every cross-boundary edge onto the collapsed node.

This module checks that the IR DAG equals the *quotient* of the ADF DAG under
the motif partition (so motif-induced differences are tolerated), and that
each contraction is **safe** -- i.e. it did not silently reorder anything.

The safety condition is graph-theoretic convexity: contracting a set ``S`` to
a point preserves every ordering constraint **iff** no activity outside ``S``
lies on a dependency path *between* two members of ``S``.  If such an external
activity exists, the collapse would force it to run both before and after the
motif (a reordering / cycle); that is the invalidation we flag.

Findings are graded:

* ``violation`` -- the migrated DAG is not a faithful quotient of the source
  (a cross-boundary ordering edge was dropped, an activity vanished, a motif
  set was non-convex, or the quotient is cyclic).  These block equivalence.
* ``warning``   -- a difference that is over-constraining or lossy but not
  unsafe (an extra ordering edge, a merged/changed dependency outcome, an
  unexplained IR-only task).
* ``tolerated`` -- a difference fully explained by motif contraction or by a
  synthesised IR-only helper task; recorded only for transparency.
"""

from __future__ import annotations

from collections import deque
from dataclasses import dataclass, field

from flowx.models.adf_ast import AdfPipeline
from flowx.models.ir import MotifActivity, Pipeline
from flowx.utils import normalize_task_key

_DEFAULT_OUTCOME = "Succeeded"
# Task-key prefixes of synthesised variable-initialiser tasks the translator injects (engine
# ``_init_<var>``); these have no ADF preimage and are expected to be IR-only.
_SYNTHESISED_KEY_PREFIXES = ("_init_",)


@dataclass(slots=True, kw_only=True)
class DagFinding:
    """A single observation from the equivalence check.

    Attributes:
        code: Stable machine-readable identifier (e.g. ``"missing_edge"``).
        severity: One of ``"violation"`` / ``"warning"`` / ``"tolerated"``.
        message: Human-readable explanation.
        nodes: Block labels / activity names the finding concerns.
    """

    code: str
    severity: str
    message: str
    nodes: tuple[str, ...] = ()


@dataclass(slots=True, kw_only=True)
class DagEquivalenceResult:
    """Outcome of :func:`check_dag_equivalence`.

    Attributes:
        equivalent: True when there are no ``violation`` findings.
        findings: All findings, in detection order.
    """

    equivalent: bool
    findings: list[DagFinding] = field(default_factory=list)

    @property
    def violations(self) -> list[DagFinding]:
        """Findings that block equivalence."""
        return [finding for finding in self.findings if finding.severity == "violation"]

    @property
    def warnings(self) -> list[DagFinding]:
        """Non-blocking findings worth surfacing to the user."""
        return [finding for finding in self.findings if finding.severity == "warning"]

    @property
    def tolerated(self) -> list[DagFinding]:
        """Differences explained by motif collapse or synthesised tasks."""
        return [finding for finding in self.findings if finding.severity == "tolerated"]


# ---------------------------------------------------------------------------
# Graph helpers
# ---------------------------------------------------------------------------


def _reachable(adjacency: dict[str, set[str]], sources: set[str]) -> set[str]:
    """Returns every node reachable from *sources* (sources excluded)."""
    seen: set[str] = set()
    queue: deque[str] = deque(sources)
    while queue:
        node = queue.popleft()
        for successor in adjacency.get(node, ()):  # noqa: B007
            if successor not in seen:
                seen.add(successor)
                queue.append(successor)
    return seen


def _has_cycle(nodes: set[str], edges: set[tuple[str, str]]) -> bool:
    """Kahn's algorithm: True when the directed graph has a cycle."""
    adjacency: dict[str, set[str]] = {node: set() for node in nodes}
    in_degree: dict[str, int] = {node: 0 for node in nodes}
    for upstream, downstream in edges:
        if downstream not in adjacency[upstream]:
            adjacency[upstream].add(downstream)
            in_degree[downstream] = in_degree.get(downstream, 0) + 1
    queue: deque[str] = deque(node for node in nodes if in_degree[node] == 0)
    visited = 0
    while queue:
        node = queue.popleft()
        visited += 1
        for successor in adjacency[node]:
            in_degree[successor] -= 1
            if in_degree[successor] == 0:
                queue.append(successor)
    return visited != len(nodes)


def _is_synthesised(task_key: str) -> bool:
    return any(task_key.startswith(prefix) for prefix in _SYNTHESISED_KEY_PREFIXES)


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------


def check_dag_equivalence(adf: AdfPipeline, ir: Pipeline) -> DagEquivalenceResult:
    """Check that the IR DAG is a safe motif-quotient of the ADF DAG.

    Args:
        adf: The source ADF pipeline (typed AST).
        ir: The translated, *motif-collapsed* IR pipeline.

    Returns:
        A :class:`DagEquivalenceResult`.  ``equivalent`` is True when the IR
        top-level DAG equals the quotient of the ADF top-level DAG under the
        motif partition recorded in the IR, and every collapsed motif set is
        convex (so nothing was reordered) and the result is acyclic.
    """
    findings: list[DagFinding] = []

    adf_names = [activity.name for activity in adf.activities]
    adf_name_set = set(adf_names)

    # ADF top-level edges (upstream -> downstream) and per-edge conditions.
    adf_adj: dict[str, set[str]] = {name: set() for name in adf_names}
    adf_edge_conditions: dict[tuple[str, str], set[str]] = {}
    # Per (downstream, upstream) the declared condition set -- used by the
    # merged-outcome check, which mirrors the collapser's dedupe-by-source.
    inbound_conditions: dict[str, dict[str, frozenset[str]]] = {}
    for activity in adf.activities:
        for adf_dep in activity.depends_on or []:
            upstream, downstream = adf_dep.activity, activity.name
            conditions = set(adf_dep.dependency_conditions or [_DEFAULT_OUTCOME])
            if upstream not in adf_name_set:
                findings.append(
                    DagFinding(
                        code="dangling_adf_dependency",
                        severity="warning",
                        message=(
                            f"ADF activity '{downstream}' depends on '{upstream}', "
                            "which is not a top-level activity; edge ignored."
                        ),
                        nodes=(upstream, downstream),
                    )
                )
                continue
            adf_adj[upstream].add(downstream)
            adf_edge_conditions.setdefault((upstream, downstream), set()).update(conditions)
            inbound_conditions.setdefault(downstream, {})[upstream] = frozenset(conditions)

    # Motif partition + IR block bookkeeping, read straight from the IR so we
    # validate the *actual* collapse rather than re-running detection.
    block_of_name: dict[str, str] = {}
    motif_members: dict[str, set[str]] = {}
    ir_blocks: set[str] = set()
    synthesised_blocks: set[str] = set()
    key_to_block: dict[str, str] = {}

    for task in ir.tasks:
        label = task.task_key
        ir_blocks.add(label)
        key_to_block[task.task_key] = label
        if isinstance(task, MotifActivity):
            member_names = set(task.matched_activity_names)
            motif_members[label] = member_names
            for name in member_names:
                block_of_name[name] = label
        elif _is_synthesised(task.task_key):
            synthesised_blocks.add(label)
        elif task.name in adf_name_set:
            # task.name is the original ADF activity name; key the singleton block by the IR task's
            # actual task_key so labels match the engine's case-preserving sanitiser. Anything else is an
            # unexpected IR-only task, left to surface as unmapped_ir_task.
            block_of_name[task.name] = label

    # Any ADF activity with no corresponding IR task at all -> sentinel block.
    # The label is absent from ``ir_blocks`` so it surfaces as ``missing_node``.
    for name in adf_names:
        block_of_name.setdefault(name, normalize_task_key(name))

    adf_blocks = set(block_of_name.values())

    # ----- Quotient of the ADF DAG under the partition -----
    quotient_edges: set[tuple[str, str]] = set()
    quotient_conditions: dict[tuple[str, str], set[str]] = {}
    collapsed_internal = 0
    for (upstream, downstream), conditions in adf_edge_conditions.items():
        block_upstream, block_downstream = block_of_name[upstream], block_of_name[downstream]
        if block_upstream == block_downstream:
            collapsed_internal += 1
            continue
        quotient_edges.add((block_upstream, block_downstream))
        quotient_conditions.setdefault((block_upstream, block_downstream), set()).update(conditions)
    if collapsed_internal:
        findings.append(
            DagFinding(
                code="collapsed_internal_edges",
                severity="tolerated",
                message=(
                    f"{collapsed_internal} intra-motif edge(s) absorbed into collapsed "
                    "motif node(s); expected and ignored."
                ),
            )
        )

    # ----- IR block-space edges -----
    ir_edges: set[tuple[str, str]] = set()
    ir_conditions: dict[tuple[str, str], set[str]] = {}
    for task in ir.tasks:
        block_downstream = key_to_block[task.task_key]
        for ir_dep in task.depends_on or []:
            block_upstream = key_to_block.get(ir_dep.task_key, ir_dep.task_key)
            if block_upstream == block_downstream:
                continue
            if block_upstream in synthesised_blocks or block_downstream in synthesised_blocks:
                findings.append(
                    DagFinding(
                        code="synthesised_edge",
                        severity="tolerated",
                        message=f"Edge involving synthesised task '{block_upstream}' -> '{block_downstream}' ignored.",
                        nodes=(block_upstream, block_downstream),
                    )
                )
                continue
            ir_edges.add((block_upstream, block_downstream))
            ir_conditions.setdefault((block_upstream, block_downstream), set()).add(ir_dep.outcome or _DEFAULT_OUTCOME)

    # ----- Node comparison -----
    for block in sorted(adf_blocks - ir_blocks):
        claimed = motif_members.get(block)
        label = block if claimed is None else f"motif[{', '.join(sorted(claimed))}]"
        findings.append(
            DagFinding(
                code="missing_node",
                severity="violation",
                message=f"ADF activity/block '{label}' has no corresponding IR task.",
                nodes=(block,),
            )
        )
    for block in sorted(ir_blocks - adf_blocks - synthesised_blocks):
        findings.append(
            DagFinding(
                code="unmapped_ir_task",
                severity="warning",
                message=f"IR task '{block}' has no ADF preimage and is not a recognised synthesised task.",
                nodes=(block,),
            )
        )
    for block in sorted(synthesised_blocks):
        findings.append(
            DagFinding(
                code="synthesised_task",
                severity="tolerated",
                message=f"IR-only synthesised task '{block}' ignored.",
                nodes=(block,),
            )
        )

    # ----- Edge comparison (only between blocks present on both sides) -----
    comparable = adf_blocks & ir_blocks
    for edge in sorted(quotient_edges - ir_edges):
        if edge[0] in comparable and edge[1] in comparable:
            findings.append(
                DagFinding(
                    code="missing_edge",
                    severity="violation",
                    message=f"Ordering edge '{edge[0]}' -> '{edge[1]}' present in ADF is missing from the IR DAG.",
                    nodes=edge,
                )
            )
    for edge in sorted(ir_edges - quotient_edges):
        findings.append(
            DagFinding(
                code="extra_edge",
                severity="warning",
                message=(
                    f"IR DAG adds ordering edge '{edge[0]}' -> '{edge[1]}' not implied by the ADF DAG "
                    "(over-constraining)."
                ),
                nodes=edge,
            )
        )
    for edge in sorted(quotient_edges & ir_edges):
        adf_outcomes = quotient_conditions.get(edge, set())
        ir_outcomes = ir_conditions.get(edge, set())
        if adf_outcomes and adf_outcomes != ir_outcomes:
            findings.append(
                DagFinding(
                    code="outcome_mismatch",
                    severity="warning",
                    message=(
                        f"Edge '{edge[0]}' -> '{edge[1]}' dependency condition changed: "
                        f"ADF {sorted(adf_outcomes)} vs IR {sorted(ir_outcomes)}."
                    ),
                    nodes=edge,
                )
            )

    # ----- Convexity: nothing reordered by any contraction -----
    reverse_adj: dict[str, set[str]] = {name: set() for name in adf_names}
    for upstream, downstreams in adf_adj.items():
        for downstream in downstreams:
            reverse_adj[downstream].add(upstream)

    for label, members in motif_members.items():
        valid_members = members & adf_name_set
        if len(valid_members) < 2:
            continue
        descendants = _reachable(adf_adj, set(valid_members))
        ancestors = _reachable(reverse_adj, set(valid_members))
        between = (descendants & ancestors) - valid_members
        if between:
            findings.append(
                DagFinding(
                    code="non_convex_motif",
                    severity="violation",
                    message=(
                        f"Motif '{label}' is non-convex: external activity(ies) "
                        f"{sorted(between)} lie on a dependency path between collapsed "
                        "members, so collapsing reorders them. Collapse is unsafe."
                    ),
                    nodes=tuple(sorted(between)),
                )
            )

        # Merged-outcome: collapser dedupes external deps by source, keeping
        # the first outcome -- flag when members disagree on a shared source.
        per_source: dict[str, set[frozenset[str]]] = {}
        for member in valid_members:
            for source, conds in inbound_conditions.get(member, {}).items():
                if source not in valid_members:
                    per_source.setdefault(source, set()).add(conds)
        for source, condition_sets in per_source.items():
            if len(condition_sets) > 1:
                findings.append(
                    DagFinding(
                        code="merged_outcome",
                        severity="warning",
                        message=(
                            f"Motif '{label}' collapses edges from '{source}' that carried "
                            f"differing conditions {[sorted(conditions) for conditions in condition_sets]}; "
                            "collapse keeps only one."
                        ),
                        nodes=(source, label),
                    )
                )

    # ----- Acyclicity backstop on the deployable IR DAG -----
    if _has_cycle(ir_blocks, ir_edges):
        findings.append(
            DagFinding(
                code="cycle",
                severity="violation",
                message="The translated IR DAG contains a dependency cycle.",
            )
        )

    equivalent = not any(finding.severity == "violation" for finding in findings)
    return DagEquivalenceResult(equivalent=equivalent, findings=findings)


def format_result(result: DagEquivalenceResult) -> str:
    """Render a result as a compact human-readable report."""
    status = "EQUIVALENT" if result.equivalent else "NOT EQUIVALENT"
    lines = [f"DAG equivalence: {status}"]
    for label, items in (
        ("Violations", result.violations),
        ("Warnings", result.warnings),
        ("Tolerated", result.tolerated),
    ):
        if not items:
            continue
        lines.append(f"  {label} ({len(items)}):")
        lines.extend(f"    - [{finding.code}] {finding.message}" for finding in items)
    return "\n".join(lines)
