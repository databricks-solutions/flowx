"""Tests for the motif-aware Tier-0 DAG equivalence check."""

from __future__ import annotations

from flowx.models.adf_ast import AdfActivity, AdfDependency, AdfPipeline
from flowx.models.ir import Activity, Dependency, Pipeline
from flowx.models.motifs import (
    MOTIF_ACTIVITY_AND_NOTIFY,
    MOTIF_METADATA_DRIVEN_BULK_COPY,
    DetectedMotif,
)
from flowx.motifs.collapser import collapse_motifs
from flowx.utils import normalize_task_key
from flowx.validate import check_dag_equivalence, format_result


def _adf(name: str, deps: dict[str, list[str]] | None = None, adf_type: str = "Copy") -> AdfActivity:
    """ADF activity; *deps* maps upstream name -> dependency conditions."""
    depends = [AdfDependency(activity=u, dependency_conditions=c) for u, c in (deps or {}).items()]
    return AdfActivity(name=name, type=adf_type, depends_on=depends or None)


def _task(name: str, deps: list[tuple[str, str]] | None = None) -> Activity:
    """IR leaf task; *deps* is a list of (upstream_name, outcome)."""
    edges = [Dependency(task_key=normalize_task_key(u), outcome=o) for u, o in (deps or [])]
    return Activity(name=name, task_key=normalize_task_key(name), depends_on=edges or None)


def _codes(result) -> set[str]:
    return {f.code for f in result.findings}


# ---------------------------------------------------------------------------
# Identity (no motifs)
# ---------------------------------------------------------------------------


def test_identity_dag_is_equivalent():
    adf = AdfPipeline(
        name="p",
        activities=[_adf("A"), _adf("B", {"A": ["Succeeded"]}), _adf("C", {"B": ["Succeeded"]})],
    )
    ir = Pipeline(
        name="p",
        tasks=[_task("A"), _task("B", [("A", "Succeeded")]), _task("C", [("B", "Succeeded")])],
    )
    result = check_dag_equivalence(adf, ir)
    assert result.equivalent
    assert not result.violations
    assert not result.warnings


# ---------------------------------------------------------------------------
# Motif collapse (convex) -- differences tolerated
# ---------------------------------------------------------------------------


def test_convex_motif_collapse_is_tolerated():
    # ADF: Lookup -> ForEach -> Sink.  Motif collapses {Lookup, ForEach}.
    adf = AdfPipeline(
        name="p",
        activities=[
            _adf("Lookup", adf_type="Lookup"),
            _adf("ForEach", {"Lookup": ["Succeeded"]}, adf_type="ForEach"),
            _adf("Sink", {"ForEach": ["Succeeded"]}),
        ],
    )
    pre = Pipeline(
        name="p",
        tasks=[_task("Lookup"), _task("ForEach", [("Lookup", "Succeeded")]), _task("Sink", [("ForEach", "Succeeded")])],
    )
    motif = DetectedMotif(definition=MOTIF_METADATA_DRIVEN_BULK_COPY, matched_activities=["Lookup", "ForEach"])
    collapsed = collapse_motifs(pre, [motif])

    result = check_dag_equivalence(adf, collapsed)
    assert result.equivalent
    assert not result.violations
    # The Lookup -> ForEach internal edge was absorbed, not reported as a loss.
    assert "collapsed_internal_edges" in _codes(result)
    assert "missing_edge" not in _codes(result)


def test_activity_and_notify_collapse_is_equivalent():
    # ADF: Copy -> {NotifySuccess, NotifyFailure}.  All three collapse.
    adf = AdfPipeline(
        name="p",
        activities=[
            _adf("Copy"),
            _adf("NotifySuccess", {"Copy": ["Succeeded"]}, adf_type="WebActivity"),
            _adf("NotifyFailure", {"Copy": ["Failed"]}, adf_type="WebActivity"),
        ],
    )
    pre = Pipeline(
        name="p",
        tasks=[
            _task("Copy"),
            _task("NotifySuccess", [("Copy", "Succeeded")]),
            _task("NotifyFailure", [("Copy", "Failed")]),
        ],
    )
    motif = DetectedMotif(
        definition=MOTIF_ACTIVITY_AND_NOTIFY,
        matched_activities=["Copy", "NotifySuccess", "NotifyFailure"],
    )
    collapsed = collapse_motifs(pre, [motif])

    result = check_dag_equivalence(adf, collapsed)
    assert result.equivalent
    assert not result.violations


# ---------------------------------------------------------------------------
# Non-convex collapse -- the invariant violation
# ---------------------------------------------------------------------------


def test_non_convex_motif_is_a_violation():
    # ADF: a1 -> w -> a2, but the motif tries to collapse {a1, a2} with the
    # external w sandwiched between them.  Collapsing would reorder w.
    adf = AdfPipeline(
        name="p",
        activities=[
            _adf("a1"),
            _adf("w", {"a1": ["Succeeded"]}),
            _adf("a2", {"w": ["Succeeded"]}),
        ],
    )
    pre = Pipeline(
        name="p",
        tasks=[_task("a1"), _task("w", [("a1", "Succeeded")]), _task("a2", [("w", "Succeeded")])],
    )
    motif = DetectedMotif(definition=MOTIF_METADATA_DRIVEN_BULK_COPY, matched_activities=["a1", "a2"])
    collapsed = collapse_motifs(pre, [motif])

    result = check_dag_equivalence(adf, collapsed)
    assert not result.equivalent
    assert "non_convex_motif" in _codes(result)
    non_convex = next(f for f in result.violations if f.code == "non_convex_motif")
    assert "w" in non_convex.nodes


# ---------------------------------------------------------------------------
# Dropped cross-boundary edge -- ordering constraint lost
# ---------------------------------------------------------------------------


def test_dropped_ordering_edge_is_a_violation():
    adf = AdfPipeline(
        name="p",
        activities=[_adf("A"), _adf("B", {"A": ["Succeeded"]}), _adf("C", {"B": ["Succeeded"]})],
    )
    # IR forgot the B -> C edge.
    ir = Pipeline(name="p", tasks=[_task("A"), _task("B", [("A", "Succeeded")]), _task("C")])
    result = check_dag_equivalence(adf, ir)
    assert not result.equivalent
    assert "missing_edge" in _codes(result)


# ---------------------------------------------------------------------------
# Synthesised init task -- IR-only, tolerated
# ---------------------------------------------------------------------------


def test_synthesised_init_task_is_tolerated():
    adf = AdfPipeline(name="p", activities=[_adf("A"), _adf("B", {"A": ["Succeeded"]})])
    init = Activity(name="_init_flag", task_key="_init_flag")
    ir = Pipeline(name="p", tasks=[init, _task("A"), _task("B", [("A", "Succeeded")])])
    result = check_dag_equivalence(adf, ir)
    assert result.equivalent
    assert "synthesised_task" in _codes(result)
    assert "unmapped_ir_task" not in _codes(result)


# ---------------------------------------------------------------------------
# Merged dependency outcomes -- lossy collapse, warned (not blocking)
# ---------------------------------------------------------------------------


def test_merged_outcome_is_warned_not_blocking():
    # Both motif members depend on E, but with different conditions; the
    # collapser keeps only one, which we surface as a warning.
    adf = AdfPipeline(
        name="p",
        activities=[
            _adf("E"),
            _adf("a1", {"E": ["Succeeded"]}),
            _adf("a2", {"E": ["Failed"]}),
        ],
    )
    pre = Pipeline(
        name="p",
        tasks=[_task("E"), _task("a1", [("E", "Succeeded")]), _task("a2", [("E", "Failed")])],
    )
    motif = DetectedMotif(definition=MOTIF_ACTIVITY_AND_NOTIFY, matched_activities=["a1", "a2"])
    collapsed = collapse_motifs(pre, [motif])

    result = check_dag_equivalence(adf, collapsed)
    assert result.equivalent  # warning, not violation
    assert "merged_outcome" in _codes(result)


# ---------------------------------------------------------------------------
# Extra ordering edge -- over-constraining, warned
# ---------------------------------------------------------------------------


def test_extra_edge_is_warned():
    adf = AdfPipeline(name="p", activities=[_adf("A"), _adf("B")])  # A and B independent
    ir = Pipeline(name="p", tasks=[_task("A"), _task("B", [("A", "Succeeded")])])  # IR adds A -> B
    result = check_dag_equivalence(adf, ir)
    assert result.equivalent
    assert "extra_edge" in _codes(result)


def test_format_result_reports_status():
    adf = AdfPipeline(name="p", activities=[_adf("A"), _adf("B", {"A": ["Succeeded"]})])
    ir = Pipeline(name="p", tasks=[_task("A"), _task("B")])  # missing edge
    rendered = format_result(check_dag_equivalence(adf, ir))
    assert "NOT EQUIVALENT" in rendered
    assert "missing_edge" in rendered
