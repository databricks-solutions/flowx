"""Agentic insights (discover phase) -- agent-authored judgment merged into inventory.json.

References pipelines by name. Its cross-pipeline edges are either ANNOTATIONS of a
deterministic ``Lineage`` edge (control/data -- carry no facts of their own) or an
agent-INFERRED coupling the deterministic layer could not see (e.g. data flow that
happens inside notebook code, an external trigger, a message queue). Inferred edges
must cite their evidence and a confidence level so they are never mistaken for proven
lineage.

These models are **source-neutral**: they describe the shape of the ``insights`` object
the agent authors, independent of whether the pipelines were discovered from ADF or
Airflow. The validate/merge engine in ``flowx.parser.pipeline_insights`` works on the
raw dict form; these dataclasses document the contract and back the unit tests.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Literal


@dataclass(slots=True, kw_only=True)
class LineageEdgeRef:
    """A typed reference from a PipelineRelationship to one cross-pipeline edge.

    Two tiers:

    * ``"control"`` / ``"data"`` -- an **annotation** of a deterministic edge.
      ``edge_identity`` echoes that edge verbatim (``ControlEdge.activity_name``
      for control, ``DataEdge.match_key`` for data) so enrichment can resolve it
      against the inventory's ``lineage``. ``evidence`` / ``confidence`` are not
      used (the deterministic edge *is* the evidence, confidence is implicitly
      high) and must be omitted.
    * ``"inferred"`` -- an agent-asserted coupling the deterministic layer did
      not find. There is no lineage edge to resolve against, so ``edge_identity``
      is an agent-authored descriptor of what couples the pipelines (e.g. a
      shared table or asset name), and ``evidence`` (why the agent believes the
      coupling exists) plus ``confidence`` are **required**. This tier stays
      pattern-agnostic: it does not encode *why* the deterministic layer missed
      the edge, so it generalises to couplings flowx cannot yet see.

    Attributes:
        edge_type: The tier -- ``"control"``, ``"data"``, or ``"inferred"``.
        edge_identity: For ``"control"`` the ``ControlEdge.activity_name``; for
            ``"data"`` the ``DataEdge.match_key`` (both echoed verbatim from a
            real edge); for ``"inferred"`` an agent-authored descriptor of the
            coupling.
        evidence: Inferred edges only -- the observable basis for the asserted
            coupling. Required for ``"inferred"``; must be omitted otherwise.
        confidence: Inferred edges only -- ``"high"`` / ``"medium"`` / ``"low"``.
            Required for ``"inferred"``; must be omitted otherwise.
    """

    edge_type: Literal["control", "data", "inferred"]
    edge_identity: str
    evidence: str | None = None
    confidence: Literal["high", "medium", "low"] | None = None


@dataclass(slots=True, kw_only=True)
class RecommendedPattern:
    """One ranked Databricks target pattern recommended for a pipeline.

    A pipeline insight carries 1-4 of these, ordered best-first, drawn from the
    agent's *holistic* read of the pipeline and grounded in publicly-documented
    Databricks capabilities. ``simplification_pattern`` ranks the distinctive
    capabilities that collapse a legacy pattern ahead of like-for-like ports and
    plain building blocks.

    Attributes:
        pattern: The named, publicly-documented Databricks capability (e.g.
            ``"Lakeflow Connect SQL Server connector"``). Never an invented name.
        fit: One line on why it fits this pipeline / what custom logic it replaces.
        simplification_pattern: ``True`` *only* when the pattern uses a **distinctive**
            Databricks capability that collapses or eliminates a whole legacy
            pattern -- a managed connector (Lakeflow Connect), declarative CDC
            (``AUTO CDC``), Auto Loader, or system tables replacing a home-grown
            logging tier. ``False`` for a like-for-like port AND for plain native
            building blocks that merely re-home the same work (a bare parameterized
            Lakeflow Job, a for-each/run-job orchestrator, a plain Delta control
            table, ``MERGE INTO``) -- "runs on Databricks" is not a simplification,
            so reserve this flag for the capability that makes the old pattern
            *disappear*. Rank the ``True`` patterns first.
    """

    pattern: str
    fit: str
    simplification_pattern: bool


@dataclass(slots=True, kw_only=True)
class SystemRecommendation:
    """The single top-level architectural decision spanning the whole factory.

    Per-pipeline ``recommended_patterns`` are chosen *under* this decision: the
    system-level branch you pick (e.g. adopt a managed connector for an entire
    extraction family) cascades into what each pipeline becomes, so it is authored
    first and the per-pipeline patterns are kept consistent with it. It captures
    the payoff a reader cannot see from any single pipeline card.

    Attributes:
        headline: One line naming the decision a migrator must make before any
            per-pipeline work (e.g. "Managed ingestion collapses the extraction
            factory").
        recommended_patterns: 1-4 whole-system target architectures, ordered
            best-first (the simplifying/native branch first), each a
            :class:`RecommendedPattern`. ``recommended_patterns[0]`` is the
            recommended branch; later entries are the ranked fallbacks.
        cascade: What choosing ``recommended_patterns[0]`` collapses or eliminates
            across the whole system (e.g. "5 child extractors -> managed connector
            pipelines", "version-watermark CSV -> gone"). Empty when the decision
            does not cascade.
        decision_driver: The gating question that selects the branch (e.g. "Is the
            Lakeflow Connect SQL Server connector GA/approved for this source?");
            omit when there is no single deciding factor.
    """

    headline: str
    recommended_patterns: list[RecommendedPattern] = field(default_factory=list)
    cascade: list[str] = field(default_factory=list)
    decision_driver: str | None = None


@dataclass(slots=True, kw_only=True)
class PipelineInsight:
    """Per-pipeline judgment; references a pipeline by name (foreign key)."""

    pipeline: str
    pattern_name: str | None = None
    intent: str | None = None
    databricks_pattern: str | None = None
    recommended_patterns: list[RecommendedPattern] = field(default_factory=list)
    conversion_notes: list[str] = field(default_factory=list)
    risk_if_ignored: str | None = None


@dataclass(slots=True, kw_only=True)
class PipelineRelationship:
    """Cross-pipeline judgment.

    Either annotates one deterministic lineage edge (``lineage_edge.edge_type``
    is ``"control"`` / ``"data"``) or records an agent-inferred coupling the
    deterministic layer could not see (``"inferred"``). Both endpoints are always
    real pipeline names validated against the inventory.
    """

    from_pipeline: str
    to_pipeline: str
    lineage_edge: LineageEdgeRef
    relationship_summary: str | None = None
    databricks_pattern: str | None = None
    risk_if_ignored: str | None = None


@dataclass(slots=True, kw_only=True)
class AgenticMotif:
    """The agent's view on a motif -- its review of a deterministically detected
    pattern, or an agent-inferred pattern the deterministic engine missed.

    Two tiers, mirroring a relationship's ``lineage_edge``:

    * ``"detected"`` -- an **annotation** of a real detected motif. ``motif_id``
      echoes a motif in the inventory's ``motifs`` and every pipeline in
      ``applies_to`` must be one the engine detected it in. The agent confirms,
      refines, or rejects the detection and attaches its own ranked target(s).
    * ``"inferred"`` -- a motif-shaped pattern the deterministic engine did **not**
      detect. ``motif_id`` is an agent-authored name; there is nothing to resolve
      against, so this is a *candidate new deterministic motif*.

    A review can span the whole factory: ``applies_to`` lists every pipeline the
    motif covers (one for an instance, many for a recurring framework). When the
    review is a whole-factory decision (e.g. a metadata-driven framework becoming
    Lakeflow Connect), it is also surfaced in the top-level ``system_recommendation``
    -- this record is that decision's motif-level justification.

    Attributes:
        motif_id: The detected motif reviewed (``"detected"``) or an agent-authored
            pattern name (``"inferred"``).
        applies_to: The pipelines this review covers (>=1); each must exist in the
            inventory, and for ``"detected"`` each must be one the motif was
            detected in.
        origin: ``"detected"`` (reviews a real motif) or ``"inferred"`` (agent-spotted).
        agent_view: The agent's read -- confirm / refine / reject, and why.
        recommended_patterns: The agent's ranked Databricks target(s) for this
            motif, in the same product vocabulary as a pipeline insight; may
            *elevate* beyond the motif's coarse ``databricks_replacement``.
        risk_if_ignored: Optional one-line hazard if the motif is ported naively.
    """

    motif_id: str
    agent_view: str
    applies_to: list[str] = field(default_factory=list)
    origin: Literal["detected", "inferred"] = "detected"
    recommended_patterns: list[RecommendedPattern] = field(default_factory=list)
    risk_if_ignored: str | None = None


@dataclass(slots=True, kw_only=True)
class Insights:
    """Agent-authored insights merged into inventory.json under the ``insights`` key."""

    overview: str | None = None
    system_recommendation: SystemRecommendation | None = None
    pipeline_insights: list[PipelineInsight] = field(default_factory=list)
    pipeline_relationships: list[PipelineRelationship] = field(default_factory=list)
    agentic_motifs: list[AgenticMotif] = field(default_factory=list)
