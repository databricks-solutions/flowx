"""Agentic insights (discover phase) -- agent-authored judgment merged into ``inventory.json``.

The deterministic discover layer captures what a source workflow *is*; these models
capture what to *do* about it -- the judgment the deterministic pass can never derive:
a factory-wide architectural recommendation, per-pipeline intent + recommended
Databricks patterns, and how pipelines couple. An external agent *authors* this object
by reading the inventory and the source artifacts; the library only validates and merges
it (see :mod:`flowx.discovery_insights`). There is **no LLM in the tool** -- the same
author-then-validate-merge contract :mod:`flowx.agentic` uses for gap resolution.

These models are **source-neutral**: they describe the shape of the ``insights`` object
independent of whether the pipelines came from ADF or Airflow, because the inventory they
attach to is itself standardised across sources via the shared discovery AST (#61/#62).
The validate/merge engine works on the raw dict form; these dataclasses document the
contract and back the unit tests.

Cross-pipeline couplings come in two accountable tiers (:class:`LineageEdgeRef`):

* ``control`` -- an **annotation** of a deterministic control edge already proven in the
  inventory's per-pipeline ``lineage`` block (an ``ExecutePipeline`` / run-job invocation).
  It carries no facts of its own; enrichment resolves it against a real ``ControlEdge``.
* ``inferred`` -- a coupling the deterministic layer could not see (e.g. data flow buried
  in notebook code, an external trigger, a shared table the parser never resolved). There
  is nothing to resolve against, so it must cite ``evidence`` and a ``confidence`` level and
  is never mistaken for proven lineage.

Deterministic cross-pipeline **data** edges are intentionally *not* a tier here: the shared
``DataEdge`` is task-endpoint and intra-pipeline only, so a cross-pipeline data coupling has
no deterministic edge to annotate and must ride the ``inferred`` tier until cross-pipeline
data lineage lands in a later issue.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Literal

# The insights schema version stamped onto the merged block. Bump on any
# backwards-incompatible change to the authored shape.
SCHEMA_VERSION = "1"

# Cap on a ranked ``recommended_patterns`` list (per pipeline and system-wide). A short,
# ranked shortlist keeps the recommendation legible; an unbounded list is noise.
MAX_RECOMMENDED_PATTERNS = 4

# The confidence levels an ``inferred`` edge may carry.
CONFIDENCE_LEVELS: tuple[str, ...] = ("high", "medium", "low")

# The Databricks GA/Preview release states a recommended pattern may declare, verified against
# current public docs at authoring time. They drive *tiered* surfacing downstream (routing):
#
# * ``"ga"`` -- Generally Available; no warning.
# * ``"public_preview"`` -- Public Preview; generally production-ready and supported per Databricks,
#   so it is *disclosed* informationally, not alarmed (confirm workspace availability).
# * ``"private_preview"`` -- gated; requires confirmed enrollment/entitlement and is not for
#   production without it (a prominent warning).
# * ``"beta"`` -- not production-ready (a prominent warning).
# * ``"unknown"`` -- the release state could not be verified; prefer a verified alternative.
RELEASE_STATES: tuple[str, ...] = ("ga", "public_preview", "private_preview", "beta", "unknown")

# Release states that MUST cite a source (``release_state_source``): the non-GA preview/beta states
# whose availability and eligibility are not self-evident. ``"ga"`` and ``"unknown"`` need no citation
# (``"ga"`` is the stable default; ``"unknown"`` is by definition unverifiable, so it carries no claim
# to ground).
RELEASE_STATES_REQUIRING_SOURCE: tuple[str, ...] = ("public_preview", "private_preview", "beta")


@dataclass(slots=True, kw_only=True)
class LineageEdgeRef:
    """A typed reference from a :class:`PipelineRelationship` to how two pipelines couple.

    Two tiers, validated differently by :mod:`flowx.discovery_insights`:

    * ``"control"`` -- an **annotation** of a deterministic control edge. ``edge_identity``
      echoes that edge's ``ControlEdge.via_task_key`` verbatim so enrichment can resolve the
      full ``(from_pipeline, to_pipeline, edge_identity)`` triple against the inventory's
      ``lineage``. ``evidence`` / ``confidence`` are unused (the proven edge *is* the
      evidence) and must be omitted.
    * ``"inferred"`` -- a coupling the deterministic layer never found. There is no lineage
      edge to resolve against, so ``edge_identity`` is an agent-authored descriptor of what
      couples the pipelines (e.g. a shared table name), and ``evidence`` (why the agent
      believes the coupling exists) plus ``confidence`` are **required**.

    Attributes:
        edge_type: The tier -- ``"control"`` or ``"inferred"``.
        edge_identity: For ``"control"`` the ``ControlEdge.via_task_key`` echoed verbatim
            from a real edge; for ``"inferred"`` an agent-authored descriptor of the coupling.
        evidence: Inferred edges only -- the observable basis for the asserted coupling.
            Required for ``"inferred"``; must be omitted otherwise.
        confidence: Inferred edges only -- ``"high"`` / ``"medium"`` / ``"low"``.
            Required for ``"inferred"``; must be omitted otherwise.
    """

    edge_type: Literal["control", "inferred"]
    edge_identity: str
    evidence: str | None = None
    confidence: Literal["high", "medium", "low"] | None = None


@dataclass(slots=True, kw_only=True)
class RecommendedPattern:
    """One ranked Databricks target pattern recommended for a pipeline or the whole factory.

    A recommendation carries 1-:data:`MAX_RECOMMENDED_PATTERNS` of these, ordered best-first,
    drawn from the agent's holistic read and grounded in publicly-documented Databricks
    capabilities. ``simplification_pattern`` ranks the distinctive capabilities that collapse
    a legacy pattern ahead of like-for-like ports and plain building blocks.

    Attributes:
        pattern: The named, publicly-documented Databricks capability (e.g. ``"Lakeflow
            Connect SQL Server connector"``). Never an invented name.
        fit: One line on why it fits / what custom logic it replaces.
        simplification_pattern: ``True`` *only* when the pattern uses a **distinctive**
            Databricks capability that collapses or eliminates a whole legacy pattern -- a
            managed connector (Lakeflow Connect), declarative CDC (``AUTO CDC``), Auto Loader,
            or system tables replacing a home-grown logging tier. ``False`` for a like-for-like
            port and for plain native building blocks that merely re-home the same work.
            Rank the ``True`` patterns first.
        release_state: The pattern's verified Databricks release state -- one of
            :data:`RELEASE_STATES` -- established against **current public docs** at authoring time
            (never hardcoded). It is surfaced downstream (routing) as a neutral factual **disclosure**,
            never a warning: ``"ga"`` and ``"unknown"`` are silent (``"unknown"`` is treated exactly
            like ``"ga"`` -- the two are indistinguishable in the surfaced output); ``"public_preview"``
            is disclosed as production-ready (Public Preview is generally production-ready and supported
            per Databricks); ``"private_preview"`` and ``"beta"`` are stated as plain factual labels.
            **Required** whenever :attr:`simplification_pattern` is ``True`` -- a distinctive capability
            must declare its release state; optional otherwise, defaulting to ``None`` (unstated) for
            back-compat with insights authored before this field.
        release_state_source: The doc URL / citation grounding :attr:`release_state`. **Required**
            (a non-empty string) whenever ``release_state`` is one of
            :data:`RELEASE_STATES_REQUIRING_SOURCE` (``"public_preview"`` / ``"private_preview"`` /
            ``"beta"``); not required for ``"ga"`` or ``"unknown"``.
    """

    pattern: str
    fit: str
    simplification_pattern: bool
    release_state: Literal["ga", "public_preview", "private_preview", "beta", "unknown"] | None = None
    release_state_source: str | None = None


@dataclass(slots=True, kw_only=True)
class SystemRecommendation:
    """The single top-level architectural decision spanning the whole factory.

    Per-pipeline ``recommended_patterns`` are chosen *under* this decision: the system-level
    branch you pick (e.g. adopt a managed connector for an entire extraction family) cascades
    into what each pipeline becomes, so it is authored first and the per-pipeline patterns are
    kept consistent with it. It captures the payoff a reader cannot see from any single
    pipeline card.

    Attributes:
        headline: One line naming the decision a migrator must make before any per-pipeline
            work (e.g. "Managed ingestion collapses the extraction factory").
        recommended_patterns: 1-:data:`MAX_RECOMMENDED_PATTERNS` whole-system target
            architectures, ordered best-first (the simplifying/native branch first).
            ``recommended_patterns[0]`` is the recommended branch; later entries are ranked
            fallbacks.
        cascade: What choosing ``recommended_patterns[0]`` collapses or eliminates across the
            whole system (e.g. "5 child extractors -> managed connector pipelines"). Empty
            when the decision does not cascade.
        decision_driver: The gating question that selects the branch (e.g. "Is the Lakeflow
            Connect SQL Server connector GA/approved for this source?"); omit when there is no
            single deciding factor.
    """

    headline: str
    recommended_patterns: list[RecommendedPattern] = field(default_factory=list)
    cascade: list[str] = field(default_factory=list)
    decision_driver: str | None = None


@dataclass(slots=True, kw_only=True)
class PipelineInsight:
    """Per-pipeline judgment; references a pipeline by name (a foreign key to the inventory).

    Attributes:
        pipeline: Name of the pipeline this insight annotates. Must be a real pipeline in the
            inventory (validated on enrichment).
        pattern_name: A short label naming the pipeline's recognised shape, when one applies.
        intent: What the pipeline is really trying to accomplish, in business terms.
        databricks_pattern: The single headline Databricks pattern the pipeline maps to.
        recommended_patterns: 1-:data:`MAX_RECOMMENDED_PATTERNS` ranked target patterns,
            best-first, chosen under the factory-wide :class:`SystemRecommendation`.
        conversion_notes: Concrete notes a migrator should heed when converting.
        risk_if_ignored: What breaks or degrades if the recommendation is not followed.
    """

    pipeline: str
    pattern_name: str | None = None
    intent: str | None = None
    databricks_pattern: str | None = None
    recommended_patterns: list[RecommendedPattern] = field(default_factory=list)
    conversion_notes: list[str] = field(default_factory=list)
    risk_if_ignored: str | None = None


@dataclass(slots=True, kw_only=True)
class PipelineRelationship:
    """Cross-pipeline judgment; both endpoints are real inventory pipeline names.

    Either annotates one deterministic control edge (``lineage_edge.edge_type == "control"``)
    or records an agent-inferred coupling the deterministic layer could not see
    (``"inferred"``).

    Attributes:
        from_pipeline: Source endpoint -- the caller (control) or upstream (inferred) pipeline.
        to_pipeline: Target endpoint -- the callee (control) or downstream (inferred) pipeline.
        lineage_edge: The typed edge reference describing and grounding the coupling.
        relationship_summary: One line on how the two pipelines relate.
        databricks_pattern: The Databricks construct that should carry this coupling.
        risk_if_ignored: What breaks if the coupling is dropped in the migration.
    """

    from_pipeline: str
    to_pipeline: str
    lineage_edge: LineageEdgeRef
    relationship_summary: str | None = None
    databricks_pattern: str | None = None
    risk_if_ignored: str | None = None


@dataclass(slots=True, kw_only=True)
class Insights:
    """Agent-authored insights merged into ``inventory.json`` under the additive ``insights`` key.

    The agent authors only these four content fields. The library injects the ``schema_version``
    and the ``inventory_sha256`` fingerprint on merge (see :mod:`flowx.discovery_insights`), so
    the authored insights stay bound to the exact deterministic inventory they describe.

    Attributes:
        overview: A short factory-wide narrative -- what this collection of pipelines is.
        system_recommendation: The one whole-factory architectural decision.
        pipeline_insights: Per-pipeline judgments, one per pipeline the agent chose to annotate.
        pipeline_relationships: Cross-pipeline couplings (control annotations + inferred).
    """

    overview: str | None = None
    system_recommendation: SystemRecommendation | None = None
    pipeline_insights: list[PipelineInsight] = field(default_factory=list)
    pipeline_relationships: list[PipelineRelationship] = field(default_factory=list)
