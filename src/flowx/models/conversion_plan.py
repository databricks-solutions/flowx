"""Conversion-plan artifact (routing, #77) -- the user-approved per-component conversion decision.

The routing step (:mod:`flowx.routing`) groups pipelines into connected components over control
lineage, presents each component's two conversion options as first-class peers, and records the
user's per-component choice as ``metadata/conversion_plan.json``. These models are **source-neutral**
and document the shape of that artifact; the validate/record engine works on the raw dict form and
these dataclasses back the unit tests, mirroring the split in :mod:`flowx.models.insights`.

The agent authors **only** :attr:`ComponentPlan.decision` (and an optional
:attr:`ComponentPlan.rationale`). Everything else -- ``component_id``, ``members``, ``recommended``,
and both :class:`ComponentOptions` -- is recomputed by the library on record so the recorded facts
can never drift from the inventory or be faked. The library also owns :attr:`ConversionPlan.schema_version`
and :attr:`ConversionPlan.inventory_sha256` (the fingerprint that binds the plan to the inventory).

This is Phase-1, descriptive-only routing metadata: recording a plan does not alter ``convert``.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Literal

# The plan schema version stamped onto the recorded artifact. Bump on any backwards-incompatible
# change to the recorded shape.
SCHEMA_VERSION = "1"

# The two conversion routes a component can take.
DECISION_DETERMINISTIC = "deterministic"
DECISION_AGENTIC = "agentic"
DECISIONS: tuple[str, ...] = (DECISION_DETERMINISTIC, DECISION_AGENTIC)

# Recommended-pattern release states surfaced as a neutral disclosure label on the agentic option
# (:attr:`AgenticOption.release_disclosures`). ``"ga"`` and ``"unknown"`` are deliberately **silent**
# -- they contribute no entry, and ``"unknown"`` is treated exactly like ``"ga"`` (we do not surface
# or distinguish it). This is factual labelling, never a warning or an alarm.
DISCLOSED_RELEASE_STATES: tuple[str, ...] = ("public_preview", "private_preview", "beta")


@dataclass(slots=True, kw_only=True)
class DeterministicOption:
    """The deterministic (1:1 engine) conversion option for a component.

    Attributes:
        capable: ``True`` when every activity in the component is engine-capable -- each either has a
            ``"deterministic"`` strategy or is claimed by a detected motif -- so the whole component
            can convert deterministically with no gap.
        activity_counts: Count of activities by strategy bucket (``deterministic`` / ``agentic`` /
            ``unsupported``) across the component's pipelines.
        motifs: The motif ids detected in the component (the #64 multi-activity capability signal),
            sorted and de-duplicated.
        uncovered: One entry per activity that keeps the component from being fully deterministic --
            a dict of ``pipeline`` / ``activity`` / ``type`` / ``strategy``. Empty when ``capable``.
    """

    capable: bool
    activity_counts: dict[str, int] = field(default_factory=dict)
    motifs: list[str] = field(default_factory=list)
    uncovered: list[dict[str, Any]] = field(default_factory=list)


@dataclass(slots=True, kw_only=True)
class AgenticPattern:
    """One recommended Databricks pattern for the agentic option, drawn from a pipeline's insights.

    Attributes:
        pipeline: The member pipeline the pattern was recommended for.
        pattern: The named, publicly-documented Databricks capability (verbatim from the insight).
        fit: One line on why it fits / what it replaces.
        simplification_pattern: ``True`` when the pattern uses a distinctive capability that collapses
            a whole legacy pattern (e.g. a multi-pipeline -> Lakeflow Connect re-architecture).
        release_state: The pattern's verified Databricks GA/Preview release state, carried verbatim
            from the insight (one of :data:`~flowx.models.insights.RELEASE_STATES`, or ``None`` when
            the insight left it unstated). Drives the neutral release-state disclosure on
            :class:`AgenticOption`.
        release_state_source: The doc URL / citation grounding :attr:`release_state`, carried verbatim
            from the insight; ``None`` when unstated.
    """

    pipeline: str
    pattern: str
    fit: str
    simplification_pattern: bool
    release_state: Literal["ga", "public_preview", "private_preview", "beta", "unknown"] | None = None
    release_state_source: str | None = None


@dataclass(slots=True, kw_only=True)
class AgenticOption:
    """The agentic conversion option for a component.

    Attributes:
        recommended_patterns: The recommended patterns gathered from the member pipelines' insights,
            each tagged with its pipeline. Empty when the inventory carries no insights.
        has_simplification: ``True`` when any recommended pattern is a ``simplification_pattern`` --
            surfaced prominently so the user sees a re-architecture option, not a buried sub-key.
        release_disclosures: A neutral, per-pattern **disclosure** of any non-silent ``release_state``
            -- one entry per recommended pattern whose state is in :data:`DISCLOSED_RELEASE_STATES`,
            carrying the ``pipeline``, ``pattern``, ``release_state``, and a factual ``label``
            (``public_preview`` labelled "Public Preview (production-ready)"; ``private_preview`` /
            ``beta`` stated as the plain labels "Private Preview" / "Beta"). ``"ga"`` and ``"unknown"``
            are **silent** -- they add nothing (``"unknown"`` is treated exactly like ``"ga"``). Empty
            when nothing needs disclosing. Factual labelling, never a warning or an alarm.
    """

    recommended_patterns: list[AgenticPattern] = field(default_factory=list)
    has_simplification: bool = False
    release_disclosures: list[dict[str, Any]] = field(default_factory=list)


@dataclass(slots=True, kw_only=True)
class ComponentOptions:
    """Both conversion options for a component, as first-class peers."""

    deterministic: DeterministicOption
    agentic: AgenticOption


@dataclass(slots=True, kw_only=True)
class ComponentPlan:
    """One component's routing decision.

    Attributes:
        component_id: Stable id assigned by the library (``"component-<n>"``).
        members: The component's pipeline names, sorted (library-computed).
        recommended: The library's starting suggestion -- ``"deterministic"`` when the component is
            engine-capable, else ``"agentic"``.
        decision: The user's authored per-component choice (may override :attr:`recommended`).
        options: Both conversion options with their evidence (library-computed). Optional here so the
            authored input -- which carries only the decision -- can round-trip through this model.
        rationale: Optional author note on why this decision was chosen.
    """

    component_id: str
    members: list[str] = field(default_factory=list)
    recommended: str | None = None
    decision: str
    options: ComponentOptions | None = None
    rationale: str | None = None


@dataclass(slots=True, kw_only=True)
class ConversionPlan:
    """The recorded conversion-plan artifact.

    Attributes:
        components: One :class:`ComponentPlan` per connected component.
        findings: Human-readable notes about unresolved/dangling control edges retained during
            component computation (never silently severed).
        schema_version: Library-owned plan schema version.
        inventory_sha256: Library-owned fingerprint binding the plan to the deterministic inventory
            base (see :func:`flowx.discovery_insights.inventory_fingerprint`).
    """

    components: list[ComponentPlan] = field(default_factory=list)
    findings: list[str] = field(default_factory=list)
    schema_version: str = SCHEMA_VERSION
    inventory_sha256: str | None = None
