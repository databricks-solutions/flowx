"""Conversion-plan artifact (routing, #77) -- the user-approved per-component conversion decision.

The routing step (:mod:`flowx.routing`) groups pipelines into connected components over control
lineage, presents each component's two conversion options as first-class peers, and records the
user's per-component choice as ``metadata/conversion_plan.json``. These models are **source-neutral**
and are the library's typed form of that artifact: :class:`ConversionPlan` loads, round-trips and
writes it, and every reader (route, the fill, package) goes through it. Validation of the authored
decision against the inventory lives in :mod:`flowx.routing`.

The agent authors **only** :attr:`ComponentPlan.decision` (and an optional
:attr:`ComponentPlan.rationale`). Everything else -- ``component_id``, ``members``, ``recommended``,
and both conversion options -- is recomputed by the library on record so the recorded facts
can never drift from the inventory or be faked. The library also owns :attr:`ConversionPlan.schema_version`
and the hashes that bind the plan to what it was decided on: :attr:`ConversionPlan.inventory_sha256`
(the deterministic inventory), :attr:`ConversionPlan.source_graphs_sha256` (the saved
``source_graphs.json``) and :attr:`ConversionPlan.agentic_insights_sha256` (the saved
``agentic_insights.json``, when enrich ran).

Phase 1 records one decision per component and applies it to the IR after convert. Each component
also carries :attr:`ComponentPlan.assignments`, reserved for per-node or per-subgraph routing
(deterministic, agentic with a named pattern, or mixed at a boundary); Phase 1 requires it empty.
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

# The plan schema version stamped onto the recorded artifact. Bump on any backwards-incompatible
# change to the recorded shape. Version 2 adds the source graph and agentic insights hashes and the
# reserved per-node assignments.
SCHEMA_VERSION = "2"

# Where the recorded plan lives, beside inventory.json under the output's metadata/ folder.
PLAN_FILENAME = "conversion_plan.json"

# The two conversion routes a component can take.
DECISION_DETERMINISTIC = "deterministic"
DECISION_AGENTIC = "agentic"
DECISIONS: tuple[str, ...] = (DECISION_DETERMINISTIC, DECISION_AGENTIC)

# Routes a reserved per-node assignment may name once Phase 2 uses them; "mixed" joins a
# deterministic and an agentic part at explicit boundary edges.
ROUTE_MIXED = "mixed"
ASSIGNMENT_ROUTES: tuple[str, ...] = (DECISION_DETERMINISTIC, DECISION_AGENTIC, ROUTE_MIXED)

# Recommended-pattern release states surfaced as a neutral disclosure label on the agentic option's
# ``release_disclosures``. ``"ga"`` and ``"unknown"`` are deliberately **silent**
# -- they contribute no entry, and ``"unknown"`` is treated exactly like ``"ga"`` (we do not surface
# or distinguish it). This is factual labelling, never a warning or an alarm.
DISCLOSED_RELEASE_STATES: tuple[str, ...] = ("public_preview", "private_preview", "beta")


@dataclass(slots=True, kw_only=True)
class NodeAssignment:
    """A routing choice for one node or a bounded subgraph inside a component (reserved for Phase 2).

    Attributes:
        pipeline: The pipeline the nodes belong to.
        task_keys: The node, or the nodes of a bounded subgraph, this assignment covers.
        route: One of :data:`ASSIGNMENT_ROUTES`.
        pattern: The named pattern an agentic part is converted to, for example a recommended
            pattern from the insights.
        boundary: For ``"mixed"``, the edges where the deterministic and agentic parts meet.
    """

    pipeline: str
    task_keys: list[str] = field(default_factory=list)
    route: str
    pattern: str | None = None
    boundary: list[dict[str, Any]] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        """Serialise to the recorded JSON shape."""
        return {
            "pipeline": self.pipeline,
            "task_keys": list(self.task_keys),
            "route": self.route,
            "pattern": self.pattern,
            "boundary": list(self.boundary),
        }

    @classmethod
    def from_dict(cls, raw: dict[str, Any]) -> NodeAssignment:
        """Rehydrate from the recorded JSON shape."""
        return cls(
            pipeline=str(raw.get("pipeline", "")),
            task_keys=[str(key) for key in raw.get("task_keys") or []],
            route=str(raw.get("route", "")),
            pattern=raw.get("pattern"),
            boundary=list(raw.get("boundary") or []),
        )


@dataclass(slots=True, kw_only=True)
class ComponentPlan:
    """One component's routing decision.

    Attributes:
        component_id: Stable id assigned by the library (``"component-<n>"``).
        members: The component's pipeline names, sorted (library-computed).
        recommended: The library's starting suggestion -- ``"deterministic"`` when the component is
            engine-capable, else ``"agentic"``.
        decision: The user's authored per-component choice (may override :attr:`recommended`).
        options: Both conversion options with their evidence (library-computed), in the recorded
            JSON shape :mod:`flowx.routing` builds. Optional so the authored input -- which carries
            only the decision -- can round-trip through this model.
        rationale: Optional author note on why this decision was chosen.
        assignments: Reserved per-node or per-subgraph routing (Phase 2); empty in Phase 1.
    """

    component_id: str
    members: list[str] = field(default_factory=list)
    recommended: str | None = None
    decision: str
    options: dict[str, Any] | None = None
    rationale: str | None = None
    assignments: list[NodeAssignment] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        """Serialise to the recorded JSON shape (``rationale`` only when set)."""
        result: dict[str, Any] = {
            "component_id": self.component_id,
            "members": list(self.members),
            "recommended": self.recommended,
            "decision": self.decision,
            "options": self.options,
        }
        if self.rationale is not None:
            result["rationale"] = self.rationale
        result["assignments"] = [assignment.to_dict() for assignment in self.assignments]
        return result

    @classmethod
    def from_dict(cls, raw: dict[str, Any]) -> ComponentPlan:
        """Rehydrate from the recorded JSON shape."""
        return cls(
            component_id=str(raw.get("component_id", "")),
            members=[str(member) for member in raw.get("members") or []],
            recommended=raw.get("recommended"),
            decision=str(raw.get("decision", "")),
            options=raw.get("options"),
            rationale=raw.get("rationale"),
            assignments=[NodeAssignment.from_dict(item) for item in raw.get("assignments") or []],
        )


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
        source_graphs_sha256: Hash of the saved ``source_graphs.json`` the inventory was projected
            from, or ``None`` when the source does not persist one.
        agentic_insights_sha256: Hash of the saved ``agentic_insights.json``, or ``None`` when enrich
            did not run.
    """

    components: list[ComponentPlan] = field(default_factory=list)
    findings: list[str] = field(default_factory=list)
    schema_version: str = SCHEMA_VERSION
    inventory_sha256: str | None = None
    source_graphs_sha256: str | None = None
    agentic_insights_sha256: str | None = None

    def to_dict(self) -> dict[str, Any]:
        """Serialise to the recorded ``conversion_plan.json`` shape."""
        return {
            "schema_version": self.schema_version,
            "inventory_sha256": self.inventory_sha256,
            "source_graphs_sha256": self.source_graphs_sha256,
            "agentic_insights_sha256": self.agentic_insights_sha256,
            "components": [component.to_dict() for component in self.components],
            "findings": list(self.findings),
        }

    @classmethod
    def from_dict(cls, raw: Any) -> ConversionPlan:
        """Rehydrate a recorded plan.

        Raises:
            ValueError: The document is not an object, or was recorded under another schema version
                (re-run ``route`` to record it again).
        """
        if not isinstance(raw, dict):
            raise ValueError(f"{PLAN_FILENAME} must contain a JSON object, got {type(raw).__name__}")
        if raw.get("schema_version") != SCHEMA_VERSION:
            raise ValueError(
                f"{PLAN_FILENAME} has schema_version {raw.get('schema_version')!r}; expected {SCHEMA_VERSION!r}. "
                "Re-run route to record the decision again."
            )
        components = [item for item in raw.get("components") or [] if isinstance(item, dict)]
        return cls(
            schema_version=SCHEMA_VERSION,
            inventory_sha256=raw.get("inventory_sha256"),
            source_graphs_sha256=raw.get("source_graphs_sha256"),
            agentic_insights_sha256=raw.get("agentic_insights_sha256"),
            components=[ComponentPlan.from_dict(item) for item in components],
            findings=[str(finding) for finding in raw.get("findings") or []],
        )

    @classmethod
    def load(cls, output_dir: Path) -> ConversionPlan | None:
        """Load the recorded plan from ``<output_dir>/metadata``, or ``None`` when none was recorded.

        Raises:
            ValueError: The file is not valid JSON or not a version this library reads.
        """
        path = Path(output_dir) / "metadata" / PLAN_FILENAME
        if not path.is_file():
            return None
        try:
            raw = json.loads(path.read_text(encoding="utf-8"))
        except json.JSONDecodeError as error:
            raise ValueError(f"{PLAN_FILENAME} is not valid JSON: {error}") from error
        return cls.from_dict(raw)

    def write(self, output_dir: Path) -> Path:
        """Write the plan to ``<output_dir>/metadata`` atomically; the same plan rewrites identical bytes."""
        path = Path(output_dir) / "metadata" / PLAN_FILENAME
        path.parent.mkdir(parents=True, exist_ok=True)
        temporary = path.with_name(f".{path.name}.tmp")
        temporary.write_text(json.dumps(self.to_dict(), indent=2), encoding="utf-8")
        os.replace(temporary, path)
        return path
