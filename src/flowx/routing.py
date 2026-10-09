"""Compute a per-connected-component conversion route over the discover inventory (#77).

The discover phase writes a deterministic ``metadata/inventory.json``; :mod:`flowx.discovery_insights`
optionally enriches it with an additive ``insights`` block. This module turns those signals into a
**routing recommendation** and records the **user's decision** as a fingerprint-bound
``metadata/conversion_plan.json`` artifact -- a Phase-1, descriptive-only step:

* pipelines are grouped into weak/undirected **connected components** over the inventory's control
  lineage (``lineage.control_edges``), so mutually-referencing pipelines are decided together and a
  caller/callee reference is never split across incompatible routes;
* each component surfaces **both conversion options as first-class peers** -- a *deterministic*
  option (the engine-capability assessment: every activity engine-capable via its ``strategy`` or
  claimed by a detected motif, plus its motif/coverage evidence and any uncovered gaps) and an
  *agentic* option (the recommended Databricks patterns from the ``insights`` block, with any
  ``simplification_pattern`` such as a multi-pipeline -> Lakeflow Connect re-architecture surfaced
  prominently) -- so the user can choose per group;
* ``recommended`` is a library-computed starting suggestion (deterministic when the whole component
  is engine-capable), and ``decision`` is the user's authored per-component choice, ``None`` while
  it is still pending;
* **suggested groupings** join whole components the insights link -- by an inferred relationship,
  or by a simplification pattern recommended for pipelines in more than one component -- so the user
  can accept converting them together agentically as one unit (:func:`suggest_groupings`).

Like :mod:`flowx.discovery_insights`, there is **no LLM here**: the tool computes the recommendation
deterministically and only *validates and records* the agent-authored decision. Route writes the
recommendation straight into ``conversion_plan.json`` with every decision pending; the agent edits
the decisions, the groupings' ``accepted`` flags and the optional routing ``conversation`` there (or
passes them as a plan), and the library recomputes ``members``, ``recommended``, both options'
evidence and the groupings on every record so they can never drift from the inventory or be faked.

The recorded plan is bound to the inventory via :func:`flowx.discovery_insights.inventory_fingerprint`
-- a SHA-256 over the deterministic inventory base (the ``insights`` block excluded). That base is
exactly the structural signal that decides component membership and engine capability (pipelines,
control lineage, per-activity strategy, motifs); insights are advisory evidence for the agentic
option, not part of the binding. The write is atomic (temp file + ``os.replace``) and idempotent, so
re-recording the same decision against the same inventory rewrites byte-identical bytes.

This is additive, opt-in routing metadata only: with no recorded plan, ``convert`` and ``package``
behave exactly as today. Component computation reads ``lineage.control_edges``, so the artifact's
shape is source-neutral, but agentic decisions are accepted only for ADF inventories in Phase 1 (see
:data:`AGENTIC_ROUTING_SOURCES`). Airflow gaps stay with the fingerprint-bound per-gap resolver, so
for an Airflow inventory route recommends and records deterministic decisions only.
"""

from __future__ import annotations

import json
from collections import Counter
from collections.abc import Iterable
from pathlib import Path
from typing import Any

from flowx.discovery_insights import (
    AGENTIC_INSIGHTS_FILENAME,
    ENRICH_LOCK_FILENAME,
    INSIGHTS_KEY,
    agentic_insights_hash_violations,
    inventory_fingerprint,
)
from flowx.models.conversion_plan import (
    BASIS_INFERRED_RELATIONSHIP,
    BASIS_SHARED_PATTERN,
    DECISION_AGENTIC,
    DECISION_DETERMINISTIC,
    DECISIONS,
    PLAN_FILENAME,
    SCHEMA_VERSION,
    ComponentPlan,
    ConversationEntry,
    ConversionPlan,
    SuggestedGrouping,
)

# The strategy value that marks an activity as individually engine-capable (a 1:1 deterministic
# translation). Any other value (``"agentic"`` / ``"unsupported"`` / missing) is a gap unless the
# activity is claimed by a detected motif -- the multi-activity capability signal from #64.
_DETERMINISTIC_STRATEGY = "deterministic"


# Sources whose inventories route may send agentic in Phase 1. An inventory with no top-level
# ``source`` predates the unified emitter and is ADF.
AGENTIC_ROUTING_SOURCES = frozenset({"adf"})
_LEGACY_INVENTORY_SOURCE = "adf"

# Each level of the plan holds authored keys and library-owned keys. The library recomputes its own
# keys on every record, so an edited copy of the recorded plan can be handed straight back; any key
# that is neither is refused.
_PLAN_AUTHORED_KEYS = {"components", "suggested_groupings", "conversation"}
_PLAN_LIBRARY_KEYS = {
    "schema_version",
    "inventory_sha256",
    "source_graphs_sha256",
    "agentic_insights_sha256",
    "findings",
}
_COMPONENT_AUTHORED_KEYS = {"component_id", "members", "decision", "rationale", "assignments"}
_COMPONENT_LIBRARY_KEYS = {"recommended", "options"}
_GROUPING_AUTHORED_KEYS = {"grouping_id", "accepted"}
_GROUPING_LIBRARY_KEYS = {"components", "members", "basis"}
_CONVERSATION_KEYS = {"question", "answer"}

# How to bring agentic_insights.json and inventory.json back in step, mirroring enrich's own advice.
_INSIGHTS_RECOVERY = (
    "enrich may have stopped between its two writes, or one of the files was edited. To recover, run enrich "
    f"again with the same insights (locally, first delete metadata/{ENRICH_LOCK_FILENAME} if it is still there; "
    "on the hosted MCP server, run discover again, then enrich prepare and apply)"
)


def inventory_source(inventory: dict[str, Any]) -> str:
    """The source an inventory was discovered from (an inventory without one is ADF)."""
    return str(inventory.get("source", _LEGACY_INVENTORY_SOURCE))


def agentic_routing_supported(inventory: dict[str, Any]) -> bool:
    """Whether route may take an agentic decision for this inventory's source (ADF only in Phase 1)."""
    return inventory_source(inventory) in AGENTIC_ROUTING_SOURCES


def _agentic_routing_unsupported_note(inventory: dict[str, Any]) -> str:
    """The finding / violation text that explains why a non-ADF inventory routes deterministic only."""
    return (
        f"agentic routing is ADF-only in Phase 1 (inventory source {inventory.get('source')!r}); "
        "route every component 'deterministic' and resolve its gaps with the flowx-resolve-airflow-gaps skill"
    )


def _pipeline_names(inventory: dict[str, Any]) -> set[str]:
    """The set of real pipeline names in the inventory (the foreign-key domain).

    A local copy of the same projection :mod:`flowx.discovery_insights` uses, kept here so routing
    does not depend on that module's private helpers.
    """
    return {
        str(pipeline["name"])
        for pipeline in inventory.get("pipelines", [])
        if isinstance(pipeline, dict) and pipeline.get("name") is not None
    }


def _control_edges(inventory: dict[str, Any]) -> Iterable[tuple[str, str, str, bool]]:
    """Yield ``(source_workflow, target_workflow, via_task_key, resolved)`` for every control edge.

    Lineage is placed per pipeline (one block beside each pipeline's ``activities``), so every
    pipeline's ``lineage.control_edges`` are gathered into one stream, preserving inventory order.
    """
    for pipeline in inventory.get("pipelines", []):
        if not isinstance(pipeline, dict):
            continue
        lineage = pipeline.get("lineage") or {}
        for edge in lineage.get("control_edges", []):
            if not isinstance(edge, dict):
                continue
            yield (
                str(edge.get("source_workflow") or ""),
                str(edge.get("target_workflow") or ""),
                str(edge.get("via_task_key") or ""),
                bool(edge.get("resolved", True)),
            )


def build_components(inventory: dict[str, Any]) -> tuple[list[list[str]], list[str]]:
    """Group pipelines into weak/undirected connected components over control lineage.

    Two pipelines share a component when a **resolved** control edge joins them in either direction
    and both endpoints are real inventory pipelines. Isolated pipelines each form their own
    singleton component. An edge whose callee is unresolved (``resolved`` is False) or names a
    pipeline absent from the inventory is **not** used to join anything and is **not** silently
    severed -- it is recorded as a finding so a partial export never quietly collapses two
    components into one or drops a coupling.

    Args:
        inventory: The discover ``inventory.json`` document (deterministic or enriched).

    Returns:
        ``(components, findings)`` where ``components`` is a list of member lists -- each sorted,
        the whole list ordered by first member -- so the output is deterministic and idempotent for
        a given inventory; and ``findings`` is a sorted list of human-readable notes about
        unresolved/dangling edges.
    """
    names = _pipeline_names(inventory)
    parent: dict[str, str] = {name: name for name in names}

    def find(node: str) -> str:
        root = node
        while parent[root] != root:
            root = parent[root]
        while parent[node] != root:
            parent[node], node = root, parent[node]
        return root

    def union(left: str, right: str) -> None:
        left_root, right_root = find(left), find(right)
        if left_root != right_root:
            # Attach the lexicographically larger root under the smaller for a stable shape.
            low, high = sorted((left_root, right_root))
            parent[high] = low

    findings: set[str] = set()
    for source, target, via, resolved in _control_edges(inventory):
        if not resolved or not target or target not in names:
            shown_target = target or "<unresolved>"
            findings.add(
                f"unresolved control edge {source!r} -> {shown_target!r} (via {via!r}) "
                f"kept as a finding, not severed; {source!r} is routed within its own component"
            )
            continue
        if source in names:
            union(source, target)

    groups: dict[str, list[str]] = {}
    for name in names:
        groups.setdefault(find(name), []).append(name)
    components = sorted((sorted(members) for members in groups.values()), key=lambda members: members)
    return components, sorted(findings)


def _activities_by_pipeline(inventory: dict[str, Any]) -> dict[str, list[dict[str, Any]]]:
    """Map each pipeline name to its inventory ``activities`` list (flattened, as emitted)."""
    result: dict[str, list[dict[str, Any]]] = {}
    for pipeline in inventory.get("pipelines", []):
        if isinstance(pipeline, dict) and pipeline.get("name") is not None:
            activities = pipeline.get("activities") or []
            result[str(pipeline["name"])] = [entry for entry in activities if isinstance(entry, dict)]
    return result


def _motifs_by_pipeline(inventory: dict[str, Any]) -> dict[str, list[dict[str, Any]]]:
    """Map each pipeline name to its additive ``motifs`` list (the #64 capability signal)."""
    result: dict[str, list[dict[str, Any]]] = {}
    for pipeline in inventory.get("pipelines", []):
        if isinstance(pipeline, dict) and pipeline.get("name") is not None:
            motifs = pipeline.get("motifs") or []
            result[str(pipeline["name"])] = [entry for entry in motifs if isinstance(entry, dict)]
    return result


def _pipeline_insights(inventory: dict[str, Any]) -> dict[str, dict[str, Any]]:
    """Map each pipeline name to its ``insights.pipeline_insights`` entry, when insights are present.

    Returns an empty map when the inventory has not been enriched, so the agentic option degrades
    to an empty pattern list rather than failing.
    """
    insights = inventory.get("insights")
    if not isinstance(insights, dict):
        return {}
    result: dict[str, dict[str, Any]] = {}
    for entry in insights.get("pipeline_insights", []):
        if isinstance(entry, dict) and entry.get("pipeline") is not None:
            result[str(entry["pipeline"])] = entry
    return result


def _deterministic_option(members: list[str], inventory: dict[str, Any]) -> dict[str, Any]:
    """Assess the deterministic (1:1 engine) conversion of a component.

    An activity is engine-capable when its ``strategy`` is ``"deterministic"`` or it is claimed by a
    detected motif (present in some motif's ``member_task_keys``). ``capable`` is True only when the
    whole component leaves no gap; ``uncovered`` cites each activity that keeps it from being fully
    deterministic.
    """
    activities_by_pipeline = _activities_by_pipeline(inventory)
    motifs_by_pipeline = _motifs_by_pipeline(inventory)

    motif_ids: list[str] = []
    # Motif coverage is keyed by (pipeline, activity name), never bare name: a motif claiming a task
    # in one pipeline must not mark an unrelated same-named task in another pipeline of the component
    # as covered, which would silently hide a real gap.
    covered_activities: set[tuple[str, str]] = set()
    for pipeline in members:
        for motif in motifs_by_pipeline.get(pipeline, []):
            if motif.get("motif_id") is not None:
                motif_ids.append(str(motif["motif_id"]))
            covered_activities.update((pipeline, str(key)) for key in motif.get("member_task_keys") or [])

    counts = {"deterministic": 0, "agentic": 0, "unsupported": 0}
    uncovered: list[dict[str, Any]] = []
    for pipeline in members:
        for activity in activities_by_pipeline.get(pipeline, []):
            strategy = activity.get("strategy")
            bucket = strategy if strategy in ("deterministic", "agentic") else "unsupported"
            counts[bucket] += 1
            name = activity.get("name")
            if strategy != _DETERMINISTIC_STRATEGY and (pipeline, name) not in covered_activities:
                uncovered.append(
                    {
                        "pipeline": pipeline,
                        "activity": name,
                        "type": activity.get("type"),
                        "strategy": strategy,
                    }
                )
    return {
        "capable": not uncovered,
        "activity_counts": counts,
        "motifs": sorted(set(motif_ids)),
        "uncovered": uncovered,
    }


# Neutral disclosure labels for the agentic option, keyed by release state. ``"ga"`` and ``"unknown"``
# carry NO entry -- they are silent (we do not surface them, and ``"unknown"`` is treated exactly like
# ``"ga"``). ``"public_preview"`` is labelled production-ready per Databricks; ``"private_preview"`` /
# ``"beta"`` are stated as plain factual labels. This is disclosure, not a warning.
_RELEASE_STATE_LABELS: dict[str, str] = {
    "public_preview": "Public Preview (production-ready)",
    "private_preview": "Private Preview",
    "beta": "Beta",
}


def _release_disclosure(pattern: dict[str, Any]) -> dict[str, Any] | None:
    """Build a neutral release-state disclosure for one recommended pattern, or ``None``.

    Returns ``None`` for ``"ga"`` / ``"unknown"`` (both silent -- ``"unknown"`` is treated exactly like
    ``"ga"``) and for a pattern with no ``release_state``. ``"public_preview"`` / ``"private_preview"``
    / ``"beta"`` each yield a factual state ``label`` -- ``public_preview`` noted as production-ready --
    with no warning framing. The cited source is appended when the insight carried one.
    """
    release_state = pattern.get("release_state")
    label = _RELEASE_STATE_LABELS.get(release_state) if isinstance(release_state, str) else None
    if label is None:
        return None
    message = f"'{pattern.get('pattern')}' (pipeline '{pattern.get('pipeline')}'): {label}."
    source = pattern.get("release_state_source")
    if isinstance(source, str) and source.strip():
        message = f"{message} Source: {source.strip()}"
    return {
        "pipeline": pattern.get("pipeline"),
        "pattern": pattern.get("pattern"),
        "release_state": release_state,
        "label": label,
        "message": message,
    }


def _agentic_option(members: list[str], inventory: dict[str, Any]) -> dict[str, Any]:
    """Surface the agent-authored recommended patterns for a component as a first-class option.

    Draws every ``recommended_patterns`` entry from the member pipelines' insights, tagging each with
    its pipeline (the per-pattern ``release_state`` / ``release_state_source`` ride along in the
    ``{**pattern}`` spread), and computes two signals for the user:

    * ``has_simplification`` -- any pattern is a ``simplification_pattern`` (a distinctive
      re-architecture such as a multi-pipeline -> Lakeflow Connect collapse), surfaced prominently.
    * ``release_disclosures`` -- a neutral, per-pattern disclosure of any non-silent ``release_state``
      (:data:`~flowx.models.conversion_plan.DISCLOSED_RELEASE_STATES`): ``public_preview`` labelled
      production-ready, ``private_preview`` / ``beta`` stated as plain labels. ``ga`` and ``unknown``
      add nothing (silent). Factual labelling, not a warning.
    """
    insights_by_pipeline = _pipeline_insights(inventory)
    recommended_patterns: list[dict[str, Any]] = []
    for pipeline in members:
        insight = insights_by_pipeline.get(pipeline)
        if not insight:
            continue
        for pattern in insight.get("recommended_patterns") or []:
            if isinstance(pattern, dict):
                recommended_patterns.append({"pipeline": pipeline, **pattern})
    has_simplification = any(pattern.get("simplification_pattern") for pattern in recommended_patterns)
    release_disclosures: list[dict[str, Any]] = []
    for pattern in recommended_patterns:
        disclosure = _release_disclosure(pattern)
        if disclosure is not None:
            release_disclosures.append(disclosure)
    return {
        "recommended_patterns": recommended_patterns,
        "has_simplification": has_simplification,
        "release_disclosures": release_disclosures,
    }


def recommend_component(members: list[str], inventory: dict[str, Any]) -> tuple[str, dict[str, Any]]:
    """Compute the recommended route and both conversion options for one component.

    Args:
        members: The component's pipeline names.
        inventory: The discover ``inventory.json`` document (deterministic or enriched).

    Returns:
        ``(recommended, options)`` where ``recommended`` is ``"deterministic"`` when the whole
        component is engine-capable or the inventory's source cannot be routed agentic, else
        ``"agentic"``; and ``options`` carries the ``deterministic`` and ``agentic`` peers as
        first-class entries.
    """
    deterministic = _deterministic_option(members, inventory)
    agentic = _agentic_option(members, inventory)
    route_deterministic = deterministic["capable"] or not agentic_routing_supported(inventory)
    recommended = _DETERMINISTIC_STRATEGY if route_deterministic else "agentic"
    return recommended, {"deterministic": deterministic, "agentic": agentic}


def suggest_groupings(inventory: dict[str, Any]) -> list[dict[str, Any]]:
    """Suggest whole components to convert together, agentically, from the inventory's insights.

    Two kinds of link join components, both read from the insights enrich recorded:

    * an ``inferred`` pipeline relationship (a coupling the parser could not see, with the agent's
      evidence and confidence) between pipelines of different components;
    * one simplification pattern (a ``recommended_patterns`` entry with ``simplification_pattern``
      true, by the same pattern name) recommended for pipelines in more than one component -- the
      independent extractors one managed pattern could replace, which have no coupling at all.

    Each set of two or more components joined by these links, directly or through each other,
    becomes one suggestion, so suggestions never overlap. A suggestion lists its ``basis``: every
    relationship or shared pattern behind it. Nothing is suggested without insights, or for an
    inventory whose source cannot be routed agentic (:data:`AGENTIC_ROUTING_SOURCES`).

    Returns ``[{"grouping_id", "components", "members", "basis", "accepted": False}]`` with ids
    ``grouping-<n>`` in component order, deterministic for a given inventory.
    """
    insights = inventory.get(INSIGHTS_KEY)
    if not isinstance(insights, dict) or not agentic_routing_supported(inventory):
        return []
    components_by_id = _components_by_id(inventory)
    component_of = {member: component_id for component_id, members in components_by_id.items() for member in members}

    links: list[tuple[set[str], dict[str, Any]]] = []
    for relationship in insights.get("pipeline_relationships") or []:
        edge = relationship.get("lineage_edge") if isinstance(relationship, dict) else None
        if not isinstance(edge, dict) or edge.get("edge_type") != "inferred":
            continue
        source, target = relationship.get("from_pipeline"), relationship.get("to_pipeline")
        if source not in component_of or target not in component_of:
            continue
        basis: dict[str, Any] = {
            "kind": BASIS_INFERRED_RELATIONSHIP,
            "from_pipeline": source,
            "to_pipeline": target,
            "edge_identity": edge.get("edge_identity"),
            "evidence": edge.get("evidence"),
            "confidence": edge.get("confidence"),
        }
        if relationship.get("relationship_summary"):
            basis["relationship_summary"] = relationship["relationship_summary"]
        links.append(({component_of[source], component_of[target]}, basis))

    pipelines_by_pattern: dict[str, set[str]] = {}
    for insight in insights.get("pipeline_insights") or []:
        pipeline = insight.get("pipeline") if isinstance(insight, dict) else None
        if pipeline not in component_of:
            continue
        for pattern in insight.get("recommended_patterns") or []:
            if isinstance(pattern, dict) and pattern.get("simplification_pattern") is True:
                if isinstance(pattern.get("pattern"), str) and pattern["pattern"]:
                    pipelines_by_pattern.setdefault(pattern["pattern"], set()).add(pipeline)
    for pattern_name in sorted(pipelines_by_pattern):
        pipelines = pipelines_by_pattern[pattern_name]
        basis = {"kind": BASIS_SHARED_PATTERN, "pattern": pattern_name, "pipelines": sorted(pipelines)}
        links.append(({component_of[pipeline] for pipeline in pipelines}, basis))

    parent = {component_id: component_id for component_id in components_by_id}

    def find(component_id: str) -> str:
        while parent[component_id] != component_id:
            component_id = parent[component_id]
        return component_id

    order = {component_id: index for index, component_id in enumerate(components_by_id)}
    for linked, _ in links:
        roots = sorted({find(component_id) for component_id in linked}, key=order.__getitem__)
        for root in roots[1:]:
            parent[root] = roots[0]

    joined: dict[str, list[str]] = {}
    for component_id in components_by_id:
        joined.setdefault(find(component_id), []).append(component_id)
    groupings: list[dict[str, Any]] = []
    for component_ids in joined.values():
        if len(component_ids) < 2:
            continue
        members = sorted(member for component_id in component_ids for member in components_by_id[component_id])
        groupings.append(
            {
                "grouping_id": f"grouping-{len(groupings) + 1}",
                "components": component_ids,
                "members": members,
                "basis": [basis for linked, basis in links if linked <= set(component_ids) and len(linked) > 1],
                "accepted": False,
            }
        )
    return groupings


def build_recommendation(inventory: dict[str, Any]) -> dict[str, Any]:
    """Compute the full routing recommendation over every component in the inventory.

    Returns a dict with ``components`` (each carrying ``component_id``, sorted ``members``,
    ``recommended`` and both ``options``), the ``suggested_groupings`` (see :func:`suggest_groupings`),
    the ``findings`` from component computation, and a ``default_plan`` that proposes
    ``decision == recommended`` for every component -- ready to hand straight to :func:`record_plan`
    when the user accepts the recommendations wholesale, or to edit per component for overrides.
    """
    components, findings = build_components(inventory)
    if not agentic_routing_supported(inventory):
        findings = [*findings, _agentic_routing_unsupported_note(inventory)]
    component_entries: list[dict[str, Any]] = []
    default_plan_components: list[dict[str, Any]] = []
    for index, members in enumerate(components, start=1):
        component_id = f"component-{index}"
        recommended, options = recommend_component(members, inventory)
        component_entries.append(
            {"component_id": component_id, "members": members, "recommended": recommended, "options": options}
        )
        default_plan_components.append({"component_id": component_id, "members": members, "decision": recommended})
    return {
        "components": component_entries,
        "suggested_groupings": suggest_groupings(inventory),
        "findings": findings,
        "default_plan": {"components": default_plan_components},
    }


# --------------------------------------------------------------------------- #
# Validation. All violations are collected (never fail-fast) so the authoring
# agent can fix every problem in one pass.
# --------------------------------------------------------------------------- #


def _components_by_id(inventory: dict[str, Any]) -> dict[str, list[str]]:
    """Computed components keyed by their library id (``component-<n>``)."""
    components, _ = build_components(inventory)
    return {f"component-{index}": members for index, members in enumerate(components, start=1)}


def validate_plan(raw: Any, inventory: dict[str, Any]) -> list[str]:
    """Validate an authored conversion plan against the inventory's connected components.

    Returns a list of human-readable violation strings; an empty list means the plan is valid. Never
    raises on a malformed payload. The plan may be the authored fields alone or an edited copy of the
    recorded ``conversion_plan.json``: library-owned keys are accepted and ignored (they are recomputed
    on record), and any other unknown key is refused. The rules:

    * ``decision`` is one of the known routes, or ``None`` / absent while still pending, and
      ``rationale`` (when present) is a non-empty string;
    * every member must be a real inventory pipeline, and a component's ``members`` must exactly match
      one computed connected component -- so a decision can never split a component or span two;
    * the plan is a **bijection** over components: every component is listed exactly once (no
      partial plan, no duplicate/conflicting decisions);
    * an ``agentic`` decision is only accepted for an ADF inventory (:func:`agentic_routing_supported`);
    * ``suggested_groupings`` entries name a grouping route suggests for this inventory, ``accepted``
      is a boolean, and an accepted grouping has none of its components decided ``deterministic``;
    * ``conversation`` is a list of ``{"question", "answer"}`` objects with non-empty strings.
    """
    if not isinstance(raw, dict):
        return [f"conversion plan must be a JSON object, got {type(raw).__name__}"]

    violations: list[str] = []
    for key in sorted(set(raw) - _PLAN_AUTHORED_KEYS - _PLAN_LIBRARY_KEYS):
        violations.append(f"unknown top-level key: {key!r}")

    components = raw.get("components")
    if not isinstance(components, list):
        violations.append("'components' must be a list")
        return violations

    names = _pipeline_names(inventory)
    computed_by_id = _components_by_id(inventory)
    computed_by_members = {frozenset(members): component_id for component_id, members in computed_by_id.items()}

    decided_ids: list[str] = []
    decision_by_id: dict[str, Any] = {}
    for index, component in enumerate(components):
        loc = f"components[{index}]"
        matched_id = _validate_component_entry(component, loc, names, computed_by_members, violations)
        if matched_id is not None:
            decided_ids.append(matched_id)
            decision_by_id[matched_id] = component.get("decision")

    if not agentic_routing_supported(inventory):
        for index, component in enumerate(components):
            if isinstance(component, dict) and component.get("decision") == DECISION_AGENTIC:
                violations.append(f"components[{index}]: {_agentic_routing_unsupported_note(inventory)}")

    for component_id, count in Counter(decided_ids).items():
        if count > 1:
            violations.append(
                f"component {component_id!r} is decided {count} times; each component needs exactly one decision"
            )
    decided = set(decided_ids)
    for component_id, members in computed_by_id.items():
        if component_id not in decided:
            violations.append(f"component {component_id!r} ({members}) has no decision; every component must be routed")

    violations.extend(_grouping_violations(raw.get("suggested_groupings", []), inventory, decision_by_id))
    violations.extend(_conversation_violations(raw.get("conversation", [])))
    return violations


def _validate_component_entry(
    component: Any,
    loc: str,
    names: set[str],
    computed_by_members: dict[frozenset[str], str],
    violations: list[str],
) -> str | None:
    """Validate one authored component entry, appending problems; return the matched component id.

    Returns the computed ``component-<n>`` id this entry decides when its ``members`` exactly match a
    connected component (so the caller can enforce the bijection), else ``None``.
    """
    if not isinstance(component, dict):
        violations.append(f"{loc} must be an object")
        return None

    for key in sorted(set(component) - _COMPONENT_AUTHORED_KEYS - _COMPONENT_LIBRARY_KEYS):
        violations.append(f"{loc}: unknown field {key!r}")

    decision = component.get("decision")
    if decision is not None and decision not in DECISIONS:
        allowed = ", ".join(repr(value) for value in DECISIONS)
        violations.append(f"{loc}: 'decision' must be one of {{{allowed}}}, or null while pending, got {decision!r}")

    rationale = component.get("rationale")
    if rationale is not None and (not isinstance(rationale, str) or not rationale.strip()):
        violations.append(f"{loc}: 'rationale' must be a non-empty string when present")

    if component.get("assignments"):
        violations.append(
            f"{loc}: 'assignments' (per-node or subgraph routing) is reserved for Phase 2; "
            "decide the whole component and leave it empty"
        )

    component_id = component.get("component_id")
    if not isinstance(component_id, str) or not component_id:
        violations.append(f"{loc}: 'component_id' must be a non-empty string")

    members = component.get("members")
    if not isinstance(members, list) or not all(isinstance(member, str) for member in members):
        violations.append(f"{loc}: 'members' must be a list of pipeline names")
        return None

    for member in members:
        if member not in names:
            violations.append(f"{loc}: pipeline {member!r} not in inventory")

    # Reject duplicate members explicitly: a frozenset match would collapse ["a", "a"] to {"a"} and
    # wrongly accept it as the component {"a"}, breaking the members-match / bijection contract.
    duplicates = sorted({member for member in members if members.count(member) > 1})
    if duplicates:
        violations.append(f"{loc}: duplicate members {duplicates}; list each pipeline once")
        return None

    matched_id = computed_by_members.get(frozenset(members))
    if matched_id is None:
        violations.append(
            f"{loc}: members {sorted(members)} do not form a connected component "
            f"(they split or span computed components); route each component as a whole, "
            "or accept a suggested grouping to convert several together"
        )
        return None
    if isinstance(component_id, str) and component_id and component_id != matched_id:
        violations.append(
            f"{loc}: component_id {component_id!r} does not match the component for these members "
            f"(expected {matched_id!r})"
        )
    return matched_id


def _grouping_violations(groupings: Any, inventory: dict[str, Any], decision_by_id: dict[str, Any]) -> list[str]:
    """Check the authored ``suggested_groupings`` entries against the groupings route suggests."""
    if not isinstance(groupings, list):
        return ["'suggested_groupings' must be a list"]
    suggestions = {grouping["grouping_id"]: grouping for grouping in suggest_groupings(inventory)}
    violations: list[str] = []
    seen: set[str] = set()
    for index, entry in enumerate(groupings):
        loc = f"suggested_groupings[{index}]"
        if not isinstance(entry, dict):
            violations.append(f"{loc} must be an object")
            continue
        for key in sorted(set(entry) - _GROUPING_AUTHORED_KEYS - _GROUPING_LIBRARY_KEYS):
            violations.append(f"{loc}: unknown field {key!r}")
        grouping_id = entry.get("grouping_id")
        suggestion = suggestions.get(grouping_id) if isinstance(grouping_id, str) else None
        if suggestion is None:
            violations.append(
                f"{loc}: {grouping_id!r} is not a grouping route suggests for this inventory; only a suggested "
                "grouping can be accepted (to group other components, record an inferred relationship through enrich)"
            )
            continue
        if grouping_id in seen:
            violations.append(f"{loc}: grouping {grouping_id!r} is listed more than once")
        seen.add(str(grouping_id))
        accepted = entry.get("accepted", False)
        if not isinstance(accepted, bool):
            violations.append(f"{loc}: 'accepted' must be true or false, got {accepted!r}")
        elif accepted:
            deterministic = [
                component_id
                for component_id in suggestion["components"]
                if decision_by_id.get(component_id) == DECISION_DETERMINISTIC
            ]
            if deterministic:
                violations.append(
                    f"{loc}: grouping {grouping_id!r} converts {suggestion['components']} together agentically, but "
                    f"{deterministic} {'is' if len(deterministic) == 1 else 'are'} decided 'deterministic'; decide "
                    "each of its components 'agentic', or set accepted to false"
                )
    return violations


def _conversation_violations(conversation: Any) -> list[str]:
    """Check the authored routing ``conversation``: a list of non-empty ``{question, answer}`` pairs."""
    if not isinstance(conversation, list):
        return ["'conversation' must be a list of {question, answer} objects"]
    violations: list[str] = []
    for index, entry in enumerate(conversation):
        loc = f"conversation[{index}]"
        if not isinstance(entry, dict):
            violations.append(f"{loc} must be an object")
            continue
        for key in sorted(set(entry) - _CONVERSATION_KEYS):
            violations.append(f"{loc}: unknown field {key!r}")
        for key in sorted(_CONVERSATION_KEYS):
            value = entry.get(key)
            if not isinstance(value, str) or not value.strip():
                violations.append(f"{loc}: {key!r} must be a non-empty string")
    return violations


# --------------------------------------------------------------------------- #
# Loading, recording, and the atomic idempotent write.
# --------------------------------------------------------------------------- #


def load_plan(*, plan: dict[str, Any] | None = None, plan_path: Path | None = None) -> dict[str, Any]:
    """Return the raw authored plan dict from exactly one source (inline or file).

    Raises:
        ValueError: if neither or both sources are provided.
    """
    if (plan is None) == (plan_path is None):
        raise ValueError("provide exactly one of 'plan' (inline dict) or 'plan_path'")
    if plan is not None:
        return plan
    assert plan_path is not None  # guaranteed by the guard above
    return json.loads(Path(plan_path).read_text(encoding="utf-8"))


def carried_forward_plan(previous: Any, inventory: dict[str, Any]) -> dict[str, Any]:
    """Build the authored plan route records when no plan is supplied: the recommendation, kept decisions.

    This is how route writes its recommendation straight into ``conversion_plan.json`` and then picks
    up the agent's edits to it. Every current component starts pending (``decision: None``); a
    component the *previous* recorded plan lists with the same members keeps its ``decision``,
    ``rationale`` and ``assignments`` as written there, a suggested grouping with the same id and
    members keeps its ``accepted`` flag, and the ``conversation`` is kept. Entries whose components no
    longer exist are dropped, so a re-discover that regroups pipelines leaves those components
    pending. Values are carried as written; :func:`validate_plan` then checks them.

    Args:
        previous: The parsed ``conversion_plan.json`` (edited or not), or ``None`` when there is none.
        inventory: The current discover ``inventory.json`` document.
    """
    recommendation = build_recommendation(inventory)
    previous = previous if isinstance(previous, dict) else {}
    previous_components = previous.get("components") if isinstance(previous.get("components"), list) else []
    by_members: dict[frozenset[str], dict[str, Any]] = {}
    for entry in previous_components:
        members = entry.get("members") if isinstance(entry, dict) else None
        if isinstance(members, list) and all(isinstance(member, str) for member in members):
            by_members[frozenset(members)] = entry

    components: list[dict[str, Any]] = []
    for component in recommendation["components"]:
        authored: dict[str, Any] = {
            "component_id": component["component_id"],
            "members": component["members"],
            "decision": None,
        }
        kept = by_members.get(frozenset(component["members"]))
        if kept is not None:
            for key in sorted(_COMPONENT_AUTHORED_KEYS - {"component_id", "members"}):
                if key in kept:
                    authored[key] = kept[key]
        components.append(authored)

    previous_groupings = previous.get("suggested_groupings")
    accepted_before: set[tuple[Any, tuple[str, ...]]] = set()
    for entry in previous_groupings if isinstance(previous_groupings, list) else []:
        if isinstance(entry, dict) and entry.get("accepted") is True and isinstance(entry.get("members"), list):
            accepted_before.add((entry.get("grouping_id"), tuple(sorted(str(member) for member in entry["members"]))))
    groupings = [
        {
            "grouping_id": grouping["grouping_id"],
            "accepted": (grouping["grouping_id"], tuple(grouping["members"])) in accepted_before,
        }
        for grouping in recommendation["suggested_groupings"]
    ]
    conversation = previous.get("conversation", [])
    return {"components": components, "suggested_groupings": groupings, "conversation": conversation}


def build_plan(inventory: dict[str, Any], raw: dict[str, Any]) -> ConversionPlan:
    """Build the typed plan from a validated authored plan.

    The library recomputes ``members`` / ``recommended`` / both ``options`` and the suggested
    groupings, and overlays only the authored ``decision`` (and optional ``rationale``) per component,
    each grouping's ``accepted`` flag and the ``conversation``, so the recorded facts cannot drift from
    the inventory. It stamps the schema version and the hashes that bind the plan to what it was
    decided on: the inventory fingerprint, the saved source graphs the inventory records, and the saved
    agentic insights when enrich ran. Does not mutate the inputs and performs no I/O.
    """
    recommendation = build_recommendation(inventory)
    computed_by_id = _components_by_id(inventory)
    members_to_id = {frozenset(members): component_id for component_id, members in computed_by_id.items()}

    authored_by_id: dict[str, dict[str, Any]] = {}
    for component in raw.get("components", []):
        component_id = members_to_id[frozenset(component["members"])]
        authored_by_id[component_id] = component

    components = [
        ComponentPlan(
            component_id=entry["component_id"],
            members=entry["members"],
            recommended=entry["recommended"],
            decision=authored_by_id[entry["component_id"]].get("decision"),
            options=entry["options"],
            rationale=authored_by_id[entry["component_id"]].get("rationale"),
        )
        for entry in recommendation["components"]
    ]
    accepted_ids = {
        entry.get("grouping_id")
        for entry in raw.get("suggested_groupings") or []
        if isinstance(entry, dict) and entry.get("accepted") is True
    }
    groupings = [
        SuggestedGrouping(
            grouping_id=grouping["grouping_id"],
            components=grouping["components"],
            members=grouping["members"],
            basis=grouping["basis"],
            accepted=grouping["grouping_id"] in accepted_ids,
        )
        for grouping in recommendation["suggested_groupings"]
    ]
    insights = inventory.get(INSIGHTS_KEY)
    return ConversionPlan(
        schema_version=SCHEMA_VERSION,
        inventory_sha256=inventory_fingerprint(inventory),
        source_graphs_sha256=inventory.get("source_graphs_sha256"),
        agentic_insights_sha256=insights.get("agentic_insights_sha256") if isinstance(insights, dict) else None,
        components=components,
        suggested_groupings=groupings,
        conversation=[
            ConversationEntry(question=entry["question"], answer=entry["answer"])
            for entry in raw.get("conversation") or []
        ],
        findings=recommendation["findings"],
    )


def plan_binding_violations(plan: ConversionPlan, inventory: dict[str, Any]) -> list[str]:
    """Check a recorded plan still matches the inventory, source graphs and insights it was decided on.

    Returns one message per mismatch; an empty list means the plan is current. A plan is stale when
    discover or enrich ran again after it was recorded.
    """
    violations: list[str] = []
    current = inventory_fingerprint(inventory)
    if plan.inventory_sha256 != current:
        violations.append(
            f"{PLAN_FILENAME} was recorded against inventory {plan.inventory_sha256!r} but the current "
            f"inventory is {current!r}; re-run route against the current inventory"
        )
    if plan.source_graphs_sha256 != inventory.get("source_graphs_sha256"):
        violations.append(f"{PLAN_FILENAME} was recorded against different source graphs; re-run route")
    insights = inventory.get(INSIGHTS_KEY)
    current_insights = insights.get("agentic_insights_sha256") if isinstance(insights, dict) else None
    if plan.agentic_insights_sha256 != current_insights:
        violations.append(f"{PLAN_FILENAME} was recorded against different agentic insights; re-run route")
    return violations


def recorded_plan_violations(plan: ConversionPlan, inventory: dict[str, Any]) -> list[str]:
    """Check a recorded plan is still a complete, valid decision for the inventory's components.

    Runs the same rules :func:`validate_plan` applies on record, so a recorded file that lost a
    component, gained one, or holds an invalid grouping is refused rather than packaged as if every
    component had been decided.
    """
    return validate_plan(plan.to_dict(), inventory)


def insights_file_violations(output_dir: Path, inventory: dict[str, Any] | None) -> list[str]:
    """Check ``agentic_insights.json`` and the inventory's ``insights`` block still agree.

    Enrich writes ``metadata/agentic_insights.json`` and then ``inventory.json``; a run stopped between
    the two, or a hand edit, leaves them out of step. This refuses when the inventory carries insights
    but the file is missing, the file exists but the inventory's block differs from it (or there is no
    inventory to compare with), or the file no longer matches its own ``agentic_insights_sha256``.
    Returns one message saying how to recover; an empty list when they agree or enrich never ran.
    """
    path = Path(output_dir) / "metadata" / AGENTIC_INSIGHTS_FILENAME
    inventory_insights = inventory.get(INSIGHTS_KEY) if isinstance(inventory, dict) else None
    if not path.exists() and inventory_insights is None:
        return []
    problem: str | None = None
    if not path.exists():
        problem = f"inventory.json carries insights but metadata/{AGENTIC_INSIGHTS_FILENAME} is missing"
    else:
        try:
            document = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as error:
            document = None
            problem = f"metadata/{AGENTIC_INSIGHTS_FILENAME} is unreadable ({error})"
        if problem is None and not isinstance(document, dict):
            problem = f"metadata/{AGENTIC_INSIGHTS_FILENAME} must contain a JSON object"
        elif problem is None and isinstance(document, dict) and agentic_insights_hash_violations(document):
            problem = f"metadata/{AGENTIC_INSIGHTS_FILENAME} does not match its recorded agentic_insights_sha256"
        elif problem is None and inventory is None:
            problem = f"metadata/{AGENTIC_INSIGHTS_FILENAME} is present but metadata/inventory.json is missing"
        elif problem is None and inventory_insights != document:
            problem = f"metadata/{AGENTIC_INSIGHTS_FILENAME} and metadata/inventory.json disagree"
    if problem is None:
        return []
    return [f"{problem}; {_INSIGHTS_RECOVERY}"]


def record_plan(
    output_dir: Path,
    *,
    plan: dict[str, Any] | None = None,
    plan_path: Path | None = None,
) -> dict[str, Any]:
    """Validate an authored plan against the inventory, then record it on success.

    Reads ``<output_dir>/metadata/inventory.json``, validates the authored plan, and -- only when
    there are no violations -- writes ``<output_dir>/metadata/conversion_plan.json`` atomically. The
    inventory file is never touched. Provide the authored plan via exactly one of ``plan`` (inline
    dict) or ``plan_path`` (a JSON file). A plan may leave decisions pending; it is recorded, and
    route applies it once nothing is pending.

    Returns ``{"ok", "violations", "inventory_sha256", "components", "pending", "findings"}``. ``ok``
    is ``False`` (and no plan written) when there are violations.

    Raises:
        FileNotFoundError: when ``inventory.json`` does not exist (run discover first).
        ValueError: when neither or both plan sources are provided, or the inventory is not a JSON
            object.
    """
    inventory_path = Path(output_dir) / "metadata" / "inventory.json"
    if not inventory_path.exists():
        raise FileNotFoundError(f"No inventory.json under {inventory_path.parent}; run the discover phase first.")
    inventory = json.loads(inventory_path.read_text(encoding="utf-8"))
    if not isinstance(inventory, dict):
        raise ValueError(f"inventory.json must contain a JSON object, got {type(inventory).__name__}")

    raw = load_plan(plan=plan, plan_path=plan_path)
    violations = validate_plan(raw, inventory)
    if violations:
        return {"ok": False, "violations": violations, "components": 0}

    recorded = build_plan(inventory, raw)
    recorded.write(Path(output_dir))
    return {
        "ok": True,
        "violations": [],
        "inventory_sha256": recorded.inventory_sha256,
        "source_graphs_sha256": recorded.source_graphs_sha256,
        "agentic_insights_sha256": recorded.agentic_insights_sha256,
        "components": len(recorded.components),
        "pending": recorded.pending_components(),
        "accepted_groupings": [grouping.grouping_id for grouping in recorded.suggested_groupings if grouping.accepted],
        "findings": len(recorded.findings),
    }
