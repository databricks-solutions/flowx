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
  is engine-capable), and ``decision`` is the user's authored per-component choice.

Like :mod:`flowx.discovery_insights`, there is **no LLM here**: the tool computes the recommendation
deterministically and only *validates and records* the agent-authored decision. The agent authors
**only** the decision (and an optional rationale); the library recomputes ``members``,
``recommended``, and both options' evidence on record so they can never drift from the inventory or
be faked.

The recorded plan is bound to the inventory via :func:`flowx.discovery_insights.inventory_fingerprint`
-- a SHA-256 over the deterministic inventory base (the ``insights`` block excluded). That base is
exactly the structural signal that decides component membership and engine capability (pipelines,
control lineage, per-activity strategy, motifs); insights are advisory evidence for the agentic
option, not part of the binding. The write is atomic (temp file + ``os.replace``) and idempotent, so
re-recording the same decision against the same inventory rewrites byte-identical bytes.

This is additive, opt-in routing metadata only: with no recorded plan, ``convert`` and ``package``
behave exactly as today. Component computation reads ``lineage.control_edges``, which both ADF
(``ExecutePipeline``) and Airflow (``RunJob``) emit, so the artifact is source-neutral.
"""

from __future__ import annotations

import json
import os
from collections import Counter
from collections.abc import Iterable
from pathlib import Path
from typing import Any

from flowx.discovery_insights import inventory_fingerprint
from flowx.models.conversion_plan import DECISIONS, SCHEMA_VERSION

# The strategy value that marks an activity as individually engine-capable (a 1:1 deterministic
# translation). Any other value (``"agentic"`` / ``"unsupported"`` / missing) is a gap unless the
# activity is claimed by a detected motif -- the multi-activity capability signal from #64.
_DETERMINISTIC_STRATEGY = "deterministic"

# The recorded conversion-plan artifact lives beside inventory.json under metadata/.
PLAN_FILENAME = "conversion_plan.json"

# Authored top-level keys (everything else the library owns and rejects on input).
_PLAN_TOP_KEYS = {"components"}
_LIBRARY_TOP_KEYS = {"schema_version", "inventory_sha256", "findings"}
# Authored per-component keys vs the fields the library recomputes and rejects on input.
_COMPONENT_AUTHORED_KEYS = {"component_id", "members", "decision", "rationale"}
_COMPONENT_LIBRARY_KEYS = {"recommended", "options"}


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


def _agentic_option(members: list[str], inventory: dict[str, Any]) -> dict[str, Any]:
    """Surface the agent-authored recommended patterns for a component as a first-class option.

    Draws every ``recommended_patterns`` entry from the member pipelines' insights, tagging each with
    its pipeline, and flags whether any is a ``simplification_pattern`` (a distinctive re-architecture
    such as a multi-pipeline -> Lakeflow Connect collapse) so the user sees it prominently.
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
    return {"recommended_patterns": recommended_patterns, "has_simplification": has_simplification}


def recommend_component(members: list[str], inventory: dict[str, Any]) -> tuple[str, dict[str, Any]]:
    """Compute the recommended route and both conversion options for one component.

    Args:
        members: The component's pipeline names.
        inventory: The discover ``inventory.json`` document (deterministic or enriched).

    Returns:
        ``(recommended, options)`` where ``recommended`` is ``"deterministic"`` when the whole
        component is engine-capable, else ``"agentic"``; and ``options`` carries the ``deterministic``
        and ``agentic`` peers as first-class entries.
    """
    deterministic = _deterministic_option(members, inventory)
    agentic = _agentic_option(members, inventory)
    recommended = _DETERMINISTIC_STRATEGY if deterministic["capable"] else "agentic"
    return recommended, {"deterministic": deterministic, "agentic": agentic}


def build_recommendation(inventory: dict[str, Any]) -> dict[str, Any]:
    """Compute the full routing recommendation over every component in the inventory.

    Returns a dict with ``components`` (each carrying ``component_id``, sorted ``members``,
    ``recommended`` and both ``options``), the ``findings`` from component computation, and a
    ``default_plan`` that proposes ``decision == recommended`` for every component -- ready to hand
    straight to :func:`record_plan` when the user accepts the recommendations wholesale, or to edit
    per component for overrides.
    """
    components, findings = build_components(inventory)
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
    raises on a malformed payload. The rules:

    * only the authored top-level key ``components`` is allowed (library-owned keys are rejected with
      a hint), and each component entry may carry only the authored fields (``recommended`` /
      ``options`` are library-computed and rejected on input);
    * ``decision`` must be one of the known routes and ``rationale`` (when present) a non-empty string;
    * every member must be a real inventory pipeline, and a component's ``members`` must exactly match
      one computed connected component -- so a decision can never split a component or span two;
    * the plan is a **bijection** over components: every component is decided exactly once (no
      partial plan, no duplicate/conflicting decisions).
    """
    if not isinstance(raw, dict):
        return [f"conversion plan must be a JSON object, got {type(raw).__name__}"]

    violations: list[str] = []
    for key in sorted(set(raw) - _PLAN_TOP_KEYS):
        hint = " (set by the library, not the author)" if key in _LIBRARY_TOP_KEYS else ""
        violations.append(f"unknown top-level key: {key!r}{hint}")

    components = raw.get("components")
    if not isinstance(components, list):
        violations.append("'components' must be a list")
        return violations

    names = _pipeline_names(inventory)
    computed_by_id = _components_by_id(inventory)
    computed_by_members = {frozenset(members): component_id for component_id, members in computed_by_id.items()}

    decided_ids: list[str] = []
    for index, component in enumerate(components):
        loc = f"components[{index}]"
        matched_id = _validate_component_entry(component, loc, names, computed_by_members, violations)
        if matched_id is not None:
            decided_ids.append(matched_id)

    for component_id, count in Counter(decided_ids).items():
        if count > 1:
            violations.append(
                f"component {component_id!r} is decided {count} times; each component needs exactly one decision"
            )
    decided = set(decided_ids)
    for component_id, members in computed_by_id.items():
        if component_id not in decided:
            violations.append(f"component {component_id!r} ({members}) has no decision; every component must be routed")
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

    for key in sorted(set(component) - _COMPONENT_AUTHORED_KEYS):
        hint = " (set by the library, not the author)" if key in _COMPONENT_LIBRARY_KEYS else ""
        violations.append(f"{loc}: unknown field {key!r}{hint}")

    decision = component.get("decision")
    if decision not in DECISIONS:
        allowed = ", ".join(repr(value) for value in DECISIONS)
        violations.append(f"{loc}: 'decision' must be one of {{{allowed}}}, got {decision!r}")

    rationale = component.get("rationale")
    if rationale is not None and (not isinstance(rationale, str) or not rationale.strip()):
        violations.append(f"{loc}: 'rationale' must be a non-empty string when present")

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
            f"(they split or span computed components); route each component as a whole"
        )
        return None
    if isinstance(component_id, str) and component_id and component_id != matched_id:
        violations.append(
            f"{loc}: component_id {component_id!r} does not match the component for these members "
            f"(expected {matched_id!r})"
        )
    return matched_id


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


def build_plan_document(inventory: dict[str, Any], raw: dict[str, Any]) -> dict[str, Any]:
    """Build the recorded plan document from a validated authored plan.

    The library recomputes ``members`` / ``recommended`` / both ``options`` and overlays only the
    authored ``decision`` (and optional ``rationale``) per component, so the recorded facts cannot
    drift from the inventory. Stamps the library-owned ``schema_version`` and ``inventory_sha256``.
    Does not mutate the inputs and performs no I/O.
    """
    recommendation = build_recommendation(inventory)
    computed_by_id = _components_by_id(inventory)
    members_to_id = {frozenset(members): component_id for component_id, members in computed_by_id.items()}

    authored_by_id: dict[str, dict[str, Any]] = {}
    for component in raw.get("components", []):
        component_id = members_to_id[frozenset(component["members"])]
        authored_by_id[component_id] = component

    components_out: list[dict[str, Any]] = []
    for entry in recommendation["components"]:
        component_id = entry["component_id"]
        authored = authored_by_id[component_id]
        recorded: dict[str, Any] = {
            "component_id": component_id,
            "members": entry["members"],
            "recommended": entry["recommended"],
            "decision": authored["decision"],
            "options": entry["options"],
        }
        rationale = authored.get("rationale")
        if rationale is not None:
            recorded["rationale"] = rationale
        components_out.append(recorded)

    return {
        "schema_version": SCHEMA_VERSION,
        "inventory_sha256": inventory_fingerprint(inventory),
        "components": components_out,
        "findings": recommendation["findings"],
    }


def _write_plan_atomic(path: Path, document: dict[str, Any]) -> None:
    """Write the plan JSON atomically (temp file + ``os.replace``), matching the inventory formatting."""
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.tmp")
    temporary.write_text(json.dumps(document, indent=2), encoding="utf-8")
    os.replace(temporary, path)


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
    dict) or ``plan_path`` (a JSON file).

    Returns ``{"ok", "violations", "inventory_sha256", "components", "findings"}``. ``ok`` is
    ``False`` (and no plan written) when there are violations.

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

    document = build_plan_document(inventory, raw)
    _write_plan_atomic(inventory_path.with_name(PLAN_FILENAME), document)
    return {
        "ok": True,
        "violations": [],
        "inventory_sha256": document["inventory_sha256"],
        "components": len(document["components"]),
        "findings": len(document["findings"]),
    }
