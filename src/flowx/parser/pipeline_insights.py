"""Validate and merge agent-authored insights into the discover inventory.

The discover phase writes a pure ``metadata/inventory.json`` (pipelines, summary,
lineage). The agent then *authors* an ``insights`` object -- its judgment about
pipeline intent, Databricks patterns, and cross-pipeline relationships. This
module *enriches* the inventory: it validates the authored JSON against the
inventory and, only when clean, appends the single ``insights`` key while
re-serialising the rest byte-identically.

A relationship's ``lineage_edge`` comes in two tiers, validated differently:

* ``control`` / ``data`` -- an **annotation** of a deterministic edge. Its
  ``edge_identity`` must resolve to a real edge in the inventory's ``lineage``
  (a ``ControlEdge.activity_name`` or ``DataEdge.match_key``); ``evidence`` /
  ``confidence`` must be absent.
* ``inferred`` -- an agent-asserted coupling the deterministic layer never
  found (e.g. data flow inside notebook code). There is nothing to resolve
  against, so instead the edge must carry a non-empty ``evidence`` string and a
  ``confidence`` of ``high`` / ``medium`` / ``low``. Endpoints are still real
  pipeline names. This keeps every edge accountable -- annotations to a proven
  fact, inferences to stated evidence -- without letting an inference
  masquerade as proven lineage.

There is no LLM here -- the tool only validates and merges.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

_INSIGHTS_TOP_KEYS = {"overview", "system_recommendation", "pipeline_insights", "pipeline_relationships"}
_INSIGHT_KEYS = {
    "pipeline",
    "pattern_name",
    "intent",
    "databricks_pattern",
    "recommended_patterns",
    "conversion_notes",
    "risk_if_ignored",
}
_RECOMMENDED_PATTERN_KEYS = {"pattern", "fit", "simplification_pattern"}
_MAX_RECOMMENDED_PATTERNS = 4
_SYSTEM_RECOMMENDATION_KEYS = {"headline", "recommended_patterns", "cascade", "decision_driver"}
_RELATIONSHIP_KEYS = {
    "from_pipeline",
    "to_pipeline",
    "lineage_edge",
    "relationship_summary",
    "databricks_pattern",
    "risk_if_ignored",
}
_EDGE_KEYS = {"edge_type", "edge_identity", "evidence", "confidence"}
_CONFIDENCE_LEVELS = {"high", "medium", "low"}


def _pipeline_names(inventory: dict) -> set[str]:
    return {str(p["name"]) for p in inventory.get("pipelines", []) if isinstance(p, dict) and p.get("name") is not None}


def _control_edge_triples(inventory: dict) -> set[tuple[str, str, str]]:
    """Real control edges as ``(caller_pipeline, callee_pipeline, activity_name)``.

    Resolving on the full triple (not the bare ``activity_name``) is what pins a
    relationship to a *specific* edge: ADF names the ExecutePipeline activity
    after the callee, so a single ``activity_name`` is shared by every caller of
    that callee -- a global-set check would accept a relationship whose
    ``from``/``to`` point at the wrong pair.
    """
    lineage = inventory.get("lineage") or {}
    return {
        (str(e["caller_pipeline"]), str(e["callee_pipeline"]), str(e["activity_name"]))
        for e in lineage.get("control_edges", [])
        if isinstance(e, dict)
        and e.get("caller_pipeline") is not None
        and e.get("callee_pipeline") is not None
        and e.get("activity_name") is not None
    }


def _data_edge_triples(inventory: dict) -> set[tuple[str, str, str]]:
    """Real data edges as ``(producer_pipeline, consumer_pipeline, match_key)``.

    Same rationale as control edges: a ``match_key`` (a shared table/path) can be
    produced and consumed across many pipeline pairs, so the producer/consumer
    endpoints must match too. A relationship's ``from``/``to`` map to
    producer/consumer respectively.
    """
    lineage = inventory.get("lineage") or {}
    return {
        (str(e["producer_pipeline"]), str(e["consumer_pipeline"]), str(e["match_key"]))
        for e in lineage.get("data_edges", [])
        if isinstance(e, dict)
        and e.get("producer_pipeline") is not None
        and e.get("consumer_pipeline") is not None
        and e.get("match_key") is not None
    }


def validate_insights(raw: dict, inventory: dict) -> list[str]:
    """Validate an authored insights dict against the inventory.

    Returns a list of human-readable violation strings; an empty list means the
    insights are valid. All violations are collected (never fail-fast) so the
    agent can fix every problem in one pass.
    """
    violations: list[str] = []
    if not isinstance(raw, dict):
        return [f"insights must be a JSON object, got {type(raw).__name__}"]

    for key in set(raw) - _INSIGHTS_TOP_KEYS:
        violations.append(f"unknown top-level key: {key!r}")

    if "system_recommendation" in raw:
        violations.extend(_validate_system_recommendation(raw["system_recommendation"]))

    names = _pipeline_names(inventory)
    control_triples = _control_edge_triples(inventory)
    data_triples = _data_edge_triples(inventory)

    insights = raw.get("pipeline_insights", [])
    if not isinstance(insights, list):
        violations.append("'pipeline_insights' must be a list")
        insights = []
    for i, item in enumerate(insights):
        loc = f"pipeline_insights[{i}]"
        if not isinstance(item, dict):
            violations.append(f"{loc} must be an object")
            continue
        for key in set(item) - _INSIGHT_KEYS:
            violations.append(f"{loc}: unknown field {key!r}")
        name = item.get("pipeline")
        if not name:
            violations.append(f"{loc}: missing required field 'pipeline'")
        elif name not in names:
            violations.append(f"{loc}: pipeline {name!r} not in inventory")
        if "recommended_patterns" in item:
            violations.extend(_validate_recommended_patterns(item["recommended_patterns"], loc))

    relationships = raw.get("pipeline_relationships", [])
    if not isinstance(relationships, list):
        violations.append("'pipeline_relationships' must be a list")
        relationships = []
    for i, rel in enumerate(relationships):
        loc = f"pipeline_relationships[{i}]"
        if not isinstance(rel, dict):
            violations.append(f"{loc} must be an object")
            continue
        for key in set(rel) - _RELATIONSHIP_KEYS:
            violations.append(f"{loc}: unknown field {key!r}")
        from_pipeline = rel.get("from_pipeline")
        to_pipeline = rel.get("to_pipeline")
        for endpoint, value in (("from_pipeline", from_pipeline), ("to_pipeline", to_pipeline)):
            if not value:
                violations.append(f"{loc}: missing required field {endpoint!r}")
            elif value not in names:
                violations.append(f"{loc}: {endpoint} {value!r} not in inventory")
        violations.extend(
            _validate_edge(rel.get("lineage_edge"), loc, from_pipeline, to_pipeline, control_triples, data_triples)
        )

    return violations


def _validate_edge(
    edge: Any,
    loc: str,
    from_pipeline: Any,
    to_pipeline: Any,
    control_triples: set[tuple[str, str, str]],
    data_triples: set[tuple[str, str, str]],
) -> list[str]:
    """Validate one lineage_edge ref.

    ``control`` / ``data`` edges annotate a deterministic edge: the full
    ``(from, to, edge_identity)`` triple must resolve against the inventory's
    lineage -- so the annotation connects exactly the pipelines it claims, not
    merely some edge that happens to share the ``activity_name`` / ``match_key``
    -- and ``evidence`` / ``confidence`` must be absent. ``inferred`` edges assert
    a coupling the deterministic layer never found: nothing to resolve, but a
    non-empty ``evidence`` string and a ``confidence`` level are required instead.
    """
    if edge is None:
        return [f"{loc}: missing required field 'lineage_edge'"]
    if not isinstance(edge, dict):
        return [f"{loc}.lineage_edge must be an object"]
    problems: list[str] = []
    for key in set(edge) - _EDGE_KEYS:
        problems.append(f"{loc}.lineage_edge: unknown field {key!r}")
    edge_type = edge.get("edge_type")
    identity = edge.get("edge_identity")
    if edge_type not in ("control", "data", "inferred"):
        problems.append(f"{loc}.lineage_edge: edge_type must be 'control', 'data', or 'inferred', got {edge_type!r}")
        return problems
    if not isinstance(identity, str) or not identity:
        problems.append(f"{loc}.lineage_edge: edge_identity must be a non-empty string")
        return problems

    if edge_type == "inferred":
        problems.extend(_validate_inferred_edge(edge, loc))
        return problems

    # Annotation tier: must resolve to a real edge, and must NOT carry the
    # inferred-only evidence/confidence fields.
    for field_name in ("evidence", "confidence"):
        if edge.get(field_name) is not None:
            problems.append(f"{loc}.lineage_edge: {field_name!r} is only valid on an 'inferred' edge")
    # Resolve on the full triple. Endpoint problems are already reported above; only
    # attempt the lookup when both endpoints are strings, else it is meaningless.
    if not isinstance(from_pipeline, str) or not isinstance(to_pipeline, str):
        return problems
    valid = control_triples if edge_type == "control" else data_triples
    if (from_pipeline, to_pipeline, identity) not in valid:
        problems.append(
            f"{loc}.lineage_edge: {edge_type} edge {identity!r} does not resolve to a lineage edge "
            f"from {from_pipeline!r} to {to_pipeline!r}"
        )
    return problems


def _validate_recommended_patterns(value: Any, loc: str) -> list[str]:
    """Validate a pipeline_insight's ``recommended_patterns`` ranked list.

    When present it must hold 1-``_MAX_RECOMMENDED_PATTERNS`` objects, ordered
    best-first. Each object requires a non-empty ``pattern`` and ``fit`` string
    and a boolean ``simplification_pattern``. All problems are collected.

    Shared by both a pipeline's ``recommended_patterns`` and the top-level
    ``system_recommendation.recommended_patterns`` (``loc`` distinguishes them).
    """
    field_loc = f"{loc}.recommended_patterns"
    if not isinstance(value, list):
        return [f"{field_loc} must be a list"]
    if not value:
        return [
            f"{field_loc} must contain 1-{_MAX_RECOMMENDED_PATTERNS} patterns when present "
            f"(omit the field instead of sending an empty list)"
        ]
    problems: list[str] = []
    if len(value) > _MAX_RECOMMENDED_PATTERNS:
        problems.append(f"{field_loc} has {len(value)} patterns; at most {_MAX_RECOMMENDED_PATTERNS} are allowed")
    for j, pattern in enumerate(value):
        ploc = f"{field_loc}[{j}]"
        if not isinstance(pattern, dict):
            problems.append(f"{ploc} must be an object")
            continue
        for key in set(pattern) - _RECOMMENDED_PATTERN_KEYS:
            problems.append(f"{ploc}: unknown field {key!r}")
        for required in ("pattern", "fit"):
            text = pattern.get(required)
            if not isinstance(text, str) or not text.strip():
                problems.append(f"{ploc}: {required!r} must be a non-empty string")
        # A JSON bool parses to Python bool; reject ints/strings so 1/"yes" don't slip through.
        if not isinstance(pattern.get("simplification_pattern"), bool):
            problems.append(
                f"{ploc}: 'simplification_pattern' must be a boolean (true/false), "
                f"got {type(pattern.get('simplification_pattern')).__name__}"
            )
    return problems


def _validate_system_recommendation(value: Any) -> list[str]:
    """Validate the optional top-level ``system_recommendation`` object.

    The one whole-factory architectural decision, authored before per-pipeline
    insights. When present it must be an object with a non-empty ``headline`` and
    a ``recommended_patterns`` ranked list (validated exactly like a pipeline's --
    the whole-system branches, best-first). ``cascade`` (a list of non-empty
    strings naming what the top branch collapses) and ``decision_driver`` (the
    gating question) are optional. All problems are collected.
    """
    loc = "system_recommendation"
    if not isinstance(value, dict):
        return [f"{loc} must be an object"]
    problems: list[str] = []
    for key in set(value) - _SYSTEM_RECOMMENDATION_KEYS:
        problems.append(f"{loc}: unknown field {key!r}")
    headline = value.get("headline")
    if not isinstance(headline, str) or not headline.strip():
        problems.append(f"{loc}: 'headline' must be a non-empty string")
    if "recommended_patterns" not in value:
        problems.append(f"{loc}: missing required field 'recommended_patterns'")
    else:
        problems.extend(_validate_recommended_patterns(value["recommended_patterns"], loc))
    cascade = value.get("cascade")
    if cascade is not None and (
        not isinstance(cascade, list) or not all(isinstance(c, str) and c.strip() for c in cascade)
    ):
        problems.append(f"{loc}: 'cascade' must be a list of non-empty strings when present")
    driver = value.get("decision_driver")
    if driver is not None and (not isinstance(driver, str) or not driver.strip()):
        problems.append(f"{loc}: 'decision_driver' must be a non-empty string when present")
    return problems


def _validate_inferred_edge(edge: dict, loc: str) -> list[str]:
    """Validate the inferred-only fields: non-empty evidence + a confidence level."""
    problems: list[str] = []
    evidence = edge.get("evidence")
    if not isinstance(evidence, str) or not evidence.strip():
        problems.append(f"{loc}.lineage_edge: an 'inferred' edge requires a non-empty 'evidence' string")
    confidence = edge.get("confidence")
    if confidence not in _CONFIDENCE_LEVELS:
        problems.append(
            f"{loc}.lineage_edge: an 'inferred' edge requires 'confidence' in "
            f"{{'high', 'medium', 'low'}}, got {confidence!r}"
        )
    return problems


def load_insights(*, insights: dict | None = None, insights_path: Path | None = None) -> dict:
    """Return the raw insights dict from exactly one source (inline or file).

    Raises:
        ValueError: if neither or both sources are provided.
    """
    if (insights is None) == (insights_path is None):
        raise ValueError("provide exactly one of 'insights' (inline dict) or 'insights_path'")
    if insights is not None:
        return insights
    assert insights_path is not None  # guaranteed by the guard above
    return json.loads(insights_path.read_text(encoding="utf-8"))


def merge_into_inventory(inventory: dict, raw: dict) -> dict:
    """Return a new dict identical to *inventory* with one added ``insights`` key.

    Does not mutate the input. No I/O.
    """
    merged = dict(inventory)
    merged["insights"] = raw
    return merged


def enrich_inventory(
    output_dir: Path,
    *,
    insights: dict | None = None,
    insights_path: Path | None = None,
) -> dict:
    """Validate authored insights against the inventory, then merge on success.

    Reads ``<output_dir>/metadata/inventory.json``, validates the authored
    insights, and -- only when there are no violations -- writes the merged
    inventory back byte-identically (adding just the ``insights`` key).

    Returns ``{"ok", "violations", "pipeline_insights", "relationships"}``.
    On violations, ``ok`` is False and the file is left untouched.

    Raises:
        FileNotFoundError: when ``inventory.json`` does not exist.
    """
    inventory_path = Path(output_dir) / "metadata" / "inventory.json"
    if not inventory_path.exists():
        raise FileNotFoundError(f"No inventory.json under {inventory_path.parent}; run discover first.")
    inventory = json.loads(inventory_path.read_text(encoding="utf-8"))

    raw = load_insights(insights=insights, insights_path=insights_path)
    violations = validate_insights(raw, inventory)
    if violations:
        return {"ok": False, "violations": violations, "pipeline_insights": 0, "relationships": 0}

    merged = merge_into_inventory(inventory, raw)
    inventory_path.write_text(json.dumps(merged, indent=2), encoding="utf-8")
    return {
        "ok": True,
        "violations": [],
        "pipeline_insights": len(raw.get("pipeline_insights", [])),
        "relationships": len(raw.get("pipeline_relationships", [])),
    }
