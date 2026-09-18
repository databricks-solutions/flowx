"""Validate agent-authored insights against the discover inventory, then merge them in.

The discover phase writes a purely deterministic ``metadata/inventory.json`` (source,
pipelines, per-pipeline ``lineage``, summary). An external agent then *authors* an
``insights`` object -- its judgment about factory-wide architecture, per-pipeline intent
and recommended Databricks patterns, and cross-pipeline relationships (see
:mod:`flowx.models.insights`). This module *enriches* the inventory: it validates the
authored JSON against the real inventory and, **only when clean**, adds a single additive
``insights`` key while leaving every existing key byte-identical.

There is **no LLM here** -- the tool only validates and merges, mirroring the
author-then-validate-merge contract :mod:`flowx.agentic` uses for gap resolution. That
keeps the deterministic inventory trustworthy and every insight accountable:

* every ``pipeline`` and every relationship endpoint must be a real pipeline in the
  inventory (foreign-key validation);
* a ``control`` relationship edge must resolve to a real ``ControlEdge`` in the inventory's
  ``lineage`` -- the full ``(from, to, via_task_key)`` triple, so the annotation connects
  exactly the pipelines it claims, not merely some edge that shares a ``via_task_key``;
* an ``inferred`` edge has nothing to resolve against, so it must instead carry a non-empty
  ``evidence`` string and a ``confidence`` level.

The merge is **atomic** (temp file + ``os.replace``) and **idempotent**: it recomputes the
``inventory_sha256`` fingerprint from the deterministic inventory (with any prior ``insights``
stripped) and *replaces* the whole ``insights`` block, so re-running with the same authored
insights rewrites byte-identical bytes and never stacks. The library owns ``schema_version``
and ``inventory_sha256``; authored insights carrying either are rejected as unknown keys.
"""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

from flowx.models.insights import (
    CONFIDENCE_LEVELS,
    MAX_RECOMMENDED_PATTERNS,
    SCHEMA_VERSION,
)

# The single additive top-level key insights merge into.
INSIGHTS_KEY = "insights"

# Library-owned keys injected on merge; authored insights must not supply them.
_SCHEMA_VERSION_KEY = "schema_version"
_FINGERPRINT_KEY = "inventory_sha256"

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
_EDGE_TYPES = ("control", "inferred")


# --------------------------------------------------------------------------- #
# Inventory projections used for foreign-key + lineage-edge resolution.
# --------------------------------------------------------------------------- #


def _pipeline_names(inventory: dict[str, Any]) -> set[str]:
    """The set of real pipeline names in the inventory (the foreign-key domain)."""
    return {
        str(pipeline["name"])
        for pipeline in inventory.get("pipelines", [])
        if isinstance(pipeline, dict) and pipeline.get("name") is not None
    }


def _control_edge_triples(inventory: dict[str, Any]) -> set[tuple[str, str, str]]:
    """Real control edges as ``(source_workflow, target_workflow, via_task_key)`` triples.

    The unified inventory places lineage **per pipeline** (one block beside each pipeline's
    ``activities``), so every pipeline's ``lineage.control_edges`` are gathered into one set.
    Resolving on the full triple -- not the bare ``via_task_key`` -- pins a relationship to a
    *specific* edge: a callee is often invoked from several callers, so a ``via_task_key``
    alone could match an edge between the wrong pair.
    """
    triples: set[tuple[str, str, str]] = set()
    for pipeline in inventory.get("pipelines", []):
        if not isinstance(pipeline, dict):
            continue
        lineage = pipeline.get("lineage") or {}
        for edge in lineage.get("control_edges", []):
            if (
                isinstance(edge, dict)
                and edge.get("source_workflow") is not None
                and edge.get("target_workflow") is not None
                and edge.get("via_task_key") is not None
            ):
                triples.add((str(edge["source_workflow"]), str(edge["target_workflow"]), str(edge["via_task_key"])))
    return triples


# --------------------------------------------------------------------------- #
# Validation. All violations are collected (never fail-fast) so the authoring
# agent can fix every problem in one pass.
# --------------------------------------------------------------------------- #


def validate_insights(raw: Any, inventory: dict[str, Any]) -> list[str]:
    """Validate an authored insights payload against the inventory.

    Returns a list of human-readable violation strings; an empty list means the insights are
    valid. Never raises on a malformed payload -- a non-dict payload is reported as a violation
    so the caller can surface it the same way as every other problem.
    """
    if not isinstance(raw, dict):
        return [f"insights must be a JSON object, got {type(raw).__name__}"]

    violations: list[str] = []
    for key in sorted(set(raw) - _INSIGHTS_TOP_KEYS):
        hint = " (set by the library, not the author)" if key in (_SCHEMA_VERSION_KEY, _FINGERPRINT_KEY) else ""
        violations.append(f"unknown top-level key: {key!r}{hint}")

    overview = raw.get("overview")
    if overview is not None and (not isinstance(overview, str) or not overview.strip()):
        violations.append("'overview' must be a non-empty string when present")

    if "system_recommendation" in raw:
        violations.extend(_validate_system_recommendation(raw["system_recommendation"]))

    names = _pipeline_names(inventory)
    control_triples = _control_edge_triples(inventory)

    violations.extend(_validate_pipeline_insights(raw.get("pipeline_insights", []), names))
    violations.extend(_validate_relationships(raw.get("pipeline_relationships", []), names, control_triples))
    return violations


def _validate_pipeline_insights(insights: Any, names: set[str]) -> list[str]:
    """Validate the ``pipeline_insights`` list: shape, unknown fields, and the pipeline FK."""
    if not isinstance(insights, list):
        return ["'pipeline_insights' must be a list"]
    violations: list[str] = []
    for index, item in enumerate(insights):
        loc = f"pipeline_insights[{index}]"
        if not isinstance(item, dict):
            violations.append(f"{loc} must be an object")
            continue
        for key in sorted(set(item) - _INSIGHT_KEYS):
            violations.append(f"{loc}: unknown field {key!r}")
        name = item.get("pipeline")
        if not name:
            violations.append(f"{loc}: missing required field 'pipeline'")
        elif name not in names:
            violations.append(f"{loc}: pipeline {name!r} not in inventory")
        if "recommended_patterns" in item:
            violations.extend(_validate_recommended_patterns(item["recommended_patterns"], loc))
    return violations


def _validate_relationships(
    relationships: Any,
    names: set[str],
    control_triples: set[tuple[str, str, str]],
) -> list[str]:
    """Validate the ``pipeline_relationships`` list: endpoints (FK) + each ``lineage_edge``."""
    if not isinstance(relationships, list):
        return ["'pipeline_relationships' must be a list"]
    violations: list[str] = []
    for index, relationship in enumerate(relationships):
        loc = f"pipeline_relationships[{index}]"
        if not isinstance(relationship, dict):
            violations.append(f"{loc} must be an object")
            continue
        for key in sorted(set(relationship) - _RELATIONSHIP_KEYS):
            violations.append(f"{loc}: unknown field {key!r}")
        from_pipeline = relationship.get("from_pipeline")
        to_pipeline = relationship.get("to_pipeline")
        for endpoint, value in (("from_pipeline", from_pipeline), ("to_pipeline", to_pipeline)):
            if not value:
                violations.append(f"{loc}: missing required field {endpoint!r}")
            elif value not in names:
                violations.append(f"{loc}: {endpoint} {value!r} not in inventory")
        violations.extend(
            _validate_edge(relationship.get("lineage_edge"), loc, from_pipeline, to_pipeline, control_triples)
        )
    return violations


def _validate_edge(
    edge: Any,
    loc: str,
    from_pipeline: Any,
    to_pipeline: Any,
    control_triples: set[tuple[str, str, str]],
) -> list[str]:
    """Validate one ``lineage_edge`` reference.

    A ``control`` edge annotates a deterministic edge: the full ``(from, to, edge_identity)``
    triple must resolve against the inventory's lineage and the inferred-only ``evidence`` /
    ``confidence`` keys must be **absent entirely** (an explicit ``null`` is still a violation --
    the deterministic edge *is* the evidence). An ``inferred`` edge asserts a coupling the
    deterministic layer never found: nothing to resolve, but a non-empty ``evidence`` string
    and a ``confidence`` level are required instead.

    Every problem on the edge is collected (never fail-fast), so a single edge that is wrong in
    several ways -- e.g. an ``inferred`` edge with both an invalid identity and missing evidence
    -- surfaces all its errors in one pass, matching the rest of the validator.
    """
    if edge is None:
        return [f"{loc}: missing required field 'lineage_edge'"]
    if not isinstance(edge, dict):
        return [f"{loc}.lineage_edge must be an object"]
    problems: list[str] = []
    for key in sorted(set(edge) - _EDGE_KEYS):
        problems.append(f"{loc}.lineage_edge: unknown field {key!r}")

    edge_type = edge.get("edge_type")
    identity = edge.get("edge_identity")
    if edge_type not in _EDGE_TYPES:
        problems.append(
            f"{loc}.lineage_edge: edge_type must be 'control' or 'inferred', got {edge_type!r}. "
            "There is no 'data' edge_type: a cross-pipeline data coupling (one pipeline writes a "
            "table/file another reads) belongs on the 'inferred' tier -- set edge_type 'inferred' "
            "with an 'evidence' string and a 'confidence' level, not a 'data' edge."
        )
    if not isinstance(identity, str) or not identity:
        problems.append(f"{loc}.lineage_edge: edge_identity must be a non-empty string")

    # Tier-specific checks run independently of the type/identity checks above so every problem
    # on the edge is reported together rather than masked by an early return.
    if edge_type == "inferred":
        problems.extend(_validate_inferred_edge(edge, loc))
    elif edge_type == "control":
        # Annotation tier: the inferred-only fields must be ABSENT (key not present), not merely
        # non-null -- an explicit `evidence: null` / `confidence: null` is still a violation.
        for inferred_only in ("evidence", "confidence"):
            if inferred_only in edge:
                problems.append(f"{loc}.lineage_edge: {inferred_only!r} is only valid on an 'inferred' edge")
        # Resolve the full triple only when the identity and both endpoints are usable strings
        # (a bad identity / endpoint is already reported here or by the caller).
        if (
            isinstance(identity, str)
            and identity
            and isinstance(from_pipeline, str)
            and isinstance(to_pipeline, str)
            and (from_pipeline, to_pipeline, identity) not in control_triples
        ):
            problems.append(
                f"{loc}.lineage_edge: control edge {identity!r} does not resolve to a lineage edge "
                f"from {from_pipeline!r} to {to_pipeline!r}"
            )
    return problems


def _validate_inferred_edge(edge: dict[str, Any], loc: str) -> list[str]:
    """Validate the inferred-only fields: a non-empty ``evidence`` string + a ``confidence`` level."""
    problems: list[str] = []
    evidence = edge.get("evidence")
    if not isinstance(evidence, str) or not evidence.strip():
        problems.append(f"{loc}.lineage_edge: an 'inferred' edge requires a non-empty 'evidence' string")
    confidence = edge.get("confidence")
    if confidence not in CONFIDENCE_LEVELS:
        levels = ", ".join(repr(level) for level in CONFIDENCE_LEVELS)
        problems.append(
            f"{loc}.lineage_edge: an 'inferred' edge requires 'confidence' in {{{levels}}}, got {confidence!r}"
        )
    return problems


def _validate_recommended_patterns(value: Any, loc: str) -> list[str]:
    """Validate a ranked ``recommended_patterns`` list.

    When present it must hold 1-:data:`MAX_RECOMMENDED_PATTERNS` objects, ordered best-first.
    Each requires a non-empty ``pattern`` and ``fit`` string and a boolean
    ``simplification_pattern``. Shared by a pipeline's list and the system recommendation's
    (``loc`` distinguishes them). All problems are collected.
    """
    field_loc = f"{loc}.recommended_patterns"
    if not isinstance(value, list):
        return [f"{field_loc} must be a list"]
    if not value:
        return [
            f"{field_loc} must contain 1-{MAX_RECOMMENDED_PATTERNS} patterns when present "
            f"(omit the field instead of sending an empty list)"
        ]
    problems: list[str] = []
    if len(value) > MAX_RECOMMENDED_PATTERNS:
        problems.append(f"{field_loc} has {len(value)} patterns; at most {MAX_RECOMMENDED_PATTERNS} are allowed")
    for index, pattern in enumerate(value):
        pattern_loc = f"{field_loc}[{index}]"
        if not isinstance(pattern, dict):
            problems.append(f"{pattern_loc} must be an object")
            continue
        for key in sorted(set(pattern) - _RECOMMENDED_PATTERN_KEYS):
            problems.append(f"{pattern_loc}: unknown field {key!r}")
        for required in ("pattern", "fit"):
            text = pattern.get(required)
            if not isinstance(text, str) or not text.strip():
                problems.append(f"{pattern_loc}: {required!r} must be a non-empty string")
        # A JSON bool parses to a Python bool; reject ints/strings so 1 / "yes" don't slip through.
        if not isinstance(pattern.get("simplification_pattern"), bool):
            problems.append(
                f"{pattern_loc}: 'simplification_pattern' must be a boolean (true/false), "
                f"got {type(pattern.get('simplification_pattern')).__name__}"
            )
    return problems


def _validate_system_recommendation(value: Any) -> list[str]:
    """Validate the optional top-level ``system_recommendation`` object.

    The one whole-factory decision, authored before per-pipeline insights. When present it must
    be an object with a non-empty ``headline`` and a ``recommended_patterns`` ranked list.
    ``cascade`` (non-empty strings) and ``decision_driver`` (the gating question) are optional.
    All problems are collected.
    """
    loc = "system_recommendation"
    if not isinstance(value, dict):
        return [f"{loc} must be an object"]
    problems: list[str] = []
    for key in sorted(set(value) - _SYSTEM_RECOMMENDATION_KEYS):
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
        not isinstance(cascade, list) or not all(isinstance(item, str) and item.strip() for item in cascade)
    ):
        problems.append(f"{loc}: 'cascade' must be a list of non-empty strings when present")
    driver = value.get("decision_driver")
    if driver is not None and (not isinstance(driver, str) or not driver.strip()):
        problems.append(f"{loc}: 'decision_driver' must be a non-empty string when present")
    return problems


# --------------------------------------------------------------------------- #
# Loading, fingerprinting, and the atomic idempotent merge.
# --------------------------------------------------------------------------- #


def load_insights(*, insights: dict[str, Any] | None = None, insights_path: Path | None = None) -> dict[str, Any]:
    """Return the raw authored insights dict from exactly one source (inline or file).

    Raises:
        ValueError: if neither or both sources are provided.
    """
    if (insights is None) == (insights_path is None):
        raise ValueError("provide exactly one of 'insights' (inline dict) or 'insights_path'")
    if insights is not None:
        return insights
    assert insights_path is not None  # guaranteed by the guard above
    return json.loads(insights_path.read_text(encoding="utf-8"))


def _base_inventory(inventory: dict[str, Any]) -> dict[str, Any]:
    """The deterministic inventory with any previously-merged ``insights`` block stripped.

    Fingerprinting and re-serialisation both work off this so re-enriching an already-enriched
    inventory is stable: the fingerprint reflects only the deterministic layer, never a prior
    ``insights`` block.
    """
    return {key: value for key, value in inventory.items() if key != INSIGHTS_KEY}


def inventory_fingerprint(inventory: dict[str, Any]) -> str:
    """A stable SHA-256 over the deterministic inventory (any ``insights`` block excluded).

    Canonicalised with ``sort_keys`` so the digest is independent of key insertion order --
    it binds the authored insights to *what the inventory says*, not to a particular byte layout.
    """
    import hashlib

    canonical = json.dumps(_base_inventory(inventory), sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(canonical).hexdigest()


def merge_into_inventory(inventory: dict[str, Any], raw: dict[str, Any]) -> dict[str, Any]:
    """Return a new inventory dict with exactly one additive ``insights`` key.

    Does not mutate the input and performs no I/O. Existing keys are preserved in their original
    order and values, so re-serialising them is byte-identical to the deterministic write. The
    ``insights`` block is the authored content plus the library-owned ``schema_version`` and
    ``inventory_sha256`` fingerprint, and *replaces* any prior block (idempotent).
    """
    base = _base_inventory(inventory)
    block: dict[str, Any] = {_SCHEMA_VERSION_KEY: SCHEMA_VERSION, _FINGERPRINT_KEY: inventory_fingerprint(base)}
    for key in ("overview", "system_recommendation", "pipeline_insights", "pipeline_relationships"):
        if key in raw:
            block[key] = raw[key]
    base[INSIGHTS_KEY] = block
    return base


def _write_inventory_atomic(path: Path, inventory: dict[str, Any]) -> None:
    """Write the inventory JSON atomically, matching the deterministic write's formatting.

    Uses ``json.dumps(..., indent=2)`` with no trailing newline -- exactly how the discover
    phase writes ``inventory.json`` -- so every pre-existing key stays byte-identical. The temp
    file + ``os.replace`` keeps the on-disk inventory intact if the process dies mid-write.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.tmp")
    temporary.write_text(json.dumps(inventory, indent=2), encoding="utf-8")
    os.replace(temporary, path)


def enrich_inventory(
    output_dir: Path,
    *,
    insights: dict[str, Any] | None = None,
    insights_path: Path | None = None,
) -> dict[str, Any]:
    """Validate authored insights against the inventory, then merge them in on success.

    Reads ``<output_dir>/metadata/inventory.json``, validates the authored insights, and -- only
    when there are no violations -- writes the merged inventory back atomically (adding just the
    additive ``insights`` key, every existing key byte-unchanged). On any validation failure the
    inventory file is left untouched.

    Provide the authored insights via exactly one of ``insights`` (an inline dict) or
    ``insights_path`` (a JSON file).

    Returns a result dict ``{"ok", "violations", "inventory_sha256", "pipeline_insights",
    "relationships"}``. ``ok`` is ``False`` (and the file untouched) when there are violations.

    Raises:
        FileNotFoundError: when ``inventory.json`` does not exist (run discover first).
        ValueError: when neither or both insight sources are provided, or the inventory file is
            not a JSON object.
    """
    inventory_path = Path(output_dir) / "metadata" / "inventory.json"
    if not inventory_path.exists():
        raise FileNotFoundError(f"No inventory.json under {inventory_path.parent}; run the discover phase first.")
    inventory = json.loads(inventory_path.read_text(encoding="utf-8"))
    if not isinstance(inventory, dict):
        raise ValueError(f"inventory.json must contain a JSON object, got {type(inventory).__name__}")

    raw = load_insights(insights=insights, insights_path=insights_path)
    violations = validate_insights(raw, inventory)
    if violations:
        return {"ok": False, "violations": violations, "pipeline_insights": 0, "relationships": 0}

    merged = merge_into_inventory(inventory, raw)
    _write_inventory_atomic(inventory_path, merged)
    return {
        "ok": True,
        "violations": [],
        "inventory_sha256": merged[INSIGHTS_KEY][_FINGERPRINT_KEY],
        "pipeline_insights": len(raw.get("pipeline_insights", [])),
        "relationships": len(raw.get("pipeline_relationships", [])),
    }
