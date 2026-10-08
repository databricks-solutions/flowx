"""Validate agent-authored insights against the discover inventory, then record them.

The discover phase writes a purely deterministic ``metadata/inventory.json`` (source,
pipelines, per-pipeline ``lineage``, summary). An external agent then *authors* an
``insights`` object -- its judgment about factory-wide architecture, per-pipeline intent
and recommended Databricks patterns, and cross-pipeline relationships (see
:mod:`flowx.models.insights`). This module *enriches* the inventory: it validates the
authored JSON against the real inventory and, **only when clean**, records it in
``metadata/agentic_insights.json`` and rebuilds ``inventory.json`` from the saved source graphs
plus that document -- it never patches the inventory in place.

There is **no LLM here** -- the tool only validates and records, mirroring the
author-then-validate-record contract :mod:`flowx.agentic` uses for gap resolution. That
keeps the deterministic inventory trustworthy and every insight accountable:

* every ``pipeline`` and every relationship endpoint must be a real pipeline in the
  inventory (foreign-key validation);
* a ``control`` relationship edge must resolve to a real ``ControlEdge`` in the inventory's
  ``lineage`` -- the full ``(from, to, via_task_key)`` triple, so the annotation connects
  exactly the pipelines it claims, not merely some edge that shares a ``via_task_key``;
* an ``inferred`` edge has nothing to resolve against, so it must instead carry a non-empty
  ``evidence`` string and a ``confidence`` level.

Validated insights are written to their own file, ``metadata/agentic_insights.json``, which
records the persisted ``source_graphs.json`` hash it was checked against
(``source_graphs_sha256``) and carries its own content hash (``agentic_insights_sha256``).
``inventory.json`` is then rebuilt by the library alone: when the inventory records a
``source_graphs_sha256``, its deterministic part is projected again from ``source_graphs.json``
(byte-identical to what discover writes), otherwise discover's deterministic part is reused; that
same block is added under ``insights``, so the inventory is always built from those two pieces
rather than patched. Only one enrich writes an output directory at a time, and the two files are
replaced back to back (see ``_write_both_or_neither``). A process killed or interrupted between
those two replaces, or a rollback that itself fails, can leave ``agentic_insights.json`` a write ahead of
``inventory.json``; the lock file then stays behind, so the next enrich refuses until the lock is
removed and enrich is run again. The record is **idempotent**: re-running with the same
authored insights rewrites byte-identical bytes and never stacks. The library owns
``schema_version``, ``inventory_sha256``, ``source_graphs_sha256`` and ``agentic_insights_sha256``;
authored insights carrying any of them are rejected as unknown keys.
The author may also supply ``authored_against``, the ``source_graphs_sha256`` copied from the
``inventory.json`` it read. It is optional; when given it is verified, so insights written before
discover ran again are refused, and it is never recorded.
"""

from __future__ import annotations

import json
import os
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import Any

from flowx.discovery_inventory import build_source_inventory
from flowx.discovery_serde import SOURCE_GRAPHS_FILENAME, canonical_sha256, source_graphs_from_document
from flowx.models.discovery import SOURCE_ADF, SourceGraph
from flowx.models.insights import (
    CONFIDENCE_LEVELS,
    MAX_RECOMMENDED_PATTERNS,
    RELEASE_STATES,
    RELEASE_STATES_REQUIRING_SOURCE,
    SCHEMA_VERSION,
)

# The single additive top-level key the insights block is rendered under in inventory.json.
INSIGHTS_KEY = "insights"

# The file enrich writes; inventory.json's insights block is always a copy of it.
AGENTIC_INSIGHTS_FILENAME = "agentic_insights.json"

# Held in metadata/ while one enrich reads, validates and writes, so two enrich calls never interleave.
ENRICH_LOCK_FILENAME = ".enrich.lock"

# Library-owned keys stamped on record; authored insights must not supply them.
_SCHEMA_VERSION_KEY = "schema_version"
_FINGERPRINT_KEY = "inventory_sha256"
_SOURCE_GRAPHS_HASH_KEY = "source_graphs_sha256"
_AGENTIC_INSIGHTS_HASH_KEY = "agentic_insights_sha256"
_LIBRARY_KEYS = (_SCHEMA_VERSION_KEY, _FINGERPRINT_KEY, _SOURCE_GRAPHS_HASH_KEY, _AGENTIC_INSIGHTS_HASH_KEY)

# The source_graphs_sha256 the author read from inventory.json, checked when given so stale insights never
# bind to a newer discover. Optional and not recorded: the library stamps the hash itself.
_AUTHORED_AGAINST_KEY = "authored_against"
_INSIGHTS_TOP_KEYS = {
    "overview",
    "system_recommendation",
    "pipeline_insights",
    "pipeline_relationships",
    _AUTHORED_AGAINST_KEY,
}
_INSIGHT_KEYS = {
    "pipeline",
    "pattern_name",
    "intent",
    "databricks_pattern",
    "recommended_patterns",
    "conversion_notes",
    "risk_if_ignored",
}
_INSIGHT_TEXT_FIELDS = ("pattern_name", "intent", "databricks_pattern", "risk_if_ignored")
_RECOMMENDED_PATTERN_KEYS = {"pattern", "fit", "simplification_pattern", "release_state", "release_state_source"}
_SYSTEM_RECOMMENDATION_KEYS = {"headline", "recommended_patterns", "cascade", "decision_driver"}
_RELATIONSHIP_KEYS = {
    "from_pipeline",
    "to_pipeline",
    "lineage_edge",
    "relationship_summary",
    "databricks_pattern",
    "risk_if_ignored",
}
_RELATIONSHIP_TEXT_FIELDS = ("relationship_summary", "databricks_pattern", "risk_if_ignored")
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
        hint = " (set by the library, not the author)" if key in _LIBRARY_KEYS else ""
        violations.append(f"unknown top-level key: {key!r}{hint}")

    violations.extend(_authored_against_violations(raw.get(_AUTHORED_AGAINST_KEY), inventory))

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


def _authored_against_violations(authored_against: Any, inventory: dict[str, Any]) -> list[str]:
    """Check the insights were authored against the inventory enrich is about to record them on.

    ``authored_against`` is optional: without it the library stamps the graphs hash itself. When the
    author does copy the inventory's ``source_graphs_sha256`` into it, a different value means
    discover ran again after the insights were written, so they would otherwise bind to a newer
    inventory unchecked.
    """
    if authored_against is None:
        return []
    recorded = inventory.get(_SOURCE_GRAPHS_HASH_KEY)
    if recorded is None:
        return [f"'{_AUTHORED_AGAINST_KEY}' was given but inventory.json records no {_SOURCE_GRAPHS_HASH_KEY}"]
    if authored_against != recorded:
        return [
            f"the insights were authored against a different inventory ({_AUTHORED_AGAINST_KEY} "
            f"{authored_against!r}, inventory {_SOURCE_GRAPHS_HASH_KEY} {recorded!r}); re-read inventory.json "
            "and author the insights again"
        ]
    return []


def _validate_pipeline_insights(insights: Any, names: set[str]) -> list[str]:
    """Validate the ``pipeline_insights`` list: shape, unknown fields, the pipeline FK, one entry per pipeline."""
    if not isinstance(insights, list):
        return ["'pipeline_insights' must be a list"]
    violations: list[str] = []
    first_index_by_name: dict[str, int] = {}
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
        elif not isinstance(name, str):
            violations.append(f"{loc}: 'pipeline' must be a string, got {type(name).__name__}")
        elif name in first_index_by_name:
            violations.append(
                f"{loc}: duplicate insight for pipeline {name!r} (already given at "
                f"pipeline_insights[{first_index_by_name[name]}]); give one entry per pipeline"
            )
        else:
            first_index_by_name[name] = index
            if name not in names:
                violations.append(f"{loc}: pipeline {name!r} not in inventory")
        violations.extend(_validate_optional_strings(item, _INSIGHT_TEXT_FIELDS, loc))
        notes = item.get("conversion_notes")
        if notes is not None and (not isinstance(notes, list) or not all(isinstance(note, str) for note in notes)):
            violations.append(f"{loc}: 'conversion_notes' must be a list of strings when present")
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
            elif not isinstance(value, str):
                violations.append(f"{loc}: {endpoint!r} must be a string, got {type(value).__name__}")
            elif value not in names:
                violations.append(f"{loc}: {endpoint} {value!r} not in inventory")
        violations.extend(_validate_optional_strings(relationship, _RELATIONSHIP_TEXT_FIELDS, loc))
        violations.extend(
            _validate_edge(relationship.get("lineage_edge"), loc, from_pipeline, to_pipeline, control_triples)
        )
    return violations


def _validate_optional_strings(record: dict[str, Any], fields: tuple[str, ...], loc: str) -> list[str]:
    """Report each of *fields* that *record* sets to something other than a string (``null`` is allowed)."""
    return [
        f"{loc}: {field_name!r} must be a string when present, got {type(record[field_name]).__name__}"
        for field_name in fields
        if record.get(field_name) is not None and not isinstance(record[field_name], str)
    ]


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
        problems.extend(_validate_release_state(pattern, pattern_loc))
    return problems


def _validate_release_state(pattern: dict[str, Any], loc: str) -> list[str]:
    """Validate a recommended pattern's optional GA/Preview ``release_state`` fields.

    The release state is authored judgment the deterministic layer can never derive, verified against
    current public Databricks docs. Rules (all collected, never fail-fast, matching the rest of the
    validator):

    * ``release_state``, when set, must be one of :data:`~flowx.models.insights.RELEASE_STATES`.
    * ``release_state`` is **required** whenever ``simplification_pattern`` is ``True`` -- a
      distinctive capability that collapses a legacy pattern must declare its verified release state
      so the downstream routing surface can disclose it correctly rather than recommend blindly.
    * ``release_state_source`` (a non-empty citation) is **required** whenever ``release_state`` is a
      non-GA preview/beta state (:data:`~flowx.models.insights.RELEASE_STATES_REQUIRING_SOURCE`); it
      is not required for ``"ga"`` (the stable default) or ``"unknown"`` (no claim to ground).
    * ``release_state_source``, when set, must be a string whatever the ``release_state``.
    """
    problems: list[str] = []
    release_state = pattern.get("release_state")
    source = pattern.get("release_state_source")

    if release_state is not None and release_state not in RELEASE_STATES:
        allowed = ", ".join(repr(state) for state in RELEASE_STATES)
        problems.append(f"{loc}: 'release_state' must be one of {{{allowed}}}, got {release_state!r}")

    if pattern.get("simplification_pattern") is True and release_state is None:
        problems.append(
            f"{loc}: 'release_state' is required when 'simplification_pattern' is true "
            f"(a distinctive capability must declare its verified GA/Preview release state)"
        )

    if source is not None and not isinstance(source, str):
        problems.append(f"{loc}: 'release_state_source' must be a string when present, got {type(source).__name__}")
    elif release_state in RELEASE_STATES_REQUIRING_SOURCE and (source is None or not source.strip()):
        problems.append(
            f"{loc}: 'release_state_source' (a non-empty doc URL / citation) is required when "
            f"'release_state' is {release_state!r}"
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
# Loading, fingerprinting, and the locked idempotent record.
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
    """The deterministic inventory with any previously-rendered ``insights`` block stripped.

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
    return canonical_sha256(_base_inventory(inventory))


def build_agentic_insights(inventory: dict[str, Any], raw: dict[str, Any]) -> dict[str, Any]:
    """Build the ``agentic_insights.json`` document from validated authored insights.

    The document is the authored content plus the library-owned ``schema_version``, the
    ``inventory_sha256`` fingerprint of the deterministic inventory, the persisted
    ``source_graphs_sha256`` the inventory records (when it records one), and finally
    ``agentic_insights_sha256``, a hash over everything above it. Does not mutate the inputs.
    """
    base = _base_inventory(inventory)
    document: dict[str, Any] = {_SCHEMA_VERSION_KEY: SCHEMA_VERSION, _FINGERPRINT_KEY: inventory_fingerprint(base)}
    source_graphs_sha256 = base.get(_SOURCE_GRAPHS_HASH_KEY)
    if source_graphs_sha256 is not None:
        document[_SOURCE_GRAPHS_HASH_KEY] = source_graphs_sha256
    for key in ("overview", "system_recommendation", "pipeline_insights", "pipeline_relationships"):
        if key in raw:
            document[key] = raw[key]
    document[_AGENTIC_INSIGHTS_HASH_KEY] = canonical_sha256(document)
    return document


def agentic_insights_hash_violations(document: dict[str, Any]) -> list[str]:
    """Check an ``agentic_insights.json`` document's own hash; an empty list means it is intact."""
    recorded = document.get(_AGENTIC_INSIGHTS_HASH_KEY)
    content = {key: value for key, value in document.items() if key != _AGENTIC_INSIGHTS_HASH_KEY}
    if recorded != canonical_sha256(content):
        return ["agentic_insights.json does not match its recorded agentic_insights_sha256"]
    return []


def render_inventory(inventory: dict[str, Any], agentic_insights: dict[str, Any]) -> dict[str, Any]:
    """Return the inventory built from its deterministic part plus the agentic insights document.

    Does not mutate the input and performs no I/O. The deterministic keys keep their original
    order and values, so re-serialising them is byte-identical to discover's write, and the
    ``insights`` block is exactly the ``agentic_insights.json`` content, replacing any prior block.
    """
    base = _base_inventory(inventory)
    base[INSIGHTS_KEY] = dict(agentic_insights)
    return base


def _load_source_graphs(output_dir: Path, inventory: dict[str, Any]) -> tuple[list[SourceGraph] | None, list[str]]:
    """Read the saved ``source_graphs.json`` the inventory records, as ``(graphs, violations)``.

    An inventory without a recorded hash (an older discover, or a source that does not persist
    graphs yet) has nothing to read, so it yields ``(None, [])``. Otherwise the saved file must be
    intact and its document hash must be the one the inventory records, so insights are never
    bound to a stale graph; any problem yields ``(None, [violation])``.
    """
    recorded = inventory.get(_SOURCE_GRAPHS_HASH_KEY)
    if recorded is None:
        return None, []
    graphs_path = Path(output_dir) / "metadata" / SOURCE_GRAPHS_FILENAME
    if not graphs_path.is_file():
        return None, [
            f"inventory.json records source_graphs_sha256 but {SOURCE_GRAPHS_FILENAME} is missing; re-run discover"
        ]
    try:
        document = json.loads(graphs_path.read_text(encoding="utf-8"))
        if not isinstance(document, dict):
            raise ValueError(f"expected a JSON object, got {type(document).__name__}")
        if document.get("document_sha256") != recorded:
            return None, [f"inventory.json was projected from a different {SOURCE_GRAPHS_FILENAME}; re-run discover"]
        return source_graphs_from_document(document), []
    except (OSError, json.JSONDecodeError, ValueError) as error:
        return None, [f"{SOURCE_GRAPHS_FILENAME} is not usable: {error}"]


# Sources whose discover leaves zero-activity pipelines out of the per-pipeline listing (see the ADF loader).
_SOURCES_OMITTING_EMPTY_PIPELINES = frozenset({SOURCE_ADF})


def project_inventory(graphs: list[SourceGraph], inventory: dict[str, Any]) -> dict[str, Any]:
    """Project the deterministic inventory again from the saved source graphs, as discover did.

    The graphs are the content; ``inventory.json`` only supplies the two settings discover passed
    alongside them and that ``source_graphs.json`` does not record -- the ``source_dir`` echoed for
    provenance and the recorded ``source_graphs_sha256`` -- plus the source, which decides whether
    empty pipelines are listed. The result serialises byte-identically to discover's inventory.
    """
    source = str(inventory.get("source", ""))
    return build_source_inventory(
        graphs,
        source=source,
        source_dir=str(inventory.get("source_dir", "")),
        include_empty_pipelines=source not in _SOURCES_OMITTING_EMPTY_PIPELINES,
        source_graphs_sha256=inventory.get(_SOURCE_GRAPHS_HASH_KEY),
    )


class _UnrecoveredWriteError(OSError):
    """The two files may disagree: putting the previous insights file back failed, or an interrupt hit a replace."""


@contextmanager
def _enrich_lock(metadata_dir: Path) -> Iterator[bool]:
    """Hold the output directory's enrich lock, yielding ``False`` when another enrich already holds it.

    The lock is a file created with ``O_EXCL`` (portable, unlike ``fcntl``) and removed when the
    enrich finishes or raises. It is left behind only when the two files may disagree -- a killed
    process, or an error that is or was raised from :class:`_UnrecoveredWriteError` (a failed
    rollback, or an interrupt between the two replaces) -- so the next enrich refuses until the lock
    is removed.
    """
    lock_path = metadata_dir / ENRICH_LOCK_FILENAME
    try:
        os.close(os.open(lock_path, os.O_CREAT | os.O_EXCL | os.O_WRONLY))
    except FileExistsError:
        yield False
        return
    keep_lock = False
    try:
        yield True
    except BaseException as error:
        keep_lock = isinstance(error, _UnrecoveredWriteError) or isinstance(error.__cause__, _UnrecoveredWriteError)
        raise
    finally:
        if not keep_lock:
            lock_path.unlink(missing_ok=True)


def _write_both_or_neither(
    insights_path: Path, insights_document: dict[str, Any], inventory_path: Path, inventory_document: dict[str, Any]
) -> None:
    """Replace ``agentic_insights.json`` and ``inventory.json`` back to back, keeping them consistent.

    Both temp files are written before either target is replaced. If replacing the inventory raises
    ``OSError`` (the rename did not happen), the previous insights file is put back by replacing it
    from a temp file (or the new one is removed), so the live file is never left half-written. If
    that rollback fails too, the two may disagree, so :class:`_UnrecoveredWriteError` is raised and
    the caller's lock stays behind. An interrupt (``KeyboardInterrupt``, ``SystemExit``) from the
    first replace on can arrive just after a rename succeeded, so nothing is rolled back: it is
    treated like a kill and re-raised from an :class:`_UnrecoveredWriteError`, keeping the lock.
    Callers hold :func:`_enrich_lock`, so no other enrich writes between, and the fixed temp names
    mean the next write overwrites and removes any left behind by a killed one.
    """
    previous_insights = insights_path.read_bytes() if insights_path.exists() else None
    insights_temporary = insights_path.with_name(f".{insights_path.name}.tmp")
    inventory_temporary = inventory_path.with_name(f".{inventory_path.name}.tmp")
    try:
        insights_temporary.write_text(json.dumps(insights_document, indent=2), encoding="utf-8")
        inventory_temporary.write_text(json.dumps(inventory_document, indent=2), encoding="utf-8")
        try:
            os.replace(insights_temporary, insights_path)
            try:
                os.replace(inventory_temporary, inventory_path)
            except OSError:
                try:
                    if previous_insights is None:
                        insights_path.unlink(missing_ok=True)
                    else:
                        insights_temporary.write_bytes(previous_insights)
                        os.replace(insights_temporary, insights_path)
                except OSError as rollback_error:
                    raise _UnrecoveredWriteError(
                        f"inventory.json was not updated and the previous {insights_path.name} could not be put "
                        f"back ({rollback_error}); the enrich lock is kept, so delete it and run enrich again"
                    ) from rollback_error
                raise
        except Exception:
            raise
        except BaseException as interrupt:
            raise interrupt from _UnrecoveredWriteError(
                f"enrich was interrupted while replacing {insights_path.name} and inventory.json, so they may "
                "disagree; the enrich lock is kept, so delete it and run enrich again"
            )
    finally:
        insights_temporary.unlink(missing_ok=True)
        inventory_temporary.unlink(missing_ok=True)


def enrich_inventory(
    output_dir: Path,
    *,
    insights: dict[str, Any] | None = None,
    insights_path: Path | None = None,
) -> dict[str, Any]:
    """Validate authored insights against the inventory, then record them on success.

    Reads ``<output_dir>/metadata/inventory.json``, validates the authored insights and checks the
    inventory still matches the saved ``source_graphs.json`` it records. Only when there are no
    violations does it write ``metadata/agentic_insights.json`` and rebuild ``inventory.json`` from
    the saved source graphs (or, without a recorded graphs hash, its existing deterministic part)
    plus that document, every deterministic key byte-identical to discover's write. On any
    violation both files are left untouched. The read, the checks and the write all happen under
    the output directory's enrich lock; when another enrich holds it, that is reported as a
    violation and nothing is written.

    Provide the authored insights via exactly one of ``insights`` (an inline dict) or
    ``insights_path`` (a JSON file).

    Returns a result dict ``{"ok", "violations", "inventory_sha256", "agentic_insights_sha256",
    "pipeline_insights", "relationships"}``. ``ok`` is ``False`` (and the files untouched) when
    there are violations.

    Raises:
        FileNotFoundError: when ``inventory.json`` does not exist (run discover first).
        ValueError: when neither or both insight sources are provided, or the inventory file is
            not a JSON object.
    """
    inventory_path = Path(output_dir) / "metadata" / "inventory.json"
    if not inventory_path.exists():
        raise FileNotFoundError(f"No inventory.json under {inventory_path.parent}; run the discover phase first.")
    raw = load_insights(insights=insights, insights_path=insights_path)
    with _enrich_lock(inventory_path.parent) as locked:
        if not locked:
            lock_path = inventory_path.parent / ENRICH_LOCK_FILENAME
            return {
                "ok": False,
                "violations": [
                    f"another enrich is writing to {inventory_path.parent} ({lock_path} exists); run enrich again "
                    "once it finishes. If no enrich is running, a previous one was killed mid-write: delete the "
                    "lock and run enrich again to rewrite both files"
                ],
                "pipeline_insights": 0,
                "relationships": 0,
            }
        return _enrich_locked(Path(output_dir), inventory_path, raw)


def _enrich_locked(output_dir: Path, inventory_path: Path, raw: Any) -> dict[str, Any]:
    """The read-validate-write half of :func:`enrich_inventory`, run while holding the enrich lock."""
    inventory = json.loads(inventory_path.read_text(encoding="utf-8"))
    if not isinstance(inventory, dict):
        raise ValueError(f"inventory.json must contain a JSON object, got {type(inventory).__name__}")

    graphs, graph_violations = _load_source_graphs(output_dir, inventory)
    deterministic = inventory if graphs is None else project_inventory(graphs, inventory)
    violations = validate_insights(raw, deterministic) + graph_violations
    if violations:
        return {"ok": False, "violations": violations, "pipeline_insights": 0, "relationships": 0}

    agentic_insights = build_agentic_insights(deterministic, raw)
    _write_both_or_neither(
        inventory_path.with_name(AGENTIC_INSIGHTS_FILENAME),
        agentic_insights,
        inventory_path,
        render_inventory(deterministic, agentic_insights),
    )
    return {
        "ok": True,
        "violations": [],
        "inventory_sha256": agentic_insights[_FINGERPRINT_KEY],
        "agentic_insights_sha256": agentic_insights[_AGENTIC_INSIGHTS_HASH_KEY],
        "pipeline_insights": len(raw.get("pipeline_insights", [])),
        "relationships": len(raw.get("pipeline_relationships", [])),
    }
