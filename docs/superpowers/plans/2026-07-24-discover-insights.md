# Agentic Insights in inventory.json Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add an agent-authored `insights` key to `metadata/inventory.json` during the discover phase, validated and merged by a pure tool, so the later convert phase can consume pipeline intent, Databricks patterns, and cross-pipeline relationships.

**Architecture:** The agent *authors* an `insights` JSON object (judgment: intent, patterns, relationships that annotate #9's deterministic lineage edges); a new pure tool path *enriches* the inventory — it validates the authored JSON against the inventory (foreign keys to pipeline names and lineage edges), and on success appends exactly one `insights` key while re-serializing the rest byte-identically. There is **no LLM inside the tool**. The feature is surfaced through a new `enrich` adapter subcommand and an `enrich` MCP command, and driven by a new Step 5 in the discover skill.

**Tech Stack:** Python 3.12, `@dataclass(slots=True, kw_only=True)` models, argparse CLI subcommands, FastMCP dispatcher tool, pytest unit tests, ruff + mypy via `make fmt`.

## Global Constraints

- **Python version:** 3.12+ (matches repo floor).
- **Dataclasses:** every model uses `@dataclass(slots=True, kw_only=True)` (AGENTS.md Code Style Rules).
- **Byte-identical write:** the enrich write-back MUST use `json.dumps(obj, indent=2)` with **no** `sort_keys`, **no** `default=str`, and **no** trailing newline — exactly matching discover's write at `src/flowx/parser/adf_loader.py:1021` (`inventory_path.write_text(json.dumps(inventory_dict, indent=2), encoding="utf-8")`). Any deviation breaks the "deterministic keys byte-identical" invariant.
- **Validation before I/O:** `enrich_inventory` MUST return the failure result before writing anything when violations exist. On `ok:false` the inventory file is left untouched on disk.
- **No new dependencies:** validator is hand-rolled; use only stdlib (`json`, `pathlib`, `tempfile`) and existing flowx modules.
- **Tests assert structure/schema, never prose.** No live LLM in any test — all insights fixtures are stubbed JSON literals. Unit tests live in `tests/unit/`, fixtures in `tests/resources/json/`.
- **Customer confidentiality:** no real customer names or customer-derived vocabulary in code, tests, fixtures, comments, or commit messages. Use generic placeholders ("Factory A", `entityID`, "dummy dataset"). The forbidden denylist ("a customer factory", "a large factory", `engagementID`, `engagementDBVersions`, `etl-parameters`) lives ONLY in the design doc as a grep reference — never introduce those terms.
- **Test command:** `PYTHONPATH=src uv run pytest tests/unit -v` (or a single node id with `::`). Format/lint: `make fmt` (runs `ruff format`, `ruff check --fix`, `mypy src/flowx/`).
- **`edge_identity` grammar:** for `edge_type="control"` it is the `ControlEdge.activity_name`; for `edge_type="data"` it is the `DataEdge.match_key`. Validation resolves against exactly these keys.
- **Enriched marker:** presence of the top-level `insights` key IS the enriched marker. Do NOT add any `schema_version` field.

---

## File Structure

**Created:**
- `src/flowx/parser/pipeline_insights.py` — `load_insights`, `validate_insights`, `merge_into_inventory`, `enrich_inventory`. The full validate-then-merge core.
- `tests/unit/test_pipeline_insights.py` — all unit tests for the models + parser module.

**Modified:**
- `src/flowx/models/adf_ast.py` — add 4 dataclasses (`LineageEdgeRef`, `PipelineInsight`, `PipelineRelationship`, `Insights`) after `Lineage` (currently ends at line 403).
- `src/flowx/adapter/__main__.py` — add the `enrich` subparser (modeled on `record-results`), an `_run_enrich` handler, and dispatch in `main()`.
- `src/flowx/mcp/runner.py` — add `materialize_json(obj)` helper (parallels `materialize_adf_definitions`).
- `src/flowx/mcp/server.py` — add `_cmd_enrich`, register `"enrich"` in `_COMMANDS`, extend the `flowx` tool docstring.
- `src/flowx/reporting/coverage.py` — add a `has_insights` column (optional, gated on the `insights` key existing).
- `tests/unit/test_reporting_coverage.py` — cover the new column (only if Task 7 is done).
- `skills/flowx-discover/SKILL.md` — insert the new Step 5 (author → enrich); renumber existing Steps 5–8; reword the summary step.

---

## Task 1: Insights data models

**Files:**
- Modify: `src/flowx/models/adf_ast.py` (append after line 403, the end of `class Lineage`)
- Test: `tests/unit/test_pipeline_insights.py`

**Interfaces:**
- Consumes: nothing (leaf dataclasses). `Literal` is already imported at `adf_ast.py:7`; `dataclass`/`field` at line 5.
- Produces: `LineageEdgeRef(edge_type: Literal["control","data"], edge_identity: str)`; `PipelineInsight(pipeline: str, pattern_name: str|None=None, intent: str|None=None, databricks_pattern: str|None=None, recommended_databricks_features: list[str]=[], conversion_notes: list[str]=[])`; `PipelineRelationship(from_pipeline: str, to_pipeline: str, lineage_edge: LineageEdgeRef, relationship_summary: str|None=None, databricks_pattern: str|None=None, risk_if_ignored: str|None=None)`; `Insights(overview: str|None=None, pipeline_insights: list[PipelineInsight]=[], pipeline_relationships: list[PipelineRelationship]=[])`.

- [ ] **Step 1: Write the failing test**

Create `tests/unit/test_pipeline_insights.py` with this first test (imports at top of file):

```python
"""Tests for agentic insights models, validation, and enrichment (discover phase)."""

from __future__ import annotations

import json
from pathlib import Path

from flowx.models.adf_ast import (
    Insights,
    LineageEdgeRef,
    PipelineInsight,
    PipelineRelationship,
)


def test_insights_dataclasses_construct_with_defaults():
    edge = LineageEdgeRef(edge_type="control", edge_identity="Run Ingestion Pipeline")
    rel = PipelineRelationship(
        from_pipeline="factory_a", to_pipeline="factory_b", lineage_edge=edge
    )
    insight = PipelineInsight(pipeline="factory_a")
    doc = Insights(
        overview="whole factory",
        pipeline_insights=[insight],
        pipeline_relationships=[rel],
    )
    assert doc.pipeline_insights[0].pipeline == "factory_a"
    assert doc.pipeline_relationships[0].lineage_edge.edge_type == "control"
    assert doc.pipeline_relationships[0].lineage_edge.edge_identity == "Run Ingestion Pipeline"
    # optional fields default cleanly
    assert insight.recommended_databricks_features == []
    assert insight.conversion_notes == []
    assert rel.relationship_summary is None
```

- [ ] **Step 2: Run test to verify it fails**

Run: `PYTHONPATH=src uv run pytest tests/unit/test_pipeline_insights.py::test_insights_dataclasses_construct_with_defaults -v`
Expected: FAIL with `ImportError: cannot import name 'Insights' from 'flowx.models.adf_ast'`

- [ ] **Step 3: Add the dataclasses**

Append to `src/flowx/models/adf_ast.py` (after line 403, following the existing section-comment style):

```python
# ---------------------------------------------------------------------------
# Agentic insights (discover phase) -- agent-authored judgment merged into
# inventory.json. References pipelines by name and annotates deterministic
# Lineage edges; carries no facts of its own.
# ---------------------------------------------------------------------------


@dataclass(slots=True, kw_only=True)
class LineageEdgeRef:
    """A typed reference from a PipelineRelationship to one deterministic edge.

    Attributes:
        edge_type: Which lineage graph the edge lives in.
        edge_identity: For ``"control"`` -- the ``ControlEdge.activity_name``;
            for ``"data"`` -- the ``DataEdge.match_key``. Echoed verbatim from a
            real edge so enrichment can resolve it.
    """

    edge_type: Literal["control", "data"]
    edge_identity: str


@dataclass(slots=True, kw_only=True)
class PipelineInsight:
    """Per-pipeline judgment; references a pipeline by name (foreign key)."""

    pipeline: str
    pattern_name: str | None = None
    intent: str | None = None
    databricks_pattern: str | None = None
    recommended_databricks_features: list[str] = field(default_factory=list)
    conversion_notes: list[str] = field(default_factory=list)


@dataclass(slots=True, kw_only=True)
class PipelineRelationship:
    """Cross-pipeline judgment; annotates one deterministic lineage edge."""

    from_pipeline: str
    to_pipeline: str
    lineage_edge: LineageEdgeRef
    relationship_summary: str | None = None
    databricks_pattern: str | None = None
    risk_if_ignored: str | None = None


@dataclass(slots=True, kw_only=True)
class Insights:
    """Agent-authored insights merged into inventory.json under the ``insights`` key."""

    overview: str | None = None
    pipeline_insights: list[PipelineInsight] = field(default_factory=list)
    pipeline_relationships: list[PipelineRelationship] = field(default_factory=list)
```

- [ ] **Step 4: Run test to verify it passes**

Run: `PYTHONPATH=src uv run pytest tests/unit/test_pipeline_insights.py::test_insights_dataclasses_construct_with_defaults -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/flowx/models/adf_ast.py tests/unit/test_pipeline_insights.py
git commit -m "$(cat <<'EOF'
Add agentic insights dataclasses to adf_ast

LineageEdgeRef, PipelineInsight, PipelineRelationship, Insights -- the
typed round-trip side of the discover-phase insights block.

Co-authored-by: Isaac
EOF
)"
```

---

## Task 2: The `validate_insights` pure validator

**Files:**
- Create: `src/flowx/parser/pipeline_insights.py`
- Test: `tests/unit/test_pipeline_insights.py`

**Interfaces:**
- Consumes: an `inventory` dict shaped like discover's `_inventory_to_dict` output — `inventory["pipelines"]` is a list of `{"name": str, "activities": [...]}`; `inventory["lineage"]["control_edges"]` is a list of `{"caller_pipeline","callee_pipeline","activity_name","wait_on_completion"}`; `inventory["lineage"]["data_edges"]` is a list of `{"dataset_name","identity","producer_pipeline","producer_activity","consumer_pipeline","consumer_activity","match_kind","match_key"}`.
- Produces: `validate_insights(raw: dict, inventory: dict) -> list[str]` — returns a list of human-readable violation strings; empty list means valid. Collects ALL violations (never fail-fast).

- [ ] **Step 1: Write the failing tests**

Add to `tests/unit/test_pipeline_insights.py` (extend the import from the parser module):

```python
from flowx.parser.pipeline_insights import validate_insights


def _inventory() -> dict:
    """A minimal inventory dict in discover's serialized shape."""
    return {
        "source_dir": "/tmp/adf",
        "pipelines": [
            {"name": "factory_a", "activities": []},
            {"name": "factory_b", "activities": []},
        ],
        "summary": {"pipeline_count": 2},
        "lineage": {
            "control_edges": [
                {
                    "caller_pipeline": "factory_a",
                    "callee_pipeline": "factory_b",
                    "activity_name": "Run Ingestion Pipeline",
                    "wait_on_completion": True,
                }
            ],
            "data_edges": [
                {
                    "dataset_name": "ds_orders",
                    "identity": "curated.orders",
                    "producer_pipeline": "factory_a",
                    "producer_activity": "Write Orders",
                    "consumer_pipeline": "factory_b",
                    "consumer_activity": "Read Orders",
                    "match_kind": "identity",
                    "match_key": "curated.orders",
                }
            ],
        },
    }


def _good_insights() -> dict:
    return {
        "overview": "Two-stage ingestion then transform.",
        "pipeline_insights": [
            {"pipeline": "factory_a", "intent": "Ingest", "databricks_pattern": "Autoloader"},
            {"pipeline": "factory_b", "intent": "Transform"},
        ],
        "pipeline_relationships": [
            {
                "from_pipeline": "factory_a",
                "to_pipeline": "factory_b",
                "lineage_edge": {
                    "edge_type": "control",
                    "edge_identity": "Run Ingestion Pipeline",
                },
                "relationship_summary": "A invokes B",
                "databricks_pattern": "run_job_task",
                "risk_if_ignored": "ordering lost",
            }
        ],
    }


def test_validator_accepts_good_insights():
    assert validate_insights(_good_insights(), _inventory()) == []


def test_rejects_pipeline_not_in_inventory():
    raw = _good_insights()
    raw["pipeline_insights"][0]["pipeline"] = "ghost_pipeline"
    violations = validate_insights(raw, _inventory())
    assert violations
    assert any("ghost_pipeline" in v for v in violations)


def test_rejects_relationship_endpoint_not_in_inventory():
    raw = _good_insights()
    raw["pipeline_relationships"][0]["to_pipeline"] = "ghost_pipeline"
    violations = validate_insights(raw, _inventory())
    assert any("ghost_pipeline" in v for v in violations)


def test_rejects_unresolvable_control_edge():
    raw = _good_insights()
    raw["pipeline_relationships"][0]["lineage_edge"]["edge_identity"] = "No Such Activity"
    violations = validate_insights(raw, _inventory())
    assert any("No Such Activity" in v for v in violations)


def test_data_edge_binds_on_match_key():
    raw = _good_insights()
    raw["pipeline_relationships"][0]["lineage_edge"] = {
        "edge_type": "data",
        "edge_identity": "curated.orders",
    }
    assert validate_insights(raw, _inventory()) == []
    # a non-matching key is rejected
    raw["pipeline_relationships"][0]["lineage_edge"]["edge_identity"] = "curated.missing"
    assert validate_insights(raw, _inventory())


def test_rejects_missing_required_field():
    # PipelineInsight missing 'pipeline'
    raw = {"pipeline_insights": [{"intent": "x"}], "pipeline_relationships": []}
    assert any("pipeline" in v for v in validate_insights(raw, _inventory()))
    # PipelineRelationship missing 'lineage_edge'
    raw2 = {
        "pipeline_insights": [],
        "pipeline_relationships": [{"from_pipeline": "factory_a", "to_pipeline": "factory_b"}],
    }
    assert any("lineage_edge" in v for v in validate_insights(raw2, _inventory()))


def test_rejects_unknown_field():
    raw = _good_insights()
    raw["pipeline_insights"][0]["bogus_key"] = "x"
    assert any("bogus_key" in v for v in validate_insights(raw, _inventory()))


def test_rejects_unknown_top_level_key():
    raw = _good_insights()
    raw["surprise"] = 1
    assert any("surprise" in v for v in validate_insights(raw, _inventory()))
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `PYTHONPATH=src uv run pytest tests/unit/test_pipeline_insights.py -k validate -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'flowx.parser.pipeline_insights'` (plus the `data_edge`/`missing`/`unknown` tests erroring on import)

- [ ] **Step 3: Write the validator**

Create `src/flowx/parser/pipeline_insights.py`:

```python
"""Validate and merge agent-authored insights into the discover inventory.

The discover phase writes a pure ``metadata/inventory.json`` (pipelines, summary,
lineage). The agent then *authors* an ``insights`` object -- its judgment about
pipeline intent, Databricks patterns, and cross-pipeline relationships that
annotate the deterministic lineage edges. This module *enriches* the inventory:
it validates the authored JSON against the inventory (foreign keys to pipeline
names and lineage edges) and, only when clean, appends the single ``insights``
key while re-serialising the rest byte-identically.

There is no LLM here -- the tool only validates and merges.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

_INSIGHTS_TOP_KEYS = {"overview", "pipeline_insights", "pipeline_relationships"}
_INSIGHT_KEYS = {
    "pipeline",
    "pattern_name",
    "intent",
    "databricks_pattern",
    "recommended_databricks_features",
    "conversion_notes",
}
_RELATIONSHIP_KEYS = {
    "from_pipeline",
    "to_pipeline",
    "lineage_edge",
    "relationship_summary",
    "databricks_pattern",
    "risk_if_ignored",
}
_EDGE_KEYS = {"edge_type", "edge_identity"}


def _pipeline_names(inventory: dict) -> set[str]:
    return {p.get("name") for p in inventory.get("pipelines", []) if isinstance(p, dict)}


def _control_edge_identities(inventory: dict) -> set[str]:
    lineage = inventory.get("lineage") or {}
    return {e.get("activity_name") for e in lineage.get("control_edges", []) if isinstance(e, dict)}


def _data_edge_identities(inventory: dict) -> set[str]:
    lineage = inventory.get("lineage") or {}
    return {e.get("match_key") for e in lineage.get("data_edges", []) if isinstance(e, dict)}


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

    names = _pipeline_names(inventory)
    control_ids = _control_edge_identities(inventory)
    data_ids = _data_edge_identities(inventory)

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
        for endpoint in ("from_pipeline", "to_pipeline"):
            value = rel.get(endpoint)
            if not value:
                violations.append(f"{loc}: missing required field {endpoint!r}")
            elif value not in names:
                violations.append(f"{loc}: {endpoint} {value!r} not in inventory")
        violations.extend(_validate_edge(rel.get("lineage_edge"), loc, control_ids, data_ids))

    return violations


def _validate_edge(edge: Any, loc: str, control_ids: set[str], data_ids: set[str]) -> list[str]:
    """Validate one lineage_edge ref: shape + resolution to a real edge."""
    if edge is None:
        return [f"{loc}: missing required field 'lineage_edge'"]
    if not isinstance(edge, dict):
        return [f"{loc}.lineage_edge must be an object"]
    problems: list[str] = []
    for key in set(edge) - _EDGE_KEYS:
        problems.append(f"{loc}.lineage_edge: unknown field {key!r}")
    edge_type = edge.get("edge_type")
    identity = edge.get("edge_identity")
    if edge_type not in ("control", "data"):
        problems.append(f"{loc}.lineage_edge: edge_type must be 'control' or 'data', got {edge_type!r}")
        return problems
    if not isinstance(identity, str) or not identity:
        problems.append(f"{loc}.lineage_edge: edge_identity must be a non-empty string")
        return problems
    valid = control_ids if edge_type == "control" else data_ids
    if identity not in valid:
        problems.append(
            f"{loc}.lineage_edge: {edge_type} edge {identity!r} does not resolve to any lineage edge"
        )
    return problems
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `PYTHONPATH=src uv run pytest tests/unit/test_pipeline_insights.py -k validate -v` then `PYTHONPATH=src uv run pytest tests/unit/test_pipeline_insights.py -k "data_edge or missing or unknown" -v`
Expected: PASS (all validator + edge + field tests)

- [ ] **Step 5: Commit**

```bash
git add src/flowx/parser/pipeline_insights.py tests/unit/test_pipeline_insights.py
git commit -m "$(cat <<'EOF'
Add validate_insights: FK + lineage-edge validator

Pure, violation-collecting validator. Checks pipeline-name FKs, resolves
each lineage_edge ref to a real control/data edge (activity_name /
match_key), and rejects unknown or missing fields.

Co-authored-by: Isaac
EOF
)"
```

---

## Task 3: `load_insights`, `merge_into_inventory`, `enrich_inventory` (orchestrator + byte-identical write)

**Files:**
- Modify: `src/flowx/parser/pipeline_insights.py`
- Test: `tests/unit/test_pipeline_insights.py`

**Interfaces:**
- Consumes: `validate_insights` (Task 2). Reads `<output_dir>/metadata/inventory.json`.
- Produces:
  - `load_insights(*, insights: dict|None=None, insights_path: Path|None=None) -> dict` — returns the raw insights dict from exactly one source; raises `ValueError` if neither or both are given.
  - `merge_into_inventory(inventory: dict, raw: dict) -> dict` — returns a new dict with one added `insights` key; does not mutate input; no I/O.
  - `enrich_inventory(output_dir: Path, *, insights: dict|None=None, insights_path: Path|None=None) -> dict` — orchestrator returning `{"ok": bool, "violations": list[str], "pipeline_insights": int, "relationships": int}`. On violations, returns `ok=False` WITHOUT writing. On success, writes the merged inventory and returns `ok=True`.

- [ ] **Step 1: Write the failing tests**

Add to `tests/unit/test_pipeline_insights.py`:

```python
import pytest

from flowx.parser.pipeline_insights import (
    enrich_inventory,
    load_insights,
    merge_into_inventory,
)


def _write_inventory(tmp_path: Path, inventory: dict) -> Path:
    """Write inventory.json exactly as discover does (indent=2, no trailing newline)."""
    metadata = tmp_path / "metadata"
    metadata.mkdir(parents=True, exist_ok=True)
    path = metadata / "inventory.json"
    path.write_text(json.dumps(inventory, indent=2), encoding="utf-8")
    return path


def test_load_insights_requires_exactly_one_source():
    with pytest.raises(ValueError):
        load_insights()
    with pytest.raises(ValueError):
        load_insights(insights={"a": 1}, insights_path=Path("/x"))


def test_load_insights_from_inline_dict():
    assert load_insights(insights={"overview": "x"}) == {"overview": "x"}


def test_load_insights_from_path(tmp_path: Path):
    p = tmp_path / "ins.json"
    p.write_text(json.dumps({"overview": "y"}), encoding="utf-8")
    assert load_insights(insights_path=p) == {"overview": "y"}


def test_merge_into_inventory_adds_one_key_without_mutating():
    inv = {"pipelines": [], "summary": {}, "lineage": {}}
    raw = {"overview": "z"}
    merged = merge_into_inventory(inv, raw)
    assert merged["insights"] == {"overview": "z"}
    assert "insights" not in inv  # input not mutated
    assert set(merged) == {"pipelines", "summary", "lineage", "insights"}


def test_enrich_success_counts_and_writes(tmp_path: Path):
    _write_inventory(tmp_path, _inventory())
    result = enrich_inventory(tmp_path, insights=_good_insights())
    assert result["ok"] is True
    assert result["violations"] == []
    assert result["pipeline_insights"] == 2
    assert result["relationships"] == 1
    on_disk = json.loads((tmp_path / "metadata" / "inventory.json").read_text())
    assert on_disk["insights"]["overview"] == "Two-stage ingestion then transform."


def test_two_pass_deterministic_keys_byte_identical(tmp_path: Path):
    path = _write_inventory(tmp_path, _inventory())
    before = path.read_text(encoding="utf-8")
    enrich_inventory(tmp_path, insights=_good_insights())
    after = json.loads(path.read_text(encoding="utf-8"))
    # every key except the added 'insights' is byte-identical to the pre-enrich file
    after_without_insights = {k: v for k, v in after.items() if k != "insights"}
    assert json.dumps(after_without_insights, indent=2) == before


def test_enrich_is_idempotent(tmp_path: Path):
    path = _write_inventory(tmp_path, _inventory())
    enrich_inventory(tmp_path, insights=_good_insights())
    first = path.read_text(encoding="utf-8")
    enrich_inventory(tmp_path, insights=_good_insights())
    second = path.read_text(encoding="utf-8")
    assert first == second


def test_validation_failure_does_not_write(tmp_path: Path):
    path = _write_inventory(tmp_path, _inventory())
    before = path.read_text(encoding="utf-8")
    bad = _good_insights()
    bad["pipeline_insights"][0]["pipeline"] = "ghost_pipeline"
    result = enrich_inventory(tmp_path, insights=bad)
    assert result["ok"] is False
    assert result["violations"]
    assert path.read_text(encoding="utf-8") == before  # file untouched


def test_enrich_missing_inventory_raises(tmp_path: Path):
    with pytest.raises(FileNotFoundError):
        enrich_inventory(tmp_path, insights=_good_insights())
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `PYTHONPATH=src uv run pytest tests/unit/test_pipeline_insights.py -k "load_insights or merge_into or enrich or two_pass or idempotent or validation_failure" -v`
Expected: FAIL with `ImportError: cannot import name 'enrich_inventory'`

- [ ] **Step 3: Add the orchestrator functions**

Append to `src/flowx/parser/pipeline_insights.py`:

```python
def load_insights(*, insights: dict | None = None, insights_path: Path | None = None) -> dict:
    """Return the raw insights dict from exactly one source (inline or file).

    Raises:
        ValueError: if neither or both sources are provided.
    """
    if (insights is None) == (insights_path is None):
        raise ValueError("provide exactly one of 'insights' (inline dict) or 'insights_path'")
    if insights is not None:
        return insights
    return json.loads(Path(insights_path).read_text(encoding="utf-8"))


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
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `PYTHONPATH=src uv run pytest tests/unit/test_pipeline_insights.py -v`
Expected: PASS (all tests in the file)

- [ ] **Step 5: Commit**

```bash
git add src/flowx/parser/pipeline_insights.py tests/unit/test_pipeline_insights.py
git commit -m "$(cat <<'EOF'
Add enrich_inventory: validate-then-append two-pass write

load_insights (inline|path), merge_into_inventory (pure), and
enrich_inventory (orchestrator). Byte-identical re-serialize adds only
the 'insights' key; validation runs before any write, so a rejected
payload leaves inventory.json untouched. Idempotent.

Co-authored-by: Isaac
EOF
)"
```

---

## Task 4: Edge-binding tests on the real nested fixture

This task hardens the validator against a *genuine* inventory built from the shipped fixture (not a hand-faked dict), proving control-edge identities resolve on real `activity_name` values.

**Files:**
- Test: `tests/unit/test_pipeline_insights.py`

**Interfaces:**
- Consumes: `load_adf_definitions` (`flowx.parser.adf_loader`), `build_inventory` (`flowx.parser.adf_loader`, attaches lineage at line 247), `_inventory_to_dict` (`flowx.parser.adf_loader`), and `validate_insights` (Task 2). The fixture `pipeline_execute_pipeline_nested.json` yields control edges with `activity_name` values `"Run Ingestion Pipeline"`, `"Run Transform Pipeline"`, `"Run Cleanup Pipeline"` and callees `pipeline_copy_sql_to_delta`, `pipeline_notebook_with_params`, `pipeline_delete_recursive`.

- [ ] **Step 1: Write the failing test**

Add to `tests/unit/test_pipeline_insights.py`:

```python
from flowx.parser.adf_loader import (
    _inventory_to_dict,
    build_inventory,
    load_adf_definitions,
)


def _real_inventory(fixtures_dir) -> dict:
    """Build a real inventory dict (with lineage) from the shipped fixtures."""
    definitions = load_adf_definitions(fixtures_dir)
    inventory = build_inventory(definitions)
    return _inventory_to_dict(inventory, str(fixtures_dir))


def test_control_edge_binding_matches_and_rejects(fixtures_dir):
    inventory = _real_inventory(fixtures_dir)
    good = {
        "pipeline_insights": [],
        "pipeline_relationships": [
            {
                "from_pipeline": "pipeline_execute_pipeline_nested",
                "to_pipeline": "pipeline_copy_sql_to_delta",
                "lineage_edge": {
                    "edge_type": "control",
                    "edge_identity": "Run Ingestion Pipeline",
                },
            }
        ],
    }
    assert validate_insights(good, inventory) == []

    bad = json.loads(json.dumps(good))
    bad["pipeline_relationships"][0]["lineage_edge"]["edge_identity"] = "No Such Activity"
    assert validate_insights(bad, inventory)


def test_real_inventory_enrich_round_trip(fixtures_dir, tmp_path: Path):
    inventory = _real_inventory(fixtures_dir)
    metadata = tmp_path / "metadata"
    metadata.mkdir(parents=True)
    (metadata / "inventory.json").write_text(json.dumps(inventory, indent=2), encoding="utf-8")
    result = enrich_inventory(
        tmp_path,
        insights={
            "overview": "orchestrated ingest/transform/cleanup",
            "pipeline_insights": [{"pipeline": "pipeline_execute_pipeline_nested", "intent": "orchestrate"}],
            "pipeline_relationships": [],
        },
    )
    assert result["ok"] is True
    on_disk = json.loads((metadata / "inventory.json").read_text())
    assert on_disk["insights"]["pipeline_insights"][0]["pipeline"] == "pipeline_execute_pipeline_nested"
```

- [ ] **Step 2: Run test to verify it passes (validator already handles this)**

Run: `PYTHONPATH=src uv run pytest tests/unit/test_pipeline_insights.py -k "control_edge_binding or real_inventory" -v`
Expected: PASS — the validator from Task 2 already resolves control edges by `activity_name`. If any test fails, fix `validate_insights`, not the test. (This task is a regression guard against a real inventory, so no new production code is expected.)

- [ ] **Step 3: Commit**

```bash
git add tests/unit/test_pipeline_insights.py
git commit -m "$(cat <<'EOF'
Test insights validation against a real fixture-built inventory

Builds a genuine inventory (with lineage) from the shipped nested
ExecutePipeline fixture and asserts control-edge identities resolve on
real activity_name values.

Co-authored-by: Isaac
EOF
)"
```

---

## Task 5: The `enrich` adapter subcommand

**Files:**
- Modify: `src/flowx/adapter/__main__.py` (add subparser in `_build_parser` after the `record` block ~line 362; add `_run_enrich` handler after `_run_record_results` ~line 116; add dispatch in `main` after line 88)
- Test: `tests/unit/test_pipeline_insights.py`

**Interfaces:**
- Consumes: `enrich_inventory` (Task 3).
- Produces: CLI `python -m flowx.adapter enrich --output-dir <dir> [--insights-path <file>] [--insights <json-string>]`. Returns exit code 0 on success, 1 on any failure (missing inventory, bad/absent/both payload sources, validation violations). Exposed via `adapter.__main__.main(argv)`.

- [ ] **Step 1: Write the failing test**

Add to `tests/unit/test_pipeline_insights.py`:

```python
from flowx.adapter.__main__ import main as adapter_cli_main


def test_adapter_enrich_success(tmp_path: Path):
    _write_inventory(tmp_path, _inventory())
    ins = tmp_path / "insights.json"
    ins.write_text(json.dumps(_good_insights()), encoding="utf-8")
    code = adapter_cli_main(["enrich", "--output-dir", str(tmp_path), "--insights-path", str(ins)])
    assert code == 0
    on_disk = json.loads((tmp_path / "metadata" / "inventory.json").read_text())
    assert "insights" in on_disk


def test_adapter_enrich_validation_failure_returns_1(tmp_path: Path):
    path = _write_inventory(tmp_path, _inventory())
    before = path.read_text(encoding="utf-8")
    bad = _good_insights()
    bad["pipeline_insights"][0]["pipeline"] = "ghost_pipeline"
    ins = tmp_path / "insights.json"
    ins.write_text(json.dumps(bad), encoding="utf-8")
    code = adapter_cli_main(["enrich", "--output-dir", str(tmp_path), "--insights-path", str(ins)])
    assert code == 1
    assert path.read_text(encoding="utf-8") == before  # untouched


def test_adapter_enrich_missing_inventory_returns_1(tmp_path: Path):
    ins = tmp_path / "insights.json"
    ins.write_text(json.dumps(_good_insights()), encoding="utf-8")
    code = adapter_cli_main(["enrich", "--output-dir", str(tmp_path), "--insights-path", str(ins)])
    assert code == 1


def test_adapter_enrich_inline_json_string(tmp_path: Path):
    _write_inventory(tmp_path, _inventory())
    code = adapter_cli_main(
        ["enrich", "--output-dir", str(tmp_path), "--insights", json.dumps(_good_insights())]
    )
    assert code == 0
    assert "insights" in json.loads((tmp_path / "metadata" / "inventory.json").read_text())
```

- [ ] **Step 2: Run test to verify it fails**

Run: `PYTHONPATH=src uv run pytest tests/unit/test_pipeline_insights.py -k adapter_enrich -v`
Expected: FAIL — argparse exits with code 2 ("invalid choice: 'enrich'") since the subcommand does not exist yet.

- [ ] **Step 3: Add dispatch in `main()`**

In `src/flowx/adapter/__main__.py`, add after line 88 (`return _run_record_results(args)`):

```python
    if args.command == "enrich":
        return _run_enrich(args)
```

- [ ] **Step 4: Add the `_run_enrich` handler**

Add after `_run_record_results` (after line 115), following its structure:

```python
def _run_enrich(args: argparse.Namespace) -> int:
    """Implements ``enrich``: validate + merge agent-authored insights into inventory.json.

    Returns 0 on success, 1 on any failure (missing inventory, unreadable/absent/
    both payload sources, or validation violations).
    """
    from flowx.parser.pipeline_insights import enrich_inventory

    metadata_dir = args.output_dir / "metadata"
    if not (metadata_dir / "inventory.json").exists():
        print(f"No inventory.json under {metadata_dir}; run the discover phase first.", file=sys.stderr)
        return 1

    inline: dict[str, Any] | None = None
    if args.insights is not None:
        try:
            inline = json.loads(args.insights)
        except json.JSONDecodeError as error:
            print(f"Invalid --insights JSON: {error}", file=sys.stderr)
            return 1
    if (inline is None) == (args.insights_path is None):
        print("Provide exactly one of --insights (inline JSON) or --insights-path.", file=sys.stderr)
        return 1

    try:
        result = enrich_inventory(args.output_dir, insights=inline, insights_path=args.insights_path)
    except (OSError, ValueError, json.JSONDecodeError) as error:
        print(f"Failed to enrich inventory: {error}", file=sys.stderr)
        return 1

    if not result["ok"]:
        for violation in result["violations"]:
            print(f"  - {violation}", file=sys.stderr)
        print(
            f"Insights validation failed ({len(result['violations'])} violation(s)); "
            "inventory not modified.",
            file=sys.stderr,
        )
        return 1
    print(
        f"Enriched inventory: {result['pipeline_insights']} pipeline insight(s), "
        f"{result['relationships']} relationship(s)."
    )
    return 0
```

- [ ] **Step 5: Add the `enrich` subparser**

In `_build_parser`, add after the `record` subparser block (after line 362, before the `dashboard` parser):

```python
    enrich = subparsers.add_parser(
        "enrich",
        help="Validate and merge agent-authored insights into metadata/inventory.json.",
    )
    enrich.add_argument(
        "--output-dir",
        type=Path,
        required=True,
        help="Migration output directory (reads/writes metadata/inventory.json).",
    )
    enrich.add_argument(
        "--insights-path",
        type=Path,
        default=None,
        help="Path to a JSON file holding the insights object.",
    )
    enrich.add_argument(
        "--insights",
        type=str,
        default=None,
        help="Insights object as an inline JSON string (convenience for direct CLI use).",
    )
```

- [ ] **Step 6: Run tests to verify they pass**

Run: `PYTHONPATH=src uv run pytest tests/unit/test_pipeline_insights.py -k adapter_enrich -v`
Expected: PASS

- [ ] **Step 7: Commit**

```bash
git add src/flowx/adapter/__main__.py tests/unit/test_pipeline_insights.py
git commit -m "$(cat <<'EOF'
Add 'enrich' adapter subcommand

python -m flowx.adapter enrich --output-dir <dir> (--insights-path <file>
| --insights <json>). Validates + merges insights; returns 1 (inventory
untouched) on missing inventory, bad payload, or validation violations.

Co-authored-by: Isaac
EOF
)"
```

---

## Task 6: MCP `enrich` command + `runner.materialize_json`

**Files:**
- Modify: `src/flowx/mcp/runner.py` (add `materialize_json` near `materialize_adf_definitions` ~line 156)
- Modify: `src/flowx/mcp/server.py` (add `_cmd_enrich` before `_COMMANDS` ~line 363; register `"enrich"` in `_COMMANDS` ~line 377; add a docstring bullet ~line 426)
- Test: `tests/unit/test_pipeline_insights.py`

**Interfaces:**
- Consumes: `runner.run_adapter`, `runner.summarize_inventory`, `runner.materialize_json` (new), `runner.cleanup_materialized`, `server._phase_result`.
- Produces: `runner.materialize_json(obj: Any) -> str` (writes a temp JSON file, returns its path; cleaned up by `cleanup_materialized`). `server._cmd_enrich(p)` accepting `output_dir`, `insights` (inline dict) or `insights_path`.

- [ ] **Step 1: Write the failing test**

Add to `tests/unit/test_pipeline_insights.py`:

```python
from flowx.mcp import runner as mcp_runner
from flowx.mcp.server import _cmd_enrich


def test_materialize_json_round_trips(tmp_path: Path):
    path = mcp_runner.materialize_json({"overview": "x"})
    try:
        assert json.loads(Path(path).read_text()) == {"overview": "x"}
    finally:
        mcp_runner.cleanup_materialized(path)
    assert not Path(path).exists()


def test_cmd_enrich_requires_a_payload():
    result = _cmd_enrich({"output_dir": "./flowx_output"})
    assert result["ok"] is False
    assert "insights" in result["error"]


def test_cmd_enrich_inline_dict_success(tmp_path: Path):
    _write_inventory(tmp_path, _inventory())
    result = _cmd_enrich({"output_dir": str(tmp_path), "insights": _good_insights()})
    assert result["ok"] is True
    assert "insights" in json.loads((tmp_path / "metadata" / "inventory.json").read_text())
```

- [ ] **Step 2: Run test to verify it fails**

Run: `PYTHONPATH=src uv run pytest tests/unit/test_pipeline_insights.py -k "materialize_json or cmd_enrich" -v`
Expected: FAIL with `ImportError: cannot import name '_cmd_enrich'` / `AttributeError: module ... has no attribute 'materialize_json'`

- [ ] **Step 3: Add `materialize_json` to the runner**

In `src/flowx/mcp/runner.py`, add after `materialize_adf_definitions` (after line 204):

```python
def materialize_json(obj: Any) -> str:
    """Write a JSON-serialisable object to a temp file and return its path.

    Lets the MCP server pass an inline ``insights`` dict to the adapter's
    ``enrich`` subcommand (which reads from ``--insights-path``). Clean up with
    :func:`cleanup_materialized`.
    """
    fd, path = tempfile.mkstemp(prefix="flowx-insights-", suffix=".json")
    with os.fdopen(fd, "w", encoding="utf-8") as handle:
        json.dump(obj, handle)
    return path
```

(`os`, `json`, `tempfile` are already imported at the top of runner.py — lines 12-18.)

- [ ] **Step 4: Add `_cmd_enrich` to the server**

In `src/flowx/mcp/server.py`, add before `_COMMANDS` (before line 365):

```python
def _cmd_enrich(p: dict[str, Any]) -> dict[str, Any]:
    output_dir = p.get("output_dir", "./flowx_output")
    insights = p.get("insights")
    insights_path = p.get("insights_path")
    if insights is None and not insights_path:
        return {"ok": False, "error": "Provide 'insights' (inline dict) or 'insights_path'."}
    tmp: str | None = None
    try:
        if insights is not None:
            tmp = runner.materialize_json(insights)
            insights_path = tmp
        args: list[Any] = ["enrich", "--output-dir", output_dir, "--insights-path", insights_path]
        result = runner.run_adapter(args)
        out = Path(output_dir)
        return _phase_result(result, out, inventory=runner.summarize_inventory(out))
    finally:
        if tmp:
            runner.cleanup_materialized(tmp)
```

- [ ] **Step 5: Register the command and document it**

In `src/flowx/mcp/server.py`, add to the `_COMMANDS` dict (after line 368, `"convert": _cmd_convert,` grouping — place it right after `"discover": _cmd_discover,`):

```python
    "enrich": _cmd_enrich,
```

Then add a bullet to the `flowx` tool docstring after the "discover" bullet (after line 408):

```python
        - "enrich": output_dir(req), one of insights(inline dict) | insights_path — validate + merge
          agent-authored insights into metadata/inventory.json (returns {ok:false, ...} without writing
          on validation failure).
```

- [ ] **Step 6: Run tests to verify they pass**

Run: `PYTHONPATH=src uv run pytest tests/unit/test_pipeline_insights.py -k "materialize_json or cmd_enrich" -v`
Expected: PASS

- [ ] **Step 7: Commit**

```bash
git add src/flowx/mcp/runner.py src/flowx/mcp/server.py tests/unit/test_pipeline_insights.py
git commit -m "$(cat <<'EOF'
Add 'enrich' MCP command + runner.materialize_json

_cmd_enrich materializes an inline insights dict to a temp file and drives
the adapter enrich subcommand; runner.materialize_json parallels
materialize_adf_definitions. Registered in _COMMANDS and documented.

Co-authored-by: Isaac
EOF
)"
```

---

## Task 7: Optional `has_insights` reporting column

Optional per the spec — include only if it stays trivial and does not perturb existing coverage tests.

**Files:**
- Modify: `src/flowx/reporting/coverage.py` (`COVERAGE_METRIC_COLUMNS` ~line 22; `build_coverage_rows` ~line 57)
- Test: `tests/unit/test_reporting_coverage.py`

**Interfaces:**
- Consumes: the inventory dict already loaded in `build_coverage_rows` at line 73.
- Produces: a `has_insights` boolean field on each coverage row, gated on the top-level `insights` key. Non-enriched inventories yield `False`; the column is identical for every pipeline in a run (it is a factory-level marker).

- [ ] **Step 1: Write the failing test**

Add to `tests/unit/test_reporting_coverage.py` (the module already imports `json`, `Path`, and `build_coverage_rows`, and defines the `_write_metadata(tmp_path) -> Path` helper that writes `metadata/inventory.json` without an `insights` key):

```python
def test_has_insights_column_reflects_insights_key(tmp_path: Path):
    md = _write_metadata(tmp_path)  # writes inventory.json with no insights key
    rows = build_coverage_rows(md)
    assert all(row["has_insights"] is False for row in rows)

    inv_path = md / "inventory.json"
    inv = json.loads(inv_path.read_text())
    inv["insights"] = {"overview": "x", "pipeline_insights": [], "pipeline_relationships": []}
    inv_path.write_text(json.dumps(inv), encoding="utf-8")
    rows2 = build_coverage_rows(md)
    assert all(row["has_insights"] is True for row in rows2)
```

- [ ] **Step 2: Run test to verify it fails**

Run: `PYTHONPATH=src uv run pytest tests/unit/test_reporting_coverage.py::test_has_insights_column_reflects_insights_key -v`
Expected: FAIL with `KeyError: 'has_insights'`

- [ ] **Step 3: Add the column**

In `src/flowx/reporting/coverage.py`, append `"has_insights"` to `COVERAGE_METRIC_COLUMNS` (after `"complexity_size"` at line 36):

```python
    "complexity_size",
    "has_insights",
```

Then, in `build_coverage_rows`, compute the factory-level marker once just before the `rows: list[dict[str, Any]] = []` line (line 82):

```python
    has_insights = "insights" in inventory
```

and add `"has_insights": has_insights,` as the last entry of the per-pipeline dict appended in the loop, immediately after `"complexity_size": csv_row.get("complexity_size", "") or "",` (line 113):

```python
                "complexity_size": csv_row.get("complexity_size", "") or "",
                "has_insights": has_insights,
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `PYTHONPATH=src uv run pytest tests/unit/test_reporting_coverage.py -v`
Expected: PASS. The two existing coverage tests (`test_build_coverage_rows_joins_inventory_and_csv`, `test_build_coverage_rows_full_coverage_and_missing_csv`) assert individual named columns, not an exact column count or the full `COVERAGE_METRIC_COLUMNS` tuple, so adding a column does not break them.

- [ ] **Step 5: Commit**

```bash
git add src/flowx/reporting/coverage.py tests/unit/test_reporting_coverage.py
git commit -m "$(cat <<'EOF'
Add has_insights coverage column (gated on insights key)

Factory-level marker in the coverage rows; False for non-enriched
inventories so existing runs are unaffected.

Co-authored-by: Isaac
EOF
)"
```

---

## Task 8: Discover skill — Step 5 (author → enrich) + summary rewording

**Files:**
- Modify: `skills/flowx-discover/SKILL.md`

**Interfaces:**
- Consumes: the `enrich` MCP command (Task 6) and the `enrich` adapter subcommand (Task 5). Documentation only — no test cycle; verified manually in Task 9.

- [ ] **Step 1: Insert the new Step 5 (author → enrich)**

In `skills/flowx-discover/SKILL.md`, insert this new section between the current Step 4b (ends at line 212) and the current `### Step 5 — Present the summary` (line 214):

````markdown
### Step 5 — Author and merge agentic insights

The deterministic inventory records *what* each pipeline contains; it cannot
record *what the factory is trying to do* or *how the pipelines relate as a
system*. Author that judgment now and merge it into `inventory.json` under an
`insights` key. This always runs.

1. **Read** the just-written `inventory.json` (`pipelines`, `lineage`, `summary`)
   and `profile_report.csv`.
2. **Author** an `insights` object:
   - `overview` — the whole factory as one system, plus the single biggest
     migration steer.
   - `pipeline_insights[]` — **sparse**; per pipeline an `intent` and a
     `databricks_pattern` (optionally `pattern_name`,
     `recommended_databricks_features`, `conversion_notes`). Omit pipelines with
     nothing worth saying.
   - `pipeline_relationships[]` — annotate the lineage edges the discover phase
     already found. Each relationship carries a `lineage_edge`
     (`{edge_type, edge_identity}`) plus `relationship_summary`,
     `databricks_pattern`, and `risk_if_ignored`. For a **control** edge,
     `edge_identity` is the edge's `activity_name`; for a **data** edge, it is
     the edge's `match_key` — copied **verbatim** from a real edge in
     `lineage`.
   - **Authoring rules:** reference only pipeline names that exist in the
     inventory; every `lineage_edge` must echo a real edge; do not invent
     relationships the lineage did not find (annotate, don't rediscover).
3. **Enrich** — merge the object in:

   - **MCP tool path** (inline dict; the only path in Genie Code):

     ```
     flowx(command="enrich", parameters={
       "output_dir": "<output dir>",
       "insights": { ...authored object... }})
     ```

   - **venv CLI fallback** (write the object to a JSON file first):

     ```bash
     "$PY" -m flowx.adapter enrich --output-dir <dir> --insights-path <file>
     ```

4. **On `ok:false`** the tool did **not** write the file: read `violations`, fix
   the offending pipeline name / lineage edge / field, and call `enrich` again.
   On `ok:true` the `insights` key is now merged into `inventory.json`.
````

- [ ] **Step 2: Renumber the existing reporting steps**

Renumber the four existing headings that follow (they currently read Steps 5–8):
- `### Step 5 — Present the summary` → `### Step 6 — Present the summary`
- `### Step 6 — Detail agentic activities` → `### Step 7 — Detail agentic activities`
- `### Step 7 — Warn about unsupported activities` → `### Step 8 — Warn about unsupported activities`
- `### Step 8 — Confirm output location` → `### Step 9 — Confirm output location`

- [ ] **Step 3: Reword the summary step to surface insights**

In the renumbered `### Step 6 — Present the summary`, after the existing summary code block (the `Coverage: 95.7%` block ending at line 230), append:

````markdown
Then surface the authored judgment so the user sees *what the factory does*, not
just coverage numbers: print the factory `overview`, and for each
`pipeline_insights` entry its `pattern_name` / `intent` and recommended
Databricks pattern. Read these back from the enriched `inventory.json`.
````

- [ ] **Step 4: Add a "Future considerations" note**

At the end of the file (after the `## Output Artifacts` table, line 273), append:

````markdown
## Future considerations

Insights are currently authored in a single pass over the whole factory. For very
large factories, revisit partitioning the authoring across subagents keyed on
lineage clusters (the connected components of the combined control/data-edge
graph), so each subagent reasons about one coherent subsystem. Out of scope for
now — always enrich in one pass.
````

- [ ] **Step 5: Verify the skill reads coherently**

Run: `PYTHONPATH=src uv run pytest tests/unit -q` (sanity — no test touches the skill, but confirm nothing regressed)
Read the edited `skills/flowx-discover/SKILL.md` end-to-end and confirm: steps are numbered 1→9 with no duplicates or gaps, the new Step 5 sits between 4b and the summary, and both tool paths are shown.

- [ ] **Step 6: Commit**

```bash
git add skills/flowx-discover/SKILL.md
git commit -m "$(cat <<'EOF'
Add discover Step 5: author + enrich agentic insights

New always-on author->enrich loop (both MCP and venv-CLI paths), summary
step reworded to surface factory overview + per-pipeline intent/pattern,
and a future-considerations note on partitioning authoring by lineage
cluster. Renumbers the reporting steps to 6-9.

Co-authored-by: Isaac
EOF
)"
```

---

## Task 9: End-to-end verification + format/lint gate

**Files:** none (verification only)

**Interfaces:** exercises the shipped `discover` → `enrich` path end-to-end over the repo fixtures.

- [ ] **Step 1: Run the full unit suite**

Run: `PYTHONPATH=src uv run pytest tests/unit -v`
Expected: PASS (all tests, including the new `test_pipeline_insights.py` and the coverage test).

- [ ] **Step 2: Format + lint (ruff + mypy)**

Run: `make fmt`
Expected: ruff formats/fixes cleanly and `mypy src/flowx/` reports no errors. Fix any type errors (e.g. add annotations) and re-run until clean.

- [ ] **Step 3: End-to-end discover → enrich over fixtures**

Run (real CLI, real fixture inventory, temp output dir):

```bash
cd /Users/matthew.moorcroft/Code/work/flowx-worktrees/feat-discover-insights
OUT="$(mktemp -d)"
PYTHONPATH=src uv run python -m flowx.adapter discover \
  --adf-source-path tests/resources/json --output-dir "$OUT"
# capture the deterministic portion before enrich
PYTHONPATH=src uv run python -c "import json,sys; d=json.load(open(sys.argv[1])); print(json.dumps({k:v for k,v in d.items() if k!='insights'}, indent=2))" "$OUT/metadata/inventory.json" > /tmp/before.json
# author a tiny valid insights object referencing a real pipeline + control edge, then enrich
PYTHONPATH=src uv run python -m flowx.adapter enrich --output-dir "$OUT" --insights '{"overview":"fixtures","pipeline_insights":[{"pipeline":"pipeline_execute_pipeline_nested","intent":"orchestrate"}],"pipeline_relationships":[{"from_pipeline":"pipeline_execute_pipeline_nested","to_pipeline":"pipeline_copy_sql_to_delta","lineage_edge":{"edge_type":"control","edge_identity":"Run Ingestion Pipeline"}}]}'
# confirm insights present AND deterministic portion byte-identical
PYTHONPATH=src uv run python -c "import json,sys; d=json.load(open(sys.argv[1])); assert 'insights' in d; print('insights present:', bool(d['insights']))" "$OUT/metadata/inventory.json"
PYTHONPATH=src uv run python -c "import json,sys; d=json.load(open(sys.argv[1])); print(json.dumps({k:v for k,v in d.items() if k!='insights'}, indent=2))" "$OUT/metadata/inventory.json" > /tmp/after.json
diff /tmp/before.json /tmp/after.json && echo "DETERMINISTIC PORTION BYTE-IDENTICAL"
rm -rf "$OUT" /tmp/before.json /tmp/after.json
```

Expected: the enrich prints `Enriched inventory: 1 pipeline insight(s), 1 relationship(s).`, `insights present: True`, and `diff` reports no differences (`DETERMINISTIC PORTION BYTE-IDENTICAL`).

- [ ] **Step 4: Confidentiality grep**

Run: `git grep -nE "a customer factory|a large factory|engagementID|engagementDBVersions|etl-parameters" -- ':!docs/superpowers/specs/'`
Expected: no output (zero hits outside the spec).

- [ ] **Step 5: Final commit (only if Steps 2/3 required fixups)**

```bash
git add -A
git commit -m "$(cat <<'EOF'
Format/lint fixups for discover insights

Co-authored-by: Isaac
EOF
)"
```

---

## Self-Review

**1. Spec coverage** (checked against `docs/superpowers/specs/2026-07-23-discover-insights-design.md`):
- §4 data models → Task 1. §5 parser (`load_insights`/`validate_insights`/`merge_into_inventory`/`enrich_inventory`) → Tasks 2–3. §6a adapter subcommand → Task 5. §6b MCP command + `materialize_json` → Task 6. §7 skill Step 5 + summary + future note → Task 8. §8 test matrix → Tasks 2–6 (each issue invariant maps to a named test); edge-binding on the real fixture → Task 4; optional reporting column → Task 7; end-to-end → Task 9. §3 resolved decisions (data-edge on `match_key`, no `schema_version`, always-enrich, validate-before-write, byte-identical) are enforced in Global Constraints + Tasks 3/8. §10 confidentiality → Global Constraints + Task 9 Step 4.
- Every §8 test row has a home: `test_validator_accepts_good_insights` (T2), `test_rejects_pipeline_not_in_inventory` (T2), `test_rejects_unresolvable_*_edge` (T2), `test_rejects_missing_required_field` (T2), `test_rejects_unknown_field` (T2), `test_control_edge_binding_matches_and_rejects` (T4), `test_data_edge_binds_on_match_key` (T2), `test_two_pass_deterministic_keys_byte_identical` (T3), `test_enrich_is_idempotent` (T3), `test_validation_failure_does_not_write` (T3).

**2. Placeholder scan:** No "TBD"/"handle edge cases"/"similar to Task N" — every code step shows complete code. Task 7 anchors its edits to exact line numbers and the real `_write_metadata` helper (verified present in `test_reporting_coverage.py`), so no "inspect at implementation time" hand-waving remains.

**3. Type consistency:** `enrich_inventory(output_dir, *, insights=None, insights_path=None) -> dict` used identically in Tasks 3, 5, 6. Return keys `ok`/`violations`/`pipeline_insights`/`relationships` consistent across Task 3 (definition), Task 5 (`_run_enrich` reads `result["ok"]`, `result["violations"]`, `result["pipeline_insights"]`, `result["relationships"]`). `validate_insights(raw, inventory) -> list[str]` consistent across Tasks 2, 3, 4. `materialize_json(obj) -> str` / `cleanup_materialized(path)` consistent in Task 6. `LineageEdgeRef`/`edge_type`/`edge_identity` naming consistent between Task 1 models and the validator's `_EDGE_KEYS` in Task 2.
