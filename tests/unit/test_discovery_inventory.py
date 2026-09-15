"""Tests for the source-agnostic inventory emitter (:mod:`flowx.discovery_inventory`).

These tests build the shared discovery AST by hand -- no ADF, no Airflow -- so
they prove the emitter is genuinely source-agnostic: it takes ``SourceGraph``
objects in and projects the ``inventory.json`` document out, with no coupling to
any particular front-end.
"""

from __future__ import annotations

import ast

import flowx.discovery_inventory as discovery_inventory
from flowx.discovery_inventory import STRATEGY_PROPERTY, build_source_inventory
from flowx.models.discovery import (
    CONCEPT_BRANCH,
    CONCEPT_NOTEBOOK,
    ContainerNode,
    SourceDependency,
    SourceGraph,
    SourceNode,
)


def _node(task_key: str, native_type: str, strategy: str, *, deps: list[SourceDependency] | None = None) -> SourceNode:
    return SourceNode(
        source_id=task_key,
        task_key=task_key,
        concept=CONCEPT_NOTEBOOK,
        source="unit",
        name=task_key,
        native_type=native_type,
        dependencies=deps or [],
        properties={STRATEGY_PROPERTY: strategy},
        raw={"name": task_key, "type": native_type},
    )


def test_emitter_has_no_source_specific_imports() -> None:
    """The emitter module must not import any per-source package.

    Source-agnostic means the ADF/Airflow loaders depend on the emitter, never
    the other way round. Guard that by inspecting the module's actual import
    statements (not arbitrary text -- the docstring legitimately names the
    ``"adf"`` / ``"airflow"`` discriminator values).
    """
    source = (discovery_inventory.__file__ or "").rstrip("c")
    with open(source, encoding="utf-8") as handle:
        tree = ast.parse(handle.read())

    imported: list[str] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imported.extend(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            imported.append(node.module)

    assert not any(name.startswith("flowx.sources") for name in imported), imported


def test_top_level_shape_and_summary_counts() -> None:
    """A hand-built graph projects to the canonical top-level shape and counts."""
    graph = SourceGraph(
        name="g1",
        source="unit",
        tasks=[
            _node("n1", "Notebook", "deterministic"),
            _node("n2", "DataFlow", "agentic"),
            _node("n3", "Mystery", "unsupported"),
        ],
    )

    inventory = build_source_inventory([graph], source="unit", source_dir="/tmp/src")

    assert sorted(inventory.keys()) == ["pipelines", "source", "source_dir", "summary"]
    assert inventory["source"] == "unit"
    assert inventory["source_dir"] == "/tmp/src"
    assert inventory["summary"] == {
        "pipeline_count": 1,
        "activity_count": 3,
        "deterministic_count": 1,
        "agentic_count": 1,
        "unsupported_count": 1,
        "coverage_pct": 66.7,
    }


def test_activity_entry_carries_legacy_and_additive_fields() -> None:
    """Each activity keeps the legacy keys and gains the additive standardized ones."""
    graph = SourceGraph(
        name="g1",
        source="unit",
        tasks=[
            _node("a", "Notebook", "deterministic"),
            _node(
                "b",
                "Copy",
                "deterministic",
                deps=[SourceDependency(upstream="a", conditions=["Succeeded", "Skipped"])],
            ),
        ],
    )

    inventory = build_source_inventory([graph], source="unit", source_dir="/tmp/src")
    entries = {entry["name"]: entry for entry in inventory["pipelines"][0]["activities"]}

    # Legacy keys (byte-compatible with the historical shape).
    assert entries["a"]["type"] == "Notebook"
    assert entries["a"]["strategy"] == "deterministic"
    assert "depends_on" not in entries["a"]  # no deps -> key omitted, as before
    assert entries["b"]["depends_on"] == ["a"]

    # Additive standardized fields.
    assert entries["a"]["original_type"] == "Notebook"
    assert entries["a"]["dependencies"] == []
    assert entries["a"]["raw"] == {"name": "a", "type": "Notebook"}
    assert entries["b"]["dependencies"] == [{"upstream": "a", "conditions": ["Succeeded", "Skipped"], "resolved": True}]


def test_container_branches_are_flattened_in_source_order() -> None:
    """Container children are flattened depth-first in branch declaration order."""
    branch_true = _node("t", "Notebook", "deterministic")
    branch_false = _node("f", "Notebook", "deterministic")
    container = ContainerNode(
        source_id="if",
        task_key="if",
        concept=CONCEPT_BRANCH,
        source="unit",
        name="if",
        native_type="IfCondition",
        properties={STRATEGY_PROPERTY: "deterministic"},
        branches={"true": [branch_true], "false": [branch_false]},
    )
    graph = SourceGraph(name="g", source="unit", tasks=[container, _node("after", "Notebook", "deterministic")])

    inventory = build_source_inventory([graph], source="unit", source_dir="/tmp/src")
    names = [entry["name"] for entry in inventory["pipelines"][0]["activities"]]

    assert names == ["if", "t", "f", "after"]
    assert inventory["summary"]["activity_count"] == 4


def test_include_empty_pipelines_toggle_preserves_membership_semantics() -> None:
    """Zero-activity graphs stay counted in summary but drop from the listing when asked.

    Reproduces ADF's long-standing rule: a pipeline with no activities is omitted
    from ``pipelines`` yet still counted in ``summary.pipeline_count``.
    """
    empty = SourceGraph(name="empty", source="unit", tasks=[])
    populated = SourceGraph(name="full", source="unit", tasks=[_node("n", "Notebook", "deterministic")])

    omitted = build_source_inventory(
        [empty, populated], source="unit", source_dir="/tmp", include_empty_pipelines=False
    )
    assert [p["name"] for p in omitted["pipelines"]] == ["full"]
    assert omitted["summary"]["pipeline_count"] == 2  # empty still counted

    listed = build_source_inventory([empty, populated], source="unit", source_dir="/tmp", include_empty_pipelines=True)
    assert [p["name"] for p in listed["pipelines"]] == ["empty", "full"]
    assert listed["summary"]["pipeline_count"] == 2


def test_empty_input_yields_zero_coverage() -> None:
    """No graphs -> empty listing, zeroed summary, 0.0 coverage (no divide-by-zero)."""
    inventory = build_source_inventory([], source="unit", source_dir="/tmp")
    assert inventory["pipelines"] == []
    assert inventory["summary"]["coverage_pct"] == 0.0
    assert inventory["summary"]["pipeline_count"] == 0
