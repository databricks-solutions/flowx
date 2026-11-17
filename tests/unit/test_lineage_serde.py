"""Tests for the lineage substrate's own serialiser (:mod:`flowx.lineage_serde`)."""

from __future__ import annotations

import ast
from pathlib import Path

import pytest

from flowx import ir_serde, lineage_serde
from flowx.models.lineage import ControlEdge, DataAsset, DataEdge, Lineage, MotifAnnotation

SOURCE_ROOT = Path(__file__).resolve().parents[2] / "src" / "flowx"


def _imported_modules(path: Path) -> set[str]:
    """Every module *path* imports from, read from its syntax tree."""
    tree = ast.parse(path.read_text(encoding="utf-8"))
    imported: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom) and node.module:
            imported.add(node.module)
        elif isinstance(node, ast.Import):
            imported.update(alias.name for alias in node.names)
    return imported


@pytest.mark.parametrize("module", ["discovery_serde.py", "discovery_inventory.py", "lineage_serde.py"])
def test_discovery_and_lineage_serialisers_do_not_depend_on_the_ir_serialiser(module: str) -> None:
    """Discovery reads and writes lineage through lineage_serde, not through the IR's serialiser."""
    assert "flowx.ir_serde" not in _imported_modules(SOURCE_ROOT / module)


@pytest.mark.parametrize("name", ["data_asset_to_dict", "data_asset_from_dict", "lineage_to_dict", "lineage_from_dict"])
def test_ir_serde_still_offers_the_lineage_serialisers_it_used_to_define(name: str) -> None:
    """Existing ``from flowx.ir_serde import ...`` imports get the very same functions."""
    assert getattr(ir_serde, name) is getattr(lineage_serde, name)


def test_a_lineage_block_round_trips_through_lineage_serde() -> None:
    lineage = Lineage(
        control_edges=[ControlEdge(source_workflow="parent", target_workflow="child", via_task_key="run")],
        data_edges=[
            DataEdge(
                source_task_key="write",
                target_task_key="read",
                match_kind="identity",
                match_key="srv/dbo.Orders",
                identity="srv/dbo.Orders",
            )
        ],
        motifs=[MotifAnnotation(motif_id="m", member_task_keys=["write", "read"], source_type_hint="Copy")],
    )
    asset = DataAsset(signature="srv/dbo.Orders", identity="srv/dbo.Orders", asset_type="table")

    assert lineage_serde.lineage_from_dict(lineage_serde.lineage_to_dict(lineage)) == lineage
    assert lineage_serde.data_asset_from_dict(lineage_serde.data_asset_to_dict(asset)) == asset
