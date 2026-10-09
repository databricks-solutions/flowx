"""The routing review page: one standard HTML view, the same eight sections in the same order every run.

Route writes ``metadata/routing_review.html`` from the recorded plan, the inventory's insights and the
report's routing record. It escapes every authored value, carries no timestamp (the same inputs give
the same bytes), and says per unit what package will do.
"""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any

import pytest

from flowx.adapter.__main__ import main as adapter_cli_main
from flowx.discovery_inventory import STRATEGY_PROPERTY, build_source_inventory
from flowx.discovery_serde import canonical_sha256
from flowx.models.conversion_plan import ComponentPlan, ConversationEntry, ConversionPlan, SuggestedGrouping
from flowx.models.discovery import CONCEPT_NOTEBOOK, SourceGraph, SourceNode
from flowx.reporting.routing_review import render_routing_review

_SECTIONS = [
    "1. Summary",
    "2. What the source does",
    "3. Components",
    "4. Suggested groupings",
    "5. How it will be converted",
    "6. Routing conversation",
    "7. Findings",
    "8. How to steer",
]


def _plan(**overrides: Any) -> ConversionPlan:
    fields: dict[str, Any] = {
        "inventory_sha256": "inv",
        "components": [
            ComponentPlan(component_id="component-1", members=["extract"], recommended="agentic", decision=None),
            ComponentPlan(
                component_id="component-2", members=["mart"], recommended="deterministic", decision="agentic"
            ),
            ComponentPlan(component_id="component-3", members=["solo"], decision="deterministic"),
        ],
    }
    fields.update(overrides)
    return ConversionPlan(**fields)


def _headings(page: str) -> list[str]:
    return re.findall(r"<h2>(.*?)</h2>", page)


def test_every_page_has_the_same_eight_sections_in_order() -> None:
    assert _headings(render_routing_review(_plan(), None, None)) == _SECTIONS
    assert _headings(render_routing_review(ConversionPlan(), {"pipelines": []}, None)) == _SECTIONS


def test_authored_text_is_escaped() -> None:
    plan = _plan(
        conversation=[ConversationEntry(question="<script>q</script>", answer="a & <b>")],
        components=[
            ComponentPlan(component_id="component-1", members=["x"], decision="agentic", rationale="<img src=x>")
        ],
    )
    inventory = {"insights": {"overview": "<script>alert(1)</script>"}}

    page = render_routing_review(plan, inventory, None)

    assert "<script>" not in page and "<img" not in page
    assert "&lt;script&gt;alert(1)&lt;/script&gt;" in page
    assert "a &amp; &lt;b&gt;" in page


def test_the_same_inputs_render_the_same_bytes() -> None:
    assert render_routing_review(_plan(), None, None) == render_routing_review(_plan(), None, None)


def test_each_unit_says_what_package_will_do() -> None:
    output_sha256 = canonical_sha256(["authored"])
    record = {
        "components": {"component-2": {"outcome": "agentic-applied", "output_sha256": output_sha256}},
    }
    conversion = render_routing_review(_plan(), None, record).split('id="conversion"')[1].split("</section>")[0]

    assert "Decision pending" in conversion
    assert f"<code>{output_sha256[:12]}</code>" in conversion
    assert "Converted 1:1 by the deterministic engine" in conversion
    waiting = render_routing_review(_plan(), None, None).split('id="conversion"')[1].split("</section>")[0]
    assert "package refuses until it is filled" in waiting


def test_a_grouping_shows_its_basis_and_whether_it_is_accepted() -> None:
    grouping = SuggestedGrouping(
        grouping_id="grouping-1",
        components=["component-1", "component-2"],
        members=["extract", "mart"],
        basis=[{"kind": "shared_pattern", "pattern": "Lakeflow Connect", "pipelines": ["extract", "mart"]}],
        accepted=True,
    )

    page = render_routing_review(_plan(suggested_groupings=[grouping]), None, None)

    groupings = page.split('id="groupings"')[1].split("</section>")[0]
    assert "grouping-1" in groupings and "Lakeflow Connect" in groupings and ">accepted<" in groupings
    conversion = page.split('id="conversion"')[1].split("</section>")[0]
    assert "grouping-1 (component-1, component-2)" in conversion


@pytest.mark.parametrize(("inventory", "shown"), [({"source": "adf"}, "adf"), ({}, "&mdash;")])
def test_the_summary_shows_the_recorded_source_or_a_dash(inventory: dict[str, Any], shown: str) -> None:
    summary = render_routing_review(_plan(), inventory, None).split('id="summary"')[1].split("</section>")[0]

    assert re.findall(r"<th>Source</th><td>(.*?)</td>", summary) == [shown]


def test_without_insights_the_page_says_enrich_has_not_run() -> None:
    assert "Enrich has not run" in render_routing_review(_plan(), None, None)


def test_route_writes_the_review_page_beside_the_plan(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    node = SourceNode(
        source_id="load",
        task_key="load",
        concept=CONCEPT_NOTEBOOK,
        source="adf",
        name="load",
        native_type="DatabricksNotebook",
        properties={STRATEGY_PROPERTY: "deterministic"},
    )
    inventory = build_source_inventory(
        [SourceGraph(name="solo", source="adf", tasks=[node])], source="adf", source_dir="/src"
    )
    (tmp_path / "metadata").mkdir()
    (tmp_path / "metadata" / "inventory.json").write_text(json.dumps(inventory), encoding="utf-8")

    assert adapter_cli_main(["route", "--output-dir", str(tmp_path)]) == 0

    payload = json.loads(capsys.readouterr().out)
    page = (tmp_path / "metadata" / "routing_review.html").read_text(encoding="utf-8")
    assert payload["review_path"] == str(tmp_path / "metadata" / "routing_review.html")
    assert _headings(page) == _SECTIONS and "solo" in page
