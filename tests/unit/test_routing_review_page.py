"""The routing review page: one short, standard view for a decision maker, the same eight sections every run.

Route writes ``metadata/routing_review.html`` from the recorded plan, the inventory's insights and the
report's routing record. It leads with what needs deciding, folds technical detail away, escapes every
authored value, and carries no timestamp (the same inputs give the same bytes).
"""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any

import pytest

from flowx.adapter.__main__ import main as adapter_cli_main
from flowx.discovery_inventory import STRATEGY_PROPERTY, build_source_inventory
from flowx.models.conversion_plan import ComponentPlan, ConversationEntry, ConversionPlan, SuggestedGrouping
from flowx.models.discovery import CONCEPT_NOTEBOOK, SourceGraph, SourceNode
from flowx.reporting.routing_review import render_routing_review

_SECTIONS = [
    "At a glance",
    "What the source does",
    "Recommended conversion",
    "Suggested groupings",
    "What happens next",
    "What you told us",
    "Notes",
    "How to change a decision",
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


def _section(page: str, section_id: str) -> str:
    return page.split(f'id="{section_id}"')[1].split("</section>")[0]


def test_the_summary_leads_with_the_counts_and_what_still_needs_deciding() -> None:
    summary = _section(render_routing_review(_plan(), {"pipelines": [{}, {}, {}]}, None), "summary")

    assert (
        "3 pipelines in 3 groups. 1 recommended for deterministic conversion; 1 is recommended for agentic redesign."
        in summary
    )
    assert "1 of 3 groups still need a decision. Nothing is converted until every group has one." in summary
    assert '<div class="tile alert"><b>1</b><span>Decisions pending</span></div>' in summary
    assert '<div class="tile"><b>1</b><span>Recommended deterministic</span></div>' in summary


def test_a_deterministic_recommendation_with_gaps_is_not_called_automatic() -> None:
    """A non-ADF component is recommended deterministic even with gaps; the page says those stay gaps to fill."""
    with_gaps = ComponentPlan(
        component_id="component-1",
        members=["dag"],
        recommended="deterministic",
        options={"deterministic": {"capable": False, "uncovered": [{"type": "Until"}, {"type": "Until"}]}},
    )
    page = render_routing_review(_plan(components=[with_gaps]), None, None)

    summary, components = _section(page, "summary"), _section(page, "components")
    assert "1 recommended for deterministic conversion" in summary
    assert "automatic" not in summary + components
    assert "<summary>Recommended deterministic (1, 1 pending)</summary>" in components
    assert "translates these 1:1; any activity listed under Why is left as a gap to fill." in components
    assert "2 activities can&#x27;t convert 1:1 (Until)." in components


def _recorded(members: list[str], decision: str, outcome: str) -> dict[str, Any]:
    """One unit's routing-record entry, as route writes it."""
    return {"members": members, "decision": decision, "outcome": outcome}


def test_what_happens_next_counts_each_state_and_says_what_blocks_packaging() -> None:
    record = {
        "components": {
            "component-1": _recorded(["extract"], "agentic", "agentic-not-viable"),
            "component-2": {**_recorded(["mart"], "agentic", "agentic-applied"), "output_sha256": "abc"},
            "component-3": _recorded(["solo"], "deterministic", "deterministic"),
        }
    }
    decided = _plan(
        components=[
            ComponentPlan(component_id="component-1", members=["extract"], decision="agentic"),
            ComponentPlan(component_id="component-2", members=["mart"], decision="agentic"),
            ComponentPlan(component_id="component-3", members=["solo"], decision="deterministic"),
        ]
    )

    pending = _section(render_routing_review(_plan(), None, record), "conversion")
    waiting = _section(render_routing_review(decided, None, record), "conversion")

    assert "The plan is not applied yet. Decide the pending groups, then run route again." in pending
    assert "Packaging is blocked until the agent builds every agentic unit." in waiting
    assert "<strong>component-1</strong>: the agent builds extract with fill-agentic." in waiting
    assert '<span class="pill pill-agt">1</span><span>Agent build ready</span>' in waiting
    assert '<span class="pill pill-det">1</span><span>Converts automatically</span>' in waiting


def test_a_decision_route_has_not_applied_says_to_run_route_again_not_to_wait_for_the_agent() -> None:
    """A unit decided since the last route needs route, not the agent's build, so it gets its own state."""
    decided = _plan(
        components=[
            ComponentPlan(component_id="component-1", members=["extract"], decision="agentic"),
            ComponentPlan(component_id="component-2", members=["mart"], decision="agentic"),
            ComponentPlan(component_id="component-3", members=["solo"], decision="deterministic"),
        ]
    )
    record = {
        "components": {
            "component-1": _recorded(["extract"], "deterministic", "deterministic"),
            "component-2": _recorded(["mart"], "agentic", "agentic-not-viable"),
            "component-3": _recorded(["solo"], "agentic", "agentic-applied"),
        }
    }

    conversion = _section(render_routing_review(decided, None, record), "conversion")
    never_routed = _section(render_routing_review(decided, None, None), "conversion")

    assert '<span class="pill pill-pend">2</span><span>Decided, not applied yet</span>' in conversion
    assert "Some decisions are not applied yet. Run route again to apply them." in conversion
    assert "<strong>component-2</strong>: the agent builds mart with fill-agentic." in conversion
    assert "<strong>component-1</strong>" not in conversion
    assert '<span class="pill pill-pend">2</span><span>Decided, not applied yet</span>' in never_routed
    assert '<span class="pill pill-det">1</span><span>Converts automatically</span>' in never_routed


@pytest.mark.parametrize("members", [7, "extract", [7]], ids=["number", "string", "list-of-numbers"])
def test_a_hand_edited_record_entry_reads_as_not_applied(members: Any) -> None:
    decided = _plan(components=[ComponentPlan(component_id="component-1", members=["extract"], decision="agentic")])
    record = {"components": {"component-1": {"members": members, "decision": "agentic", "outcome": "agentic-applied"}}}

    conversion = _section(render_routing_review(decided, None, record), "conversion")

    assert '<span class="pill pill-pend">1</span><span>Decided, not applied yet</span>' in conversion


def test_an_accepted_grouping_over_a_pending_component_still_reads_as_pending() -> None:
    """Route refuses to apply while any component is pending, so the grouping says to decide, not to re-run."""
    grouping = SuggestedGrouping(
        grouping_id="grouping-1",
        components=["component-1", "component-2"],
        members=["extract", "mart"],
        accepted=True,
    )
    plan = _plan(
        components=[
            ComponentPlan(component_id="component-1", members=["extract"], decision=None),
            ComponentPlan(component_id="component-2", members=["mart"], decision="agentic"),
        ],
        suggested_groupings=[grouping],
    )

    conversion = _section(render_routing_review(plan, None, None), "conversion")

    assert '<span class="pill pill-pend">1</span><span>Decision pending</span>' in conversion
    assert '<span class="pill pill-pend">0</span><span>Decided, not applied yet</span>' in conversion
    assert "The plan is not applied yet. Decide the pending groups, then run route again." in conversion
    assert "Some decisions are not applied yet." not in conversion


def test_a_grouping_says_why_and_whether_it_is_accepted() -> None:
    grouping = SuggestedGrouping(
        grouping_id="grouping-1",
        components=["component-1", "component-2"],
        members=["extract", "mart"],
        basis=[{"kind": "shared_pattern", "pattern": "Lakeflow Connect", "pipelines": ["extract", "mart"]}],
        accepted=True,
    )

    page = render_routing_review(_plan(suggested_groupings=[grouping]), None, None)

    groupings = _section(page, "groupings")
    assert "Convert 2 pipelines together" in groupings and "Accepted" in groupings
    assert "Both are candidates for Lakeflow Connect." in groupings
    assert "Converts as one unit with component-2." in _section(page, "components")


@pytest.mark.parametrize(("inventory", "shown"), [({"source": "adf"}, "Source: Azure Data Factory."), ({}, None)])
def test_the_header_names_the_recorded_source_and_never_assumes_adf(inventory: dict[str, Any], shown: str) -> None:
    header = render_routing_review(_plan(), inventory, None).split("<main>")[0]

    if shown is None:
        assert "Source:" not in header and "Azure Data Factory" not in header
    else:
        assert shown in header


def test_notes_count_unresolved_calls_in_plain_words_and_keep_routes_wording() -> None:
    finding = "unresolved control edge 'a' -> 'b' (via 'Call B') kept as a finding, not severed; 'a' is routed alone"

    notes = _section(render_routing_review(_plan(findings=[finding, "another note"]), None, None), "findings")

    assert "1 pipeline call points to a pipeline that isn't in this export." in notes
    assert "another note" in notes
    assert "Route's wording" in notes and "unresolved control edge" in notes


def test_technical_details_are_folded_away_with_the_binding_hashes() -> None:
    page = render_routing_review(_plan(), None, None)

    footer = page.split("<footer>")[1]
    assert "<summary>Technical details</summary>" in footer and "<code>inv</code>" in footer


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
