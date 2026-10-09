"""The routing review page: one standard HTML view of how a factory will be converted.

Route writes ``metadata/routing_review.html`` from the recorded ``conversion_plan.json``, the
inventory's insights, the report's routing record and any accepted grouping route dropped on this run
because it is no longer suggested, so every run presents the same eight sections in
the same order: a summary, what the source does, the components, the suggested groupings, how each
unit will be converted, the routing conversation, the findings, and how to steer. The page is filled
from :data:`routing_review_template.html <_TEMPLATE_PATH>` with the standard library only (no script
or external asset, so it opens offline and in the workspace file viewer), carries no timestamp, and
every value from the plan or insights is HTML-escaped. The same inputs always give the same bytes.

The page is generated and never read back: the user steers by editing ``conversion_plan.json`` (or
passing a plan) and running route again.
"""

from __future__ import annotations

import json
from html import escape
from pathlib import Path
from string import Template
from typing import Any

from flowx.models.conversion_plan import DECISION_AGENTIC, DECISION_DETERMINISTIC, ConversionPlan

_TEMPLATE_PATH = Path(__file__).with_name("routing_review_template.html")

# Where the page is written, beside the plan it presents.
REVIEW_FILENAME = "routing_review.html"


def render_routing_review(
    plan: ConversionPlan,
    inventory: dict[str, Any] | None,
    record: dict[str, Any] | None,
    *,
    dropped_groupings: list[dict[str, Any]] | None = None,
) -> str:
    """Render the routing review page for a recorded plan.

    Args:
        plan: The recorded conversion plan.
        inventory: The discover ``inventory.json`` (its ``insights`` block describes the source), or
            ``None`` when it cannot be read.
        record: The live report's routing record (each unit's outcome), or ``None`` before route has
            applied anything.
        dropped_groupings: The accepted groupings route dropped on this run because they are no longer
            suggested (see :func:`flowx.routing.carried_forward_plan`).

    Returns:
        The complete HTML document.
    """
    from flowx.route_agentic import routing_units

    insights = (inventory or {}).get("insights")
    insights = insights if isinstance(insights, dict) else None
    units = routing_units(plan.to_dict())
    outcomes = record.get("components") if isinstance(record, dict) else None
    outcomes = outcomes if isinstance(outcomes, dict) else {}
    template = Template(_TEMPLATE_PATH.read_text(encoding="utf-8"))
    return template.substitute(
        summary=_summary(plan, inventory, units),
        source=_source(insights),
        components=_components(plan, insights),
        groupings=_groupings(plan, dropped_groupings or []),
        conversion=_conversion(units, outcomes),
        conversation=_conversation(plan),
        findings=_list(plan.findings, empty="No findings."),
        steer=_steer(),
    )


def write_routing_review(output_dir: Path, *, dropped_groupings: list[dict[str, Any]] | None = None) -> Path | None:
    """Write ``metadata/routing_review.html`` for the recorded plan; ``None`` when there is no plan.

    Reads the plan, the inventory and the live report's routing record from ``output_dir``. A report
    or inventory that cannot be read is shown as not yet routed or without insights.
    ``dropped_groupings`` are the accepted groupings route dropped on this run.
    """
    from flowx.route_agentic import REPORT_FILENAME, WORK_DIRNAME, routing_record

    plan = ConversionPlan.load(Path(output_dir))
    if plan is None:
        return None
    metadata_dir = Path(output_dir) / "metadata"
    inventory = _read_json(metadata_dir / "inventory.json")
    record = routing_record(_read_json(Path(output_dir) / WORK_DIRNAME / REPORT_FILENAME))
    path = metadata_dir / REVIEW_FILENAME
    page = render_routing_review(
        plan, inventory if isinstance(inventory, dict) else None, record, dropped_groupings=dropped_groupings
    )
    path.write_text(page, "utf-8")
    return path


def _read_json(path: Path) -> Any:
    """The parsed JSON at ``path``, or ``None`` when it is missing or unreadable."""
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None


def _text(value: Any) -> str:
    """Escape any value for HTML; ``None`` shows as a dash."""
    return "&mdash;" if value is None else escape(str(value))


def _tag(label: str, css_class: str) -> str:
    """A small coloured label."""
    return f'<span class="tag {css_class}">{escape(label)}</span>'


def _decision_tag(decision: str | None) -> str:
    """The label for a decision, pending included."""
    if decision is None:
        return _tag("pending", "pending")
    return _tag(decision, "agentic" if decision == DECISION_AGENTIC else "deterministic")


def _rows(rows: list[tuple[str, str]]) -> str:
    """A two-column table of already-escaped cells."""
    body = "".join(f"<tr><th>{escape(label)}</th><td>{cell}</td></tr>" for label, cell in rows)
    return f"<table>{body}</table>"


def _list(items: list[Any], *, empty: str) -> str:
    """An escaped bullet list, or a muted note when there is nothing to list."""
    if not items:
        return f'<p class="muted">{escape(empty)}</p>'
    return "<ul>" + "".join(f"<li>{_text(item)}</li>" for item in items) + "</ul>"


def _summary(plan: ConversionPlan, inventory: dict[str, Any] | None, units: dict[str, dict[str, Any]]) -> str:
    """Section 1: counts and the hashes the plan is bound to."""
    pipelines = (inventory or {}).get("pipelines")
    decisions = [component.decision for component in plan.components]
    accepted = [grouping.grouping_id for grouping in plan.suggested_groupings if grouping.accepted]
    return _rows(
        [
            ("Source", _text((inventory or {}).get("source"))),
            ("Pipelines", _text(len(pipelines) if isinstance(pipelines, list) else None)),
            ("Components", _text(len(plan.components))),
            ("Suggested groupings", _text(f"{len(plan.suggested_groupings)} ({len(accepted)} accepted)")),
            ("Routing units", _text(len(units))),
            (
                "Decisions",
                _text(
                    f"{decisions.count(DECISION_DETERMINISTIC)} deterministic, {decisions.count(DECISION_AGENTIC)} "
                    f"agentic, {decisions.count(None)} pending"
                ),
            ),
            ("Inventory", f"<code>{_text(plan.inventory_sha256)}</code>"),
            ("Source graphs", f"<code>{_text(plan.source_graphs_sha256)}</code>"),
            ("Agentic insights", f"<code>{_text(plan.agentic_insights_sha256)}</code>"),
        ]
    )


def _patterns(patterns: Any) -> str:
    """A list of recommended patterns with their fit and simplification flag."""
    items = []
    for pattern in patterns if isinstance(patterns, list) else []:
        if not isinstance(pattern, dict):
            continue
        flag = " " + _tag("simplification", "agentic") if pattern.get("simplification_pattern") else ""
        where = f' <span class="muted">({_text(pattern["pipeline"])})</span>' if pattern.get("pipeline") else ""
        items.append(
            f"<li><strong>{_text(pattern.get('pattern'))}</strong>{flag}{where}: {_text(pattern.get('fit'))}</li>"
        )
    return "<ul>" + "".join(items) + "</ul>" if items else '<span class="muted">none</span>'


def _source(insights: dict[str, Any] | None) -> str:
    """Section 2: the insights overview and the factory-wide recommendation."""
    if insights is None:
        return '<p class="muted">Enrich has not run, so there are no insights about what the source does.</p>'
    system = insights.get("system_recommendation")
    system = system if isinstance(system, dict) else {}
    cascade = system.get("cascade")
    return _rows(
        [
            ("Overview", _text(insights.get("overview"))),
            ("Headline", _text(system.get("headline"))),
            ("Recommended architecture", _patterns(system.get("recommended_patterns"))),
            ("What it collapses", _list(cascade if isinstance(cascade, list) else [], empty="Nothing stated.")),
            ("Deciding question", _text(system.get("decision_driver"))),
        ]
    )


def _components(plan: ConversionPlan, insights: dict[str, Any] | None) -> str:
    """Section 3: one card per component, always with the same fields."""
    intents: dict[str, Any] = {}
    for entry in (insights or {}).get("pipeline_insights") or []:
        if isinstance(entry, dict) and entry.get("pipeline") is not None:
            intents[str(entry["pipeline"])] = entry.get("intent")
    cards = []
    for component in plan.components:
        options = component.options or {}
        deterministic = options.get("deterministic") or {}
        agentic = options.get("agentic") or {}
        members = (
            "<ul>"
            + "".join(
                f"<li><code>{_text(member)}</code>: {_text(intents.get(member))}</li>" for member in component.members
            )
            + "</ul>"
        )
        uncovered = [
            f"{entry.get('pipeline')} / {entry.get('activity')} ({entry.get('type')})"
            for entry in deterministic.get("uncovered") or []
            if isinstance(entry, dict)
        ]
        counts = deterministic.get("activity_counts") or {}
        disclosures = [
            entry.get("message") for entry in agentic.get("release_disclosures") or [] if isinstance(entry, dict)
        ]
        rows = [
            ("Members and intent", members),
            ("Recommended", _decision_tag(component.recommended)),
            ("Decision", _decision_tag(component.decision)),
            ("Deterministic: fully capable", _text("yes" if deterministic.get("capable") else "no")),
            (
                "Deterministic: activities",
                _text(", ".join(f"{count} {bucket}" for bucket, count in counts.items()) or None),
            ),
            ("Deterministic: motifs", _list(list(deterministic.get("motifs") or []), empty="none")),
            ("Deterministic: not covered", _list(uncovered, empty="none")),
            ("Agentic: recommended patterns", _patterns(agentic.get("recommended_patterns"))),
            ("Agentic: release disclosures", _list(disclosures, empty="none")),
            ("Rationale", _text(component.rationale)),
        ]
        cards.append(f'<div class="card"><h3>{_text(component.component_id)}</h3>{_rows(rows)}</div>')
    return "".join(cards) or '<p class="muted">No components.</p>'


def _describe_basis(basis: dict[str, Any]) -> str:
    """One escaped line on why a grouping is suggested."""
    if basis.get("kind") == "shared_pattern":
        pipelines = ", ".join(str(pipeline) for pipeline in basis.get("pipelines") or [])
        pattern = _text(basis.get("pattern"))
        return f"Shared simplification pattern <strong>{pattern}</strong>, recommended for {_text(pipelines)}"
    summary = f" &mdash; {_text(basis['relationship_summary'])}" if basis.get("relationship_summary") else ""
    return (
        f"Inferred relationship <code>{_text(basis.get('from_pipeline'))}</code> &rarr; "
        f"<code>{_text(basis.get('to_pipeline'))}</code> via {_text(basis.get('edge_identity'))} "
        f"({_text(basis.get('confidence'))} confidence; evidence: {_text(basis.get('evidence'))}){summary}"
    )


def _groupings(plan: ConversionPlan, dropped_groupings: list[dict[str, Any]]) -> str:
    """Section 4: every suggested grouping, its basis, and whether it is accepted; then any dropped one."""
    cards = []
    for grouping in plan.suggested_groupings:
        status = _tag("accepted", "agentic") if grouping.accepted else _tag("not accepted", "pending")
        basis = "<ul>" + "".join(f"<li>{_describe_basis(entry)}</li>" for entry in grouping.basis) + "</ul>"
        rows = [
            ("Status", status),
            ("Components", _text(", ".join(grouping.components))),
            ("Members", _text(", ".join(grouping.members))),
            ("Why it is suggested", basis),
        ]
        cards.append(f'<div class="card"><h3>{_text(grouping.grouping_id)}</h3>{_rows(rows)}</div>')
    for dropped in dropped_groupings:
        rows = [
            ("Status", _tag("no longer suggested", "refused")),
            ("Components", _text(", ".join(dropped["components"]))),
            ("Members", _text(", ".join(dropped["members"]))),
            ("Replaced by", _text(", ".join(dropped["replaced_by"]) or None)),
            (
                "What changed",
                _text(
                    "You accepted this grouping, but enrich has changed it since, so route no longer suggests it. "
                    "Its components are pending again: decide them again, and accept a replacing suggestion to "
                    "keep converting them together."
                ),
            ),
        ]
        cards.append(f'<div class="card"><h3>{_text(dropped["grouping_id"])}</h3>{_rows(rows)}</div>')
    return "".join(cards) or '<p class="muted">No groupings are suggested.</p>'


def _conversion(units: dict[str, dict[str, Any]], outcomes: dict[str, Any]) -> str:
    """Section 5: what package will do with each routing unit."""
    from flowx.route_agentic import OUTCOME_AGENTIC_APPLIED

    rows = []
    for unit_id, unit in units.items():
        recorded = outcomes.get(unit_id)
        outcome: dict[str, Any] = recorded if isinstance(recorded, dict) else {}
        if unit["decision"] is None:
            what = _tag("pending", "pending") + " Decision pending; route applies nothing until it is decided."
        elif unit["decision"] == DECISION_DETERMINISTIC:
            what = _tag("deterministic", "deterministic") + " Converted 1:1 by the deterministic engine."
        elif outcome.get("outcome") == OUTCOME_AGENTIC_APPLIED:
            what = _tag("agentic", "agentic") + (
                f" The agent's output <code>{_text(str(outcome.get('output_sha256'))[:12])}</code> replaces these "
                "pipelines; package ships it."
            )
        else:
            what = _tag("waiting", "refused") + (
                " Waiting for the agent's output (fill-agentic); package refuses until it is filled."
            )
        label = f"{unit_id} ({', '.join(unit.get('components') or [])})" if unit.get("components") else unit_id
        rows.append((label, f'{what}<br><span class="muted">{_text(", ".join(unit["members"]))}</span>'))
    return _rows(rows) if rows else '<p class="muted">Nothing to convert.</p>'


def _conversation(plan: ConversionPlan) -> str:
    """Section 6: the questions asked while routing and the user's answers."""
    if not plan.conversation:
        return '<p class="muted">No routing conversation is recorded.</p>'
    return _rows([(entry.question, _text(entry.answer)) for entry in plan.conversation])


def _steer() -> str:
    """Section 8: the exact edits and commands that change the conversion."""
    steps = [
        'Set each component\'s <code>decision</code> to <code>"deterministic"</code> or <code>"agentic"</code> '
        "in <code>metadata/conversion_plan.json</code> (or pass the edited plan as <code>plan</code>).",
        "To convert a suggested grouping as one unit, set its <code>accepted</code> to <code>true</code> and "
        'decide each of its components <code>"agentic"</code>.',
        "Optionally record the questions you asked and the answers under <code>conversation</code>.",
        "Run route again: <code>python -m flowx.adapter route --output-dir &lt;dir&gt;</code>, or "
        '<code>flowx(command="route", parameters={"output_dir": ...})</code>. Route applies the plan once '
        "nothing is pending; the plan can be changed and re-applied as often as needed.",
        "Fill each agentic unit with <code>fill-agentic --members &lt;members&gt; --pipelines-path &lt;file&gt;</code> "
        "(MCP: <code>fill_agentic</code>); run it again with new pipelines to change the approach.",
    ]
    return "<ol>" + "".join(f"<li>{step}</li>" for step in steps) + "</ol>"
