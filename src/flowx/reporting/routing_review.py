"""The routing review page: one short, standard view of how a factory will be converted.

Route writes ``metadata/routing_review.html`` for the person deciding each group deterministic or
agentic. It is filled from the plan, the inventory's insights, the report's routing record and any
accepted grouping route dropped on this run, and every run shows the same eight sections in the same
order: at a glance, what the source does, the recommended conversion, suggested groupings, what
happens next, what you told us, notes, and how to change a decision. Detail a decision maker does not
need (activity counts, detected patterns, hashes) is folded into a closing "Technical details" block.

The page uses Databricks colours and is filled from :data:`routing_review_template.html <_TEMPLATE_PATH>`
with the standard library only (no script or external asset, so it opens offline and in the workspace
file viewer). It carries no timestamp, and every value from the plan or insights is HTML-escaped, so
the same inputs always give the same bytes. The page is generated and never read back: the user steers
by editing ``conversion_plan.json`` (or passing a plan) and running route again.
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
        source_line=_source_line(inventory),
        summary=_summary(plan, inventory),
        source=_source(insights),
        components=_components(plan),
        groupings=_groupings(plan, dropped_groupings or []),
        conversion=_conversion(units, outcomes),
        conversation=_conversation(plan),
        findings=_findings(plan.findings),
        steer=_steer(),
        details=_details(plan),
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


def _pill(label: str, kind: str) -> str:
    """A small coloured status label; ``kind`` is one of det, agt, pend, wait or muted."""
    return f'<span class="pill pill-{kind}">{escape(label)}</span>'


def _decision_pill(decision: str | None) -> str:
    """The label for a decision, pending included."""
    if decision == DECISION_DETERMINISTIC:
        return _pill("Deterministic", "det")
    if decision == DECISION_AGENTIC:
        return _pill("Agentic", "agt")
    return _pill("Decision pending", "pend")


def _plural(count: int, singular: str, plural: str | None = None) -> str:
    """``count`` and the noun, in the right number."""
    return f"{count} {singular if count == 1 else plural or singular + 's'}"


def _pipelines(members: list[str], limit: int = 3) -> str:
    """The first few pipeline names, then how many more there are."""
    shown = ", ".join(escape(member) for member in members[:limit])
    more = len(members) - limit
    return f'{shown} <span class="muted">+{more} more</span>' if more > 0 else shown


def _deterministic_option(component: Any) -> dict[str, Any]:
    """The recorded deterministic option of a component, or an empty one."""
    options = component.options if isinstance(component.options, dict) else {}
    option = options.get("deterministic")
    return option if isinstance(option, dict) else {}


def _activity_counts(option: dict[str, Any]) -> dict[str, int]:
    """The per-strategy activity counts of a deterministic option, ignoring anything malformed."""
    counts = option.get("activity_counts")
    if not isinstance(counts, dict):
        return {}
    return {str(bucket): value for bucket, value in counts.items() if isinstance(value, int)}


def _agentic_option(component: Any) -> dict[str, Any]:
    """The recorded agentic option of a component, or an empty one."""
    options = component.options if isinstance(component.options, dict) else {}
    option = options.get("agentic")
    return option if isinstance(option, dict) else {}


def _why(component: Any) -> str:
    """One plain sentence on why the library recommends what it does for this component."""
    deterministic = _deterministic_option(component)
    total = sum(_activity_counts(deterministic).values())
    uncovered = [entry for entry in deterministic.get("uncovered") or [] if isinstance(entry, dict)]
    if deterministic.get("capable"):
        if total == 1:
            return "Its one activity converts automatically."
        return escape(f"All {total} activities convert automatically.")
    kinds = sorted({str(entry.get("type")) for entry in uncovered if entry.get("type")})
    reason = f"{_plural(len(uncovered), 'activity', 'activities')} can't convert 1:1"
    return escape(f"{reason} ({', '.join(kinds)})." if kinds else f"{reason}.")


def _suggested_pattern(component: Any) -> str:
    """The first pattern the insights recommend for this component, with its release label."""
    agentic = _agentic_option(component)
    patterns = [entry for entry in agentic.get("recommended_patterns") or [] if isinstance(entry, dict)]
    if not patterns:
        return '<span class="muted">None suggested</span>'
    first = patterns[0]
    labels = {
        str(entry.get("pattern")): str(entry.get("label"))
        for entry in agentic.get("release_disclosures") or []
        if isinstance(entry, dict)
    }
    label = labels.get(str(first.get("pattern")))
    simplification = " " + _pill("Simplifies", "agt") if first.get("simplification_pattern") else ""
    release = f' <span class="muted small">{escape(label)}</span>' if label else ""
    return f"{_text(first.get('pattern'))}{simplification}{release}"


def _summary(plan: ConversionPlan, inventory: dict[str, Any] | None) -> str:
    """At a glance: one headline sentence and the counts a decision maker needs."""
    pipelines = (inventory or {}).get("pipelines")
    pipeline_count = len(pipelines) if isinstance(pipelines, list) else sum(len(c.members) for c in plan.components)
    components = len(plan.components)
    automatic = sum(1 for component in plan.components if component.recommended == DECISION_DETERMINISTIC)
    redesign = sum(1 for component in plan.components if component.recommended == DECISION_AGENTIC)
    pending = len(plan.pending_components())
    accepted = sum(1 for grouping in plan.suggested_groupings if grouping.accepted)
    headline = (
        f"{_plural(pipeline_count, 'pipeline')} in {_plural(components, 'group')}. "
        f"{automatic} recommended for deterministic conversion; {redesign} "
        f"{'is' if redesign == 1 else 'are'} recommended for agentic redesign."
    )
    status = (
        f"{pending} of {components} groups still need a decision. Nothing is converted until every group has one."
        if pending
        else "Every group has a decision."
    )
    tiles = [
        (pipeline_count, "Pipelines", False),
        (components, "Groups", False),
        (automatic, "Recommended deterministic", False),
        (redesign, "Recommended for agentic redesign", False),
        (len(plan.suggested_groupings), f"Suggested groupings ({accepted} accepted)", False),
        (pending, "Decisions pending", pending > 0),
    ]
    tile_html = "".join(
        f'<div class="tile{" alert" if alert else ""}"><b>{value}</b><span>{escape(label)}</span></div>'
        for value, label, alert in tiles
    )
    return f'<p class="lead">{escape(headline)} {escape(status)}</p><div class="tiles">{tile_html}</div>'


def _source(insights: dict[str, Any] | None) -> str:
    """What the source does: the insights overview and the factory-wide recommendation, briefly."""
    if insights is None:
        return '<p class="muted">Enrich has not run yet, so there is no summary of what the pipelines do.</p>'
    parts = [f'<p class="lead">{_text(insights.get("overview") or "No overview was recorded.")}</p>']
    system = insights.get("system_recommendation")
    if isinstance(system, dict) and system:
        lines = []
        if system.get("headline"):
            lines.append(f"<p><strong>{_text(system['headline'])}</strong></p>")
        patterns = [entry for entry in system.get("recommended_patterns") or [] if isinstance(entry, dict)]
        if patterns:
            fit = f": {_text(patterns[0].get('fit'))}" if patterns[0].get("fit") else ""
            lines.append(f"<p>Recommended approach: {_text(patterns[0].get('pattern'))}{fit}</p>")
        cascade = [str(item) for item in system.get("cascade") or [] if item]
        if cascade:
            lines.append(f"<p>What it replaces: {escape('; '.join(cascade))}</p>")
        if system.get("decision_driver"):
            lines.append(f'<p class="muted">Key question: {_text(system["decision_driver"])}</p>')
        if lines:
            parts.append(f'<div class="callout">{"".join(lines)}</div>')
    return f'<div class="stack">{"".join(parts)}</div>'


def _components(plan: ConversionPlan) -> str:
    """Recommended conversion: the groups to look at closely, then the ones recommended for deterministic conversion."""
    redesign = [
        component
        for component in plan.components
        if component.recommended == DECISION_AGENTIC or component.decision == DECISION_AGENTIC
    ]
    automatic = [component for component in plan.components if component not in redesign]
    grouped_with = {
        component_id: [other for other in grouping.components if other != component_id]
        for grouping in plan.suggested_groupings
        if grouping.accepted
        for component_id in grouping.components
    }
    parts: list[str] = []
    if redesign:
        rows = "".join(
            f"<tr><td><strong>{_text(component.component_id)}</strong><br>{_pipelines(component.members)}</td>"
            f"<td>{_why(component)}{_grouped_note(grouped_with.get(component.component_id))}</td>"
            f"<td>{_suggested_pattern(component)}</td>"
            f'<td class="nowrap">{_decision_pill(component.decision)}</td></tr>'
            for component in redesign
        )
        parts.append(
            f"<h3>Agentic redesign ({len(redesign)})</h3>"
            '<p class="muted small">The agent builds these as new Databricks pipelines rather than translating 1:1.</p>'
            '<div class="scroll"><table><thead><tr><th>Group</th><th>Why</th><th>Suggested pattern</th>'
            f"<th>Decision</th></tr></thead><tbody>{rows}</tbody></table></div>"
        )
    else:
        parts.append(
            '<p class="lead">No group needs agentic redesign: every group is recommended for deterministic '
            "conversion.</p>"
        )
    if automatic:
        pending = sum(1 for component in automatic if component.decision is None)
        rows = "".join(
            f"<tr><td><strong>{_text(component.component_id)}</strong></td><td>{_pipelines(component.members, 4)}</td>"
            f'<td>{_why(component)}</td><td class="nowrap">{_decision_pill(component.decision)}</td></tr>'
            for component in automatic
        )
        parts.append(
            f"<details><summary>Recommended deterministic ({len(automatic)}"
            f"{f', {pending} pending' if pending else ''})</summary>"
            '<div class="stack"><p class="muted small">flowx\'s deterministic engine translates these 1:1; any '
            "activity listed under Why is left as a gap to fill.</p>"
            '<div class="scroll"><table><thead><tr><th>Group</th><th>Pipelines</th><th>Why</th><th>Decision</th></tr>'
            f"</thead><tbody>{rows}</tbody></table></div></div></details>"
        )
    return f'<div class="stack">{"".join(parts)}</div>'


def _grouped_note(others: list[str] | None) -> str:
    """A short line saying a component converts together with others, under an accepted grouping."""
    if not others:
        return ""
    return f'<br><span class="muted small">Converts as one unit with {escape(", ".join(others))}.</span>'


def _basis_sentence(basis: dict[str, Any], members: list[str]) -> str:
    """One plain sentence on why a grouping is suggested."""
    if basis.get("kind") == "shared_pattern":
        pipelines = [str(pipeline) for pipeline in basis.get("pipelines") or []]
        pattern = _text(basis.get("pattern"))
        if sorted(pipelines) == sorted(members):
            who = "Both are" if len(pipelines) == 2 else "All of them are"
            return f"{who} candidates for {pattern}."
        return f"{escape(', '.join(pipelines))} are candidates for {pattern}."
    confidence = f" ({escape(str(basis['confidence']))} confidence)" if basis.get("confidence") else ""
    return (
        f"{_text(basis.get('to_pipeline'))} depends on {_text(basis.get('from_pipeline'))} through "
        f"{_text(basis.get('edge_identity'))}{confidence}."
    )


def _groupings(plan: ConversionPlan, dropped_groupings: list[dict[str, Any]]) -> str:
    """Suggested groupings: pipelines the insights say to convert together, and any no longer suggested."""
    cards = []
    for grouping in plan.suggested_groupings:
        status = _pill("Accepted", "agt") if grouping.accepted else _pill("Not accepted", "muted")
        reasons = "".join(f"<li>{_basis_sentence(entry, grouping.members)}</li>" for entry in grouping.basis)
        cards.append(
            f'<div class="card"><div class="card-head"><h3>Convert '
            f"{_plural(len(grouping.members), 'pipeline')} together</h3>{status}</div>"
            f"<p>{_pipelines(grouping.members, 6)}</p><ul>{reasons}</ul>"
            f'<p class="muted small">{_text(grouping.grouping_id)} &middot; joins '
            f"{_text(', '.join(grouping.components))}</p></div>"
        )
    for dropped in dropped_groupings:
        replaced_by = ", ".join(dropped.get("replaced_by") or [])
        replacement = f" It is replaced by {escape(replaced_by)}." if replaced_by else ""
        cards.append(
            f'<div class="card"><div class="card-head"><h3>{_text(dropped["grouping_id"])}</h3>'
            f"{_pill('No longer suggested', 'wait')}</div>"
            f"<p>{_pipelines(list(dropped.get('members') or []), 6)}</p>"
            "<p>You accepted this grouping, but the insights have changed since, so it is no longer suggested and "
            f"its groups are pending again.{replacement}</p></div>"
        )
    if not cards:
        return '<p class="muted">No groupings are suggested.</p>'
    intro = (
        '<p class="muted small">A grouping converts several groups as one agentic unit, so one new pipeline can '
        "replace them all. Accepting one is optional.</p>"
    )
    return f'<div class="stack">{intro}<div class="cards">{"".join(cards)}</div></div>'


def _conversion(units: dict[str, dict[str, Any]], outcomes: dict[str, Any]) -> str:
    """What happens next: where conversion stands per unit, and whether the bundle can be packaged."""
    from flowx.route_agentic import OUTCOME_AGENTIC_APPLIED

    pending, automatic, waiting, ready = [], [], [], []
    for unit_id, unit in units.items():
        recorded = outcomes.get(unit_id)
        outcome = recorded.get("outcome") if isinstance(recorded, dict) else None
        if unit["decision"] is None:
            pending.append(unit_id)
        elif unit["decision"] == DECISION_DETERMINISTIC:
            automatic.append(unit_id)
        elif outcome == OUTCOME_AGENTIC_APPLIED:
            ready.append(unit_id)
        else:
            waiting.append(unit_id)
    states = [
        (pending, "Decision pending", "pend"),
        (automatic, "Converts automatically", "det"),
        (waiting, "Waiting for the agent's build", "wait"),
        (ready, "Agent build ready", "agt"),
    ]
    status = "".join(
        f"<div>{_pill(str(len(ids)), kind)}<span>{escape(label)}</span></div>" for ids, label, kind in states
    )
    if pending:
        verdict = "The plan is not applied yet. Decide the pending groups, then run route again."
    elif waiting:
        verdict = "Packaging is blocked until the agent builds every agentic unit."
    else:
        verdict = "Everything is decided and built. The bundle can be packaged."
    steps = [
        f"<li><strong>{_text(unit_id)}</strong>: the agent builds "
        f"{_pipelines(units[unit_id]['members'])} with fill-agentic.</li>"
        for unit_id in waiting
    ]
    step_list = f"<ul>{''.join(steps)}</ul>" if steps else ""
    return (
        f'<div class="stack"><div class="status">{status}</div><p class="lead">{escape(verdict)}</p>{step_list}</div>'
    )


def _conversation(plan: ConversionPlan) -> str:
    """What you told us: the questions asked while routing and the answers."""
    if not plan.conversation:
        return '<p class="muted">No routing conversation is recorded yet.</p>'
    items = "".join(
        f"<div><dt>{_text(entry.question)}</dt><dd>{_text(entry.answer)}</dd></div>" for entry in plan.conversation
    )
    return f"<dl>{items}</dl>"


def _findings(findings: list[str]) -> str:
    """Notes: a plain summary of what route could not resolve, with route's own wording folded away.

    Route reports a call to a pipeline that isn't in the export as an "unresolved control edge"; those
    are counted in one sentence, since the caller is simply routed on its own.
    """
    if not findings:
        return '<p class="muted">Nothing to note.</p>'
    unresolved = [finding for finding in findings if finding.startswith("unresolved control edge")]
    other = [finding for finding in findings if finding not in unresolved]
    lines = [f"<li>{_text(finding)}</li>" for finding in other]
    if unresolved:
        lines.insert(
            0,
            f"<li>{_plural(len(unresolved), 'pipeline call points', 'pipeline calls point')} to a pipeline that "
            "isn't in this export. The calling pipelines are converted on their own; check whether the missing "
            "pipelines should be exported too.</li>",
        )
    raw = "".join(f"<li>{_text(finding)}</li>" for finding in findings)
    return (
        f'<div class="stack"><ul>{"".join(lines)}</ul><details><summary>Route\'s wording</summary>'
        f'<div class="stack"><ul class="small">{raw}</ul></div></details></div>'
    )


def _steer() -> str:
    """How to change a decision: the edits and the command, in short."""
    commands = (
        "python -m flowx.adapter route --output-dir &lt;dir&gt;\n"
        'flowx(command="route", parameters={"output_dir": "&lt;dir&gt;"})'
    )
    steps = [
        'Set each group\'s <code>decision</code> to <code>"deterministic"</code> or <code>"agentic"</code> in '
        "<code>metadata/conversion_plan.json</code>.",
        "To convert a suggested grouping as one unit, set its <code>accepted</code> to <code>true</code> and decide "
        'its groups <code>"agentic"</code>.',
        "Run route again. It applies the plan once nothing is pending, and you can change it and re-run as often as "
        "you like.",
    ]
    return f'<div class="stack"><ol>{"".join(f"<li>{step}</li>" for step in steps)}</ol><pre>{commands}</pre></div>'


def _details(plan: ConversionPlan) -> str:
    """Technical details, folded away: per-group evidence and the hashes the plan is bound to."""
    rows = []
    for component in plan.components:
        deterministic = _deterministic_option(component)
        counts = _activity_counts(deterministic)
        uncovered = [
            f"{entry.get('pipeline')} / {entry.get('activity')} ({entry.get('type')})"
            for entry in deterministic.get("uncovered") or []
            if isinstance(entry, dict)
        ]
        rows.append(
            f"<tr><td>{_text(component.component_id)}</td><td>{_text(', '.join(component.members))}</td>"
            f"<td>{_text(', '.join(f'{count} {bucket}' for bucket, count in counts.items()) or None)}</td>"
            f"<td>{_text(', '.join(str(motif) for motif in deterministic.get('motifs') or []) or None)}</td>"
            f"<td>{_text('; '.join(uncovered) or None)}</td><td>{_text(component.rationale)}</td></tr>"
        )
    hashes = (
        f'<p class="small">Inventory <code>{_text(plan.inventory_sha256)}</code><br>Source graphs '
        f"<code>{_text(plan.source_graphs_sha256)}</code><br>Agentic insights "
        f"<code>{_text(plan.agentic_insights_sha256)}</code></p>"
    )
    table = (
        '<div class="scroll"><table class="small"><thead><tr><th>Group</th><th>Pipelines</th><th>Activities</th>'
        "<th>Detected patterns</th><th>Not convertible 1:1</th><th>Rationale</th></tr></thead>"
        f"<tbody>{''.join(rows)}</tbody></table></div>"
    )
    return f'<details><summary>Technical details</summary><div class="stack">{table}{hashes}</div></details>'


def _source_line(inventory: dict[str, Any] | None) -> str:
    """The line under the page title naming the source the plan covers."""
    source = (inventory or {}).get("source")
    label = {"adf": "Azure Data Factory", "airflow": "Apache Airflow"}.get(str(source), source)
    named = f"Source: {escape(str(label))}. " if label else ""
    return (
        f"<p>{named}How your pipelines will be converted, and the decisions still to make. Generated by route from "
        "<code>metadata/conversion_plan.json</code>; edit that file, not this page.</p>"
    )
