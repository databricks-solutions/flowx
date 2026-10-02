"""Parsing for AWS Glue Workflow graph exports.

Parses the output of ``aws glue get-workflow --include-graph`` -- either the
bare workflow object or a payload wrapped under a ``Workflow`` key -- into a
typed, read-only model the translator walks. The workflow graph lists job,
crawler, and trigger nodes; the trigger objects carry the authoritative
orchestration (``Predicate`` for upstream state, ``Actions`` for downstream
nodes, ``Schedule`` for scheduled starts), so the translator reads them rather
than the denormalised ``Edges`` list. Parsing never raises on unexpected node
shapes; the translator decides how each node maps to the flowx IR.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any


@dataclass(frozen=True, slots=True)
class Condition:
    """One predicate condition on an upstream job or crawler state.

    Attributes:
        job_name: The upstream job the condition watches, or ``None`` for a crawler condition.
        crawler_name: The upstream crawler the condition watches, or ``None`` for a job condition.
        state: The required state (``SUCCEEDED`` / ``FAILED`` / ``TIMEOUT`` / ``STOPPED`` for jobs,
            ``SUCCEEDED`` / ``FAILED`` / ``CANCELLED`` for crawlers).
    """

    job_name: str | None
    crawler_name: str | None
    state: str | None


@dataclass(frozen=True, slots=True)
class Trigger:
    """A Glue workflow trigger.

    Attributes:
        name: The trigger's name.
        type: ``SCHEDULED``, ``CONDITIONAL``, ``ON_DEMAND``, or ``EVENT``.
        schedule: The raw ``cron(...)`` expression for a scheduled trigger, else ``None``.
        logical: The predicate's ``AND`` / ``ANY`` combinator for a conditional trigger.
        conditions: The predicate conditions for a conditional trigger.
        action_jobs: Names of jobs this trigger starts.
        action_crawlers: Names of crawlers this trigger starts.
    """

    name: str
    type: str
    schedule: str | None = None
    logical: str = "AND"
    conditions: list[Condition] = field(default_factory=list)
    action_jobs: list[str] = field(default_factory=list)
    action_crawlers: list[str] = field(default_factory=list)


@dataclass(frozen=True, slots=True)
class GlueWorkflow:
    """A parsed Glue Workflow.

    Attributes:
        name: The workflow name.
        jobs: Names of job nodes in the graph.
        crawlers: Names of crawler nodes in the graph.
        triggers: Parsed triggers, in graph order.
    """

    name: str
    jobs: list[str]
    crawlers: list[str]
    triggers: list[Trigger]


def parse_workflow(raw: dict[str, Any], *, default_name: str) -> GlueWorkflow:
    """Parses a raw get-workflow payload into a :class:`GlueWorkflow`.

    Args:
        raw: Parsed JSON of a bare workflow object or a ``{"Workflow": {...}}`` payload.
        default_name: Name to use when the payload carries no ``Name``.

    Returns:
        The typed workflow.

    Raises:
        ValueError: When the payload has no graph ``Nodes``.
    """
    wrapped = raw.get("Workflow")
    body = wrapped if isinstance(wrapped, dict) else raw
    name = body.get("Name") or default_name
    graph = body.get("Graph")
    nodes = graph.get("Nodes") if isinstance(graph, dict) else None
    if not isinstance(nodes, list):
        raise ValueError(f"{name!r} is not a valid workflow graph: missing 'Graph.Nodes'.")

    jobs = [node["Name"] for node in nodes if _node_type(node) == "JOB" and node.get("Name")]
    crawlers = [node["Name"] for node in nodes if _node_type(node) == "CRAWLER" and node.get("Name")]
    triggers = [_parse_trigger(node) for node in nodes if _node_type(node) == "TRIGGER"]
    return GlueWorkflow(name=name, jobs=jobs, crawlers=crawlers, triggers=[t for t in triggers if t is not None])


def load_workflow_files(source_dir: Path) -> list[tuple[str, dict[str, Any]]]:
    """Loads every workflow JSON file under *source_dir*.

    Args:
        source_dir: A single ``.json`` file or a directory scanned recursively.

    Returns:
        ``(file_stem, raw_json)`` pairs in sorted path order. Files that are not
        readable JSON objects are skipped.
    """
    files = [source_dir] if source_dir.is_file() else sorted(source_dir.rglob("*.json"))
    results: list[tuple[str, dict[str, Any]]] = []
    for path in files:
        try:
            raw = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
        if isinstance(raw, dict):
            results.append((path.stem, raw))
    return results


def _parse_trigger(node: dict[str, Any]) -> Trigger | None:
    """Parses a TRIGGER node's inner ``Trigger`` object, or ``None`` when absent."""
    details = node.get("TriggerDetails")
    trigger = details.get("Trigger") if isinstance(details, dict) else None
    if not isinstance(trigger, dict):
        return None
    raw_predicate = trigger.get("Predicate")
    predicate = raw_predicate if isinstance(raw_predicate, dict) else {}
    conditions = [
        Condition(
            job_name=condition.get("JobName"),
            crawler_name=condition.get("CrawlerName"),
            state=condition.get("State") or condition.get("CrawlState"),
        )
        for condition in predicate.get("Conditions", [])
        if isinstance(condition, dict)
    ]
    actions = [action for action in trigger.get("Actions", []) if isinstance(action, dict)]
    return Trigger(
        name=trigger.get("Name") or node.get("Name") or "",
        type=str(trigger.get("Type", "")),
        schedule=trigger.get("Schedule"),
        logical=str(predicate.get("Logical", "AND")),
        conditions=conditions,
        action_jobs=[action["JobName"] for action in actions if action.get("JobName")],
        action_crawlers=[action["CrawlerName"] for action in actions if action.get("CrawlerName")],
    )


def _node_type(node: Any) -> str:
    """Returns a node's ``Type`` string, or ``""`` when the node is malformed."""
    return str(node.get("Type", "")) if isinstance(node, dict) else ""
