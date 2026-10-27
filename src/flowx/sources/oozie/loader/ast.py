"""Typed nodes for an Oozie ``workflow.xml``.

These mirror the Oozie workflow schema closely (one class per node kind) so the lowering code can
reason about Oozie's own semantics before anything Databricks-specific happens. Values stay as the raw
strings from the XML, ``${...}`` expressions included; resolving them is the EL renderers' job.
"""

from __future__ import annotations

from dataclasses import dataclass, field


class OozieParseError(ValueError):
    """Raised when a workflow.xml can't be read as an Oozie workflow."""


@dataclass(slots=True, kw_only=True)
class PrepareStep:
    """One ``<prepare>`` operation (``delete`` or ``mkdir``) that Oozie runs before the action."""

    operation: str
    path: str


@dataclass(slots=True, kw_only=True)
class SparkAction:
    """The body of a ``<spark>`` action: what spark-submit would have been given."""

    jar: str
    master: str | None = None
    mode: str | None = None
    spark_name: str | None = None
    main_class: str | None = None
    spark_opts: str = ""
    arguments: list[str] = field(default_factory=list)
    files: list[str] = field(default_factory=list)
    archives: list[str] = field(default_factory=list)
    prepare: list[PrepareStep] = field(default_factory=list)
    configuration: dict[str, str] = field(default_factory=dict)
    job_xml: list[str] = field(default_factory=list)


@dataclass(slots=True, kw_only=True)
class GenericAction:
    """Any action type without a typed payload yet; keeps the raw XML so nothing is lost."""

    element_tag: str
    raw_xml: str
    configuration: dict[str, str] = field(default_factory=dict)
    job_xml: list[str] = field(default_factory=list)


ActionPayload = SparkAction | GenericAction


@dataclass(slots=True, kw_only=True)
class StartNode:
    """The ``<start>`` node: where execution begins."""

    to: str
    name: str = "start"


@dataclass(slots=True, kw_only=True)
class EndNode:
    """An ``<end>`` node: the workflow finishes successfully."""

    name: str


@dataclass(slots=True, kw_only=True)
class KillNode:
    """A ``<kill>`` node: the workflow stops as KILLED with this message."""

    name: str
    message: str = ""


@dataclass(slots=True, kw_only=True)
class DecisionCase:
    """One ``<case>`` of a decision: go to ``to`` when ``predicate`` is true."""

    predicate: str
    to: str


@dataclass(slots=True, kw_only=True)
class DecisionNode:
    """A ``<decision>``: cases are tried in order and the first true one wins, else ``default_to``."""

    name: str
    cases: list[DecisionCase] = field(default_factory=list)
    default_to: str = ""


@dataclass(slots=True, kw_only=True)
class ForkNode:
    """A ``<fork>``: every path starts in parallel."""

    name: str
    paths: list[str] = field(default_factory=list)


@dataclass(slots=True, kw_only=True)
class JoinNode:
    """A ``<join>``: waits for every path of its fork, then continues to ``to``."""

    name: str
    to: str


@dataclass(slots=True, kw_only=True)
class ActionNode:
    """An ``<action>``: runs its payload, then follows ``ok_to`` on success or ``error_to`` on failure."""

    name: str
    action_type: str
    payload: ActionPayload
    ok_to: str
    error_to: str
    retry_max: int | None = None
    retry_interval_minutes: int | None = None
    raw_xml: str = ""


Node = EndNode | KillNode | DecisionNode | ForkNode | JoinNode | ActionNode


@dataclass(slots=True, kw_only=True)
class OozieWorkflow:
    """A parsed ``workflow.xml``."""

    name: str
    start: StartNode
    nodes: dict[str, Node]
    schema_version: str | None = None
    parameters: dict[str, str | None] = field(default_factory=dict)
    global_configuration: dict[str, str] = field(default_factory=dict)
    global_job_xml: list[str] = field(default_factory=list)
    credentials: list[str] = field(default_factory=list)
    raw_xml: str = ""
