"""Captured Airflow constructs: source spans, DAG declarations, task/edge captures."""

from __future__ import annotations

import ast
from dataclasses import dataclass, field
from typing import Any


@dataclass(frozen=True, slots=True, kw_only=True)
class SourceSpan:
    """Stable source location used to identify captured Airflow constructs."""

    line: int
    column: int
    end_line: int
    end_column: int


@dataclass(frozen=True, slots=True, kw_only=True)
class DagDeclaration:
    """One statically discovered DAG declaration in a Python module."""

    capture_id: str
    variable: str | None
    node: ast.stmt
    span: SourceSpan
    kind: str = "direct"
    factory: ast.FunctionDef | None = None
    bindings: dict[str, Any] = field(default_factory=dict)
    target_dag_variable: str | None = None
    decorator_overrides: dict[str, ast.expr] = field(default_factory=dict)
    unsupported_reason: str | None = None


@dataclass(frozen=True, slots=True, kw_only=True)
class TaskCapture:
    """One operator or TaskFlow invocation before Databricks key allocation."""

    capture_id: str
    variable: str
    task_id: str
    operator: str
    call: ast.Call
    span: SourceSpan


@dataclass(frozen=True, slots=True, kw_only=True)
class EdgeCapture:
    """A dependency edge expressed in capture identities rather than task keys."""

    upstream_id: str
    downstream_id: str
    span: SourceSpan


@dataclass(slots=True)
class _TaskFlowTask:
    """A TaskFlow ``@task`` invocation captured from a ``@dag`` body.

    ``positional_deps`` / ``keyword_deps`` map each argument position / keyword the callable was
    invoked with to the upstream task var it references (TaskFlow's implicit XCom data flow), so the
    emitted notebook can read that upstream's return value via ``dbutils.jobs.taskValues``. Literal
    args are preserved when literal and routed to a placeholder when they cannot be resolved safely.

    ``.expand(param=<iterable>)`` dynamic mapping is captured in ``expand_kwarg`` (the mapped
    parameter name) and ``expand_items_json`` (the iterable as a JSON-array literal) when the
    iterable is statically knowable; a non-literal iterable leaves ``expand_items_json`` None and
    routes the task to the agentic-gap round.
    """

    task_id: str
    def_name: str
    decorator: str
    source_reference: str
    is_async: bool = False
    positional_deps: dict[int, str] = field(default_factory=dict)
    keyword_deps: dict[str, str] = field(default_factory=dict)
    positional_values: dict[int, str] = field(default_factory=dict)
    keyword_values: dict[str, str] = field(default_factory=dict)
    unresolved_arguments: list[str] = field(default_factory=list)
    expand_kwarg: str | None = None
    expand_items_json: str | None = None
