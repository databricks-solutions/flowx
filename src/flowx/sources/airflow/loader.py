"""Airflow DAG parser: parse a DAG file into a flowx Pipeline IR.

Core parser for the Airflow source (``flowx.sources.airflow``).  It reads a DAG
module statically with :mod:`ast` (no Airflow install or DAG execution) and
produces the same :class:`~flowx.models.ir.Pipeline` IR the ADF path emits, so
the shared downstream half -- ``prepare_workflow`` -> ``write_bundle`` -> DABs --
is reused unchanged.  The ``discover`` and ``convert`` phase entry points in
this package wrap :func:`load_airflow_dag`.

Coverage spans the four tiers (per-operator builders live in
:mod:`flowx.sources.airflow.operators`): Tier 1 direct mappings (Python/Bash,
Spark-submit, Databricks provider, SQL, dbt CLI), Tier 2 semantic
(branch/virtualenv, cosmos ``DbtTaskGroup`` -> DbtFactoryActivity, Dummy/Empty
dropped + rewired), Tier 3 sensors (a root file/table sensor with no schedule ->
``file_arrival`` / ``table_update`` trigger, otherwise retained as a polling task;
time sensors -> PlaceholderActivity), and Tier 4 (unmapped -> PlaceholderActivity).
``>>`` / ``<<`` dependencies and cron ``schedule_interval`` -> Quartz are
handled here.
"""

from __future__ import annotations

import ast
import copy
import json
import re
from collections import Counter
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from flowx.models.ir import (
    Activity,
    DbtFactoryActivity,
    Dependency,
    ForEachActivity,
    NotebookActivity,
    Pipeline,
    PlaceholderActivity,
    RunJobActivity,
    SqlActivity,
)
from flowx.sources.airflow import audit as source_audit
from flowx.sources.airflow import callable_notebook, templating
from flowx.sources.airflow import operators as ops
from flowx.utils import normalize_task_key


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
    positional_deps: dict[int, str] = field(default_factory=dict)
    keyword_deps: dict[str, str] = field(default_factory=dict)
    positional_values: dict[int, str] = field(default_factory=dict)
    keyword_values: dict[str, str] = field(default_factory=dict)
    unresolved_arguments: list[str] = field(default_factory=list)
    expand_kwarg: str | None = None
    expand_items_json: str | None = None


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


def _span(node: ast.AST) -> SourceSpan:
    """Returns a complete source span for an AST node."""
    return SourceSpan(
        line=getattr(node, "lineno", 0),
        column=getattr(node, "col_offset", 0),
        end_line=getattr(node, "end_lineno", getattr(node, "lineno", 0)),
        end_column=getattr(node, "end_col_offset", getattr(node, "col_offset", 0)),
    )


def _sanitize_task_key(name: str) -> str:
    """Converts an Airflow task_id into a valid Databricks task key."""
    key = re.sub(r"[^a-zA-Z0-9_-]", "_", name)
    key = re.sub(r"_+", "_", key).strip("_")
    return key or "unnamed"


def _param_default(node: ast.expr) -> Any:
    """The default value of a DAG ``params`` entry: a bare literal or ``Param(default=...)``.

    Returns ``None`` when no literal default can be read (the caller emits an empty-string default so
    the job parameter still validates).
    """
    if isinstance(node, ast.Call):
        func = node.func
        name = func.attr if isinstance(func, ast.Attribute) else (func.id if isinstance(func, ast.Name) else "")
        if name == "Param":
            for kw in node.keywords:
                if kw.arg == "default":
                    return ops.literal_value(kw.value)
            if node.args:
                return ops.literal_value(node.args[0])
        return None
    return ops.literal_value(node)


def _shift_weekday_field(dow: str) -> str:
    """Shifts Unix-cron day-of-week numbering (0-6, Sun=0) to Quartz (1-7, Sun=1).

    Airflow/Unix: 0=Sun..6=Sat (7 also = Sun). Quartz: 1=Sun..7=Sat. Each numeric token is
    shifted +1, with 7 -> 1. Ranges/lists/steps (e.g. ``1-5``, ``0,3``, ``*/2``) have their
    numeric components shifted individually; ``*`` / ``?`` and named days pass through.
    """

    def _shift_token(token: str) -> str:
        if token.isdigit():
            n = int(token)
            return "1" if n == 7 else str(n + 1) if 0 <= n <= 6 else token
        return token

    # Split on commas (lists), then on '/' (steps) and '-' (ranges), shifting numeric pieces.
    def _shift_part(part: str) -> str:
        step = ""
        if "/" in part:
            part, _, step = part.partition("/")
            step = "/" + step
        if "-" in part:
            lo, _, hi = part.partition("-")
            shifted_lo, shifted_hi = _shift_token(lo), _shift_token(hi)
            # A range that wraps the week in Unix numbering (e.g. 5-0, Fri-Sun) shifts to a descending
            # range Quartz reads as empty; split it at the week boundary instead (6-7,1).
            if shifted_lo.isdigit() and shifted_hi.isdigit() and int(shifted_lo) > int(shifted_hi):
                head = shifted_lo if shifted_lo == "7" else f"{shifted_lo}-7"
                tail = shifted_hi if shifted_hi == "1" else f"1-{shifted_hi}"
                return f"{head},{tail}{step}"
            return f"{shifted_lo}-{shifted_hi}{step}"
        return f"{_shift_token(part)}{step}"

    return ",".join(_shift_part(p) for p in dow.split(","))


def _cron_to_quartz(cron: str) -> str | None:
    """Converts a 5-field Unix cron to a 6-field Quartz expression.

    Quartz is ``second minute hour day-of-month month day-of-week``; Unix cron
    is ``minute hour day-of-month month day-of-week``. Prepend the seconds
    field, shift the day-of-week from Unix (0-6) to Quartz (1-7) numbering, and
    reconcile the day-of-month / day-of-week wildcard (Quartz rejects ``*`` in
    both simultaneously -- one must be ``?``).
    """
    fields = cron.split()
    if len(fields) != 5:
        return None
    minute, hour, dom, month, dow = fields
    if dow not in ("*", "?"):
        dow = _shift_weekday_field(dow)
    if dom != "*" and dow not in ("*", "?"):
        # Unix cron ORs a restricted day-of-month with a restricted day-of-week; Quartz cannot express
        # both (it rejects the expression outright). Keep the day-of-week and drop the day-of-month so
        # the job is still valid -- narrower than the Airflow schedule, and flagged for review.
        dom = "?"
    elif dow == "*" and dom != "*":
        dow = "?"
    elif dom == "*":
        dom = "?"
    return f"0 {minute} {hour} {dom} {month} {dow}"


_CRON_PRESETS: dict[str, str] = {
    "@hourly": "0 0 * * * ?",
    "@daily": "0 0 0 * * ?",
    "@midnight": "0 0 0 * * ?",
    "@weekly": "0 0 0 ? * SUN",
    "@monthly": "0 0 0 1 * ?",
    "@yearly": "0 0 0 1 1 ?",
    "@annually": "0 0 0 1 1 ?",
}


_TIMEDELTA_UNIT_SECONDS: dict[str, int] = {
    "weeks": 604800,
    "days": 86400,
    "hours": 3600,
    "minutes": 60,
    "seconds": 1,
}


def _extract_timezone(node: ast.expr | None) -> str | None:
    """Extracts an IANA timezone from a ``pendulum.timezone("…")`` call or a tz string kwarg.

    Handles ``start_date=datetime(..., tzinfo=pendulum.timezone("Europe/Madrid"))``,
    ``timezone="Europe/Madrid"``, and ``pendulum.timezone("…")`` directly. Returns None
    when no literal timezone is present (caller falls back to UTC).
    """
    if node is None:
        return None
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        return node.value
    if isinstance(node, ast.Call):
        func = node.func
        name = func.attr if isinstance(func, ast.Attribute) else (func.id if isinstance(func, ast.Name) else "")
        if name in ("timezone", "timezone_") and node.args:
            return ops.literal_str(node.args[0])
        # datetime(..., tzinfo=pendulum.timezone("…")) / tz=...
        for kw in node.keywords:
            if kw.arg in ("tzinfo", "tz"):
                return _extract_timezone(kw.value)
    return None


def _timedelta_to_periodic(node: ast.expr | None) -> dict[str, object] | None:
    """Maps a ``timedelta(...)`` schedule to a ``trigger.periodic`` spec.

    Databricks periodic units are DAYS/HOURS/WEEKS. A timedelta of whole weeks/days/hours
    maps to the largest exact unit; anything finer (minutes/seconds) is expressed as a
    cron in the caller, so this returns None for those.
    """
    total = _timedelta_seconds(node)
    if total <= 0:
        return None
    for unit, unit_seconds in (("WEEKS", 604800), ("DAYS", 86400), ("HOURS", 3600)):
        if total % unit_seconds == 0:
            return {"kind": "periodic", "interval": total // unit_seconds, "unit": unit, "pause_status": "UNPAUSED"}
    return None


def _timedelta_seconds(node: ast.expr | None) -> int:
    """Returns the number of seconds in a literal timedelta call, or zero."""
    if not isinstance(node, ast.Call):
        return 0
    func = node.func
    name = func.attr if isinstance(func, ast.Attribute) else (func.id if isinstance(func, ast.Name) else "")
    if name != "timedelta":
        return 0
    total = 0
    for keyword in node.keywords:
        if keyword.arg in _TIMEDELTA_UNIT_SECONDS and isinstance(keyword.value, ast.Constant):
            if isinstance(keyword.value.value, int):
                total += keyword.value.value * _TIMEDELTA_UNIT_SECONDS[keyword.arg]
    return total


def _schedule_from_interval(
    interval: str | None,
    *,
    node: ast.expr | None = None,
    timezone: str | None = None,
) -> dict[str, object] | None:
    """Builds a Pipeline.schedule spec from an Airflow schedule.

    A string cron / preset -> ``kind: schedule`` (Quartz) with the DAG timezone;
    a ``timedelta(...)`` -> ``kind: periodic``. Returns None when neither applies.
    """
    if interval:
        if interval == "@continuous":
            return {"kind": "continuous", "pause_status": "UNPAUSED"}
        quartz: str | None = _CRON_PRESETS.get(interval) or _cron_to_quartz(interval)
        if quartz is not None:
            return {
                "kind": "schedule",
                "quartz_cron_expression": quartz,
                "timezone_id": timezone or "UTC",
                "pause_status": "UNPAUSED",
            }
    periodic = _timedelta_to_periodic(node)
    if periodic is not None:
        return periodic
    total_seconds = _timedelta_seconds(node)
    if 0 < total_seconds < 60 and 60 % total_seconds == 0:
        return {
            "kind": "schedule",
            "quartz_cron_expression": f"0/{total_seconds} * * * * ?",
            "timezone_id": timezone or "UTC",
            "pause_status": "UNPAUSED",
        }
    if total_seconds % 60 == 0:
        minutes = total_seconds // 60
        if 0 < minutes < 60 and 60 % minutes == 0:
            return {
                "kind": "schedule",
                "quartz_cron_expression": f"0 0/{minutes} * * * ?",
                "timezone_id": timezone or "UTC",
                "pause_status": "UNPAUSED",
            }
    return None


_UNRESOLVED = object()


def _import_aliases(module: ast.Module) -> dict[str, str]:
    """Returns local import bindings mapped to their canonical dotted names."""
    aliases: dict[str, str] = {}
    for node in module.body:
        if isinstance(node, ast.Import):
            for item in node.names:
                aliases[item.asname or item.name.split(".")[0]] = item.name
        elif isinstance(node, ast.ImportFrom) and node.module:
            for item in node.names:
                if item.name != "*":
                    aliases[item.asname or item.name] = f"{node.module}.{item.name}"
    return aliases


def _canonical_name(node: ast.expr, aliases: dict[str, str]) -> str:
    """Resolves an imported name or attribute chain without importing its module."""
    parts: list[str] = []
    current: ast.expr = node
    while isinstance(current, ast.Attribute):
        parts.append(current.attr)
        current = current.value
    if not isinstance(current, ast.Name):
        return ""
    root = aliases.get(current.id, current.id)
    return ".".join([root, *reversed(parts)])


def _construct_name(node: ast.expr, aliases: dict[str, str]) -> str:
    """Returns the canonical class/function leaf name for a call target."""
    canonical = _canonical_name(node, aliases)
    return canonical.rsplit(".", 1)[-1] if canonical else ""


def _safe_static_value(node: ast.expr, constants: dict[str, Any]) -> Any:
    """Evaluates the small literal expression subset used by static DAG factories."""
    if isinstance(node, ast.Constant):
        return node.value
    if isinstance(node, ast.Name):
        return constants.get(node.id, _UNRESOLVED)
    if isinstance(node, (ast.List, ast.Tuple, ast.Set)):
        values = [_safe_static_value(item, constants) for item in node.elts]
        if any(value is _UNRESOLVED for value in values):
            return _UNRESOLVED
        if isinstance(node, ast.Tuple):
            return tuple(values)
        if isinstance(node, ast.Set):
            return set(values)
        return values
    if isinstance(node, ast.Dict):
        keys = [_safe_static_value(item, constants) for item in node.keys if item is not None]
        values = [_safe_static_value(item, constants) for item in node.values]
        if len(keys) != len(node.values) or any(value is _UNRESOLVED for value in [*keys, *values]):
            return _UNRESOLVED
        return dict(zip(keys, values))
    if isinstance(node, ast.JoinedStr):
        parts: list[str] = []
        for item in node.values:
            if isinstance(item, ast.Constant) and isinstance(item.value, str):
                parts.append(item.value)
                continue
            if isinstance(item, ast.FormattedValue):
                value = _safe_static_value(item.value, constants)
                if value is not _UNRESOLVED:
                    parts.append(str(value))
                    continue
            return _UNRESOLVED
        return "".join(parts)
    if isinstance(node, ast.BinOp) and isinstance(node.op, ast.Add):
        left = _safe_static_value(node.left, constants)
        right = _safe_static_value(node.right, constants)
        if left is _UNRESOLVED or right is _UNRESOLVED:
            return _UNRESOLVED
        try:
            return left + right
        except TypeError:
            return _UNRESOLVED
    if isinstance(node, ast.UnaryOp) and isinstance(node.op, (ast.USub, ast.UAdd)):
        value = _safe_static_value(node.operand, constants)
        if value is _UNRESOLVED or not isinstance(value, (int, float)):
            return _UNRESOLVED
        return -value if isinstance(node.op, ast.USub) else value
    return _UNRESOLVED


def _value_node(value: Any) -> ast.expr:
    """Builds an expression node for a statically evaluated Python value."""
    return ast.parse(repr(value), mode="eval").body


class _ConstantSubstituter(ast.NodeTransformer):
    """Replaces known constant names and folds the supported literal subset."""

    def __init__(self, constants: dict[str, Any]) -> None:
        self.constants = constants

    def visit_Name(self, node: ast.Name) -> ast.expr:
        value = self.constants.get(node.id, _UNRESOLVED)
        return ast.copy_location(_value_node(value), node) if value is not _UNRESOLVED else node

    def generic_visit(self, node: ast.AST) -> ast.AST:
        visited = super().generic_visit(node)
        if isinstance(visited, ast.expr):
            value = _safe_static_value(visited, {})
            if value is not _UNRESOLVED:
                return ast.copy_location(_value_node(value), visited)
        return visited


def _bind_constants(node: ast.AST, constants: dict[str, Any]) -> Any:
    """Returns a deep-copied AST with known constant names substituted and folded."""
    bound = _ConstantSubstituter(constants).visit(copy.deepcopy(node))
    ast.fix_missing_locations(bound)
    if isinstance(bound, ast.expr):
        value = _safe_static_value(bound, {})
        if value is not _UNRESOLVED:
            return ast.copy_location(_value_node(value), bound)
    return bound


def _static_iteration_nodes(node: ast.expr, constants: dict[str, Any]) -> list[ast.expr] | None:
    """Returns bounded literal/range loop values, or None for a dynamic iterable."""
    if isinstance(node, ast.Call) and _construct_name(node.func, {}) == "range":
        values = [_safe_static_value(argument, constants) for argument in node.args]
        if any(value is _UNRESOLVED or not isinstance(value, int) for value in values):
            return None
        try:
            result = list(range(*values))
        except (TypeError, ValueError):
            return None
        return [_value_node(value) for value in result] if len(result) <= 256 else None
    if isinstance(node, (ast.List, ast.Tuple)):
        return [copy.deepcopy(item) for item in node.elts] if len(node.elts) <= 256 else None
    value = _safe_static_value(node, constants)
    if isinstance(value, (list, tuple)) and len(value) <= 256:
        return [_value_node(item) for item in value]
    return None


def _expand_top_level_loops(module: ast.Module) -> ast.Module:
    """Unrolls bounded module-level loops so generated DAG declarations stay distinct."""
    body: list[ast.stmt] = []
    constants: dict[str, Any] = {}
    for statement in module.body:
        if (
            isinstance(statement, ast.Assign)
            and len(statement.targets) == 1
            and isinstance(statement.targets[0], ast.Name)
        ):
            value = _safe_static_value(statement.value, constants)
            if value is not _UNRESOLVED:
                constants[statement.targets[0].id] = value
        if isinstance(statement, ast.For) and isinstance(statement.target, ast.Name):
            items = _static_iteration_nodes(statement.iter, constants)
            if items is not None:
                for item in items:
                    value = _safe_static_value(item, constants)
                    if value is _UNRESOLVED:
                        continue
                    iteration_constants = {**constants, statement.target.id: value}
                    body.extend(_bind_constants(child, iteration_constants) for child in statement.body)
                continue
        body.append(statement)
    expanded = ast.Module(body=body, type_ignores=list(module.type_ignores))
    ast.fix_missing_locations(expanded)
    return expanded


def _index_lexical_functions(
    module: ast.Module,
) -> dict[int, dict[str, list[tuple[int, bool, ast.FunctionDef]]]]:
    """Indexes function bindings by lexical scope, source order, and conditionality."""
    index: dict[int, dict[str, list[tuple[int, bool, ast.FunctionDef]]]] = {}

    def add(scope: ast.Module | ast.FunctionDef, definition: ast.FunctionDef, conditional: bool) -> None:
        by_name = index.setdefault(id(scope), {})
        by_name.setdefault(definition.name, []).append((definition.lineno, conditional, definition))

    def scan_statements(
        scope: ast.Module | ast.FunctionDef,
        statements: list[ast.stmt],
        *,
        conditional: bool,
    ) -> None:
        for statement in statements:
            if isinstance(statement, ast.FunctionDef):
                add(scope, statement, conditional)
                scan_statements(statement, statement.body, conditional=False)
                continue
            if isinstance(statement, (ast.ClassDef, ast.AsyncFunctionDef)):
                continue
            if isinstance(statement, (ast.With, ast.AsyncWith)):
                scan_statements(scope, statement.body, conditional=conditional)
                continue
            if isinstance(statement, ast.If):
                scan_statements(scope, statement.body, conditional=True)
                scan_statements(scope, statement.orelse, conditional=True)
                continue
            if isinstance(statement, (ast.For, ast.AsyncFor, ast.While)):
                scan_statements(scope, statement.body, conditional=True)
                scan_statements(scope, statement.orelse, conditional=True)
                continue
            if isinstance(statement, (ast.Try, ast.TryStar)):
                scan_statements(scope, statement.body, conditional=True)
                scan_statements(scope, statement.orelse, conditional=True)
                scan_statements(scope, statement.finalbody, conditional=True)
                for handler in statement.handlers:
                    scan_statements(scope, handler.body, conditional=True)
                continue
            if isinstance(statement, ast.Match):
                for case in statement.cases:
                    scan_statements(scope, case.body, conditional=True)

    scan_statements(module, module.body, conditional=False)
    return index


class _DagVisitor(ast.NodeVisitor):
    """Collects operator calls, dependency edges, and the DAG's schedule."""

    def __init__(self, module: ast.Module, *, target_dag_variable: str | None = None) -> None:
        self._aliases = _import_aliases(module)
        self._target_dag_variable = target_dag_variable
        # Classic python_callable resolution starts at module scope. Nested functions are only visible
        # from their lexical parent and must never overwrite a same-named module function.
        self._functions: dict[str, ast.FunctionDef] = {
            node.name: node for node in module.body if isinstance(node, ast.FunctionDef)
        }
        self._lexical_functions = _index_lexical_functions(module)
        self._scope_stack: list[ast.Module | ast.FunctionDef] = [module]
        self._resolved_callables: dict[str, tuple[str, ast.FunctionDef | None]] = {}
        self.helper_expansions: list[dict[str, Any]] = []
        self._constants: dict[str, Any] = {}
        self._task_bindings: dict[str, str | list[str]] = {}
        self._list_bindings: dict[str, list[str]] = {}
        self._capture_sequence = 0
        self.task_captures: dict[str, TaskCapture] = {}
        self.edge_captures: list[EdgeCapture] = []
        self.unclaimed_task_calls: list[ast.Call] = []
        self.unclaimed_statements: list[ast.stmt] = []
        self.unresolved_constructs: list[tuple[str, ast.AST]] = []
        self._claimed_task_call_ids: set[int] = set()
        self._claimed_statement_ids: set[int] = set()
        self._dag_scope_depth = 0
        self.captured_dag_settings: set[str] = set()
        # task variable name -> (task_id, operator, kwargs)
        self.operators: dict[str, tuple[str, str, dict[str, ast.expr]]] = {}
        # task variable name -> the operator's ast.Call node (for source-slicing placeholders)
        self.calls: dict[str, ast.Call] = {}
        self.edges: list[tuple[str, str]] = []  # (upstream_var, downstream_var)
        self.dag_id: str | None = None
        self.schedule_interval: str | None = None
        self.schedule_node: ast.expr | None = None
        self.timezone: str | None = None
        # DAG catchup= flag: True means Airflow backfills missed intervals, which maps to a native
        # Databricks backfill overriding the run_date parameter rather than any DABs schedule setting.
        self.catchup: bool = False
        self.default_args: dict[str, ast.expr] = {}
        # DAG-level params={...} defaults (param name -> literal default), so emitted job parameters
        # carry a Databricks-required default rather than an empty placeholder.
        self.dag_params: dict[str, Any] = {}
        # task variable name -> TaskGroup id prefix (for task-key namespacing)
        self.groups: dict[str, str] = {}
        self._group_stack: list[str] = []
        # `with TaskGroup(...) as tg:` binding -> the group's prefix, so a group-level edge
        # (tg >> other) can expand to edges between the groups' boundary tasks.
        self.group_vars: dict[str, str] = {}
        # task variable names defined via dynamic mapping (.expand()) -> wrapped in a for_each
        self.mapped: set[str] = set()
        # mapped var -> the kwarg names passed to .expand(). Only these fan out; a list-valued
        # .partial() arg is a fixed value and must not be mistaken for the mapped iterable.
        self.expand_kwargs: dict[str, list[str]] = {}
        self.partial_mapped: set[str] = set()
        # Disambiguates synthetic vars for operators instantiated without an assignment.
        self._bare_operator_counter = 0
        # TaskFlow: function name -> (FunctionDef, decorator dotted-name) for @task-decorated defs.
        # Pre-scanned so a @task def defined after the @dag body that uses it is still resolved.
        self.taskflow_defs: dict[str, tuple[ast.FunctionDef, str]] = {}
        # @task_group def names -- a group is a sub-pipeline, not a single renderable task, so an
        # invocation routes to a placeholder + gap rather than being expanded here.
        self.taskgroup_defs: set[str] = set()
        for fn in _iter_functions(module):
            decorator = next(
                (_decorator_name(d) for d in fn.decorator_list if _decorator_name(d) in _TASK_DECORATORS), None
            )
            if decorator is not None:
                self.taskflow_defs[fn.name] = (fn, decorator)
            elif _has_decorator(fn, _TASK_GROUP_DECORATORS):
                self.taskgroup_defs.add(fn.name)
        # TaskFlow task instances: var name -> _TaskFlowTask (id, def-name, decorator, arg bindings).
        self.taskflow_tasks: dict[str, _TaskFlowTask] = {}
        # @task_group invocations: var name -> (task_id, def-name, is_mapped).
        self.taskgroup_calls: dict[str, tuple[str, str, bool]] = {}
        self._taskflow_counter = 0
        self._taskgroup_counter = 0
        # A @dag-decorated function was found (so a bare `@task` file is still recognized as a DAG).
        self.is_taskflow_dag: bool = False

    def functions(self) -> dict[str, ast.FunctionDef]:
        taskflow = {name: definition for name, (definition, _decorator) in self.taskflow_defs.items()}
        return {**self._functions, **taskflow}

    def functions_for(self, task_var: str) -> dict[str, ast.FunctionDef]:
        """Returns module functions with a task's lexically resolved callable overlaid."""
        functions = self.functions()
        resolved = self._resolved_callables.get(task_var)
        if resolved is None:
            return functions
        name, definition = resolved
        functions.pop(name, None)
        if definition is not None:
            functions[name] = definition
        return functions

    def visit_FunctionDef(self, node: ast.FunctionDef) -> None:
        # A @task- or @task_group-decorated function defines a task / sub-pipeline from its body,
        # which is internal logic rather than DAG structure, so don't descend. @dag marks the
        # DAG-defining function: read its config off the decorator, then descend so the body's task
        # instances / edges are collected.
        if _has_decorator(node, _TASK_DECORATORS) or _has_decorator(node, _TASK_GROUP_DECORATORS):
            if self._dag_scope_depth:
                self._claimed_statement_ids.add(id(node))
            return
        is_dag_definition = _has_decorator(node, _DAG_DECORATORS)
        if not is_dag_definition:
            if self._dag_scope_depth:
                self._claimed_statement_ids.add(id(node))
            return
        if is_dag_definition:
            self.is_taskflow_dag = True
            dag_kwargs = _decorator_kwargs(node.decorator_list, _DAG_DECORATORS)
            self._apply_dag_kwargs(dag_kwargs)
            if self.dag_id is None:
                self.dag_id = ops.literal_str(dag_kwargs.get("dag_id")) or node.name
        self._scope_stack.append(node)
        if is_dag_definition:
            self._dag_scope_depth += 1
        try:
            for statement in node.body:
                if is_dag_definition:
                    self._visit_dag_statement(statement)
                else:
                    self.visit(statement)
        finally:
            if is_dag_definition:
                self._dag_scope_depth -= 1
            self._scope_stack.pop()

    def visit_Assign(self, node: ast.Assign) -> None:
        if len(node.targets) == 1 and isinstance(node.targets[0], ast.Name) and isinstance(node.value, ast.Call):
            var = node.targets[0].id
            if _construct_name(node.value.func, self._aliases) == "DAG":
                self._read_dag_kwargs(node.value)
                self.dag_id = self.dag_id or var
                self._claimed_statement_ids.add(id(node))
                return
            internal_var = self._new_task_var(var, node.value)
            if self._register_operator_call(node.value, internal_var, binding=var):
                self._claimed_statement_ids.add(id(node))
                pass  # a `x = SomeOperator(...)` (optionally .expand()-mapped) instantiation
            elif self._register_helper_factory_call(node.value, internal_var, binding=var):
                self._claimed_statement_ids.add(id(node))
                pass
            elif self._register_taskflow_call(node.value, internal_var):
                self._task_bindings[var] = internal_var
                self._claimed_statement_ids.add(id(node))
                pass  # a `x = mytask(...)` TaskFlow invocation, captured with var as its key
            else:
                if self._register_taskgroup_call(node.value, internal_var):
                    self._task_bindings[var] = internal_var
                    self._claimed_statement_ids.add(id(node))
                    return
        elif len(node.targets) == 1 and isinstance(node.targets[0], ast.Name):
            target = node.targets[0].id
            if isinstance(node.value, ast.Name):
                resolved = self._resolve_task_names(node.value)
                if resolved:
                    self._task_bindings[target] = resolved[0] if len(resolved) == 1 else resolved
                    self._constants.pop(target, None)
                    self._claimed_statement_ids.add(id(node))
                    return
            value = _safe_static_value(node.value, self._constants)
            if value is not _UNRESOLVED:
                self._constants[target] = value
                self._task_bindings.pop(target, None)
                if isinstance(value, list):
                    self._list_bindings[target] = []
                self._claimed_statement_ids.add(id(node))
                return
        self.generic_visit(node)

    def _new_task_var(self, binding: str, node: ast.AST) -> str:
        """Allocates an internal identity while preserving Python's latest name binding."""
        if binding not in self.operators and binding not in self.taskflow_tasks and binding not in self.taskgroup_calls:
            return binding
        self._capture_sequence += 1
        return f"{binding}__L{getattr(node, 'lineno', 0)}_{self._capture_sequence}"

    def _register_operator_call(self, node: ast.Call, var: str, *, binding: str | None = None) -> bool:
        """Registers a classic operator/sensor instantiation under the task variable *var*.

        Airflow registers a task when the operator is instantiated inside a DAG context; assigning it
        to a name is a Python convenience, not a requirement. So this is shared by the assigned form
        and the bare-statement / bare-chain forms, which synthesise *var* from the task_id.

        Returns True when *node* was a (possibly ``.expand()``-mapped) operator call.
        """
        direct = _direct_operator_call(node, self._aliases)
        mapped = None if direct is not None else _mapped_operator_call(node, self._aliases)
        call = direct or (mapped[0] if mapped is not None else None)
        if call is None:
            return False
        construct = _construct_name(call.func, self._aliases)
        kwargs = {kw.arg: _bind_constants(kw.value, self._constants) for kw in call.keywords if kw.arg}
        dag_node = kwargs.get("dag")
        if self._target_dag_variable is not None and not (
            isinstance(dag_node, ast.Name) and dag_node.id == self._target_dag_variable
        ):
            return False
        call = ast.Call(
            func=ast.Name(id=construct, ctx=ast.Load()),
            args=[],
            keywords=[ast.keyword(arg=key, value=value) for key, value in kwargs.items()],
        )
        ast.copy_location(call, node)
        task_id = ops.literal_str(kwargs.get("task_id")) or ops.literal_str(kwargs.get("group_id")) or var
        self.operators[var] = (task_id, construct, kwargs)
        self.calls[var] = call
        self._task_bindings[binding or var] = var
        self.task_captures[var] = TaskCapture(
            capture_id=var,
            variable=binding or var,
            task_id=task_id,
            operator=construct,
            call=call,
            span=_span(node),
        )
        self._claimed_task_call_ids.add(id(node))
        callable_node = kwargs.get("python_callable")
        if isinstance(callable_node, ast.Name):
            self._resolved_callables[var] = (
                callable_node.id,
                self._resolve_lexical_function(callable_node.id, node),
            )
        if mapped is not None:
            self.mapped.add(var)
            self.expand_kwargs[var] = mapped[1]
            if mapped[2]:
                self.partial_mapped.add(var)
        if self._group_stack:
            self.groups[var] = "__".join(self._group_stack)
        return True

    def _register_helper_factory_call(self, node: ast.Call, var: str, *, binding: str) -> bool:
        """Expands the deliberately narrow single-return operator factory shape."""
        helper_return = self._helper_factory_return(node)
        if helper_return is None:
            return False
        helper, return_call = helper_return
        parameters = [*helper.args.posonlyargs, *helper.args.args, *helper.args.kwonlyargs]
        if len(node.args) > len(parameters) or any(keyword.arg is None for keyword in node.keywords):
            return False
        bound: dict[str, Any] = {}
        for parameter, argument in zip(parameters, node.args):
            bound[parameter.arg] = _bind_constants(argument, self._constants)
        for keyword in node.keywords:
            if keyword.arg:
                bound[keyword.arg] = _bind_constants(keyword.value, self._constants)
        missing = [parameter.arg for parameter in parameters if parameter.arg not in bound]
        positional_defaults = [None] * (len(helper.args.args) - len(helper.args.defaults)) + list(helper.args.defaults)
        defaults = {
            parameter.arg: default
            for parameter, default in zip(helper.args.args, positional_defaults)
            if default is not None
        }
        defaults.update(
            {
                parameter.arg: default
                for parameter, default in zip(helper.args.kwonlyargs, helper.args.kw_defaults)
                if default is not None
            }
        )
        for name in missing:
            if name not in defaults:
                return False
            bound[name] = defaults[name]
        constants = dict(self._constants)
        for name, expression in bound.items():
            if isinstance(expression, ast.expr):
                value = _safe_static_value(expression, constants)
                if value is _UNRESOLVED:
                    return False
                constants[name] = value
        factory_call = _bind_constants(return_call, constants)
        registered = isinstance(factory_call, ast.Call) and self._register_operator_call(
            factory_call, var, binding=binding
        )
        if registered:
            self._claimed_task_call_ids.add(id(node))
            self.helper_expansions.append(
                {
                    "code": "helper_factory_expanded",
                    "capture_id": var,
                    "helper": helper.name,
                    "helper_line": helper.lineno,
                    "invocation_line": getattr(node, "lineno", 0),
                }
            )
        return registered

    def _helper_factory_return(self, node: ast.Call) -> tuple[ast.FunctionDef, ast.Call] | None:
        """Returns the operator call from a supported single-return helper invocation."""
        if not isinstance(node.func, ast.Name):
            return None
        helper = self._resolve_lexical_function(node.func.id, node)
        if helper is None or helper.decorator_list or helper.args.vararg or helper.args.kwarg:
            return None
        body = list(helper.body)
        if body and isinstance(body[0], ast.Expr) and isinstance(body[0].value, ast.Constant):
            if isinstance(body[0].value.value, str):
                body = body[1:]
        if len(body) != 1 or not isinstance(body[0], ast.Return) or not isinstance(body[0].value, ast.Call):
            return None
        if _direct_operator_call(body[0].value, self._aliases) is None:
            return None
        return helper, body[0].value

    def _resolve_lexical_function(self, name: str, reference: ast.AST) -> ast.FunctionDef | None:
        """Resolves a function name by lexical scope and source-order binding semantics."""
        line = getattr(reference, "lineno", 0)
        for scope in reversed(self._scope_stack):
            events = self._lexical_functions.get(id(scope), {}).get(name, [])
            visible = [event for event in events if event[0] <= line]
            if visible:
                _event_line, conditional, definition = visible[-1]
                return None if conditional else definition
            if events and isinstance(scope, ast.FunctionDef):
                return None
        return None

    def _register_bare_operator_call(self, node: ast.Call) -> str | None:
        """Registers an operator instantiated without an assignment, keyed by a synthetic var.

        The var is derived from the literal ``task_id`` (which is what the emitted task key comes from
        anyway), with a counter suffix if two bare operators somehow share one.
        """
        if _direct_operator_call(node, self._aliases) is None and _mapped_operator_call(node, self._aliases) is None:
            return None
        kwargs = {kw.arg: kw.value for kw in node.keywords if kw.arg}
        base = ops.literal_str(kwargs.get("task_id")) or ops.literal_str(kwargs.get("group_id"))
        if base is None:
            # `.expand()` chains carry task_id on the inner .partial(...) call, not the outer one.
            mapped = _mapped_operator_call(node, self._aliases)
            if mapped is not None:
                inner_kwargs = {kw.arg: kw.value for kw in mapped[0].keywords if kw.arg}
                base = ops.literal_str(inner_kwargs.get("task_id")) or ops.literal_str(inner_kwargs.get("group_id"))
        if base is None:
            self._bare_operator_counter += 1
            base = f"_bare_task{self._bare_operator_counter}"
        var = base
        while var in self.operators:
            self._bare_operator_counter += 1
            var = f"{base}__{self._bare_operator_counter}"
        return var if self._register_operator_call(node, var, binding=var) else None

    def _taskflow_def_name(self, call: ast.Call) -> tuple[str | None, bool, str | None]:
        """Resolves a call's underlying ``@task`` def name, unwrapping the mapping/config chain.

        Handles ``.expand(...)`` / ``.expand_kwargs(...)`` (both set ``is_mapped``) and the
        ``.override(...)`` / ``.partial(...)`` config calls, in any order, so forms like
        ``op.partial(...).expand(...)`` resolve. Returns ``(def_name_or_None, is_mapped, override_id)``.
        """
        func = call.func
        mapped = False
        override_id: str | None = None
        while True:
            if isinstance(func, ast.Attribute):
                if func.attr in ("expand", "expand_kwargs"):
                    mapped = True
                func = func.value
                continue
            if isinstance(func, ast.Call) and isinstance(func.func, ast.Attribute):
                config_call = func.func
                if config_call.attr == "override":
                    arguments = {keyword.arg: keyword.value for keyword in func.keywords if keyword.arg}
                    override_id = ops.literal_str(arguments.get("task_id"))
                    func = config_call.value
                    continue
                if config_call.attr == "partial":
                    func = config_call.value
                    continue
            break
        if isinstance(func, ast.Name) and func.id in self.taskflow_defs:
            return func.id, mapped, override_id
        return None, mapped, override_id

    def _register_taskflow_call(self, call: ast.Call, var: str) -> bool:
        """Records a TaskFlow ``@task`` invocation as a task instance keyed by *var*.

        Binds each call argument that references (or nests) another ``@task`` to that upstream task
        var -- TaskFlow's implicit XCom data flow (``transform(extract())`` wires extract ->
        transform). Nested calls (``load(transform(extract()))``) register their own instances
        recursively. A ``.override(task_id=...)`` renames the task. Returns True when captured.
        """
        def_name, mapped, override_id = self._taskflow_def_name(call)
        if def_name is None:
            return False
        _fn, decorator = self.taskflow_defs[def_name]
        task = _TaskFlowTask(task_id=override_id or var, def_name=def_name, decorator=decorator)
        self.taskflow_tasks[var] = task
        self.calls[var] = call
        self._claimed_task_call_ids.add(id(call))
        if mapped:
            self.mapped.add(var)
            # ``.expand(param=<iterable>)`` args live on the outer call; capture the single mapped
            # parameter + its literal iterable (Tier 1). A non-literal iterable leaves items None,
            # which routes the task to the agentic-gap round in _build_taskflow_task.
            self._capture_expand(task, call)
            # A mapped iterable OR a .partial(...) fixed arg can be an upstream task's output
            # (``process.partial(x=raw).expand(y=vals)``); wire those data-flow edges so the mapped
            # task still depends on its producers, whether it lowers to a for_each or a placeholder.
            for mapped_arg in _mapping_chain_args(call):
                dep = self._resolve_taskflow_arg(mapped_arg)
                if dep is not None and dep != var:
                    self._add_edges([dep], [var], call)
        if self._group_stack:
            self.groups[var] = "__".join(self._group_stack)
        if mapped:
            return True
        # Bind each arg that resolves to an upstream task var, and add the data-flow edge.
        for index, arg in enumerate(call.args):
            dep = self._resolve_taskflow_arg(arg)
            if dep is not None:
                task.positional_deps[index] = dep
                self._add_edges([dep], [var], call)
            else:
                value = _literal_argument_source(arg)
                if value is None:
                    task.unresolved_arguments.append(ast.unparse(arg))
                else:
                    task.positional_values[index] = value
        for kw in call.keywords:
            if kw.arg is None:
                task.unresolved_arguments.append(f"**{ast.unparse(kw.value)}")
                continue
            dep = self._resolve_taskflow_arg(kw.value)
            if dep is not None:
                task.keyword_deps[kw.arg] = dep
                self._add_edges([dep], [var], call)
            else:
                value = _literal_argument_source(kw.value)
                if value is None:
                    task.unresolved_arguments.append(f"{kw.arg}={ast.unparse(kw.value)}")
                else:
                    task.keyword_values[kw.arg] = value
        return True

    def _capture_expand(self, task: _TaskFlowTask, call: ast.Call) -> None:
        """Captures a ``@task.expand(param=<iterable>)`` mapping onto *task*.

        Tier 1 (deterministic -> for_each_task): a plain ``.expand(...)`` with exactly one mapped
        parameter whose iterable is a literal list, and no ``.partial(...)`` fixed args (a for_each
        inner task can't carry them). Anything else -- ``.expand_kwargs``, multiple mapped params, a
        ``.partial(...).expand(...)`` chain, or a non-literal iterable -- leaves ``expand_items_json``
        None so _build_taskflow_task routes the task to the agentic-gap round.
        """
        if not (isinstance(call.func, ast.Attribute) and call.func.attr == "expand"):
            return  # .expand_kwargs(...) or other mapping form -> not Tier 1
        if _has_partial_call(call.func.value):
            return  # .partial(...) fixed args can't be represented on a for_each inner task
        keywords = [kw for kw in call.keywords if kw.arg]
        if len(keywords) != 1 or len(keywords) != len(call.keywords):
            return  # 0 / multiple mapped params, or **expand_kwargs -> not Tier 1
        keyword = keywords[0]
        task.expand_kwarg = keyword.arg
        value = ops.literal_value(keyword.value)
        if isinstance(value, list):
            # Encode each element as its own JSON text, so the for_each `inputs` is a list of JSON
            # strings and the inner notebook's json.loads unambiguously recovers the original value.
            # (A bare list like [1, 2, 3] would make `{{input}}` deliver "1"/"2"/"3" -- indistinguishable
            # from the string elements ["1", "2", "3"]; wrapping each element removes that ambiguity.)
            task.expand_items_json = json.dumps([json.dumps(element) for element in value])

    def _register_taskgroup_call(self, call: ast.Call, var: str | None) -> bool:
        """Records a ``@task_group`` invocation (``pair(...)`` / ``pair.expand(...)``) as a placeholder.

        A ``@task_group`` is a sub-pipeline of tasks, not a single renderable callable, so it can't be
        mechanically lowered here -- it's captured (keyed by *var*, or a synthetic name for a bare
        call) so an edge to/from it resolves, and emitted as a placeholder + gap for the agentic round.
        Returns True when the call resolved to a known group def.
        """
        func = call.func
        mapped = False
        while isinstance(func, ast.Attribute):
            if func.attr == "expand":
                mapped = True
            func = func.value
        if not (isinstance(func, ast.Name) and func.id in self.taskgroup_defs):
            return False
        def_name = func.id
        if var is None:
            self._taskgroup_counter += 1
            var = f"{def_name}__tg{self._taskgroup_counter}"
        self.taskgroup_calls[var] = (var, def_name, mapped)
        self._claimed_task_call_ids.add(id(call))
        if self._group_stack:
            self.groups[var] = "__".join(self._group_stack)
        return True

    def _resolve_taskflow_arg(self, arg: ast.expr) -> str | None:
        """Returns the upstream task var an argument refers to, else None (a literal / unknown).

        A bare ``Name`` is an existing task var. A nested ``@task`` call (``transform(extract())``)
        is registered as its own synthetic task instance and its var returned, so the whole
        expression tree becomes a chain of task instances.
        """
        if isinstance(arg, ast.Name):
            resolved = self._resolve_task_names(arg)
            if len(resolved) == 1:
                return resolved[0]
        if isinstance(arg, ast.Call):
            def_name, _mapped, _override = self._taskflow_def_name(arg)
            if def_name is not None:
                self._taskflow_counter += 1
                synthetic = f"{def_name}__tf{self._taskflow_counter}"
                self._register_taskflow_call(arg, synthetic)
                return synthetic
        return None

    def visit_With(self, node: ast.With) -> None:
        pushed_group = False
        opens_dag_scope = False
        for item in node.items:
            call = item.context_expr
            if isinstance(call, ast.Call):
                construct = _construct_name(call.func, self._aliases)
                if construct == "DAG":
                    self._read_dag_kwargs(call)
                    opens_dag_scope = True
                elif construct == "TaskGroup":
                    # `with TaskGroup("etl") as tg:` — namespace the member tasks by group id.
                    kwargs = {kw.arg: kw.value for kw in call.keywords if kw.arg}
                    group_id = (
                        ops.literal_str(kwargs.get("group_id"))
                        or (ops.literal_str(call.args[0]) if call.args else None)
                        or "group"
                    )
                    self._group_stack.append(_sanitize_task_key(group_id))
                    pushed_group = True
                    # Record the `as tg` binding (with the full nested prefix) so a group-level
                    # edge on `tg` resolves to the group's member tasks.
                    if isinstance(item.optional_vars, ast.Name):
                        self.group_vars[item.optional_vars.id] = "__".join(self._group_stack)
                elif _is_task_construct(construct) and item.optional_vars is not None:
                    # `with DbtTaskGroup(...) as g:` — a cosmos group bound to a name.
                    if isinstance(item.optional_vars, ast.Name):
                        var = item.optional_vars.id
                        kwargs = {kw.arg: kw.value for kw in call.keywords if kw.arg}
                        task_id = ops.literal_str(kwargs.get("group_id")) or var
                        self.operators[var] = (task_id, construct, kwargs)
                        self.calls[var] = call
        if opens_dag_scope:
            self._dag_scope_depth += 1
            try:
                for statement in node.body:
                    self._visit_dag_statement(statement)
            finally:
                self._dag_scope_depth -= 1
        elif self._dag_scope_depth:
            for statement in node.body:
                self._visit_dag_statement(statement)
        else:
            self.generic_visit(node)
        if pushed_group:
            self._group_stack.pop()
        self._claimed_statement_ids.add(id(node))

    def _visit_dag_statement(self, statement: ast.stmt) -> None:
        """Visits one DAG-body statement and records any unclaimed structural source."""
        unclaimed_calls_before = len(self.unclaimed_task_calls)
        unresolved_before = len(self.unresolved_constructs)
        self.visit(statement)
        if id(statement) in self._claimed_statement_ids:
            return
        if (
            len(self.unclaimed_task_calls) > unclaimed_calls_before
            or len(self.unresolved_constructs) > unresolved_before
        ):
            return
        if isinstance(statement, (ast.Import, ast.ImportFrom, ast.Pass, ast.Return)):
            self._claimed_statement_ids.add(id(statement))
            return
        if isinstance(statement, ast.Expr) and isinstance(statement.value, ast.Constant):
            self._claimed_statement_ids.add(id(statement))
            return
        self.unclaimed_statements.append(statement)

    def visit_Call(self, node: ast.Call) -> None:
        """Fails closed when a task-producing call in a DAG scope was not captured."""
        in_selected_assigned_dag = False
        is_assigned_task_factory = False
        if self._target_dag_variable is not None:
            direct = _direct_operator_call(node, self._aliases)
            mapped = None if direct is not None else _mapped_operator_call(node, self._aliases)
            operator_call = direct or (mapped[0] if mapped is not None else None)
            if operator_call is not None:
                dag_argument = next((kw.value for kw in operator_call.keywords if kw.arg == "dag"), None)
                in_selected_assigned_dag = (
                    isinstance(dag_argument, ast.Name) and dag_argument.id == self._target_dag_variable
                )
            is_assigned_task_factory = self._helper_targets_assigned_dag(node)
            in_selected_assigned_dag = in_selected_assigned_dag or is_assigned_task_factory
        if (self._dag_scope_depth or in_selected_assigned_dag) and id(node) not in self._claimed_task_call_ids:
            is_operator = _direct_operator_call(node, self._aliases) is not None
            is_mapped_operator = _mapped_operator_call(node, self._aliases) is not None
            is_taskflow = self._taskflow_def_name(node)[0] is not None
            is_taskgroup = any(
                isinstance(candidate, ast.Name) and candidate.id in self.taskgroup_defs
                for candidate in ast.walk(node.func)
            )
            if (
                is_operator
                or is_mapped_operator
                or is_taskflow
                or is_taskgroup
                or is_assigned_task_factory
                or self._helper_factory_return(node)
            ):
                self.unclaimed_task_calls.append(node)
        self.generic_visit(node)

    def _helper_targets_assigned_dag(self, call: ast.Call) -> bool:
        """Returns whether a local helper can construct a task for the selected assigned DAG."""
        if self._target_dag_variable is None or not isinstance(call.func, ast.Name):
            return False
        helper = self._resolve_lexical_function(call.func.id, call)
        if helper is None:
            return False
        parameters = [*helper.args.posonlyargs, *helper.args.args, *helper.args.kwonlyargs]
        bound: dict[str, ast.expr] = {parameter.arg: argument for parameter, argument in zip(parameters, call.args)}
        bound.update({keyword.arg: keyword.value for keyword in call.keywords if keyword.arg})
        for candidate in ast.walk(helper):
            if not isinstance(candidate, ast.Call):
                continue
            direct = _direct_operator_call(candidate, self._aliases)
            mapped = None if direct is not None else _mapped_operator_call(candidate, self._aliases)
            operator_call = direct or (mapped[0] if mapped is not None else None)
            if operator_call is None:
                continue
            dag_argument = next((keyword.value for keyword in operator_call.keywords if keyword.arg == "dag"), None)
            if not isinstance(dag_argument, ast.Name):
                continue
            if dag_argument.id == self._target_dag_variable:
                return True
            bound_argument = bound.get(dag_argument.id)
            if isinstance(bound_argument, ast.Name) and bound_argument.id == self._target_dag_variable:
                return True
        return False

    def _read_dag_kwargs(self, call: ast.Call) -> None:
        kwargs = {kw.arg: _bind_constants(kw.value, self._constants) for kw in call.keywords if kw.arg}
        self.dag_id = ops.literal_str(kwargs.get("dag_id"))
        self._apply_dag_kwargs(kwargs)

    def _apply_dag_kwargs(self, kwargs: dict[str, ast.expr]) -> None:
        self.captured_dag_settings.update(kwargs)
        self.schedule_node = kwargs.get("schedule_interval") or kwargs.get("schedule")
        self.schedule_interval = ops.literal_str(kwargs.get("schedule_interval")) or ops.literal_str(
            kwargs.get("schedule")
        )
        self.timezone = _extract_timezone(kwargs.get("start_date")) or _extract_timezone(kwargs.get("timezone"))
        self.catchup = ops.literal_value(kwargs.get("catchup")) is True
        # default_args is a dict literal of DAG-wide task settings (retries, timeouts, email).
        default_args = kwargs.get("default_args")
        if isinstance(default_args, ast.Dict):
            self.default_args = {
                key.value: val
                for key, val in zip(default_args.keys, default_args.values)
                if isinstance(key, ast.Constant) and isinstance(key.value, str)
            }
            self.captured_dag_settings.update(f"default_args.{name}" for name in self.default_args)
        # params={...} supplies DAG parameter defaults; each value is a literal or a Param(default=...).
        params = kwargs.get("params")
        if isinstance(params, ast.Dict):
            for key, val in zip(params.keys, params.values):
                if isinstance(key, ast.Constant) and isinstance(key.value, str):
                    self.dag_params[key.value] = _param_default(val)

    def visit_Expr(self, node: ast.Expr) -> None:
        # Dependency edges come from two forms:
        #   - shift chains: `a >> b >> c`, `a >> [b, c]`, `[a, b] >> c`, `a << b`
        #   - method calls: `a.set_upstream(b)` / `a.set_downstream([b, c])`
        value = node.value
        if isinstance(value, ast.BinOp) and isinstance(value.op, (ast.RShift, ast.LShift)):
            before = len(self.edge_captures)
            self._collect_shift_chain(value)
            if len(self.edge_captures) > before:
                self._claimed_statement_ids.add(id(node))
        elif isinstance(value, ast.Call):
            call_name = _construct_name(value.func, self._aliases)
            if call_name == "chain":
                positions = [self._resolve_task_names(argument) for argument in value.args]
                for left, right in zip(positions, positions[1:]):
                    self._add_edges(left, right, value)
                self._claimed_statement_ids.add(id(node))
                return
            if call_name == "cross_downstream" and len(value.args) >= 2:
                self._add_edges(
                    self._resolve_task_names(value.args[0]),
                    self._resolve_task_names(value.args[1]),
                    value,
                )
                self._claimed_statement_ids.add(id(node))
                return
            if isinstance(value.func, ast.Attribute) and value.func.attr == "append" and value.args:
                owner = value.func.value
                if isinstance(owner, ast.Name):
                    appended = value.args[0]
                    if isinstance(appended, ast.Call):
                        internal = self._register_bare_operator_call(appended)
                        if internal is not None:
                            self._list_bindings.setdefault(owner.id, []).append(internal)
                            self._claimed_statement_ids.add(id(node))
                            return
                    resolved = self._resolve_task_names(appended)
                    if resolved:
                        self._list_bindings.setdefault(owner.id, []).extend(resolved)
                        self._claimed_statement_ids.add(id(node))
                        return
            # A bare TaskFlow call (`extract()` with no assignment) is a task instance keyed by its
            # def name; otherwise it may be a set_upstream/set_downstream dependency call.
            def_name, _mapped, _override = self._taskflow_def_name(value)
            if def_name is not None:
                task_var = def_name
                if task_var in self.taskflow_tasks:
                    self._taskflow_counter += 1
                    task_var = f"{def_name}__tf{self._taskflow_counter}"
                self._register_taskflow_call(value, task_var)
                self._claimed_statement_ids.add(id(node))
            elif self._register_bare_operator_call(value) is not None:
                self._claimed_statement_ids.add(id(node))
                pass  # a bare `SomeOperator(task_id=...)` statement -- registered under a synthetic var
            elif self._register_taskgroup_call(value, None):
                self._claimed_statement_ids.add(id(node))
            else:
                before = len(self.edge_captures)
                self._collect_set_dependency(value)
                if len(self.edge_captures) > before:
                    self._claimed_statement_ids.add(id(node))
        self.generic_visit(node)

    def visit_For(self, node: ast.For) -> None:
        """Executes bounded literal/range loops with Python name rebinding semantics."""
        if not isinstance(node.target, ast.Name):
            self.unresolved_constructs.append(("dynamic_loop_target", node))
            self._claimed_statement_ids.add(id(node))
            return
        items = _static_iteration_nodes(node.iter, self._constants)
        if items is None:
            # A tuple/list of task variables is also statically bounded even though the values are
            # capture identities rather than Python literals.
            if isinstance(node.iter, (ast.List, ast.Tuple)):
                items = list(node.iter.elts)
            else:
                self.unresolved_constructs.append(("dynamic_loop_iterable", node))
                self._claimed_statement_ids.add(id(node))
                return
        for item in items:
            resolved_tasks = self._resolve_task_names(item)
            if resolved_tasks:
                self._task_bindings[node.target.id] = resolved_tasks[0] if len(resolved_tasks) == 1 else resolved_tasks
                self._constants.pop(node.target.id, None)
            else:
                value = _safe_static_value(item, self._constants)
                if value is _UNRESOLVED:
                    self.unresolved_constructs.append(("dynamic_loop_value", item))
                    self._claimed_statement_ids.add(id(node))
                    return
                self._constants[node.target.id] = value
                self._task_bindings.pop(node.target.id, None)
            for statement in node.body:
                self._visit_dag_statement(statement) if self._dag_scope_depth else self.visit(statement)
        for statement in node.orelse:
            self._visit_dag_statement(statement) if self._dag_scope_depth else self.visit(statement)
        self._claimed_statement_ids.add(id(node))

    def visit_If(self, node: ast.If) -> None:
        """Follows a statically decidable branch; records ambiguous control flow explicitly."""
        value = _safe_static_value(node.test, self._constants)
        if value is _UNRESOLVED and isinstance(node.test, ast.Name) and node.test.id in self._task_bindings:
            value = True
        if value is _UNRESOLVED:
            self.unresolved_constructs.append(("ambiguous_condition", node))
            self._claimed_statement_ids.add(id(node))
            return
        branch = node.body if bool(value) else node.orelse
        for statement in branch:
            self._visit_dag_statement(statement) if self._dag_scope_depth else self.visit(statement)
        self._claimed_statement_ids.add(id(node))

    def _collect_shift_chain(self, binop: ast.BinOp) -> None:
        self._collect_shift_expression(binop)

    def _collect_shift_expression(self, node: ast.expr) -> list[str]:
        """Collects each shift edge recursively and returns the expression's chain result."""
        if not isinstance(node, ast.BinOp) or not isinstance(node.op, (ast.RShift, ast.LShift)):
            return self._shift_position_names(node)
        left = self._collect_shift_expression(node.left)
        right = self._collect_shift_expression(node.right)
        upstream, downstream = (left, right) if isinstance(node.op, ast.RShift) else (right, left)
        self._add_edges(upstream, downstream, node)
        return right

    def _shift_position_names(self, node: ast.expr) -> list[str]:
        # A shift-chain position resolves to task vars. An inline TaskFlow call (`extract()`) is
        # registered as its own instance so `prep >> finalize()` doesn't drop finalize.
        if isinstance(node, (ast.List, ast.Tuple, ast.Name)):
            return self._resolve_task_names(node)
        if isinstance(node, ast.Call):
            def_name, _mapped, _override = self._taskflow_def_name(node)
            if def_name is not None:
                self._taskflow_counter += 1
                synthetic = f"{def_name}__tf{self._taskflow_counter}"
                self._register_taskflow_call(node, synthetic)
                return [synthetic]
            # An inline classic operator (`Op(...) >> Op(...)` with no assignments) is still a task.
            bare_var = self._register_bare_operator_call(node)
            if bare_var is not None:
                return [bare_var]
        return []

    def _resolve_task_names(self, node: ast.expr) -> list[str]:
        """Resolves current Python bindings to stable task capture identities."""
        if isinstance(node, ast.Name):
            binding = self._task_bindings.get(node.id)
            if isinstance(binding, str):
                return [binding]
            if isinstance(binding, list):
                return list(binding)
            if node.id in self._list_bindings:
                return list(self._list_bindings[node.id])
            if node.id in self.group_vars:
                return [node.id]
            if node.id in self.operators or node.id in self.taskflow_tasks or node.id in self.taskgroup_calls:
                return [node.id]
            return []
        if isinstance(node, (ast.List, ast.Tuple)):
            return [task for item in node.elts for task in self._resolve_task_names(item)]
        return []

    def _add_edges(self, upstreams: list[str], downstreams: list[str], node: ast.AST) -> None:
        for upstream_var in upstreams:
            for downstream_var in downstreams:
                self.edges.append((upstream_var, downstream_var))
                self.edge_captures.append(
                    EdgeCapture(upstream_id=upstream_var, downstream_id=downstream_var, span=_span(node))
                )

    def _collect_set_dependency(self, call: ast.Call) -> None:
        # `x.set_upstream(y)` / `x.set_downstream(y)` where y is a Name or a list of Names.
        func = call.func
        if not (isinstance(func, ast.Attribute) and isinstance(func.value, ast.Name) and call.args):
            return
        this_names = self._resolve_task_names(func.value)
        others = self._resolve_task_names(call.args[0])
        if func.attr == "set_downstream":
            self._add_edges(this_names, others, call)
        elif func.attr == "set_upstream":
            self._add_edges(others, this_names, call)


def _expand_group_edges(
    edges: list[tuple[str, str]],
    groups: dict[str, str],
    group_vars: dict[str, str],
) -> list[tuple[str, str]]:
    """Rewrites edges whose endpoint is a ``TaskGroup`` var into task-to-task edges.

    A group endpoint expands to its boundary tasks: as an upstream, the group's *leaves* (members
    with no downstream inside the group); as a downstream, the group's *roots* (members with no
    upstream inside the group). Airflow connects leaves(upstream) -> roots(downstream). A non-group
    var resolves to itself. Membership includes nested subgroups (prefix match).
    """
    if not group_vars:
        return edges

    # Group prefix -> member task vars (a member's group prefix equals or nests under the group's).
    def _members(prefix: str) -> list[str]:
        return [var for var, gp in groups.items() if gp == prefix or gp.startswith(prefix + "__")]

    # Intra-group edges decide which members are roots (no in-group upstream) / leaves (no
    # in-group downstream). Edges here are still in var terms.
    def _roots_leaves(prefix: str) -> tuple[list[str], list[str]]:
        members = set(_members(prefix))
        has_in_up = {v: False for v in members}
        has_in_down = {v: False for v in members}
        for up, down in edges:
            if up in members and down in members:
                has_in_down[up] = True
                has_in_up[down] = True
        roots = [v for v in members if not has_in_up[v]]
        leaves = [v for v in members if not has_in_down[v]]
        return roots or list(members), leaves or list(members)

    def _resolve(var: str, *, as_upstream: bool) -> list[str]:
        prefix = group_vars.get(var)
        if prefix is None:
            return [var]
        roots, leaves = _roots_leaves(prefix)
        return leaves if as_upstream else roots

    expanded: list[tuple[str, str]] = []
    for up, down in edges:
        if up not in group_vars and down not in group_vars:
            expanded.append((up, down))
            continue
        for u in _resolve(up, as_upstream=True):
            for d in _resolve(down, as_upstream=False):
                if u != d:
                    expanded.append((u, d))
    return expanded


def _has_partial_call(node: ast.expr) -> bool:
    """True when a ``@task`` mapping chain contains a ``.partial(...)`` config call."""
    current: ast.expr = node
    while True:
        if isinstance(current, ast.Call):
            if isinstance(current.func, ast.Attribute) and current.func.attr == "partial":
                return True
            current = current.func
        elif isinstance(current, ast.Attribute):
            current = current.value
        else:
            return False


def _mapping_chain_args(node: ast.expr) -> list[ast.expr]:
    """Every argument expression across a ``@task`` mapping chain's call nodes.

    Walks ``op.partial(x=up).expand(y=vals)`` (and ``.override(...)``), collecting the args of every
    ``.partial`` / ``.expand`` / ``.expand_kwargs`` call so upstream-task references in either the
    fixed args or the mapped iterable are found for data-flow edge wiring.
    """
    args: list[ast.expr] = []
    current: ast.expr = node
    while isinstance(current, (ast.Call, ast.Attribute)):
        if isinstance(current, ast.Call):
            args.extend(current.args)
            args.extend(kw.value for kw in current.keywords)
            current = current.func
        else:
            current = current.value
    return args


def _iter_functions(module: ast.Module) -> list[ast.FunctionDef]:
    """All FunctionDefs in *module*, including those nested inside a ``@dag`` function body.

    TaskFlow ``@task`` defs are often nested inside the ``@dag`` function, so a top-level-only scan
    would miss them. Async defs are skipped (flowx renders sync notebooks).
    """
    found: list[ast.FunctionDef] = []
    for node in ast.walk(module):
        if isinstance(node, ast.FunctionDef):
            found.append(node)
    return found


def _names_in(node: ast.expr) -> list[str]:
    """Returns the task-variable names in a Name or a ``[Name, ...]`` list node."""
    if isinstance(node, ast.Name):
        return [node.id]
    if isinstance(node, (ast.List, ast.Tuple)):
        return [elt.id for elt in node.elts if isinstance(elt, ast.Name)]
    return []


def _literal_argument_source(node: ast.expr) -> str | None:
    """Returns stable Python source for a literal TaskFlow call argument when available."""
    try:
        return repr(ast.literal_eval(node))
    except (ValueError, SyntaxError):
        return None


def _direct_operator_call(node: ast.Call, aliases: dict[str, str] | None = None) -> ast.Call | None:
    """Returns *node* if it is a direct ``SomeOperator(...)`` / ``SomeSensor(...)`` call."""
    if _is_task_construct(_construct_name(node.func, aliases or {})):
        return node
    return None


def _mapped_operator_call(
    node: ast.Call, aliases: dict[str, str] | None = None
) -> tuple[ast.Call, list[str], bool] | None:
    """Returns the underlying operator call for a dynamic-mapping ``.expand(...)`` chain.

    Handles ``Op(...).expand(...)`` and ``Op.partial(...).expand(...)``. Returns
    ``(merged_call, expand_kwarg_names)``: the Call's keywords are the merged operator kwargs
    (partial args + expand args) and its ``.func`` is the operator Name, so the caller treats it like a
    direct operator call. The expand kwarg names are returned separately because only those are
    fanned out -- a list-valued ``.partial()`` arg is a fixed value, not the mapped iterable.
    """
    if not (isinstance(node.func, ast.Attribute) and node.func.attr == "expand"):
        return None
    inner = node.func.value  # the Op(...) or Op.partial(...) call
    if not isinstance(inner, ast.Call):
        return None
    alias_map = aliases or {}
    if _is_task_construct(_construct_name(inner.func, alias_map)):
        operator_name = _construct_name(inner.func, alias_map)  # Op(...).expand(...)
    elif (
        isinstance(inner.func, ast.Attribute)
        and inner.func.attr == "partial"
        and _is_task_construct(_construct_name(inner.func.value, alias_map))
    ):
        operator_name = _construct_name(inner.func.value, alias_map)  # Op.partial(...).expand(...)
    else:
        return None
    merged = ast.Call(
        func=ast.Name(id=operator_name, ctx=ast.Load()),
        args=[],
        keywords=list(inner.keywords) + list(node.keywords),
    )
    return merged, [kw.arg for kw in node.keywords if kw.arg], isinstance(inner.func, ast.Attribute)


def _is_task_construct(name: str) -> bool:
    """True when a call name is an Airflow task-defining construct we should capture.

    Covers operators (``*Operator``), sensors (``*Sensor``), and the cosmos
    constructs (``DbtDag`` / ``DbtTaskGroup``) that don't follow either suffix.
    """
    return name.endswith("Operator") or name.endswith("Sensor") or name in ops.COSMOS_CONSTRUCTS


def _decorator_name(node: ast.expr) -> str:
    """Dotted name of a decorator, ignoring call args: ``@task`` / ``@task.branch()`` -> 'task.branch'."""
    if isinstance(node, ast.Call):
        node = node.func
    parts: list[str] = []
    while isinstance(node, ast.Attribute):
        parts.append(node.attr)
        node = node.value
    if isinstance(node, ast.Name):
        parts.append(node.id)
    return ".".join(reversed(parts))


def _decorator_kwargs(decorators: list[ast.expr], names: frozenset[str]) -> dict[str, ast.expr]:
    """Merged keyword args of the first decorator whose dotted name is in *names* (if it's a call)."""
    for dec in decorators:
        if _decorator_name(dec) in names and isinstance(dec, ast.Call):
            return {kw.arg: kw.value for kw in dec.keywords if kw.arg}
    return {}


# TaskFlow decorators. ``@dag`` marks a DAG-defining function; ``@task`` (and its variants) mark a
# task-defining function. The bare ``task`` and dotted forms (``task.branch`` / ``task.virtualenv`` /
# ``task.short_circuit`` / ``task.sensor``) all define one task from the decorated callable.
_DAG_DECORATORS: frozenset[str] = frozenset({"dag"})
_TASK_DECORATORS: frozenset[str] = frozenset(
    {"task", "task.branch", "task.virtualenv", "task.short_circuit", "task.sensor", "task.external_python"}
)
_TASK_GROUP_DECORATORS: frozenset[str] = frozenset({"task_group"})


def _has_decorator(func: ast.FunctionDef, names: frozenset[str]) -> bool:
    return any(_decorator_name(dec) in names for dec in func.decorator_list)


def load_airflow_dag(dag_path: Path, *, dbt_mode: str = "static") -> Pipeline:
    """Parses the first Airflow DAG in a file into a flowx Pipeline IR."""
    pipelines = load_airflow_dags(dag_path, dbt_mode=dbt_mode)
    if not pipelines:
        raise ValueError(f"No Airflow DAG found in {dag_path}")
    return pipelines[0]


def load_airflow_dags(
    dag_path: Path,
    *,
    dbt_mode: str = "static",
    source_file: str | None = None,
) -> list[Pipeline]:
    """Parses every independently declared Airflow DAG in a Python file."""
    source = Path(dag_path).read_text(encoding="utf-8")
    module = _expand_top_level_loops(ast.parse(source))
    declarations = _top_level_dag_declarations(module)
    if not declarations:
        return [
            _load_airflow_module(
                dag_path,
                source,
                module,
                dbt_mode=dbt_mode,
                source_file=source_file or dag_path.name,
            )
        ]
    return [
        _load_airflow_module(
            dag_path,
            source,
            _module_for_dag(module, declaration),
            dbt_mode=dbt_mode,
            target_dag_variable=declaration.variable,
            source_file=source_file or dag_path.name,
        )
        for declaration in declarations
    ]


def _top_level_dag_declarations(module: ast.Module) -> list[DagDeclaration]:
    """Returns context-manager, decorated, and assigned top-level DAG declarations."""
    aliases = _import_aliases(module)
    declarations: list[DagDeclaration] = []
    for node in module.body:
        variable: str | None = None
        is_dag = False
        if isinstance(node, ast.FunctionDef) and _has_decorator(node, _DAG_DECORATORS):
            is_dag = True
        elif isinstance(node, ast.With) and any(
            isinstance(item.context_expr, ast.Call) and _construct_name(item.context_expr.func, aliases) == "DAG"
            for item in node.items
        ):
            is_dag = True
        elif (
            isinstance(node, ast.Assign)
            and len(node.targets) == 1
            and isinstance(node.targets[0], ast.Name)
            and isinstance(node.value, ast.Call)
            and _construct_name(node.value.func, aliases) == "DAG"
        ):
            is_dag = True
            variable = node.targets[0].id
        if is_dag:
            span = _span(node)
            declarations.append(
                DagDeclaration(
                    capture_id=f"dag:{span.line}:{span.column}:{len(declarations) + 1}",
                    variable=variable,
                    node=node,
                    span=span,
                )
            )
    return declarations


def _module_for_dag(module: ast.Module, declaration: DagDeclaration) -> ast.Module:
    """Returns a module containing shared definitions and one DAG declaration."""
    dag_nodes = {item.node for item in _top_level_dag_declarations(module)}
    body = [node for node in module.body if node is declaration.node or node not in dag_nodes]
    return ast.Module(body=body, type_ignores=list(module.type_ignores))


def _load_airflow_module(
    dag_path: Path,
    source: str,
    module: ast.Module,
    *,
    dbt_mode: str = "static",
    target_dag_variable: str | None = None,
    source_file: str | None = None,
) -> Pipeline:
    """Parses one isolated DAG declaration into a flowx Pipeline IR.

    Args:
        dag_path: Path to a ``.py`` DAG module.
        dbt_mode: dbt-factory render mode for any dbt workload -- ``"static"`` (default,
            an inner job of per-node tasks) or ``"pydabs"`` (a deploy-time hook module).

    Returns:
        A :class:`~flowx.models.ir.Pipeline`. Mapped operators become their IR
        node (NotebookActivity, SparkPython/JarActivity, RunJobActivity,
        DbtFactoryActivity, ...); Dummy/Empty are dropped with dependency
        rewiring; file sensors lift to a job-level file_arrival trigger; time
        sensors remain explicit placeholders; unmapped operators become a
        PlaceholderActivity.
    """
    audit = source_audit.audit_module(module, target_dag_variable=target_dag_variable)
    visitor = _DagVisitor(module, target_dag_variable=target_dag_variable)
    visitor.visit(module)
    functions = visitor.functions()

    # Prefix TaskGroup member keys with the group id (e.g. extract__run) so two tasks named
    # `run` in different groups don't collide.
    def _task_key(var: str, task_id: str) -> str:
        key = _sanitize_task_key(task_id)
        return f"{visitor.groups[var]}__{key}" if var in visitor.groups else key

    # TaskFlow @task instances share the task table with classic operators (both are just tasks with
    # a task_key and dependency edges downstream).
    var_task_ids: dict[str, str] = {var: tid for var, (tid, _, _) in visitor.operators.items()}
    var_task_ids.update({var: tf.task_id for var, tf in visitor.taskflow_tasks.items()})
    var_task_ids.update({var: task_id for var, (task_id, _, _) in visitor.taskgroup_calls.items()})
    var_to_task_key: dict[str, str] = {}
    used_task_keys: set[str] = set()
    for var, task_id in var_task_ids.items():
        base = _task_key(var, task_id)
        candidate = base
        suffix = 2
        while candidate in used_task_keys:
            candidate = f"{base}__{suffix}"
            suffix += 1
        used_task_keys.add(candidate)
        var_to_task_key[var] = candidate

    # Expand group-level edges (`group_a >> group_b`, `task >> group`, ...) into edges between the
    # groups' boundary tasks: leaves of the upstream group -> roots of the downstream group, matching
    # Airflow's TaskGroup dependency semantics. A non-group var resolves to itself.
    edges = _expand_group_edges(visitor.edges, visitor.groups, visitor.group_vars)

    # Build the upstream adjacency in dependency terms, then drop structural nodes
    # (Dummy/Empty and lifted root sensors) by rewiring their downstreams to their upstreams.
    upstreams: dict[str, list[str]] = {var: [] for var in var_task_ids}
    for upstream_var, downstream_var in edges:
        if downstream_var in upstreams and upstream_var in var_to_task_key:
            upstreams[downstream_var].append(upstream_var)

    # Sensor / schedule precedence. Airflow semantics are "run on schedule, THEN wait for data",
    # and Databricks treats schedule / file_arrival / table_update as mutually-exclusive job trigger
    # types -- so a data sensor lifts to a file_arrival/table_update *trigger* only when it stands at
    # the DAG root AND no cron/timedelta schedule is present. With a schedule (cron AND-THEN wait) or
    # mid-DAG (an ordering gate, not the DAG's entry condition), the sensor is retained as a polling
    # task instead of being silently dropped.
    schedule = _schedule_from_interval(visitor.schedule_interval, node=visitor.schedule_node, timezone=visitor.timezone)
    has_schedule = schedule is not None

    # Dummy/Empty operators are structural and can be removed after dependency rewiring.
    dropped = {var for var, (_, op, _) in visitor.operators.items() if op in ops.DUMMY_OPERATORS}
    sensor_lift_proof: dict[str, Any] | None = None
    if not has_schedule:
        trigger_candidate = _root_trigger_sensor(visitor.operators, upstreams, set(var_task_ids))
        if trigger_candidate is not None:
            trigger_var, covered_tasks = trigger_candidate
            trigger = _trigger_from_sensor(*visitor.operators[trigger_var][1:])
            if trigger is not None:
                schedule = trigger
                dropped.add(trigger_var)
                sensor_lift_proof = {
                    "code": "sensor_lift_dominates_dag",
                    "capture_id": trigger_var,
                    "task_key": var_to_task_key[trigger_var],
                    "covered_capture_ids": sorted(covered_tasks),
                }
    upstreams = _rewire_dropped(upstreams, dropped)

    # Collapse all dbt CLI operators over the one project into a single DbtFactoryActivity emitted at
    # the first dbt task's position. Every dbt var's task_key remaps to that single key, so a
    # downstream task that depended on a later dbt op (e.g. `dbt_test`) points at the factory task
    # rather than a task_key that was never emitted (which would dangle).
    dbt_vars = [var for var, (_, op, _) in visitor.operators.items() if op in ops.DBT_CLI_OPERATORS]
    dbt_var_set = set(dbt_vars)
    dbt_factory_key = var_to_task_key[dbt_vars[0]] if dbt_vars else None
    dbt_key_remap = {var_to_task_key[v]: dbt_factory_key for v in dbt_vars} if dbt_factory_key else {}

    # Non-dbt tasks reachable *downstream* from the collapsed dbt set. Because every dbt op folds into
    # one factory task, a task that sat between two dbt ops (e.g. `dbt_seed >> task_b >> dbt_run`) is
    # downstream of the factory; the factory therefore cannot depend on it without forming a cycle,
    # but it must still depend on the factory and gate whatever followed it.
    downstream_of_factory: set[str] = set()
    if dbt_factory_key:
        adjacency: dict[str, list[str]] = {v: [] for v in var_task_ids}
        for downstream_var, ups in upstreams.items():
            for upstream_var in ups:
                adjacency.setdefault(upstream_var, []).append(downstream_var)
        stack = list(dbt_vars)
        seen_ds: set[str] = set(dbt_vars)
        while stack:
            for nxt in adjacency.get(stack.pop(), []):
                if nxt not in seen_ds:
                    seen_ds.add(nxt)
                    stack.append(nxt)
        downstream_of_factory = {var_to_task_key[v] for v in seen_ds if v not in dbt_var_set}

    def _sandwiched_before(dbt_var: str) -> set[str]:
        """Non-dbt tasks that fed *dbt_var* (through the collapsed dbt chain) and sit downstream of the
        factory. A task consuming a later dbt op must still wait for these, since the collapse drops
        the intermediate dbt op they fed."""
        result: set[str] = set()
        for upstream_var in upstreams.get(dbt_var, []):
            if upstream_var in dbt_var_set:
                result |= _sandwiched_before(upstream_var)
            elif var_to_task_key[upstream_var] in downstream_of_factory:
                result.add(var_to_task_key[upstream_var])
        return result

    # The factory absorbs every dbt op's external (non-dbt) upstream that is not itself downstream of
    # the factory -- not just the first dbt op's, so a later dbt op's upstream is not silently dropped.
    factory_dep_keys: set[str] = set()
    for dbt_var in dbt_vars:
        for upstream_var in upstreams.get(dbt_var, []):
            if upstream_var in dbt_var_set:
                continue
            key = var_to_task_key[upstream_var]
            if key not in downstream_of_factory:
                factory_dep_keys.add(key)

    def _dep(upstream_var: str, outcome: str | None) -> str:
        key = var_to_task_key[upstream_var]
        return dbt_key_remap.get(key, key)

    tasks: list[Activity] = []
    semantic_findings: list[dict[str, Any]] = []
    argument_proofs = [
        {
            "code": "operator_arguments_classified",
            "capture_id": var,
            "task_key": var_to_task_key[var],
            "operator": operator,
            "arguments": ops.argument_classification(operator, kwargs),
        }
        for var, (_task_id, operator, kwargs) in visitor.operators.items()
    ]
    referenced_params: set[str] = set()
    emitted_dbt = False
    for var, (task_id, operator, kwargs) in visitor.operators.items():
        if var in dropped:
            continue
        task_key = var_to_task_key[var]
        trigger_mapping = templating.trigger_rule_mapping(kwargs)
        outcome = trigger_mapping.outcome
        # Remap dbt-chain upstreams to the single factory key and drop self-edges (a dbt op
        # depending on another dbt op in the same collapsed chain).
        dep_keys = {_dep(u, outcome) for u in upstreams[var]}
        # A task consuming a later dbt op must also wait for any non-dbt task that sat between two dbt
        # ops (the collapse folds away the intermediate dbt op that carried that ordering).
        for upstream_var in upstreams[var]:
            if upstream_var in dbt_var_set:
                dep_keys |= _sandwiched_before(upstream_var)
        dep_keys.discard(task_key if operator not in ops.DBT_CLI_OPERATORS else dbt_factory_key)
        depends_on = [Dependency(task_key=k, outcome=outcome) for k in sorted(dep_keys)] or None

        if operator in ops.COSMOS_CONSTRUCTS:
            tasks.append(
                _build_dbt_factory(task_id, task_key, [kwargs], depends_on, dbt_mode, operator_types=[operator])
            )
            continue
        if operator in ops.DBT_CLI_OPERATORS:
            # Emit one factory job for the whole dbt chain, at the first dbt task's position.
            if emitted_dbt:
                continue
            emitted_dbt = True
            # The factory gates on every dbt op's external upstreams (not just the first op's), minus
            # any that are downstream of the factory itself (a sandwiched task, which would cycle).
            factory_depends_on = [Dependency(task_key=k, outcome=outcome) for k in sorted(factory_dep_keys)] or None
            dbt_kwargs = [visitor.operators[v][2] for v in dbt_vars]
            tasks.append(
                _build_dbt_factory(
                    task_id,
                    task_key,
                    dbt_kwargs,
                    factory_depends_on,
                    dbt_mode,
                    operator_types=[visitor.operators[dbt_var][1] for dbt_var in dbt_vars],
                )
            )
            continue

        call_node = visitor.calls.get(var)
        call_source = ast.get_source_segment(source, call_node) or "" if call_node is not None else ""
        ctx = ops.OperatorContext(
            task_id=task_id,
            task_key=task_key,
            operator=operator,
            kwargs=kwargs,
            functions=visitor.functions_for(var),
            source=source,
            call_source=call_source,
            default_args=visitor.default_args,
        )
        builder = ops.OPERATOR_REGISTRY.get(operator, ops.build_placeholder)
        activity = builder(ctx)
        activity.depends_on = depends_on
        if trigger_mapping.status == "unsupported":
            activity = ops.build_placeholder_with_comment(
                ctx,
                f"Airflow trigger_rule {trigger_mapping.rule!r} is unsupported. {trigger_mapping.message}",
            )
            activity.depends_on = depends_on
            semantic_findings.append(
                _semantic_finding(
                    source_file or dag_path.name,
                    visitor.calls.get(var),
                    code="unsupported_trigger_rule",
                    message=(f"Task {task_id!r} uses trigger_rule {trigger_mapping.rule!r}; {trigger_mapping.message}"),
                    task_key=task_key,
                )
            )
        elif trigger_mapping.status == "approximate":
            semantic_findings.append(
                _semantic_finding(
                    source_file or dag_path.name,
                    visitor.calls.get(var),
                    code="approximated_trigger_rule",
                    message=(
                        f"Task {task_id!r} maps trigger_rule {trigger_mapping.rule!r} to "
                        f"{trigger_mapping.outcome}. {trigger_mapping.message}"
                    ),
                    task_key=task_key,
                )
            )
        unconsumed = ops.unconsumed_kwargs(operator, kwargs)
        if unconsumed:
            names = ", ".join(sorted(unconsumed))
            activity = ops.build_placeholder_with_comment(
                ctx,
                f"Airflow {operator} argument(s) {names} are not represented by the Databricks task; "
                "translate them explicitly.",
            )
            activity.depends_on = depends_on
            semantic_findings.append(
                _semantic_finding(
                    source_file or dag_path.name,
                    visitor.calls.get(var),
                    code="unconsumed_operator_arguments",
                    message=f"Task {task_id!r} has unconsumed operator argument(s): {names}.",
                    task_key=task_key,
                    arguments=sorted(unconsumed),
                )
            )
        # Stamp DAG/task retry + timeout policy (per-task kwargs override default_args).
        policy = templating.retry_policy(visitor.default_args, kwargs)
        activity.max_retries = policy.get("max_retries")
        activity.timeout_seconds = policy.get("timeout_seconds")
        activity.min_retry_interval_millis = policy.get("min_retry_interval_millis")
        # Convert Airflow Jinja in the activity's parameter fields to DAB refs; collect params.
        referenced_params |= _convert_activity_templates(activity)
        unresolved_templates = _unresolved_activity_templates(activity)
        if unresolved_templates:
            expressions = ", ".join(sorted(unresolved_templates))
            activity = ops.build_placeholder_with_comment(
                ctx,
                f"Airflow template expression(s) {expressions} have no deterministic Databricks mapping; "
                "translate the value manually.",
            )
            activity.depends_on = depends_on
            semantic_findings.append(
                _semantic_finding(
                    source_file or dag_path.name,
                    visitor.calls.get(var),
                    code="unresolved_airflow_template",
                    message=f"Task {task_id!r} contains unresolved Airflow template expression(s): {expressions}.",
                    task_key=task_key,
                    expressions=sorted(unresolved_templates),
                )
            )

        if var in visitor.mapped:
            mapped_names = visitor.expand_kwargs.get(var) or []
            partial_note = (
                " The mapping also contains .partial() fixed arguments." if var in visitor.partial_mapped else ""
            )
            activity = ops.build_placeholder_with_comment(
                ctx,
                "Classic Airflow dynamic mapping cannot be emitted until every mapped argument is "
                f"bound into the inner task ({', '.join(mapped_names) or 'unknown mapping'}).{partial_note}",
            )
            activity.depends_on = depends_on
            semantic_findings.append(
                _semantic_finding(
                    source_file or dag_path.name,
                    visitor.calls.get(var),
                    code="classic_mapping_arguments_unbound",
                    message=(
                        f"Task {task_id!r} maps argument(s) {', '.join(mapped_names) or '<unknown>'}, "
                        "but the generated inner task cannot bind them safely."
                    ),
                    task_key=task_key,
                    arguments=mapped_names,
                    has_partial=var in visitor.partial_mapped,
                )
            )
            tasks.append(_wrap_in_for_each(activity, task_id, task_key, depends_on, kwargs, mapped_names))
        else:
            tasks.append(activity)

    # TaskFlow @task instances: emit each as a notebook that reads upstream return values via
    # dbutils.jobs.taskValues, calls the decorated function, and sets its own return value.
    for var, tf in visitor.taskflow_tasks.items():
        task_key = var_to_task_key[var]
        dep_keys = {var_to_task_key[u] for u in upstreams.get(var, []) if u in var_to_task_key}
        dep_keys.discard(task_key)
        depends_on = [Dependency(task_key=k) for k in sorted(dep_keys)] or None
        if var in visitor.mapped and tf.expand_items_json is None:
            # .expand over a non-literal iterable (e.g. an upstream task's output) can't be lowered to
            # a static for_each inputs array -- route to the agentic-gap round instead of silently
            # emitting a single-run notebook.
            reason = f"mapped parameter {tf.expand_kwarg!r}" if tf.expand_kwarg else "multiple mapped parameters"
            func = functions.get(tf.def_name)
            # The mapping call carries the .partial(...) fixed args and the mapped iterable, neither of
            # which appears in the callable's own source -- without it the agentic round can't
            # reconstruct the invocation.
            mapping_call = visitor.calls.get(var)
            mapping_source = ast.get_source_segment(source, mapping_call) if mapping_call is not None else None
            placeholder = PlaceholderActivity(
                name=tf.task_id,
                task_key=task_key,
                original_type=f"@{tf.decorator}.expand",
                comment=(
                    f"TaskFlow @{tf.decorator} '{tf.def_name}'.expand() maps over a non-literal iterable "
                    f"({reason}); translate to a Databricks for_each_task whose inputs reference the "
                    "upstream task value, iterating the callable."
                ),
                raw_definition={
                    "operator": f"@{tf.decorator}.expand",
                    "source": ast.get_source_segment(source, func) if func is not None else "",
                    "mapping": mapping_source or "",
                },
            )
            placeholder.depends_on = depends_on
            tasks.append(placeholder)
            continue
        activity = _build_taskflow_task(tf, var_to_task_key, functions, source, task_key)
        activity.depends_on = depends_on
        referenced_params |= _convert_activity_templates(activity)
        if var in visitor.mapped and isinstance(activity, NotebookActivity):
            # .expand(param=[literal list]) -> a for_each_task iterating the callable notebook; the
            # inner notebook reads the mapped parameter from the per-iteration `item` widget.
            tasks.append(_wrap_taskflow_in_for_each(activity, tf, task_key, depends_on))
        else:
            tasks.append(activity)

    # @task_group invocations: a group is a sub-pipeline of tasks with no single-task lowering, so
    # emit a placeholder + gap (never silently drop the whole group) for the agentic round to expand.
    for var, (task_id, def_name, mapped) in visitor.taskgroup_calls.items():
        task_key = var_to_task_key[var]
        dep_keys = {var_to_task_key[u] for u in upstreams.get(var, []) if u in var_to_task_key}
        dep_keys.discard(task_key)
        depends_on = [Dependency(task_key=k) for k in sorted(dep_keys)] or None
        group_func = functions.get(def_name)
        detail = (
            "maps the group over an iterable (one group run per element); translate to a for_each_task "
            "whose inner task expands the group's tasks"
            if mapped
            else "bundles multiple tasks; expand it into its member tasks with their dependencies"
        )
        placeholder = PlaceholderActivity(
            name=task_id,
            task_key=task_key,
            original_type="@task_group",
            comment=f"Airflow @task_group '{def_name}' {detail}. flowx does not lower task groups.",
            raw_definition={
                "operator": "@task_group",
                "source": ast.get_source_segment(source, group_func) if group_func is not None else "",
            },
        )
        placeholder.depends_on = depends_on
        tasks.append(placeholder)

    # Declare every job parameter -- those referenced in templates plus any from the DAG's
    # params={...} -- each with a default (Databricks requires one): the params={...} default when
    # present; a logical-date parameter (run_date/execution_date/...) its schedule-aware time ref so a
    # native backfill can override it per window; else an empty string so the bundle still validates.
    param_names = referenced_params | set(visitor.dag_params)
    parameters = [
        {"name": name, "default": _declared_param_default(name, visitor.dag_params, schedule)}
        for name in sorted(param_names)
    ] or None
    tags = {"source": "airflow", "dag_id": visitor.dag_id or ""}
    if visitor.catchup:
        # Airflow catchup=True has no DABs schedule setting; it maps to running a native Databricks
        # backfill, which overrides the run_date job parameter with {{backfill.iso_date}} per window.
        tags["airflow_catchup"] = "true"
    expected_ir_edges = {(dependency.task_key, task.task_key) for task in tasks for dependency in task.depends_on or []}
    pipeline = Pipeline(
        name=visitor.dag_id or Path(dag_path).stem,
        tasks=tasks,
        parameters=parameters,
        schedule=schedule,
        tags=tags,
    )
    return _reconcile_pipeline(
        pipeline,
        audit=audit,
        visitor=visitor,
        source_file=source_file or dag_path.name,
        var_to_task_key=var_to_task_key,
        dropped=dropped,
        dbt_vars=dbt_vars,
        semantic_findings=semantic_findings,
        sensor_lift_proof=sensor_lift_proof,
        argument_proofs=argument_proofs,
        expected_ir_edges=expected_ir_edges,
    )


_SUPPORTED_DAG_SETTINGS = frozenset(
    {
        "dag_id",
        "schedule",
        "schedule_interval",
        "start_date",
        "timezone",
        "catchup",
        "default_args",
        "params",
        "default_args.retries",
        "default_args.retry_delay",
        "default_args.execution_timeout",
    }
)


def _semantic_finding(
    source_file: str,
    node: ast.AST | None,
    *,
    code: str,
    message: str,
    task_key: str,
    **details: Any,
) -> dict[str, Any]:
    """Builds a stable gap finding for a captured task-level semantic limitation."""
    candidate = source_audit.AuditCandidate(
        kind="task_semantics",
        code=code,
        line=getattr(node, "lineno", 0),
        column=getattr(node, "col_offset", 0),
        occurrence=1,
        end_line=getattr(node, "end_lineno", 0),
        end_column=getattr(node, "end_col_offset", 0),
        details={"task_key": task_key, **details},
    )
    return source_audit.finding(
        source_file=source_file,
        code=code,
        severity="gap",
        message=message,
        candidate=candidate,
    )


def _iter_placeholders(tasks: list[Activity]) -> list[PlaceholderActivity]:
    """Returns placeholders in top-level and Airflow-generated for_each tasks."""
    placeholders: list[PlaceholderActivity] = []
    for task in tasks:
        if isinstance(task, PlaceholderActivity):
            placeholders.append(task)
        if isinstance(task, ForEachActivity):
            placeholders.extend(_iter_placeholders(task.inner_activities))
    return placeholders


def _reconcile_pipeline(
    pipeline: Pipeline,
    *,
    audit: source_audit.SourceAudit,
    visitor: _DagVisitor,
    source_file: str,
    var_to_task_key: dict[str, str],
    dropped: set[str],
    dbt_vars: list[str],
    semantic_findings: list[dict[str, Any]],
    sensor_lift_proof: dict[str, Any] | None,
    argument_proofs: list[dict[str, Any]],
    expected_ir_edges: set[tuple[str, str]],
) -> Pipeline:
    """Reconciles an independent source audit with captured graph and emitted IR."""
    findings: list[dict[str, Any]] = list(semantic_findings)
    transformations: list[dict[str, Any]] = list(argument_proofs)
    transformations.extend(visitor.helper_expansions)
    transformations.extend(
        {
            "code": "edge_captured",
            "upstream_capture_id": edge.upstream_id,
            "downstream_capture_id": edge.downstream_id,
            "upstream_task_key": var_to_task_key.get(edge.upstream_id),
            "downstream_task_key": var_to_task_key.get(edge.downstream_id),
            "source_span": {
                "line": edge.span.line,
                "column": edge.span.column,
                "end_line": edge.span.end_line,
                "end_column": edge.span.end_column,
            },
        }
        for edge in visitor.edge_captures
    )
    if sensor_lift_proof is not None:
        transformations.append(sensor_lift_proof)
    captured_task_count = len(visitor.operators) + len(visitor.taskflow_tasks) + len(visitor.taskgroup_calls)

    unresolved = list(audit.unresolved)
    for code, node in visitor.unresolved_constructs:
        if not any(
            candidate.line == getattr(node, "lineno", 0) and candidate.column == getattr(node, "col_offset", 0)
            for candidate in unresolved
        ):
            unresolved.append(
                source_audit.AuditCandidate(
                    kind="unresolved",
                    code=code,
                    line=getattr(node, "lineno", 0),
                    column=getattr(node, "col_offset", 0),
                    occurrence=1,
                    end_line=getattr(node, "end_lineno", 0),
                    end_column=getattr(node, "end_col_offset", 0),
                    details={"expression": ast.unparse(node)},
                )
            )

    helper_claims = {
        ("helper_factory_task", int(item["invocation_line"]), str(item["helper"])): 1
        for item in visitor.helper_expansions
    }
    helper_capture_ids = {str(item["capture_id"]) for item in visitor.helper_expansions}
    capture_claims: Counter[tuple[str, int, str]] = Counter(helper_claims)
    for capture in visitor.task_captures.values():
        if capture.capture_id not in helper_capture_ids:
            capture_claims[("operator_task", capture.span.line, capture.operator)] += 1
    for var, taskflow_task in visitor.taskflow_tasks.items():
        call = visitor.calls.get(var)
        capture_claims[("taskflow_task", getattr(call, "lineno", 0), taskflow_task.def_name)] += 1

    unmatched_audit_tasks: list[source_audit.AuditCandidate] = []
    for candidate in audit.tasks:
        discriminator = str(
            candidate.details.get("operator")
            or candidate.details.get("helper")
            or candidate.details.get("callable")
            or ""
        )
        key = (candidate.code, candidate.line, discriminator)
        if capture_claims[key]:
            capture_claims[key] -= 1
        else:
            unmatched_audit_tasks.append(candidate)

    for candidate in unmatched_audit_tasks:
        findings.append(
            source_audit.finding(
                source_file=source_file,
                code="task_capture_mismatch",
                severity="failed",
                message="An independently audited Airflow task candidate was not claimed by the capture pass.",
                candidate=candidate,
            )
        )
    for call in visitor.unclaimed_task_calls:
        candidate = source_audit.AuditCandidate(
            kind="task",
            code="unclaimed_dag_task",
            line=getattr(call, "lineno", 0),
            column=getattr(call, "col_offset", 0),
            occurrence=1,
            end_line=getattr(call, "end_lineno", 0),
            end_column=getattr(call, "end_col_offset", 0),
            details={"expression": ast.unparse(call)},
        )
        findings.append(
            source_audit.finding(
                source_file=source_file,
                code="unclaimed_dag_task",
                severity="failed",
                message="A task-producing call in the DAG body was not claimed by the capture pass.",
                candidate=candidate,
            )
        )
    for statement in visitor.unclaimed_statements:
        candidate = source_audit.AuditCandidate(
            kind="statement",
            code="unclaimed_dag_statement",
            line=getattr(statement, "lineno", 0),
            column=getattr(statement, "col_offset", 0),
            occurrence=1,
            end_line=getattr(statement, "end_lineno", 0),
            end_column=getattr(statement, "end_col_offset", 0),
            details={"expression": ast.unparse(statement)},
        )
        findings.append(
            source_audit.finding(
                source_file=source_file,
                code="unclaimed_dag_statement",
                severity="failed",
                message="A DAG-body statement was not classified by the static capture pass.",
                candidate=candidate,
            )
        )

    if len(audit.edges) != len(visitor.edge_captures):
        findings.append(
            source_audit.finding(
                source_file=source_file,
                code="edge_capture_mismatch",
                severity="failed",
                message=(
                    f"Source audit found {len(audit.edges)} dependency edge(s), but capture produced "
                    f"{len(visitor.edge_captures)}."
                ),
                details={"audited": len(audit.edges), "captured": len(visitor.edge_captures)},
            )
        )
    comparable_audit_edges = [
        candidate
        for candidate in audit.edges
        if candidate.details.get("syntax") != "taskflow_data"
        and candidate.details.get("upstream")
        and candidate.details.get("downstream")
    ]
    comparable_spans = {
        (candidate.line, candidate.column, candidate.end_line, candidate.end_column)
        for candidate in comparable_audit_edges
    }

    def source_reference(capture_id: str) -> str:
        capture = visitor.task_captures.get(capture_id)
        return capture.variable if capture is not None else capture_id.split("__L", 1)[0]

    audited_edge_identities = sorted(
        (str(candidate.details["upstream"]), str(candidate.details["downstream"]))
        for candidate in comparable_audit_edges
    )
    captured_edge_identities = sorted(
        (source_reference(edge.upstream_id), source_reference(edge.downstream_id))
        for edge in visitor.edge_captures
        if (edge.span.line, edge.span.column, edge.span.end_line, edge.span.end_column) in comparable_spans
    )
    if audited_edge_identities != captured_edge_identities:
        findings.append(
            source_audit.finding(
                source_file=source_file,
                code="edge_identity_mismatch",
                severity="failed",
                message="Captured Airflow dependency endpoints do not match the audited source endpoints.",
                details={
                    "audited_edges": [list(edge) for edge in audited_edge_identities],
                    "captured_edges": [list(edge) for edge in captured_edge_identities],
                },
            )
        )

    emitted_ir_edges = {
        (dependency.task_key, task.task_key) for task in pipeline.tasks for dependency in task.depends_on or []
    }
    missing_ir_edges = sorted(expected_ir_edges - emitted_ir_edges)
    unexpected_ir_edges = sorted(emitted_ir_edges - expected_ir_edges)
    if missing_ir_edges:
        findings.append(
            source_audit.finding(
                source_file=source_file,
                code="captured_edge_not_emitted",
                severity="failed",
                message="Captured dependency edge(s) were not emitted to Pipeline IR.",
                details={"missing_edges": [list(edge) for edge in missing_ir_edges]},
            )
        )
    if unexpected_ir_edges:
        findings.append(
            source_audit.finding(
                source_file=source_file,
                code="unexplained_emitted_edge",
                severity="failed",
                message="Pipeline IR contains dependency edge(s) absent from the transformation ledger.",
                details={"unexpected_edges": [list(edge) for edge in unexpected_ir_edges]},
            )
        )

    capture_by_location: dict[tuple[int, str], list[TaskCapture]] = {}
    for capture in visitor.task_captures.values():
        capture_by_location.setdefault((capture.span.line, capture.operator), []).append(capture)
    argument_failure_keys: set[str] = set()
    audit_candidate_by_capture: dict[str, source_audit.AuditCandidate] = {}
    for candidate in audit.tasks:
        if candidate.code != "operator_task":
            continue
        operator = str(candidate.details.get("operator", ""))
        matches = capture_by_location.get((candidate.line, operator), [])
        if not matches:
            continue
        capture = matches.pop(0)
        audit_candidate_by_capture[capture.capture_id] = candidate
        expected = set(candidate.details.get("kwargs", []))
        actual = set(visitor.operators[capture.capture_id][2])
        if expected == actual:
            continue
        task_key = var_to_task_key.get(capture.capture_id, capture.capture_id)
        argument_failure_keys.add(task_key)
        findings.append(
            source_audit.finding(
                source_file=source_file,
                code="operator_argument_capture_mismatch",
                severity="failed",
                message=(
                    f"Airflow task {capture.task_id!r} audited argument(s) {sorted(expected)}, "
                    f"but capture retained {sorted(actual)}."
                ),
                candidate=candidate,
                details={
                    "task_key": task_key,
                    "missing": sorted(expected - actual),
                    "unexpected": sorted(actual - expected),
                },
            )
        )

    dbt_factory_var = dbt_vars[0] if dbt_vars else None
    expected_key_by_capture: dict[str, str] = {}
    for var, task_key in var_to_task_key.items():
        if var in dropped:
            continue
        expected_key_by_capture[var] = (
            var_to_task_key[dbt_factory_var] if var in dbt_vars and dbt_factory_var is not None else task_key
        )
    expected_task_keys = set(expected_key_by_capture.values())
    emitted_task_keys = {task.task_key for task in pipeline.tasks}
    missing_task_keys = sorted(expected_task_keys - emitted_task_keys)
    unexpected_task_keys = sorted(emitted_task_keys - expected_task_keys)
    if missing_task_keys:
        missing_capture = next(
            (var for var, task_key in expected_key_by_capture.items() if task_key in missing_task_keys),
            None,
        )
        findings.append(
            source_audit.finding(
                source_file=source_file,
                code="captured_task_not_emitted",
                severity="failed",
                message=f"Captured Airflow task key(s) were not emitted to Pipeline IR: {missing_task_keys}.",
                candidate=audit_candidate_by_capture.get(missing_capture or ""),
                details={"task_keys": missing_task_keys},
            )
        )
    if unexpected_task_keys:
        findings.append(
            source_audit.finding(
                source_file=source_file,
                code="unexplained_emitted_task",
                severity="failed",
                message=f"Pipeline IR contains task key(s) with no captured Airflow task: {unexpected_task_keys}.",
                details={"task_keys": unexpected_task_keys},
            )
        )

    unsupported_settings = [
        candidate for candidate in audit.settings if candidate.details.get("name") not in _SUPPORTED_DAG_SETTINGS
    ]
    missing_supported_settings = [
        candidate
        for candidate in audit.settings
        if candidate.details.get("name") in _SUPPORTED_DAG_SETTINGS
        and candidate.details.get("name") not in visitor.captured_dag_settings
    ]
    for candidate in missing_supported_settings:
        name = str(candidate.details.get("name"))
        findings.append(
            source_audit.finding(
                source_file=source_file,
                code="dag_setting_capture_mismatch",
                severity="failed",
                message=f"Audited DAG setting {name!r} was not captured by the Airflow loader.",
                candidate=candidate,
            )
        )
    for candidate in unsupported_settings:
        name = str(candidate.details.get("name"))
        findings.append(
            source_audit.finding(
                source_file=source_file,
                code="unsupported_dag_setting",
                severity="gap",
                message=f"Airflow DAG setting {name!r} has no deterministic Databricks Jobs mapping.",
                candidate=candidate,
            )
        )

    for candidate in unresolved:
        findings.append(
            source_audit.finding(
                source_file=source_file,
                code=candidate.code,
                severity="gap",
                message="Dynamic Airflow control flow could not be expanded safely by the static parser.",
                candidate=candidate,
            )
        )

    for var, task_key in var_to_task_key.items():
        task_id = (
            visitor.operators[var][0]
            if var in visitor.operators
            else visitor.taskflow_tasks[var].task_id
            if var in visitor.taskflow_tasks
            else visitor.taskgroup_calls[var][0]
        )
        base = _sanitize_task_key(task_id)
        if var in visitor.groups:
            base = f"{visitor.groups[var]}__{base}"
        transformations.append(
            {
                "code": "task_key_allocated",
                "capture_id": var,
                "source_task_id": task_id,
                "task_key": task_key,
                "emitted_task_key": expected_key_by_capture.get(var),
            }
        )
        if task_key != base:
            transformations.append(
                {
                    "code": "task_key_collision_resolved",
                    "capture_id": var,
                    "source_task_id": task_id,
                    "task_key": task_key,
                }
            )
    for var in sorted(dropped):
        transformations.append(
            {
                "code": "structural_task_rewired",
                "capture_id": var,
                "task_key": var_to_task_key.get(var, var),
            }
        )
    if len(dbt_vars) > 1:
        transformations.append(
            {
                "code": "dbt_chain_collapsed",
                "capture_ids": list(dbt_vars),
                "task_key": var_to_task_key.get(dbt_vars[0], ""),
            }
        )

    placeholder_by_key = {
        placeholder.task_key: placeholder
        for placeholder in _iter_placeholders(pipeline.tasks)
        if not placeholder.task_key.startswith("__flowx_")
    }
    for index, placeholder in enumerate(placeholder_by_key.values()):
        placeholder_candidate = audit.tasks[index] if index < len(audit.tasks) else None
        findings.append(
            source_audit.finding(
                source_file=source_file,
                code="operator_placeholder",
                severity="gap",
                message=(
                    f"Airflow task {placeholder.name!r} ({placeholder.original_type}) requires explicit migration."
                ),
                candidate=placeholder_candidate,
                details={"task_key": placeholder.task_key, "operator": placeholder.original_type},
            )
        )

    blocking_gaps = [*unsupported_settings, *unresolved]
    if blocking_gaps:
        placeholder_key = "__flowx_source_gaps"
        gap_task = PlaceholderActivity(
            name="Airflow source semantics requiring migration",
            task_key=placeholder_key,
            original_type="AirflowSourceSemantics",
            comment="Resolve the source-audit findings before enabling this DAG.",
            raw_definition={"findings": [item for item in findings if item["severity"] == "gap"]},
        )
        for task in pipeline.tasks:
            if not task.depends_on:
                task.depends_on = [Dependency(task_key=placeholder_key)]
        pipeline.tasks.insert(0, gap_task)

    failed_findings = [item for item in findings if item["severity"] == "failed"]
    gap_findings = [item for item in findings if item["severity"] == "gap"]
    status = "failed" if failed_findings else "verified_with_gaps" if gap_findings else "verified"
    failed_capture_keys = argument_failure_keys | set(missing_task_keys)
    agentic_captured_count = len(placeholder_by_key)
    deterministic_count = captured_task_count - len(failed_capture_keys) - agentic_captured_count
    agentic_count = agentic_captured_count + len(unresolved)
    failed_count = (
        len(failed_capture_keys)
        + len(unmatched_audit_tasks)
        + len(visitor.unclaimed_task_calls)
        + len(visitor.unclaimed_statements)
    )
    audited_task_count = (
        captured_task_count
        + len(unresolved)
        + len(unmatched_audit_tasks)
        + len(visitor.unclaimed_task_calls)
        + len(visitor.unclaimed_statements)
    )

    pipeline.not_translatable = findings
    pipeline.reconciliation_status = status
    pipeline.audit = {
        "source_file": source_file,
        "audited_activity_count": audited_task_count,
        "captured_task_count": captured_task_count,
        "audited_edge_count": len(audit.edges),
        "captured_edge_count": len(visitor.edge_captures),
        "deterministic_count": deterministic_count,
        "agentic_count": agentic_count,
        "failed_count": failed_count,
        "excluded_count": 0,
        "transformations": transformations,
    }
    return pipeline


def _wrap_in_for_each(
    activity: Activity,
    task_id: str,
    task_key: str,
    depends_on: list[Dependency] | None,
    kwargs: dict[str, ast.expr],
    expand_kwargs: list[str],
) -> ForEachActivity:
    """Wraps a dynamically-mapped operator in a ForEachActivity (-> for_each_task).

    Airflow ``.expand(x=[...])`` fans a task out over an iterable. The for_each's ``inputs`` is the
    first list-valued kwarg passed to ``.expand()`` -- restricted to *expand* kwargs because a
    list-valued ``.partial()`` arg is a fixed value, and taking it would fan the task out over the
    wrong list. The mapped operator becomes the single inner activity, re-keyed so it doesn't collide
    with the for_each task key.
    """
    items = "[]"
    candidates = expand_kwargs or [key for key in kwargs if key not in ("task_id", "group_id")]
    for key in candidates:
        node = kwargs.get(key)
        if node is None or key in ("task_id", "group_id"):
            continue
        value = ops.literal_value(node)
        if isinstance(value, list):
            items = json.dumps(value)
            break
    inner = activity
    inner.task_key = f"{task_key}_iteration"
    inner.name = f"{task_id}_iteration"
    inner.depends_on = None
    return ForEachActivity(
        name=task_id,
        task_key=task_key,
        depends_on=depends_on,
        items_expression=items,
        inner_activities=[inner],
    )


def _wrap_taskflow_in_for_each(
    activity: NotebookActivity,
    tf: _TaskFlowTask,
    task_key: str,
    depends_on: list[Dependency] | None,
) -> ForEachActivity:
    """Wraps a mapped ``@task.expand(param=[...])`` notebook in a ForEachActivity (-> for_each_task).

    The literal iterable becomes the for_each ``inputs`` array; the preparer injects each element as
    the inner task's ``item`` widget, which the callable notebook reads for the mapped parameter.
    """
    activity.task_key = f"{task_key}_iteration"
    activity.name = f"{tf.task_id}_iteration"
    activity.depends_on = None
    return ForEachActivity(
        name=tf.task_id,
        task_key=task_key,
        depends_on=depends_on,
        items_expression=tf.expand_items_json or "[]",
        inner_activities=[activity],
    )


# TaskFlow decorators that gate downstream tasks at runtime -- can't lower to a notebook (same
# reason BranchPythonOperator/ShortCircuitOperator route to the agentic round).
_TASKFLOW_BRANCHING = frozenset({"task.branch", "task.short_circuit"})


def _build_taskflow_task(
    tf: _TaskFlowTask,
    var_to_task_key: dict[str, str],
    functions: dict[str, ast.FunctionDef],
    source: str,
    task_key: str,
) -> Activity:
    """Builds an Activity for one TaskFlow ``@task`` instance.

    The callable is rendered as a notebook that reads each upstream task's return value via
    ``dbutils.jobs.taskValues.get`` (TaskFlow's implicit XCom data flow), invokes the function with
    those bound arguments, and publishes its own return value. Callables that read Airflow task
    context/XCom, or use a branching decorator, route to a placeholder for the agentic round.
    """

    func = functions.get(tf.def_name)
    if func is None:
        return PlaceholderActivity(
            name=tf.task_id,
            task_key=task_key,
            original_type=f"@{tf.decorator}",
            comment=f"TaskFlow @{tf.decorator} '{tf.def_name}' could not be resolved; translate manually.",
        )
    if tf.unresolved_arguments:
        arguments = ", ".join(tf.unresolved_arguments)
        return PlaceholderActivity(
            name=tf.task_id,
            task_key=task_key,
            original_type=f"@{tf.decorator}",
            comment=f"TaskFlow call uses nonliteral argument(s) {arguments}; bind them manually.",
            raw_definition={"operator": f"@{tf.decorator}", "source": ast.get_source_segment(source, func) or ""},
        )
    if tf.decorator in _TASKFLOW_BRANCHING:
        return PlaceholderActivity(
            name=tf.task_id,
            task_key=task_key,
            original_type=f"@{tf.decorator}",
            comment=(
                f"TaskFlow @{tf.decorator} '{tf.def_name}' selects downstream tasks at runtime. "
                "Translate to a Databricks condition_task and gate each downstream branch with a "
                "true/false outcome dependency; do NOT run all branches."
            ),
            raw_definition={"operator": f"@{tf.decorator}", "source": ast.get_source_segment(source, func) or ""},
        )
    reason = callable_notebook.airflow_runtime_reason(func, source)
    if reason is not None:
        return PlaceholderActivity(
            name=tf.task_id,
            task_key=task_key,
            original_type=f"@{tf.decorator}",
            comment=(
                f"TaskFlow @{tf.decorator} '{tf.def_name}' {reason}. flowx has no Airflow runtime to "
                "supply it; pass upstream data via job parameters or map XCom to dbutils.jobs.taskValues."
            ),
            raw_definition={"operator": f"@{tf.decorator}", "source": ast.get_source_segment(source, func) or ""},
        )

    prelude = callable_notebook.render_definitions(func, source, note=f"TaskFlow @{tf.decorator}")
    body = _taskflow_invocation(func, tf, var_to_task_key)
    return NotebookActivity(
        name=tf.task_id,
        task_key=task_key,
        notebook_path=f"notebooks/{task_key}.py",
        generated_source=prelude + body,
    )


def _taskflow_invocation(func: ast.FunctionDef, tf: _TaskFlowTask, var_to_task_key: dict[str, str]) -> str:
    """The invocation cell for a TaskFlow task: read upstream taskValues, call, publish return.

    Each bound upstream task's ``return_value`` is fetched with ``dbutils.jobs.taskValues.get`` and
    passed in the argument position/keyword it was wired to. Unbound parameters fall back to the
    callable's own defaults.
    """
    lines: list[str] = []
    call_positional: list[str] = []
    call_keywords: list[str] = []

    def _reader(dep_var: str) -> str:
        dep_key = var_to_task_key.get(dep_var, dep_var)
        return f"dbutils.jobs.taskValues.get(taskKey='{dep_key}', key='return_value', debugValue=None)"

    for position in sorted(set(tf.positional_deps) | set(tf.positional_values)):
        if position in tf.positional_deps:
            variable = f"_upstream_{position}"
            lines.append(f"{variable} = {_reader(tf.positional_deps[position])}")
            call_positional.append(variable)
        else:
            call_positional.append(tf.positional_values[position])
    for name, dep_var in tf.keyword_deps.items():
        variable = f"_upstream_{name}"
        lines.append(f"{variable} = {_reader(dep_var)}")
        call_keywords.append(f"{name}={variable}")
    call_keywords.extend(f"{name}={value}" for name, value in tf.keyword_values.items())
    if tf.expand_kwarg is not None:
        # .expand(param=[...]) fan-out: each for_each `inputs` element is the JSON text of the
        # original value (see _capture_expand), so json.loads on the injected `item` widget recovers
        # it exactly -- ints stay ints and JSON-looking strings stay strings. The except is a defensive
        # fallback for an unexpected raw value.
        lines.append("_raw_item = dbutils.widgets.get('item')")
        lines.append("try:")
        lines.append("    _expand_item = json.loads(_raw_item)")
        lines.append("except (ValueError, TypeError):")
        lines.append("    _expand_item = _raw_item")
        call_keywords.append(f"{tf.expand_kwarg}=_expand_item")

    call_args = ", ".join(call_positional + call_keywords)
    returns = any(isinstance(n, ast.Return) and n.value is not None for n in ast.walk(func))
    prefix = "result = " if returns else ""
    lines.append(f"{prefix}{func.name}({call_args})")
    if returns:
        lines.append("dbutils.jobs.taskValues.set(key='return_value', value=result)")
    return "\n".join(lines) + "\n"


def _declared_param_default(name: str, dag_params: dict[str, Any], schedule: dict[str, object] | None) -> Any:
    """Returns the Databricks-required default for a declared job parameter.

    A DAG ``params={...}`` default wins. A macro-derived parameter (``run_date`` etc. from an Airflow
    ``{{ ds }}``/``execution_date`` macro, or ``run_id``) gets its schedule-aware / inline default so
    the value resolves at run time (and a native backfill can override a logical date). Everything else
    defaults to an empty string.
    """
    if dag_params.get(name) is not None:
        return dag_params[name]
    macro_default = templating.macro_param_default(name, schedule)
    if macro_default is not None:
        return macro_default
    return ""


def _convert_activity_templates(activity: Activity) -> set[str]:
    """Converts Airflow Jinja in an activity's parameter fields to DAB refs.

    Mutates ``base_parameters`` (NotebookActivity), ``parameters`` (Spark/Sql/RunJob),
    ``job_parameters`` (RunJob), and ``sql`` (SqlActivity) in place, returning the set
    of ``{{job.parameters.X}}`` names referenced so the pipeline can declare them.
    """
    referenced: set[str] = set()
    for attr in ("base_parameters", "job_parameters", "parameters"):
        value = getattr(activity, attr, None)
        if value:
            converted, refs = templating.convert_params(value)
            setattr(activity, attr, converted)
            referenced |= refs
    if isinstance(activity, SqlActivity):
        # SQL dynamic refs must go through :name markers + sql_task.parameters, not inline text.
        marked_sql, sql_params = templating.convert_sql_template(activity.sql)
        activity.sql = marked_sql
        activity.parameters = {**(activity.parameters or {}), **sql_params}
        # sql_task.parameters values that resolve to {{job.parameters.X}} need X declared.
        for value in sql_params.values():
            referenced |= set(_JOB_PARAM_REF.findall(value))
    # generated_source was already rewritten (Variable.get -> dbutils.widgets.get); collect the
    # widget names so the pipeline declares them as job parameters. Skip the internal __flowx_*
    # widgets (op_args/op_kwargs) -- those are fed by the task's base_parameters, not job params.
    generated = getattr(activity, "generated_source", None)
    if isinstance(generated, str):
        referenced |= {name for name in _WIDGET_GET.findall(generated) if not name.startswith("__flowx_")}
    return referenced


_WIDGET_GET = re.compile(r"""dbutils\.widgets\.get\(\s*['"]([A-Za-z_][A-Za-z0-9_]*)['"]\s*\)""")
_JOB_PARAM_REF = re.compile(r"\{\{\s*job\.parameters\.([A-Za-z0-9_]+)\s*\}\}")


def _unresolved_activity_templates(activity: Activity) -> set[str]:
    """Returns residual Airflow Jinja expressions in task parameter fields."""
    unresolved: set[str] = set()
    for attribute in ("base_parameters", "job_parameters", "parameters", "sql", "generated_source"):
        unresolved |= templating.unresolved_jinja_expressions(getattr(activity, attribute, None))
    return unresolved


def _rewire_dropped(upstreams: dict[str, list[str]], dropped: set[str]) -> dict[str, list[str]]:
    """Returns upstream edges with *dropped* vars removed and their edges bridged.

    A downstream of a dropped node inherits the dropped node's (transitive)
    non-dropped upstreams, so the DAG stays connected after Dummy/Empty and
    lifted sensors are removed.
    """

    def resolve(var: str, seen: set[str]) -> list[str]:
        result: list[str] = []
        for up in upstreams.get(var, []):
            if up in dropped:
                if up not in seen:
                    result.extend(resolve(up, seen | {up}))
            else:
                result.append(up)
        # De-dup while preserving order.
        return list(dict.fromkeys(result))

    return {var: resolve(var, {var}) for var in upstreams if var not in dropped}


def _root_trigger_sensor(
    operators: dict[str, tuple[str, str, dict[str, ast.expr]]],
    upstreams: dict[str, list[str]],
    all_task_vars: set[str],
) -> tuple[str, set[str]] | None:
    """Returns a root sensor and its proven descendant set, or None.

    Only a sensor with no upstreams (the DAG's entry gate) can lift to a file_arrival /
    table_update trigger: mid-DAG sensors are ordering gates within the run and must stay
    as tasks. The sensor must reach every non-sensor task; otherwise lifting it would gate
    independent work that Airflow did not gate. File sensors win over table sensors when both
    qualify. A table/SQL sensor lifts only when it names a literal table.
    """
    adjacency: dict[str, set[str]] = {var: set() for var in all_task_vars}
    for downstream, dependencies in upstreams.items():
        for upstream in dependencies:
            adjacency.setdefault(upstream, set()).add(downstream)

    def _descendants(root: str) -> set[str]:
        descendants: set[str] = set()
        stack = list(adjacency.get(root, ()))
        while stack:
            current = stack.pop()
            if current in descendants:
                continue
            descendants.add(current)
            stack.extend(adjacency.get(current, ()))
        return descendants

    sensor_vars = {var for var, (_id, operator, _kwargs) in operators.items() if operator.endswith("Sensor")}
    required = all_task_vars - sensor_vars
    candidates = [
        var
        for var, (_id, operator, _kwargs) in operators.items()
        if operator in ops.FILE_SENSORS and not upstreams.get(var)
    ]
    candidates.extend(
        var
        for var, (_id, operator, kwargs) in operators.items()
        if operator in ops.TABLE_SENSORS
        and not upstreams.get(var)
        and ops.literal_str(kwargs.get("table_name")) is not None
    )
    for candidate in candidates:
        descendants = _descendants(candidate)
        if required <= descendants:
            return candidate, descendants
    return None


def _trigger_from_sensor(operator: str, kwargs: dict[str, ast.expr]) -> dict[str, object] | None:
    """Builds a job-level trigger dict from a single sensor's operator + kwargs.

    File sensors (S3/GCS/File/HDFS) -> ``trigger.file_arrival``; table sensors with a literal
    ``table_name`` -> ``trigger.table_update``. Returns None when the sensor can't lift.
    """
    if operator in ops.FILE_SENSORS:
        url = ops.file_sensor_path(kwargs) or "<file_arrival_url>"
        return {"kind": "file_arrival", "url": url, "pause_status": "UNPAUSED"}
    if operator in ops.TABLE_SENSORS:
        table_name = ops.literal_str(kwargs.get("table_name"))
        if table_name is not None:
            return {
                "kind": "table_update",
                "table_names": [table_name],
                "condition": "ANY_UPDATED",
                "pause_status": "UNPAUSED",
            }
    return None


def _build_dbt_factory(
    task_id: str,
    task_key: str,
    kwargs_list: list[dict[str, ast.expr]],
    depends_on: list[Dependency] | None,
    dbt_mode: str = "static",
    operator_types: list[str] | None = None,
) -> DbtFactoryActivity:
    """Builds a DbtFactoryActivity from cosmos config or a set of dbt CLI operators.

    Extracts project_dir / profiles_dir / target from cosmos ProjectConfig/ProfileConfig
    args or dbt operator kwargs. ``dbt_mode`` selects the render mode (static | pydabs);
    the manifest is read at package time from project_dir/target/manifest.json.
    """
    project_dir = "."
    profiles_dir = "dbt_profiles"
    target = "dev"
    manifest_path: str | None = None
    selectors: list[str] = []
    exclude_selectors: list[str] = []
    variables: dict[str, Any] | str | None = None
    full_refresh = False
    for kwargs in kwargs_list:
        # dbt CLI operators pass project_dir/target directly as kwargs.
        project_dir = ops.literal_str(kwargs.get("project_dir")) or ops.literal_str(kwargs.get("dir")) or project_dir
        profiles_dir = ops.literal_str(kwargs.get("profiles_dir")) or profiles_dir
        target = ops.literal_str(kwargs.get("target")) or ops.literal_str(kwargs.get("target_name")) or target
        # Cosmos nests config in ProjectConfig(...) / ProfileConfig(...) calls.
        project_dir = _cosmos_project_dir(kwargs.get("project_config")) or project_dir
        target = _cosmos_target(kwargs.get("profile_config")) or target
        manifest_path = _cosmos_manifest_path(kwargs.get("project_config")) or manifest_path
        selectors.extend(_dbt_selector_list(ops.literal_value(kwargs.get("select") or kwargs.get("models"))))
        exclude_selectors.extend(_dbt_selector_list(ops.literal_value(kwargs.get("exclude"))))
        dbt_variables = ops.literal_value(kwargs.get("vars"))
        if isinstance(dbt_variables, (dict, str)):
            variables = dbt_variables
        full_refresh = full_refresh or ops.literal_value(kwargs.get("full_refresh")) is True
    # The static preparer needs the standard manifest produced under target/ unless Cosmos supplied
    # an explicit manifest path. Without this the child job would be empty.
    if manifest_path is None:
        base = project_dir.rstrip("/") if project_dir not in ("", ".") else "."
        manifest_path = f"{base}/target/manifest.json" if base != "." else "target/manifest.json"
    commands = {
        ops.DBT_OPERATOR_COMMAND[operator] for operator in operator_types or [] if operator in ops.DBT_OPERATOR_COMMAND
    }
    resource_types: set[str] = set()
    for command in commands:
        if command == "build":
            resource_types.update(("model", "seed", "snapshot", "test"))
        elif command == "deps":
            resource_types.add("dependency")
        else:
            resource_types.add({"run": "model", "seed": "seed", "snapshot": "snapshot", "test": "test"}[command])
    if not operator_types or any(operator in ops.COSMOS_CONSTRUCTS for operator in operator_types):
        resource_types.update(("model", "seed", "snapshot", "test"))
    return DbtFactoryActivity(
        name=task_id,
        task_key=task_key,
        depends_on=depends_on,
        project_dir=project_dir,
        profiles_dir=profiles_dir,
        target=target,
        manifest_path=manifest_path,
        render_mode="pydabs" if dbt_mode == "pydabs" else "static",
        selectors=list(dict.fromkeys(selectors)),
        exclude_selectors=list(dict.fromkeys(exclude_selectors)),
        variables=variables,
        full_refresh=full_refresh,
        resource_types=sorted(resource_types),
    )


def _dbt_selector_list(value: Any) -> list[str]:
    """Returns literal dbt selectors as a normalized string list."""
    if isinstance(value, str):
        return [value]
    if isinstance(value, (list, tuple)):
        return [selector for selector in value if isinstance(selector, str)]
    return []


def _cosmos_project_dir(node: ast.expr | None) -> str | None:
    """Extracts the dbt project path from a cosmos ``ProjectConfig(...)`` call.

    Accepts the path as the first positional arg or as ``dbt_project_path=`` /
    ``project_dir=``. Returns None when *node* is not such a call.
    """
    if not isinstance(node, ast.Call):
        return None
    if node.args:
        positional = ops.literal_str(node.args[0])
        if positional:
            return positional
    kwargs = {kw.arg: kw.value for kw in node.keywords if kw.arg}
    return ops.literal_str(kwargs.get("dbt_project_path")) or ops.literal_str(kwargs.get("project_dir"))


def _cosmos_target(node: ast.expr | None) -> str | None:
    """Extracts ``target_name`` from a cosmos ``ProfileConfig(...)`` call."""
    if not isinstance(node, ast.Call):
        return None
    kwargs = {kw.arg: kw.value for kw in node.keywords if kw.arg}
    return ops.literal_str(kwargs.get("target_name"))


def _cosmos_manifest_path(node: ast.expr | None) -> str | None:
    """Extracts an explicit ``manifest_path`` from a cosmos ``ProjectConfig(...)`` call, if any."""
    if not isinstance(node, ast.Call):
        return None
    kwargs = {kw.arg: kw.value for kw in node.keywords if kw.arg}
    return ops.literal_str(kwargs.get("manifest_path"))


def discover_dags(source_path: Path) -> list[Path]:
    """Returns the DAG ``.py`` files under *source_path*.

    Accepts either a single ``.py`` file or a directory (scanned recursively).
    Files whose source contains no ``DAG(`` construct are skipped so helper
    modules in a DAGs folder are not mistaken for DAG definitions.
    """
    source_path = Path(source_path)
    candidates = [source_path] if source_path.is_file() else sorted(source_path.rglob("*.py"))
    dags: list[Path] = []
    for candidate in candidates:
        if candidate.suffix != ".py":
            continue
        try:
            text = candidate.read_text(encoding="utf-8")
        except OSError:
            continue
        if "DAG(" in text or "@dag" in text:
            dags.append(candidate)
    return dags


def load_pipelines(
    source_path: Path,
    pipeline: str | None = None,
    *,
    dbt_mode: str = "static",
    exclude_dags: set[str] | None = None,
) -> list[Pipeline]:
    """Loads every DAG under *source_path* into Pipeline IR.

    Args:
        source_path: A DAG ``.py`` file or a directory of them.
        pipeline: When set, keep only the pipeline whose name (dag_id) matches.
        dbt_mode: dbt-factory render mode -- ``"static"`` (default) or ``"pydabs"``.

    Returns:
        One :class:`~flowx.models.ir.Pipeline` per discovered DAG, filtered to
        *pipeline* when provided.
    """
    root = source_path if source_path.is_dir() else source_path.parent
    pipelines = [
        loaded
        for dag_path in discover_dags(source_path)
        for loaded in load_airflow_dags(
            dag_path,
            dbt_mode=dbt_mode,
            source_file=source_audit.source_label(dag_path, root),
        )
    ]
    if pipeline is not None:
        pipelines = [p for p in pipelines if p.name == pipeline]
    excluded = set(exclude_dags or ())
    for loaded in pipelines:
        if loaded.name in excluded:
            loaded.migration_status = "excluded"
            count = int(loaded.audit.get("audited_activity_count", 0))
            loaded.audit.update(
                {
                    "deterministic_count": 0,
                    "agentic_count": 0,
                    "failed_count": 0,
                    "excluded_count": count,
                }
            )
    if excluded:
        _replace_excluded_dag_references(pipelines, excluded)
    return pipelines


def _replace_excluded_dag_references(pipelines: list[Pipeline], excluded: set[str]) -> None:
    """Replaces included-to-excluded run-job references with explicit placeholders."""
    excluded_by_key = {normalize_task_key(name): name for name in excluded}
    for pipeline in pipelines:
        if pipeline.migration_status == "excluded":
            continue
        for index, task in enumerate(pipeline.tasks):
            if isinstance(task, RunJobActivity) and task.job_name in excluded_by_key:
                excluded_name = excluded_by_key[task.job_name]
                placeholder = PlaceholderActivity(
                    name=task.name,
                    task_key=task.task_key,
                    depends_on=task.depends_on,
                    original_type="ExcludedDagReference",
                    comment=f"Referenced Airflow DAG {excluded_name!r} was excluded from this migration.",
                    raw_definition={"excluded_dag": excluded_name},
                )
                pipeline.tasks[index] = placeholder
                entry = source_audit.finding(
                    source_file=str(pipeline.audit.get("source_file", "")),
                    code="excluded_dag_reference",
                    severity="gap",
                    message=f"Task {task.task_key!r} references excluded DAG {excluded_name!r}.",
                    details={"task_key": task.task_key, "excluded_dag": excluded_name},
                )
                pipeline.not_translatable.append(entry)
                if pipeline.reconciliation_status != "failed":
                    pipeline.reconciliation_status = "verified_with_gaps"
                pipeline.audit["agentic_count"] = int(pipeline.audit.get("agentic_count", 0)) + 1
                pipeline.audit["deterministic_count"] = max(0, int(pipeline.audit.get("deterministic_count", 0)) - 1)


_HOST_PATTERN = re.compile(r"https://([A-Za-z0-9._-]*(?:azuredatabricks\.net|databricks\.com|cloud\.databricks\.com))")


def detect_hosts(source_path: Path) -> list[str]:
    """Returns Databricks workspace hosts referenced by the DAG files under *source_path*.

    Scans DAG source text for ``https://<workspace>.azuredatabricks.net`` /
    ``.databricks.com`` URLs (e.g. in a DatabricksNotebook/RunNow operator's host or a
    connection default). Returns a sorted, de-duplicated list; empty when none are found.
    """
    hosts: set[str] = set()
    for dag_path in discover_dags(source_path):
        try:
            text = dag_path.read_text(encoding="utf-8")
        except OSError:
            continue
        hosts.update(match.rstrip("/") for match in _HOST_PATTERN.findall(text))
    return sorted(hosts)
