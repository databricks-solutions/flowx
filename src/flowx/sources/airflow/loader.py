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
time sensors -> schedule), and Tier 4 (unmapped -> PlaceholderActivity).
``>>`` / ``<<`` dependencies and cron ``schedule_interval`` -> Quartz are
handled here.
"""

from __future__ import annotations

import ast
import json
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from flowx.models.ir import (
    Activity,
    DbtFactoryActivity,
    Dependency,
    ForEachActivity,
    Pipeline,
    SqlActivity,
)
from flowx.sources.airflow import callable_notebook, templating
from flowx.sources.airflow import operators as ops


@dataclass(slots=True)
class _TaskFlowTask:
    """A TaskFlow ``@task`` invocation captured from a ``@dag`` body.

    ``positional_deps`` / ``keyword_deps`` map each argument position / keyword the callable was
    invoked with to the upstream task var it references (TaskFlow's implicit XCom data flow), so the
    emitted notebook can read that upstream's return value via ``dbutils.jobs.taskValues``. Literal
    args are ignored (the callable's own defaults apply).
    """

    task_id: str
    def_name: str
    decorator: str
    positional_deps: dict[int, str] = field(default_factory=dict)
    keyword_deps: dict[str, str] = field(default_factory=dict)


def _sanitize_task_key(name: str) -> str:
    """Converts an Airflow task_id into a valid Databricks task key."""
    import re

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
            return f"{_shift_token(lo)}-{_shift_token(hi)}{step}"
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
    if dow == "*" and dom != "*":
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
    if not isinstance(node, ast.Call):
        return None
    func = node.func
    name = func.attr if isinstance(func, ast.Attribute) else (func.id if isinstance(func, ast.Name) else "")
    if name != "timedelta":
        return None
    total = 0
    for kw in node.keywords:
        if kw.arg in _TIMEDELTA_UNIT_SECONDS and isinstance(kw.value, ast.Constant):
            if isinstance(kw.value.value, int):
                total += kw.value.value * _TIMEDELTA_UNIT_SECONDS[kw.arg]
    if total <= 0:
        return None
    for unit, unit_seconds in (("WEEKS", 604800), ("DAYS", 86400), ("HOURS", 3600)):
        if total % unit_seconds == 0:
            return {"kind": "periodic", "interval": total // unit_seconds, "unit": unit, "pause_status": "UNPAUSED"}
    return None


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
        quartz: str | None = _CRON_PRESETS.get(interval) or _cron_to_quartz(interval)
        if quartz is not None:
            return {
                "kind": "schedule",
                "quartz_cron_expression": quartz,
                "timezone_id": timezone or "UTC",
                "pause_status": "UNPAUSED",
            }
    return _timedelta_to_periodic(node)


class _DagVisitor(ast.NodeVisitor):
    """Collects operator calls, dependency edges, and the DAG's schedule."""

    def __init__(self, module: ast.Module) -> None:
        self._functions: dict[str, ast.FunctionDef] = {
            node.name: node for node in _iter_functions(module) if isinstance(node, ast.FunctionDef)
        }
        # task variable name -> (task_id, operator, kwargs)
        self.operators: dict[str, tuple[str, str, dict[str, ast.expr]]] = {}
        # task variable name -> the operator's ast.Call node (for source-slicing placeholders)
        self.calls: dict[str, ast.Call] = {}
        self.edges: list[tuple[str, str]] = []  # (upstream_var, downstream_var)
        self.dag_id: str | None = None
        self.schedule_interval: str | None = None
        self.schedule_node: ast.expr | None = None
        self.timezone: str | None = None
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
        # TaskFlow: function name -> (FunctionDef, decorator dotted-name) for @task-decorated defs.
        # Pre-scanned so a @task def defined after the @dag body that uses it is still resolved.
        self.taskflow_defs: dict[str, tuple[ast.FunctionDef, str]] = {}
        for fn in _iter_functions(module):
            decorator = next(
                (_decorator_name(d) for d in fn.decorator_list if _decorator_name(d) in _TASK_DECORATORS), None
            )
            if decorator is not None:
                self.taskflow_defs[fn.name] = (fn, decorator)
        # TaskFlow task instances: var name -> _TaskFlowTask (id, def-name, decorator, arg bindings).
        self.taskflow_tasks: dict[str, _TaskFlowTask] = {}
        self._taskflow_counter = 0
        # A @dag-decorated function was found (so a bare `@task` file is still recognized as a DAG).
        self.is_taskflow_dag: bool = False

    def functions(self) -> dict[str, ast.FunctionDef]:
        return self._functions

    def visit_FunctionDef(self, node: ast.FunctionDef) -> None:
        # A @task-decorated function defines a task from its callable; its body is task logic, not DAG
        # structure, so don't descend. @dag marks the DAG-defining function: read its config off the
        # decorator, then descend so the body's task instances / edges are collected.
        if _has_decorator(node, _TASK_DECORATORS):
            return
        if _has_decorator(node, _DAG_DECORATORS):
            self.is_taskflow_dag = True
            dag_kwargs = _decorator_kwargs(node.decorator_list, _DAG_DECORATORS)
            self._apply_dag_kwargs(dag_kwargs)
            if self.dag_id is None:
                self.dag_id = ops.literal_str(dag_kwargs.get("dag_id")) or node.name
        self.generic_visit(node)

    def visit_Assign(self, node: ast.Assign) -> None:
        if len(node.targets) == 1 and isinstance(node.targets[0], ast.Name) and isinstance(node.value, ast.Call):
            var = node.targets[0].id
            direct = _direct_operator_call(node.value)
            mapped = None if direct is not None else _mapped_operator_call(node.value)
            call = direct or mapped
            if call is not None and isinstance(call.func, ast.Name):
                construct = call.func.id
                kwargs = {kw.arg: kw.value for kw in call.keywords if kw.arg}
                task_id = ops.literal_str(kwargs.get("task_id")) or ops.literal_str(kwargs.get("group_id")) or var
                self.operators[var] = (task_id, construct, kwargs)
                self.calls[var] = call
                if mapped is not None:
                    self.mapped.add(var)
                if self._group_stack:
                    self.groups[var] = "__".join(self._group_stack)
            elif self._register_taskflow_call(node.value, var):
                pass  # a `x = mytask(...)` TaskFlow invocation, captured with var as its key
        self.generic_visit(node)

    def _taskflow_def_name(self, call: ast.Call) -> tuple[str | None, bool, str | None]:
        """Resolves a call's underlying ``@task`` def name, unwrapping ``.expand`` / ``.override``.

        Returns ``(def_name_or_None, is_mapped, override_task_id)``.
        """
        func = call.func
        mapped = False
        override_id: str | None = None
        while isinstance(func, ast.Attribute):
            if func.attr == "expand":
                mapped = True
            elif func.attr == "override":
                override_id = ops.literal_str({kw.arg: kw.value for kw in call.keywords if kw.arg}.get("task_id"))
            func = func.value
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
        if mapped:
            self.mapped.add(var)
        if self._group_stack:
            self.groups[var] = "__".join(self._group_stack)
        # Bind each arg that resolves to an upstream task var, and add the data-flow edge.
        for index, arg in enumerate(call.args):
            dep = self._resolve_taskflow_arg(arg)
            if dep is not None:
                task.positional_deps[index] = dep
                self.edges.append((dep, var))
        for kw in call.keywords:
            if kw.arg is None:
                continue
            dep = self._resolve_taskflow_arg(kw.value)
            if dep is not None:
                task.keyword_deps[kw.arg] = dep
                self.edges.append((dep, var))
        return True

    def _resolve_taskflow_arg(self, arg: ast.expr) -> str | None:
        """Returns the upstream task var an argument refers to, else None (a literal / unknown).

        A bare ``Name`` is an existing task var. A nested ``@task`` call (``transform(extract())``)
        is registered as its own synthetic task instance and its var returned, so the whole
        expression tree becomes a chain of task instances.
        """
        if isinstance(arg, ast.Name):
            return arg.id
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
        for item in node.items:
            call = item.context_expr
            if isinstance(call, ast.Call) and isinstance(call.func, ast.Name):
                if call.func.id == "DAG":
                    self._read_dag_kwargs(call)
                elif call.func.id == "TaskGroup":
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
                elif _is_task_construct(call.func.id) and item.optional_vars is not None:
                    # `with DbtTaskGroup(...) as g:` — a cosmos group bound to a name.
                    if isinstance(item.optional_vars, ast.Name):
                        var = item.optional_vars.id
                        kwargs = {kw.arg: kw.value for kw in call.keywords if kw.arg}
                        task_id = ops.literal_str(kwargs.get("group_id")) or var
                        self.operators[var] = (task_id, call.func.id, kwargs)
                        self.calls[var] = call
        self.generic_visit(node)
        if pushed_group:
            self._group_stack.pop()

    def _read_dag_kwargs(self, call: ast.Call) -> None:
        kwargs = {kw.arg: kw.value for kw in call.keywords if kw.arg}
        self.dag_id = ops.literal_str(kwargs.get("dag_id"))
        self._apply_dag_kwargs(kwargs)

    def _apply_dag_kwargs(self, kwargs: dict[str, ast.expr]) -> None:
        self.schedule_node = kwargs.get("schedule_interval") or kwargs.get("schedule")
        self.schedule_interval = ops.literal_str(kwargs.get("schedule_interval")) or ops.literal_str(
            kwargs.get("schedule")
        )
        self.timezone = _extract_timezone(kwargs.get("start_date")) or _extract_timezone(kwargs.get("timezone"))
        # default_args is a dict literal of DAG-wide task settings (retries, timeouts, email).
        default_args = kwargs.get("default_args")
        if isinstance(default_args, ast.Dict):
            self.default_args = {
                key.value: val
                for key, val in zip(default_args.keys, default_args.values)
                if isinstance(key, ast.Constant) and isinstance(key.value, str)
            }
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
            self._collect_shift_chain(value)
        elif isinstance(value, ast.Call):
            # A bare TaskFlow call (`extract()` with no assignment) is a task instance keyed by its
            # def name; otherwise it may be a set_upstream/set_downstream dependency call.
            func = value.func
            if isinstance(func, ast.Name) and func.id in self.taskflow_defs and func.id not in self.taskflow_tasks:
                self._register_taskflow_call(value, func.id)
            else:
                self._collect_set_dependency(value)
        self.generic_visit(node)

    def _collect_shift_chain(self, binop: ast.BinOp) -> None:
        # Each chain position is a *group* of task names (a bare name, a [list], or an inline
        # TaskFlow call like `extract()`); adjacent groups are connected as a cross-product so
        # `a >> [b, c]` yields a->b and a->c.
        groups = [self._shift_position_names(node) for node in _flatten_shift_nodes(binop)]
        groups = [g for g in groups if g]
        if len(groups) < 2:
            return
        rightward = isinstance(binop.op, ast.RShift)
        for upstream_group, downstream_group in zip(groups, groups[1:]):
            up, down = (upstream_group, downstream_group) if rightward else (downstream_group, upstream_group)
            for u in up:
                for d in down:
                    self.edges.append((u, d))

    def _shift_position_names(self, node: ast.expr) -> list[str]:
        # A shift-chain position resolves to task vars. An inline TaskFlow call (`extract()`) is
        # registered as its own instance so `prep >> finalize()` doesn't drop finalize.
        if isinstance(node, (ast.List, ast.Tuple)):
            names: list[str] = []
            for elt in node.elts:
                names.extend(self._shift_position_names(elt))
            return names
        if isinstance(node, ast.Name):
            return [node.id]
        if isinstance(node, ast.Call):
            def_name, _mapped, _override = self._taskflow_def_name(node)
            if def_name is not None:
                self._taskflow_counter += 1
                synthetic = f"{def_name}__tf{self._taskflow_counter}"
                self._register_taskflow_call(node, synthetic)
                return [synthetic]
        return []

    def _collect_set_dependency(self, call: ast.Call) -> None:
        # `x.set_upstream(y)` / `x.set_downstream(y)` where y is a Name or a list of Names.
        func = call.func
        if not (isinstance(func, ast.Attribute) and isinstance(func.value, ast.Name) and call.args):
            return
        this = func.value.id
        others = _names_in(call.args[0])
        if func.attr == "set_downstream":
            self.edges.extend((this, other) for other in others)
        elif func.attr == "set_upstream":
            self.edges.extend((other, this) for other in others)


def _expand_group_edges(
    edges: list[tuple[str, str]],
    operators: dict[str, tuple[str, str, dict[str, ast.expr]]],
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


def _referenced_names(node: ast.expr) -> set[str]:
    """Every bare Name id loaded anywhere in *node* (for TaskFlow data-flow edge detection)."""
    return {n.id for n in ast.walk(node) if isinstance(n, ast.Name) and isinstance(n.ctx, ast.Load)}


def _names_in(node: ast.expr) -> list[str]:
    """Returns the task-variable names in a Name or a ``[Name, ...]`` list node."""
    if isinstance(node, ast.Name):
        return [node.id]
    if isinstance(node, (ast.List, ast.Tuple)):
        return [elt.id for elt in node.elts if isinstance(elt, ast.Name)]
    return []


def _flatten_shift_nodes(node: ast.expr) -> list[ast.expr]:
    """Flattens a ``>>`` / ``<<`` chain into its per-position operand nodes, left to right.

    ``a >> [b, c] >> d()`` becomes ``[Name('a'), List([b, c]), Call(d)]``; the caller resolves each
    position to task vars (registering an inline TaskFlow call along the way).
    """
    if isinstance(node, ast.BinOp) and isinstance(node.op, (ast.RShift, ast.LShift)):
        return _flatten_shift_nodes(node.left) + _flatten_shift_nodes(node.right)
    return [node]


def _direct_operator_call(node: ast.Call) -> ast.Call | None:
    """Returns *node* if it is a direct ``SomeOperator(...)`` / ``SomeSensor(...)`` call."""
    if isinstance(node.func, ast.Name) and _is_task_construct(node.func.id):
        return node
    return None


def _mapped_operator_call(node: ast.Call) -> ast.Call | None:
    """Returns the underlying operator call for a dynamic-mapping ``.expand(...)`` chain.

    Handles ``Op(...).expand(...)`` and ``Op.partial(...).expand(...)``. The returned
    Call's keywords are the merged operator kwargs (partial args + expand args), and its
    ``.func`` is the operator Name, so the caller treats it like a direct operator call.
    The mapped kwargs let the loader wrap the operator in a for_each_task.
    """
    if not (isinstance(node.func, ast.Attribute) and node.func.attr == "expand"):
        return None
    inner = node.func.value  # the Op(...) or Op.partial(...) call
    if not isinstance(inner, ast.Call):
        return None
    if isinstance(inner.func, ast.Name) and _is_task_construct(inner.func.id):
        operator_name = inner.func.id  # Op(...).expand(...)
    elif (
        isinstance(inner.func, ast.Attribute)
        and inner.func.attr == "partial"
        and isinstance(inner.func.value, ast.Name)
        and _is_task_construct(inner.func.value.id)
    ):
        operator_name = inner.func.value.id  # Op.partial(...).expand(...)
    else:
        return None
    merged = ast.Call(
        func=ast.Name(id=operator_name, ctx=ast.Load()),
        args=[],
        keywords=list(inner.keywords) + list(node.keywords),
    )
    return merged


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


def _has_decorator(func: ast.FunctionDef, names: frozenset[str]) -> bool:
    return any(_decorator_name(dec) in names for dec in func.decorator_list)


def load_airflow_dag(dag_path: Path, *, dbt_mode: str = "static") -> Pipeline:
    """Parses an Airflow DAG file into a flowx Pipeline IR.

    Args:
        dag_path: Path to a ``.py`` DAG module.
        dbt_mode: dbt-factory render mode for any dbt workload -- ``"static"`` (default,
            an inner job of per-node tasks) or ``"pydabs"`` (a deploy-time hook module).

    Returns:
        A :class:`~flowx.models.ir.Pipeline`. Mapped operators become their IR
        node (NotebookActivity, SparkPython/JarActivity, RunJobActivity,
        DbtFactoryActivity, ...); Dummy/Empty are dropped with dependency
        rewiring; file sensors lift to a job-level file_arrival trigger; time
        sensors are absorbed into the schedule; unmapped operators become a
        PlaceholderActivity.
    """
    source = Path(dag_path).read_text(encoding="utf-8")
    module = ast.parse(source)
    visitor = _DagVisitor(module)
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
    var_to_task_key = {var: _task_key(var, tid) for var, tid in var_task_ids.items()}

    # Expand group-level edges (`group_a >> group_b`, `task >> group`, ...) into edges between the
    # groups' boundary tasks: leaves of the upstream group -> roots of the downstream group, matching
    # Airflow's TaskGroup dependency semantics. A non-group var resolves to itself.
    edges = _expand_group_edges(visitor.edges, visitor.operators, visitor.groups, visitor.group_vars)

    # Build the upstream adjacency in dependency terms, then drop structural nodes
    # (Dummy/Empty, file/time sensors) by rewiring their downstreams to their upstreams.
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

    # Dummy/Empty and time sensors always drop (structural / absorbed into the schedule as a delay).
    dropped = {
        var
        for var, (_, op, _) in visitor.operators.items()
        if op in ops.DUMMY_OPERATORS or op in ops.TIME_SENSORS
    }
    if not has_schedule:
        trigger_var = _root_trigger_sensor(visitor.operators, upstreams)
        if trigger_var is not None:
            trigger = _trigger_from_sensor(*visitor.operators[trigger_var][1:])
            if trigger is not None:
                schedule = trigger
                dropped.add(trigger_var)
    upstreams = _rewire_dropped(upstreams, dropped)

    # Collapse all dbt CLI operators over the one project into a single DbtFactoryActivity emitted at
    # the first dbt task's position. Every dbt var's task_key remaps to that single key, so a
    # downstream task that depended on a later dbt op (e.g. `dbt_test`) points at the factory task
    # rather than a task_key that was never emitted (which would dangle).
    dbt_vars = [var for var, (_, op, _) in visitor.operators.items() if op in ops.DBT_CLI_OPERATORS]
    dbt_factory_key = var_to_task_key[dbt_vars[0]] if dbt_vars else None
    dbt_key_remap = {var_to_task_key[v]: dbt_factory_key for v in dbt_vars} if dbt_factory_key else {}

    def _dep(upstream_var: str, outcome: str | None) -> str:
        key = var_to_task_key[upstream_var]
        return dbt_key_remap.get(key, key)

    tasks: list[Activity] = []
    referenced_params: set[str] = set()
    emitted_dbt = False
    for var, (task_id, operator, kwargs) in visitor.operators.items():
        if var in dropped:
            continue
        task_key = var_to_task_key[var]
        outcome = templating.trigger_rule_outcome(kwargs)
        # Remap dbt-chain upstreams to the single factory key and drop self-edges (a dbt op
        # depending on another dbt op in the same collapsed chain).
        dep_keys = {_dep(u, outcome) for u in upstreams[var]}
        dep_keys.discard(task_key if operator not in ops.DBT_CLI_OPERATORS else dbt_factory_key)
        depends_on = [Dependency(task_key=k, outcome=outcome) for k in sorted(dep_keys)] or None

        if operator in ops.COSMOS_CONSTRUCTS:
            tasks.append(_build_dbt_factory(task_id, task_key, [kwargs], depends_on, dbt_mode))
            continue
        if operator in ops.DBT_CLI_OPERATORS:
            # Emit one factory job for the whole dbt chain, at the first dbt task's position.
            if emitted_dbt:
                continue
            emitted_dbt = True
            dbt_kwargs = [visitor.operators[v][2] for v in dbt_vars]
            tasks.append(_build_dbt_factory(task_id, task_key, dbt_kwargs, depends_on, dbt_mode))
            continue

        call_node = visitor.calls.get(var)
        call_source = ast.get_source_segment(source, call_node) or "" if call_node is not None else ""
        ctx = ops.OperatorContext(
            task_id=task_id,
            task_key=task_key,
            operator=operator,
            kwargs=kwargs,
            functions=functions,
            source=source,
            call_source=call_source,
            default_args=visitor.default_args,
        )
        builder = ops.OPERATOR_REGISTRY.get(operator, ops.build_placeholder)
        activity = builder(ctx)
        activity.depends_on = depends_on
        # Stamp DAG/task retry + timeout policy (per-task kwargs override default_args).
        policy = templating.retry_policy(visitor.default_args, kwargs)
        activity.max_retries = policy.get("max_retries")
        activity.timeout_seconds = policy.get("timeout_seconds")
        activity.min_retry_interval_millis = policy.get("min_retry_interval_millis")
        # Convert Airflow Jinja in the activity's parameter fields to DAB refs; collect params.
        referenced_params |= _convert_activity_templates(activity)

        if var in visitor.mapped:
            # Dynamic mapping (.expand()) -> a for_each_task iterating the mapped operator.
            tasks.append(_wrap_in_for_each(activity, task_id, task_key, depends_on, kwargs))
        else:
            tasks.append(activity)

    # TaskFlow @task instances: emit each as a notebook that reads upstream return values via
    # dbutils.jobs.taskValues, calls the decorated function, and sets its own return value.
    for var, tf in visitor.taskflow_tasks.items():
        task_key = var_to_task_key[var]
        dep_keys = {var_to_task_key[u] for u in upstreams.get(var, []) if u in var_to_task_key}
        dep_keys.discard(task_key)
        depends_on = [Dependency(task_key=k) for k in sorted(dep_keys)] or None
        activity = _build_taskflow_task(tf, var_to_task_key, functions, source, task_key)
        activity.depends_on = depends_on
        referenced_params |= _convert_activity_templates(activity)
        tasks.append(activity)

    # Declare every job parameter -- those referenced in templates plus any from the DAG's
    # params={...} -- each with a default (Databricks requires one): the params={...} default when
    # present, else an empty string so the bundle still validates.
    param_names = referenced_params | set(visitor.dag_params)
    parameters = [
        {"name": name, "default": visitor.dag_params[name] if visitor.dag_params.get(name) is not None else ""}
        for name in sorted(param_names)
    ] or None
    return Pipeline(
        name=visitor.dag_id or Path(dag_path).stem,
        tasks=tasks,
        parameters=parameters,
        schedule=schedule,
        tags={"source": "airflow", "dag_id": visitor.dag_id or ""},
    )


def _wrap_in_for_each(
    activity: Activity,
    task_id: str,
    task_key: str,
    depends_on: list[Dependency] | None,
    kwargs: dict[str, ast.expr],
) -> ForEachActivity:
    """Wraps a dynamically-mapped operator in a ForEachActivity (-> for_each_task).

    Airflow ``.expand(x=[...])`` fans a task out over an iterable. The for_each's
    ``inputs`` is the first list-valued expand kwarg (rendered as a JSON array literal
    when it is a static list; otherwise ``{{job.parameters...}}`` is left for review).
    The mapped operator becomes the single inner activity, re-keyed so it doesn't
    collide with the for_each task key.
    """
    items = "[]"
    for key, node in kwargs.items():
        if key in ("task_id", "group_id"):
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
    from flowx.models.ir import NotebookActivity, PlaceholderActivity

    func = functions.get(tf.def_name)
    if func is None:
        return PlaceholderActivity(
            name=tf.task_id,
            task_key=task_key,
            original_type=f"@{tf.decorator}",
            comment=f"TaskFlow @{tf.decorator} '{tf.def_name}' could not be resolved; translate manually.",
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
    reason = callable_notebook.task_context_reason(func)
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

    # Positional args must stay contiguous from index 0 -- only bind a leading run of positions so a
    # gap doesn't shift later args. Any remaining bound positions are passed by parameter name.
    param_names = [a.arg for a in (func.args.posonlyargs + func.args.args)]
    index = 0
    while index in tf.positional_deps:
        var = f"_upstream_{index}"
        lines.append(f"{var} = {_reader(tf.positional_deps[index])}")
        call_positional.append(var)
        index += 1
    for pos, dep_var in sorted(tf.positional_deps.items()):
        if pos < index or pos >= len(param_names):
            continue
        name = param_names[pos]
        var = f"_upstream_{name}"
        lines.append(f"{var} = {_reader(dep_var)}")
        call_keywords.append(f"{name}={var}")
    for name, dep_var in tf.keyword_deps.items():
        var = f"_upstream_{name}"
        lines.append(f"{var} = {_reader(dep_var)}")
        call_keywords.append(f"{name}={var}")

    call_args = ", ".join(call_positional + call_keywords)
    returns = any(isinstance(n, ast.Return) and n.value is not None for n in ast.walk(func))
    prefix = "result = " if returns else ""
    lines.append(f"{prefix}{func.name}({call_args})")
    if returns:
        lines.append("dbutils.jobs.taskValues.set(key='return_value', value=result)")
    return "\n".join(lines) + "\n"


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
) -> str | None:
    """Returns the var of a root sensor eligible to become a job-level trigger, else None.

    Only a sensor with no upstreams (the DAG's entry gate) can lift to a file_arrival /
    table_update trigger: mid-DAG sensors are ordering gates within the run and must stay
    as tasks. File sensors win over table sensors when both sit at the root (Databricks jobs
    take a single trigger). A table/SQL sensor lifts only when it names a literal table; one
    without a ``table_name`` is an arbitrary-condition sensor kept as a polling task.
    """
    file_roots = [
        var
        for var, (_id, op, _kw) in operators.items()
        if op in ops.FILE_SENSORS and not upstreams.get(var)
    ]
    if file_roots:
        return file_roots[0]
    for var, (_id, op, kw) in operators.items():
        if op in ops.TABLE_SENSORS and not upstreams.get(var) and ops.literal_str(kw.get("table_name")) is not None:
            return var
    return None


def _trigger_from_sensor(operator: str, kwargs: dict[str, ast.expr]) -> dict[str, object] | None:
    """Builds a job-level trigger dict from a single sensor's operator + kwargs.

    File sensors (S3/GCS/File/HDFS) -> ``trigger.file_arrival``; table sensors with a literal
    ``table_name`` -> ``trigger.table_update``. Returns None when the sensor can't lift.
    """
    if operator in ops.FILE_SENSORS:
        url = (
            ops.literal_str(kwargs.get("bucket_key"))
            or ops.literal_str(kwargs.get("filepath"))
            or ops.literal_str(kwargs.get("filepath_"))
            or ops.literal_str(kwargs.get("bucket_name"))
            or "<file_arrival_url>"
        )
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
    for kwargs in kwargs_list:
        # dbt CLI operators pass project_dir/target directly as kwargs.
        project_dir = ops.literal_str(kwargs.get("project_dir")) or ops.literal_str(kwargs.get("dir")) or project_dir
        profiles_dir = ops.literal_str(kwargs.get("profiles_dir")) or profiles_dir
        target = ops.literal_str(kwargs.get("target")) or ops.literal_str(kwargs.get("target_name")) or target
        # Cosmos nests config in ProjectConfig(...) / ProfileConfig(...) calls.
        project_dir = _cosmos_project_dir(kwargs.get("project_config")) or project_dir
        target = _cosmos_target(kwargs.get("profile_config")) or target
        manifest_path = _cosmos_manifest_path(kwargs.get("project_config")) or manifest_path
    # The static preparer needs a manifest to explode into tasks. Point it at the per-target
    # manifest `make manifest` produces (target/<target>/manifest.json), rooted at the project dir,
    # unless cosmos gave an explicit manifest_path. Without this the child job would be empty.
    if manifest_path is None:
        base = project_dir.rstrip("/") if project_dir not in ("", ".") else "."
        manifest_path = f"{base}/target/{target}/manifest.json" if base != "." else f"target/{target}/manifest.json"
    return DbtFactoryActivity(
        name=task_id,
        task_key=task_key,
        depends_on=depends_on,
        project_dir=project_dir,
        profiles_dir=profiles_dir,
        target=target,
        manifest_path=manifest_path,
        render_mode="pydabs" if dbt_mode == "pydabs" else "static",
    )


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


def load_pipelines(source_path: Path, pipeline: str | None = None, *, dbt_mode: str = "static") -> list[Pipeline]:
    """Loads every DAG under *source_path* into Pipeline IR.

    Args:
        source_path: A DAG ``.py`` file or a directory of them.
        pipeline: When set, keep only the pipeline whose name (dag_id) matches.
        dbt_mode: dbt-factory render mode -- ``"static"`` (default) or ``"pydabs"``.

    Returns:
        One :class:`~flowx.models.ir.Pipeline` per discovered DAG, filtered to
        *pipeline* when provided.
    """
    pipelines = [load_airflow_dag(dag_path, dbt_mode=dbt_mode) for dag_path in discover_dags(source_path)]
    if pipeline is not None:
        pipelines = [p for p in pipelines if p.name == pipeline]
    return pipelines


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
