"""Operator, mapping, and decorator detection helpers."""

from __future__ import annotations

import ast
from typing import Any

from flowx.sources.airflow import operators as ops
from flowx.sources.airflow.loader.ast_utils import (
    _UNRESOLVED,
    _bind_constants,
    _canonical_name,
    _construct_name,
    _safe_static_value,
)

# TaskFlow decorators. ``@dag`` marks a DAG-defining function; ``@task`` (and its variants) mark a
# task-defining function. The bare ``task`` and dotted forms (``task.branch`` / ``task.virtualenv`` /
# ``task.short_circuit`` / ``task.sensor``) all define one task from the decorated callable.
_DAG_DECORATORS: frozenset[str] = frozenset({"dag"})


_TASK_DECORATORS: frozenset[str] = frozenset(
    {"task", "task.branch", "task.virtualenv", "task.short_circuit", "task.sensor", "task.external_python"}
)


_TASK_GROUP_DECORATORS: frozenset[str] = frozenset({"task_group"})


_ALL_AIRFLOW_DECORATORS = _DAG_DECORATORS | _TASK_DECORATORS | _TASK_GROUP_DECORATORS


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


def _decorator_name(node: ast.expr, aliases: dict[str, str] | None = None) -> str:
    """Returns the normalized Airflow decorator name without importing its module."""
    if isinstance(node, ast.Call):
        node = node.func
    canonical = _canonical_name(node, aliases or {})
    for name in sorted(_ALL_AIRFLOW_DECORATORS, key=len, reverse=True):
        if canonical == name or (canonical.startswith("airflow.") and canonical.endswith(f".{name}")):
            return name
    return canonical


def _decorator_kwargs(
    decorators: list[ast.expr],
    names: frozenset[str],
    aliases: dict[str, str] | None = None,
) -> dict[str, ast.expr]:
    """Merged keyword args of the first decorator whose dotted name is in *names* (if it's a call)."""
    for dec in decorators:
        if _decorator_name(dec, aliases) in names and isinstance(dec, ast.Call):
            return {kw.arg: kw.value for kw in dec.keywords if kw.arg}
    return {}


def _has_decorator(
    func: ast.FunctionDef | ast.AsyncFunctionDef,
    names: frozenset[str],
    aliases: dict[str, str] | None = None,
) -> bool:
    return any(_decorator_name(dec, aliases) in names for dec in func.decorator_list)


def _statement_call(statement: ast.stmt) -> ast.Call | None:
    """Returns the top-level call produced by an expression or simple assignment."""
    if isinstance(statement, ast.Expr) and isinstance(statement.value, ast.Call):
        return statement.value
    if isinstance(statement, ast.Assign) and isinstance(statement.value, ast.Call):
        return statement.value
    if isinstance(statement, ast.AnnAssign) and isinstance(statement.value, ast.Call):
        return statement.value
    return None


def _statement_binding(statement: ast.stmt) -> str | None:
    """Returns the single name bound by a top-level statement, when present."""
    if isinstance(statement, ast.Assign) and len(statement.targets) == 1 and isinstance(statement.targets[0], ast.Name):
        return statement.targets[0].id
    if isinstance(statement, ast.AnnAssign) and isinstance(statement.target, ast.Name):
        return statement.target.id
    return None


def _static_function_bindings(
    function: ast.FunctionDef,
    call: ast.Call,
    constants: dict[str, Any],
) -> dict[str, Any] | None:
    """Binds a factory invocation when every argument is statically knowable."""
    if function.args.vararg or function.args.kwarg or any(keyword.arg is None for keyword in call.keywords):
        return None
    positional = [*function.args.posonlyargs, *function.args.args]
    keyword_only = list(function.args.kwonlyargs)
    all_parameters = {parameter.arg for parameter in [*positional, *keyword_only]}
    if len(call.args) > len(positional):
        return None
    expressions: dict[str, ast.expr] = {parameter.arg: argument for parameter, argument in zip(positional, call.args)}
    for keyword in call.keywords:
        if keyword.arg not in all_parameters or keyword.arg in expressions:
            return None
        expressions[keyword.arg] = keyword.value
    positional_defaults = [None] * (len(positional) - len(function.args.defaults)) + list(function.args.defaults)
    defaults = {
        parameter.arg: default for parameter, default in zip(positional, positional_defaults) if default is not None
    }
    defaults.update(
        {
            parameter.arg: default
            for parameter, default in zip(keyword_only, function.args.kw_defaults)
            if default is not None
        }
    )
    bindings: dict[str, Any] = {}
    for parameter in [*positional, *keyword_only]:
        expression = expressions.get(parameter.arg) or defaults.get(parameter.arg)
        if expression is None:
            return None
        bound = _bind_constants(expression, constants)
        value = _safe_static_value(bound, constants) if isinstance(bound, ast.expr) else _UNRESOLVED
        if value is _UNRESOLVED:
            return None
        bindings[parameter.arg] = value
    return bindings
