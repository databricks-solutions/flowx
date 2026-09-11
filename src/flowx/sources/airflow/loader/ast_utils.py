"""Shared AST helpers: name resolution, static literal evaluation, loop unrolling."""

from __future__ import annotations

import ast
import copy
import re
from typing import Any

from flowx.sources.airflow import operators as ops
from flowx.sources.airflow.loader.captures import SourceSpan

_UNRESOLVED = object()


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


def _airflow_generation(module: ast.Module) -> str:
    """Infers version-specific authoring syntax only when imports are unambiguous."""
    imported_modules: list[str] = []
    for statement in module.body:
        if isinstance(statement, ast.Import):
            imported_modules.extend(item.name for item in statement.names)
        elif isinstance(statement, ast.ImportFrom) and statement.module:
            imported_modules.append(statement.module)
    if any(name == "airflow.sdk" or name.startswith("airflow.sdk.") for name in imported_modules) or any(
        name == "airflow.providers.standard" or name.startswith("airflow.providers.standard.")
        for name in imported_modules
    ):
        return "3"
    legacy_module = re.compile(r"^airflow\.(?:operators|sensors)\.[^.]+_(?:operator|sensor)$")
    if any(name.startswith("airflow.contrib.") or legacy_module.fullmatch(name) for name in imported_modules):
        return "1.10"
    return "unknown"


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
        if not isinstance(node.ctx, ast.Load):
            return node
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


def _iter_functions(module: ast.Module) -> list[ast.FunctionDef | ast.AsyncFunctionDef]:
    """All function definitions, including those nested inside a ``@dag`` function body.

    TaskFlow ``@task`` defs are often nested inside the ``@dag`` function, so a top-level-only scan
    would miss them. Async definitions are captured so they can become explicit agentic leaf gaps.
    """
    found: list[ast.FunctionDef | ast.AsyncFunctionDef] = []
    for node in ast.walk(module):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
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
