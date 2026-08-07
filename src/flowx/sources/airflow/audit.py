"""Independent static source audit for Airflow DAG reconciliation."""

from __future__ import annotations

import ast
import hashlib
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any


@dataclass(frozen=True, slots=True, kw_only=True)
class AuditCandidate:
    """One source construct that must be accounted for by capture and translation."""

    kind: str
    code: str
    line: int
    column: int
    occurrence: int
    end_line: int = 0
    end_column: int = 0
    details: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True, kw_only=True)
class SourceAudit:
    """Source-side candidates collected without consulting loader captures or IR."""

    tasks: list[AuditCandidate] = field(default_factory=list)
    edges: list[AuditCandidate] = field(default_factory=list)
    settings: list[AuditCandidate] = field(default_factory=list)
    unresolved: list[AuditCandidate] = field(default_factory=list)


def finding(
    *,
    source_file: str,
    code: str,
    message: str,
    severity: str,
    candidate: AuditCandidate | None = None,
    details: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Builds a stable, serializable reconciliation finding."""
    line = candidate.line if candidate else 0
    column = candidate.column if candidate else 0
    end_line = candidate.end_line if candidate else 0
    end_column = candidate.end_column if candidate else 0
    identity = f"{source_file}:{line}:{column}:{end_line}:{end_column}:{code}"
    return {
        "fingerprint": hashlib.sha256(identity.encode("utf-8")).hexdigest()[:16],
        "code": code,
        "severity": severity,
        "message": message,
        "source_file": source_file,
        "line": line,
        "column": column,
        "end_line": end_line,
        "end_column": end_column,
        "details": {**(candidate.details if candidate else {}), **(details or {})},
    }


def audit_module(module: ast.Module, *, target_dag_variable: str | None = None) -> SourceAudit:
    """Audits one isolated DAG module using a parser independent of the capture visitor."""
    auditor = _SourceAuditor(module, target_dag_variable=target_dag_variable)
    auditor.visit(module)
    return auditor.audit


def source_label(path: Path, root: Path | None = None) -> str:
    """Returns a stable source-relative path for finding fingerprints."""
    if root is not None:
        try:
            return path.resolve().relative_to(root.resolve()).as_posix()
        except ValueError:
            pass
    return path.name


class _SourceAuditor(ast.NodeVisitor):
    """Counts DAG constructs without using loader captures or translated activities."""

    def __init__(self, module: ast.Module, *, target_dag_variable: str | None) -> None:
        self.audit = SourceAudit()
        self.aliases = _aliases(module)
        self.target_dag_variable = target_dag_variable
        self.occurrences: dict[tuple[str, int, int], int] = {}
        self.values: dict[str, Any] = {}
        self.task_refs: dict[str, list[str]] = {}
        self.taskflow_defs = {
            node.name
            for node in ast.walk(module)
            if isinstance(node, ast.FunctionDef) and _decorator_leaf(node) in _TASK_DECORATORS
        }
        self.dag_defs = {
            node.name for node in module.body if isinstance(node, ast.FunctionDef) and _decorator_leaf(node) == "dag"
        }
        self.factories = {
            node.name
            for node in module.body
            if isinstance(node, ast.FunctionDef) and _single_operator_return(node, self.aliases)
        }

    def _candidate(self, kind: str, code: str, node: ast.AST, **details: Any) -> AuditCandidate:
        key = (kind, getattr(node, "lineno", 0), getattr(node, "col_offset", 0))
        occurrence = self.occurrences.get(key, 0) + 1
        self.occurrences[key] = occurrence
        return AuditCandidate(
            kind=kind,
            code=code,
            line=key[1],
            column=key[2],
            occurrence=occurrence,
            end_line=getattr(node, "end_lineno", key[1]),
            end_column=getattr(node, "end_col_offset", key[2]),
            details=details,
        )

    def visit_FunctionDef(self, node: ast.FunctionDef) -> None:
        if node.name in self.dag_defs:
            decorator = next((item for item in node.decorator_list if _leaf(item, self.aliases) == "dag"), None)
            if isinstance(decorator, ast.Call):
                self._audit_settings(decorator)
            for statement in node.body:
                if not isinstance(statement, ast.FunctionDef):
                    self.visit(statement)

    def visit_With(self, node: ast.With) -> None:
        for item in node.items:
            if isinstance(item.context_expr, ast.Call) and _leaf(item.context_expr.func, self.aliases) == "DAG":
                self._audit_settings(item.context_expr)
            elif isinstance(item.context_expr, ast.Call):
                self._audit_task_call(item.context_expr)
        for statement in node.body:
            self.visit(statement)

    def visit_Assign(self, node: ast.Assign) -> None:
        target = node.targets[0].id if len(node.targets) == 1 and isinstance(node.targets[0], ast.Name) else None
        if isinstance(node.value, ast.Call):
            if _leaf(node.value.func, self.aliases) == "DAG":
                self._audit_settings(node.value)
                return
            if self._audit_task_call(node.value):
                if target:
                    self.values[target] = True
                    self.task_refs[target] = [target]
                return
        if target:
            if isinstance(node.value, ast.Name) and node.value.id in self.values:
                self.values[target] = self.values[node.value.id]
                if node.value.id in self.task_refs:
                    self.task_refs[target] = list(self.task_refs[node.value.id])
                return
            try:
                self.values[target] = ast.literal_eval(node.value)
                if isinstance(self.values[target], list):
                    self.task_refs[target] = []
                return
            except (ValueError, SyntaxError):
                self.values.pop(target, None)
                self.task_refs.pop(target, None)
        self.generic_visit(node)

    def visit_Expr(self, node: ast.Expr) -> None:
        value = node.value
        if isinstance(value, ast.BinOp) and isinstance(value.op, (ast.RShift, ast.LShift)):
            self._audit_shift(value)
            return
        if isinstance(value, ast.Call):
            name = _leaf(value.func, self.aliases)
            if name == "chain":
                positions = [self._audit_position(argument) for argument in value.args]
                for left, right in zip(positions, positions[1:]):
                    self._add_edges(value, left, right, "chain")
                return
            if name == "cross_downstream" and len(value.args) >= 2:
                left = self._audit_position(value.args[0])
                right = self._audit_position(value.args[1])
                self._add_edges(value, left, right, "cross_downstream")
                return
            if isinstance(value.func, ast.Attribute) and value.func.attr == "append" and value.args:
                if isinstance(value.args[0], ast.Call):
                    if self._audit_task_call(value.args[0]) and isinstance(value.func.value, ast.Name):
                        self.task_refs.setdefault(value.func.value.id, []).append(self._call_reference(value.args[0]))
                return
            if self._audit_task_call(value):
                return
            if isinstance(value.func, ast.Attribute) and value.func.attr in ("set_upstream", "set_downstream"):
                owner = self._audit_position(value.func.value)
                other = self._audit_position(value.args[0]) if value.args else []
                upstreams, downstreams = (owner, other) if value.func.attr == "set_downstream" else (other, owner)
                self._add_edges(value, upstreams, downstreams, value.func.attr)

    def visit_For(self, node: ast.For) -> None:
        cardinality = _literal_cardinality(node.iter)
        if cardinality is None or cardinality > 256:
            self.audit.unresolved.append(
                self._candidate("unresolved", "dynamic_loop", node, expression=ast.unparse(node.iter))
            )
            return
        iteration_nodes = list(node.iter.elts) if isinstance(node.iter, (ast.List, ast.Tuple)) else []
        for index in range(cardinality):
            if isinstance(node.target, ast.Name):
                self.values[node.target.id] = True
                if index < len(iteration_nodes):
                    references = self._audit_position(iteration_nodes[index])
                    if references:
                        self.task_refs[node.target.id] = references
                    else:
                        self.task_refs.pop(node.target.id, None)
            for statement in node.body:
                self.visit(statement)
        for statement in node.orelse:
            self.visit(statement)

    def visit_If(self, node: ast.If) -> None:
        if isinstance(node.test, ast.Name) and node.test.id in self.values:
            value = self.values[node.test.id]
        else:
            try:
                value = ast.literal_eval(node.test)
            except (ValueError, SyntaxError):
                self.audit.unresolved.append(
                    self._candidate("unresolved", "ambiguous_condition", node, expression=ast.unparse(node.test))
                )
                return
        for statement in node.body if bool(value) else node.orelse:
            self.visit(statement)

    def _audit_task_call(self, call: ast.Call) -> bool:
        operator, keywords, mapped = _operator_call(call, self.aliases)
        if operator:
            dag = keywords.get("dag")
            if self.target_dag_variable is not None and not (
                isinstance(dag, ast.Name) and dag.id == self.target_dag_variable
            ):
                return False
            task_id = _literal_string(keywords.get("task_id")) or _literal_string(keywords.get("group_id"))
            self.audit.tasks.append(
                self._candidate(
                    "task",
                    "operator_task",
                    call,
                    operator=operator,
                    task_id=task_id,
                    kwargs=sorted(keywords),
                    mapped=mapped,
                )
            )
            if call.args or any(keyword.arg is None for keyword in call.keywords):
                self.audit.unresolved.append(
                    self._candidate(
                        "unresolved",
                        "dynamic_operator_arguments",
                        call,
                        expression=ast.unparse(call),
                    )
                )
            return True
        base = _base_call_name(call)
        if base in self.factories:
            self.audit.tasks.append(
                self._candidate("task", "helper_factory_task", call, helper=base, kwargs=_call_argument_names(call))
            )
            return True
        if base in self.taskflow_defs:
            for argument in [*call.args, *(keyword.value for keyword in call.keywords)]:
                dependency = isinstance(argument, ast.Name) and self.values.get(argument.id) is True
                if isinstance(argument, ast.Call):
                    dependency = self._audit_task_call(argument)
                if dependency:
                    upstreams = (
                        [self._call_reference(argument)]
                        if isinstance(argument, ast.Call)
                        else self._audit_position(argument)
                    )
                    self._add_edges(
                        argument,
                        upstreams,
                        [self._call_reference(call)],
                        "taskflow_data",
                    )
            self.audit.tasks.append(
                self._candidate("task", "taskflow_task", call, callable=base, kwargs=_call_argument_names(call))
            )
            return True
        return False

    def _audit_position(self, node: ast.expr) -> list[str]:
        if isinstance(node, ast.Call):
            return [self._call_reference(node)] if self._audit_task_call(node) else []
        elif isinstance(node, (ast.List, ast.Tuple)):
            return [reference for item in node.elts for reference in self._audit_position(item)]
        if isinstance(node, ast.Name):
            if node.id in self.task_refs:
                return list(self.task_refs[node.id])
            return [] if self.target_dag_variable is not None else [node.id]
        return []

    def _audit_shift(self, node: ast.expr) -> list[str]:
        if not isinstance(node, ast.BinOp) or not isinstance(node.op, (ast.RShift, ast.LShift)):
            return self._audit_position(node)
        left = self._audit_shift(node.left)
        right = self._audit_shift(node.right)
        upstreams, downstreams = (left, right) if isinstance(node.op, ast.RShift) else (right, left)
        self._add_edges(node, upstreams, downstreams, "shift")
        return right

    def _call_reference(self, call: ast.Call) -> str:
        operator, keywords, _mapped = _operator_call(call, self.aliases)
        if operator:
            return (
                _literal_string(keywords.get("task_id"))
                or _literal_string(keywords.get("group_id"))
                or (f"call@{getattr(call, 'lineno', 0)}")
            )
        return _base_call_name(call) or f"call@{getattr(call, 'lineno', 0)}"

    def _add_edges(self, node: ast.AST, upstreams: list[str], downstreams: list[str], syntax: str) -> None:
        for upstream in upstreams:
            for downstream in downstreams:
                self.audit.edges.append(
                    self._candidate(
                        "edge",
                        "dependency_edge",
                        node,
                        syntax=syntax,
                        upstream=upstream,
                        downstream=downstream,
                    )
                )

    def _audit_settings(self, call: ast.Call) -> None:
        for keyword in call.keywords:
            if keyword.arg:
                self.audit.settings.append(self._candidate("setting", "dag_setting", keyword.value, name=keyword.arg))
                if keyword.arg == "default_args" and isinstance(keyword.value, ast.Dict):
                    for key, value in zip(keyword.value.keys, keyword.value.values):
                        if isinstance(key, ast.Constant) and isinstance(key.value, str):
                            self.audit.settings.append(
                                self._candidate(
                                    "setting",
                                    "dag_default_arg",
                                    value,
                                    name=f"default_args.{key.value}",
                                )
                            )


_TASK_DECORATORS = {"task", "branch", "virtualenv", "short_circuit", "sensor", "external_python"}


def _aliases(module: ast.Module) -> dict[str, str]:
    aliases: dict[str, str] = {}
    for statement in module.body:
        if isinstance(statement, ast.Import):
            for item in statement.names:
                aliases[item.asname or item.name.split(".")[0]] = item.name
        elif isinstance(statement, ast.ImportFrom) and statement.module:
            for item in statement.names:
                aliases[item.asname or item.name] = f"{statement.module}.{item.name}"
    return aliases


def _dotted(node: ast.expr, aliases: dict[str, str]) -> str:
    if isinstance(node, ast.Call):
        node = node.func
    parts: list[str] = []
    while isinstance(node, ast.Attribute):
        parts.append(node.attr)
        node = node.value
    if not isinstance(node, ast.Name):
        return ""
    return ".".join([aliases.get(node.id, node.id), *reversed(parts)])


def _leaf(node: ast.expr, aliases: dict[str, str]) -> str:
    return _dotted(node, aliases).rsplit(".", 1)[-1]


def _decorator_leaf(node: ast.FunctionDef) -> str:
    return _leaf(node.decorator_list[0], {}) if node.decorator_list else ""


def _is_operator(name: str) -> bool:
    return name.endswith(("Operator", "Sensor")) or name in {"DbtDag", "DbtTaskGroup"}


def _operator_call(call: ast.Call, aliases: dict[str, str]) -> tuple[str, dict[str, ast.expr], bool]:
    direct = _leaf(call.func, aliases)
    if _is_operator(direct):
        return direct, {keyword.arg: keyword.value for keyword in call.keywords if keyword.arg}, False
    if not (isinstance(call.func, ast.Attribute) and call.func.attr in ("expand", "expand_kwargs")):
        return "", {}, False
    inner = call.func.value
    if not isinstance(inner, ast.Call):
        return "", {}, False
    operator = _leaf(inner.func, aliases)
    if operator == "partial" and isinstance(inner.func, ast.Attribute):
        operator = _leaf(inner.func.value, aliases)
    if not _is_operator(operator):
        return "", {}, False
    keywords = {keyword.arg: keyword.value for keyword in [*inner.keywords, *call.keywords] if keyword.arg}
    return operator, keywords, True


def _single_operator_return(function: ast.FunctionDef, aliases: dict[str, str]) -> bool:
    if function.decorator_list or function.args.vararg or function.args.kwarg:
        return False
    body = list(function.body)
    if body and isinstance(body[0], ast.Expr) and isinstance(body[0].value, ast.Constant):
        if isinstance(body[0].value.value, str):
            body = body[1:]
    return (
        len(body) == 1
        and isinstance(body[0], ast.Return)
        and isinstance(body[0].value, ast.Call)
        and bool(_operator_call(body[0].value, aliases)[0])
    )


def _base_call_name(call: ast.Call) -> str:
    node: ast.expr = call.func
    while isinstance(node, ast.Attribute):
        node = node.value
        if isinstance(node, ast.Call):
            node = node.func
    return node.id if isinstance(node, ast.Name) else ""


def _call_argument_names(call: ast.Call) -> list[str]:
    names = [f"arg{index}" for index, _argument in enumerate(call.args)]
    names.extend(keyword.arg or "**kwargs" for keyword in call.keywords)
    return names


def _literal_string(node: ast.expr | None) -> str | None:
    return node.value if isinstance(node, ast.Constant) and isinstance(node.value, str) else None


def _literal_cardinality(node: ast.expr) -> int | None:
    if isinstance(node, (ast.List, ast.Tuple, ast.Set)):
        return len(node.elts)
    if isinstance(node, ast.Dict):
        return len(node.keys)
    if isinstance(node, ast.Call) and _leaf(node.func, {}) == "range":
        try:
            values = [ast.literal_eval(argument) for argument in node.args]
            return len(range(*values))
        except (TypeError, ValueError, SyntaxError):
            return None
    return None
