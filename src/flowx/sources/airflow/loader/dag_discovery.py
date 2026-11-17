"""Static discovery of DAG declarations and factory invocations in a module."""

from __future__ import annotations

import ast
import copy
from pathlib import Path
from typing import Any

from flowx.models.ir import (
    Pipeline,
)
from flowx.sources.airflow import audit as source_audit
from flowx.sources.airflow import operators as ops
from flowx.sources.airflow.loader.ast_utils import (
    _UNRESOLVED,
    _bind_constants,
    _construct_name,
    _import_aliases,
    _safe_static_value,
    _span,
)
from flowx.sources.airflow.loader.captures import DagDeclaration
from flowx.sources.airflow.loader.dispatch import (
    _DAG_DECORATORS,
    _decorator_name,
    _has_decorator,
    _statement_binding,
    _statement_call,
    _static_function_bindings,
)


def _classic_dag_factory_body(
    function: ast.FunctionDef,
    aliases: dict[str, str],
) -> tuple[list[ast.stmt], str | None] | None:
    """Returns the narrow classic DAG-factory body and any assigned DAG variable."""
    if function.decorator_list or function.args.vararg or function.args.kwarg:
        return None
    body = list(function.body)
    if body and isinstance(body[0], ast.Expr) and isinstance(body[0].value, ast.Constant):
        if isinstance(body[0].value.value, str):
            body = body[1:]
    if len(body) < 2 or not isinstance(body[-1], ast.Return) or not isinstance(body[-1].value, ast.Name):
        return None
    returned_name = body[-1].value.id
    statements = body[:-1]
    if len(statements) == 1 and isinstance(statements[0], ast.With):
        dag_items = [
            item
            for item in statements[0].items
            if isinstance(item.context_expr, ast.Call) and _construct_name(item.context_expr.func, aliases) == "DAG"
        ]
        if len(dag_items) != 1 or not isinstance(dag_items[0].optional_vars, ast.Name):
            return None
        if dag_items[0].optional_vars.id != returned_name:
            return None
        return statements, None
    assigned_names: list[str] = []
    for statement in statements:
        if not (
            isinstance(statement, ast.Assign)
            and len(statement.targets) == 1
            and isinstance(statement.targets[0], ast.Name)
            and isinstance(statement.value, ast.Call)
            and _construct_name(statement.value.func, aliases) == "DAG"
        ):
            continue
        assigned_names.append(statement.targets[0].id)
    if assigned_names != [returned_name]:
        return None
    return statements, returned_name


def _decorated_factory_invocation(
    call: ast.Call,
    factories: dict[str, ast.FunctionDef],
) -> tuple[ast.FunctionDef, ast.Call, dict[str, ast.expr]] | None:
    """Resolves ``factory()`` and ``factory.override(...)(...)`` DAG invocations."""
    if isinstance(call.func, ast.Name) and call.func.id in factories:
        return factories[call.func.id], call, {}
    if not isinstance(call.func, ast.Call):
        return None
    configuration = call.func
    configuration_function = configuration.func
    if not isinstance(configuration_function, ast.Attribute):
        return None
    if configuration_function.attr != "override" or not isinstance(configuration_function.value, ast.Name):
        return None
    name = configuration_function.value.id
    if name not in factories or any(keyword.arg is None for keyword in configuration.keywords):
        return None
    overrides = {keyword.arg: keyword.value for keyword in configuration.keywords if keyword.arg}
    return factories[name], call, overrides


def _function_contains_dag_constructor(function: ast.FunctionDef, aliases: dict[str, str]) -> bool:
    """Returns whether a function body contains a static Airflow ``DAG(...)`` call."""
    return any(
        isinstance(node, ast.Call) and _construct_name(node.func, aliases) == "DAG"
        for statement in function.body
        for node in ast.walk(statement)
    )


def _bound_decorated_factory(
    declaration: DagDeclaration,
    aliases: dict[str, str],
) -> ast.FunctionDef:
    """Clones one decorated factory invocation into an isolated static DAG definition."""
    if declaration.factory is None:
        raise ValueError("Decorated factory declaration has no function definition")
    function = copy.deepcopy(declaration.factory)
    function.body = [_bind_constants(statement, declaration.bindings) for statement in function.body]
    for index, decorator in enumerate(function.decorator_list):
        if _decorator_name(decorator, aliases) != "dag":
            continue
        if isinstance(decorator, ast.Call):
            keywords = {keyword.arg: keyword for keyword in decorator.keywords if keyword.arg}
            for name, value in declaration.decorator_overrides.items():
                keywords[name] = ast.keyword(arg=name, value=copy.deepcopy(value))
            decorator.keywords = list(keywords.values())
        elif declaration.decorator_overrides:
            function.decorator_list[index] = ast.copy_location(
                ast.Call(
                    func=decorator,
                    args=[],
                    keywords=[
                        ast.keyword(arg=name, value=copy.deepcopy(value))
                        for name, value in declaration.decorator_overrides.items()
                    ],
                ),
                decorator,
            )
        break
    ast.fix_missing_locations(function)
    return function


def _top_level_dag_declarations(module: ast.Module) -> list[DagDeclaration]:
    """Returns direct DAG declarations and statically invoked DAG factories."""
    aliases = _import_aliases(module)
    functions = {node.name: node for node in module.body if isinstance(node, ast.FunctionDef)}
    decorated_factories = {
        name: function for name, function in functions.items() if _has_decorator(function, _DAG_DECORATORS, aliases)
    }
    classic_factories = {
        name: function
        for name, function in functions.items()
        if name not in decorated_factories and _function_contains_dag_constructor(function, aliases)
    }
    declarations: list[DagDeclaration] = []
    constants: dict[str, Any] = {}

    def add(
        node: ast.stmt,
        *,
        variable: str | None = None,
        kind: str = "direct",
        factory: ast.FunctionDef | None = None,
        bindings: dict[str, Any] | None = None,
        target_dag_variable: str | None = None,
        decorator_overrides: dict[str, ast.expr] | None = None,
        unsupported_reason: str | None = None,
    ) -> None:
        span = _span(node)
        declarations.append(
            DagDeclaration(
                capture_id=f"dag:{span.line}:{span.column}:{len(declarations) + 1}",
                variable=variable,
                node=node,
                span=span,
                kind=kind,
                factory=factory,
                bindings=bindings or {},
                target_dag_variable=target_dag_variable,
                decorator_overrides=decorator_overrides or {},
                unsupported_reason=unsupported_reason,
            )
        )

    for node in module.body:
        if isinstance(node, ast.With) and any(
            isinstance(item.context_expr, ast.Call) and _construct_name(item.context_expr.func, aliases) == "DAG"
            for item in node.items
        ):
            add(node)
            continue
        elif (
            isinstance(node, ast.Assign)
            and len(node.targets) == 1
            and isinstance(node.targets[0], ast.Name)
            and isinstance(node.value, ast.Call)
            and _construct_name(node.value.func, aliases) == "DAG"
        ):
            variable = node.targets[0].id
            add(node, variable=variable, target_dag_variable=variable)
            continue

        call = _statement_call(node)
        binding = _statement_binding(node)
        if call is not None:
            decorated = _decorated_factory_invocation(call, decorated_factories)
            if decorated is not None:
                factory, invocation, overrides = decorated
                bindings = _static_function_bindings(factory, invocation, constants)
                bound_overrides = {name: _bind_constants(value, constants) for name, value in overrides.items()}
                dag_id_override = bound_overrides.get("dag_id")
                reason = None
                if bindings is None:
                    reason = "Decorated DAG factory arguments are not statically bindable."
                elif dag_id_override is not None and ops.literal_str(dag_id_override) is None:
                    reason = "Decorated DAG factory dag_id override is not a literal string."
                add(
                    node,
                    variable=binding,
                    kind="decorated_factory",
                    factory=factory,
                    bindings=bindings,
                    decorator_overrides=bound_overrides,
                    unsupported_reason=reason,
                )
                continue

            if isinstance(call.func, ast.Name) and call.func.id in classic_factories:
                factory = classic_factories[call.func.id]
                factory_body = _classic_dag_factory_body(factory, aliases)
                bindings = _static_function_bindings(factory, call, constants)
                reason = None
                target_dag_variable = None
                if factory_body is None:
                    reason = "Classic DAG factory body is outside the supported static shape."
                elif bindings is None:
                    reason = "Classic DAG factory arguments are not statically bindable."
                else:
                    _statements, target_dag_variable = factory_body
                add(
                    node,
                    variable=binding,
                    kind="classic_factory",
                    factory=factory,
                    bindings=bindings,
                    target_dag_variable=target_dag_variable,
                    unsupported_reason=reason,
                )
                continue

        if not isinstance(node, ast.FunctionDef) and any(
            isinstance(candidate, ast.Call) and _construct_name(candidate.func, aliases) == "DAG"
            for candidate in ast.walk(node)
        ):
            add(
                node,
                variable=binding,
                kind="unsupported",
                unsupported_reason="DAG construction is outside the supported static declaration shapes.",
            )
            continue

        if isinstance(node, ast.Assign) and len(node.targets) == 1 and isinstance(node.targets[0], ast.Name):
            value = _safe_static_value(node.value, constants)
            if value is not _UNRESOLVED:
                constants[node.targets[0].id] = value
    return declarations


def _module_for_dag(
    module: ast.Module,
    declaration: DagDeclaration,
    declarations: list[DagDeclaration],
) -> ast.Module:
    """Returns a module containing shared definitions and one DAG declaration."""
    aliases = _import_aliases(module)
    declaration_nodes = {item.node for item in declarations}
    decorated_factories = {
        item.factory for item in declarations if item.kind == "decorated_factory" and item.factory is not None
    }
    body: list[ast.stmt] = []
    for node in module.body:
        if node in decorated_factories:
            if declaration.kind == "decorated_factory" and node is declaration.factory:
                body.append(_bound_decorated_factory(declaration, aliases))
            continue
        if node in declaration_nodes:
            if node is not declaration.node:
                continue
            if declaration.kind == "direct":
                body.append(node)
            elif declaration.kind == "classic_factory" and declaration.factory is not None:
                factory_body = _classic_dag_factory_body(declaration.factory, aliases)
                if factory_body is None:
                    raise ValueError("Supported classic DAG factory has no static body")
                statements, _target = factory_body
                body.extend(_bind_constants(statement, declaration.bindings) for statement in statements)
            continue
        body.append(node)
    isolated = ast.Module(body=body, type_ignores=list(module.type_ignores))
    ast.fix_missing_locations(isolated)
    return isolated


def _failed_dag_declaration_pipeline(
    dag_path: Path,
    declaration: DagDeclaration,
    *,
    source_file: str,
) -> Pipeline:
    """Returns a failed, reportable pipeline for an unrepresentable DAG declaration."""
    candidate = source_audit.AuditCandidate(
        kind="dag",
        code="unsupported_dag_factory",
        line=declaration.span.line,
        column=declaration.span.column,
        occurrence=1,
        end_line=declaration.span.end_line,
        end_column=declaration.span.end_column,
        details={"expression": ast.unparse(declaration.node)},
    )
    finding = source_audit.finding(
        source_file=source_file,
        code="unsupported_dag_factory",
        severity="failed",
        message=declaration.unsupported_reason or "Airflow DAG declaration could not be captured statically.",
        candidate=candidate,
    )
    name = declaration.variable or Path(dag_path).stem
    return Pipeline(
        name=name,
        tasks=[],
        tags={"source": "airflow", "dag_id": name},
        not_translatable=[finding],
        reconciliation_status="failed",
        audit={
            "source_file": source_file,
            "audited_activity_count": 1,
            "captured_task_count": 0,
            "audited_edge_count": 0,
            "captured_edge_count": 0,
            "deterministic_count": 0,
            "agentic_count": 0,
            "failed_count": 1,
            "excluded_count": 0,
            "transformations": [],
        },
    )
