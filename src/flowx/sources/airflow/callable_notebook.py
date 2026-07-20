"""Render an Airflow PythonOperator callable into a valid, runnable Databricks notebook.

The callable's complete ``def`` is preserved (so early ``return``s stay legal), its
transitive module-level dependencies (helper functions, literal constants, non-Airflow
imports) are carried, and ``op_args`` / ``op_kwargs`` are passed as JSON widgets and
splatted into a call. Airflow/provider imports are dropped (they fail on Databricks);
Variable/connection access is rewritten by :func:`flowx.sources.airflow.templating.rewrite_airflow_calls`.
"""

from __future__ import annotations

import ast

from flowx.sources.airflow import templating

# Import roots that don't exist on Databricks -- never copy these into the notebook.
_AIRFLOW_IMPORT_ROOTS: frozenset[str] = frozenset({"airflow", "cosmos", "airflow_dbt"})


def _module_symbols(module: ast.Module) -> tuple[dict[str, ast.stmt], dict[str, ast.stmt]]:
    """Returns ``(defs, assigns)`` -- module-level function/class defs and simple constant assigns."""
    defs: dict[str, ast.stmt] = {}
    assigns: dict[str, ast.stmt] = {}
    for node in module.body:
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
            defs[node.name] = node
        elif isinstance(node, ast.Assign):
            for target in node.targets:
                if isinstance(target, ast.Name):
                    assigns[target.id] = node
    return defs, assigns


def _import_bindings(module: ast.Module) -> dict[str, tuple[ast.stmt, str]]:
    """Maps each imported name -> (import stmt, root module) for non-Airflow import filtering."""
    bindings: dict[str, tuple[ast.stmt, str]] = {}
    for node in module.body:
        if isinstance(node, ast.Import):
            for alias in node.names:
                bound = (alias.asname or alias.name).split(".")[0]
                root = alias.name.split(".")[0]
                bindings[bound] = (node, root)
        elif isinstance(node, ast.ImportFrom):
            root = (node.module or "").split(".")[0]
            for alias in node.names:
                bindings[alias.asname or alias.name] = (node, root)
    return bindings


def _names_used(node: ast.AST) -> set[str]:
    """Every bare Name id loaded anywhere in *node*."""
    return {n.id for n in ast.walk(node) if isinstance(n, ast.Name) and isinstance(n.ctx, ast.Load)}


def _closure(
    func: ast.FunctionDef, defs: dict[str, ast.stmt], assigns: dict[str, ast.stmt]
) -> tuple[list[str], set[str]]:
    """Returns transitively-referenced module symbols (defs+assigns) in source order, plus all names used.

    Walks the callable, then any helper defs/constants it references, collecting their names
    too (BFS), so a helper that calls another helper is carried.
    """
    ordered: list[str] = []
    seen: set[str] = set()
    all_names: set[str] = set()
    queue = [func.name]
    seen.add(func.name)
    while queue:
        name = queue.pop(0)
        node = defs.get(name) or assigns.get(name)
        if node is None:
            continue
        used = _names_used(node)
        all_names |= used
        if name != func.name:
            ordered.append(name)
        for used_name in used:
            if used_name not in seen and (used_name in defs or used_name in assigns):
                seen.add(used_name)
                queue.append(used_name)
    return ordered, all_names


def render_definitions(func: ast.FunctionDef, source: str, *, note: str) -> str:
    """Renders the callable's ``def`` plus its transitive deps as a notebook prelude (no invocation).

    Carried: an ``import json`` line, the non-Airflow imports the callable/helpers use, the
    referenced module-level helpers/constants, and *func* verbatim. Variable/connection access is
    rewritten. The caller appends its own invocation (a splatted call, a poll loop, ...).
    """
    module = ast.parse(source)
    defs, assigns = _module_symbols(module)
    imports = _import_bindings(module)

    dep_names, used_names = _closure(func, defs, assigns)

    lines: list[str] = ["# Databricks notebook source", f"# Migrated from Airflow {note} '{func.name}'.", ""]

    # 1. Carried non-Airflow imports the callable/helpers actually use.
    import_segments: list[str] = []
    emitted_import_nodes: set[int] = set()
    for name in sorted(used_names):
        binding = imports.get(name)
        if binding is None:
            continue
        stmt, root = binding
        if root in _AIRFLOW_IMPORT_ROOTS or id(stmt) in emitted_import_nodes:
            continue
        emitted_import_nodes.add(id(stmt))
        segment = ast.get_source_segment(source, stmt)
        if segment:
            import_segments.append(segment)
    lines.append("import json")
    lines.extend(sorted(import_segments))
    lines.append("")

    # 2. Carried helper defs / constants, in module source order.
    for name in dep_names:
        node = defs.get(name) or assigns.get(name)
        segment = ast.get_source_segment(source, node) if node is not None else None
        if segment:
            lines.append(segment)
            lines.append("")

    # 3. The callable itself, verbatim (keeps early returns valid).
    func_segment = ast.get_source_segment(source, func) or ""
    lines.append(func_segment)
    lines.append("")

    prelude = "\n".join(lines) + "\n"
    # Rewrite Variable.get / BaseHook.get_connection in the emitted definitions.
    rewritten, _params, _notes = templating.rewrite_airflow_calls(prelude)
    return rewritten


def render(func: ast.FunctionDef, source: str, *, op_args: bool, op_kwargs: bool) -> str:
    """Renders *func* (a PythonOperator callable) as a notebook body.

    Args:
        func: The callable's FunctionDef.
        source: Full DAG module source (for slicing dependency segments).
        op_args / op_kwargs: Whether the operator supplied op_args / op_kwargs (drives the
            JSON-widget call form).

    Returns:
        Notebook source: carried imports + constants + helpers + the ``def`` + a widget-driven call.
    """
    prelude = render_definitions(func, source, note="PythonOperator")

    lines: list[str] = []
    # Widget-driven invocation. op_args/op_kwargs arrive as JSON so lists/dicts survive.
    call_prefix = "result = " if _returns_value(func) else ""
    if op_args:
        lines.append("op_args = json.loads(dbutils.widgets.get('__flowx_op_args'))")
    if op_kwargs:
        lines.append("op_kwargs = json.loads(dbutils.widgets.get('__flowx_op_kwargs'))")
    call_args = ", ".join(filter(None, ["*op_args" if op_args else "", "**op_kwargs" if op_kwargs else ""]))
    lines.append(f"{call_prefix}{func.name}({call_args})")
    if call_prefix:
        lines.append("dbutils.jobs.taskValues.set(key='return_value', value=result)")

    return prelude + "\n".join(lines) + "\n"


def _returns_value(func: ast.FunctionDef) -> bool:
    """True when the callable has a ``return <expr>`` (a value consumed downstream)."""
    for node in ast.walk(func):
        if isinstance(node, ast.Return) and node.value is not None:
            return True
    return False


# Airflow injects execution context (the templated context dict, the task instance ``ti``, XCom)
# into a callable at runtime. flowx runs the callable as a plain notebook with no Airflow runtime,
# so a callable that reads task context or XCom cannot be lowered deterministically.
_TASK_CONTEXT_PARAMS: frozenset[str] = frozenset({"ti", "task_instance"})
_XCOM_METHODS: frozenset[str] = frozenset({"xcom_pull", "xcom_push"})


def task_context_reason(func: ast.FunctionDef) -> str | None:
    """Returns a short reason if *func* depends on Airflow task context / XCom, else None.

    Detects a ``**context`` / ``**kwargs`` catch-all (Airflow passes the whole templated context
    dict there), a ``ti`` / ``task_instance`` parameter, and ``xcom_pull`` / ``xcom_push`` calls.
    These make the callable unrunnable as a plain notebook, so the caller routes it to a placeholder
    for manual/agentic translation rather than emitting code that fails at runtime.
    """
    args = func.args
    if args.kwarg is not None:
        return f"callable takes **{args.kwarg.arg} (Airflow task context)"
    named = {a.arg for a in (args.posonlyargs + args.args + args.kwonlyargs)}
    hit = sorted(named & _TASK_CONTEXT_PARAMS)
    if hit:
        return f"callable takes the '{hit[0]}' task-instance parameter"
    for node in ast.walk(func):
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute) and node.func.attr in _XCOM_METHODS:
            return f"callable calls {node.func.attr}() (XCom)"
    return None
