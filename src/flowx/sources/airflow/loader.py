"""Airflow DAG parser: parse a DAG file into a flowx Pipeline IR.

Core parser for the Airflow source (``flowx.sources.airflow``).  It reads a DAG
module statically with :mod:`ast` (no Airflow install or DAG execution) and
produces the same :class:`~flowx.models.ir.Pipeline` IR the ADF path emits, so
the shared downstream half -- ``prepare_workflow`` -> ``write_bundle`` -> DABs --
is reused unchanged.  The ``discover`` and ``convert`` phase entry points in
this package wrap :func:`load_airflow_dag`.

Coverage: PythonOperator (callable body -> generated notebook), BashOperator
(command -> generated notebook), ``>>`` / ``<<`` dependencies, and a cron
``schedule_interval`` -> Quartz. Operators without a mapping become
PlaceholderActivity so coverage still counts them.
"""

from __future__ import annotations

import ast
import textwrap
from pathlib import Path

from flowx.models.ir import (
    Activity,
    Dependency,
    NotebookActivity,
    Pipeline,
    PlaceholderActivity,
)


def _sanitize_task_key(name: str) -> str:
    """Converts an Airflow task_id into a valid Databricks task key."""
    import re

    key = re.sub(r"[^a-zA-Z0-9_-]", "_", name)
    key = re.sub(r"_+", "_", key).strip("_")
    return key or "unnamed"


def _cron_to_quartz(cron: str) -> str | None:
    """Converts a 5-field Unix cron to a 6-field Quartz expression.

    Quartz is ``second minute hour day-of-month month day-of-week``; Unix cron
    is ``minute hour day-of-month month day-of-week``. Prepend the seconds
    field and reconcile the day-of-month / day-of-week wildcard (Quartz rejects
    ``*`` in both simultaneously -- one must be ``?``).
    """
    fields = cron.split()
    if len(fields) != 5:
        return None
    minute, hour, dom, month, dow = fields
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


def _schedule_from_interval(interval: str | None) -> dict[str, object] | None:
    """Builds a Pipeline.schedule spec from an Airflow schedule_interval."""
    if not interval:
        return None
    quartz: str | None = _CRON_PRESETS.get(interval) or _cron_to_quartz(interval)
    if quartz is None:
        return None
    return {
        "kind": "schedule",
        "quartz_cron_expression": quartz,
        "timezone_id": "UTC",
        "pause_status": "UNPAUSED",
    }


def _notebook_source_from_callable(func: ast.FunctionDef, source: str) -> str:
    """Renders a PythonOperator callable's body as a Databricks notebook.

    Slices each body statement's source segment out of the module *source* and
    dedents so the extracted block is valid top-level notebook code.
    """
    segments = [ast.get_source_segment(source, stmt) for stmt in func.body]
    body_src = "\n\n".join(seg for seg in segments if seg)
    body_src = textwrap.dedent(body_src)
    return f"# Databricks notebook source\n# Migrated from Airflow PythonOperator '{func.name}'.\n\n" + body_src + "\n"


def _notebook_source_from_bash(task_id: str, command: str) -> str:
    """Renders a BashOperator command as a Databricks notebook shell cell."""
    return (
        "# Databricks notebook source\n"
        f"# Migrated from Airflow BashOperator '{task_id}'.\n\n"
        "# MAGIC %sh\n" + "".join(f"# MAGIC {line}\n" for line in command.splitlines())
    )


class _DagVisitor(ast.NodeVisitor):
    """Collects operator calls, dependency edges, and the DAG's schedule."""

    def __init__(self, module: ast.Module) -> None:
        self._functions: dict[str, ast.FunctionDef] = {
            node.name: node for node in module.body if isinstance(node, ast.FunctionDef)
        }
        # task variable name -> (task_id, operator, kwargs)
        self.operators: dict[str, tuple[str, str, dict[str, ast.expr]]] = {}
        self.edges: list[tuple[str, str]] = []  # (upstream_var, downstream_var)
        self.dag_id: str | None = None
        self.schedule_interval: str | None = None

    def functions(self) -> dict[str, ast.FunctionDef]:
        return self._functions

    def visit_Assign(self, node: ast.Assign) -> None:
        if (
            isinstance(node.value, ast.Call)
            and isinstance(node.value.func, ast.Name)
            and node.value.func.id.endswith("Operator")
            and len(node.targets) == 1
            and isinstance(node.targets[0], ast.Name)
        ):
            var = node.targets[0].id
            kwargs = {kw.arg: kw.value for kw in node.value.keywords if kw.arg}
            task_id = _literal_str(kwargs.get("task_id")) or var
            self.operators[var] = (task_id, node.value.func.id, kwargs)
        self.generic_visit(node)

    def visit_With(self, node: ast.With) -> None:
        for item in node.items:
            call = item.context_expr
            if isinstance(call, ast.Call) and isinstance(call.func, ast.Name) and call.func.id == "DAG":
                self._read_dag_kwargs(call)
        self.generic_visit(node)

    def _read_dag_kwargs(self, call: ast.Call) -> None:
        kwargs = {kw.arg: kw.value for kw in call.keywords if kw.arg}
        self.dag_id = _literal_str(kwargs.get("dag_id"))
        self.schedule_interval = _literal_str(kwargs.get("schedule_interval")) or _literal_str(kwargs.get("schedule"))

    def visit_Expr(self, node: ast.Expr) -> None:
        # Capture `a >> b >> c` and `a << b` dependency chains.
        if isinstance(node.value, ast.BinOp) and isinstance(node.value.op, (ast.RShift, ast.LShift)):
            self._collect_shift_chain(node.value)
        self.generic_visit(node)

    def _collect_shift_chain(self, binop: ast.BinOp) -> None:
        names = _flatten_shift(binop)
        if not names:
            return
        pairs = zip(names, names[1:])
        for left, right in pairs:
            if isinstance(binop.op, ast.RShift):
                self.edges.append((left, right))
            else:
                self.edges.append((right, left))


def _flatten_shift(node: ast.expr) -> list[str]:
    """Flattens a chain of ``>>`` / ``<<`` Name nodes into an ordered list."""
    if isinstance(node, ast.Name):
        return [node.id]
    if isinstance(node, ast.BinOp) and isinstance(node.op, (ast.RShift, ast.LShift)):
        return _flatten_shift(node.left) + _flatten_shift(node.right)
    return []


def _literal_str(node: ast.expr | None) -> str | None:
    """Returns the string value of a constant AST node, else None."""
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        return node.value
    return None


def load_airflow_dag(dag_path: Path) -> Pipeline:
    """Parses an Airflow DAG file into a flowx Pipeline IR.

    Args:
        dag_path: Path to a ``.py`` DAG module.

    Returns:
        A :class:`~flowx.models.ir.Pipeline` whose tasks are NotebookActivity
        (for mapped operators) or PlaceholderActivity (for unmapped ones).
    """
    source = Path(dag_path).read_text(encoding="utf-8")
    module = ast.parse(source)
    visitor = _DagVisitor(module)
    visitor.visit(module)
    functions = visitor.functions()

    # var -> task_key, and per-var dependency edges resolved to task_keys.
    var_to_task_key = {var: _sanitize_task_key(task_id) for var, (task_id, _, _) in visitor.operators.items()}
    upstreams: dict[str, list[str]] = {var: [] for var in visitor.operators}
    for upstream_var, downstream_var in visitor.edges:
        if downstream_var in upstreams and upstream_var in var_to_task_key:
            upstreams[downstream_var].append(upstream_var)

    tasks: list[Activity] = []
    for var, (task_id, operator, kwargs) in visitor.operators.items():
        task_key = var_to_task_key[var]
        depends_on = [Dependency(task_key=var_to_task_key[u]) for u in upstreams[var]] or None
        tasks.append(_build_activity(task_id, task_key, operator, kwargs, functions, depends_on, source))

    return Pipeline(
        name=visitor.dag_id or Path(dag_path).stem,
        tasks=tasks,
        schedule=_schedule_from_interval(visitor.schedule_interval),
        tags={"source": "airflow", "dag_id": visitor.dag_id or ""},
    )


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


def load_pipelines(source_path: Path, pipeline: str | None = None) -> list[Pipeline]:
    """Loads every DAG under *source_path* into Pipeline IR.

    Args:
        source_path: A DAG ``.py`` file or a directory of them.
        pipeline: When set, keep only the pipeline whose name (dag_id) matches.

    Returns:
        One :class:`~flowx.models.ir.Pipeline` per discovered DAG, filtered to
        *pipeline* when provided.
    """
    pipelines = [load_airflow_dag(dag_path) for dag_path in discover_dags(source_path)]
    if pipeline is not None:
        pipelines = [p for p in pipelines if p.name == pipeline]
    return pipelines


def _build_activity(
    task_id: str,
    task_key: str,
    operator: str,
    kwargs: dict[str, ast.expr],
    functions: dict[str, ast.FunctionDef],
    depends_on: list[Dependency] | None,
    source: str,
) -> Activity:
    """Maps one Airflow operator to an Activity IR node."""
    if operator == "PythonOperator":
        callable_node = kwargs.get("python_callable")
        func = functions.get(callable_node.id) if isinstance(callable_node, ast.Name) else None
        if func is not None:
            return NotebookActivity(
                name=task_id,
                task_key=task_key,
                depends_on=depends_on,
                notebook_path=f"notebooks/{task_key}.py",
                generated_source=_notebook_source_from_callable(func, source),
            )
    if operator == "BashOperator":
        command = _literal_str(kwargs.get("bash_command"))
        if command is not None:
            return NotebookActivity(
                name=task_id,
                task_key=task_key,
                depends_on=depends_on,
                notebook_path=f"notebooks/{task_key}.py",
                generated_source=_notebook_source_from_bash(task_id, command),
            )
    return PlaceholderActivity(
        name=task_id,
        task_key=task_key,
        depends_on=depends_on,
        original_type=operator,
        comment=f"Airflow operator '{operator}' has no deterministic flowx mapping yet.",
    )
