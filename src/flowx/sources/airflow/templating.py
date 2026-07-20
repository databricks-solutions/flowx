"""Airflow Jinja templating, default_args, and trigger_rule -> flowx IR helpers.

Airflow DAGs template values with Jinja (`{{ ds }}`, `{{ params.x }}`, macros) and
carry cross-cutting task settings in `default_args` (retries, timeouts, email) and
per-edge `trigger_rule`. This module converts those to the shared IR's equivalents:
Databricks dynamic-value references, `max_retries`/`timeout_seconds`, and dependency
`outcome`s (which the preparer reduces to `run_if`).
"""

from __future__ import annotations

import ast
import re
from typing import Any

# Airflow Jinja macros -> Databricks job dynamic-value references. Only macros with an exact
# Databricks equivalent are mapped; date macros -> the job start time, run_id -> the run id.
# ``ds_nodash``/``ts_nodash`` have no dashless dynamic-value form, so they are intentionally NOT
# mapped -- they're left untouched (surfaced as an unresolved reference) rather than emitting an
# invalid ref.
_MACRO_TO_DAB_REF: dict[str, str] = {
    "ds": "{{job.start_time.iso_date}}",
    "ts": "{{job.start_time.iso_datetime}}",
    "data_interval_start": "{{job.start_time.iso_datetime}}",
    "data_interval_end": "{{job.start_time.iso_datetime}}",
    "execution_date": "{{job.start_time.iso_datetime}}",
    "logical_date": "{{job.start_time.iso_datetime}}",
    "run_id": "{{job.run_id}}",
}

# {{ params.X }} / {{ var.value.X }} / {{ dag_run.conf['X'] }} -> {{job.parameters.X}}
_PARAM_PATTERNS: list[re.Pattern[str]] = [
    re.compile(r"^params\.([A-Za-z_][A-Za-z0-9_]*)$"),
    re.compile(r"^params\[['\"]([^'\"]+)['\"]\]$"),
    re.compile(r"^var\.value\.([A-Za-z_][A-Za-z0-9_]*)$"),
    re.compile(r"^dag_run\.conf\[['\"]([^'\"]+)['\"]\]$"),
]

_JINJA = re.compile(r"\{\{\s*(.*?)\s*\}\}")


def convert_template(value: str) -> tuple[str, set[str]]:
    """Converts Airflow Jinja in *value* to DAB dynamic-value references.

    Returns ``(converted_value, referenced_param_names)``.  Date/system macros map
    to ``{{job.start_time.*}}`` refs; ``params.X`` / ``var.value.X`` / ``dag_run.conf['X']``
    map to ``{{job.parameters.X}}`` and X is reported so the pipeline can declare it.
    An unrecognised expression is left as-is (so nothing is silently corrupted).
    """
    params: set[str] = set()

    def _sub(match: re.Match[str]) -> str:
        expr = match.group(1).strip()
        if expr in _MACRO_TO_DAB_REF:
            return _MACRO_TO_DAB_REF[expr]
        for pattern in _PARAM_PATTERNS:
            m = pattern.match(expr)
            if m:
                name = m.group(1)
                params.add(name)
                return "{{job.parameters." + name + "}}"
        return match.group(0)  # unknown expression: leave untouched

    return _JINJA.sub(_sub, value), params


# Airflow macro -> the sql_task.parameters name + the DAB dynamic value it resolves to. Databricks
# requires dynamic references in SQL to go through named :markers + sql_task.parameters, never inline.
_SQL_MACRO_PARAM: dict[str, tuple[str, str]] = {
    "ds": ("run_date", "{{job.start_time.iso_date}}"),
    "ts": ("run_timestamp", "{{job.start_time.iso_datetime}}"),
    "data_interval_start": ("data_interval_start", "{{job.start_time.iso_datetime}}"),
    "data_interval_end": ("data_interval_end", "{{job.start_time.iso_datetime}}"),
    "execution_date": ("execution_date", "{{job.start_time.iso_datetime}}"),
    "logical_date": ("logical_date", "{{job.start_time.iso_datetime}}"),
    "run_id": ("run_id", "{{job.run_id}}"),
}
_SQL_IDENTIFIER_CONTEXT = re.compile(
    r"(?:\bFROM|\bJOIN|\bINTO|\bUPDATE|\bTABLE|\bVIEW|\bSCHEMA|\bCATALOG)\s*$",
    re.IGNORECASE,
)


def convert_sql_template(sql: str) -> tuple[str, dict[str, str]]:
    """Rewrites Airflow Jinja in *sql* to ``:name`` markers + a ``sql_task.parameters`` map.

    Databricks requires dynamic references in a ``sql_task`` to be passed through named parameters,
    not interpolated into the SQL text. ``{{ ds }}`` -> ``:run_date`` with
    ``{"run_date": "{{job.start_time.iso_date}}"}``; ``{{ params.x }}`` -> ``:x`` with
    ``{"x": "{{job.parameters.x}}"}``. Unknown expressions are left untouched.

    Returns ``(sql_with_markers, parameters)``.
    """
    parameters: dict[str, str] = {}

    def _marker(name: str, match: re.Match[str]) -> str:
        marker = f":{name}"
        return f"IDENTIFIER({marker})" if _SQL_IDENTIFIER_CONTEXT.search(sql[: match.start()]) else marker

    def _sub(match: re.Match[str]) -> str:
        expr = match.group(1).strip()
        if expr in _SQL_MACRO_PARAM:
            name, ref = _SQL_MACRO_PARAM[expr]
            parameters[name] = ref
            return _marker(name, match)
        for pattern in _PARAM_PATTERNS:
            m = pattern.match(expr)
            if m:
                name = m.group(1)
                parameters[name] = "{{job.parameters." + name + "}}"
                return _marker(name, match)
        return match.group(0)

    return _JINJA.sub(_sub, sql), parameters


def convert_params(value: Any) -> tuple[Any, set[str]]:
    """Recursively converts templates in a str / list / dict value.

    Returns ``(converted, referenced_param_names)``.  Non-string leaves pass through.
    """
    params: set[str] = set()
    if isinstance(value, str):
        converted, refs = convert_template(value)
        return converted, refs
    if isinstance(value, list):
        out_list = []
        for item in value:
            conv, refs = convert_params(item)
            out_list.append(conv)
            params |= refs
        return out_list, params
    if isinstance(value, dict):
        out_dict = {}
        for key, item in value.items():
            conv, refs = convert_params(item)
            out_dict[key] = conv
            params |= refs
        return out_dict, params
    return value, params


def unresolved_jinja_expressions(value: Any) -> set[str]:
    """Returns Jinja expressions that remain after deterministic conversion."""
    if isinstance(value, str):
        return {
            expression
            for match in _JINJA.findall(value)
            if not (expression := match.strip()).startswith(("job.", "tasks.", "input."))
        }
    if isinstance(value, list):
        return set().union(*(unresolved_jinja_expressions(item) for item in value)) if value else set()
    if isinstance(value, dict):
        return set().union(*(unresolved_jinja_expressions(item) for item in value.values())) if value else set()
    return set()


# --------------------------------------------------------------------------------------
# default_args (retries / timeouts / email)
# --------------------------------------------------------------------------------------


def _timedelta_seconds(node: ast.expr | None) -> int | None:
    """Parses a ``timedelta(...)`` AST call into total seconds (keyword args only)."""
    if not isinstance(node, ast.Call):
        return None
    func = node.func
    name = func.attr if isinstance(func, ast.Attribute) else (func.id if isinstance(func, ast.Name) else "")
    if name != "timedelta":
        return None
    units = {"weeks": 604800, "days": 86400, "hours": 3600, "minutes": 60, "seconds": 1, "milliseconds": 0.001}
    total = 0.0
    for kw in node.keywords:
        if kw.arg in units and isinstance(kw.value, ast.Constant) and isinstance(kw.value.value, (int, float)):
            total += kw.value.value * units[kw.arg]
    return int(total) if total > 0 else None


def _literal_int(node: ast.expr | None) -> int | None:
    if isinstance(node, ast.Constant) and isinstance(node.value, bool):
        return None
    if isinstance(node, ast.Constant) and isinstance(node.value, int):
        return node.value
    return None


def retry_policy(dag_default_args: dict[str, ast.expr], task_kwargs: dict[str, ast.expr]) -> dict[str, int]:
    """Returns ``max_retries`` / ``timeout_seconds`` / ``min_retry_interval_millis``.

    Per-task kwargs override DAG-level ``default_args``.  ``retries`` -> max_retries,
    ``execution_timeout=timedelta(...)`` -> timeout_seconds, ``retry_delay=timedelta(...)``
    -> min_retry_interval_millis.  Missing values are omitted.
    """
    result: dict[str, int] = {}

    def pick(key: str) -> ast.expr | None:
        return task_kwargs.get(key, dag_default_args.get(key))

    retries = _literal_int(pick("retries"))
    if retries is not None and retries > 0:
        result["max_retries"] = retries

    timeout = _timedelta_seconds(pick("execution_timeout"))
    if timeout is not None:
        result["timeout_seconds"] = timeout

    retry_delay = _timedelta_seconds(pick("retry_delay"))
    if retry_delay is not None:
        result["min_retry_interval_millis"] = retry_delay * 1000

    return result


def email_on_failure(dag_default_args: dict[str, ast.expr], task_kwargs: dict[str, ast.expr]) -> list[str]:
    """Returns email recipients when email_on_failure is set (for a job-level notification note)."""
    on_failure = task_kwargs.get("email_on_failure", dag_default_args.get("email_on_failure"))
    if isinstance(on_failure, ast.Constant) and on_failure.value is False:
        return []
    email_node = task_kwargs.get("email", dag_default_args.get("email"))
    if isinstance(email_node, ast.Constant) and isinstance(email_node.value, str):
        return [email_node.value]
    if isinstance(email_node, ast.List):
        return [e.value for e in email_node.elts if isinstance(e, ast.Constant) and isinstance(e.value, str)]
    return []


# --------------------------------------------------------------------------------------
# trigger_rule -> dependency outcome
# --------------------------------------------------------------------------------------

# Map Airflow trigger_rule -> the DAB job ``run_if`` constant carried as a dependency outcome. The
# preparer's reducer passes these through unchanged. Rules with no exact DAB equivalent fall back to
# the closest safe constant: none_failed_min_one_success -> NONE_FAILED so an upstream failure
# never permits downstream execution; none_failed_or_skipped -> NONE_FAILED (skips are non-failures).
# Airflow's default all_success maps to None (no run_if key -> Databricks default ALL_SUCCESS).
_TRIGGER_RULE_TO_RUN_IF: dict[str, str | None] = {
    "all_success": None,
    "all_done": "ALL_DONE",
    "all_failed": "ALL_FAILED",
    "one_failed": "AT_LEAST_ONE_FAILED",
    "one_success": "AT_LEAST_ONE_SUCCESS",
    "none_failed": "NONE_FAILED",
    "none_failed_min_one_success": "NONE_FAILED",
    "none_failed_or_skipped": "NONE_FAILED",
    "always": "ALL_DONE",
}


def trigger_rule_outcome(task_kwargs: dict[str, ast.expr]) -> str | None:
    """Maps a task's ``trigger_rule`` kwarg to a DAB ``run_if`` constant, or None (ALL_SUCCESS)."""
    node = task_kwargs.get("trigger_rule")
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        rule = node.value
    elif isinstance(node, ast.Attribute) and isinstance(node.value, ast.Name) and node.value.id == "TriggerRule":
        rule = node.attr.lower()
    else:
        rule = None
    if rule is None:
        return None
    return _TRIGGER_RULE_TO_RUN_IF.get(rule)


# --------------------------------------------------------------------------------------
# Airflow Variable / Connection calls in notebook bodies
# --------------------------------------------------------------------------------------

# Variable.get("x") / Variable.get('x', default) -> dbutils.widgets.get("x") (a job parameter).
_VARIABLE_GET = re.compile(r"""Variable\.get\(\s*['"]([A-Za-z_][A-Za-z0-9_]*)['"]\s*(?:,[^)]*)?\)""")
# BaseHook.get_connection("c") / Connection.get_connection_from_secrets("c") -> flagged (needs a
# secret-scope decision), rewritten to a dbutils.secrets.get with a placeholder scope.
_CONNECTION_GET = re.compile(
    r"""(?:BaseHook|Connection)\.get_connection(?:_from_secrets)?\(\s*['"]([A-Za-z_][A-Za-z0-9_.\-]*)['"]\s*\)"""
)


def airflow_connection_names(source: str) -> set[str]:
    """Returns literal Airflow connection identifiers referenced in Python source."""
    return set(_CONNECTION_GET.findall(source))


def rewrite_airflow_calls(source: str) -> tuple[str, set[str], list[str]]:
    """Rewrites Airflow Variable/Connection calls in notebook-body *source*.

    - ``Variable.get("x")`` -> ``dbutils.widgets.get("x")`` (a job parameter; ``x`` is
      reported so the pipeline declares it and the notebook reads it as a widget).
    - ``BaseHook.get_connection("c")`` -> ``dbutils.secrets.get(scope="<c>_scope", key="...")``
      with a note (connections need a manual secret-scope / UC-connection decision).

    Returns ``(rewritten_source, referenced_params, migration_notes)``. Unrecognised
    references are left untouched.
    """
    params: set[str] = set()
    notes: list[str] = []

    def _var(match: re.Match[str]) -> str:
        name = match.group(1)
        params.add(name)
        return f'dbutils.widgets.get("{name}")'

    def _conn(match: re.Match[str]) -> str:
        conn = match.group(1)
        notes.append(
            f"Airflow connection '{conn}' -> replace with dbutils.secrets.get(scope=..., key=...) "
            f"or a Unity Catalog connection; a placeholder secret scope was emitted."
        )
        return f'dbutils.secrets.get(scope="{conn}_scope", key="value")  # TODO: set real scope/key'

    rewritten = _VARIABLE_GET.sub(_var, source)
    rewritten = _CONNECTION_GET.sub(_conn, rewritten)
    return rewritten, params, notes
