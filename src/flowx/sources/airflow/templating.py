"""Airflow Jinja templating, default_args, and trigger_rule -> flowx IR helpers.

Airflow DAGs template values with Jinja (`{{ ds }}`, `{{ params.x }}`, macros) and
carry cross-cutting task settings in `default_args` (retries, timeouts, email) and
per-edge `trigger_rule`. This module converts those to the shared IR's equivalents:
Databricks dynamic-value references, `max_retries`/`timeout_seconds`, and dependency
`outcome`s (which the preparer reduces to `run_if`).
"""

from __future__ import annotations

import ast
import math
import re
from dataclasses import dataclass
from typing import Any

# Airflow date/time macros carrying the run's *logical date* -> a named job parameter (not an inline
# time ref), so a native Databricks backfill can override the parameter per replayed window with
# {{backfill.iso_date}}. Each maps to (parameter_name, time_field); the loader assigns the parameter a
# schedule-aware default (see `date_param_default`). ``ds_nodash``/``ts_nodash`` have no dashless
# dynamic-value form, so they are intentionally NOT mapped -- they're left untouched (surfaced as an
# unresolved reference) rather than emitting an invalid ref.
_DATE_MACRO_PARAM: dict[str, tuple[str, str]] = {
    "ds": ("run_date", "iso_date"),
    "ts": ("run_timestamp", "iso_datetime"),
    "data_interval_start": ("data_interval_start", "iso_datetime"),
    "data_interval_end": ("data_interval_end", "iso_datetime"),
    "execution_date": ("execution_date", "iso_datetime"),
    "logical_date": ("logical_date", "iso_datetime"),
}

# job parameter name -> its time field, so the loader can default each to the right granularity.
DATE_PARAM_FIELDS: dict[str, str] = {param: field for param, field in _DATE_MACRO_PARAM.values()}

# Non-date macros with an exact Databricks equivalent, mapped inline (no backfill relevance).
_MACRO_TO_DAB_REF: dict[str, str] = {
    "run_id": "{{job.run_id}}",
}


def date_param_default(field: str, schedule: dict[str, object] | None) -> str:
    """Returns the default dynamic-value ref for a logical-date job parameter.

    On a cron/periodic schedule the logical date is the scheduled trigger time
    (``{{job.trigger.time...}}``) -- ``start_time`` would drift with queue delay and retries. On an
    event-triggered job (``file_arrival``/``table_update``/``continuous``) or an unscheduled job there
    is no scheduled trigger time, so approximate with the run's start time. A native backfill overrides
    the parameter regardless of this default.
    """
    kind = schedule.get("kind") if schedule else None
    base = "{{job.trigger.time." if kind in ("schedule", "periodic") else "{{job.start_time."
    return f"{base}{field}}}}}"


def macro_param_default(name: str, schedule: dict[str, object] | None) -> str | None:
    """Returns the Databricks-required default for a macro-derived job parameter, or None.

    A logical-date parameter (``run_date`` etc.) gets its schedule-aware time ref; ``run_id`` gets the
    inline run-id ref (bash/env-var threading forces even ``run_id`` through a job parameter, and its
    default must resolve to the run id rather than an empty string). Any other name is not
    macro-derived, so this returns None and the caller falls back to its own default.
    """
    field = DATE_PARAM_FIELDS.get(name)
    if field is not None:
        return date_param_default(field, schedule)
    if name == "run_id":
        return _MACRO_TO_DAB_REF["run_id"]
    return None


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

    Returns ``(converted_value, referenced_param_names)``.  A logical-date macro (``ds``,
    ``execution_date``, ...) maps to ``{{job.parameters.run_date}}`` (etc.) so a native backfill can
    override it; ``params.X`` / ``var.value.X`` / ``dag_run.conf['X']`` map to ``{{job.parameters.X}}``;
    ``run_id`` maps to its inline ref. Referenced parameter names are reported so the pipeline can
    declare them. An unrecognised expression is left as-is (so nothing is silently corrupted).
    """
    params: set[str] = set()

    def _sub(match: re.Match[str]) -> str:
        expr = match.group(1).strip()
        if expr in _DATE_MACRO_PARAM:
            name, _ = _DATE_MACRO_PARAM[expr]
            params.add(name)
            return "{{job.parameters." + name + "}}"
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


_SQL_IDENTIFIER_CONTEXT = re.compile(
    r"(?:\bFROM|\bJOIN|\bINTO|\bUPDATE|\bTABLE|\bVIEW|\bSCHEMA|\bCATALOG)\s*$",
    re.IGNORECASE,
)


def convert_sql_template(sql: str) -> tuple[str, dict[str, str]]:
    """Rewrites Airflow Jinja in *sql* to ``:name`` markers + a ``sql_task.parameters`` map.

    Databricks requires dynamic references in a ``sql_task`` to be passed through named parameters,
    not interpolated into the SQL text. A logical-date macro ``{{ ds }}`` -> ``:run_date`` with
    ``{"run_date": "{{job.parameters.run_date}}"}`` (a job parameter, so a native backfill can override
    it); ``{{ params.x }}`` -> ``:x`` with ``{"x": "{{job.parameters.x}}"}``; ``run_id`` binds to its
    inline ref. Unknown expressions are left untouched.

    Returns ``(sql_with_markers, parameters)``.
    """
    parameters: dict[str, str] = {}

    def _marker(name: str, match: re.Match[str]) -> str:
        marker = f":{name}"
        return f"IDENTIFIER({marker})" if _SQL_IDENTIFIER_CONTEXT.search(sql[: match.start()]) else marker

    def _sub(match: re.Match[str]) -> str:
        expr = match.group(1).strip()
        if expr in _DATE_MACRO_PARAM:
            name, _ = _DATE_MACRO_PARAM[expr]
            parameters[name] = "{{job.parameters." + name + "}}"
            return _marker(name, match)
        if expr in _MACRO_TO_DAB_REF:
            parameters["run_id"] = _MACRO_TO_DAB_REF[expr]
            return _marker("run_id", match)
        for pattern in _PARAM_PATTERNS:
            m = pattern.match(expr)
            if m:
                name = m.group(1)
                parameters[name] = "{{job.parameters." + name + "}}"
                return _marker(name, match)
        return match.group(0)

    return _JINJA.sub(_sub, sql), parameters


def convert_shell_template(command: str) -> tuple[str, dict[str, str]]:
    """Rewrites Airflow Jinja in a bash command to ``$NAME`` shell variable references.

    A DAB dynamic-value ref (``{{job.parameters.X}}``) only resolves in a task *parameter* value, not
    inside ``%sh`` notebook source, so a bash macro can't be replaced inline. Instead each recognised
    macro becomes a ``$name`` shell variable the runner notebook exports from a widget of the same
    name. ``{{ ds }}`` -> ``$run_date``; ``{{ params.x }}`` -> ``$x``; ``run_id`` -> ``$run_id``.
    Unknown expressions are left untouched.

    Returns ``(command_with_shell_vars, {name: dynamic_value_ref})`` where each ref is what the widget
    of that name must resolve to (a job parameter, or an inline ref for run_id).
    """
    bindings: dict[str, str] = {}

    def _sub(match: re.Match[str]) -> str:
        expr = match.group(1).strip()
        if expr in _DATE_MACRO_PARAM:
            name, _ = _DATE_MACRO_PARAM[expr]
            bindings[name] = "{{job.parameters." + name + "}}"
            return f"${name}"
        if expr in _MACRO_TO_DAB_REF:
            bindings["run_id"] = _MACRO_TO_DAB_REF[expr]
            return "$run_id"
        for pattern in _PARAM_PATTERNS:
            m = pattern.match(expr)
            if m:
                name = m.group(1)
                bindings[name] = "{{job.parameters." + name + "}}"
                return f"${name}"
        return match.group(0)

    return _JINJA.sub(_sub, command), bindings


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
    # Round a sub-second total UP to 1s rather than truncating to 0 -- a sub-second timeout/retry_delay
    # is better preserved as 1s than silently dropped (int(0.5) == 0 would read as "unset").
    return math.ceil(total) if total > 0 else None


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


def unrepresented_retry_policy_arguments(
    dag_default_args: dict[str, ast.expr],
    task_kwargs: dict[str, ast.expr],
) -> list[str]:
    """Returns supplied retry/timeout settings that cannot be lowered exactly."""

    def supplied(key: str) -> ast.expr | None:
        return task_kwargs.get(key, dag_default_args.get(key))

    unresolved: list[str] = []
    retries = supplied("retries")
    if retries is not None and not (
        isinstance(retries, ast.Constant)
        and (
            retries.value is None
            or (isinstance(retries.value, int) and not isinstance(retries.value, bool) and retries.value >= 0)
        )
    ):
        unresolved.append("retries")
    for name in ("retry_delay", "execution_timeout"):
        value = supplied(name)
        if value is None or (isinstance(value, ast.Constant) and value.value is None):
            continue
        if _timedelta_seconds(value) is None:
            unresolved.append(name)
    return unresolved


def email_on_failure(dag_default_args: dict[str, ast.expr], task_kwargs: dict[str, ast.expr]) -> list[str]:
    """Returns email recipients when email_on_failure is set (for a job-level notification note).

    TODO: not wired up yet -- the shared IR has no email-notification field, so carrying these through
    to a job's ``email_notifications`` needs an IR addition (tracked separately).
    """
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


@dataclass(frozen=True, slots=True, kw_only=True)
class TriggerRuleMapping:
    """Databricks run-if mapping and its semantic confidence."""

    rule: str
    outcome: str | None
    status: str
    message: str | None = None


# Exact mappings only. Rules outside this table must not collapse to ALL_SUCCESS.
_TRIGGER_RULE_TO_RUN_IF: dict[str, str | None] = {
    "all_success": None,
    "all_done": "ALL_DONE",
    "all_failed": "ALL_FAILED",
    "one_failed": "AT_LEAST_ONE_FAILED",
    "one_success": "AT_LEAST_ONE_SUCCESS",
    "none_failed": "NONE_FAILED",
    "none_failed_or_skipped": "NONE_FAILED",
}

_UNSUPPORTED_TRIGGER_RULES = frozenset({"always", "dummy", "none_skipped", "all_skipped", "one_done"})


def _trigger_rule_name(task_kwargs: dict[str, ast.expr]) -> str | None:
    node = task_kwargs.get("trigger_rule")
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        return node.value.lower()
    if isinstance(node, ast.Attribute):
        return node.attr.lower()
    return None


def trigger_rule_mapping(task_kwargs: dict[str, ast.expr]) -> TriggerRuleMapping:
    """Classifies an Airflow trigger rule as exact, approximate, or unsupported."""
    trigger_rule = task_kwargs.get("trigger_rule")
    if trigger_rule is None:
        rule = "all_success"
    else:
        resolved_rule = _trigger_rule_name(task_kwargs)
        if resolved_rule is None:
            return TriggerRuleMapping(
                rule=ast.unparse(trigger_rule),
                outcome=None,
                status="unsupported",
                message="The trigger rule cannot be resolved statically.",
            )
        rule = resolved_rule
    if rule == "none_failed_min_one_success":
        return TriggerRuleMapping(
            rule=rule,
            outcome="NONE_FAILED",
            status="approximate",
            message=(
                "Databricks NONE_FAILED preserves the no-upstream-failure requirement but may run "
                "when every upstream task was skipped or excluded."
            ),
        )
    if rule in _TRIGGER_RULE_TO_RUN_IF:
        return TriggerRuleMapping(rule=rule, outcome=_TRIGGER_RULE_TO_RUN_IF[rule], status="exact")
    detail = (
        "Databricks has no run_if predicate with equivalent skipped/not-run behavior."
        if rule in _UNSUPPORTED_TRIGGER_RULES
        else "The trigger rule is not recognized by the static Airflow translator."
    )
    return TriggerRuleMapping(rule=rule, outcome=None, status="unsupported", message=detail)


def trigger_rule_outcome(task_kwargs: dict[str, ast.expr]) -> str | None:
    """Maps a task's ``trigger_rule`` kwarg to a DAB ``run_if`` constant, or None (ALL_SUCCESS)."""
    return trigger_rule_mapping(task_kwargs).outcome


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
