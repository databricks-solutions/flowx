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

FLOWX_INTERNAL_PARAMETER_PREFIX = "__flowx_"
FLOWX_AIRFLOW_PARAMETER_PREFIX = "__flowx_airflow_"
AIRFLOW_RUN_ID_PARAMETER = f"{FLOWX_AIRFLOW_PARAMETER_PREFIX}run_id"

# Airflow date/time macros carry the run's logical date through reserved job parameters so native
# Databricks backfills can override them without colliding with user-defined DAG parameters.
_DATE_MACRO_FIELDS: dict[str, tuple[str, str]] = {
    "ds": ("run_date", "iso_date"),
    "ts": ("run_timestamp", "iso_datetime"),
    "data_interval_start": ("data_interval_start", "iso_datetime"),
    "data_interval_end": ("data_interval_end", "iso_datetime"),
    "execution_date": ("execution_date", "iso_datetime"),
    "logical_date": ("logical_date", "iso_datetime"),
}

# Job parameter name -> dynamic-value time field.
DATE_PARAM_FIELDS: dict[str, str] = {
    f"{FLOWX_AIRFLOW_PARAMETER_PREFIX}{suffix}": field for suffix, field in _DATE_MACRO_FIELDS.values()
}

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

    Reserved logical-date parameters get schedule-aware time refs. The reserved run-id parameter gets
    the inline run-id ref because shell and SQL tasks must bind it through a named value. Other names
    are not macro-derived, so the caller supplies their default.
    """
    field = DATE_PARAM_FIELDS.get(name)
    if field is not None:
        return date_param_default(field, schedule)
    if name == AIRFLOW_RUN_ID_PARAMETER:
        return _MACRO_TO_DAB_REF["run_id"]
    return None


_PARAM_PATTERNS: list[tuple[str, re.Pattern[str]]] = [
    ("parameter", re.compile(r"^params\.([A-Za-z_][A-Za-z0-9_]*)$")),
    ("parameter", re.compile(r"^params\[['\"]([^'\"]+)['\"]\]$")),
    ("variable", re.compile(r"^var\.value\.([A-Za-z_][A-Za-z0-9_]*)$")),
    ("conf", re.compile(r"^dag_run\.conf\[['\"]([^'\"]+)['\"]\]$")),
]

_JINJA = re.compile(r"\{\{\s*(.*?)\s*\}\}")
_PARAMETER_NAME = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


@dataclass(frozen=True, slots=True)
class _TemplateBinding:
    """One recognized Airflow expression and its collision-free Databricks binding."""

    name: str
    value_ref: str
    job_parameter: str | None


def _job_parameter_ref(name: str) -> str:
    return "{{job.parameters." + name + "}}"


def _airflow_parameter(namespace: str, name: str) -> str:
    return f"{FLOWX_AIRFLOW_PARAMETER_PREFIX}{namespace}_{name}"


def _template_binding(expression: str) -> _TemplateBinding | None:
    date_macro = _DATE_MACRO_FIELDS.get(expression)
    if date_macro is not None:
        name = f"{FLOWX_AIRFLOW_PARAMETER_PREFIX}{date_macro[0]}"
        return _TemplateBinding(name=name, value_ref=_job_parameter_ref(name), job_parameter=name)
    if expression in _MACRO_TO_DAB_REF:
        return _TemplateBinding(
            name=AIRFLOW_RUN_ID_PARAMETER,
            value_ref=_MACRO_TO_DAB_REF[expression],
            job_parameter=None,
        )
    for namespace, pattern in _PARAM_PATTERNS:
        match = pattern.match(expression)
        if match is None:
            continue
        source_name = match.group(1)
        if not _PARAMETER_NAME.fullmatch(source_name):
            return None
        if namespace == "parameter":
            if source_name.startswith(FLOWX_INTERNAL_PARAMETER_PREFIX):
                return None
            name = source_name
        else:
            name = _airflow_parameter(namespace, source_name)
        return _TemplateBinding(name=name, value_ref=_job_parameter_ref(name), job_parameter=name)
    return None


def convert_template(value: str) -> tuple[str, set[str]]:
    """Converts Airflow Jinja in *value* to DAB dynamic-value references.

    Airflow-owned values use the reserved ``__flowx_airflow_`` namespace, while ``params.X`` retains
    the user-visible job parameter name. This keeps logical dates, Variables, run configuration, and
    user parameters distinct even when their source names match. Unknown expressions stay unchanged.
    """
    params: set[str] = set()

    def _sub(match: re.Match[str]) -> str:
        binding = _template_binding(match.group(1).strip())
        if binding is None:
            return match.group(0)
        if binding.job_parameter is not None:
            params.add(binding.job_parameter)
        return binding.value_ref

    return _JINJA.sub(_sub, value), params


_SQL_IDENTIFIER_CONTEXT = re.compile(
    r"(?:\bFROM|\bJOIN|\bINTO|\bUPDATE|\bTABLE|\bVIEW|\bSCHEMA|\bCATALOG)\s*$",
    re.IGNORECASE,
)
_SQL_TYPED_LITERAL_CONTEXT = re.compile(r"\b(?:DATE|INTERVAL|TIME|TIMESTAMP)\s*$", re.IGNORECASE)
_SQL_UNSAFE_MARKER_ADJACENCY = frozenset("._")


@dataclass(frozen=True, slots=True)
class _SqlQuotedSpan:
    start: int
    end: int
    delimiter: str
    terminated: bool


def _sql_quoted_spans(sql: str) -> list[_SqlQuotedSpan]:
    """Returns SQL quoted regions while ignoring quotes inside line and block comments."""
    spans: list[_SqlQuotedSpan] = []
    index = 0
    while index < len(sql):
        if sql.startswith("--", index):
            newline = sql.find("\n", index + 2)
            index = len(sql) if newline < 0 else newline + 1
            continue
        if sql.startswith("/*", index):
            closing = sql.find("*/", index + 2)
            index = len(sql) if closing < 0 else closing + 2
            continue
        delimiter = sql[index]
        if delimiter not in ("'", '"', "`"):
            index += 1
            continue
        start = index
        index += 1
        terminated = False
        while index < len(sql):
            if sql[index] != delimiter:
                index += 1
                continue
            if index + 1 < len(sql) and sql[index + 1] == delimiter:
                index += 2
                continue
            index += 1
            terminated = True
            break
        spans.append(_SqlQuotedSpan(start=start, end=index, delimiter=delimiter, terminated=terminated))
    return spans


def _sql_marker_has_unsafe_adjacency(sql: str, start: int, end: int) -> bool:
    """Returns whether replacing this expression would splice a marker into an SQL token."""

    def unsafe(character: str) -> bool:
        return character.isalnum() or character in _SQL_UNSAFE_MARKER_ADJACENCY

    return (start > 0 and unsafe(sql[start - 1])) or (end < len(sql) and unsafe(sql[end]))


def convert_sql_template(sql: str) -> tuple[str, dict[str, str]]:
    """Rewrites Airflow Jinja in *sql* to ``:name`` markers + a ``sql_task.parameters`` map.

    Databricks parameter markers are expressions, not text substitution. A macro that occupies an
    entire single-quoted literal therefore replaces the quotes as well. A macro embedded inside a
    string, quoted identifier, or adjacent SQL token remains unresolved so the loader emits a gap
    instead of changing its meaning.

    Returns ``(sql_with_markers, parameters)``.
    """
    parameters: dict[str, str] = {}
    quoted_spans = _sql_quoted_spans(sql)

    def _marker(name: str, start: int) -> str:
        marker = f":{name}"
        return f"IDENTIFIER({marker})" if _SQL_IDENTIFIER_CONTEXT.search(sql[:start]) else marker

    parts: list[str] = []
    cursor = 0
    for match in _JINJA.finditer(sql):
        binding = _template_binding(match.group(1).strip())
        if binding is None:
            continue
        quoted = next(
            (span for span in quoted_spans if span.start < match.start() and match.end() <= span.end),
            None,
        )
        replacement_start = match.start()
        replacement_end = match.end()
        if quoted is not None:
            whole_single_literal = (
                quoted.delimiter == "'"
                and quoted.terminated
                and match.start() == quoted.start + 1
                and match.end() == quoted.end - 1
                and not _SQL_IDENTIFIER_CONTEXT.search(sql[: quoted.start])
                and not _SQL_TYPED_LITERAL_CONTEXT.search(sql[: quoted.start])
                and not (quoted.start > 0 and (sql[quoted.start - 1].isalnum() or sql[quoted.start - 1] == "_"))
            )
            if not whole_single_literal:
                continue
            replacement_start = quoted.start
            replacement_end = quoted.end
        elif _sql_marker_has_unsafe_adjacency(sql, match.start(), match.end()):
            continue
        parts.append(sql[cursor:replacement_start])
        parts.append(_marker(binding.name, replacement_start))
        cursor = replacement_end
        parameters[binding.name] = binding.value_ref
    parts.append(sql[cursor:])
    return "".join(parts), parameters


def convert_shell_template(command: str) -> tuple[str, dict[str, str]]:
    """Rewrites Airflow Jinja in a bash command to ``$NAME`` shell variable references.

    A DAB dynamic-value ref resolves in a task parameter, not inside ``%sh`` source. Each recognized
    macro therefore becomes a braced shell variable exported from a widget. Braces preserve adjacent
    text, and a macro inside single quotes temporarily exits that quote so the variable still expands.

    Returns ``(command_with_shell_vars, {name: dynamic_value_ref})`` where each ref is what the widget
    of that name must resolve to (a job parameter, or an inline ref for run_id).
    """
    bindings: dict[str, str] = {}

    def _quote_context(position: int) -> tuple[str | None, int | None]:
        quote: str | None = None
        quote_start: int | None = None
        index = 0
        while index < position:
            character = command[index]
            if (
                quote is None
                and character == "#"
                and (index == 0 or command[index - 1].isspace() or command[index - 1] in ";|&()")
            ):
                newline = command.find("\n", index + 1)
                index = position if newline < 0 else newline + 1
                continue
            if character == "\\" and quote != "'":
                index += 2
                continue
            if character in ("'", '"'):
                if quote is None:
                    quote = character
                    quote_start = index
                elif quote == character:
                    quote = None
                    quote_start = None
            index += 1
        return quote, quote_start

    def _inside_quoted_heredoc(position: int) -> bool:
        delimiter: str | None = None
        strip_tabs = False
        for line in command[:position].splitlines():
            candidate = line.lstrip("\t") if strip_tabs else line
            if delimiter is not None:
                if candidate == delimiter:
                    delimiter = None
                    strip_tabs = False
                continue
            match = re.search(r"<<(-?)\s*(['\"])([A-Za-z_][A-Za-z0-9_]*)\2", line)
            if match is not None:
                strip_tabs = bool(match.group(1))
                delimiter = match.group(3)
        return delimiter is not None

    def _escaped(position: int) -> bool:
        backslashes = 0
        index = position - 1
        while index >= 0 and command[index] == "\\":
            backslashes += 1
            index -= 1
        return backslashes % 2 == 1

    def _sub(match: re.Match[str]) -> str:
        binding = _template_binding(match.group(1).strip())
        if binding is None:
            return match.group(0)
        quote, quote_start = _quote_context(match.start())
        if _inside_quoted_heredoc(match.start()) or (quote != "'" and _escaped(match.start())):
            return match.group(0)
        if quote == "'" and quote_start is not None and quote_start > 0 and command[quote_start - 1] == "$":
            return match.group(0)
        bindings[binding.name] = binding.value_ref
        variable = f"${{{binding.name}}}"
        return f"'\"{variable}\"'" if quote == "'" else variable

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


def timedelta_seconds(node: ast.expr | None) -> int | None:
    """Parses a statically numeric ``timedelta(...)`` call into positive whole seconds."""
    if not isinstance(node, ast.Call):
        return None
    func = node.func
    name = func.attr if isinstance(func, ast.Attribute) else (func.id if isinstance(func, ast.Name) else "")
    if name != "timedelta":
        return None

    units = {
        "weeks": 604800,
        "days": 86400,
        "hours": 3600,
        "minutes": 60,
        "seconds": 1,
        "milliseconds": 0.001,
        "microseconds": 0.000001,
    }
    positional_names = ("days", "seconds", "microseconds", "milliseconds", "minutes", "hours", "weeks")
    if len(node.args) > len(positional_names) or any(keyword.arg is None for keyword in node.keywords):
        return None

    values: dict[str, float] = {}
    for unit, argument in zip(positional_names, node.args):
        try:
            value = ast.literal_eval(argument)
        except (ValueError, SyntaxError):
            return None
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            return None
        values[unit] = float(value)
    for keyword in node.keywords:
        if keyword.arg not in units or keyword.arg in values:
            return None
        try:
            value = ast.literal_eval(keyword.value)
        except (ValueError, SyntaxError):
            return None
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            return None
        values[keyword.arg] = float(value)

    total = 0.0
    for unit, value in values.items():
        total += value * units[unit]
    # Round a sub-second total UP to 1s rather than truncating to 0 -- a sub-second timeout/retry_delay
    # is better preserved as 1s than silently dropped (int(0.5) == 0 would read as "unset").
    return math.ceil(total) if total > 0 else None


def literal_email_recipients(node: ast.expr | None) -> list[str] | None:
    """Returns statically declared email recipients, or ``None`` for a dynamic value."""
    if node is None:
        return []
    try:
        value = ast.literal_eval(node)
    except (ValueError, SyntaxError):
        return None
    if value is None:
        return []
    if isinstance(value, str):
        return [value] if value else []
    if isinstance(value, (list, tuple)) and all(isinstance(recipient, str) for recipient in value):
        return [recipient for recipient in value if recipient]
    return None


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

    timeout = timedelta_seconds(pick("execution_timeout"))
    if timeout is not None:
        result["timeout_seconds"] = timeout

    retry_delay = timedelta_seconds(pick("retry_delay"))
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
        if timedelta_seconds(value) is None:
            unresolved.append(name)
    return unresolved


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
}

_APPROXIMATE_NONE_FAILED_RULES = frozenset({"none_failed_min_one_success", "none_failed_or_skipped"})

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
    if rule in _APPROXIMATE_NONE_FAILED_RULES:
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

# Variable.get("x") / Variable.get('x', default) -> a reserved job-parameter widget.
_VARIABLE_GET = re.compile(r"""(?<![A-Za-z0-9_])Variable\.get\(\s*['"]([A-Za-z_][A-Za-z0-9_]*)['"]\s*\)""")
# BaseHook.get_connection("c") / Connection.get_connection_from_secrets("c") -> flagged (needs a
# secret-scope decision), rewritten to a dbutils.secrets.get with a placeholder scope.
_CONNECTION_GET = re.compile(
    r"""(?:BaseHook|Connection)\.get_connection(?:_from_secrets)?\(\s*['"]([A-Za-z_][A-Za-z0-9_.\-]*)['"]\s*\)"""
)


def airflow_connection_names(source: str) -> set[str]:
    """Returns literal Airflow connection identifiers referenced in Python source."""
    return set(_CONNECTION_GET.findall(source))


def rewrite_airflow_calls(source: str, *, rewrite_variable: bool = True) -> tuple[str, set[str], list[str]]:
    """Rewrites Airflow Variable/Connection calls in notebook-body *source*.

    - ``Variable.get("x")`` -> ``dbutils.widgets.get("__flowx_airflow_variable_x")``. The reserved
      name prevents an Airflow Variable from colliding with a DAG parameter of the same name.
    - ``BaseHook.get_connection("c")`` -> ``dbutils.secrets.get(scope="<c>_scope", key="...")``
      with a note (connections need a manual secret-scope / UC-connection decision).

    Returns ``(rewritten_source, referenced_params, migration_notes)``. Unrecognised
    references are left untouched.
    """
    params: set[str] = set()
    notes: list[str] = []

    def _var(match: re.Match[str]) -> str:
        name = _airflow_parameter("variable", match.group(1))
        params.add(name)
        return f'dbutils.widgets.get("{name}")'

    def _conn(match: re.Match[str]) -> str:
        conn = match.group(1)
        notes.append(
            f"Airflow connection '{conn}' -> replace with dbutils.secrets.get(scope=..., key=...) "
            f"or a Unity Catalog connection; a placeholder secret scope was emitted."
        )
        return f'dbutils.secrets.get(scope="{conn}_scope", key="value")  # TODO: set real scope/key'

    rewritten = _VARIABLE_GET.sub(_var, source) if rewrite_variable else source
    rewritten = _CONNECTION_GET.sub(_conn, rewritten)
    return rewritten, params, notes
