"""Translates ADF expressions to a unified ExpressionResult."""

from __future__ import annotations

import re
from collections.abc import Callable
from typing import Any

from orchestra.models.ir import ExpressionResult, TranslationContext

_ITEM_RE = re.compile(r"item\(\s*\)$", re.IGNORECASE)

# C-35 (CF4-004): anchor the end-of-string so multi-segment chains like
# ``item().condition.name`` don't match here and silently drop the trailing
# ``.name`` (the previous behaviour mapped to ``{{input.condition}}``).
_ITEM_FIELD_RE = re.compile(r"item\(\s*\)\.(\w+)\s*$", re.IGNORECASE)

_ACTIVITY_OUTPUT_RE = re.compile(
    r"""activity\(\s*'([^']+)'\s*\)\.output(?:\.(.+))?""",
    re.IGNORECASE,
)

_PIPELINE_PARAM_RE = re.compile(
    r"""pipeline\(\s*\)\.parameters\.(\w+)""",
    re.IGNORECASE,
)

_PIPELINE_GLOBAL_PARAM_RE = re.compile(
    r"""pipeline\(\s*\)\.globalParameters\.(\w+)""",
    re.IGNORECASE,
)

_PIPELINE_PROPERTY_RE = re.compile(
    r"""pipeline\(\s*\)\.(\w+)""",
    re.IGNORECASE,
)

_LINKED_SERVICE_PARAM_RE = re.compile(
    r"""linkedService\(\s*\)\.(\w+)""",
    re.IGNORECASE,
)

_ITEM_SAFE_NAV_RE = re.compile(
    r"item\(\s*\)((?:\??\.\w+)+)$",
    re.IGNORECASE,
)

_VARIABLE_RE = re.compile(
    r"""variables\(\s*'([^']+)'\s*\)""",
    re.IGNORECASE,
)

_CONCAT_RE = re.compile(
    r"""concat\((.+)\)""",
    re.IGNORECASE | re.DOTALL,
)

_UTCNOW_RE = re.compile(
    r"""utcNow\(\s*(?:'([^']*)')?\s*\)""",
    re.IGNORECASE,
)

_DATE_FORMAT_MAP: dict[str, str] = {
    "yyyy": "%Y",
    "yy": "%y",
    "MM": "%m",
    "dd": "%d",
    "HH": "%H",
    "hh": "%I",
    "mm": "%M",
    "ss": "%S",
    "fff": "%f",
    "tt": "%p",
}

_DAB_PIPELINE_PROPERTY_MAP: dict[str, str] = {
    "RunId": "{{job.run_id}}",
    "GroupId": "{{job.run_id}}",
    "TriggerTime": "{{job.start_time.iso_datetime}}",
    "Pipeline": "{{job.name}}",
    "TriggerName": "{{job.trigger.type}}",
    "DataFactory": "{{job.run_id}}",
}

_INTERPOLATION_RE = re.compile(r"@\{(.+?)\}")

_FUNCTION_CALL_RE = re.compile(
    r"([a-zA-Z_]\w*)\((.*)?\)\s*$",
    re.IGNORECASE | re.DOTALL,
)

# Function names that are no-op wrappers when they appear at the outermost
# position around a single deterministic parameter / variable reference.
# Stripping these lets resolve_expression reach the underlying ref instead
# of falling through to notebook_code for trivial @json(pipeline().parameters.X)
# style wrappers commonly used in ADF for type coercion.
_NOOP_WRAPPER_NAMES: frozenset[str] = frozenset({"json", "string", "array"})

_DATETIME_IMPORTS = ["from datetime import datetime, timezone, timedelta"]

_TIME_UNIT_MAP: dict[str, str] = {
    "Second": "seconds",
    "Minute": "minutes",
    "Hour": "hours",
    "Day": "days",
    "Week": "weeks",
}


def resolve_expression(
    value: str | dict[str, Any] | int | float | bool,
    context: TranslationContext,
    *,
    variable_task_keys: dict[str, str] | None = None,
) -> ExpressionResult | None:
    """Translates an ADF expression to an :class:`ExpressionResult`.

    Args:
        value: The ADF expression value.  May be a plain scalar, an
            ``@``-prefixed expression string, or an ``{"type": "Expression",
            "value": "..."}`` dict.
        context: Translation context carrying variable mappings.
        variable_task_keys: Optional explicit mapping of variable names to
            setter task keys.  When provided these take precedence over
            ``context.variable_cache``.

    Returns:
        An :class:`ExpressionResult`, or ``None`` if the expression is too
        complex for deterministic translation.
    """
    if isinstance(value, dict):
        if value.get("type") == "Expression" and "value" in value:
            return resolve_expression(value["value"], context, variable_task_keys=variable_task_keys)
        return None

    if isinstance(value, bool):
        # VAREX3-002: render Python bool as lowercase 'true'/'false' so
        # downstream ADF comparisons like @equals(variables('X'), true)
        # match ADF's lowercase boolean tokens.
        return ExpressionResult(kind="literal", value="true" if value else "false")
    if isinstance(value, (int, float)):
        return ExpressionResult(kind="literal", value=str(value))

    if not isinstance(value, str):
        return None

    if not value.startswith("@"):
        return ExpressionResult(kind="literal", value=value)

    expr = value[1:].rstrip()  # strip leading @ and trailing whitespace/newlines

    # Strip no-op wrappers like @json(pipeline().parameters.X) so the inner
    # ref resolves to its DAB dynamic value.  We only unwrap when the inner
    # expression itself resolves cleanly (literal / dab_ref) so we don't
    # eat the wrapper's semantics where it actually matters.
    unwrapped = _unwrap_noop_call(expr, context, variable_task_keys=variable_task_keys)
    if unwrapped is not None:
        return unwrapped

    if _ITEM_RE.match(expr):
        return ExpressionResult(kind="dab_ref", value="{{input}}")

    match = _ITEM_FIELD_RE.match(expr)
    if match:
        field_name = match.group(1)
        return ExpressionResult(kind="dab_ref", value="{{input." + field_name + "}}")

    result = _resolve_item_safe_nav(expr)
    if result is not None:
        return result

    result = _resolve_pipeline_global_param(expr, context)
    if result is not None:
        return result

    result = _resolve_linked_service_param(expr, context)
    if result is not None:
        return result

    result = _resolve_pipeline_param(expr)
    if result is not None:
        return result

    result = _resolve_pipeline_property(expr)
    if result is not None:
        return result

    result = _resolve_activity_output(expr)
    if result is not None:
        return result

    result = _resolve_variable(expr, context, variable_task_keys=variable_task_keys)
    if result is not None:
        return result

    result = _resolve_utcnow(expr)
    if result is not None:
        return result

    result = _resolve_concat(expr, context, variable_task_keys=variable_task_keys)
    if result is not None:
        return result

    result = _resolve_function_call(expr, context, variable_task_keys=variable_task_keys)
    if result is not None:
        return result

    # CF3-004 / fix-attribute-access-on-function-results: handle
    # ``<function_call>.<attribute>...`` chains like
    # ``json(pipeline().parameters.items).type`` by resolving the function
    # call first then chaining `.get('attr')` onto the resulting code.
    result = _resolve_function_call_with_attribute(expr, context, variable_task_keys=variable_task_keys)
    if result is not None:
        return result

    # C-33 (VAREX4-001): handle ``<function_call>[N]`` chains so e.g.
    # ``@split(pipeline().parameters.referenceDate,'/')[0]`` lowers to
    # notebook_code.
    result = _resolve_function_call_with_index(expr, context, variable_task_keys=variable_task_keys)
    if result is not None:
        return result

    return None


def resolve_interpolated_string(
    value: str,
    context: TranslationContext,
    *,
    variable_task_keys: dict[str, str] | None = None,
) -> str:
    """Resolves ``@{...}`` interpolation tokens within a string.

    Args:
        value: A string potentially containing ``@{...}`` tokens.
        context: Translation context for resolving variables.
        variable_task_keys: Optional explicit variable-name-to-task-key map.

    Returns:
        The string with all ``@{...}`` tokens replaced by resolved values.
        Tokens that cannot be resolved are left unchanged.
    """
    if not isinstance(value, str):
        return value

    if "@{" not in value:
        return value

    def _replace_match(match: re.Match[str]) -> str:
        inner_expr = match.group(1)
        result = resolve_expression("@" + inner_expr, context, variable_task_keys=variable_task_keys)
        if result is not None and result.kind in ("dab_ref", "literal"):
            return result.value
        return match.group(0)

    return _INTERPOLATION_RE.sub(_replace_match, value)


def resolve_interpolated_string_for_notebook(
    value: str,
    context: TranslationContext,
    *,
    variable_task_keys: dict[str, str] | None = None,
) -> str:
    """Resolves ``@{...}`` tokens to Python f-string expressions for notebook code.

    Args:
        value: A string containing ``@{...}`` tokens.
        context: Translation context for resolving variables.
        variable_task_keys: Optional explicit variable-name-to-task-key map.

    Returns:
        A string with ``@{...}`` tokens replaced by Python f-string expressions.
    """
    if not isinstance(value, str) or "@{" not in value:
        return value

    def _replace_match(match: re.Match[str]) -> str:
        inner_expr = match.group(1)
        result = resolve_expression("@" + inner_expr, context, variable_task_keys=variable_task_keys)
        if result is None:
            return match.group(0)
        if result.kind == "literal":
            return result.value
        if result.kind == "dab_ref":
            ref = result.value
            param_match = re.match(r"\{\{job\.parameters\.(\w+)\}\}", ref)
            if param_match:
                return "{dbutils.widgets.get('" + param_match.group(1) + "')}"
            task_value_match = re.match(r"\{\{tasks\.([^.]+)\.values\.(\w+)\}\}", ref)
            if task_value_match:
                return (
                    "{dbutils.jobs.taskValues.get(taskKey='"
                    + task_value_match.group(1)
                    + "', key='"
                    + task_value_match.group(2)
                    + "')}"
                )
            if ref == "{{job.run_id}}":
                return "{spark.conf.get('spark.databricks.job.runId', '')}"
            if ref == "{{job.name}}":
                return "{spark.conf.get('spark.databricks.job.parentName', '')}"
            if ref == "{{job.start_time.iso_datetime}}":
                return "{spark.conf.get('spark.databricks.job.triggerTime', '')}"
            if ref == "{{input}}":
                return "{dbutils.widgets.get('item')}"
            return ref
        return "{" + result.value + "}"

    return _INTERPOLATION_RE.sub(_replace_match, value)


def parse_expression(value: str | dict[str, Any] | int | float | bool, context: TranslationContext) -> str | None:
    """Backward-compatible wrapper: return the resolved value for any kind, or None.

    Args:
        value: The ADF expression value.
        context: Translation context.

    Returns:
        A string value, or ``None`` for unsupported expressions.
    """
    result = resolve_expression(value, context)
    if result is None:
        return None
    return result.value


def parse_expression_for_dab(
    value: str | dict[str, Any] | int | float | bool,
    *,
    variable_task_keys: dict[str, str] | None = None,
) -> str | None:
    """Backward-compatible wrapper: return DAB dynamic value ref or None.

    Args:
        value: The ADF expression value.
        variable_task_keys: Optional mapping of variable names to setter task keys.

    Returns:
        A DAB dynamic value reference string, or ``None``.
    """
    context = TranslationContext()
    result = resolve_expression(value, context, variable_task_keys=variable_task_keys)
    if result is None:
        return None
    if result.kind == "dab_ref":
        return result.value
    return None


def _resolve_pipeline_param(expr: str) -> ExpressionResult | None:
    """Resolves ``pipeline().parameters.X`` -> DAB ref."""
    match = _PIPELINE_PARAM_RE.match(expr)
    if match is None:
        return None
    param_name = match.group(1)
    return ExpressionResult(kind="dab_ref", value="{{" + f"job.parameters.{param_name}" + "}}")


def _resolve_pipeline_global_param(expr: str, context: TranslationContext) -> ExpressionResult | None:
    """Resolves ``pipeline().globalParameters.X`` against factory globals.

    When ``context.global_parameters`` carries a concrete value for *X*
    the expression collapses to a literal so downstream callers (notably
    ``concat`` reductions) get the actual factory value baked in.  When
    no factory value is available we fall back to a job-parameter DAB
    ref so the bundle YAML can supply it.
    """
    match = _PIPELINE_GLOBAL_PARAM_RE.match(expr)
    if match is None:
        return None
    param_name = match.group(1)
    value = context.get_global_parameter(param_name)
    if value is None:
        return ExpressionResult(kind="dab_ref", value="{{" + f"job.parameters.{param_name}" + "}}")
    return ExpressionResult(kind="literal", value=str(value))


def _resolve_linked_service_param(expr: str, context: TranslationContext) -> ExpressionResult | None:
    """Resolves ``linkedService().X`` against activity-supplied LS parameters."""
    match = _LINKED_SERVICE_PARAM_RE.match(expr)
    if match is None:
        return None
    param_name = match.group(1)
    if param_name in context.linked_service_parameters:
        value = context.get_linked_service_parameter(param_name)
        if value is None:
            return None
        return ExpressionResult(kind="literal", value=str(value))
    return None


def _resolve_item_safe_nav(expr: str) -> ExpressionResult | None:
    """Resolves ``item()?.X``, ``item().a?.b?.c`` and ``item().a.b`` chains.

    Any chain that uses the ADF safe-navigation operator ``?.`` — even a
    single segment ``item()?.X`` — must lower to notebook_code so the bridge
    lowering can fire (the trivial ``item().X`` case stays a DAB ref via
    ``_ITEM_FIELD_RE``).  The returned ``notebook_code`` walks the chain
    using ``.get()`` so missing keys do not raise.

    C-16 (CF3-005 / VAREX3-005): the previous ``len(parts) < 2`` guard
    blocked Switch on-expressions and SetVariable expressions wrapping
    ``item()?.X`` from triggering bridge lowering.
    """
    match = _ITEM_SAFE_NAV_RE.match(expr)
    if match is None:
        return None
    chain = match.group(1)
    # Collect (operator, field) tuples so single-segment item()?.X still
    # lowers to notebook_code (was: skipped when len(parts) < 2).
    segments: list[tuple[str, str]] = re.findall(r"(\??\.)(\w+)", chain)
    if not segments:
        return None
    # If the chain has no safe-nav operator at all (purely ``item().a.b``)
    # AND only one segment, defer to _ITEM_FIELD_RE's dab_ref path.
    # C-35 (CF4-004): multi-segment pure-dotted chains like
    # ``item().condition.name`` must lower to notebook_code so the
    # downstream consumers can walk both segments instead of mapping to
    # ``{{input.condition}}`` and silently dropping ``.name``.
    has_safe_nav = any(op == "?." for op, _ in segments)
    if not has_safe_nav and len(segments) < 2:
        return None
    expr_code = "__import__('json').loads(dbutils.widgets.get('item'))"
    for _, part in segments:
        expr_code = f"({expr_code} or {{}}).get('{part}')"
    return ExpressionResult(kind="notebook_code", value=expr_code)


def _unwrap_noop_call(
    expr: str,
    context: TranslationContext,
    *,
    variable_task_keys: dict[str, str] | None = None,
) -> ExpressionResult | None:
    """If *expr* is a no-op wrapper around a deterministic ref, return inner.

    Handles patterns like ``@json(pipeline().parameters.items)``,
    ``@string(pipeline().parameters.X)``, and ``@array(...)`` where the
    sole argument resolves to a clean literal/dab_ref.  When the inner
    cannot be deterministically resolved we return ``None`` so the
    regular function dispatcher takes over.
    """
    match = _FUNCTION_CALL_RE.match(expr)
    if match is None:
        return None
    func_name = match.group(1)
    if func_name.lower() not in _NOOP_WRAPPER_NAMES:
        return None
    inner = (match.group(2) or "").strip()
    if not inner:
        return None
    args = _split_args(inner)
    if len(args) != 1:
        return None
    sole_arg = args[0].strip()
    if sole_arg.startswith("'") and sole_arg.endswith("'"):
        return None  # bare literal, let upstream string() handler decide
    sub_expr = sole_arg if sole_arg.startswith("@") else "@" + sole_arg
    inner_result = resolve_expression(sub_expr, context, variable_task_keys=variable_task_keys)
    if inner_result is None:
        return None
    if inner_result.kind in ("literal", "dab_ref"):
        return inner_result
    return None


def _resolve_pipeline_property(expr: str) -> ExpressionResult | None:
    """Resolves ``pipeline().PropertyName`` -> DAB ref."""
    match = _PIPELINE_PROPERTY_RE.match(expr)
    if match is None:
        return None
    prop = match.group(1)
    if prop == "parameters":
        return None
    dab_ref = _DAB_PIPELINE_PROPERTY_MAP.get(prop)
    if dab_ref is not None:
        return ExpressionResult(kind="dab_ref", value=dab_ref)
    return None


def _resolve_activity_output(expr: str) -> ExpressionResult | None:
    """Resolves ``activity('Name').output...`` -> DAB ref."""
    match = _ACTIVITY_OUTPUT_RE.match(expr)
    if match is None:
        return None
    activity_name = match.group(1)
    task_key = re.sub(r"[^a-zA-Z0-9_-]", "_", activity_name)
    task_key = re.sub(r"_+", "_", task_key).strip("_") or "unnamed"

    property_path = match.group(2) or ""
    if property_path:
        parts = property_path.split(".")
        field = parts[-1] if parts[-1] != "firstRow" else "result"
        if field == "value":
            field = "result"
    else:
        field = "result"

    return ExpressionResult(kind="dab_ref", value="{{" + f"tasks.{task_key}.values.{field}" + "}}")


def _resolve_variable(
    expr: str,
    context: TranslationContext,
    *,
    variable_task_keys: dict[str, str] | None = None,
) -> ExpressionResult | None:
    """Resolves ``variables('name')`` -> task value DAB ref.

    C-05 (VAREX-002): when neither the explicit mapping nor the context's
    variable_cache knows a setter for *var_name*, return ``None`` instead
    of falling back to ``{{tasks.<name>.values.<name>}}`` — that
    self-referential placeholder is never satisfied at runtime and
    pollutes the bundle with hundreds of dangling refs.  Init tasks
    synthesised in :func:`engine._build_variable_init_activities` seed
    the cache for default-valued variables, so this path now triggers
    only for genuinely unset variables (caller logs / surfaces a setup
    note).
    """
    match = _VARIABLE_RE.match(expr)
    if match is None:
        return None
    var_name = match.group(1)

    variable_task_keys_map = variable_task_keys or {}
    setter_key = variable_task_keys_map.get(var_name) or context.get_variable_task_key(var_name)
    if setter_key is None:
        return None
    return ExpressionResult(kind="dab_ref", value="{{" + f"tasks.{setter_key}.values.{var_name}" + "}}")


_UTCNOW_APPROXIMATION_NOTE = (
    "Mapped ADF `utcnow()` to the Databricks job start time. "
    "Single-task jobs see sub-second skew; multi-task jobs can see minutes of skew "
    "between job start and the moment the activity actually runs."
)

# ADF .NET-style format strings that map cleanly onto a Databricks dynamic
# value reference.  Anything not in this table falls back to a notebook_code
# strftime call.
_UTCNOW_FORMAT_TO_DAB_REF: dict[str, str] = {
    "yyyy-MM-dd": "{{job.start_time.iso_date}}",
    "yyyy-MM-ddTHH:mm:ss": "{{job.start_time.iso_datetime}}",
    "yyyy-MM-ddTHH:mm:ssZ": "{{job.start_time.iso_datetime}}",
    "o": "{{job.start_time.iso_datetime}}",
    "s": "{{job.start_time.iso_datetime}}",
}


def _resolve_utcnow(expr: str) -> ExpressionResult | None:
    """Resolves ``utcNow()`` / ``utcNow('format')``.

    Bare ``utcnow()`` and ``utcnow('<known iso fmt>')`` map to a Databricks
    dynamic value reference so the result can land directly inside DAB
    YAML (``base_parameters`` and similar).  Unrecognised format strings
    keep the legacy ``notebook_code`` translation.
    """
    match = _UTCNOW_RE.match(expr)
    if match is None:
        return None
    format_string = match.group(1)
    if not format_string:
        return ExpressionResult(
            kind="dab_ref",
            value="{{job.start_time.iso_datetime}}",
            notes=[_UTCNOW_APPROXIMATION_NOTE],
        )
    dab_ref = _UTCNOW_FORMAT_TO_DAB_REF.get(format_string)
    if dab_ref:
        return ExpressionResult(
            kind="dab_ref",
            value=dab_ref,
            notes=[_UTCNOW_APPROXIMATION_NOTE],
        )
    python_format = _convert_date_format(format_string)
    return ExpressionResult(
        kind="notebook_code",
        value=f"datetime.now(timezone.utc).strftime('{python_format}')",
        imports=["from datetime import datetime, timezone"],
    )


def _convert_date_format(adf_format: str) -> str:
    """Converts an ADF .NET date format string to Python strftime format.

    Args:
        adf_format: ADF format string (e.g., ``"yyyy-MM-dd"``).

    Returns:
        Python strftime format string (e.g., ``"%Y-%m-%d"``).
    """
    result = adf_format
    for adf_token, python_token in sorted(_DATE_FORMAT_MAP.items(), key=lambda x: -len(x[0])):
        result = result.replace(adf_token, python_token)
    return result


def _resolve_concat(
    expr: str,
    context: TranslationContext,
    *,
    variable_task_keys: dict[str, str] | None = None,
) -> ExpressionResult | None:
    """Resolves ``concat(arg1, arg2, ...)`` -> notebook_code."""
    match = _CONCAT_RE.match(expr)
    if match is None:
        return None

    inner = match.group(1).strip()
    parts: list[str] = _split_concat_args(inner)
    if not parts:
        return None

    all_imports: list[str] = []
    code_parts: list[str] = []
    literal_parts: list[str] = []
    all_literal = True
    all_required_parameters: dict[str, str] = {}

    for part in parts:
        part = part.strip()
        if not part:
            continue
        if part.startswith("'") and part.endswith("'"):
            value_text = part[1:-1]
            code_parts.append(repr(value_text))
            literal_parts.append(value_text)
        else:
            sub_result = resolve_expression("@" + part, context, variable_task_keys=variable_task_keys)
            if sub_result is None:
                return None
            if sub_result.kind == "literal":
                code_parts.append(repr(sub_result.value))
                literal_parts.append(sub_result.value)
            elif sub_result.kind == "dab_ref":
                code_parts.append(_dab_ref_to_widget_code(sub_result.value))
                widget_name, dab_ref = _required_parameter_for_ref(sub_result.value)
                all_required_parameters.setdefault(widget_name, dab_ref)
                all_literal = False
            elif sub_result.kind == "notebook_code":
                code_parts.append(f"str({sub_result.value})")
                all_imports.extend(sub_result.imports)
                all_required_parameters.update(sub_result.required_parameters)
                all_literal = False

    if not code_parts:
        return None

    # If every part collapsed to a literal value, fold the whole concat into
    # a single literal so downstream consumers (notebook library install,
    # cluster fields, etc.) get a plain string instead of Python source.
    if all_literal:
        return ExpressionResult(kind="literal", value="".join(literal_parts))

    value = " + ".join(code_parts)
    return ExpressionResult(
        kind="notebook_code",
        value=value,
        imports=list(dict.fromkeys(all_imports)),
        required_parameters=all_required_parameters,
    )


def _dab_ref_to_widget_code(dab_ref: str) -> str:
    """Converts a DAB ref like ``{{tasks.X.values.Y}}`` to widget get code."""
    widget_name, _ = _required_parameter_for_ref(dab_ref)
    return f"dbutils.widgets.get('{widget_name}')"


def _required_parameter_for_ref(dab_ref: str) -> tuple[str, str]:
    """Return ``(widget_name, dab_ref)`` for a DAB dynamic value reference."""
    inner = dab_ref.strip("{}")
    widget_name = inner.split(".")[-1]
    return widget_name, dab_ref


def _split_concat_args(inner: str) -> list[str]:
    """Splits concat arguments respecting nested parentheses and quoted strings.

    Args:
        inner: The inner content of ``concat(...)``.

    Returns:
        List of argument strings.
    """
    return _split_args(inner)


def _split_args(inner: str) -> list[str]:
    """Splits function arguments respecting nested parentheses and quoted strings.

    Args:
        inner: The inner content between the outermost parentheses.

    Returns:
        List of argument strings.
    """
    parts: list[str] = []
    depth = 0
    current: list[str] = []
    in_quote = False

    for char in inner:
        if char == "'" and depth == 0:
            in_quote = not in_quote
            current.append(char)
        elif in_quote:
            current.append(char)
        elif char == "(":
            depth += 1
            current.append(char)
        elif char == ")":
            depth -= 1
            current.append(char)
        elif char == "," and depth == 0:
            parts.append("".join(current).strip())
            current = []
        else:
            current.append(char)

    if current:
        parts.append("".join(current).strip())

    return parts


_FUNCTION_CALL_WITH_ATTRIBUTE_RE = re.compile(
    # Captures `funcName(args).attr.attr...` -- the trailing attribute chain
    # must end with a word character so we don't accidentally swallow other
    # closing parens / spaces.  Used to lower
    # ``json(pipeline().parameters.items).type`` to a notebook_code expression
    # since the bare function dispatcher requires the function call to be the
    # outermost token.
    r"^([a-zA-Z_]\w*)\((.*)\)((?:\.\w+)+)\s*$",
    re.IGNORECASE | re.DOTALL,
)

_FUNCTION_CALL_WITH_INDEX_RE = re.compile(
    # C-33 (VAREX4-001): ``funcName(args)[N]`` — captures a trailing
    # integer subscript so ``split(...)[0]`` and similar ADF expressions
    # lower to notebook_code (the bare dispatcher only matched when the
    # function call was the outermost token).  We support a single
    # numeric subscript for now; nested chains (``...[0][1]``) fall
    # through to the legacy unsupported path.
    r"^([a-zA-Z_]\w*)\((.*)\)\[\s*(-?\d+)\s*\]\s*$",
    re.IGNORECASE | re.DOTALL,
)


def _resolve_function_call_with_index(
    expr: str,
    context: TranslationContext,
    *,
    variable_task_keys: dict[str, str] | None = None,
) -> ExpressionResult | None:
    """Lowers ``<func_call>[N]`` to notebook_code.

    C-33 (VAREX4-001): ADF SetVariable expressions like
    ``@split(pipeline().parameters.referenceDate,'/')[0]`` were
    previously rejected because the bare function dispatcher matched
    only when the call was the outermost token.  Resolve the function
    call as usual, then append ``[N]`` to the resulting Python code.
    """
    match = _FUNCTION_CALL_WITH_INDEX_RE.match(expr)
    if match is None:
        return None
    func_name = match.group(1)
    inner = match.group(2)
    index = match.group(3)
    func_expr = f"@{func_name}({inner})"
    base_result = resolve_expression(func_expr, context, variable_task_keys=variable_task_keys)
    if base_result is None:
        return None
    if base_result.kind == "literal":
        base_code = repr(base_result.value)
    elif base_result.kind == "dab_ref":
        base_code = _dab_ref_to_widget_code(base_result.value)
    else:
        base_code = base_result.value
    code = f"({base_code})[{index}]"
    return ExpressionResult(
        kind="notebook_code",
        value=code,
        imports=list(base_result.imports),
        required_parameters=dict(base_result.required_parameters),
    )


def _resolve_function_call_with_attribute(
    expr: str,
    context: TranslationContext,
    *,
    variable_task_keys: dict[str, str] | None = None,
) -> ExpressionResult | None:
    """Lowers `<func_call>.<attr>...` chains to a notebook_code expression.

    CF3-004 / fix-attribute-access-on-function-results: the bare function
    dispatcher only matches when the function call is the outermost token,
    so an expression like ``json(pipeline().parameters.items).type`` falls
    through with ``None`` and ships unmodified into ``condition_task.left``.
    We resolve the function call, render it as Python code, then chain
    ``.get('attr')`` for each segment of the trailing attribute path so the
    bridge lowering picks it up.
    """
    match = _FUNCTION_CALL_WITH_ATTRIBUTE_RE.match(expr)
    if match is None:
        return None
    func_name = match.group(1)
    inner = match.group(2)
    attr_chain = match.group(3)
    func_expr = f"@{func_name}({inner})"
    base_result = resolve_expression(func_expr, context, variable_task_keys=variable_task_keys)
    if base_result is None:
        return None
    if base_result.kind == "literal":
        # The result is a known literal -- render it as a Python expression
        # then chain `.get(...)` so the resulting notebook_code is valid.
        base_code = repr(base_result.value)
    elif base_result.kind == "dab_ref":
        base_code = _dab_ref_to_widget_code(base_result.value)
    else:
        base_code = base_result.value
    code = base_code
    for segment in attr_chain.strip(".").split("."):
        code = f"({code}).get('{segment}')"
    return ExpressionResult(
        kind="notebook_code",
        value=code,
        imports=list(base_result.imports),
        required_parameters=dict(base_result.required_parameters),
    )


def _resolve_function_call(
    expr: str,
    context: TranslationContext,
    *,
    variable_task_keys: dict[str, str] | None = None,
) -> ExpressionResult | None:
    """Resolves a generic ADF function call via the dispatch table."""
    match = _FUNCTION_CALL_RE.match(expr)
    if match is None:
        return None

    func_name = match.group(1)
    inner = (match.group(2) or "").strip()

    handler = _FUNCTION_HANDLERS.get(func_name)
    if handler is None:
        handler = _FUNCTION_HANDLERS_CI.get(func_name.lower())
    if handler is None:
        return None

    if not inner:
        raw_args: list[str] = []
    else:
        raw_args = _split_args(inner)

    resolved_args: list[ExpressionResult] = []
    for raw_arg in raw_args:
        raw_arg = raw_arg.strip()
        if not raw_arg:
            continue

        if (raw_arg.startswith("'") and raw_arg.endswith("'")) or (raw_arg.startswith('"') and raw_arg.endswith('"')):
            # C-34 (VAREX4-002): preserve the quotedness so the codegen
            # downstream emits ``repr(value)`` rather than a bare token —
            # otherwise quoted ``'09'`` / ``'12'`` collapse to a bare
            # numeric and either raise a SyntaxError (leading zero) or
            # silently compare against the wrong value.
            resolved_args.append(ExpressionResult(kind="literal", value=raw_arg[1:-1], was_string_literal=True))
        elif _is_numeric(raw_arg):
            resolved_args.append(ExpressionResult(kind="literal", value=raw_arg))
        elif raw_arg.lower() in ("true", "false"):
            # C-34 (VAREX4-003): ADF Booleans (``true`` / ``false``) match
            # lowercase strings on the SetVariable consumer side (C-21).
            # Mark the literal so ``_arg_to_code`` emits ``'true'`` /
            # ``'false'`` strings rather than the bare Python ``True`` /
            # ``False`` (whose ``str()`` is title-case and never matches).
            resolved_args.append(
                ExpressionResult(
                    kind="literal",
                    value="true" if raw_arg.lower() == "true" else "false",
                    was_bool_literal=True,
                )
            )
        elif raw_arg.lower() == "null":
            resolved_args.append(ExpressionResult(kind="literal", value="None"))
        else:
            sub_expr = raw_arg if raw_arg.startswith("@") else "@" + raw_arg
            sub_result = resolve_expression(sub_expr, context, variable_task_keys=variable_task_keys)
            if sub_result is None:
                return None
            resolved_args.append(sub_result)

    handler_result = handler(resolved_args)
    # Auto-propagate required_parameters from args onto notebook_code results
    # so preparers can thread DAB refs into base_parameters even for handlers
    # that pre-date the required_parameters contract.
    if handler_result is not None and handler_result.kind == "notebook_code":
        extra_parameters = _collect_required_parameters(*resolved_args)
        if extra_parameters:
            merged = dict(extra_parameters)
            merged.update(handler_result.required_parameters)
            handler_result = ExpressionResult(
                kind=handler_result.kind,
                value=handler_result.value,
                imports=handler_result.imports,
                required_parameters=merged,
            )
    return handler_result


def _is_numeric(text: str) -> bool:
    """Check if a string is a numeric literal."""
    try:
        float(text)
        return True
    except ValueError:
        return False


def _arg_to_code(arg: ExpressionResult) -> str:
    """Converts a resolved argument to a Python code snippet.

    C-34 (VAREX4-002/003): quoted-string and Boolean-literal arguments
    must emit ``repr()`` of the value (e.g. ``'09'`` rather than the
    bare token ``09``) so the resulting code (a) parses (leading-zero
    integers are SyntaxErrors in modern Python) and (b) compares against
    the right concrete value (Booleans on the SetVariable consumer side
    serialise as lowercase strings, not Python bools).
    """
    if arg.kind == "literal":
        if arg.was_string_literal or arg.was_bool_literal:
            return repr(arg.value)
        if arg.value in ("True", "False", "None") or _is_numeric(arg.value):
            return arg.value
        return repr(arg.value)
    elif arg.kind == "dab_ref":
        return _dab_ref_to_widget_code(arg.value)
    elif arg.kind == "notebook_code":
        return arg.value
    return repr(arg.value)


def _datetime_arg_code(arg: ExpressionResult) -> str:
    """Return Python code that produces a ``datetime`` object from *arg*.

    Examples:
        utcNow()                                    -> ``datetime.now(timezone.utc)``
        '2024-01-01T00:00:00'                       -> ``datetime.fromisoformat('2024-01-01T00:00:00')``
        @pipeline().parameters.start_date           -> ``datetime.fromisoformat(dbutils.widgets.get('start_date'))``
    """
    if arg.kind == "notebook_code":
        # If the value already produces a datetime object that was just
        # converted to ISO via .isoformat(), drop the conversion.
        if arg.value.endswith(".isoformat()"):
            return arg.value[: -len(".isoformat()")]
        # If the value already ends in .strftime(...), the upstream caller
        # was using it as a string; we still need a datetime, so parse it.
    return f"datetime.fromisoformat({_arg_to_code(arg)})"


def _collect_imports(*args: ExpressionResult) -> list[str]:
    """Collects unique imports from resolved arguments."""
    imports: list[str] = []
    for arg in args:
        imports.extend(arg.imports)
    return list(dict.fromkeys(imports))


def _collect_required_parameters(*args: ExpressionResult) -> dict[str, str]:
    """Collects widget → DAB ref mappings across resolved arguments."""
    merged: dict[str, str] = {}
    for arg in args:
        merged.update(arg.required_parameters)
        if arg.kind == "dab_ref":
            widget_name, dab_ref = _required_parameter_for_ref(arg.value)
            merged.setdefault(widget_name, dab_ref)
    return merged


def _collect_notes(*args: ExpressionResult) -> list[str]:
    """Collects caveat notes across resolved arguments."""
    return [note for arg in args for note in arg.notes]


def _result_from_args(
    value: str,
    args: list[ExpressionResult],
    *,
    extra_imports: list[str] | None = None,
) -> ExpressionResult:
    """Builds a ``notebook_code`` result that propagates imports and widget refs.

    Args:
        value: The Python expression string for the result.
        args: Resolved arguments of the surrounding ADF function call.
        extra_imports: Imports the handler itself introduces (e.g.
            ``_DATETIME_IMPORTS`` or ``_ZONEINFO_IMPORTS``) in addition to
            those already declared by *args*.
    """
    imports = list(extra_imports or ()) + _collect_imports(*args)
    return ExpressionResult(
        kind="notebook_code",
        value=value,
        imports=imports,
        required_parameters=_collect_required_parameters(*args),
        notes=_collect_notes(*args),
    )


def _handle_concat(args: list[ExpressionResult]) -> ExpressionResult | None:
    """concat(a, b, ...) -> str(a) + str(b) + ...

    When every argument resolved to a ``literal`` kind, collapse the whole
    expression to a single literal so downstream consumers (cluster fields,
    library paths, notebook-install jar refs, etc.) get a plain string
    instead of Python source.
    """
    if not args:
        return None
    if all(a.kind == "literal" for a in args):
        return ExpressionResult(kind="literal", value="".join(a.value for a in args))
    parts = [f"str({_arg_to_code(a)})" for a in args]
    return _result_from_args(" + ".join(parts), args)


def _handle_ends_with(args: list[ExpressionResult]) -> ExpressionResult | None:
    """endsWith(text, search) -> str(text).endswith(str(search))"""
    if len(args) != 2:
        return None
    return _result_from_args(f"str({_arg_to_code(args[0])}).endswith(str({_arg_to_code(args[1])}))", args)


def _handle_guid(args: list[ExpressionResult]) -> ExpressionResult | None:
    """guid() -> str(uuid4()) or guid('N') -> no-dash variant."""
    if len(args) == 0:
        return ExpressionResult(
            kind="notebook_code",
            value="str(__import__('uuid').uuid4())",
        )
    if len(args) == 1 and args[0].kind == "literal" and args[0].value == "N":
        return ExpressionResult(
            kind="notebook_code",
            value="str(__import__('uuid').uuid4()).replace('-', '')",
        )
    return ExpressionResult(
        kind="notebook_code",
        value="str(__import__('uuid').uuid4())",
    )


def _handle_index_of(args: list[ExpressionResult]) -> ExpressionResult | None:
    """indexOf(text, search) -> str(text).lower().find(str(search).lower())"""
    if len(args) != 2:
        return None
    return _result_from_args(f"str({_arg_to_code(args[0])}).lower().find(str({_arg_to_code(args[1])}).lower())", args)


def _handle_last_index_of(args: list[ExpressionResult]) -> ExpressionResult | None:
    """lastIndexOf(text, search) -> str(text).lower().rfind(str(search).lower())"""
    if len(args) != 2:
        return None
    return _result_from_args(f"str({_arg_to_code(args[0])}).lower().rfind(str({_arg_to_code(args[1])}).lower())", args)


def _handle_replace(args: list[ExpressionResult]) -> ExpressionResult | None:
    """replace(text, old, new) -> str(text).replace(str(old), str(new))"""
    if len(args) != 3:
        return None
    return _result_from_args(
        f"str({_arg_to_code(args[0])}).replace(str({_arg_to_code(args[1])}), str({_arg_to_code(args[2])}))", args
    )


def _handle_split(args: list[ExpressionResult]) -> ExpressionResult | None:
    """split(text, delim) -> str(text).split(str(delim))"""
    if len(args) != 2:
        return None
    return _result_from_args(f"str({_arg_to_code(args[0])}).split(str({_arg_to_code(args[1])}))", args)


def _handle_starts_with(args: list[ExpressionResult]) -> ExpressionResult | None:
    """startsWith(text, search) -> str(text).lower().startswith(str(search).lower())"""
    if len(args) != 2:
        return None
    return _result_from_args(
        f"str({_arg_to_code(args[0])}).lower().startswith(str({_arg_to_code(args[1])}).lower())", args
    )


def _handle_substring(args: list[ExpressionResult]) -> ExpressionResult | None:
    """substring(text, start[, length]) -> Python slice.

    C-33 (VAREX4-001): ADF accepts the 2-arg form ``substring(x, start)``
    in addition to the documented 3-arg form.  Treat the 2-arg case as
    ``str(text)[int(start):]`` so SetVariable activities that wrap it can
    actually resolve.
    """
    if len(args) == 2:
        text = _arg_to_code(args[0])
        start = _arg_to_code(args[1])
        return _result_from_args(f"str({text})[int({start}):]", args)
    if len(args) != 3:
        return None
    text = _arg_to_code(args[0])
    start = _arg_to_code(args[1])
    length = _arg_to_code(args[2])
    return _result_from_args(f"str({text})[int({start}):int({start})+int({length})]", args)


def _handle_to_lower(args: list[ExpressionResult]) -> ExpressionResult | None:
    """toLower(text) -> str(text).lower()"""
    if len(args) != 1:
        return None
    return _result_from_args(f"str({_arg_to_code(args[0])}).lower()", args)


def _handle_to_upper(args: list[ExpressionResult]) -> ExpressionResult | None:
    """toUpper(text) -> str(text).upper()"""
    if len(args) != 1:
        return None
    return _result_from_args(f"str({_arg_to_code(args[0])}).upper()", args)


def _handle_trim(args: list[ExpressionResult]) -> ExpressionResult | None:
    """trim(text) -> str(text).strip()"""
    if len(args) != 1:
        return None
    return _result_from_args(f"str({_arg_to_code(args[0])}).strip()", args)


def _handle_contains(args: list[ExpressionResult]) -> ExpressionResult | None:
    """contains(collection, value) -> (value in collection)"""
    if len(args) != 2:
        return None
    return _result_from_args(f"({_arg_to_code(args[1])} in {_arg_to_code(args[0])})", args)


def _handle_empty(args: list[ExpressionResult]) -> ExpressionResult | None:
    """empty(collection) -> (len(collection) == 0)"""
    if len(args) != 1:
        return None
    return _result_from_args(f"(len({_arg_to_code(args[0])}) == 0)", args)


def _handle_first(args: list[ExpressionResult]) -> ExpressionResult | None:
    """first(collection) -> collection[0]"""
    if len(args) != 1:
        return None
    return _result_from_args(f"{_arg_to_code(args[0])}[0]", args)


def _handle_intersection(args: list[ExpressionResult]) -> ExpressionResult | None:
    """intersection(c1, c2, ...) -> list(set(c1) & set(c2) & ...)"""
    if len(args) < 2:
        return None
    parts = " & ".join(f"set({_arg_to_code(a)})" for a in args)
    return _result_from_args(f"list({parts})", args)


def _handle_join(args: list[ExpressionResult]) -> ExpressionResult | None:
    """join(array, delim) -> str(delim).join(str(x) for x in array)"""
    if len(args) != 2:
        return None
    return _result_from_args(f"str({_arg_to_code(args[1])}).join(str(x) for x in {_arg_to_code(args[0])})", args)


def _handle_last(args: list[ExpressionResult]) -> ExpressionResult | None:
    """last(collection) -> collection[-1]"""
    if len(args) != 1:
        return None
    return _result_from_args(f"{_arg_to_code(args[0])}[-1]", args)


def _handle_length(args: list[ExpressionResult]) -> ExpressionResult | None:
    """length(collection) -> len(collection)"""
    if len(args) != 1:
        return None
    return _result_from_args(f"len({_arg_to_code(args[0])})", args)


def _handle_skip(args: list[ExpressionResult]) -> ExpressionResult | None:
    """skip(collection, count) -> collection[int(count):]"""
    if len(args) != 2:
        return None
    return _result_from_args(f"{_arg_to_code(args[0])}[int({_arg_to_code(args[1])}):]", args)


def _handle_take(args: list[ExpressionResult]) -> ExpressionResult | None:
    """take(collection, count) -> collection[:int(count)]"""
    if len(args) != 2:
        return None
    return _result_from_args(f"{_arg_to_code(args[0])}[:int({_arg_to_code(args[1])})]", args)


def _handle_union(args: list[ExpressionResult]) -> ExpressionResult | None:
    """union(c1, c2, ...) -> list(set(c1) | set(c2) | ...)"""
    if len(args) < 2:
        return None
    parts = " | ".join(f"set({_arg_to_code(a)})" for a in args)
    return _result_from_args(f"list({parts})", args)


def _handle_and(args: list[ExpressionResult]) -> ExpressionResult | None:
    """and(a, b) -> (a and b)"""
    if len(args) != 2:
        return None
    return _result_from_args(f"({_arg_to_code(args[0])} and {_arg_to_code(args[1])})", args)


def _handle_equals(args: list[ExpressionResult]) -> ExpressionResult | None:
    """equals(a, b) -> (a == b)"""
    if len(args) != 2:
        return None
    return _result_from_args(f"({_arg_to_code(args[0])} == {_arg_to_code(args[1])})", args)


def _handle_greater(args: list[ExpressionResult]) -> ExpressionResult | None:
    """greater(a, b) -> (a > b)"""
    if len(args) != 2:
        return None
    return _result_from_args(f"({_arg_to_code(args[0])} > {_arg_to_code(args[1])})", args)


def _handle_greater_or_equals(args: list[ExpressionResult]) -> ExpressionResult | None:
    """greaterOrEquals(a, b) -> (a >= b)"""
    if len(args) != 2:
        return None
    return _result_from_args(f"({_arg_to_code(args[0])} >= {_arg_to_code(args[1])})", args)


def _handle_if(args: list[ExpressionResult]) -> ExpressionResult | None:
    """if(expr, trueVal, falseVal) -> (trueVal if expr else falseVal)"""
    if len(args) != 3:
        return None
    return _result_from_args(f"({_arg_to_code(args[1])} if {_arg_to_code(args[0])} else {_arg_to_code(args[2])})", args)


def _handle_less(args: list[ExpressionResult]) -> ExpressionResult | None:
    """less(a, b) -> (a < b)"""
    if len(args) != 2:
        return None
    return _result_from_args(f"({_arg_to_code(args[0])} < {_arg_to_code(args[1])})", args)


def _handle_less_or_equals(args: list[ExpressionResult]) -> ExpressionResult | None:
    """lessOrEquals(a, b) -> (a <= b)"""
    if len(args) != 2:
        return None
    return _result_from_args(f"({_arg_to_code(args[0])} <= {_arg_to_code(args[1])})", args)


def _handle_not(args: list[ExpressionResult]) -> ExpressionResult | None:
    """not(expr) -> (not expr)"""
    if len(args) != 1:
        return None
    return _result_from_args(f"(not {_arg_to_code(args[0])})", args)


def _handle_or(args: list[ExpressionResult]) -> ExpressionResult | None:
    """or(a, b) -> (a or b)"""
    if len(args) != 2:
        return None
    return _result_from_args(f"({_arg_to_code(args[0])} or {_arg_to_code(args[1])})", args)


def _handle_array(args: list[ExpressionResult]) -> ExpressionResult | None:
    """array(value) -> [value]"""
    if len(args) != 1:
        return None
    return _result_from_args(f"[{_arg_to_code(args[0])}]", args)


def _handle_base64(args: list[ExpressionResult]) -> ExpressionResult | None:
    """base64(value) -> base64.b64encode(str(value).encode()).decode()"""
    if len(args) != 1:
        return None
    return _result_from_args(f"__import__('base64').b64encode(str({_arg_to_code(args[0])}).encode()).decode()", args)


def _handle_base64_to_binary(args: list[ExpressionResult]) -> ExpressionResult | None:
    """base64ToBinary(value) -> base64.b64decode(value)"""
    if len(args) != 1:
        return None
    return _result_from_args(f"__import__('base64').b64decode({_arg_to_code(args[0])})", args)


def _handle_base64_to_string(args: list[ExpressionResult]) -> ExpressionResult | None:
    """base64ToString(value) -> base64.b64decode(value).decode()"""
    if len(args) != 1:
        return None
    return _result_from_args(f"__import__('base64').b64decode({_arg_to_code(args[0])}).decode()", args)


def _handle_binary(args: list[ExpressionResult]) -> ExpressionResult | None:
    """binary(value) -> str(value).encode()"""
    if len(args) != 1:
        return None
    return _result_from_args(f"str({_arg_to_code(args[0])}).encode()", args)


def _handle_bool(args: list[ExpressionResult]) -> ExpressionResult | None:
    """bool(value) -> bool(value)"""
    if len(args) != 1:
        return None
    return _result_from_args(f"bool({_arg_to_code(args[0])})", args)


def _handle_coalesce(args: list[ExpressionResult]) -> ExpressionResult | None:
    """coalesce(a, b, ...) -> next((x for x in [a, b, ...] if x is not None), None)"""
    if not args:
        return None
    items = ", ".join(_arg_to_code(a) for a in args)
    return _result_from_args(f"next((x for x in [{items}] if x is not None), None)", args)


def _handle_create_array(args: list[ExpressionResult]) -> ExpressionResult | None:
    """createArray(a, b, ...) -> [a, b, ...]"""
    items = ", ".join(_arg_to_code(a) for a in args)
    return _result_from_args(f"[{items}]", args)


def _handle_agentic(_args: list[ExpressionResult]) -> ExpressionResult | None:
    """Return None for agentic functions that are too complex for deterministic translation."""
    return None


def _handle_decode_uri_component(args: list[ExpressionResult]) -> ExpressionResult | None:
    """decodeUriComponent(value) -> urllib.parse.unquote(value)"""
    if len(args) != 1:
        return None
    return _result_from_args(f"__import__('urllib.parse', fromlist=['unquote']).unquote({_arg_to_code(args[0])})", args)


def _handle_encode_uri_component(args: list[ExpressionResult]) -> ExpressionResult | None:
    """encodeUriComponent(value) -> urllib.parse.quote(str(value), safe='')"""
    if len(args) != 1:
        return None
    return _result_from_args(
        f"__import__('urllib.parse', fromlist=['quote']).quote(str({_arg_to_code(args[0])}), safe='')", args
    )


def _handle_float(args: list[ExpressionResult]) -> ExpressionResult | None:
    """float(value) -> float(value)"""
    if len(args) != 1:
        return None
    return _result_from_args(f"float({_arg_to_code(args[0])})", args)


def _handle_int(args: list[ExpressionResult]) -> ExpressionResult | None:
    """int(value) -> int(value)"""
    if len(args) != 1:
        return None
    return _result_from_args(f"int({_arg_to_code(args[0])})", args)


def _handle_json(args: list[ExpressionResult]) -> ExpressionResult | None:
    """json(value) -> json.loads(value)"""
    if len(args) != 1:
        return None
    return _result_from_args(f"__import__('json').loads({_arg_to_code(args[0])})", args)


def _handle_string(args: list[ExpressionResult]) -> ExpressionResult | None:
    """string(value) -> str(value)."""
    if len(args) != 1:
        return None
    sole_arg = args[0]
    if sole_arg.kind == "dab_ref":
        return sole_arg
    return _result_from_args(f"str({_arg_to_code(sole_arg)})", args)


def _handle_add(args: list[ExpressionResult]) -> ExpressionResult | None:
    """add(a, b) -> (a + b)"""
    if len(args) != 2:
        return None
    return _result_from_args(f"({_arg_to_code(args[0])} + {_arg_to_code(args[1])})", args)


def _handle_div(args: list[ExpressionResult]) -> ExpressionResult | None:
    """div(a, b) -> (a // b)"""
    if len(args) != 2:
        return None
    return _result_from_args(f"({_arg_to_code(args[0])} // {_arg_to_code(args[1])})", args)


def _handle_max(args: list[ExpressionResult]) -> ExpressionResult | None:
    """max(a, b, ...) -> max(a, b, ...)"""
    if not args:
        return None
    items = ", ".join(_arg_to_code(a) for a in args)
    return _result_from_args(f"max({items})", args)


def _handle_min(args: list[ExpressionResult]) -> ExpressionResult | None:
    """min(a, b, ...) -> min(a, b, ...)"""
    if not args:
        return None
    items = ", ".join(_arg_to_code(a) for a in args)
    return _result_from_args(f"min({items})", args)


def _handle_mod(args: list[ExpressionResult]) -> ExpressionResult | None:
    """mod(a, b) -> (a % b)"""
    if len(args) != 2:
        return None
    return _result_from_args(f"({_arg_to_code(args[0])} % {_arg_to_code(args[1])})", args)


def _handle_mul(args: list[ExpressionResult]) -> ExpressionResult | None:
    """mul(a, b) -> (a * b)"""
    if len(args) != 2:
        return None
    return _result_from_args(f"({_arg_to_code(args[0])} * {_arg_to_code(args[1])})", args)


def _handle_rand(args: list[ExpressionResult]) -> ExpressionResult | None:
    """rand(min, max) -> random.randint(min, max-1)"""
    if len(args) != 2:
        return None
    return _result_from_args(
        f"__import__('random').randint({_arg_to_code(args[0])}, {_arg_to_code(args[1])} - 1)", args
    )


def _handle_range(args: list[ExpressionResult]) -> ExpressionResult | None:
    """range(start, count) -> list(range(start, start + count))"""
    if len(args) != 2:
        return None
    start = _arg_to_code(args[0])
    count = _arg_to_code(args[1])
    return _result_from_args(f"list(range({start}, {start} + {count}))", args)


def _handle_sub(args: list[ExpressionResult]) -> ExpressionResult | None:
    """sub(a, b) -> (a - b)"""
    if len(args) != 2:
        return None
    return _result_from_args(f"({_arg_to_code(args[0])} - {_arg_to_code(args[1])})", args)


def _make_add_unit_handler(
    timedelta_keyword: str,
) -> Callable[[list[ExpressionResult]], ExpressionResult | None]:
    """Builds a handler for ``addDays`` / ``addHours`` / ``addMinutes`` / ``addSeconds``."""

    def handler(args: list[ExpressionResult]) -> ExpressionResult | None:
        if len(args) < 2 or len(args) > 3:
            return None
        timestamp_dt = _datetime_arg_code(args[0])
        amount = _arg_to_code(args[1])
        format_string = _get_format_arg(args, 2)
        return _result_from_args(
            f"({timestamp_dt} + timedelta({timedelta_keyword}={amount})).strftime({format_string})",
            args,
            extra_imports=_DATETIME_IMPORTS,
        )

    return handler


_handle_add_days = _make_add_unit_handler("days")
_handle_add_hours = _make_add_unit_handler("hours")
_handle_add_minutes = _make_add_unit_handler("minutes")
_handle_add_seconds = _make_add_unit_handler("seconds")


def _handle_add_to_time(args: list[ExpressionResult]) -> ExpressionResult | None:
    """addToTime(ts, interval, unit, fmt?) -> datetime + timedelta."""
    if len(args) < 3 or len(args) > 4:
        return None
    timestamp_dt = _datetime_arg_code(args[0])
    interval = _arg_to_code(args[1])
    unit_str = args[2].value if args[2].kind == "literal" else None
    if unit_str is None:
        return None
    timedelta_keyword = _TIME_UNIT_MAP.get(unit_str)
    if timedelta_keyword is None:
        return None
    format_string = _get_format_arg(args, 3)
    return _result_from_args(
        (f"({timestamp_dt} + timedelta({timedelta_keyword}={interval})).strftime({format_string})"),
        args,
        extra_imports=_DATETIME_IMPORTS,
    )


def _handle_day_of_month(args: list[ExpressionResult]) -> ExpressionResult | None:
    """dayOfMonth(ts) -> <datetime>.day"""
    if len(args) != 1:
        return None
    return _result_from_args(f"{_datetime_arg_code(args[0])}.day", args, extra_imports=_DATETIME_IMPORTS)


def _handle_day_of_week(args: list[ExpressionResult]) -> ExpressionResult | None:
    """dayOfWeek(ts) -> <datetime>.isoweekday() % 7 (ADF: 0=Sunday)"""
    if len(args) != 1:
        return None
    return _result_from_args(f"{_datetime_arg_code(args[0])}.isoweekday() % 7", args, extra_imports=_DATETIME_IMPORTS)


def _handle_day_of_year(args: list[ExpressionResult]) -> ExpressionResult | None:
    """dayOfYear(ts) -> <datetime>.timetuple().tm_yday"""
    if len(args) != 1:
        return None
    return _result_from_args(
        f"{_datetime_arg_code(args[0])}.timetuple().tm_yday", args, extra_imports=_DATETIME_IMPORTS
    )


def _handle_format_date_time(args: list[ExpressionResult]) -> ExpressionResult | None:
    """formatDateTime(ts, fmt?) -> datetime.fromisoformat(ts).strftime(converted_fmt)"""
    if len(args) < 1 or len(args) > 2:
        return None
    # formatDateTime(utcnow(), '<known iso fmt>') -> remap straight to the
    # matching Databricks dynamic value so the result lands in YAML.
    if (
        len(args) == 2
        and args[0].kind == "dab_ref"
        and args[0].value == "{{job.start_time.iso_datetime}}"
        and _UTCNOW_APPROXIMATION_NOTE in args[0].notes
        and args[1].kind == "literal"
    ):
        dab_ref = _UTCNOW_FORMAT_TO_DAB_REF.get(args[1].value)
        if dab_ref:
            return ExpressionResult(kind="dab_ref", value=dab_ref, notes=[_UTCNOW_APPROXIMATION_NOTE])
    timestamp_dt = _datetime_arg_code(args[0])
    if len(args) == 2 and args[1].kind == "literal":
        python_format = _convert_date_format(args[1].value)
        return _result_from_args(f"{timestamp_dt}.strftime('{python_format}')", args, extra_imports=_DATETIME_IMPORTS)
    return _result_from_args(f"{timestamp_dt}.isoformat()", args, extra_imports=_DATETIME_IMPORTS)


def _make_now_offset_handler(
    operator: str,
) -> Callable[[list[ExpressionResult]], ExpressionResult | None]:
    """Builds a ``getFutureTime`` / ``getPastTime`` handler."""

    def handler(args: list[ExpressionResult]) -> ExpressionResult | None:
        if len(args) < 2 or len(args) > 3:
            return None
        interval = _arg_to_code(args[0])
        unit_str = args[1].value if args[1].kind == "literal" else None
        if unit_str is None:
            return None
        timedelta_keyword = _TIME_UNIT_MAP.get(unit_str)
        if timedelta_keyword is None:
            return None
        format_string = _get_format_arg(args, 2)
        return ExpressionResult(
            kind="notebook_code",
            value=(
                f"(datetime.now(timezone.utc) {operator} "
                f"timedelta({timedelta_keyword}={interval})).strftime({format_string})"
            ),
            imports=_DATETIME_IMPORTS + _collect_imports(*args),
        )

    return handler


_handle_get_future_time = _make_now_offset_handler("+")
_handle_get_past_time = _make_now_offset_handler("-")


def _handle_start_of_day(args: list[ExpressionResult]) -> ExpressionResult | None:
    """startOfDay(ts, fmt?) -> datetime.fromisoformat(ts).replace(hour=0,...).strftime(fmt)"""
    if len(args) < 1 or len(args) > 2:
        return None
    timestamp_dt = _datetime_arg_code(args[0])
    format_string = _get_format_arg(args, 1)
    return _result_from_args(
        (f"{timestamp_dt}.replace(hour=0, minute=0, second=0, microsecond=0).strftime({format_string})"),
        args,
        extra_imports=_DATETIME_IMPORTS,
    )


def _handle_start_of_hour(args: list[ExpressionResult]) -> ExpressionResult | None:
    """startOfHour(ts, fmt?) -> datetime.fromisoformat(ts).replace(minute=0,...).strftime(fmt)"""
    if len(args) < 1 or len(args) > 2:
        return None
    timestamp_dt = _datetime_arg_code(args[0])
    format_string = _get_format_arg(args, 1)
    return _result_from_args(
        (f"{timestamp_dt}.replace(minute=0, second=0, microsecond=0).strftime({format_string})"),
        args,
        extra_imports=_DATETIME_IMPORTS,
    )


def _handle_start_of_month(args: list[ExpressionResult]) -> ExpressionResult | None:
    """startOfMonth(ts, fmt?) -> datetime.fromisoformat(ts).replace(day=1,...).strftime(fmt)"""
    if len(args) < 1 or len(args) > 2:
        return None
    timestamp_dt = _datetime_arg_code(args[0])
    format_string = _get_format_arg(args, 1)
    return _result_from_args(
        (f"{timestamp_dt}.replace(day=1, hour=0, minute=0, second=0, microsecond=0).strftime({format_string})"),
        args,
        extra_imports=_DATETIME_IMPORTS,
    )


def _handle_subtract_from_time(args: list[ExpressionResult]) -> ExpressionResult | None:
    """subtractFromTime(ts, interval, unit, fmt?) -> datetime - timedelta."""
    if len(args) < 3 or len(args) > 4:
        return None
    timestamp_dt = _datetime_arg_code(args[0])
    interval = _arg_to_code(args[1])
    unit_str = args[2].value if args[2].kind == "literal" else None
    if unit_str is None:
        return None
    timedelta_keyword = _TIME_UNIT_MAP.get(unit_str)
    if timedelta_keyword is None:
        return None
    format_string = _get_format_arg(args, 3)
    return _result_from_args(
        (f"({timestamp_dt} - timedelta({timedelta_keyword}={interval})).strftime({format_string})"),
        args,
        extra_imports=_DATETIME_IMPORTS,
    )


def _get_format_arg(args: list[ExpressionResult], idx: int) -> str:
    """Extracts a format argument from the args list, converting ADF .NET format if needed."""
    if idx < len(args) and args[idx].kind == "literal" and args[idx].value:
        python_format = _convert_date_format(args[idx].value)
        return repr(python_format)
    return "'%Y-%m-%dT%H:%M:%SZ'"


_ZONEINFO_IMPORTS = ["from datetime import datetime, timezone", "from zoneinfo import ZoneInfo"]


def _handle_convert_from_utc(args: list[ExpressionResult]) -> ExpressionResult | None:
    """convertFromUtc(timestamp, destinationTimeZone, fmt?)"""
    if len(args) < 2 or len(args) > 3:
        return None
    src = _datetime_arg_code(args[0])
    dest_tz = _arg_to_code(args[1])
    fmt = _get_format_arg(args, 2)
    return _result_from_args(
        f"{src}.replace(tzinfo=timezone.utc).astimezone(ZoneInfo({dest_tz})).strftime({fmt})",
        args,
        extra_imports=_ZONEINFO_IMPORTS,
    )


def _handle_convert_to_utc(args: list[ExpressionResult]) -> ExpressionResult | None:
    """convertToUtc(timestamp, sourceTimeZone, fmt?) -> UTC datetime."""
    if len(args) < 2 or len(args) > 3:
        return None
    src = _datetime_arg_code(args[0])
    src_tz = _arg_to_code(args[1])
    fmt = _get_format_arg(args, 2)
    return _result_from_args(
        f"{src}.replace(tzinfo=ZoneInfo({src_tz})).astimezone(timezone.utc).strftime({fmt})",
        args,
        extra_imports=_ZONEINFO_IMPORTS,
    )


def _handle_convert_time_zone(args: list[ExpressionResult]) -> ExpressionResult | None:
    """convertTimeZone(timestamp, sourceTimeZone, destinationTimeZone, fmt?)."""
    if len(args) < 3 or len(args) > 4:
        return None
    src = _datetime_arg_code(args[0])
    src_tz = _arg_to_code(args[1])
    dst_tz = _arg_to_code(args[2])
    fmt = _get_format_arg(args, 3)
    return _result_from_args(
        (f"{src}.replace(tzinfo=ZoneInfo({src_tz})).astimezone(ZoneInfo({dst_tz})).strftime({fmt})"),
        args,
        extra_imports=_ZONEINFO_IMPORTS,
    )


def _handle_ticks(args: list[ExpressionResult]) -> ExpressionResult | None:
    """ticks(timestamp) -> .NET FILETIME ticks (100-ns intervals since 0001-01-01)."""
    if len(args) != 1:
        return None
    src = _datetime_arg_code(args[0])
    return _result_from_args(
        (f"int(({src} - datetime(1, 1, 1, tzinfo=timezone.utc)).total_seconds() * 10_000_000)"),
        args,
        extra_imports=_DATETIME_IMPORTS,
    )


_FUNCTION_HANDLERS: dict[str, Callable[[list[ExpressionResult]], ExpressionResult | None]] = {
    "concat": _handle_concat,
    "endsWith": _handle_ends_with,
    "guid": _handle_guid,
    "indexOf": _handle_index_of,
    "lastIndexOf": _handle_last_index_of,
    "replace": _handle_replace,
    "split": _handle_split,
    "startsWith": _handle_starts_with,
    "substring": _handle_substring,
    "toLower": _handle_to_lower,
    "toUpper": _handle_to_upper,
    "trim": _handle_trim,
    "contains": _handle_contains,
    "empty": _handle_empty,
    "first": _handle_first,
    "intersection": _handle_intersection,
    "join": _handle_join,
    "last": _handle_last,
    "length": _handle_length,
    "skip": _handle_skip,
    "take": _handle_take,
    "union": _handle_union,
    "and": _handle_and,
    "equals": _handle_equals,
    "greater": _handle_greater,
    "greaterOrEquals": _handle_greater_or_equals,
    "if": _handle_if,
    "less": _handle_less,
    "lessOrEquals": _handle_less_or_equals,
    "not": _handle_not,
    "or": _handle_or,
    "array": _handle_array,
    "base64": _handle_base64,
    "base64ToBinary": _handle_base64_to_binary,
    "base64ToString": _handle_base64_to_string,
    "binary": _handle_binary,
    "bool": _handle_bool,
    "coalesce": _handle_coalesce,
    "createArray": _handle_create_array,
    "dataUri": _handle_agentic,
    "dataUriToBinary": _handle_agentic,
    "dataUriToString": _handle_agentic,
    "decodeBase64": _handle_base64_to_string,
    "decodeDataUri": _handle_agentic,
    "decodeUriComponent": _handle_decode_uri_component,
    "encodeUriComponent": _handle_encode_uri_component,
    "float": _handle_float,
    "int": _handle_int,
    "json": _handle_json,
    "string": _handle_string,
    "uriComponent": _handle_encode_uri_component,
    "uriComponentToBinary": _handle_agentic,
    "uriComponentToString": _handle_decode_uri_component,
    "xml": _handle_agentic,
    "xpath": _handle_agentic,
    "add": _handle_add,
    "div": _handle_div,
    "max": _handle_max,
    "min": _handle_min,
    "mod": _handle_mod,
    "mul": _handle_mul,
    "rand": _handle_rand,
    "range": _handle_range,
    "sub": _handle_sub,
    "addDays": _handle_add_days,
    "addHours": _handle_add_hours,
    "addMinutes": _handle_add_minutes,
    "addSeconds": _handle_add_seconds,
    "addToTime": _handle_add_to_time,
    "convertFromUtc": _handle_convert_from_utc,
    "convertTimeZone": _handle_convert_time_zone,
    "convertToUtc": _handle_convert_to_utc,
    "dayOfMonth": _handle_day_of_month,
    "dayOfWeek": _handle_day_of_week,
    "dayOfYear": _handle_day_of_year,
    "formatDateTime": _handle_format_date_time,
    "getFutureTime": _handle_get_future_time,
    "getPastTime": _handle_get_past_time,
    "startOfDay": _handle_start_of_day,
    "startOfHour": _handle_start_of_hour,
    "startOfMonth": _handle_start_of_month,
    "subtractFromTime": _handle_subtract_from_time,
    "ticks": _handle_ticks,
    # utcNow is intentionally absent -- handled by _resolve_utcnow upstream
    # in resolve_expression() before the generic dispatch is reached.
}

_FUNCTION_HANDLERS_CI: dict[str, Callable[[list[ExpressionResult]], ExpressionResult | None]] = {
    k.lower(): v for k, v in _FUNCTION_HANDLERS.items() if v is not None
}
