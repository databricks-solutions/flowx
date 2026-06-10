"""Translates ADF IfCondition activities to Databricks IfConditionActivity IR.

References:
- https://docs.databricks.com/aws/en/jobs/conditional-tasks
"""

from __future__ import annotations

import re
from typing import Any

from orchestra.models.adf_ast import AdfActivity, AdfDefinitions
from orchestra.models.ir import Activity, IfConditionActivity, TranslationContext
from orchestra.translator.activity_translators.resolve import (
    BridgeRequest,
    lower_to_bridge,
    merge_bridge_requests,
)

# ---------------------------------------------------------------------------
# ADF comparison function -> Databricks condition_task op mapping
# ---------------------------------------------------------------------------

_OP_MAP: dict[str, str] = {
    "equals": "EQUAL_TO",
    "greater": "GREATER_THAN",
    "greaterorequals": "GREATER_THAN_OR_EQUAL",
    "less": "LESS_THAN",
    "lessorequals": "LESS_THAN_OR_EQUAL",
    "not": "NOT_EQUAL",
}

_COMPARISON_RE = re.compile(
    r"(equals|greater|greaterOrEquals|less|lessOrEquals|not)\s*\((.+)\)",
    re.IGNORECASE | re.DOTALL,
)

_NOT_COMPARISON_RE = re.compile(
    r"not\s*\(\s*(equals|greater|greaterOrEquals|less|lessOrEquals)\s*\((.+)\)\s*\)",
    re.IGNORECASE | re.DOTALL,
)

_NEGATE_OP_MAP: dict[str, str] = {
    "equals": "NOT_EQUAL",
    "greater": "LESS_THAN_OR_EQUAL",
    "greaterorequals": "LESS_THAN",
    "less": "GREATER_THAN_OR_EQUAL",
    "lessorequals": "GREATER_THAN",
}


def translate(
    activity: AdfActivity,
    base_kwargs: dict[str, Any],
    context: TranslationContext,
    definitions: AdfDefinitions,
    *,
    translate_activities_fn: Any = None,
) -> tuple[Activity, TranslationContext]:
    """Translates an IfCondition activity with recursive branch translation.

    Args:
        activity: The ADF activity AST node.
        base_kwargs: Common fields (name, task_key, timeout, retries, depends_on, cluster).
        context: Current translation context.
        definitions: Full ADF definitions for cross-referencing.
        translate_activities_fn: Callback to translate branch activities.
            Signature: ``(activities, context, definitions) -> (list[Activity], TranslationContext)``.

    Returns:
        Tuple of ``(IfConditionActivity, updated_context)``.
    """
    type_properties = activity.type_properties or {}

    expression_raw = type_properties.get("expression", {})
    op, left, right, bridge = _parse_condition(expression_raw, context)

    if_true_activities: list[Activity] = []
    if_true_adf = activity.if_true_activities or []
    if translate_activities_fn and if_true_adf:
        if_true_activities, _ = translate_activities_fn(if_true_adf, context, definitions)

    if_false_activities: list[Activity] = []
    if_false_adf = activity.if_false_activities or []
    if translate_activities_fn and if_false_adf:
        if_false_activities, _ = translate_activities_fn(if_false_adf, context, definitions)

    bridge_kwargs: dict[str, Any] = {}
    if bridge is not None:
        bridge_kwargs = {
            "bridge_notebook_code": bridge.notebook_code,
            "bridge_notebook_imports": list(bridge.notebook_imports),
            "bridge_required_parameters": dict(bridge.required_parameters),
        }

    if_activity = IfConditionActivity(
        **base_kwargs,
        op=op,
        left=left,
        right=right,
        if_true_activities=if_true_activities,
        if_false_activities=if_false_activities,
        **bridge_kwargs,
    )

    return if_activity, context


_BRIDGE_TASK_VALUE_KEY = "result"


def _parse_condition(
    expression: dict[str, Any] | str, context: TranslationContext
) -> tuple[str, str, str, BridgeRequest | None]:
    """Parses an ADF IfCondition expression into ``(op, left, right, bridge)``.

    C-07 (CF-iter2-001 / CF-iter2-003 / VAREX-003): when an operand
    resolves to ``notebook_code`` (e.g. ``@empty(X)``, ``@toUpper(...)``),
    we package the code into a :class:`BridgeRequest` so the preparer can
    emit a hidden SetVariable task whose value drives the
    condition_task.  The condition operand becomes the bridge's task
    value reference.

    Args:
        expression: Raw ADF expression dict or string.
        context: Translation context for resolving variables.

    Returns:
        ``(databricks_op, left_operand, right_operand, bridge_request)``
        where ``bridge_request`` is non-None when the condition required
        lowering to a notebook task.
    """
    expr_str = ""
    if isinstance(expression, dict):
        expr_str = expression.get("value", "")
    elif isinstance(expression, str):
        expr_str = expression

    if expr_str.startswith("@"):
        expr_str = expr_str[1:]

    m_not = _NOT_COMPARISON_RE.match(expr_str.strip())
    if m_not:
        inner_op_name = m_not.group(1).lower()
        op = _NEGATE_OP_MAP.get(inner_op_name, "NOT_EQUAL")
        args = _split_args(m_not.group(2).strip())
        left, left_bridge = _resolve_operand(args[0], context) if len(args) > 0 else ("", None)
        right, right_bridge = _resolve_operand(args[1], context) if len(args) > 1 else ("", None)
        return op, left, right, merge_bridge_requests(left_bridge, right_bridge)

    m = _COMPARISON_RE.match(expr_str.strip())
    if m:
        adf_op = m.group(1).lower()
        op = _OP_MAP.get(adf_op, adf_op.upper())

        if adf_op == "not":
            inner = m.group(2).strip()
            resolved, bridge = _resolve_operand(inner, context)
            # C-15 (CF3-003 / VAREX3-004): when the operand bridges to a
            # Python bool task value, compare against 'False' (not '') so
            # the IfCondition can actually evaluate to FALSE.
            right_operand = "False" if bridge is not None else ""
            return "NOT_EQUAL", resolved, right_operand, bridge

        args = _split_args(m.group(2).strip())
        left, left_bridge = _resolve_operand(args[0], context) if len(args) > 0 else ("", None)
        right, right_bridge = _resolve_operand(args[1], context) if len(args) > 1 else ("", None)
        return op, left, right, merge_bridge_requests(left_bridge, right_bridge)

    # Fallback: treat the whole expression as a truthy check.  C-07: route
    # through the bridge path when the expression is an ADF function call
    # so the operand ends up as a real task-value reference rather than
    # the legacy NOT_EQUAL '0' against a raw expression string.
    resolved, bridge = _resolve_operand(expr_str, context)
    if bridge is not None:
        return "NOT_EQUAL", _bridge_task_value_placeholder(), "False", bridge
    # C-15 (CF3-003 / VAREX3-004): when the truthy operand resolves to a
    # task-value ref backed by a SetVariable that writes a Python bool
    # (e.g. a previously-cached @variables('continue') with bridge-set
    # value), compare against 'False' so the legacy truthy path doesn't
    # silently invert behaviour.  Detected by the presence of a
    # __BRIDGE__:: placeholder or the lowercase 'true'/'false' literal
    # body of the upstream SetVariable.
    if isinstance(resolved, str) and "__BRIDGE__" in resolved:
        return "NOT_EQUAL", resolved, "False", None
    # C-43 (CF5-001 / LSC5-001): when the operand is a known-Boolean
    # variable that resolves to a parent-job task-value ref
    # (``{{tasks._init_X.values.X}}``), prefer recomputing the boolean
    # locally via a BridgeRequest, mirroring the Switch path.  Without this
    # an inner-ForEach IfCondition references a task that lives only in the
    # parent job; the bundler then blanks the operand to '' and
    # NOT_EQUAL('', '0') is always TRUE, running the true branch
    # unconditionally with no SETUP.md signal.  The bridge keeps the
    # operand local so it survives the dangling-ref safety net.
    if _operand_is_known_boolean(expr_str, context):
        bridge = _boolean_variable_bridge(expr_str, resolved, context)
        if bridge is not None:
            return "NOT_EQUAL", _bridge_task_value_placeholder(), "False", bridge
        # C-32 (CF4-002): compare against lowercase ``'false'`` (matching
        # C-21 SetVariable rendering) instead of the legacy ``'0'`` — the
        # latter is always true for a Boolean-string operand so the false
        # branch becomes dead code.
        return "NOT_EQUAL", resolved, "false", None
    return "NOT_EQUAL", resolved, "0", None


def _boolean_variable_bridge(expr: str, resolved: str, context: TranslationContext) -> BridgeRequest | None:
    """Builds a local-recompute BridgeRequest for a Boolean-variable operand.

    C-43 (CF5-001): the bridge re-derives the boolean inside whatever job
    the IfCondition lands in (parent or split-out inner ForEach job), so
    the condition operand is a *local* task value rather than a parent-job
    ref the bundler would blank.  The recomputed value is the variable's
    seeded literal default (``true``/``false``); when no literal default is
    cached the caller falls back to the in-place ``'false'`` comparison.

    Returns ``None`` when there is no literal default to recompute from
    (e.g. the variable is set dynamically), leaving the legacy path intact.
    """
    expr = expr.strip()
    if expr.startswith("@"):
        expr = expr[1:]
    var_match = re.match(r"variables\(\s*'([^']+)'\s*\)\s*$", expr, re.IGNORECASE)
    if not var_match:
        return None
    var_name = var_match.group(1)
    literal = context.get_variable_default_literal(var_name)
    if literal is None or literal.lower() not in ("true", "false"):
        return None
    python_bool = "True" if literal.lower() == "true" else "False"
    return BridgeRequest(notebook_code=python_bool)


def _operand_is_known_boolean(expr: str, context: TranslationContext) -> bool:
    """Returns True when *expr* references a Boolean-typed variable / parameter.

    Inspects ``context.variable_value_cache`` (populated by C-05 init
    SetVariable activities with lowercase ``'true'/'false'`` defaults), the
    declared ``context.variable_types`` map (C-41), and bare
    ``@pipeline().parameters.<name>`` references when the context carries
    Boolean type hints.  When the type is known to be Boolean we return
    True so the IfCondition fallback uses ``'false'`` as the right operand.
    """
    expr = expr.strip()
    if expr.startswith("@"):
        expr = expr[1:]
    # @variables('X') -> look up the cached lowercase value
    var_match = re.match(r"variables\(\s*'([^']+)'\s*\)\s*$", expr, re.IGNORECASE)
    if var_match:
        var_name = var_match.group(1)
        cached = context.get_variable_dab_ref(var_name)
        if isinstance(cached, str) and cached.lower() in ("true", "false"):
            return True
        # C-41 (CF5-001): a Boolean variable seeded only by a literal
        # default init task never populates variable_value_cache as a
        # dab_ref, so fall back to its declared ADF type.
        declared = context.get_variable_type(var_name)
        if isinstance(declared, str) and declared.lower() in ("boolean", "bool"):
            return True
    # @pipeline().parameters.X -- without parameter type metadata we
    # cannot prove Booleanness; return False conservatively.
    return False


def _bridge_task_value_placeholder() -> str:
    """Sentinel left-operand the preparer rewrites to the bridge task value."""
    return f"__BRIDGE__::{_BRIDGE_TASK_VALUE_KEY}"


def _resolve_operand(operand: str, context: TranslationContext) -> tuple[str, BridgeRequest | None]:
    """Converts an ADF expression operand to a Databricks task value reference
    or a :class:`BridgeRequest` when the operand requires a bridge task.

    Args:
        operand: A single operand string from the parsed condition.
        context: Translation context for resolving variables.

    Returns:
        ``(operand, bridge_request)`` where ``bridge_request`` is None for
        literals / DAB refs.
    """
    operand = operand.strip()

    if operand.lower() == "null":
        return "", None

    if operand.startswith("'") and operand.endswith("'"):
        return operand[1:-1], None

    if operand.lstrip("-").replace(".", "", 1).isdigit():
        return operand, None

    inner = _unwrap_functions(operand)

    sub_expr = inner if inner.startswith("@") else "@" + inner
    operand_value, bridge = lower_to_bridge(sub_expr, context)
    if operand_value is not None:
        return operand_value, None
    if bridge is not None:
        return _bridge_task_value_placeholder(), bridge

    if inner != operand:
        sub_expr_full = operand if operand.startswith("@") else "@" + operand
        operand_value, bridge = lower_to_bridge(sub_expr_full, context)
        if operand_value is not None:
            return operand_value, None
        if bridge is not None:
            return _bridge_task_value_placeholder(), bridge

    return operand, None


def _unwrap_functions(expr: str) -> str:
    """Strips wrapping ADF functions like ``int(...)`` to expose the inner expression.

    Args:
        expr: Expression that may be wrapped in a type-casting function.

    Returns:
        The inner expression, or the original if no wrapping detected.
    """
    m = re.match(r"(?:int|string|float|bool)\s*\((.+)\)\s*$", expr, re.IGNORECASE)
    if m:
        return m.group(1).strip()
    return expr


def _split_args(args_str: str) -> list[str]:
    """Splits function arguments respecting nested parentheses and quotes.

    Args:
        args_str: Comma-separated argument string.

    Returns:
        List of argument strings, stripped of leading/trailing whitespace.
    """
    parts: list[str] = []
    depth = 0
    current: list[str] = []
    in_quote = False

    for ch in args_str:
        if ch == "'" and depth == 0:
            in_quote = not in_quote
            current.append(ch)
        elif in_quote:
            current.append(ch)
        elif ch == "(":
            depth += 1
            current.append(ch)
        elif ch == ")":
            depth -= 1
            current.append(ch)
        elif ch == "," and depth == 0:
            parts.append("".join(current).strip())
            current = []
        else:
            current.append(ch)

    if current:
        parts.append("".join(current).strip())

    return parts
