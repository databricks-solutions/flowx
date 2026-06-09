"""Shared field resolution helper for activity translators."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from orchestra.models.ir import ExpressionResult, TranslationContext
from orchestra.parser.expression_parser import resolve_expression, resolve_interpolated_string


@dataclass(slots=True)
class BridgeRequest:
    """Carrier for a notebook_code expression that must run as a bridge task.

    C-07 (CF-iter2-001 / CF-iter2-003 / VAREX-003): when an operand of an
    IfCondition / Switch condition_task resolves to ``notebook_code`` (e.g.
    ``@empty(...)``, ``@toUpper(coalesce(...))``), the preparer must
    synthesise a hidden SetVariable task ahead of the condition and
    rewrite the operand to point at the bridge task's value.  Returning a
    structured request keeps the translator free of preparer-side
    concerns.
    """

    notebook_code: str
    notebook_imports: list[str] = field(default_factory=list)
    required_parameters: dict[str, str] = field(default_factory=dict)


def lower_to_bridge(value: Any, context: TranslationContext) -> tuple[str | None, BridgeRequest | None]:
    """Lowers *value* to a condition-task-safe operand or a BridgeRequest.

    Returns:
        ``(operand_value, bridge_request)`` -- exactly one of which is
        populated.  When ``operand_value`` is a string, it's a literal
        or ``{{...}}`` DAB ref that can be dropped directly into
        ``condition_task.left`` / ``.right``.  When ``bridge_request`` is
        populated, the caller must emit a hidden SetVariable task that
        runs the notebook code and reference its task value in the
        operand.  When both are None the expression is unresolvable.
    """
    if value is None:
        return None, None
    result = resolve_expression(value, context)
    if result is None:
        return None, None
    if result.kind in ("literal", "dab_ref"):
        return result.value, None
    if result.kind == "notebook_code":
        return None, BridgeRequest(
            notebook_code=result.value,
            notebook_imports=list(result.imports),
            required_parameters=dict(result.required_parameters),
        )
    return None, None


def merge_bridge_requests(*requests: BridgeRequest | None) -> BridgeRequest | None:
    """Combines several bridge requests into one merged Python expression.

    The bridges are joined with ``and`` so call-sites that compose two
    operand-level bridges (e.g. ``@and(empty(X), empty(Y))``) produce a
    single bridge task with a boolean truthiness result.  Returns ``None``
    when no non-None requests are supplied.
    """
    populated = [r for r in requests if r is not None]
    if not populated:
        return None
    if len(populated) == 1:
        return populated[0]
    expression = " and ".join(f"({r.notebook_code})" for r in populated)
    imports: list[str] = []
    required: dict[str, str] = {}
    for req in populated:
        for imp in req.notebook_imports:
            if imp not in imports:
                imports.append(imp)
        required.update(req.required_parameters)
    return BridgeRequest(
        notebook_code=expression,
        notebook_imports=imports,
        required_parameters=required,
    )


def expression_kind(value: Any, context: TranslationContext) -> ExpressionResult | None:
    """Convenience wrapper that returns the raw ExpressionResult for *value*."""
    return resolve_expression(value, context)


def resolve_field(value: Any, context: TranslationContext) -> str:
    """Resolves a field value that may contain an ADF expression.

    Args:
        value: The raw field value from ADF type properties.
        context: Translation context for variable resolution.

    Returns:
        Resolved string value.
    """
    if value is None:
        return ""
    result = resolve_expression(value, context)
    if result is not None:
        return result.value
    # Fallback: unwrap expression dicts, return raw string
    if isinstance(value, dict) and value.get("type") == "Expression":
        return value.get("value", "")
    if isinstance(value, str) and "@{" in value:
        return resolve_interpolated_string(value, context)
    return str(value) if not isinstance(value, str) else value


def resolve_field_int(value: Any, context: TranslationContext, default: int = 0) -> int:
    """Resolves a field to an integer, handling expressions that resolve to literals.

    Args:
        value: The raw field value from ADF type properties.
        context: Translation context for variable resolution.
        default: Fallback value when conversion fails.

    Returns:
        Resolved integer value.
    """
    resolved = resolve_field(value, context)
    try:
        return int(resolved)
    except (ValueError, TypeError):
        return default


def resolve_dict_values(d: dict[str, Any] | None, context: TranslationContext) -> dict[str, str]:
    """Resolves all values in a dict that may contain ADF expressions.

    Args:
        d: Dict of field name to raw values.
        context: Translation context for variable resolution.

    Returns:
        Dict with all values resolved to strings.
    """
    if not d:
        return {}
    return {k: resolve_field(v, context) for k, v in d.items()}
