"""Shared field resolution helper for activity translators."""

from __future__ import annotations

from typing import Any

from flowx.models.ir import TranslationContext
from flowx.parser.expression_parser import resolve_expression, resolve_interpolated_string


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
