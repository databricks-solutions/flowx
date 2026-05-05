"""Translates ADF WebActivity activities to Databricks WebActivity IR."""

from __future__ import annotations

from typing import Any

from flowx.models.adf_ast import AdfActivity, AdfDefinitions
from flowx.models.ir import Activity, TranslationContext
from flowx.models.ir import WebActivity as WebActivityIR
from flowx.parser.expression_parser import resolve_expression
from flowx.translator.activity_translators.resolve import resolve_dict_values, resolve_field


def translate(
    activity: AdfActivity,
    base_kwargs: dict[str, Any],
    context: TranslationContext,
    definitions: AdfDefinitions,
) -> Activity:
    """Translates a WebActivity.

    Args:
        activity: The ADF activity AST node.
        base_kwargs: Common fields (name, task_key, timeout, retries, depends_on, cluster).
        context: Current translation context.
        definitions: Full ADF definitions for cross-referencing.

    Returns:
        A :class:`WebActivity` IR node.
    """
    type_properties = activity.type_properties or {}

    url = resolve_field(type_properties.get("url", ""), context)
    method = type_properties.get("method", "GET")
    headers = resolve_dict_values(type_properties.get("headers"), context) or None
    body = _resolve_body(type_properties.get("body"), context)
    authentication = type_properties.get("authentication")
    disable_cert_validation = type_properties.get("disableCertValidation", False)
    http_request_timeout = type_properties.get("httpRequestTimeout")

    timeout_seconds: int | None = None
    if http_request_timeout and isinstance(http_request_timeout, str):
        timeout_seconds = _parse_timeout_to_seconds(http_request_timeout)

    return WebActivityIR(
        **base_kwargs,
        url=url,
        method=method,
        body=body,
        headers=headers,
        authentication=authentication,
        disable_cert_validation=disable_cert_validation,
        http_request_timeout_seconds=timeout_seconds,
    )


def _resolve_body(body: Any, context: TranslationContext) -> Any:
    """Pre-resolve ADF expressions in the request body at translate time.

    Args:
        body: Raw body from the ADF typeProperties.
        context: Current translation context with variable caches.

    Returns:
        Resolved body — either a Python code string (for notebook_code),
        the original body dict, or ``None``.
    """
    if body is None:
        return None

    if isinstance(body, dict) and body.get("type") == "Expression" and "value" in body:
        result = resolve_expression(body, context)
        if result is not None and result.kind == "notebook_code":
            return result.value
        if result is not None and result.kind == "literal":
            return result.value

    return body


def _parse_timeout_to_seconds(timeout_str: str) -> int | None:
    """Parses an ADF timeout string to seconds.

    Args:
        timeout_str: Timeout in ``"d.hh:mm:ss"`` or ``"hh:mm:ss"`` format.

    Returns:
        Total seconds, or ``None`` if the format is unrecognised.
    """
    try:
        parts = timeout_str.split(".")
        if len(parts) == 2:
            days = int(parts[0])
            time_part = parts[1]
        else:
            days = 0
            time_part = parts[0]
        time_parts = time_part.split(":")
        hours = int(time_parts[0]) if len(time_parts) > 0 else 0
        minutes = int(time_parts[1]) if len(time_parts) > 1 else 0
        seconds = int(time_parts[2]) if len(time_parts) > 2 else 0
        return days * 86400 + hours * 3600 + minutes * 60 + seconds
    except (ValueError, IndexError):
        return None
