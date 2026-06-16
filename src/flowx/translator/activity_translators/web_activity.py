"""Translates ADF WebActivity activities to Databricks WebActivity IR."""

from __future__ import annotations

import json
from typing import Any

from flowx.models.adf_ast import AdfActivity, AdfDefinitions
from flowx.models.ir import Activity, TranslationContext
from flowx.models.ir import WebActivity as WebActivityIR
from flowx.parser.expression_parser import (
    resolve_expression,
    resolve_interpolated_string_for_notebook,
)
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
    body = type_properties.get("body")
    body_code, body_imports, body_required = _resolve_body_to_code(body, context)
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
        body_code=body_code,
        body_imports=body_imports,
        body_required_parameters=body_required,
    )


def _py_literal(value: Any) -> str:
    """Renders a resolved literal as a Python expression."""
    if isinstance(value, str):
        return json.dumps(value)
    if isinstance(value, bool) or value is None:
        return repr(value)
    return json.dumps(value)


def _value_to_code(value: Any, context: TranslationContext) -> tuple[str | None, list[str], dict[str, str]]:
    """Lowers a single body value to a Python expression string.

    Returns ``(code, imports, required_parameters)`` where ``code`` is
    ``None`` when the value is a plain literal the code generator can render
    directly (no ``@``-expression present).
    """
    if isinstance(value, str):
        if "@{" in value:
            resolved = resolve_interpolated_string_for_notebook(value, context)
            return f"f{json.dumps(resolved)}", [], {}
        if value.startswith("@"):
            result = resolve_expression(value, context)
            if result is None:
                return None, [], {}
            if result.kind == "notebook_code":
                return result.value, list(result.imports), dict(result.required_parameters)
            if result.kind == "dab_ref":
                # A bare variable/pipeline ref is read from a widget at runtime; bind the DAB ref into
                # base_parameters via the returned required_parameters mapping.
                widget = result.value.strip("{}").split(".")[-1]
                return f"dbutils.widgets.get({json.dumps(widget)})", [], {widget: result.value}
            return _py_literal(result.value), [], dict(result.required_parameters)
        return None, [], {}
    if isinstance(value, dict):
        if value.get("type") == "Expression" and "value" in value:
            return _value_to_code(value["value"], context)
        # Nested dict body (e.g. {"text": {"value": "@concat(...)"}}).
        parts: list[str] = []
        imports: list[str] = []
        required: dict[str, str] = {}
        any_code = False
        for key, inner in value.items():
            code, imps, req = _value_to_code(inner, context)
            if code is None:
                parts.append(f"{json.dumps(key)}: {_py_literal(inner)}")
            else:
                any_code = True
                parts.append(f"{json.dumps(key)}: {code}")
                imports.extend(imps)
                required.update(req)
        if not any_code:
            return None, [], {}
        return "{" + ", ".join(parts) + "}", imports, required
    return None, [], {}


def _resolve_body_to_code(body: Any, context: TranslationContext) -> tuple[str | None, list[str], dict[str, str]]:
    """Pre-resolves an ADF request body to Python code at translate time.

    ADF web-activity bodies frequently embed ``@concat`` / ``@variables`` /
    ``@{...}`` expressions, either at the top level or nested inside a dict
    (``{"text": {"value": "@concat(...)"}}``).  Resolving them here -- while
    the real :class:`TranslationContext` (and its variable cache) is
    available -- lets the code generator emit parsed Python instead of the
    raw ADF token.

    Returns ``(body_code, imports, required_parameters)``.  ``body_code`` is
    ``None`` for a plain-literal body (the generator renders it directly).
    """
    if body is None:
        return None, [], {}
    return _value_to_code(body, context)


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
