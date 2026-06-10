"""Translates ADF DatabricksNotebook activities to Databricks NotebookActivity IR."""

from __future__ import annotations

import re
from typing import Any

from orchestra.models.adf_ast import AdfActivity, AdfDefinitions
from orchestra.models.ir import Activity, NotebookActivity, TranslationContext
from orchestra.parser.expression_parser import resolve_expression
from orchestra.translator.activity_translators.resolve import resolve_field


def translate(
    activity: AdfActivity,
    base_kwargs: dict[str, Any],
    context: TranslationContext,
    definitions: AdfDefinitions,
) -> Activity:
    """Translates a DatabricksNotebook activity.

    Args:
        activity: The ADF activity AST node.
        base_kwargs: Common fields (name, task_key, timeout, retries, depends_on, cluster).
        context: Current translation context.
        definitions: Full ADF definitions for cross-referencing.

    Returns:
        A :class:`NotebookActivity` IR node.
    """
    type_properties = activity.type_properties or {}

    # C-28 (NB-ITER4-001): when notebookPath is an ADF expression that lowers
    # to notebook_code (e.g. @trim(json(...).notebook_path)), preserve the raw
    # expression and mark the activity so the preparer emits a dispatch stub
    # rather than inlining Python source as the workspace path.
    notebook_path_raw = type_properties.get("notebookPath", "")
    notebook_path, notebook_path_unresolved, notebook_path_expression = _resolve_notebook_path_field(
        notebook_path_raw, context
    )
    raw_params = type_properties.get("baseParameters") or {}
    libraries, unresolved_libraries = _resolve_libraries(type_properties.get("libraries"), context)

    # Resolve base_parameters at translate time so ADF expressions like
    # @variables('runTimestamp') are inlined to DAB refs while the full
    # translation context (with variable_value_cache) is available.  Any
    # caveat notes the resolver emits (e.g. utcnow() approximations) are
    # captured into parameter_approximations so the bundler can surface
    # them in SETUP.md.
    resolved_params: dict[str, Any] = {}
    approximations: list[dict[str, str]] = []
    for key, value in raw_params.items():
        result = resolve_expression(value, context)
        if result is not None and result.kind in ("dab_ref", "literal"):
            resolved_params[key] = result.value
            for note in result.notes:
                approximations.append(
                    {
                        "widget_name": key,
                        "raw_expression": _raw_expression_text(value),
                        "replacement": result.value,
                        "note": note,
                    }
                )
        else:
            # Keep original for downstream handling (notebook_code or unresolvable)
            resolved_params[key] = value

    return NotebookActivity(
        **base_kwargs,
        notebook_path=notebook_path,
        notebook_path_unresolved=notebook_path_unresolved,
        notebook_path_expression=notebook_path_expression,
        base_parameters=resolved_params,
        libraries=libraries,
        unresolved_libraries=unresolved_libraries,
        parameter_approximations=approximations,
    )


def _resolve_notebook_path_field(
    value: Any,
    context: TranslationContext,
) -> tuple[str, bool, str | None]:
    """Resolves the ADF ``notebookPath`` field, preserving dynamic dispatch shapes.

    C-28 (NB-ITER4-001): the legacy ``resolve_field`` returns ``result.value``
    for every kind including ``notebook_code``, which means an ADF expression
    like ``@trim(json(activity('cfg').output.firstRow).notebook_path)`` ends up
    as Python source text in ``notebook_path``.  Bundle SETUP.md then
    mis-documents the source as a workspace path.

    Returns ``(notebook_path, notebook_path_unresolved, raw_expression)``.
    When the expression cannot be reduced to a workspace path we set
    ``notebook_path_unresolved=True`` so the preparer emits a dispatch-stub
    notebook and SETUP.md flags the dynamic dispatch.
    """
    if value is None:
        return "", False, None
    if isinstance(value, dict):
        if value.get("type") == "Expression" and "value" in value:
            raw_text = str(value["value"])
            result = resolve_expression(value, context)
            if result is not None and result.kind in ("literal", "dab_ref"):
                return result.value, False, None
            return "", True, raw_text
        return resolve_field(value, context), False, None
    if isinstance(value, str):
        if value.startswith("@"):
            result = resolve_expression(value, context)
            if result is not None and result.kind in ("literal", "dab_ref"):
                return result.value, False, None
            return "", True, value
        return value, False, None
    return resolve_field(value, context), False, None


def _raw_expression_text(value: Any) -> str:
    """Returns the original ADF expression text from a base_parameter value."""
    if isinstance(value, dict) and value.get("type") == "Expression" and "value" in value:
        return str(value["value"])
    return str(value)


# Library entry keys that may carry ADF expressions (jar/whl paths,
# maven coordinates with @concat, etc).  PyPI uses ``package`` and CRAN
# uses ``package``; we walk all of them through the resolver and only
# emit the entry when every expression resolves to a clean literal/dab_ref.
_LIBRARY_VALUE_KEYS: tuple[str, ...] = ("jar", "whl", "egg", "requirements")


_GLOBAL_PARAM_REF_RE = re.compile(r"pipeline\(\s*\)\.globalParameters\.(\w+)", re.IGNORECASE)
_PIPELINE_PARAM_REF_RE = re.compile(r"pipeline\(\s*\)\.parameters\.(\w+)", re.IGNORECASE)
_VARIABLE_REF_RE = re.compile(r"variables\(\s*'([^']+)'\s*\)", re.IGNORECASE)


def _extract_missing_identifiers(expression_text: str, context: TranslationContext) -> list[str]:
    """Returns identifier names referenced by *expression_text* that aren't
    bound in *context*.

    Helps surface concrete root causes in SETUP.md when a library expression
    fails to resolve (e.g. ``@concat(...proj4jLibFileName)`` referencing a
    global parameter that the factory doesn't declare).
    """
    missing: list[str] = []
    for match in _GLOBAL_PARAM_REF_RE.finditer(expression_text):
        name = match.group(1)
        if context.get_global_parameter(name) is None and name not in missing:
            missing.append(name)
    for match in _PIPELINE_PARAM_REF_RE.finditer(expression_text):
        name = match.group(1)
        if name not in missing:
            missing.append(name)
    for match in _VARIABLE_REF_RE.finditer(expression_text):
        name = match.group(1)
        if context.get_variable_task_key(name) is None and name not in missing:
            missing.append(name)
    return missing


def _resolve_libraries(
    libraries: list[dict[str, Any]] | None,
    context: TranslationContext,
) -> tuple[list[dict[str, Any]] | None, list[dict[str, Any]]]:
    """Pipes library descriptor values through the expression resolver.

    Library entries whose ``jar``/``whl``/``egg``/``requirements`` value is
    an ADF expression that resolves cleanly to a literal get the literal
    substituted in place.  Entries whose expression is unresolved (e.g.
    references a missing globalParameter) are passed through unchanged
    so downstream bundler tooling can flag them in SETUP.md.

    C-30 (NB-ITER4-003): also returns a list of unresolved library entries
    so the preparer can render an ``Unresolved libraries`` section in
    SETUP.md instead of shipping a broken ``@concat(...)`` literal jar path
    that the cluster cannot install.
    """
    unresolved: list[dict[str, Any]] = []
    if not libraries:
        return libraries, unresolved

    resolved: list[dict[str, Any]] = []
    for lib in libraries:
        if not isinstance(lib, dict):
            resolved.append(lib)
            continue
        resolved_entry: dict[str, Any] = {}
        for key, value in lib.items():
            if key in _LIBRARY_VALUE_KEYS and isinstance(value, (str, dict)):
                result = resolve_expression(value, context)
                # C-13 (NB-ITER3-004): accept both literal and dab_ref so a
                # jar path like @pipeline().parameters.libName collapses to
                # {{job.parameters.libName}} (symmetric with custom_tags
                # resolution in _resolve_ls_parameters).
                if result is not None and result.kind in ("literal", "dab_ref"):
                    resolved_entry[key] = result.value
                else:
                    expression_text = _raw_expression_text(value)
                    resolved_entry[key] = value
                    # Only surface library entries whose value carried an
                    # ADF expression (starts with ``@``).  Bare literal
                    # paths that already resolved successfully don't need a
                    # SETUP.md callout.
                    if isinstance(expression_text, str) and expression_text.startswith("@"):
                        unresolved.append(
                            {
                                "type": key,
                                "expression": expression_text,
                                "missing": _extract_missing_identifiers(expression_text, context),
                            }
                        )
            else:
                resolved_entry[key] = value
        resolved.append(resolved_entry)
    return resolved, unresolved
