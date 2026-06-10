"""Whole-IR expression rewriter.

Activity translators each call :func:`resolve_interpolated_string` on the
specific string fields they know about (``source_query``, ``url``,
``base_parameters`` values, ...).  Anything outside those known fields
-- raw SQL ``WHERE`` clauses inside ``source_properties``, REST request
bodies, dataset folder paths, ``base_parameters`` strings that pass
through a value untouched -- can carry through to the bundle as a
literal ``@{...}`` ADF expression, silently corrupting query semantics
at runtime.

This module runs a final pass over the translated IR, walks every
string-typed field on every :class:`~orchestra.models.ir.Activity`
(including strings nested inside ``dict`` / ``list`` fields and inside
control-flow inner activities), and re-applies
:func:`resolve_interpolated_string` to each.  Every ``@{...}`` token
that still remains after the pass is appended to *warnings* so the
caller surfaces the gap in the translation report.

Fields that intentionally hold raw ADF input (``linked_service_definition``,
``raw_definition``) or that preserve pre-rewrite history
(``original_activities`` on :class:`~orchestra.models.ir.MotifActivity`)
are skipped.  Identifier fields (``name``, ``task_key``,
``variable_name``) are skipped so that rewriting cannot break
cross-task references.
"""

from __future__ import annotations

import dataclasses
import re
from types import MappingProxyType
from typing import Any

from orchestra.models.ir import (
    Activity,
    AppendVariableActivity,
    Pipeline,
    SetVariableActivity,
    SwitchActivity,
    SwitchCase,
    TranslationContext,
)
from orchestra.parser.expression_parser import resolve_interpolated_string

# Fields that must never be rewritten — either they hold raw ADF input
# that downstream consumers parse separately, or they are identifiers
# whose value is used as a reference key elsewhere in the IR.
_FIELDS_TO_SKIP: frozenset[str] = frozenset(
    {
        "name",
        "task_key",
        "linked_service_definition",
        "raw_definition",
        "original_activities",
        "variable_name",
        "matched_activity_names",
    }
)

_UNRESOLVED_RE = re.compile(r"@\{[^}]+\}")


def rewrite_pipeline_expressions(
    pipeline: Pipeline,
    *,
    warnings: list[str] | None = None,
) -> Pipeline:
    """Walks every string field in *pipeline* and rewrites ``@{...}`` tokens.

    Args:
        pipeline: Translated pipeline IR.  Not mutated.
        warnings: Optional list to which the rewriter appends a message
            for every string field that still contains an unresolved
            ``@{...}`` token after the pass.  When ``None`` the rewriter
            still runs but cannot surface gaps.

    Returns:
        A new :class:`Pipeline` whose activities have had their string
        fields rewritten through
        :func:`~orchestra.parser.expression_parser.resolve_interpolated_string`.

    Notes:
        - The rewriter builds a single :class:`TranslationContext` from
          the pipeline's :class:`SetVariableActivity` and
          :class:`AppendVariableActivity` nodes so that
          ``@{variables('x')}`` tokens resolve against the same setter
          task keys the per-activity translators used.
        - Fields listed in ``_FIELDS_TO_SKIP`` -- identifiers and raw
          ADF input -- are returned unchanged.
        - Unknown field types (ints, bools, None, custom dataclasses
          beyond Activity/SwitchCase) pass through unchanged.
    """
    context = _build_context_from_pipeline(pipeline)
    sink: list[str] = warnings if warnings is not None else []
    rewritten_tasks = [_rewrite_activity(activity, context, sink) for activity in pipeline.tasks]
    return dataclasses.replace(pipeline, tasks=rewritten_tasks)


def _build_context_from_pipeline(pipeline: Pipeline) -> TranslationContext:
    """Builds a TranslationContext whose variable_cache is keyed by every
    SetVariable / AppendVariable activity in the pipeline.

    Args:
        pipeline: Translated pipeline IR.

    Returns:
        A :class:`TranslationContext` with ``variable_cache`` populated.
        Activities that nest inside control-flow types are walked too so
        a variable set inside a ForEach is still discoverable.
    """
    variable_cache: dict[str, str] = {}

    def visit(activities: list[Activity]) -> None:
        for activity in activities:
            if isinstance(activity, (SetVariableActivity, AppendVariableActivity)):
                variable_cache.setdefault(activity.variable_name, activity.task_key)
            # Recurse into control-flow branches.
            nested = _nested_activities(activity)
            if nested:
                visit(nested)

    visit(list(pipeline.tasks))
    return TranslationContext(
        activity_cache=MappingProxyType({}),
        registry=MappingProxyType({}),
        variable_cache=MappingProxyType(variable_cache),
    )


def _nested_activities(activity: Activity) -> list[Activity]:
    """Returns activities nested inside a control-flow activity, or [].

    Args:
        activity: Any IR activity.

    Returns:
        The control-flow branches' activity lists concatenated, or an
        empty list when *activity* is a leaf type.
    """
    nested: list[Activity] = []
    if hasattr(activity, "inner_activities"):
        nested.extend(getattr(activity, "inner_activities") or [])
    if hasattr(activity, "if_true_activities"):
        nested.extend(getattr(activity, "if_true_activities") or [])
    if hasattr(activity, "if_false_activities"):
        nested.extend(getattr(activity, "if_false_activities") or [])
    if isinstance(activity, SwitchActivity):
        for case in activity.cases:
            nested.extend(case.activities)
        nested.extend(activity.default_activities)
    return nested


def _rewrite_activity(activity: Activity, context: TranslationContext, warnings: list[str]) -> Activity:
    """Returns a new activity with every safe string field rewritten.

    Args:
        activity: Activity to rewrite.  Not mutated.
        context: Translation context whose ``variable_cache`` resolves
            ``@variables('x')`` tokens.
        warnings: List to append unresolved-expression warnings to.

    Returns:
        A new activity instance with rewritten string fields.  Control-
        flow activities have their inner branches recursed into.
    """
    field_overrides: dict[str, Any] = {}
    for f in dataclasses.fields(activity):
        if f.name in _FIELDS_TO_SKIP:
            continue
        original = getattr(activity, f.name)
        rewritten = _rewrite_value(
            original,
            context,
            warnings,
            field_path=f"{type(activity).__name__}.{activity.task_key}.{f.name}",
        )
        if rewritten is not original:
            field_overrides[f.name] = rewritten

    if not field_overrides:
        return activity
    return dataclasses.replace(activity, **field_overrides)


def _rewrite_value(value: Any, context: TranslationContext, warnings: list[str], *, field_path: str) -> Any:
    """Recursively rewrites every string contained in *value*.

    Args:
        value: Any IR value -- str, list, dict, Activity, SwitchCase, or
            a primitive.  Activities and SwitchCases recurse; primitives
            pass through.
        context: Translation context.
        warnings: List to append unresolved-expression warnings to.
        field_path: Dotted path describing where this value sits in the
            IR (used in warning messages so the user can find the gap).

    Returns:
        The rewritten value, or *value* unchanged when no rewrite
        applied.
    """
    if isinstance(value, str):
        return _rewrite_string(value, context, warnings, field_path=field_path)
    if isinstance(value, Activity):
        return _rewrite_activity(value, context, warnings)
    if isinstance(value, SwitchCase):
        new_value = _rewrite_value(value.value, context, warnings, field_path=f"{field_path}.value")
        new_activities = [_rewrite_activity(a, context, warnings) for a in value.activities]
        if new_value is value.value and all(n is o for n, o in zip(new_activities, value.activities)):
            return value
        return SwitchCase(value=new_value, activities=new_activities)
    if isinstance(value, list):
        new_list = [
            _rewrite_value(item, context, warnings, field_path=f"{field_path}[{i}]") for i, item in enumerate(value)
        ]
        if all(n is o for n, o in zip(new_list, value)):
            return value
        return new_list
    if isinstance(value, dict):
        new_dict = {
            k: _rewrite_value(v, context, warnings, field_path=f"{field_path}[{k!r}]") for k, v in value.items()
        }
        if all(new_dict[k] is value[k] for k in value):
            return value
        return new_dict
    return value


def _rewrite_string(value: str, context: TranslationContext, warnings: list[str], *, field_path: str) -> str:
    """Applies ``resolve_interpolated_string`` and surfaces leftover ``@{...}``.

    Args:
        value: String value to rewrite.
        context: Translation context.
        warnings: List to append unresolved-expression warnings to.
        field_path: Dotted path for the warning message.

    Returns:
        The rewritten string.  When the rewrite cannot resolve every
        ``@{...}`` token, the leftover tokens remain in the returned
        string (for forensics) and a warning is appended.
    """
    if "@{" not in value:
        return value
    rewritten = resolve_interpolated_string(value, context)
    leftovers = _UNRESOLVED_RE.findall(rewritten)
    if leftovers:
        warnings.append(
            f"Unresolved ADF expression at {field_path}: {sorted(set(leftovers))!r} (left in output verbatim)"
        )
    return rewritten
