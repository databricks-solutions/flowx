"""Preparer for FilterActivity -> notebook_task with generated filter notebook."""

from __future__ import annotations

from typing import TYPE_CHECKING

from flowx.models.ir import TranslationContext
from flowx.parser.expression_parser import resolve_expression
from flowx.preparer.activity_preparers.helpers import build_notebook_activity_task
from flowx.preparer.activity_preparers.naming import notebook_filename
from flowx.preparer.code_generator import generate_filter_notebook
from flowx.preparer.workflow_preparer import PreparedActivity

if TYPE_CHECKING:
    from flowx.models.ir import FilterActivity


def prepare(activity: FilterActivity, *, scope: str = "") -> PreparedActivity:
    """Converts a FilterActivity into a notebook_task that filters an array."""
    items_result = resolve_expression(activity.items_expression, TranslationContext())
    if items_result is not None and items_result.kind in ("dab_ref", "literal"):
        items_value = items_result.value
    else:
        items_value = activity.items_expression

    task, notebooks = build_notebook_activity_task(
        activity,
        notebook_relative_path=f"notebooks/{notebook_filename(activity.task_key, activity.name)}",
        notebook_content=generate_filter_notebook(activity),
        base_parameters={"items_expression": items_value},
    )
    return PreparedActivity(task=task, notebooks=notebooks)
