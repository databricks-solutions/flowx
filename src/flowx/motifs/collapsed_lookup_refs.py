"""Repair references to a Lookup that motif collapse replaced.

The metadata-driven bulk-copy motif collapses a ``Lookup -> ForEach -> Copy``
chain into one motif task.  The collapsed Lookup no longer exists, yet a
*separate* downstream ForEach (or Filter) may still iterate its control rows via
``{{tasks.<lookup>.values.result}}``.  The collapser rewires ``depends_on`` to the
motif but not this value reference, and the package phase's dangling-ref safety
net does not inspect ``for_each_task.inputs``, so without this repair the
reference ships dangling.

:func:`repair_collapsed_lookup_references` runs on every packaging route (before
``prepare_workflow``) and in the adapter ``modify`` flow, so raw, stamped, single-
and multi-pipeline reports are all covered.  It is idempotent: once a reference is
repaired it no longer matches.
"""

from __future__ import annotations

import dataclasses
import json
import logging
import re

from flowx.models.ir import (
    FilterActivity,
    ForEachActivity,
    IfConditionActivity,
    MotifActivity,
    Pipeline,
    SwitchActivity,
)

logger = logging.getLogger(__name__)

# Matches a whole-string reference to a collapsed Lookup's row array, e.g.
# "{{tasks.LKP.values.result}}".  The expression parser folds a Lookup's array
# output (and its ``firstRow``/``value``) to ``values.result``; per-column firstRow
# task values keep their own keys, so anchoring to ``result`` avoids rewriting those.
_WHOLE_LOOKUP_RESULT_REF = re.compile(r"^\{\{tasks\.([^.]+)\.values\.result\}\}$")

# The only ``databricks_replacement`` whose preparer emits ``<task_key>_control_lookup``
# (see ``preparer/activity_preparers/motif.py``).
_FOR_EACH_INGESTION = "for_each_ingestion"


def repair_collapsed_lookup_references(pipeline: Pipeline) -> Pipeline:
    """Repoints references to a Lookup that a metadata-driven motif collapsed.

    A separate downstream ForEach/Filter that iterated the collapsed Lookup's
    control rows (``{{tasks.<lookup>.values.result}}``) is repaired to match how
    the motif is prepared (see ``preparer/activity_preparers/motif.py``):

    * **Static** -- control rows materialised onto ``lookup_values``: the motif
      becomes a ``pipeline_task`` that publishes no task values, so the rows are
      inlined as a literal JSON array.
    * **Dynamic** -- no materialised rows: the motif expands to a ``for_each`` fed
      by a synthesised ``<task_key>_control_lookup`` task that publishes the rows
      as ``values.items`` at runtime, so the reference is repointed there.

    The collapsed Lookup is identified by ``motif_config["lookup_scope"]`` -- the
    one Lookup the motif reproduces (its query drives the control table), which is
    exactly what the dangling reference embeds, and never the collapsed ForEach.  A
    motif may collapse other Lookups too, but their data is not reproduced, so
    references to them are left untouched rather than redirected to the wrong rows.
    ``lookup_scope`` is set only on metadata-driven bulk-copy motifs, so other
    motifs are skipped; the ``consolidate_metadata_driven`` flag is not gated on, so
    the non-consolidated ``for_each_ingestion`` path is covered too.  The downstream
    reference may sit at the top level or nested inside any If/Switch/ForEach, and
    may be a plain ``items_expression`` or a value in a ForEach's inputs-bridge
    parameters (which the preparer prefers over ``items_expression`` when the items
    is a function over the Lookup output), so both are searched in every container.

    Args:
        pipeline: Pipeline IR after motif collapse (and, in the ``modify`` flow,
            after :func:`_stamp_lookup_values_into_metadata_driven_motifs`).

    Returns:
        A new :class:`Pipeline` with collapsed-Lookup references repaired, or the
        input unchanged when no motif resolves a lookup key.  Idempotent: a
        repaired reference no longer matches, so re-running is a no-op.
    """
    replacement_by_key = _build_replacements(pipeline)
    if not replacement_by_key:
        return pipeline

    def _rewrite_items(items_expression: str) -> str:
        # Tolerate surrounding whitespace; a wrapped ref (e.g. ``@json({{...}})``) is
        # not a safe whole-string rewrite, so leave it but surface a signal.
        match = _WHOLE_LOOKUP_RESULT_REF.match((items_expression or "").strip())
        if match and match.group(1) in replacement_by_key:
            return replacement_by_key[match.group(1)]
        if match is None:
            _warn_if_wrapped(items_expression, replacement_by_key)
        return items_expression

    def _rewrite_all(activities):
        return [_rewrite(child) for child in activities]

    def _rewrite(activity):
        # Descend through every container that can nest a ForEach/Filter (ForEach,
        # If, Switch) so a collapsed-Lookup reference is repaired wherever it lives.
        if isinstance(activity, ForEachActivity):
            # A ForEach whose items is a function over the lookup output (e.g.
            # ``@take(activity('LKP').output.value, 10)``) resolves through an inputs
            # bridge, which the preparer prefers over ``items_expression``; the collapsed
            # ref lives in the bridge's parameters, so repair those too.
            return dataclasses.replace(
                activity,
                items_expression=_rewrite_items(activity.items_expression),
                inputs_bridge_required_parameters={
                    name: _rewrite_items(value) for name, value in activity.inputs_bridge_required_parameters.items()
                },
                inner_activities=_rewrite_all(activity.inner_activities),
            )
        if isinstance(activity, FilterActivity):
            return dataclasses.replace(activity, items_expression=_rewrite_items(activity.items_expression))
        if isinstance(activity, IfConditionActivity):
            return dataclasses.replace(
                activity,
                if_true_activities=_rewrite_all(activity.if_true_activities),
                if_false_activities=_rewrite_all(activity.if_false_activities),
            )
        if isinstance(activity, SwitchActivity):
            return dataclasses.replace(
                activity,
                cases=[dataclasses.replace(case, activities=_rewrite_all(case.activities)) for case in activity.cases],
                default_activities=_rewrite_all(activity.default_activities),
            )
        return activity

    return dataclasses.replace(pipeline, tasks=[_rewrite(task) for task in pipeline.tasks])


def _build_replacements(pipeline: Pipeline) -> dict[str, str]:
    """Maps each collapsed Lookup's task key to its replacement iterator input."""
    replacement_by_key: dict[str, str] = {}
    for task in pipeline.tasks:
        if not isinstance(task, MotifActivity):
            continue
        # ``lookup_scope`` is the one Lookup the motif reproduces (its query drives the
        # control table).  A motif can collapse other Lookups too, but their data is not
        # reproduced, so only references to this one can be repaired -- redirecting a
        # reference to a different collapsed Lookup here would silently serve wrong rows.
        lookup_key = task.motif_config.get("lookup_scope")
        if not lookup_key:
            continue
        if task.lookup_values:
            # Static: the rows are materialised, so inline them as a literal array regardless of how
            # the motif itself is prepared -- the array is the data the Lookup would have returned.
            replacement_by_key[lookup_key] = json.dumps(task.lookup_values)
        elif task.databricks_replacement == _FOR_EACH_INGESTION:
            # Dynamic: only the for_each_ingestion path emits <task_key>_control_lookup (publishing the
            # rows as ``items``), so point the iterator there.  Other paths (e.g. a Lakeflow-Connect
            # database motif that took the generic scaffold) emit no such task, so leave the reference
            # untouched rather than point at a task that is never created.
            replacement_by_key[lookup_key] = f"{{{{tasks.{task.task_key}_control_lookup.values.items}}}}"
    return replacement_by_key


def _warn_if_wrapped(items_expression: str, replacement_by_key: dict[str, str]) -> None:
    """Logs when a collapsed-Lookup ref is embedded in a larger expression we cannot rewrite."""
    if not items_expression:
        return
    for key in replacement_by_key:
        if f"tasks.{key}.values.result" in items_expression:
            logger.warning(
                "Collapsed-lookup reference to %r is wrapped in a larger expression (%r); the iterator "
                "was left unrepaired and may dangle at runtime.",
                key,
                items_expression,
            )
            return
