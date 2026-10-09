"""Collapse detected motifs into MotifActivity IR nodes."""

from __future__ import annotations

import dataclasses
import json
import logging
import re
from typing import Any

from flowx.models.ir import (
    Activity,
    CopyActivity,
    Dependency,
    FilterActivity,
    ForEachActivity,
    IfConditionActivity,
    LookupActivity,
    MotifActivity,
    Pipeline,
    SwitchActivity,
)
from flowx.models.motifs import DetectedMotif

logger = logging.getLogger(__name__)

_WHOLE_LOOKUP_RESULT_REF = re.compile(r"^\s*\{\{tasks\.([^.]+)\.values\.result\}\}\s*$")
_WHOLE_CONTROL_LOOKUP_ITEMS_REF = re.compile(r"^\s*\{\{tasks\.([^.]+)_control_lookup\.values\.items\}\}\s*$")


def collapse_motifs(
    pipeline: Pipeline,
    motifs: list[DetectedMotif],
) -> Pipeline:
    """Replaces matched activity groups with MotifActivity nodes.

    Args:
        pipeline: The translated pipeline IR.
        motifs: Detected motif matches from the detector.

    Returns:
        A new Pipeline with motif activities collapsed.  The original
        pipeline is not mutated.
    """
    if not motifs:
        return pipeline

    claimed_names: set[str] = set()
    for motif in motifs:
        claimed_names.update(motif.matched_activities)

    tasks_by_name: dict[str, Activity] = {task.name: task for task in pipeline.tasks}
    new_tasks: list[Activity] = []
    # Maps a collapsed activity's sanitised task_key to the MotifActivity's task_key so
    # _rewire_dependencies can match Dependency.task_key (also sanitised); raw names would miss edges.
    motif_task_keys: dict[str, str] = {}

    inserted_motifs: set[str] = set()
    for task in pipeline.tasks:
        if task.name in claimed_names:
            detected = _find_motif_for_activity(task.name, motifs)
            if detected is None:
                new_tasks.append(task)
                continue

            motif_id = detected.definition.motif_id
            if motif_id in inserted_motifs:
                continue
            inserted_motifs.add(motif_id)

            motif_activity = _build_motif_activity(detected, tasks_by_name)
            new_tasks.append(motif_activity)

            for matched_name in detected.matched_activities:
                matched_task = tasks_by_name.get(matched_name)
                if matched_task is not None:
                    motif_task_keys[matched_task.task_key] = motif_activity.task_key
        else:
            new_tasks.append(task)

    _rewire_dependencies(new_tasks, motif_task_keys)

    collapsed_pipeline = Pipeline(
        name=pipeline.name,
        description=pipeline.description,
        parameters=pipeline.parameters,
        schedule=pipeline.schedule,
        timeout_seconds=pipeline.timeout_seconds,
        email_notifications=dict(pipeline.email_notifications),
        tasks=new_tasks,
        tags=pipeline.tags,
        not_translatable=pipeline.not_translatable,
        reconciliation_status=pipeline.reconciliation_status,
        migration_status=pipeline.migration_status,
        audit=dict(pipeline.audit),
        translation_configuration=pipeline.translation_configuration,
    )
    return inline_collapsed_lookup_references(collapsed_pipeline)


def inline_collapsed_lookup_references(pipeline: Pipeline) -> Pipeline:
    """Repoints iterator references to Lookups replaced by ingestion motifs.

    A collapsed ``Lookup -> ForEach -> Copy`` chain removes the Lookup task, but
    separate downstream ForEach or Filter activities can still reference its row
    array through ``{{tasks.<lookup>.values.result}}``. Expression translation
    uses ``result`` for both Lookup ``output.value`` and ``output.firstRow``; this
    repair is limited to whole iterator expressions so scalar semantics elsewhere
    remain unchanged.

    Dynamic motifs expose the rows from a synthesized ``<task_key>_control_lookup``
    task. When lookup rows have been materialized, the rows are inlined as JSON.

    Args:
        pipeline: Pipeline IR containing collapsed motif activities.

    Returns:
        A new pipeline with resolvable iterator references, or the original
        pipeline when no collapsed Lookup can be resolved.
    """
    replacement_by_lookup_key: dict[str, str] = {}
    replacement_by_motif_key: dict[str, str] = {}
    for task in pipeline.tasks:
        if not isinstance(task, MotifActivity) or task.databricks_replacement != "for_each_ingestion":
            continue
        lookup_key = task.motif_config.get("lookup_scope")
        if not lookup_key:
            continue
        replacement = (
            json.dumps(task.lookup_values)
            if task.lookup_values
            else f"{{{{tasks.{task.task_key}_control_lookup.values.items}}}}"
        )
        replacement_by_lookup_key[lookup_key] = replacement
        replacement_by_motif_key[task.task_key] = replacement
    if not replacement_by_lookup_key:
        return pipeline

    def rewrite_all(activities: list[Activity]) -> list[Activity]:
        return [rewrite(activity) for activity in activities]

    def rewrite_items_expression(activity: ForEachActivity | FilterActivity) -> str:
        lookup_match = _WHOLE_LOOKUP_RESULT_REF.match(activity.items_expression or "")
        if lookup_match:
            return replacement_by_lookup_key.get(lookup_match.group(1), activity.items_expression)
        control_match = _WHOLE_CONTROL_LOOKUP_ITEMS_REF.match(activity.items_expression or "")
        if control_match:
            return replacement_by_motif_key.get(control_match.group(1), activity.items_expression)
        return activity.items_expression

    def rewrite(activity: Activity) -> Activity:
        if isinstance(activity, ForEachActivity):
            return dataclasses.replace(
                activity,
                items_expression=rewrite_items_expression(activity),
                inner_activities=rewrite_all(activity.inner_activities),
            )
        if isinstance(activity, FilterActivity):
            return dataclasses.replace(activity, items_expression=rewrite_items_expression(activity))
        if isinstance(activity, IfConditionActivity):
            return dataclasses.replace(
                activity,
                if_true_activities=rewrite_all(activity.if_true_activities),
                if_false_activities=rewrite_all(activity.if_false_activities),
            )
        if isinstance(activity, SwitchActivity):
            return dataclasses.replace(
                activity,
                cases=[dataclasses.replace(case, activities=rewrite_all(case.activities)) for case in activity.cases],
                default_activities=rewrite_all(activity.default_activities),
            )
        return activity

    return dataclasses.replace(pipeline, tasks=rewrite_all(pipeline.tasks))


def _find_motif_for_activity(
    activity_name: str,
    motifs: list[DetectedMotif],
) -> DetectedMotif | None:
    """Finds the motif that claimed a given activity."""
    for motif in motifs:
        if activity_name in motif.matched_activities:
            return motif
    return None


def _build_motif_activity(
    motif: DetectedMotif,
    tasks_by_name: dict[str, Activity],
) -> MotifActivity:
    """Builds a MotifActivity from a detected motif and the original tasks."""
    definition = motif.definition

    task_key = f"motif_{definition.motif_id}"
    display_name = definition.display_name

    original_activities = [tasks_by_name[name] for name in motif.matched_activities if name in tasks_by_name]

    # Compare sanitised task_keys (Dependency.task_key is sanitised); raw names would mis-classify deps.
    matched_task_keys = {activity.task_key for activity in original_activities}
    external_deps = _collect_external_dependencies(original_activities, matched_task_keys)

    return MotifActivity(
        name=display_name,
        task_key=task_key,
        description=(
            f"Collapsed motif: {definition.display_name}. "
            f"Replaces {len(motif.matched_activities)} ADF activities with "
            f"{definition.databricks_replacement}."
        ),
        depends_on=external_deps,
        motif_id=definition.motif_id,
        display_name=display_name,
        databricks_replacement=definition.databricks_replacement,
        matched_activity_names=list(motif.matched_activities),
        source_type_hint=motif.source_type_hint,
        confidence_notes=list(motif.confidence_notes),
        original_activities=original_activities,
        notebook_template=definition.notebook_template,
        motif_config=_build_motif_config(definition.databricks_replacement, original_activities, task_key),
    )


def _build_motif_config(
    databricks_replacement: str,
    original_activities: list[Activity],
    motif_task_key: str,
) -> dict[str, Any]:
    """Extracts motif-specific settings from the activities being collapsed."""
    if databricks_replacement != "for_each_ingestion":
        return {}

    lookup = next((activity for activity in original_activities if isinstance(activity, LookupActivity)), None)
    copy = next((activity for activity in original_activities if isinstance(activity, CopyActivity)), None)

    config: dict[str, Any] = {}
    if lookup is not None:
        if lookup.source_query:
            config["lookup_query"] = lookup.source_query
        if lookup.source_type:
            config["lookup_source_type"] = lookup.source_type
        config["lookup_scope"] = lookup.task_key or motif_task_key
    if copy is not None:
        sink_properties = copy.sink_properties or {}
        sink_table = sink_properties.get("table") or sink_properties.get("tableName")
        if sink_table:
            config["sink_table"] = sink_table
        if copy.source_type:
            config["copy_source_type"] = copy.source_type
        config["copy_scope"] = copy.task_key or motif_task_key
    return config


def _collect_external_dependencies(
    activities: list[Activity],
    matched_task_keys: set[str],
) -> list[Dependency]:
    """Collects dependencies that point outside the matched activity group.

    *matched_task_keys* must be the sanitised task_keys of the matched
    activities -- ``Dependency.task_key`` is sanitised by the translator,
    so comparing against raw activity names produces silent false
    positives whenever a name contains spaces or other characters that
    are stripped during sanitisation.
    """
    seen: set[str] = set()
    external_deps: list[Dependency] = []

    for activity in activities:
        if not activity.depends_on:
            continue
        for dep in activity.depends_on:
            if dep.task_key not in matched_task_keys and dep.task_key not in seen:
                seen.add(dep.task_key)
                external_deps.append(Dependency(task_key=dep.task_key, outcome=dep.outcome))

    return external_deps


def _rewire_dependencies(
    tasks: list[Activity],
    motif_task_keys: dict[str, str],
) -> None:
    """Rewire dependencies so activities that depended on collapsed activities"""
    for task in tasks:
        if not task.depends_on:
            continue
        new_deps: list[Dependency] = []
        seen_keys: set[str] = set()
        for dep in task.depends_on:
            replacement_key = motif_task_keys.get(dep.task_key)
            effective_key = replacement_key if replacement_key else dep.task_key
            if effective_key not in seen_keys:
                seen_keys.add(effective_key)
                new_deps.append(Dependency(task_key=effective_key, outcome=dep.outcome))
        task.depends_on = new_deps
