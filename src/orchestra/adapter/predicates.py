"""Pure IR predicates shared by the agent adapter and the pipeline modifier."""

from __future__ import annotations

from typing import Final

from orchestra.adapter.constants import COPY_SOURCE_QUERY_KEYS, DATABASE_SOURCE_TOKENS, DELTA_SINK_TOKENS
from orchestra.models.ir import (
    Activity,
    AppendVariableActivity,
    CopyActivity,
    DeleteActivity,
    FilterActivity,
    ForEachActivity,
    IfConditionActivity,
    LookupActivity,
    MotifActivity,
    NotebookActivity,
    SetVariableActivity,
    SparkPythonActivity,
    SwitchActivity,
    WaitActivity,
    WebActivity,
)

_NON_DATABRICKS_ACTIVITY_TYPES: Final[tuple[type[Activity], ...]] = (
    CopyActivity,
    LookupActivity,
    WebActivity,
    DeleteActivity,
    WaitActivity,
    FilterActivity,
    SetVariableActivity,
    AppendVariableActivity,
    MotifActivity,
)


def walk_activities(activities: list[Activity]) -> list[Activity]:
    """Flattens an activity tree by descending into every control-flow body.

    Args:
        activities: Top-level activity list from a :class:`Pipeline`.

    Returns:
        Flat list of every activity, including those nested inside
        ForEach, IfCondition, and Switch bodies.
    """
    flattened: list[Activity] = []
    for activity in activities:
        flattened.append(activity)
        flattened.extend(_child_activities(activity))
    return flattened


def copy_targets_delta(activity: CopyActivity) -> bool:
    """Reports whether a Copy activity's sink resolves to a Delta table.

    Args:
        activity: Copy activity to inspect.

    Returns:
        ``True`` when the activity's sink format, dataset type, or
        properties indicate a Delta sink; ``False`` otherwise.
    """
    if activity.sink_format and activity.sink_format.lower() in DELTA_SINK_TOKENS:
        return True
    sink_dataset_type = (activity.sink_dataset_type or "").lower()
    if any(token in sink_dataset_type for token in DELTA_SINK_TOKENS):
        return True
    sink_properties = activity.sink_properties or {}
    return bool(sink_properties.get("table"))


def copy_has_source_query(activity: CopyActivity) -> bool:
    """Reports whether a Copy activity reads its source via an explicit SQL query.

    Args:
        activity: Copy activity to inspect.

    Returns:
        ``True`` when one of the well-known query fields
        (``query``, ``sqlReaderQuery``, ``sql_query``) is populated on
        ``source_properties``; ``False`` otherwise (table-based read).
    """
    source_properties = activity.source_properties or {}
    return any(source_properties.get(key) for key in COPY_SOURCE_QUERY_KEYS)


def copy_query_is_parseable_for_lfc(activity: CopyActivity) -> bool:
    """Reports whether a Copy's source query decomposes into LFC fields.

    Args:
        activity: Copy activity to inspect.

    Returns:
        ``True`` when the translator's query analyzer concluded the
        query can be expressed as the LFC query-based connector's
        structured fields (cursor / row_filter / include_columns /
        exclude_columns).  ``False`` for table-based Copies or Copies
        whose query contains constructs LFC cannot represent.
    """
    source_properties = activity.source_properties or {}
    return bool(source_properties.get("query_parseable_for_lfc"))


def copy_query_has_cursor_column(activity: CopyActivity) -> bool:
    """Reports whether the Copy's query analysis found a cursor column candidate.

    Args:
        activity: Copy activity to inspect.

    Returns:
        ``True`` when ``source_properties.query_cursor_column`` is set
        (the translator detected a range or BETWEEN predicate suitable
        for the LFC connector's cursor field).
    """
    source_properties = activity.source_properties or {}
    return bool(source_properties.get("query_cursor_column"))


def copy_eligible_for_lfc_cdc(activity: CopyActivity) -> bool:
    """Reports whether a Copy can be migrated to the LFC CDC connector.

    Args:
        activity: Copy activity to inspect.

    Returns:
        ``True`` when the Copy reads a database table directly (no
        ``sqlReaderQuery``) and has both a database source and a Delta
        sink.  The CDC connector reads change events without needing a
        cursor column, so table-based reads are always eligible.
    """
    return not copy_has_source_query(activity) and has_database_source(activity) and copy_targets_delta(activity)


def copy_eligible_for_lfc_query_based(activity: CopyActivity) -> bool:
    """Reports whether a Copy can be migrated to the LFC query-based connector.

    Args:
        activity: Copy activity to inspect.

    Returns:
        ``True`` when the Copy has a database source, targets Delta,
        the source query parses into the LFC query-based connector
        fields, and the query contains a range predicate that can
        drive the connector's cursor column.
    """
    if not copy_has_source_query(activity):
        return False
    if not has_database_source(activity):
        return False
    if not copy_targets_delta(activity):
        return False
    if not copy_query_is_parseable_for_lfc(activity):
        return False
    return copy_query_has_cursor_column(activity)


def copy_eligible_for_any_lfc_connector(activity: CopyActivity) -> bool:
    """Reports whether a Copy is eligible for at least one LFC connector flavour.

    Args:
        activity: Copy activity to inspect.

    Returns:
        ``True`` when either :func:`copy_eligible_for_lfc_cdc` or
        :func:`copy_eligible_for_lfc_query_based` returns ``True``.
    """
    return copy_eligible_for_lfc_cdc(activity) or copy_eligible_for_lfc_query_based(activity)


def copy_query_unfit_for_lfc(activity: CopyActivity) -> bool:
    """Reports whether a Copy carries a query that LFC cannot represent.

    Args:
        activity: Copy activity to inspect.

    Returns:
        ``True`` when the activity has a source query but the
        translator's analyzer marked it as not parseable (the query
        contains JOIN, GROUP BY, aggregates, UNION, window functions,
        subqueries, or column expressions).  Such Copies should be
        translated through PySpark notebooks regardless of paradigm
        preference because LFC's query-based connector and SDP's
        declarative table form both reject the query.
    """
    if not copy_has_source_query(activity):
        return False
    return not copy_query_is_parseable_for_lfc(activity)


def has_database_source(activity: CopyActivity) -> bool:
    """Reports whether a Copy activity's source is SQL Server, MySQL, or PostgreSQL.

    Args:
        activity: Copy activity to inspect.

    Returns:
        ``True`` when the source type contains a known database token;
        ``False`` otherwise.
    """
    source_type = (activity.source_type or "").lower()
    return any(token in source_type for token in DATABASE_SOURCE_TOKENS)


def is_non_databricks_task(activity: Activity) -> bool:
    """Reports whether *activity* runs outside the Databricks notebook surface.

    Args:
        activity: Activity to classify.

    Returns:
        ``True`` for Copy, Lookup, Web, Delete, Wait, Filter, variable,
        and motif activities; ``False`` for ADF DatabricksNotebook and
        DatabricksSparkPython tasks.
    """
    if isinstance(activity, (NotebookActivity, SparkPythonActivity)):
        return False
    return isinstance(activity, _NON_DATABRICKS_ACTIVITY_TYPES)


def _child_activities(activity: Activity) -> list[Activity]:
    """Returns the activities nested inside a control-flow activity.

    Args:
        activity: Activity to descend into.

    Returns:
        Flattened list of child activities, or an empty list when
        *activity* is a leaf type.
    """
    if isinstance(activity, ForEachActivity):
        return walk_activities(activity.inner_activities)
    if isinstance(activity, IfConditionActivity):
        return walk_activities(activity.if_true_activities + activity.if_false_activities)
    if isinstance(activity, SwitchActivity):
        nested: list[Activity] = []
        for case in activity.cases:
            nested.extend(walk_activities(case.activities))
        nested.extend(walk_activities(activity.default_activities))
        return nested
    return []
