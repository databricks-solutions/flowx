"""Oozie action translators, one per action type, dispatched by the action's element name."""

from __future__ import annotations

import logging
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from flowx.sources.oozie.loader.ast import ActionNode
from flowx.sources.oozie.loader.discovery import OozieApp
from flowx.sources.oozie.loader.el_render import ElEnvironment
from flowx.sources.oozie.loader.graph import TranslatedAction
from flowx.sources.oozie.loader.report import AppReport
from flowx.sources.oozie.translators.agentic import translate_agentic

logger = logging.getLogger(__name__)


@dataclass(slots=True, kw_only=True)
class ActionContext:
    task_key: str
    app: OozieApp
    environment: ElEnvironment
    report: AppReport


class AgenticAction(Exception):
    """Raised by a translator that finds it can't translate this particular action faithfully."""

    def __init__(self, reason: str, hints: dict[str, Any] | None = None) -> None:
        super().__init__(reason)
        self.reason = reason
        self.hints = hints or {}


_AGENTIC_REASONS: dict[str, str] = {
    "pig": "Pig scripts need rewriting as Spark or SQL",
    "sqoop": "Sqoop transfers should move to Lakeflow Connect or a JDBC read",
    "map-reduce": "MapReduce jobs need rewriting as Spark",
    "streaming": "Hadoop streaming jobs need rewriting as Spark",
    "ssh": "SSH actions run commands on hosts Databricks can't reach",
}
_PLANNED_FOR_M2 = frozenset({"hive", "hive2", "shell", "fs", "java", "sub-workflow", "email", "distcp"})

TRANSLATORS: dict[str, Callable[[ActionNode, ActionContext], TranslatedAction]] = {}

_ROLLED_BACK_REPORT_LISTS = (
    "hdfs_paths",
    "missing_files",
    "dropped_spark_conf",
    "credentials_dropped",
    "findings",
    "transformations",
)


def _report_list_lengths(context: ActionContext) -> dict[str, int]:
    return {name: len(getattr(context.report, name)) for name in _ROLLED_BACK_REPORT_LISTS}


def _roll_back_report(context: ActionContext, lengths: dict[str, int]) -> None:
    """Drops whatever a failed translator recorded, so the report only describes what the placeholder does."""
    for name, length in lengths.items():
        del getattr(context.report, name)[length:]


def translate_action(action: ActionNode, context: ActionContext) -> TranslatedAction:
    """Translates one action, falling back to an agentic placeholder whenever that's the honest answer."""
    context.report.node_types[context.task_key] = action.action_type
    translator = TRANSLATORS.get(action.action_type)
    if translator is None:
        if action.action_type in _AGENTIC_REASONS:
            reason = _AGENTIC_REASONS[action.action_type]
        elif action.action_type in _PLANNED_FOR_M2:
            reason = f"{action.action_type} actions are planned for M2"
        else:
            reason = f"custom action <{action.action_type}> has no known translation"
        return translate_agentic(action, context, reason)
    lengths = _report_list_lengths(context)
    try:
        return translator(action, context)
    except AgenticAction as signal:
        _roll_back_report(context, lengths)
        return translate_agentic(action, context, signal.reason, signal.hints)
    except Exception as error:
        logger.exception("Oozie translator for <%s> failed on action %r", action.action_type, action.name)
        _roll_back_report(context, lengths)
        reason = f"translator for <{action.action_type}> failed: {type(error).__name__}: {error}"
        return translate_agentic(action, context, reason, hints={"error": repr(error)})


from flowx.sources.oozie.translators.spark import translate_spark  # noqa: E402 - avoids an import cycle

TRANSLATORS["spark"] = translate_spark
