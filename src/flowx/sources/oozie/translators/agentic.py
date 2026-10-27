"""Turn an Oozie action flowx can't translate deterministically into a counted, explained placeholder."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from flowx.models.ir import PlaceholderActivity
from flowx.sources.oozie.loader.graph import TranslatedAction

if TYPE_CHECKING:
    from flowx.sources.oozie.loader.ast import ActionNode
    from flowx.sources.oozie.translators import ActionContext


def translate_agentic(
    action: ActionNode, context: ActionContext, reason: str, hints: dict[str, Any] | None = None
) -> TranslatedAction:
    """Returns a placeholder carrying the raw XML, the reason, and any parsed hints for the agent."""
    context.environment.agentic_actions.add(action.name)
    context.report.agentic_reasons[context.task_key] = reason
    context.report.note("AGENTIC-ACTION", action.name, reason)
    context.report.gap(
        "agentic_action",
        f"action {action.name!r} ({action.action_type}): {reason}",
        {"task_key": context.task_key, "oozie_type": action.action_type},
    )
    return TranslatedAction(
        main=PlaceholderActivity(
            name=action.name,
            task_key=context.task_key,
            original_type=f"oozie:{action.action_type}",
            comment=reason,
            raw_definition={"xml": action.raw_xml, "hints": hints or {}},
        )
    )
