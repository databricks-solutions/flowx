"""Resolves collapsed ``activity_and_notify`` specs into DAB task notifications.

Email specs become ``email_notifications`` from raw addresses; Slack/Teams/PagerDuty/Generic Webhook
specs reuse the destination id provisioned at modify time (``provision_destination``), creating one
via the SDK only as a fallback when the spec carries no pre-resolved id.
"""

from __future__ import annotations

import logging
from typing import Any

from flowx.models.dab import SetupTask

logger = logging.getLogger(__name__)

_WEBHOOK_DESTINATIONS = frozenset({"slack", "teams", "pagerduty", "webhook"})

# Optional SDK config kwargs per destination (beyond the primary field); only passed when the
# user supplied a value, so the SDK defaults anything left blank.
_DESTINATION_CONFIG_FIELDS: dict[str, tuple[str, ...]] = {
    "slack": ("url", "channel_id", "oauth_token"),
    "teams": ("url",),
    "webhook": ("url", "username", "password"),
    "pagerduty": ("integration_key",),
}


def resolve_task_notifications(spec: dict[str, Any]) -> tuple[dict[str, Any], list[SetupTask]]:
    """Return ``(task_notification_keys, setup_tasks)`` for a notification spec.

    ``task_notification_keys`` is merged into the DAB task dict (it carries an
    ``email_notifications`` or ``webhook_notifications`` entry).  ``setup_tasks``
    is non-empty only when a webhook-style destination could not be created
    (e.g. no workspace auth at prepare time), in which case a documentation
    SetupTask is emitted instead and the task ships without notifications.
    """
    destination = spec.get("destination", "")
    events: list[str] = spec.get("events") or ["on_failure"]
    args: dict[str, Any] = spec.get("args") or {}

    if destination == "email":
        recipients = [recipient for recipient in (args.get("addresses") or []) if recipient]
        if not recipients:
            logger.warning("activity_and_notify email destination has no recipients; skipping notification wiring.")
            return {}, []
        return {"email_notifications": {event: list(recipients) for event in events}}, []

    if destination not in _WEBHOOK_DESTINATIONS:
        return {}, []

    display_name = spec.get("destination_name") or f"flowx-{destination}"
    # Prefer a destination id provisioned at modify time; create one here only when the report
    # lacks a pre-resolved id (e.g. no workspace auth was available then).
    destination_id = spec.get("destination_id") or _ensure_destination(destination, display_name, args)
    if destination_id is None:
        setup = SetupTask(
            type="notification_destination",
            config={
                "destination": destination,
                "display_name": display_name,
                **{key: value for key, value in args.items() if value},
                "note": (
                    "Could not create this notification destination during prepare. Create it "
                    "(Settings > Notifications, or w.notification_destinations.create), then add "
                    '{"id": "<destination-id>"} to the task\'s webhook_notifications.'
                ),
            },
        )
        return {}, [setup]

    return {"webhook_notifications": {event: [{"id": destination_id}] for event in events}}, []


def provision_destination(spec: dict[str, Any]) -> tuple[dict[str, Any], str]:
    """Create (or reuse) the SDK notification destination for *spec* at prompt time.

    Called from the adapter ``modify`` phase right after the user answers the
    notification follow-ups.  For non-email destinations it creates (or reuses by
    display name) the Databricks notification destination via the SDK and returns a
    copy of *spec* augmented with the resolved ``destination_id`` so prepare can wire
    it without another SDK call.

    Email specs (and anything that is not a webhook-style destination) pass through
    unchanged -- email uses raw ``email_notifications`` and needs no destination.

    On failure the spec is returned unchanged (its ``args`` retained), so prepare can
    retry the create or fall back to a ``notification_destination`` setup task.

    Returns:
        ``(spec, status_message)`` where ``status_message`` is a human-readable line
        describing the outcome (empty for email / no-op).
    """
    destination = spec.get("destination", "")
    if destination == "email" or destination not in _WEBHOOK_DESTINATIONS:
        return spec, ""
    display_name = spec.get("destination_name") or f"flowx-{destination}"
    if spec.get("destination_id"):
        return (
            spec,
            f"Notification destination '{display_name}' ({destination}) already resolved -> {spec['destination_id']}.",
        )
    destination_id = _ensure_destination(destination, display_name, spec.get("args") or {})
    if destination_id is None:
        return spec, (
            f"WARNING: could not create notification destination '{display_name}' ({destination}) now; "
            "prepare will retry or emit a setup task."
        )
    return {**spec, "destination_id": destination_id}, (
        f"Created/reused notification destination '{display_name}' ({destination}) -> {destination_id}."
    )


def _build_destination_config(sdk_settings: Any, destination: str, args: dict[str, Any]) -> Any | None:
    """Build the SDK ``settings.Config`` for *destination* from the resolved *args*.

    Only kwargs the user supplied are passed; blank optional fields are omitted so
    the SDK applies its own defaults.
    """
    fields = _DESTINATION_CONFIG_FIELDS.get(destination)
    if fields is None:
        return None
    kwargs = {field: args[field] for field in fields if args.get(field)}
    if destination == "slack":
        return sdk_settings.Config(slack=sdk_settings.SlackConfig(**kwargs))
    if destination == "teams":
        return sdk_settings.Config(microsoft_teams=sdk_settings.MicrosoftTeamsConfig(**kwargs))
    if destination == "webhook":
        return sdk_settings.Config(generic_webhook=sdk_settings.GenericWebhookConfig(**kwargs))
    if destination == "pagerduty":
        return sdk_settings.Config(pagerduty=sdk_settings.PagerdutyConfig(**kwargs))
    return None


def _ensure_destination(destination: str, display_name: str, args: dict[str, Any]) -> str | None:
    """Create (or reuse) a notification destination via the SDK; return its id or None."""
    try:
        from databricks.sdk.service import settings as sdk_settings

        from flowx.preparer.workspace_downloader import _get_workspace_client

        client = _get_workspace_client()
        # Reuse an existing destination with the same display name so prepare is idempotent.
        for existing in client.notification_destinations.list():
            if getattr(existing, "display_name", None) == display_name and getattr(existing, "id", None):
                logger.info("Reusing existing notification destination '%s' (%s).", display_name, existing.id)
                return existing.id

        config = _build_destination_config(sdk_settings, destination, args)
        if config is None:
            return None

        created = client.notification_destinations.create(display_name=display_name, config=config)
        logger.info("Created notification destination '%s' (%s).", display_name, getattr(created, "id", None))
        return getattr(created, "id", None)
    except Exception as exc:  # noqa: BLE001 - degrade gracefully to a setup task
        logger.warning("Notification destination create failed for '%s' (%s): %s", display_name, destination, exc)
        return None
