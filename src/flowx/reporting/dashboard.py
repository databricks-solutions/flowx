"""Install a published AI/BI (Lakeview) dashboard that visualizes migration coverage.

Builds a dashboard from :data:`dashboard_template.json` (datasets + widgets over the
results table written by :mod:`reporting.results`), creates it via the Databricks SDK
Lakeview API, and publishes it so it is immediately viewable.
"""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any

from flowx.reporting.results import resolve_warehouse_id

logger = logging.getLogger(__name__)

_TEMPLATE_PATH = Path(__file__).with_name("dashboard_template.json")
_TABLE_PLACEHOLDER = "{{RESULTS_TABLE}}"


def build_serialized_dashboard(table_fqn: str) -> str:
    """Returns the serialized Lakeview dashboard JSON for *table_fqn*.

    Substitutes the ``{{RESULTS_TABLE}}`` placeholder in every dataset query with the
    fully-qualified results table, and returns a compact JSON string suitable for the
    Lakeview ``serialized_dashboard`` field.

    Raises:
        ValueError: When *table_fqn* is empty.
    """
    if not table_fqn:
        raise ValueError("table_fqn is required to build the coverage dashboard")
    spec = json.loads(_TEMPLATE_PATH.read_text(encoding="utf-8"))
    for dataset in spec.get("datasets", []):
        dataset["queryLines"] = [line.replace(_TABLE_PLACEHOLDER, table_fqn) for line in dataset.get("queryLines", [])]
    return json.dumps(spec)


def _default_parent_path(client: Any) -> str:
    """Returns ``/Workspace/Users/<current user>`` for the dashboard's parent folder."""
    try:
        user = client.current_user.me().user_name
        if user:
            return f"/Workspace/Users/{user}"
    except Exception as exc:  # noqa: BLE001 - fall back to /Workspace
        logger.debug("Could not resolve current user for parent path: %s", exc)
    return "/Workspace"


def install_dashboard(
    table_fqn: str,
    warehouse_id: str | None = None,
    display_name: str | None = None,
    parent_path: str | None = None,
    client: Any | None = None,
) -> tuple[str, str]:
    """Creates and publishes the migration-coverage dashboard.

    Args:
        table_fqn: Results table the dashboard reads (``catalog.schema.table``).
        warehouse_id: SQL warehouse backing the dashboard; auto-detected when omitted.
        display_name: Dashboard name; defaults to ``Migration Coverage — <table>``.
        parent_path: Workspace folder; defaults to the current user's home.
        client: Optional ``WorkspaceClient`` (injected in tests).

    Returns:
        ``(dashboard_id, url)`` -- ``url`` is best-effort (empty when host is unknown).
    """
    from databricks.sdk.service.dashboards import Dashboard

    if client is None:
        from flowx.preparer.workspace_downloader import _get_workspace_client

        client = _get_workspace_client()

    resolved_wh = resolve_warehouse_id(client, warehouse_id)
    serialized = build_serialized_dashboard(table_fqn)
    name = display_name or f"Migration Coverage — {table_fqn}"
    parent = parent_path or _default_parent_path(client)

    created = client.lakeview.create(
        dashboard=Dashboard(
            display_name=name,
            serialized_dashboard=serialized,
            warehouse_id=resolved_wh,
            parent_path=parent,
        )
    )
    dashboard_id = created.dashboard_id
    client.lakeview.publish(dashboard_id=dashboard_id, warehouse_id=resolved_wh)

    host = str(getattr(getattr(client, "config", None), "host", "") or "").rstrip("/")
    url = f"{host}/sql/dashboardsv3/{dashboard_id}" if host else ""
    logger.info("Installed coverage dashboard '%s' (%s).", name, dashboard_id)
    return dashboard_id, url
