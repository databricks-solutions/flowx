"""Oozie loader: find application directories and turn their XML into flowx Pipeline IR."""

from __future__ import annotations

from typing import Any

from flowx.sources.oozie.loader.discovery import discover_apps

__all__ = ["discover_apps", "load_app", "load_pipelines"]


def __getattr__(name: str) -> Any:
    # ``api`` imports the translators, which import loader submodules; importing it lazily lets
    # ``flowx.sources.oozie.translators`` be the first Oozie module anyone imports without a circular import.
    if name in ("load_app", "load_pipelines"):
        from flowx.sources.oozie.loader import api

        return getattr(api, name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
