"""MCP packaging for flowx: exposes the migration phases and adapter operations as MCP tools.

``build_server`` / ``build_http_app`` require the ``mcp`` extra (``pip install -e .[mcp]``) and are
imported lazily so :mod:`flowx.mcp.runner` stays usable without it.
"""

from typing import Any

__all__ = ["build_server", "build_http_app"]


def __getattr__(name: str) -> Any:
    # Lazy re-export: import server (and the `mcp` extra) only when these names are accessed.
    if name in ("build_server", "build_http_app"):
        from flowx.mcp import server

        return getattr(server, name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
