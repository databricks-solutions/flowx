"""Databricks App entry point for the flowx MCP server.

Databricks Apps run this module's ``command`` from ``app.yaml``. The flowx
package is vendored alongside this file by ``deploy.sh`` (into ``flowx/``),
so it imports directly without a separate install step.

The module also exposes ``app`` so it can be served with ``uvicorn app:app``.
"""

import os

from flowx.mcp.server import build_http_app

app = build_http_app()

if __name__ == "__main__":
    import uvicorn

    # Databricks Apps inject the port to bind via DATABRICKS_APP_PORT.
    port = int(os.environ.get("DATABRICKS_APP_PORT", "8000"))
    uvicorn.run(app, host="0.0.0.0", port=port)
