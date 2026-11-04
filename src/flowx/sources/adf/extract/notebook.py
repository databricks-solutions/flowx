"""Render the Databricks notebook a user runs themselves to extract their factory.

The notebook embeds ``core.py`` verbatim instead of installing flowx, so it runs in any
workspace with nothing but ``azure-identity`` and ``requests``, and it can't drift from the CLI:
regenerating it picks up the current extractor. A version stamp (the extractor version plus a
short hash of the embedded source) shows which code a given notebook carries.
"""

from __future__ import annotations

import hashlib
import json
from pathlib import Path

from flowx.sources.adf.extract import core

_CELL_SEPARATOR = "\n# COMMAND ----------\n\n"


def embedded_source() -> str:
    """Returns the extractor source the notebook carries (the text of ``core.py``)."""
    return Path(core.__file__).read_text(encoding="utf-8")


def source_fingerprint(source: str) -> str:
    """Returns a short, stable identifier for one version of the embedded extractor.

    Args:
        source: The extractor source, as returned by ``embedded_source``.

    Returns:
        ``v<EXTRACTOR_VERSION>-<first 12 hex digits of its SHA-256>``.
    """
    return f"v{core.EXTRACTOR_VERSION}-{hashlib.sha256(source.encode('utf-8')).hexdigest()[:12]}"


def _markdown(text: str) -> str:
    lines = text.strip("\n").splitlines()
    return "\n".join(["# MAGIC %md"] + [f"# MAGIC {line}".rstrip() for line in lines])


def _widget_defaults(
    *,
    tenant_id: str,
    subscription_id: str,
    resource_group: str,
    factory_name: str,
    output_path: str,
    service_credential: str,
) -> str:
    def literal(value: str) -> str:
        return json.dumps(value)

    default_sign_in = "service_credential" if service_credential else "device_code"

    return f"""\
dbutils.widgets.text("tenant_id", {literal(tenant_id)}, "1. Azure tenant ID")
dbutils.widgets.text("subscription_id", {literal(subscription_id)}, "2. Subscription ID")
dbutils.widgets.text("resource_group", {literal(resource_group)}, "3. Resource group")
dbutils.widgets.text("factory_name", {literal(factory_name)}, "4. Data Factory name")
dbutils.widgets.text("output_path", {literal(output_path)}, "5. Output folder (blank = your home folder)")
dbutils.widgets.dropdown(
    "auth_mode", "{default_sign_in}", ["device_code", "service_credential", "service_principal"], "6. Sign-in"
)
dbutils.widgets.text("service_credential", {literal(service_credential)}, "7. Service credential name")
dbutils.widgets.text("secret_scope", "", "8. Secret scope (service_principal only)")
dbutils.widgets.text("client_id_key", "azure-client-id", "9. Secret key: client ID")
dbutils.widgets.text("client_secret_key", "azure-client-secret", "10. Secret key: client secret")
dbutils.widgets.dropdown("overwrite", "false", ["false", "true"], "11. Replace an existing export")"""


_RUN_CELL = """\
import os

TENANT_ID = dbutils.widgets.get("tenant_id").strip()
SUBSCRIPTION_ID = dbutils.widgets.get("subscription_id").strip()
RESOURCE_GROUP = dbutils.widgets.get("resource_group").strip()
FACTORY_NAME = dbutils.widgets.get("factory_name").strip()
AUTH_MODE = dbutils.widgets.get("auth_mode")
OVERWRITE = dbutils.widgets.get("overwrite") == "true"

missing = [label for label, value in [("tenant_id", TENANT_ID), ("subscription_id", SUBSCRIPTION_ID),
                                      ("resource_group", RESOURCE_GROUP), ("factory_name", FACTORY_NAME)] if not value]
if missing:
    raise ValueError(f"Fill in these widgets first: {', '.join(missing)}")

OUTPUT_PATH = dbutils.widgets.get("output_path").strip()
if not OUTPUT_PATH:
    current_user = spark.sql("SELECT current_user()").first()[0]
    OUTPUT_PATH = f"/Workspace/Users/{current_user}/flowx_adf_export/{FACTORY_NAME}"

SERVICE_CREDENTIAL = dbutils.widgets.get("service_credential").strip()
if AUTH_MODE == "service_credential":
    if not SERVICE_CREDENTIAL:
        raise ValueError("Set the service_credential widget to the name of a Unity Catalog service credential.")
    sign_in = "service-credential"
elif AUTH_MODE == "service_principal":
    scope = dbutils.widgets.get("secret_scope").strip()
    if not scope:
        raise ValueError("Service principal sign-in reads its credentials from a secret scope; set secret_scope.")
    os.environ["AZURE_TENANT_ID"] = TENANT_ID
    os.environ["AZURE_CLIENT_ID"] = dbutils.secrets.get(scope, dbutils.widgets.get("client_id_key").strip())
    os.environ["AZURE_CLIENT_SECRET"] = dbutils.secrets.get(scope, dbutils.widgets.get("client_secret_key").strip())
    sign_in = "default"
else:
    sign_in = "device-code"

manifest = run_extraction(
    subscription_id=SUBSCRIPTION_ID,
    resource_group=RESOURCE_GROUP,
    factory_name=FACTORY_NAME,
    output_dir=OUTPUT_PATH,
    tenant_id=TENANT_ID,
    auth_mode=sign_in,
    service_credential=SERVICE_CREDENTIAL,
    overwrite=OVERWRITE,
    progress=print,
)

print()
print(f"Export written to: {OUTPUT_PATH}")
print(f"Complete: {manifest['complete']}")
for folder, count in manifest["counts"].items():
    print(f"  {folder:<20} {count}")
for warning in manifest["warnings"]:
    print(f"  WARNING: {warning}")"""


def render_notebook(
    *,
    tenant_id: str = "",
    subscription_id: str = "",
    resource_group: str = "",
    factory_name: str = "",
    output_path: str = "",
    service_credential: str = "",
) -> str:
    """Returns the notebook as Databricks ``SOURCE``-format Python, with widgets pre-filled.

    Args:
        tenant_id: Azure tenant ID of the factory.
        subscription_id: Azure subscription ID of the factory.
        resource_group: Resource group of the factory.
        factory_name: Data Factory name.
        output_path: Default output folder; blank means the user's home folder.
        service_credential: Unity Catalog service credential name. When set, the notebook signs in
            with it by default instead of a device code.

    Returns:
        The notebook source, ready for ``databricks workspace import --format SOURCE``.
    """
    source = embedded_source()
    fingerprint = source_fingerprint(source)
    cells = [
        "# Databricks notebook source\n"
        + _markdown(f"""
# Extract Azure Data Factory definitions for flowx

Generated by flowx (`flowx extract --emit-notebook`, extractor {fingerprint}). Run it to copy one
factory's definitions -- pipelines, datasets, linked services, triggers, data flows, integration
runtimes, credentials and global parameters -- into a folder that `flowx discover` reads.
It only **reads** from Azure; nothing in the factory is changed.

**You need**
- **Reader** on the factory, for yourself (or for the service principal or service credential you sign in with).
- The factory's tenant ID, subscription ID, resource group and name (ADF Studio > Manage > Properties).
- Outbound access from this compute to `management.azure.com`.
- Serverless compute, or Databricks Runtime 13.3 LTS or later (Python 3.10+).

**Sign-in**
- `device_code`: the run cell prints a URL and a code; open it and sign in as yourself. Some
  organisations block this sign-in method; if it's refused (for example `AADSTS53003`), use one of the
  options below, set up by an admin, or export the factory from its Git repository or ADF Studio instead.
- `service_principal`: the client ID and secret are read from a **secret scope** -- never type them into a widget.
- `service_credential`: a Unity Catalog **service credential** (an access connector's managed identity)
  with Reader on the factory. No sign-in and no secret, so the notebook can also run as a scheduled job.

The output folder holds the native ADF Git layout plus `_manifest.json`. It contains your
factory's configuration (host names, user names, resource IDs); keep it in your workspace.
"""),
        _markdown("## 1. Install the Azure packages"),
        '# MAGIC %pip install --quiet "azure-identity>=1.17" "requests>=2.31"',
        "dbutils.library.restartPython()",
        _markdown("## 2. Parameters"),
        _widget_defaults(
            tenant_id=tenant_id,
            subscription_id=subscription_id,
            resource_group=resource_group,
            factory_name=factory_name,
            output_path=output_path,
            service_credential=service_credential,
        ),
        _markdown(f"## 3. Extractor (flowx {fingerprint})\nGenerated from flowx; don't edit -- regenerate instead."),
        source.rstrip("\n"),
        _markdown("## 4. Sign in and extract"),
        _RUN_CELL,
        _markdown("""
## 5. Next step

Go back to your assistant and tell it the export path printed above. It checks the folder and runs
`flowx discover` on it. A `/Workspace/...` path is read as a workspace folder, a `/Volumes/...` path
as a Unity Catalog volume.
"""),
    ]
    return _CELL_SEPARATOR.join(cells) + "\n"
