"""Preparer for WebActivity -> notebook_task with generated HTTP notebook."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from orchestra.models.dab import SecretInstruction, SetupTask
from orchestra.preparer.activity_preparers.helpers import (
    build_notebook_activity_task,
    resolve_param_value,
)
from orchestra.preparer.activity_preparers.naming import notebook_filename
from orchestra.preparer.code_generator import generate_web_activity_notebook
from orchestra.preparer.workflow_preparer import PreparedActivity

if TYPE_CHECKING:
    from orchestra.models.ir import WebActivity


def prepare(activity: WebActivity, *, scope: str = "") -> PreparedActivity:
    """Converts a WebActivity into a notebook_task with a generated HTTP notebook."""
    secrets, setup_tasks = _extract_secrets_and_setup(activity, scope=scope)

    # C-38 (LSC4-002): when the preparer resolved an AzureKeyVaultSecret
    # payload to a real (scope, key) pair, thread it into the notebook
    # generator so the rendered ``dbutils.secrets.get`` references the
    # real values rather than the hard-coded ``scope=task_key,
    # key='auth-credential'`` fallback.
    credential_scope: str | None = None
    credential_key: str | None = None
    if secrets:
        first = secrets[0]
        credential_scope = first.scope
        credential_key = first.key

    task, notebooks = build_notebook_activity_task(
        activity,
        notebook_relative_path=f"notebooks/{notebook_filename(activity.task_key, activity.name)}",
        notebook_content=generate_web_activity_notebook(
            activity,
            scope=scope,
            credential_scope=credential_scope,
            credential_key=credential_key,
        ),
        base_parameters={
            "url": resolve_param_value(activity.url),
            "method": resolve_param_value(activity.method),
        },
    )

    return PreparedActivity(
        task=task,
        notebooks=notebooks,
        secrets=secrets,
        setup_tasks=setup_tasks,
    )


def _extract_secrets_and_setup(
    activity: WebActivity, *, scope: str = ""
) -> tuple[list[SecretInstruction], list[SetupTask]]:
    """Inspects the Web activity's authentication payload and emits per-secret refs.

    C-11 (LSC2-005): the legacy implementation always emitted a single
    static ``auth-credential`` SecretInstruction regardless of the
    underlying ADF auth shape, so AzureKeyVaultSecret payloads lost their
    Key Vault scope/secret name and CredentialReference (MSI) payloads
    surfaced a placeholder secret that never matches a real secret in
    the workspace.
    """
    secrets: list[SecretInstruction] = []
    setup_tasks: list[SetupTask] = []

    auth = activity.authentication or {}
    if not auth:
        return secrets, setup_tasks

    auth_type = auth.get("type", "unknown")
    default_scope = scope or activity.task_key

    # Common nested fields per ADF auth shapes.
    for field_name in ("password", "secret", "clientSecret", "pfx", "key"):
        field_value = auth.get(field_name)
        secret = _materialise_secret(field_value, default_scope=default_scope, role=field_name)
        if secret is not None:
            secrets.append(secret)

    if auth_type == "MSI" or auth.get("credential"):
        # CredentialReference (managed identity) has no static secret -- emit
        # a SETUP.md note instead of a fake placeholder secret.
        cred = auth.get("credential") or {}
        cred_name = cred.get("referenceName") if isinstance(cred, dict) else None
        setup_tasks.append(
            SetupTask(
                type="manual_credential",
                config={
                    "activity_name": activity.name,
                    "credential_reference": cred_name or "<unspecified>",
                    "note": (
                        "Web activity uses an Azure managed-identity credential. "
                        "Configure equivalent OAuth or service-principal auth in Databricks "
                        "and update the generated notebook."
                    ),
                },
            )
        )

    if not secrets and auth_type not in ("MSI",) and not auth.get("credential"):
        # Fallback for shapes the per-field probe didn't recognise -- preserve
        # the legacy behaviour so callers depending on it still get something.
        secrets.append(
            SecretInstruction(
                scope=default_scope,
                key="auth-credential",
                value_source=f"Authentication credential ({auth_type}) for web activity '{activity.name}'",
            )
        )

    return secrets, setup_tasks


def _materialise_secret(value: Any, *, default_scope: str, role: str) -> SecretInstruction | None:
    """Builds a :class:`SecretInstruction` from an ADF secret payload.

    Handles the two common shapes:
    - ``{"type": "AzureKeyVaultSecret", "store": {"referenceName": ...}, "secretName": ...}``
    - ``{"type": "SecureString", "value": ...}``

    Returns ``None`` for shapes we cannot map.
    """
    if not isinstance(value, dict):
        return None
    payload_type = value.get("type")
    if payload_type == "AzureKeyVaultSecret":
        store = value.get("store") or {}
        scope_name = store.get("referenceName") or default_scope
        secret_name = value.get("secretName") or role
        base_url = (value.get("typeProperties") or {}).get("baseUrl", "")
        value_source = f"Azure Key Vault secret '{secret_name}'"
        if base_url:
            value_source += f" at {base_url}"
        return SecretInstruction(
            scope=str(scope_name),
            key=str(secret_name),
            value_source=value_source,
        )
    if payload_type == "SecureString":
        return SecretInstruction(
            scope=default_scope,
            key=role,
            value_source=f"SecureString carried inline in the ADF activity (role={role})",
        )
    return None
