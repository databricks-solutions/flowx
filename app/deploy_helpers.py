"""Helpers for deploying the flowx MCP server as a Databricks App via the Python SDK.

Kept separate from ``deploy_app`` (the deployment notebook) so the deployment logic is
importable and unit-testable outside a notebook session. Every function takes an explicit
``WorkspaceClient`` and plain paths, so nothing here depends on ``dbutils`` or a notebook
runtime.

Mirrors what ``deploy.sh`` does, minus the CLI: assemble a self-contained source bundle
(``app.py``, ``app.yaml``, ``requirements.txt``, and a vendored copy of the ``flowx`` package)
at a workspace path, then create/deploy the app from it.
"""

import os

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.apps import App, AppDeployment, AppDeploymentMode
from databricks.sdk.service.workspace import ImportFormat

# Top-level files the app needs alongside the vendored flowx package.
APP_BUNDLE_FILES = ("app.py", "app.yaml", "requirements.txt")


def validate_repo_root(repo_root: str) -> None:
    """Raise if ``repo_root`` is not a flowx checkout (missing app/app.py or src/flowx)."""
    if not repo_root:
        raise ValueError(
            "repo_root is required: set it to the flowx checkout — the directory "
            "containing app/ and src/flowx."
        )
    has_app_entrypoint = os.path.isfile(os.path.join(repo_root, "app", "app.py"))
    has_flowx_package = os.path.isdir(os.path.join(repo_root, "src", "flowx"))
    if not (has_app_entrypoint and has_flowx_package):
        raise ValueError(
            f"{repo_root!r} does not look like a flowx checkout "
            "(expected app/app.py and src/flowx)."
        )


def upload_file(workspace_client: WorkspaceClient, local_path: str, workspace_path: str) -> None:
    """Upload one local file to a workspace path verbatim (RAW), creating parent dirs."""
    workspace_client.workspace.mkdirs(os.path.dirname(workspace_path))
    with open(local_path, "rb") as file_handle:
        workspace_client.workspace.upload(
            workspace_path, file_handle, format=ImportFormat.RAW, overwrite=True
        )


def upload_directory(
    workspace_client: WorkspaceClient, local_directory: str, workspace_directory: str
) -> int:
    """Recursively upload a directory tree, skipping ``__pycache__`` and ``.pyc`` files.

    Returns the number of files uploaded.
    """
    uploaded_file_count = 0
    for current_directory, subdirectories, file_names in os.walk(local_directory):
        subdirectories[:] = [name for name in subdirectories if name != "__pycache__"]
        for file_name in file_names:
            if file_name.endswith(".pyc"):
                continue
            local_path = os.path.join(current_directory, file_name)
            relative_path = os.path.relpath(local_path, local_directory)
            workspace_path = f"{workspace_directory}/{relative_path.replace(os.sep, '/')}"
            upload_file(workspace_client, local_path, workspace_path)
            uploaded_file_count += 1
    return uploaded_file_count


def stage_app_bundle(
    workspace_client: WorkspaceClient, repo_root: str, source_code_path: str
) -> int:
    """Assemble the self-contained app bundle at ``source_code_path`` in the workspace.

    Uploads the top-level app files and a fresh copy of the ``flowx`` package (the prior
    copy is deleted first so removed modules do not linger). Returns the flowx module count.
    """
    app_directory = os.path.join(repo_root, "app")
    workspace_client.workspace.mkdirs(source_code_path)

    for file_name in APP_BUNDLE_FILES:
        upload_file(
            workspace_client,
            os.path.join(app_directory, file_name),
            f"{source_code_path}/{file_name}",
        )

    package_source_directory = os.path.join(repo_root, "src", "flowx")
    package_workspace_directory = f"{source_code_path}/flowx"
    try:
        workspace_client.workspace.delete(package_workspace_directory, recursive=True)
    except Exception:
        pass  # first deploy: nothing to delete
    return upload_directory(
        workspace_client, package_source_directory, package_workspace_directory
    )


def ensure_app_exists(
    workspace_client: WorkspaceClient, app_name: str, app_description: str
) -> bool:
    """Create the app if it does not already exist. Returns True if it was created."""
    try:
        workspace_client.apps.get(name=app_name)
        return False
    except Exception:
        workspace_client.apps.create_and_wait(App(name=app_name, description=app_description))
        return True


def deploy_app_source(
    workspace_client: WorkspaceClient, app_name: str, source_code_path: str
) -> AppDeployment:
    """Deploy the staged source to the app and block until deployment reaches terminal state."""
    return workspace_client.apps.deploy_and_wait(
        app_name=app_name,
        app_deployment=AppDeployment(
            source_code_path=source_code_path,
            mode=AppDeploymentMode.SNAPSHOT,
        ),
    )
