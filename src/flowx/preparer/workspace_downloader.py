"""Download workspace artifacts (notebooks, scripts, JARs) via Databricks SDK."""

from __future__ import annotations

import base64
import configparser
import logging
import os
import sys
from collections.abc import Iterable
from pathlib import Path

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Databricks runtime detection and auto-configuration
# ---------------------------------------------------------------------------

_WORKSPACE_ROOT = Path("/Workspace")

# Common notebook file extensions on the Databricks workspace filesystem.
_NOTEBOOK_EXTENSIONS = (".py", ".sql", ".scala", ".r", ".R", ".ipynb")


def _is_databricks_runtime() -> bool:
    """Returns True if running inside a Databricks cluster or serverless compute."""
    return os.environ.get("DATABRICKS_RUNTIME_VERSION") is not None


def _local_workspace_accessible() -> bool:
    """Returns True if the /Workspace filesystem is mounted and readable."""
    return _WORKSPACE_ROOT.is_dir()


def _try_local_workspace_read(workspace_path: str) -> str | None:
    """Attempts to read a notebook directly from the local /Workspace filesystem.

    On Databricks compute (classic or serverless), workspace files are mounted
    at ``/Workspace/<path>``.  Notebooks are stored with a language extension
    (e.g. ``.py``, ``.sql``).  This function probes the path with each known
    extension and returns the source if found — no SDK auth required.

    Args:
        workspace_path: Logical workspace path (e.g. ``"/Shared/ETL/transform"``).

    Returns:
        Notebook source as a string, or ``None`` if not found locally.
    """
    if not _local_workspace_accessible():
        return None

    base = _WORKSPACE_ROOT / workspace_path.lstrip("/")

    # Try the exact path first (already has an extension or is a plain file)
    if base.is_file():
        try:
            return base.read_text(encoding="utf-8")
        except OSError as exc:
            logger.debug("Local read failed for %s: %s", base, exc)

    # Probe with common notebook extensions
    for ext in _NOTEBOOK_EXTENSIONS:
        candidate = base.with_suffix(ext)
        if candidate.is_file():
            try:
                content = candidate.read_text(encoding="utf-8")
                logger.info("Read notebook from local filesystem: %s", candidate)
                return content
            except OSError as exc:
                logger.debug("Local read failed for %s: %s", candidate, exc)

    return None


def _ensure_databricks_runtime_auth() -> bool:
    """Auto-configures ~/.databrickscfg from the notebook runtime context.

    On Databricks serverless (or classic cluster) compute, no CLI auth is
    pre-configured but the runtime provides host + token via the REPL context.
    This function detects that situation and writes a DEFAULT profile so the
    Databricks SDK can authenticate transparently.
    """
    cfg_path = _get_databrickscfg_path()
    if cfg_path.exists() and cfg_path.stat().st_size > 0:
        return True

    if os.environ.get("DATABRICKS_HOST") and os.environ.get("DATABRICKS_TOKEN"):
        return True

    try:
        from dbruntime.databricks_repl_context import get_context  # type: ignore[import-not-found]

        context = get_context()
        host = f"https://{context.browserHostName}"
        token = context.apiToken
        if not host or not token:
            logger.warning("Databricks runtime detected but host/token unavailable from context")
            return False

        cfg_path.parent.mkdir(parents=True, exist_ok=True)
        with open(cfg_path, "w") as f:
            f.write(f"[DEFAULT]\nhost = {host}\ntoken = {token}\n")
        logger.info("Auto-configured Databricks auth from runtime context -> %s", cfg_path)
        return True
    except ImportError:
        logger.debug("dbruntime not available; cannot auto-configure auth via REPL context")
        return False
    except Exception as exc:
        logger.warning("Failed to auto-configure Databricks runtime auth: %s", exc)
        return False


# ---------------------------------------------------------------------------
# Module-level state — resolved once per process, reused across calls
# ---------------------------------------------------------------------------

_resolved_profile: str | None = None
_profile_resolved: bool = False

# When False (default), preparers keep workspace paths in-place; the CLI flips it on so
# `databricks bundle deploy` ships the source files across environments.
_downloads_enabled: bool = False


def _get_databrickscfg_path() -> Path:
    """Returns the path to the Databricks CLI config file."""
    override = os.environ.get("DATABRICKS_CONFIG_FILE")
    if override:
        return Path(override)
    return Path.home() / ".databrickscfg"


def _list_profiles() -> list[str]:
    """Parses ``~/.databrickscfg`` and returns available profile names.

    Returns:
        Sorted list of profile section names.  Empty list if the file
        does not exist or cannot be parsed.
    """
    cfg_path = _get_databrickscfg_path()
    if not cfg_path.exists():
        return []

    config = configparser.ConfigParser(default_section="__no_default__")
    try:
        config.read(str(cfg_path), encoding="utf-8")
    except configparser.Error:
        logger.warning("Failed to parse %s", cfg_path)
        return []

    return sorted(config.sections())


def _resolve_profile() -> str | None:
    """Determines which ``~/.databrickscfg`` profile to use.

    Returns:
        Profile name string, or ``None`` to let the SDK use its own
        default resolution.
    """
    global _resolved_profile, _profile_resolved  # noqa: PLW0603

    if _profile_resolved:
        return _resolved_profile

    _profile_resolved = True

    env_profile = os.environ.get("DATABRICKS_CONFIG_PROFILE")
    if env_profile:
        logger.info("Using profile from DATABRICKS_CONFIG_PROFILE: %s", env_profile)
        _resolved_profile = env_profile
        return _resolved_profile

    profiles = _list_profiles()

    if not profiles:
        _resolved_profile = None
        return _resolved_profile

    if len(profiles) == 1:
        _resolved_profile = profiles[0]
        logger.info("Using sole .databrickscfg profile: %s", _resolved_profile)
        return _resolved_profile

    if "DEFAULT" in profiles:
        _resolved_profile = "DEFAULT"
        logger.info("Multiple profiles found; using DEFAULT")
        return _resolved_profile

    _resolved_profile = _prompt_for_profile(profiles)
    return _resolved_profile


def _prompt_for_profile(profiles: list[str]) -> str:
    """Interactively prompts the user to select a profile.

    Args:
        profiles: Available profile names.

    Returns:
        Selected profile name.
    """
    cfg_path = _get_databrickscfg_path()

    config = configparser.ConfigParser(default_section="__no_default__")
    try:
        config.read(str(cfg_path), encoding="utf-8")
    except configparser.Error:
        pass

    print(f"\nMultiple Databricks profiles found in {cfg_path}:")
    for i, name in enumerate(profiles, 1):
        host = config.get(name, "host", fallback="")
        print(f"  [{i}] {name:<30s} {host}")

    while True:
        try:
            choice = input("\nSelect a profile number (or name): ").strip()
        except (EOFError, KeyboardInterrupt):
            # Non-interactive context — fall back to first profile
            print(f"\nNon-interactive; defaulting to '{profiles[0]}'")
            return profiles[0]

        if choice.isdigit():
            idx = int(choice) - 1
            if 0 <= idx < len(profiles):
                selected = profiles[idx]
                print(f"Using profile: {selected}")
                return selected

        if choice in profiles:
            print(f"Using profile: {choice}")
            return choice

        print(f"Invalid selection: '{choice}'. Try again.")


def set_profile(profile: str | None) -> None:
    """Explicitly sets the profile to use, bypassing auto-resolution.

    Args:
        profile: Profile name, or ``None`` to reset to auto-resolution.
    """
    global _resolved_profile, _profile_resolved  # noqa: PLW0603
    _resolved_profile = profile
    _profile_resolved = profile is not None


def _get_workspace_client():
    """Returns a ``WorkspaceClient`` configured with the resolved profile.

    On Databricks runtime, auto-configures auth from the notebook context
    before constructing the client.
    """
    from databricks.sdk import WorkspaceClient  # type: ignore[import-not-found]

    # Ensure auth is available when running on Databricks compute
    if _is_databricks_runtime():
        _ensure_databricks_runtime_auth()

    profile = _resolve_profile()
    if profile:
        return WorkspaceClient(profile=profile)
    return WorkspaceClient()


# ---------------------------------------------------------------------------
# Public download API
# ---------------------------------------------------------------------------


def download_notebook(workspace_path: str) -> str | None:
    """Downloads a notebook from Databricks workspace.

    Attempts local filesystem access first (zero-auth, works on any
    Databricks compute where /Workspace is mounted).  Falls back to the
    Databricks SDK export API when local access is unavailable.

    Args:
        workspace_path: Workspace path (e.g., ``"/Shared/flowx/transform"``).

    Returns:
        Notebook source code as a string, or ``None`` if download failed.
    """
    # Fast path: read directly from /Workspace mount (no auth needed)
    local_content = _try_local_workspace_read(workspace_path)
    if local_content is not None:
        return local_content

    # Slow path: SDK-based export (requires auth)
    try:
        from databricks.sdk.service.workspace import ExportFormat  # type: ignore[import-not-found]

        w = _get_workspace_client()
        response = w.workspace.export(path=workspace_path, format=ExportFormat.SOURCE)
        if response.content:
            return base64.b64decode(response.content).decode("utf-8")
    except ImportError:
        logger.info("databricks-sdk not installed; skipping notebook download for %s", workspace_path)
    except Exception as e:
        logger.warning("Failed to download notebook %s: %s", workspace_path, e)
    return None


def download_dbfs_file(dbfs_path: str) -> bytes | None:
    """Downloads a file from DBFS.

    Args:
        dbfs_path: DBFS path (e.g., ``"dbfs:/scripts/process.py"`` or
            ``"dbfs:/jars/app.jar"``).

    Returns:
        File content as bytes, or ``None`` if download failed.
    """
    try:
        w = _get_workspace_client()
        # Strip "dbfs:" prefix for the SDK call
        path = dbfs_path.replace("dbfs:", "", 1)
        with w.dbfs.open(path, read=True) as f:
            return f.read()
    except ImportError:
        logger.info("databricks-sdk not installed; skipping DBFS download for %s", dbfs_path)
    except Exception as e:
        logger.warning("Failed to download DBFS file %s: %s", dbfs_path, e)
    return None


# ---------------------------------------------------------------------------
# Opt-in toggle + auth gating
# ---------------------------------------------------------------------------


def enable_workspace_downloads(enabled: bool = True) -> None:
    """Globally enables or disables workspace artifact downloads."""
    global _downloads_enabled  # noqa: PLW0603
    _downloads_enabled = bool(enabled)


def workspace_downloads_enabled() -> bool:
    """Returns True iff preparers should attempt to download workspace artifacts."""
    return _downloads_enabled


def auth_available() -> bool:
    """Returns True iff there is any usable Databricks authentication on this host.

    A resolvable ``.databrickscfg`` profile, ``DATABRICKS_CONFIG_PROFILE``, the
    standard ``DATABRICKS_HOST`` + ``DATABRICKS_TOKEN`` pair, or OAuth
    machine-to-machine creds (``DATABRICKS_HOST`` + ``DATABRICKS_CLIENT_ID`` +
    ``DATABRICKS_CLIENT_SECRET`` — what a Databricks App injects for its service
    principal) all satisfy this check.  Local /Workspace filesystem access
    (available on any Databricks compute) also satisfies it since notebooks can
    be read directly without API auth.

    This is a pre-flight signal — it does not validate that the credentials
    actually authorize against any specific workspace (the SDK call does that,
    falling back to a placeholder on failure). It deliberately avoids
    constructing an SDK ``Config``/client, since OAuth resolution can trigger a
    network round-trip.
    """
    if _local_workspace_accessible():
        return True
    if os.environ.get("DATABRICKS_CONFIG_PROFILE"):
        return True
    if os.environ.get("DATABRICKS_HOST") and os.environ.get("DATABRICKS_TOKEN"):
        return True
    # OAuth M2M (e.g. the MCP path: flowx as a Databricks App injects the SP client id/secret,
    # which WorkspaceClient() picks up automatically).
    if (
        os.environ.get("DATABRICKS_HOST")
        and os.environ.get("DATABRICKS_CLIENT_ID")
        and os.environ.get("DATABRICKS_CLIENT_SECRET")
    ):
        return True
    if _list_profiles():
        return True
    # Last resort: bootstrap auth from the Databricks runtime context
    if _is_databricks_runtime():
        return _ensure_databricks_runtime_auth()
    return False


def prompt_for_auth_if_missing(
    sample_paths: Iterable[str],
    *,
    interactive: bool | None = None,
) -> bool:
    """Warns the user when auth is missing and confirms how to proceed.

    Args:
        sample_paths: Workspace paths the preparer is about to try to download.
            Used in the on-screen instructions so the user knows where the
            artifacts they're missing live.
        interactive: Force interactive prompting on/off.  Default ``None``
            auto-detects via ``sys.stdin.isatty()``.

    Returns:
        ``True`` if the user wants to continue with placeholders, ``False``
        if the caller should abort so the user can run ``databricks auth login``.
    """
    if auth_available():
        return True

    paths = [path for path in sample_paths if path]
    cfg_path = _get_databrickscfg_path()

    print(
        "\nWorkspace downloads are enabled but no Databricks CLI auth was found.",
        file=sys.stderr,
    )
    print(f"  Looked for profiles in: {cfg_path}", file=sys.stderr)
    if paths:
        preview = ", ".join(paths[:3])
        suffix = ", …" if len(paths) > 3 else ""
        print(f"  Artifacts to download: {preview}{suffix}", file=sys.stderr)
    print(
        "\nTo authenticate, run one of:\n"
        "  databricks auth login --host https://<your-workspace>.cloud.databricks.com\n"
        "  databricks configure --token  # legacy PAT flow\n",
        file=sys.stderr,
    )

    if interactive is None:
        interactive = sys.stdin.isatty()
    if not interactive:
        print(
            "Non-interactive session; skipping downloads and using placeholders.",
            file=sys.stderr,
        )
        return True

    try:
        choice = input("Continue with placeholders (downloads will be skipped)? [y/N]: ").strip().lower()
    except (EOFError, KeyboardInterrupt):
        return True
    return choice in ("y", "yes")
