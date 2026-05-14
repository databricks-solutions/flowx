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
# Module-level state — resolved once per process, reused across calls
# ---------------------------------------------------------------------------

_resolved_profile: str | None = None
_profile_resolved: bool = False

# When False (default), preparers preserve workspace artifact paths in-place
# instead of attempting a network download.  The CLI flips this on so that
# `databricks bundle deploy` can ship the source files across environments.
_downloads_enabled: bool = False


def _get_databrickscfg_path() -> Path:
    """Return the path to the Databricks CLI config file."""
    override = os.environ.get("DATABRICKS_CONFIG_FILE")
    if override:
        return Path(override)
    return Path.home() / ".databrickscfg"


def _list_profiles() -> list[str]:
    """Parses ``~/.databrickscfg`` and return available profile names.

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
    """Interactively prompt the user to select a profile.

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
    """Explicitly set the profile to use, bypassing auto-resolution.

    Args:
        profile: Profile name, or ``None`` to reset to auto-resolution.
    """
    global _resolved_profile, _profile_resolved  # noqa: PLW0603
    _resolved_profile = profile
    _profile_resolved = profile is not None


def _get_workspace_client():
    """Return a ``WorkspaceClient`` configured with the resolved profile.

    Returns:
        A ``WorkspaceClient`` instance.

    Raises:
        ImportError: If ``databricks-sdk`` is not installed.
    """
    from databricks.sdk import WorkspaceClient  # type: ignore[import-not-found]

    profile = _resolve_profile()
    if profile:
        return WorkspaceClient(profile=profile)
    return WorkspaceClient()


# ---------------------------------------------------------------------------
# Public download API
# ---------------------------------------------------------------------------


def download_notebook(workspace_path: str) -> str | None:
    """Download a notebook from Databricks workspace.

    Args:
        workspace_path: Workspace path (e.g., ``"/Shared/flowx/transform"``).

    Returns:
        Notebook source code as a string, or ``None`` if download failed.
    """
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
    """Download a file from DBFS.

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
    """Globally enable or disable workspace artifact downloads."""
    global _downloads_enabled  # noqa: PLW0603
    _downloads_enabled = bool(enabled)


def workspace_downloads_enabled() -> bool:
    """Return True iff preparers should attempt to download workspace artifacts."""
    return _downloads_enabled


def auth_available() -> bool:
    """Return True iff there is any usable Databricks authentication on this host.

    A resolvable ``.databrickscfg`` profile, ``DATABRICKS_CONFIG_PROFILE``, or
    the standard ``DATABRICKS_HOST`` + ``DATABRICKS_TOKEN`` env-var pair will
    all satisfy this check.  This is a pre-flight signal — it does not validate
    that the credentials actually authorize against any specific workspace.
    """
    if os.environ.get("DATABRICKS_CONFIG_PROFILE"):
        return True
    if os.environ.get("DATABRICKS_HOST") and os.environ.get("DATABRICKS_TOKEN"):
        return True
    return bool(_list_profiles())


def prompt_for_auth_if_missing(
    sample_paths: Iterable[str],
    *,
    interactive: bool | None = None,
) -> bool:
    """Warn the user when auth is missing and confirm how to proceed.

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

    paths = [p for p in sample_paths if p]
    cfg_path = _get_databrickscfg_path()

    print(
        "\nWorkspace downloads are enabled but no Databricks CLI auth was found.",
        file=sys.stderr,
    )
    print(f"  Looked for profiles in: {cfg_path}", file=sys.stderr)
    if paths:
        preview = ", ".join(paths[:3])
        suffix = ", …" if len(paths) > 3 else ""
        print(f"  Artifacts to vendor: {preview}{suffix}", file=sys.stderr)
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
