"""Shared utilities for the flowx translation pipeline."""

from __future__ import annotations

import re
from typing import Any

from flowx.models.adf_ast import AdfPolicy

# Default ADF timeout (12 hours), used when a timeout string cannot be parsed.
DEFAULT_TIMEOUT_SECONDS = 43_200

# ---------------------------------------------------------------------------
# Case conversion
# ---------------------------------------------------------------------------

_CAMEL_RE_1 = re.compile(r"(.)([A-Z][a-z]+)")
_CAMEL_RE_2 = re.compile(r"([a-z0-9])([A-Z])")


def camel_to_snake(name: str) -> str:
    """Converts a camelCase or PascalCase string to snake_case.

    Args:
        name: Identifier in camelCase or PascalCase.

    Returns:
        Same identifier in snake_case.
    """
    substituted = _CAMEL_RE_1.sub(r"\1_\2", name)
    return _CAMEL_RE_2.sub(r"\1_\2", substituted).lower()


def recursive_camel_to_snake(obj: Any) -> Any:
    """Recursively convert all dict keys from camelCase to snake_case.

    Args:
        obj: Nested structure of dicts, lists, and primitives (e.g. ADF JSON).

    Returns:
        New structure with dict keys in snake_case.
    """
    if isinstance(obj, dict):
        return {camel_to_snake(k): recursive_camel_to_snake(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [recursive_camel_to_snake(item) for item in obj]
    return obj


# ---------------------------------------------------------------------------
# Key sanitisation
# ---------------------------------------------------------------------------

_LOWERCASE_KEY_RE = re.compile(r"[^a-z0-9_]")


def to_lowercase_key(name: str) -> str:
    """Sanitises a name into a key and lowercases it.

    Lowercases, replaces every character outside ``[a-z0-9_]`` with ``_`` (so
    hyphens fold to ``_``), collapses runs of ``_``, and strips leading/trailing
    ``_``.  Returns ``""`` for an all-illegal name.  Differs from
    :func:`to_case_preserving_key`, which keeps case and hyphens.

    Args:
        name: Original activity or pipeline name.

    Returns:
        Cleaned, lowercased key string.
    """
    lowered = name.strip().lower()
    replaced = _LOWERCASE_KEY_RE.sub("_", lowered)
    collapsed = re.sub(r"_+", "_", replaced).strip("_")
    return collapsed


_CASE_PRESERVING_KEY_RE = re.compile(r"[^a-zA-Z0-9_-]")


def to_case_preserving_key(name: str) -> str:
    """Sanitises a name into a key while keeping its original case and hyphens.

    Replaces every character outside ``[a-zA-Z0-9_-]`` with ``_``, collapses runs
    of ``_``, and strips leading/trailing ``_``.  Returns ``"unnamed"`` for an
    all-illegal name.  Differs from :func:`to_lowercase_key`, which lowercases
    and folds hyphens; use this one when the key must match the case-preserving
    key the ADF translator bakes into ``{{tasks.<key>.values.Y}}`` references.

    Args:
        name: Original activity name.

    Returns:
        Cleaned key string, or ``"unnamed"`` when nothing survives.
    """
    key = _CASE_PRESERVING_KEY_RE.sub("_", name)
    return re.sub(r"_+", "_", key).strip("_") or "unnamed"


# ---------------------------------------------------------------------------
# Timeout parsing
# ---------------------------------------------------------------------------

_TIMEOUT_PATTERN = re.compile(r"^(?:(\d+)\.)?((\d{1,2}):(\d{2}):(\d{2}))$")


def parse_timeout(timeout_str: str | None) -> int | None:
    """Parses an ADF timeout string into total seconds.

    Args:
        timeout_str: Timeout string from the ADF activity policy, or ``None``.

    Returns:
        Total seconds, ``DEFAULT_TIMEOUT_SECONDS`` on parse failure, or ``None``
        when no timeout is specified.
    """
    if timeout_str is None:
        return None

    match = _TIMEOUT_PATTERN.match(timeout_str)
    if not match:
        return DEFAULT_TIMEOUT_SECONDS

    days = int(match.group(1)) if match.group(1) is not None else 0
    hours = int(match.group(3))
    minutes = int(match.group(4))
    seconds = int(match.group(5))

    total = days * 86_400 + hours * 3_600 + minutes * 60 + seconds
    if total <= 0:
        return DEFAULT_TIMEOUT_SECONDS
    return total


# ---------------------------------------------------------------------------
# Retry policy extraction
# ---------------------------------------------------------------------------


def parse_retry_policy(policy: AdfPolicy | None) -> tuple[int | None, int | None]:
    """Extracts retry count and interval from an ADF policy.

    Args:
        policy: Parsed ``AdfPolicy``, or ``None``.

    Returns:
        Tuple of ``(max_retries, retry_interval_seconds)``.  Either or both
        values may be ``None`` when the policy does not specify them.
    """
    if policy is None:
        return None, None

    retries = policy.retry
    interval = policy.retry_interval_in_seconds
    return retries, interval
