"""DAB output models."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

# ---------------------------------------------------------------------------
# Notebook / code artefacts
# ---------------------------------------------------------------------------


@dataclass(slots=True, kw_only=True)
class DabNotebook:
    """Generated notebook to include in the bundle.

    Attributes:
        relative_path: Path relative to the bundle root (e.g. ``"src/copy_data.py"``).
        content: Full notebook source content.
        language: Notebook language (``"python"``, ``"sql"``, ``"scala"``, ``"r"``).
        binary_content: Raw bytes for binary files (e.g. JARs).  When set,
            the notebook writer writes these bytes instead of ``content``.
    """

    relative_path: str
    content: str = ""
    language: str = "python"
    binary_content: bytes | None = None


# ---------------------------------------------------------------------------
# Job / task definitions
# ---------------------------------------------------------------------------


@dataclass(slots=True, kw_only=True)
class DabJob:
    """Databricks workflow job definition.

    Attributes:
        resource_key: Unique key used in the ``databricks.yml`` resources block.
        name: Human-readable job name.
        tasks: Ordered list of task configuration dictionaries.
        schedule: Optional cron schedule configuration.
        tags: Key-value tags applied to the job.
    """

    resource_key: str
    name: str
    tasks: list[dict[str, Any]] = field(default_factory=list)
    schedule: dict[str, Any] | None = None
    tags: dict[str, str] = field(default_factory=dict)


# ---------------------------------------------------------------------------
# Pipeline (Lakeflow Declarative Pipeline)
# ---------------------------------------------------------------------------


@dataclass(slots=True, kw_only=True)
class DabPipeline:
    """Lakeflow Declarative Pipeline definition.

    Attributes:
        resource_key: Unique key used in the ``databricks.yml`` resources block.
        name: Human-readable pipeline name.
        catalog: Unity Catalog catalog for the pipeline output.
        schema: Unity Catalog schema for the pipeline output.
        notebooks: Notebook paths that make up the pipeline.
    """

    resource_key: str
    name: str
    catalog: str | None = None
    schema: str | None = None
    notebooks: list[str] = field(default_factory=list)


# ---------------------------------------------------------------------------
# Variables / secrets / setup
# ---------------------------------------------------------------------------


@dataclass(slots=True, kw_only=True)
class DabVariable:
    """Bundle variable requiring user input at deploy time.

    Attributes:
        description: Human-readable description shown during ``databricks bundle deploy``.
        default: Default value, or ``None`` if the variable is required.
    """

    description: str
    default: str | None = None


@dataclass(slots=True, kw_only=True)
class SecretInstruction:
    """Instruction for creating a Databricks secret.

    Attributes:
        scope: Secret scope name.
        key: Secret key within the scope.
        value_source: Description of where the value should come from
            (e.g. ``"Azure Key Vault: my-kv/secret-name"``).
    """

    scope: str
    key: str
    value_source: str


@dataclass(slots=True, kw_only=True)
class SetupTask:
    """One-time setup task to run before deployment.

    Attributes:
        type: Task category (``"volume"``, ``"secret"``, or ``"connection"``).
        config: Configuration dictionary specific to the task type.
    """

    type: str  # "volume", "secret", "connection"
    config: dict[str, Any] = field(default_factory=dict)


# ---------------------------------------------------------------------------
# Top-level bundle
# ---------------------------------------------------------------------------


@dataclass(slots=True, kw_only=True)
class DabBundle:
    """Complete DAB bundle ready for serialization.

    Attributes:
        name: Bundle name (used as the project directory name).
        jobs: Workflow job definitions.
        notebooks: Generated notebook files.
        pipelines: Lakeflow Declarative Pipeline definitions.
        setup_notebooks: Notebooks for one-time setup tasks.
        variables: Bundle variables requiring user input at deploy time.
    """

    name: str
    jobs: list[DabJob] = field(default_factory=list)
    notebooks: list[DabNotebook] = field(default_factory=list)
    pipelines: list[DabPipeline] = field(default_factory=list)
    setup_notebooks: list[DabNotebook] = field(default_factory=list)
    variables: dict[str, DabVariable] = field(default_factory=dict)
