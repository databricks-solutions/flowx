"""Agent-facing surfaces and the matching pipeline modifier for flowx translation.

Two roles live here, kept deliberately separate: the **agent adapter** (:mod:`~flowx.adapter.session`
plus the option shapes in :mod:`~flowx.adapter.models`) converts tool-call arguments into
deterministic calls and maps "need more input" into structured objects, while the **pipeline modifier**
(:mod:`~flowx.adapter.operations`) consumes a validated :class:`TranslationConfiguration` and stamps
concrete decisions onto a Pipeline IR with no awareness of agents. ``constants`` holds shared strings and
``predicates`` holds pure IR predicates used by both ``operations`` and the bundler.
"""

from __future__ import annotations

from flowx.adapter.models import (
    DEFAULT_CONFIGURATION,
    CopyActivityParadigm,
    LakeflowConnectorType,
    MetadataDrivenAccess,
    MetadataDrivenConsolidate,
    MetadataDrivenLookupTool,
    MetadataDrivenSize,
    MigrationInputOption,
    MotifConsolidate,
    NonDatabricksTaskCompute,
    OptionChoice,
    PendingMigrationInputs,
    PendingOptions,
    TranslationConfiguration,
    TranslationOption,
    UseLakeflowConnectors,
)
from flowx.adapter.operations import (
    allowed_values_for,
    apply_configuration,
    collect_workspace_artifact_paths,
    detect_databricks_hosts,
    enum_for,
    gather_options,
    validate_answer,
)
from flowx.adapter.session import (
    MigrationInputSession,
    TranslationInputRequired,
    TranslationSession,
    UnknownMigrationPhaseError,
)

__all__ = [
    "DEFAULT_CONFIGURATION",
    "CopyActivityParadigm",
    "LakeflowConnectorType",
    "MetadataDrivenAccess",
    "MetadataDrivenConsolidate",
    "MetadataDrivenLookupTool",
    "MetadataDrivenSize",
    "MigrationInputOption",
    "MigrationInputSession",
    "MotifConsolidate",
    "NonDatabricksTaskCompute",
    "PendingMigrationInputs",
    "PendingOptions",
    "OptionChoice",
    "TranslationInputRequired",
    "TranslationConfiguration",
    "TranslationOption",
    "TranslationSession",
    "UnknownMigrationPhaseError",
    "UseLakeflowConnectors",
    "allowed_values_for",
    "apply_configuration",
    "collect_workspace_artifact_paths",
    "detect_databricks_hosts",
    "enum_for",
    "gather_options",
    "validate_answer",
]
