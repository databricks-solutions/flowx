"""Agent-facing surfaces and the matching pipeline modifier for orchestra translation.

This package draws a deliberate line between two roles:

* **Agent adapter** -- :mod:`orchestra.adapter.session` plus the question
  shapes in :mod:`orchestra.adapter.models`.  This is the layer an agent
  calls.  It converts tool-call arguments into deterministic service calls
  and maps "need more input" signals into structured objects (and the
  :exc:`TranslationInputRequired` exception) the agent can hand back to
  the user.

* **Pipeline modifier** -- :mod:`orchestra.adapter.operations`.  The
  deterministic transformation that consumes a validated
  :class:`TranslationPreferences` snapshot and stamps concrete decisions
  onto a Pipeline IR.  It has no awareness of agents or user prompts and
  is safely importable from non-agent contexts (CLI, tests, batch jobs).

The package is organised into three primary modules plus the predicates
and session helpers:

* :mod:`~orchestra.adapter.models` -- StrEnums and dataclasses.
* :mod:`~orchestra.adapter.operations` -- Free functions
  (``gather_questions``, ``apply_preferences``, ``validate_answer``,
  ``allowed_values_for``).
* :mod:`~orchestra.adapter.constants` -- Question IDs, compute-mode
  strings, replacement names, and other shared constants.
* :mod:`~orchestra.adapter.predicates` -- Pure IR predicates used by
  both ``operations`` and the bundler.
* :mod:`~orchestra.adapter.session` -- The agent adapter class.
"""

from __future__ import annotations

from orchestra.adapter.models import (
    DEFAULT_PREFERENCES,
    CopyActivityParadigm,
    LakeflowConnectorType,
    MetadataDrivenAccess,
    MetadataDrivenConsolidate,
    MetadataDrivenLookupTool,
    MetadataDrivenSize,
    MigrationInputQuestion,
    MotifConsolidate,
    NonDatabricksTaskCompute,
    PendingMigrationInputs,
    PendingQuestions,
    QuestionOption,
    TranslationPreferences,
    TranslationQuestion,
    UseLakeflowConnectors,
)
from orchestra.adapter.operations import (
    allowed_values_for,
    apply_preferences,
    collect_workspace_artifact_paths,
    detect_databricks_hosts,
    enum_for,
    gather_questions,
    validate_answer,
)
from orchestra.adapter.session import (
    MigrationInputSession,
    TranslationInputRequired,
    TranslationSession,
    UnknownMigrationPhaseError,
)

__all__ = [
    "DEFAULT_PREFERENCES",
    "CopyActivityParadigm",
    "LakeflowConnectorType",
    "MetadataDrivenAccess",
    "MetadataDrivenConsolidate",
    "MetadataDrivenLookupTool",
    "MetadataDrivenSize",
    "MigrationInputQuestion",
    "MigrationInputSession",
    "MotifConsolidate",
    "NonDatabricksTaskCompute",
    "PendingMigrationInputs",
    "PendingQuestions",
    "QuestionOption",
    "TranslationInputRequired",
    "TranslationPreferences",
    "TranslationQuestion",
    "TranslationSession",
    "UnknownMigrationPhaseError",
    "UseLakeflowConnectors",
    "allowed_values_for",
    "apply_preferences",
    "collect_workspace_artifact_paths",
    "detect_databricks_hosts",
    "enum_for",
    "gather_questions",
    "validate_answer",
]
