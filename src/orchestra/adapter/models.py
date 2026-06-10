"""Dataclasses and StrEnums shared across the adapter package."""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import StrEnum
from types import MappingProxyType
from typing import Final


class CopyActivityParadigm(StrEnum):
    """Code paradigm used to translate Copy Data activities whose sink is Delta."""

    NOTEBOOK = "notebook"
    SDP = "sdp"


class NonDatabricksTaskCompute(StrEnum):
    """Compute mode used for non-Databricks tasks such as Copy, Web, or Lookup."""

    SERVERLESS = "serverless"
    CLASSIC = "classic"


class UseLakeflowConnectors(StrEnum):
    """Whether to swap eligible database-source Copy patterns for Lakeflow Connect."""

    LAKEFLOW_CONNECT = "lakeflow_connect"
    EXISTING = "existing"


class LakeflowConnectorType(StrEnum):
    """Lakeflow Connect connector flavour for an eligible Copy ingestion.

    Used only when ``use_lakeflow_connectors`` is ``lakeflow_connect``.  The
    modifier still routes Copy activities that read from a SQL query into
    the query-based connector regardless of this preference; this enum
    controls the default for table-based Copy activities.
    """

    QUERY_BASED = "query_based"
    CDC = "cdc"


class MetadataDrivenConsolidate(StrEnum):
    """Whether metadata-driven motifs should collapse into one managed pipeline."""

    CONSOLIDATE = "consolidate"
    KEEP = "keep"


class MetadataDrivenAccess(StrEnum):
    """Whether the user can query the metadata source for lookup values."""

    YES = "yes"
    NO = "no"


class MetadataDrivenSize(StrEnum):
    """T-shirt size for the number of metadata-driven configuration rows.

    The thresholds match the prompt rationale: ``small`` covers fewer than
    50 entries, ``medium`` covers fewer than 250, and ``large`` covers
    250 or more.  ``large`` suppresses inline lookup materialisation
    because the modifier cannot reliably enumerate the configuration in
    one translation pass.
    """

    SMALL = "small"
    MEDIUM = "medium"
    LARGE = "large"


class MetadataDrivenLookupTool(StrEnum):
    """Whether the agent has a tool that can run the lookup query."""

    HAVE = "have"
    NONE = "none"


class MotifConsolidate(StrEnum):
    """Whether to collapse a detected motif into a single :class:`MotifActivity`.

    Default for every detected motif is :data:`KEEP` -- preserving the
    underlying activity-by-activity translation -- so motif detection
    can never silently rewrite a pipeline without an explicit user
    opt-in.
    """

    KEEP = "keep"
    CONSOLIDATE = "consolidate"


FIELD_TO_ENUM: Final[MappingProxyType[str, type[StrEnum]]] = MappingProxyType(
    {
        "copy_activity_paradigm": CopyActivityParadigm,
        "non_databricks_task_compute": NonDatabricksTaskCompute,
        "use_lakeflow_connectors": UseLakeflowConnectors,
        "lakeflow_connector_type": LakeflowConnectorType,
        "metadata_driven_consolidate": MetadataDrivenConsolidate,
        "metadata_driven_access": MetadataDrivenAccess,
        "metadata_driven_size": MetadataDrivenSize,
        "metadata_driven_lookup_tool": MetadataDrivenLookupTool,
    }
)


@dataclass(frozen=True, slots=True, kw_only=True)
class TranslationPreferences:
    """Snapshot of user choices that shape downstream IR transformations.

    Each field accepts either a raw string or the corresponding enum
    member; strings are coerced to enum members at construction.

    Attributes:
        copy_activity_paradigm: Paradigm used for Copy Data activities whose
            sink resolves to a Delta table.
        non_databricks_task_compute: Compute mode for non-Databricks tasks.
        use_lakeflow_connectors: Whether eligible database-source Copy
            patterns are migrated to managed Lakeflow Connect pipelines.
        per_task: Optional per-activity overrides keyed by task_key.  Each
            value is a partial mapping of the fields above; only the
            keys present win over the pipeline-wide defaults.

    ADF DatabricksNotebook and DatabricksSparkPython tasks always keep
    the cluster binding derived from the source linked service -- the
    serverless replacement option was removed because it silently
    discarded init scripts and DBR-version constraints that the source
    pipeline relied on.
    """

    copy_activity_paradigm: CopyActivityParadigm = CopyActivityParadigm.NOTEBOOK
    non_databricks_task_compute: NonDatabricksTaskCompute = NonDatabricksTaskCompute.SERVERLESS
    use_lakeflow_connectors: UseLakeflowConnectors = UseLakeflowConnectors.EXISTING
    lakeflow_connector_type: LakeflowConnectorType = LakeflowConnectorType.CDC
    metadata_driven_consolidate: MetadataDrivenConsolidate = MetadataDrivenConsolidate.KEEP
    metadata_driven_access: MetadataDrivenAccess = MetadataDrivenAccess.NO
    metadata_driven_size: MetadataDrivenSize = MetadataDrivenSize.LARGE
    metadata_driven_lookup_tool: MetadataDrivenLookupTool = MetadataDrivenLookupTool.NONE
    motif_consolidations: dict[str, MotifConsolidate] = field(default_factory=dict)
    per_task: dict[str, dict[str, str]] = field(default_factory=dict)

    def __post_init__(self) -> None:
        """Coerces raw string inputs into their backing enum members.

        Raises:
            ValueError: When a field value is not a member of the backing
                :class:`StrEnum`.
        """
        for field_name, enum_cls in FIELD_TO_ENUM.items():
            value = getattr(self, field_name)
            if not isinstance(value, enum_cls):
                object.__setattr__(self, field_name, enum_cls(value))
        # motif_consolidations is keyed by dynamic motif_id rather than a
        # fixed field name, so it is not in FIELD_TO_ENUM.  Coerce its
        # values to MotifConsolidate members here.
        coerced: dict[str, MotifConsolidate] = {}
        for motif_id, choice in self.motif_consolidations.items():
            coerced[motif_id] = choice if isinstance(choice, MotifConsolidate) else MotifConsolidate(choice)
        object.__setattr__(self, "motif_consolidations", coerced)

    def effective_for(self, task_key: str) -> TranslationPreferences:
        """Returns a preferences view where per-task overrides for *task_key* win.

        Args:
            task_key: Sanitised task key of the activity being prepared.

        Returns:
            A new :class:`TranslationPreferences` with overrides for
            *task_key* applied on top of the pipeline-wide values, or
            ``self`` unchanged when no overrides exist for *task_key*.
        """
        override = self.per_task.get(task_key)
        if not override:
            return self
        return TranslationPreferences(
            copy_activity_paradigm=CopyActivityParadigm(
                override.get("copy_activity_paradigm", self.copy_activity_paradigm)
            ),
            non_databricks_task_compute=NonDatabricksTaskCompute(
                override.get("non_databricks_task_compute", self.non_databricks_task_compute)
            ),
            use_lakeflow_connectors=UseLakeflowConnectors(
                override.get("use_lakeflow_connectors", self.use_lakeflow_connectors)
            ),
            lakeflow_connector_type=LakeflowConnectorType(
                override.get("lakeflow_connector_type", self.lakeflow_connector_type)
            ),
            metadata_driven_consolidate=MetadataDrivenConsolidate(
                override.get("metadata_driven_consolidate", self.metadata_driven_consolidate)
            ),
            metadata_driven_access=MetadataDrivenAccess(
                override.get("metadata_driven_access", self.metadata_driven_access)
            ),
            metadata_driven_size=MetadataDrivenSize(override.get("metadata_driven_size", self.metadata_driven_size)),
            metadata_driven_lookup_tool=MetadataDrivenLookupTool(
                override.get("metadata_driven_lookup_tool", self.metadata_driven_lookup_tool)
            ),
            motif_consolidations=dict(self.motif_consolidations),
            per_task=self.per_task,
        )


DEFAULT_PREFERENCES: Final[TranslationPreferences] = TranslationPreferences()


@dataclass(frozen=True, slots=True, kw_only=True)
class QuestionOption:
    """One allowed answer to a :class:`TranslationQuestion`.

    Attributes:
        value: Machine-readable identifier matching the backing enum member.
        label: Short human-readable label suitable for a prompt button.
        description: One-sentence explanation of the trade-off this option
            implies for the migrated bundle.
    """

    value: str
    label: str
    description: str


@dataclass(frozen=True, slots=True, kw_only=True)
class TranslationQuestion:
    """A single just-in-time question raised by the IR inspector.

    Attributes:
        question_id: Stable identifier matching the preferences field.
        prompt: Human-readable question text.
        rationale: One- or two-sentence explanation of why the question
            is being raised.
        options: Allowed answers; the first option is the conservative
            default and is also exposed via ``default``.
        affected_task_keys: Activity task keys impacted by the answer.
        default: Default value applied when the caller skips the question.
        conditions: Tuples of ``(question_id, expected_value)`` that must
            already be answered with the expected value before this
            question surfaces.  An empty tuple means the question is
            evaluated solely on its IR/motif preconditions.
    """

    question_id: str
    prompt: str
    rationale: str
    options: tuple[QuestionOption, ...]
    affected_task_keys: tuple[str, ...]
    default: str
    conditions: tuple[tuple[str, str], ...] = ()


@dataclass(slots=True, kw_only=True)
class PendingQuestions:
    """Outstanding questions for a single pipeline translation.

    Attributes:
        pipeline_name: Name of the pipeline these questions belong to.
        questions: Ordered list of questions still awaiting an answer.
    """

    pipeline_name: str
    questions: list[TranslationQuestion] = field(default_factory=list)


@dataclass(frozen=True, slots=True, kw_only=True)
class MigrationInputQuestion:
    """A free-text input gathered before an orchestra phase runs.

    Attributes:
        question_id: Stable identifier the skill uses to key the answer.
        prompt: Human-readable question text.
        description: One-sentence explanation of what the value is used
            for and what shape is expected (path, URL, identifier).
        default: Default value applied when the caller skips the
            question; ``None`` when the field is required and has no
            sensible default.
        required: When ``True`` the skill must collect a value; when
            ``False`` the default (which may be ``None``) is permitted.
    """

    question_id: str
    prompt: str
    description: str
    default: str | None = None
    required: bool = True


@dataclass(slots=True, kw_only=True)
class PendingMigrationInputs:
    """Outstanding migration-phase input questions for a single phase.

    Attributes:
        phase: The migration phase name (``"ingest"``, ``"translate"``,
            ``"prepare"``).
        questions: Ordered list of questions still awaiting an answer.
    """

    phase: str
    questions: list[MigrationInputQuestion] = field(default_factory=list)
