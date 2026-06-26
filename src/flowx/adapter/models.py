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
    the query-based connector regardless of this configuration; this enum
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


class NotifyDestination(StrEnum):
    """How an activity->Notify (activity_and_notify) motif's notifications are handled.

    ``KEEP`` preserves the current behaviour (the WebActivity notify
    activities translate directly; the motif is not collapsed).  Any other
    value collapses the motif: the upstream activity (Copy, Notebook, Lookup,
    …) becomes the task and the downstream notifications become Databricks
    job-task notifications routed to the chosen destination.
    """

    KEEP = "keep"
    EMAIL = "email"
    SLACK = "slack"
    TEAMS = "teams"
    PAGERDUTY = "pagerduty"
    WEBHOOK = "webhook"


class NotifyEvents(StrEnum):
    """Which job-task events fire the collapsed notification."""

    ON_FAILURE = "on_failure"
    ON_SUCCESS = "on_success"
    BOTH = "both"


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
        "notify_destination": NotifyDestination,
        "notify_events": NotifyEvents,
    }
)


@dataclass(frozen=True, slots=True, kw_only=True)
class TranslationConfiguration:
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

    Notes:
        ADF DatabricksNotebook and DatabricksSparkPython tasks always keep the cluster binding from
        the source linked service; the serverless-replacement option was removed because it silently
        discarded init scripts and DBR-version constraints the source pipeline relied on.
    """

    copy_activity_paradigm: CopyActivityParadigm = CopyActivityParadigm.NOTEBOOK
    non_databricks_task_compute: NonDatabricksTaskCompute = NonDatabricksTaskCompute.SERVERLESS
    use_lakeflow_connectors: UseLakeflowConnectors = UseLakeflowConnectors.EXISTING
    lakeflow_connector_type: LakeflowConnectorType = LakeflowConnectorType.CDC
    metadata_driven_consolidate: MetadataDrivenConsolidate = MetadataDrivenConsolidate.KEEP
    metadata_driven_access: MetadataDrivenAccess = MetadataDrivenAccess.NO
    metadata_driven_size: MetadataDrivenSize = MetadataDrivenSize.LARGE
    metadata_driven_lookup_tool: MetadataDrivenLookupTool = MetadataDrivenLookupTool.NONE
    notify_destination: NotifyDestination = NotifyDestination.KEEP
    notify_events: NotifyEvents = NotifyEvents.BOTH
    notify_destination_name: str = ""
    # SDK config kwargs for the chosen destination (e.g. addresses/url/integration_key), keyed by SDK
    # arg name; populated from the per-field follow-up answers via collect_notify_args.
    notify_args: dict[str, str] = field(default_factory=dict)
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
        # motif_consolidations is keyed by dynamic motif_id (not in FIELD_TO_ENUM), so coerce here.
        coerced: dict[str, MotifConsolidate] = {}
        for motif_id, choice in self.motif_consolidations.items():
            coerced[motif_id] = choice if isinstance(choice, MotifConsolidate) else MotifConsolidate(choice)
        object.__setattr__(self, "motif_consolidations", coerced)

    def effective_for(self, task_key: str) -> TranslationConfiguration:
        """Returns a configuration view where per-task overrides for *task_key* win.

        Args:
            task_key: Sanitised task key of the activity being prepared.

        Returns:
            A new :class:`TranslationConfiguration` with overrides for
            *task_key* applied on top of the pipeline-wide values, or
            ``self`` unchanged when no overrides exist for *task_key*.
        """
        override = self.per_task.get(task_key)
        if not override:
            return self
        return TranslationConfiguration(
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


DEFAULT_CONFIGURATION: Final[TranslationConfiguration] = TranslationConfiguration()


@dataclass(frozen=True, slots=True, kw_only=True)
class OptionChoice:
    """One allowed answer to a :class:`TranslationOption`.

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
class TranslationOption:
    """A single just-in-time option raised by the IR inspector.

    Attributes:
        option_id: Stable identifier matching the configuration field.
        prompt: Human-readable option text.
        rationale: One- or two-sentence explanation of why the option
            is being raised.
        options: Allowed answers; the first option is the conservative
            default and is also exposed via ``default``.
        affected_task_keys: Activity task keys impacted by the answer.
        default: Default value applied when the caller skips the option.
        conditions: Tuples of ``(option_id, expected_value)`` that must
            already be answered with the expected value before this
            option surfaces.  An empty tuple means the option is
            evaluated solely on its IR/motif preconditions.
    """

    option_id: str
    prompt: str
    rationale: str
    options: tuple[OptionChoice, ...]
    affected_task_keys: tuple[str, ...]
    default: str
    conditions: tuple[tuple[str, str], ...] = ()


@dataclass(slots=True, kw_only=True)
class PendingOptions:
    """Outstanding options for a single pipeline translation.

    Attributes:
        pipeline_name: Name of the pipeline these options belong to.
        options: Ordered list of options still awaiting an answer.
    """

    pipeline_name: str
    options: list[TranslationOption] = field(default_factory=list)


@dataclass(frozen=True, slots=True, kw_only=True)
class MigrationInputOption:
    """A free-text input gathered before an flowx phase runs.

    Attributes:
        option_id: Stable identifier the skill uses to key the answer.
        prompt: Human-readable option text.
        description: One-sentence explanation of what the value is used
            for and what shape is expected (path, URL, identifier).
        default: Default value applied when the caller skips the
            option; ``None`` when the field is required and has no
            sensible default.
        required: When ``True`` the skill must collect a value; when
            ``False`` the default (which may be ``None``) is permitted.
    """

    option_id: str
    prompt: str
    description: str
    default: str | None = None
    required: bool = True


@dataclass(slots=True, kw_only=True)
class PendingMigrationInputs:
    """Outstanding migration-phase input options for a single phase.

    Attributes:
        phase: The migration phase name (``"discover"``, ``"convert"``,
            ``"package"``).
        options: Ordered list of options still awaiting an answer.
    """

    phase: str
    options: list[MigrationInputOption] = field(default_factory=list)
