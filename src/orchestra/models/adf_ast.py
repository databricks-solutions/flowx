"""Typed ADF AST nodes."""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any


class TranslationStrategy(Enum):
    """Classification of how an ADF activity should be translated."""

    DETERMINISTIC = "deterministic"
    AGENTIC = "agentic"
    UNSUPPORTED = "unsupported"


# ---------------------------------------------------------------------------
# Activity-level AST nodes
# ---------------------------------------------------------------------------


@dataclass(slots=True, kw_only=True)
class AdfDependency:
    """Dependency edge between two ADF activities.

    Attributes:
        activity: Name of the upstream activity.
        dependency_conditions: Required outcome(s) (e.g. ``["Succeeded"]``).
    """

    activity: str
    dependency_conditions: list[str] = field(default_factory=lambda: ["Succeeded"])


@dataclass(slots=True, kw_only=True)
class AdfPolicy:
    """Retry / timeout policy attached to an ADF activity.

    Attributes:
        timeout: Timeout string in ADF format (``"d.hh:mm:ss"`` or ``"hh:mm:ss"``).
        retry: Maximum number of retries.
        retry_interval_in_seconds: Delay between retries in seconds.
        secure_input: Whether the activity input is masked in logs.
        secure_output: Whether the activity output is masked in logs.
    """

    timeout: str | None = None
    retry: int | None = None
    retry_interval_in_seconds: int | None = None
    secure_input: bool = False
    secure_output: bool = False


@dataclass(slots=True, kw_only=True)
class AdfParameter:
    """Pipeline-level parameter definition.

    Attributes:
        type: ADF parameter type (``"String"``, ``"Int"``, ``"Bool"``, etc.).
        default_value: Optional default value for the parameter.
    """

    type: str = "String"
    default_value: Any = None


@dataclass(slots=True, kw_only=True)
class AdfVariable:
    """Pipeline-level variable definition.

    Attributes:
        type: ADF variable type.
        default_value: Optional initial value.
    """

    type: str = "String"
    default_value: Any = None


# ---------------------------------------------------------------------------
# Reference nodes
# ---------------------------------------------------------------------------


@dataclass(slots=True, kw_only=True)
class AdfDatasetReference:
    """Reference to an ADF dataset used as an activity input or output.

    Attributes:
        reference_name: Logical name of the dataset.
        type: Reference type (always ``"DatasetReference"``).
        parameters: Runtime parameters passed to the dataset, if any.
    """

    reference_name: str
    type: str = "DatasetReference"
    parameters: dict[str, Any] | None = None


@dataclass(slots=True, kw_only=True)
class AdfLinkedServiceReference:
    """Reference to an ADF linked service.

    Attributes:
        reference_name: Logical name of the linked service.
        type: Reference type (always ``"LinkedServiceReference"``).
    """

    reference_name: str
    type: str = "LinkedServiceReference"


# ---------------------------------------------------------------------------
# Activity node
# ---------------------------------------------------------------------------


@dataclass(slots=True, kw_only=True)
class AdfActivity:
    """Single ADF activity node.

    Attributes:
        name: Activity display name.
        type: ADF activity type string (e.g. ``"Copy"``, ``"DatabricksNotebook"``).
        depends_on: Upstream dependency edges.
        policy: Retry / timeout policy.
        type_properties: Raw ``typeProperties`` bag from the ADF JSON.
        inputs: Dataset references consumed by the activity.
        outputs: Dataset references produced by the activity.
        linked_service_name: Linked service reference used by the activity.
        if_true_activities: Activities to run when an IfCondition evaluates to true.
        if_false_activities: Activities to run when an IfCondition evaluates to false.
        activities: Child activities for ForEach / Until containers.
    """

    name: str
    type: str
    depends_on: list[AdfDependency] | None = None
    policy: AdfPolicy | None = None
    type_properties: dict[str, Any] | None = None
    inputs: list[AdfDatasetReference] | None = None
    outputs: list[AdfDatasetReference] | None = None
    linked_service_name: AdfLinkedServiceReference | None = None
    if_true_activities: list[AdfActivity] | None = None
    if_false_activities: list[AdfActivity] | None = None
    activities: list[AdfActivity] | None = None  # ForEach, Until


# ---------------------------------------------------------------------------
# Pipeline node
# ---------------------------------------------------------------------------


@dataclass(slots=True, kw_only=True)
class AdfPipeline:
    """Top-level ADF pipeline definition.

    Attributes:
        name: Pipeline display name.
        activities: Ordered list of activities that make up the pipeline.
        parameters: Pipeline parameter declarations, keyed by name.
        variables: Pipeline variable declarations, keyed by name.
        annotations: Free-form annotation strings attached to the pipeline.
        folder: Organisational folder path within the ADF workspace.
    """

    name: str
    activities: list[AdfActivity]
    parameters: dict[str, AdfParameter] | None = None
    variables: dict[str, AdfVariable] | None = None
    annotations: list[str] | None = None
    folder: str | None = None


# ---------------------------------------------------------------------------
# Supporting definition nodes
# ---------------------------------------------------------------------------


@dataclass(slots=True, kw_only=True)
class AdfDataset:
    """ADF dataset definition.

    Attributes:
        name: Dataset display name.
        type: Dataset type (e.g. ``"AzureSqlTable"``, ``"DelimitedText"``).
        properties: Full properties bag from the ADF JSON.
        linked_service_name: Name of the linked service backing this dataset.
    """

    name: str
    type: str
    properties: dict[str, Any]
    linked_service_name: str | None = None


@dataclass(slots=True, kw_only=True)
class AdfLinkedService:
    """ADF linked service definition.

    Attributes:
        name: Linked service display name.
        type: Service type (e.g. ``"AzureBlobStorage"``, ``"AzureSqlDatabase"``).
        properties: Full properties bag from the ADF JSON.
    """

    name: str
    type: str
    properties: dict[str, Any]


@dataclass(slots=True, kw_only=True)
class AdfTrigger:
    """ADF trigger definition.

    Attributes:
        name: Trigger display name.
        type: Trigger type (e.g. ``"ScheduleTrigger"``).
        properties: Full properties bag from the ADF JSON.
        pipelines: List of pipeline references activated by this trigger.
    """

    name: str
    type: str
    properties: dict[str, Any]
    pipelines: list[dict[str, Any]] | None = None


# ---------------------------------------------------------------------------
# Aggregate containers
# ---------------------------------------------------------------------------


@dataclass(slots=True, kw_only=True)
class AdfDefinitions:
    """Complete set of ADF definitions loaded from JSON files.

    Attributes:
        pipelines: All pipeline definitions.
        datasets: Dataset definitions keyed by name.
        linked_services: Linked service definitions keyed by name.
        triggers: Trigger definitions.
    """

    pipelines: list[AdfPipeline]
    datasets: dict[str, AdfDataset] = field(default_factory=dict)
    linked_services: dict[str, AdfLinkedService] = field(default_factory=dict)
    triggers: list[AdfTrigger] = field(default_factory=list)


# ---------------------------------------------------------------------------
# Inventory / classification
# ---------------------------------------------------------------------------


@dataclass(slots=True, kw_only=True)
class InventoryItem:
    """Single row in the translation inventory.

    Attributes:
        pipeline_name: Owning pipeline name.
        activity_name: Activity display name.
        activity_type: ADF activity type string.
        strategy: Determined translation strategy.
        agentic_skill: Skill identifier when strategy is ``AGENTIC``.
        depends_on: Upstream activity names.
    """

    pipeline_name: str
    activity_name: str
    activity_type: str
    strategy: TranslationStrategy
    agentic_skill: str | None = None
    depends_on: list[str] | None = None


@dataclass(slots=True, kw_only=True)
class Inventory:
    """Aggregated translation inventory for all discovered pipelines.

    Attributes:
        items: Individual inventory rows.
        deterministic_count: Number of deterministically translatable activities.
        agentic_count: Number of activities requiring agentic translation.
        unsupported_count: Number of unsupported activities.
        pipeline_count: Total number of pipelines inventoried.
    """

    items: list[InventoryItem]
    deterministic_count: int = 0
    agentic_count: int = 0
    unsupported_count: int = 0
    pipeline_count: int = 0
