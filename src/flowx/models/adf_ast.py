"""Typed ADF AST nodes."""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Literal


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
        parameters: Runtime parameter overrides supplied by the activity,
            keyed by parameter name.  These flow into the resolver as
            ``@linkedService().X`` substitutions.
    """

    reference_name: str
    type: str = "LinkedServiceReference"
    parameters: dict[str, Any] | None = None


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
    # Original ADF/ARM activity JSON, retained so agentic handlers can translate from the source.
    raw: dict[str, Any] | None = None


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
        raw: Original ADF/ARM pipeline JSON as loaded from source, retained so
            the discover phase can emit a verbatim ``<pipeline>.arm.json`` into the
            bundle's metadata folder for provenance.
    """

    name: str
    activities: list[AdfActivity]
    parameters: dict[str, AdfParameter] | None = None
    variables: dict[str, AdfVariable] | None = None
    annotations: list[str] | None = None
    folder: str | None = None
    raw: dict[str, Any] | None = None


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
        global_parameters: Factory-level ``globalParameters`` keyed by name,
            with each value parsed into ``{"type": str, "value": Any}``.
    """

    pipelines: list[AdfPipeline]
    datasets: dict[str, AdfDataset] = field(default_factory=dict)
    linked_services: dict[str, AdfLinkedService] = field(default_factory=dict)
    triggers: list[AdfTrigger] = field(default_factory=list)
    global_parameters: dict[str, Any] = field(default_factory=dict)

    def get_dataset(self, name: str | None) -> AdfDataset | None:
        """Case-insensitive dataset lookup.

        LSC3-005: ADF identifiers are documented as case-insensitive; pipelines
        sometimes reference a dataset by a different casing than the source
        JSON file declares.  Tolerate the mismatch instead of returning None.
        """
        if not name:
            return None
        found = self.datasets.get(name)
        if found is not None:
            return found
        lowered = name.lower()
        for key, value in self.datasets.items():
            if key.lower() == lowered:
                return value
        return None

    def get_linked_service(self, name: str | None) -> AdfLinkedService | None:
        """Case-insensitive linked service lookup; see :meth:`get_dataset`."""
        if not name:
            return None
        found = self.linked_services.get(name)
        if found is not None:
            return found
        lowered = name.lower()
        for key, value in self.linked_services.items():
            if key.lower() == lowered:
                return value
        return None

    def get_pipeline(self, name: str | None) -> AdfPipeline | None:
        """Case-insensitive pipeline lookup; see :meth:`get_dataset`."""
        if not name:
            return None
        lowered = name.lower()
        exact = None
        for pipeline in self.pipelines:
            if pipeline.name == name:
                return pipeline
            if exact is None and pipeline.name.lower() == lowered:
                exact = pipeline
        return exact


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
        depends_on: Upstream activity names.
    """

    pipeline_name: str
    activity_name: str
    activity_type: str
    strategy: TranslationStrategy
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
    lineage: Lineage | None = None


# ---------------------------------------------------------------------------
# Lineage graphs
# ---------------------------------------------------------------------------


@dataclass(slots=True, kw_only=True)
class ControlEdge:
    """A cross-pipeline ExecutePipeline call edge (caller -> callee)."""

    caller_pipeline: str
    callee_pipeline: str
    activity_name: str
    wait_on_completion: bool = True


@dataclass(slots=True, kw_only=True)
class DataEdge:
    """A dataset producer -> consumer relationship across activities/pipelines.

    Attributes:
        dataset_name: Producer-side ADF dataset name (provenance only; the join
            is on ``match_key``, not this name).
        identity: Resolved physical identity (``schema.table`` / storage path)
            when ``match_kind == "identity"``; ``None`` otherwise.
        match_kind: How producer and consumer were matched:
            ``"identity"`` -- both resolve to the same physical asset (high
            confidence); ``"expression"`` -- both build the same normalized
            path signature from parameterized expressions (structural match,
            value unknown at discover time -- lower confidence).
        match_key: The value the join was made on -- the resolved identity for
            ``"identity"`` edges, or the normalized path signature for
            ``"expression"`` edges. Lets consumers see exactly what coupled them.
    """

    dataset_name: str
    identity: str | None
    producer_pipeline: str
    producer_activity: str
    consumer_pipeline: str
    consumer_activity: str
    match_kind: Literal["identity", "expression"] = "identity"
    match_key: str | None = None


@dataclass(slots=True, kw_only=True)
class Lineage:
    """Deterministic lineage graphs extracted from the parsed definitions."""

    control_edges: list[ControlEdge] = field(default_factory=list)
    data_edges: list[DataEdge] = field(default_factory=list)


# ---------------------------------------------------------------------------
# Agentic insights (discover phase) -- agent-authored judgment merged into
# inventory.json. References pipelines by name. Its cross-pipeline edges are
# either ANNOTATIONS of a deterministic Lineage edge (control/data -- carry no
# facts of their own) or an agent-INFERRED coupling the deterministic layer
# could not see (e.g. data flow that happens inside notebook code, an external
# trigger, a message queue). Inferred edges must cite their evidence and a
# confidence level so they are never mistaken for proven lineage.
# ---------------------------------------------------------------------------


@dataclass(slots=True, kw_only=True)
class LineageEdgeRef:
    """A typed reference from a PipelineRelationship to one cross-pipeline edge.

    Two tiers:

    * ``"control"`` / ``"data"`` -- an **annotation** of a deterministic edge.
      ``edge_identity`` echoes that edge verbatim (``ControlEdge.activity_name``
      for control, ``DataEdge.match_key`` for data) so enrichment can resolve it
      against the inventory's ``lineage``. ``evidence`` / ``confidence`` are not
      used (the deterministic edge *is* the evidence, confidence is implicitly
      high) and must be omitted.
    * ``"inferred"`` -- an agent-asserted coupling the deterministic layer did
      not find. There is no lineage edge to resolve against, so ``edge_identity``
      is an agent-authored descriptor of what couples the pipelines (e.g. a
      shared table or asset name), and ``evidence`` (why the agent believes the
      coupling exists) plus ``confidence`` are **required**. This tier stays
      pattern-agnostic: it does not encode *why* the deterministic layer missed
      the edge, so it generalises to couplings flowx cannot yet see.

    Attributes:
        edge_type: The tier -- ``"control"``, ``"data"``, or ``"inferred"``.
        edge_identity: For ``"control"`` the ``ControlEdge.activity_name``; for
            ``"data"`` the ``DataEdge.match_key`` (both echoed verbatim from a
            real edge); for ``"inferred"`` an agent-authored descriptor of the
            coupling.
        evidence: Inferred edges only -- the observable basis for the asserted
            coupling. Required for ``"inferred"``; must be omitted otherwise.
        confidence: Inferred edges only -- ``"high"`` / ``"medium"`` / ``"low"``.
            Required for ``"inferred"``; must be omitted otherwise.
    """

    edge_type: Literal["control", "data", "inferred"]
    edge_identity: str
    evidence: str | None = None
    confidence: Literal["high", "medium", "low"] | None = None


@dataclass(slots=True, kw_only=True)
class RecommendedPattern:
    """One ranked Databricks target pattern recommended for a pipeline.

    A pipeline insight carries 1-4 of these, ordered best-first, drawn from the
    agent's *holistic* read of the pipeline and grounded in publicly-documented
    Databricks capabilities. ``simplification_pattern`` ranks the distinctive
    capabilities that collapse a legacy pattern ahead of like-for-like ports and
    plain building blocks.

    Attributes:
        pattern: The named, publicly-documented Databricks capability (e.g.
            ``"Lakeflow Connect SQL Server connector"``). Never an invented name.
        fit: One line on why it fits this pipeline / what custom logic it replaces.
        simplification_pattern: ``True`` *only* when the pattern uses a **distinctive**
            Databricks capability that collapses or eliminates a whole legacy
            pattern -- a managed connector (Lakeflow Connect), declarative CDC
            (``AUTO CDC``), Auto Loader, or system tables replacing a home-grown
            logging tier. ``False`` for a like-for-like port AND for plain native
            building blocks that merely re-home the same work (a bare parameterized
            Lakeflow Job, a for-each/run-job orchestrator, a plain Delta control
            table, ``MERGE INTO``) -- "runs on Databricks" is not a simplification,
            so reserve this flag for the capability that makes the old pattern
            *disappear*. Rank the ``True`` patterns first.
    """

    pattern: str
    fit: str
    simplification_pattern: bool


@dataclass(slots=True, kw_only=True)
class SystemRecommendation:
    """The single top-level architectural decision spanning the whole factory.

    Per-pipeline ``recommended_patterns`` are chosen *under* this decision: the
    system-level branch you pick (e.g. adopt a managed connector for an entire
    extraction family) cascades into what each pipeline becomes, so it is authored
    first and the per-pipeline patterns are kept consistent with it. It captures
    the payoff a reader cannot see from any single pipeline card.

    Attributes:
        headline: One line naming the decision a migrator must make before any
            per-pipeline work (e.g. "Managed ingestion collapses the extraction
            factory").
        recommended_patterns: 1-4 whole-system target architectures, ordered
            best-first (the simplifying/native branch first), each a
            :class:`RecommendedPattern`. ``recommended_patterns[0]`` is the
            recommended branch; later entries are the ranked fallbacks.
        cascade: What choosing ``recommended_patterns[0]`` collapses or eliminates
            across the whole system (e.g. "5 child extractors -> managed connector
            pipelines", "version-watermark CSV -> gone"). Empty when the decision
            does not cascade.
        decision_driver: The gating question that selects the branch (e.g. "Is the
            Lakeflow Connect SQL Server connector GA/approved for this source?");
            omit when there is no single deciding factor.
    """

    headline: str
    recommended_patterns: list[RecommendedPattern] = field(default_factory=list)
    cascade: list[str] = field(default_factory=list)
    decision_driver: str | None = None


@dataclass(slots=True, kw_only=True)
class PipelineInsight:
    """Per-pipeline judgment; references a pipeline by name (foreign key)."""

    pipeline: str
    pattern_name: str | None = None
    intent: str | None = None
    databricks_pattern: str | None = None
    recommended_patterns: list[RecommendedPattern] = field(default_factory=list)
    conversion_notes: list[str] = field(default_factory=list)
    risk_if_ignored: str | None = None


@dataclass(slots=True, kw_only=True)
class PipelineRelationship:
    """Cross-pipeline judgment.

    Either annotates one deterministic lineage edge (``lineage_edge.edge_type``
    is ``"control"`` / ``"data"``) or records an agent-inferred coupling the
    deterministic layer could not see (``"inferred"``). Both endpoints are always
    real pipeline names validated against the inventory.
    """

    from_pipeline: str
    to_pipeline: str
    lineage_edge: LineageEdgeRef
    relationship_summary: str | None = None
    databricks_pattern: str | None = None
    risk_if_ignored: str | None = None


@dataclass(slots=True, kw_only=True)
class Insights:
    """Agent-authored insights merged into inventory.json under the ``insights`` key."""

    overview: str | None = None
    system_recommendation: SystemRecommendation | None = None
    pipeline_insights: list[PipelineInsight] = field(default_factory=list)
    pipeline_relationships: list[PipelineRelationship] = field(default_factory=list)
