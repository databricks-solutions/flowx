"""Translation IR -- intermediate representation after translation."""

from __future__ import annotations

from dataclasses import dataclass, field
from types import MappingProxyType
from typing import Any, TypeAlias


@dataclass(slots=True, kw_only=True)
class ExpressionResult:
    """Result of resolving an ADF expression."""

    kind: str  # "literal", "dab_ref", "notebook_code"
    value: str
    imports: list[str] = field(default_factory=list)
    required_parameters: dict[str, str] = field(default_factory=dict)


@dataclass(slots=True, kw_only=True)
class Dependency:
    """Dependency on an upstream task.

    Attributes:
        task_key: Task key of the upstream activity.
        outcome: Required outcome (e.g. ``"Succeeded"``) for this edge.
    """

    task_key: str
    outcome: str | None = None


@dataclass(slots=True, kw_only=True)
class Activity:
    """Base class for all translated pipeline activities.

    Attributes:
        name: Logical activity name from ADF.
        task_key: Unique task key within the workflow.
        description: Human-readable description.
        timeout_seconds: Maximum execution time in seconds.
        max_retries: Retry limit on failure.
        min_retry_interval_millis: Minimum delay between retries (ms).
        depends_on: Upstream task dependencies.
        cluster: Cluster configuration for the task, if any.
    """

    name: str
    task_key: str
    description: str | None = None
    timeout_seconds: int | None = None
    max_retries: int | None = None
    min_retry_interval_millis: int | None = None
    depends_on: list[Dependency] | None = None
    cluster: dict[str, Any] | None = None
    # Widget-name → DAB-ref mapping for every `dbutils.widgets.get()` call
    # that shows up in any notebook_code the translator produced for this
    # activity.  Preparers thread these into ``base_parameters`` so DAB
    # resolves the refs at job runtime.
    required_parameters: dict[str, str] = field(default_factory=dict)


@dataclass(slots=True, kw_only=True)
class NotebookActivity(Activity):
    """Databricks notebook activity.

    Attributes:
        notebook_path: Workspace path to the notebook.
        base_parameters: Parameters passed to the notebook at runtime.
        linked_service_definition: Raw linked-service dictionary for cluster config.
    """

    notebook_path: str
    base_parameters: dict[str, str] | None = None
    linked_service_definition: dict[str, Any] | None = None


@dataclass(slots=True, kw_only=True)
class CopyActivity(Activity):
    """Copy data activity.

    Attributes:
        source_type: Source dataset type string.
        sink_type: Sink dataset type string.
        source_properties: Parsed source format/connection options.
        sink_properties: Parsed sink format/connection options.
        sink_dataset_type: ADF dataset ``type`` of the sink (e.g. ``DelimitedText``,
            ``Parquet``, ``Json``, ``AzureSqlTable``).  Captured from the activity's
            output dataset so the code generator can write to the actual target
            format instead of always defaulting to Delta.
        sink_format: Spark format string derived from ``sink_dataset_type``
            (``csv``, ``parquet``, ``json``, ``delta``, ...).  ``None`` if the
            target is a table, not a file.
        sink_resolved_path: Resolved abfss:// or table location for the sink,
            mirroring ``source_properties.resolved_path`` for consistency.
        column_mapping: Column-level source-to-sink mappings.
    """

    source_type: str | None = None
    sink_type: str | None = None
    source_properties: dict[str, Any] | None = None
    sink_properties: dict[str, Any] | None = None
    sink_dataset_type: str | None = None
    sink_format: str | None = None
    sink_resolved_path: str | None = None
    column_mapping: list[dict[str, str]] | None = None


@dataclass(slots=True, kw_only=True)
class ForEachActivity(Activity):
    """ForEach loop activity.

    Attributes:
        items_expression: ADF expression driving the iteration.
        inner_activities: Translated activities executed for each item.
        concurrency: Maximum parallel iterations (maps to Databricks
            ``for_each_task.concurrency``).
    """

    items_expression: str
    inner_activities: list[Activity] = field(default_factory=list)
    concurrency: int | None = None


@dataclass(slots=True, kw_only=True)
class IfConditionActivity(Activity):
    """If condition branching activity.

    Attributes:
        op: Comparison operator name.
        left: Left-hand operand expression.
        right: Right-hand operand expression.
        if_true_activities: Activities for the true branch.
        if_false_activities: Activities for the false branch.
    """

    op: str
    left: str
    right: str
    if_true_activities: list[Activity] = field(default_factory=list)
    if_false_activities: list[Activity] = field(default_factory=list)


@dataclass(slots=True, kw_only=True)
class SetVariableActivity(Activity):
    """Sets variable activity.

    Attributes:
        variable_name: Name of the variable being set.
        variable_value: Expression string that evaluates to the value.
        value_kind: Kind of the resolved expression ("literal", "dab_ref", "notebook_code").
        notebook_code: Python code for notebook_code kind values.
        notebook_imports: Import statements needed for notebook_code.
    """

    variable_name: str
    variable_value: str
    value_kind: str = "literal"  # "literal", "dab_ref", "notebook_code"
    notebook_code: str | None = None
    notebook_imports: list[str] = field(default_factory=list)


@dataclass(slots=True, kw_only=True)
class LookupActivity(Activity):
    """Lookup activity.

    Attributes:
        source_type: Type of the lookup dataset.
        source_properties: Parsed source format/connection options.
        first_row_only: When True, only the first row is returned.
        source_query: Optional SQL query or stored-procedure call.
    """

    source_type: str | None = None
    source_properties: dict[str, Any] | None = None
    first_row_only: bool = True
    source_query: str | None = None


@dataclass(slots=True, kw_only=True)
class WebActivity(Activity):
    """Web / HTTP activity.

    Attributes:
        url: Target URL.
        method: HTTP method (GET, POST, etc.).
        body: Request body payload.
        headers: HTTP headers.
        authentication: Parsed authentication configuration.
        disable_cert_validation: Skip TLS verification when True.
        http_request_timeout_seconds: Request-level timeout.
    """

    url: str
    method: str
    body: Any = None
    headers: dict[str, str] | None = None
    authentication: dict[str, Any] | None = None
    disable_cert_validation: bool = False
    http_request_timeout_seconds: int | None = None


@dataclass(slots=True, kw_only=True)
class DeleteActivity(Activity):
    """Deletes files / folders activity.

    Attributes:
        dataset_name: Reference name of the target dataset.
        folder_path: Folder path to delete within the dataset.
        recursive: Remove contents recursively when True.
    """

    dataset_name: str
    folder_path: str | None = None
    recursive: bool = True


@dataclass(slots=True, kw_only=True)
class ExecutePipelineActivity(Activity):
    """Execute (nested) pipeline activity.

    Attributes:
        pipeline_name: Name of the child pipeline to invoke.
        parameters: Parameters passed to the child pipeline.
        wait_on_completion: Block until the child pipeline finishes.
    """

    pipeline_name: str
    parameters: dict[str, Any] | None = None
    wait_on_completion: bool = True


@dataclass(slots=True, kw_only=True)
class RunJobActivity(Activity):
    """Runs an existing Databricks job.

    Attributes:
        job_name: Name of the job to run.
        existing_job_id: ID of an existing job, if known.
        job_parameters: Parameters passed to the job at runtime.
    """

    job_name: str
    existing_job_id: str | None = None
    job_parameters: dict[str, Any] | None = None


@dataclass(slots=True, kw_only=True)
class SparkJarActivity(Activity):
    """Spark JAR activity.

    Attributes:
        main_class_name: Fully qualified main class within the JAR.
        parameters: Arguments passed to the main class.
        libraries: Library descriptors (JARs, wheels, etc.).
    """

    main_class_name: str
    parameters: list[str] | None = None
    libraries: list[dict[str, Any]] | None = None


@dataclass(slots=True, kw_only=True)
class SparkPythonActivity(Activity):
    """Spark Python activity.

    Attributes:
        python_file: Path to the Python file to execute.
        parameters: Arguments passed to the script.
    """

    python_file: str
    parameters: list[str] | None = None


@dataclass(slots=True, kw_only=True)
class SwitchCase:
    """A single case branch within a SwitchActivity.

    Attributes:
        value: The literal value to compare against the switch expression.
        activities: Activities to execute when this case matches.
    """

    value: str
    activities: list[Activity] = field(default_factory=list)


@dataclass(slots=True, kw_only=True)
class SwitchActivity(Activity):
    """Switch (multi-branch) activity.

    Attributes:
        on_expression: The ADF expression to evaluate.
        cases: Ordered list of case branches.
        default_activities: Activities to run when no case matches.
    """

    on_expression: str
    cases: list[SwitchCase] = field(default_factory=list)
    default_activities: list[Activity] = field(default_factory=list)


@dataclass(slots=True, kw_only=True)
class WaitActivity(Activity):
    """Wait / sleep activity.

    Attributes:
        wait_time_seconds: Duration to wait in seconds.
    """

    wait_time_seconds: int


@dataclass(slots=True, kw_only=True)
class FilterActivity(Activity):
    """Filters activity.

    Attributes:
        items_expression: ADF expression for the input array.
        condition_expression: Original ADF condition expression (preserved
            for documentation; never executed at runtime).
        condition_code: Python expression that evaluates to a bool against
            a per-iteration ``item`` dict.  ``None`` when the translator
            could not safely pre-resolve the condition; the code generator
            emits a TODO placeholder notebook in that case.
        condition_imports: Imports the ``condition_code`` expression
            requires (e.g. ``datetime``).
    """

    items_expression: str
    condition_expression: str
    condition_code: str | None = None
    condition_imports: list[str] = field(default_factory=list)


@dataclass(slots=True, kw_only=True)
class AppendVariableActivity(Activity):
    """Appends variable activity.

    Attributes:
        variable_name: Name of the array variable.
        append_value: Expression string that evaluates to the value to append.
        value_kind: Kind of the resolved expression ("literal", "dab_ref", "notebook_code").
        notebook_code: Python code for notebook_code kind values.
        notebook_imports: Import statements needed for notebook_code.
    """

    variable_name: str
    append_value: str
    value_kind: str = "literal"  # "literal", "dab_ref", "notebook_code"
    notebook_code: str | None = None
    notebook_imports: list[str] = field(default_factory=list)


@dataclass(slots=True, kw_only=True)
class UnsupportedActivity(Activity):
    """Sentinel for activities that could not be translated.

    Attributes:
        original_type: The ADF activity type that was not supported.
        reason: Human-readable explanation of why translation failed.
    """

    original_type: str
    reason: str | None = None


@dataclass(slots=True, kw_only=True)
class PlaceholderActivity(Activity):
    """Placeholder notebook for activities that require manual intervention.

    Attributes:
        original_type: The ADF activity type being replaced.
        notebook_path: Workspace path to the placeholder notebook.
        comment: Guidance for the user on what to implement.
    """

    original_type: str
    notebook_path: str = "/UNSUPPORTED_ADF_ACTIVITY"
    comment: str | None = None


@dataclass(slots=True, kw_only=True)
class MotifActivity(Activity):
    """Activity produced by collapsing a detected motif pattern.

    Attributes:
        motif_id: Identifier of the matched motif definition.
        display_name: Human-readable motif name.
        databricks_replacement: Target Databricks construct
            (e.g. ``"auto_loader"``, ``"dlt_apply_changes"``).
        matched_activity_names: Original ADF activity names that were collapsed.
        source_type_hint: Inferred source type (``"files"``, ``"database"``,
            ``"rest_api"``) or ``None``.
        confidence_notes: Detector notes explaining the match rationale.
        original_activities: The original translated Activity IR nodes that
            were replaced, preserved for reference and fallback.
        notebook_template: Name of the code generator template, if any.
    """

    motif_id: str
    display_name: str
    databricks_replacement: str
    matched_activity_names: list[str]
    source_type_hint: str | None = None
    confidence_notes: list[str] = field(default_factory=list)
    original_activities: list[Activity] = field(default_factory=list)
    notebook_template: str | None = None
    # Small dict of motif-specific settings extracted from the collapsed
    # activities — e.g. ``{"lookup_query": ..., "lookup_scope": ...}`` for
    # ``for_each_ingestion``.  Used by the notebook generator so the motif
    # can fetch its input list itself instead of requiring an ``items``
    # widget that has no upstream writer.
    motif_config: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True, kw_only=True)
class Pipeline:
    """Top-level workflow container produced by the translator.

    Attributes:
        name: Logical pipeline name.
        parameters: Pipeline parameter definitions.
        schedule: Serialized schedule definition, if any.
        tasks: Ordered list of translated activities.
        tags: System and user-defined tags.
        not_translatable: Entries describing properties that could not be translated.
    """

    name: str
    parameters: list[dict[str, Any]] | None = None
    schedule: dict[str, Any] | None = None
    tasks: list[Activity] = field(default_factory=list)
    tags: dict[str, str] = field(default_factory=dict)
    not_translatable: list[dict[str, Any]] = field(default_factory=list)


@dataclass(frozen=True, slots=True)
class TranslationContext:
    """Immutable snapshot of translation state threaded through each visitor call.

    Attributes:
        activity_cache: Read-only mapping of activity names to translated activities.
        registry: Read-only mapping of activity type strings to translator callables.
        variable_cache: Read-only mapping of variable names to the task keys
            of the tasks that set them.
    """

    activity_cache: MappingProxyType[str, Activity] = field(default_factory=lambda: MappingProxyType({}))
    registry: MappingProxyType[str, Any] = field(default_factory=lambda: MappingProxyType({}))
    variable_cache: MappingProxyType[str, str] = field(default_factory=lambda: MappingProxyType({}))
    variable_value_cache: MappingProxyType[str, str] = field(default_factory=lambda: MappingProxyType({}))

    def with_activity(self, name: str, activity: Activity) -> TranslationContext:
        """Return a new context with *activity* added to the cache.

        Args:
            name: Activity name used as the cache key.
            activity: Translated activity to store.

        Returns:
            New ``TranslationContext`` containing the updated activity cache.
        """
        return TranslationContext(
            activity_cache=MappingProxyType({**self.activity_cache, name: activity}),
            registry=self.registry,
            variable_cache=self.variable_cache,
            variable_value_cache=self.variable_value_cache,
        )

    def get_activity(self, activity_name: str) -> Activity | None:
        """Look up a previously translated activity by name.

        Args:
            activity_name: Activity name.

        Returns:
            Cached ``Activity`` or ``None`` if not yet visited.
        """
        return self.activity_cache.get(activity_name)

    def with_variable(
        self,
        variable_name: str,
        task_key: str,
        *,
        dab_ref_value: str | None = None,
    ) -> TranslationContext:
        """Return a new context with a variable mapping added.

        Args:
            variable_name: Variable name.
            task_key: Task key of the task that sets this variable.
            dab_ref_value: When the variable's value resolves to a DAB
                dynamic value reference (e.g. ``{{job.start_time.iso_datetime}}``),
                store it so downstream ``@variables()`` calls can inline the
                ref instead of routing through the task value.

        Returns:
            New ``TranslationContext`` containing the updated caches.
        """
        new_variable_value_cache = self.variable_value_cache
        if dab_ref_value is not None:
            new_variable_value_cache = MappingProxyType({**self.variable_value_cache, variable_name: dab_ref_value})
        return TranslationContext(
            activity_cache=self.activity_cache,
            registry=self.registry,
            variable_cache=MappingProxyType({**self.variable_cache, variable_name: task_key}),
            variable_value_cache=new_variable_value_cache,
        )

    def get_variable_task_key(self, variable_name: str) -> str | None:
        """Look up the task key that sets a variable."""
        return self.variable_cache.get(variable_name)

    def get_variable_dab_ref(self, variable_name: str) -> str | None:
        """Look up the inlined DAB ref value for a variable, if available."""
        return self.variable_value_cache.get(variable_name)


TranslationResult: TypeAlias = Activity | UnsupportedActivity


@dataclass(slots=True, kw_only=True)
class AgenticGap:
    """Describes an activity that requires agentic translation.

    Attributes:
        activity_name: Display name of the activity.
        activity_type: ADF activity type string.
        recommended_skill: Skill identifier to use for translation.
        raw_definition: Original ADF JSON definition for the activity.
    """

    activity_name: str
    activity_type: str
    recommended_skill: str | None = None
    raw_definition: dict[str, Any] | None = None


@dataclass(slots=True, kw_only=True)
class TranslationReport:
    """Summary produced after translating an entire ADF pipeline.

    Attributes:
        pipeline: The translated pipeline IR.
        deterministic_count: Activities translated deterministically.
        agentic_count: Activities requiring agentic translation.
        unsupported_count: Activities that could not be translated.
        gaps: List of agentic gaps identified during translation.
        warnings: Human-readable warning messages emitted during translation.
    """

    pipeline: Pipeline
    deterministic_count: int = 0
    agentic_count: int = 0
    unsupported_count: int = 0
    gaps: list[AgenticGap] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
