"""Translation IR -- intermediate representation after translation."""

from __future__ import annotations

from dataclasses import dataclass, field
from types import MappingProxyType
from typing import TYPE_CHECKING, Any, TypeAlias

if TYPE_CHECKING:
    from orchestra.adapter.models import TranslationPreferences


@dataclass(slots=True, kw_only=True)
class ExpressionResult:
    """Result of resolving an ADF expression.

    Attributes:
        kind: One of ``"literal"`` / ``"dab_ref"`` / ``"notebook_code"``.
        value: The resolved value text.
        imports: Imports the notebook_code value needs.
        required_parameters: Widget name -> DAB ref mapping for
            base_parameters threading.
        notes: Free-form caveats surfaced in SETUP.md.
        was_string_literal: C-34 (VAREX4-002): True when the original
            ADF token was a quoted string (``'09'``, ``"12"``) so the
            function-call codegen path can ``repr()`` it instead of
            emitting a bare numeric token that strips quotedness.
        was_bool_literal: C-34 (VAREX4-003): True when the original ADF
            token was ``true`` / ``false``.  ADF Booleans serialise as
            the lowercase strings ``'true'``/``'false'`` on the
            SetVariable consumer side (post-C-21), so comparisons must
            emit ``'true'`` / ``'false'`` Python strings rather than the
            bare Python ``True`` / ``False``.
    """

    kind: str
    value: str
    imports: list[str] = field(default_factory=list)
    required_parameters: dict[str, str] = field(default_factory=dict)
    notes: list[str] = field(default_factory=list)
    was_string_literal: bool = False
    was_bool_literal: bool = False


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
        existing_cluster_id: ID of an existing all-purpose cluster the task
            should run on.
        libraries: Task-scoped library descriptors carried through from ADF.
            Each entry is a supported Databricks task library; see
            https://docs.databricks.com/aws/en/dev-tools/bundles/library-dependencies
            for the supported shapes.
    """

    name: str
    task_key: str
    description: str | None = None
    timeout_seconds: int | None = None
    max_retries: int | None = None
    min_retry_interval_millis: int | None = None
    depends_on: list[Dependency] | None = None
    cluster: dict[str, Any] | None = None
    existing_cluster_id: str | None = None
    libraries: list[dict[str, Any]] | None = None
    # Approximate parameter substitutions made at translation time (e.g.
    # ``utcnow()`` mapped to ``{{job.start_time.iso_datetime}}``).  Each
    # entry has keys ``widget_name``, ``raw_expression``, ``replacement``,
    # and ``note``; the bundler surfaces these in SETUP.md.
    parameter_approximations: list[dict[str, str]] = field(default_factory=list)
    required_parameters: dict[str, str] = field(default_factory=dict)
    # Compute mode stamped by the pipeline modifier in response to user
    # preferences.  One of "serverless", "classic_single_node",
    # "classic_multi_node", "inherit", or None when no preferences were applied.
    compute_mode: str | None = None


@dataclass(slots=True, kw_only=True)
class NotebookActivity(Activity):
    """Databricks notebook activity.

    Attributes:
        notebook_path: Workspace path to the notebook.
        base_parameters: Parameters passed to the notebook at runtime.
        linked_service_definition: Raw linked-service dictionary for cluster config.
        notebook_path_unresolved: C-28 (NB-ITER4-001): True when the ADF
            ``notebookPath`` is a dynamic expression the translator couldn't
            reduce to a literal/dab_ref workspace path.  The preparer emits
            a dispatch-stub notebook that reads ``notebook_path`` from a
            widget and ``dbutils.notebook.run()``s the resolved value.
        notebook_path_expression: Raw ADF expression text captured when
            ``notebook_path_unresolved`` is True, surfaced in SETUP.md.
        unresolved_libraries: C-30 (NB-ITER4-003): library descriptor
            entries whose value (jar/whl/egg/requirements path) carried an
            ADF expression the resolver couldn't reduce to a literal or
            dab_ref.  Each entry has ``type`` (library shape key),
            ``expression`` (raw ADF text), and ``missing`` (referenced
            identifier names not bound in the translation context).
    """

    notebook_path: str
    base_parameters: dict[str, str] | None = None
    linked_service_definition: dict[str, Any] | None = None
    notebook_path_unresolved: bool = False
    notebook_path_expression: str | None = None
    unresolved_libraries: list[dict[str, Any]] = field(default_factory=list)


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
    # Code paradigm chosen by the pipeline modifier: "notebook" (default
    # PySpark output) or "sdp" (Lakeflow Spark Declarative Pipeline).
    target_format: str | None = None
    # True when the modifier selected Lakeflow Connect for an eligible
    # database-source Copy → Delta ingestion.
    use_lakeflow_connector: bool = False
    # Lakeflow Connect connector flavour resolved by the modifier when
    # use_lakeflow_connector is True: "query_based" or "cdc".  None when
    # the modifier did not stamp a connector type.
    lakeflow_connector_type: str | None = None


@dataclass(slots=True, kw_only=True)
class ForEachActivity(Activity):
    """ForEach loop activity.

    Attributes:
        items_expression: ADF expression driving the iteration.
        inner_activities: Translated activities executed for each item.
        concurrency: Maximum parallel iterations (maps to Databricks
            ``for_each_task.concurrency``).
        inputs_bridge_notebook_code: C-31 (CF4-001): when the items
            expression resolves to ``notebook_code`` (e.g.
            ``@split(variables('fecha'),',')``), the translator captures
            the Python code here while the full TranslationContext is
            available.  The preparer reads it instead of re-resolving
            against an empty context (which silently failed before).
        inputs_bridge_notebook_imports: Imports the bridge code needs.
        inputs_bridge_required_parameters: Widget name → DAB ref mapping
            for the bridge notebook's base_parameters.
    """

    items_expression: str
    inner_activities: list[Activity] = field(default_factory=list)
    concurrency: int | None = None
    inputs_bridge_notebook_code: str | None = None
    inputs_bridge_notebook_imports: list[str] = field(default_factory=list)
    inputs_bridge_required_parameters: dict[str, str] = field(default_factory=dict)


@dataclass(slots=True, kw_only=True)
class IfConditionActivity(Activity):
    """If condition branching activity.

    Attributes:
        op: Comparison operator name.
        left: Left-hand operand expression.
        right: Right-hand operand expression.
        if_true_activities: Activities for the true branch.
        if_false_activities: Activities for the false branch.
        bridge_notebook_code: C-07 (CF-iter2-001 / VAREX-003): when the
            ADF condition expression contained a function call that
            couldn't be lowered to a literal/dab_ref operand,
            ``bridge_notebook_code`` carries the Python code that
            evaluates it.  The preparer synthesises a hidden SetVariable
            task that runs this code and points ``left`` at the
            resulting task value.
        bridge_notebook_imports: Imports the bridge notebook code needs.
        bridge_required_parameters: Widget name -> DAB ref mapping for
            the bridge notebook's base_parameters.
    """

    op: str
    left: str
    right: str
    if_true_activities: list[Activity] = field(default_factory=list)
    if_false_activities: list[Activity] = field(default_factory=list)
    bridge_notebook_code: str | None = None
    bridge_notebook_imports: list[str] = field(default_factory=list)
    bridge_required_parameters: dict[str, str] = field(default_factory=dict)


@dataclass(slots=True, kw_only=True)
class SetVariableActivity(Activity):
    """Sets variable activity.

    Attributes:
        variable_name: Name of the variable being set.
        variable_value: Expression string that evaluates to the value.
        value_kind: Kind of the resolved expression ("literal", "dab_ref",
            "notebook_code", "unresolved").
        notebook_code: Python code for notebook_code kind values.
        notebook_imports: Import statements needed for notebook_code.
        raw_expression: C-33 (VAREX4-001 / CF4-003): when ``value_kind`` is
            ``"unresolved"`` (the resolver returned None for an ADF
            ``@``-prefixed value), this carries the original ADF
            expression text so SETUP.md can surface the manual
            initialisation step.
    """

    variable_name: str
    variable_value: str
    value_kind: str = "literal"  # "literal", "dab_ref", "notebook_code", "unresolved"
    notebook_code: str | None = None
    notebook_imports: list[str] = field(default_factory=list)
    raw_expression: str | None = None


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
    """

    main_class_name: str
    parameters: list[str] | None = None


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
        bridge_notebook_code: C-07 (CF-iter2-001 / CF-iter2-003): when
            ``on_expression`` cannot be lowered to a literal/dab_ref, this
            field carries the Python code the preparer runs in a bridge
            task so the resolved value drives ``condition_task.left``.
        bridge_notebook_imports: Imports for the bridge notebook code.
        bridge_required_parameters: Widget name -> DAB ref mapping for
            the bridge notebook's base_parameters.
    """

    on_expression: str
    cases: list[SwitchCase] = field(default_factory=list)
    default_activities: list[Activity] = field(default_factory=list)
    bridge_notebook_code: str | None = None
    bridge_notebook_imports: list[str] = field(default_factory=list)
    bridge_required_parameters: dict[str, str] = field(default_factory=dict)


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
    # Set by the pipeline modifier when the user opts into metadata-driven
    # consolidation, has access to query the lookup source, and the
    # configuration size is S or M.  When True the preparer should emit
    # a single consolidated pipeline whose objects come from lookup_values.
    consolidate_metadata_driven: bool = False
    # Concrete lookup rows materialised at translation time (CLI
    # ``materialize-lookup`` subcommand or agent-supplied JSON).  Each
    # element is a dict mirroring a row from the original ADF Lookup
    # query.  Empty when consolidation is requested but values have not
    # been resolved yet.
    lookup_values: list[dict[str, Any]] = field(default_factory=list)
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
    translation_preferences: TranslationPreferences | None = None


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
    variable_types: MappingProxyType[str, str] = field(default_factory=lambda: MappingProxyType({}))
    variable_default_literals: MappingProxyType[str, str] = field(default_factory=lambda: MappingProxyType({}))
    global_parameters: MappingProxyType[str, Any] = field(default_factory=lambda: MappingProxyType({}))
    linked_service_parameters: MappingProxyType[str, Any] = field(default_factory=lambda: MappingProxyType({}))

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
            variable_types=self.variable_types,
            variable_default_literals=self.variable_default_literals,
            global_parameters=self.global_parameters,
            linked_service_parameters=self.linked_service_parameters,
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
            variable_types=self.variable_types,
            variable_default_literals=self.variable_default_literals,
            global_parameters=self.global_parameters,
            linked_service_parameters=self.linked_service_parameters,
        )

    def with_variable_types(
        self,
        types: dict[str, str],
        *,
        default_literals: dict[str, str] | None = None,
    ) -> TranslationContext:
        """Return a new context seeded with declared variable types.

        Args:
            types: Mapping of variable name -> ADF declared type
                (``"String"``, ``"Boolean"``, ``"Array"``, ...).  Used by
                the IfCondition fallback to recognise Boolean variables
                whose value is seeded only by a literal init task (and
                therefore absent from ``variable_value_cache``).
            default_literals: Optional mapping of variable name -> seeded
                literal default (e.g. ``"true"``/``"false"``).  The
                IfCondition bridge (C-43) recomputes a Boolean operand
                locally from this literal so an inner-ForEach condition does
                not dangle to a parent-job task value.

        Returns:
            New context carrying the variable type / default-literal maps.
        """
        return TranslationContext(
            activity_cache=self.activity_cache,
            registry=self.registry,
            variable_cache=self.variable_cache,
            variable_value_cache=self.variable_value_cache,
            variable_types=MappingProxyType({**self.variable_types, **types}),
            variable_default_literals=MappingProxyType({**self.variable_default_literals, **(default_literals or {})}),
            global_parameters=self.global_parameters,
            linked_service_parameters=self.linked_service_parameters,
        )

    def get_variable_task_key(self, variable_name: str) -> str | None:
        """Look up the task key that sets a variable."""
        return self.variable_cache.get(variable_name)

    def get_variable_type(self, variable_name: str) -> str | None:
        """Look up a variable's declared ADF type, if known."""
        return self.variable_types.get(variable_name)

    def get_variable_default_literal(self, variable_name: str) -> str | None:
        """Look up a variable's seeded literal default value, if known."""
        return self.variable_default_literals.get(variable_name)

    def get_variable_dab_ref(self, variable_name: str) -> str | None:
        """Look up the inlined DAB ref value for a variable, if available."""
        return self.variable_value_cache.get(variable_name)

    def with_linked_service_parameters(self, params: dict[str, Any]) -> TranslationContext:
        """Return a new context with linked-service-scoped parameters applied.

        Args:
            params: Mapping of LS parameter name -> resolved value.  Used
                by ``@linkedService().X`` references in LS typeProperties.

        Returns:
            New context with the parameters bound for the current activity.
        """
        return TranslationContext(
            activity_cache=self.activity_cache,
            registry=self.registry,
            variable_cache=self.variable_cache,
            variable_value_cache=self.variable_value_cache,
            variable_types=self.variable_types,
            variable_default_literals=self.variable_default_literals,
            global_parameters=self.global_parameters,
            linked_service_parameters=MappingProxyType(dict(params)),
        )

    def get_global_parameter(self, name: str) -> Any:
        """Look up a factory-level global parameter value.

        Args:
            name: Global parameter name (e.g. ``"env_variable"``).

        Returns:
            The parameter value if present, else ``None``.  Values may be
            scalar or dict-typed (e.g. ``{"type": "string", "value": "t"}``).
        """
        raw = self.global_parameters.get(name)
        if isinstance(raw, dict) and "value" in raw:
            return raw["value"]
        return raw

    def get_linked_service_parameter(self, name: str) -> Any:
        """Look up an activity-supplied linked-service parameter value."""
        return self.linked_service_parameters.get(name)


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
        warnings: Human-readable warning messages emitted during translation,
            including unresolved ``@{...}`` ADF expressions surfaced by the
            whole-IR rewriter.
        detected_motifs: Multi-activity patterns the detector matched on the
            source AST.  When the caller did not supply a motif-consolidation
            answer the translator collapses every entry into a single
            :class:`MotifActivity`; otherwise the list still reports what
            was detected so the adapter can prompt the user.  The objects
            here are :class:`~orchestra.models.motifs.DetectedMotif` instances.
    """

    pipeline: Pipeline
    deterministic_count: int = 0
    agentic_count: int = 0
    unsupported_count: int = 0
    gaps: list[AgenticGap] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    detected_motifs: list[Any] = field(default_factory=list)
