"""Agent adapter that drives the ask-validate-resume loop.

:class:`TranslationSession` is the entry point an agent uses to
translate tool-call arguments into validated configuration.  When the IR
raises options the agent cannot answer from context alone, the
session surfaces them as structured :class:`TranslationOption`
objects (and, via :exc:`TranslationInputRequired`, as exceptions) so
the agent can route them back to the user.  The pipeline modifier is
invoked only once every option has an answer.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from flowx.adapter.constants import (
    INPUT_ADF_RESOURCE_URL,
    INPUT_ADF_SOURCE_PATH,
    INPUT_BUNDLE_NAME,
    INPUT_CATALOG,
    INPUT_DATABRICKS_PROFILE,
    INPUT_GLOBAL_PARAMETER_RESOLUTION,
    INPUT_INSTALL_DASHBOARD,
    INPUT_INVENTORY_PATH,
    INPUT_OUTPUT_BUNDLE_PATH,
    INPUT_OUTPUT_DIR,
    INPUT_RESULTS_TABLE,
    INPUT_RESULTS_WAREHOUSE,
    INPUT_SCHEMA,
    INPUT_TRANSLATION_REPORT_PATH,
    MOTIF_CONSOLIDATE_OPTION_PREFIX,
    PHASE_CONVERT,
    PHASE_DISCOVER,
    PHASE_PACKAGE,
)
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
    PendingMigrationInputs,
    PendingOptions,
    TranslationConfiguration,
    TranslationOption,
    UseLakeflowConnectors,
)
from flowx.adapter.operations import (
    apply_configuration,
    gather_options,
    validate_answer,
)
from flowx.models.ir import Pipeline
from flowx.models.motifs import DetectedMotif


class TranslationInputRequired(Exception):
    """Raised by :meth:`TranslationSession.run` when answers are still missing.

    Attributes:
        pending: The outstanding options the agent should route to the
            user before retrying :meth:`TranslationSession.run`.
    """

    def __init__(self, pending: PendingOptions) -> None:
        """Stores the pending options on the exception.

        Args:
            pending: Outstanding options surfaced by the session.
        """
        super().__init__(
            f"{len(pending.options)} translation option(s) require user input for pipeline {pending.pipeline_name!r}"
        )
        self.pending = pending


@dataclass(slots=True, kw_only=True)
class TranslationSession:
    """Coordinates the ask-validate-resume loop for one translated pipeline.

    A session is single-use: the caller drives it by either polling via
    :meth:`pending` and :meth:`answer`, or calling :meth:`run` and
    handling :exc:`TranslationInputRequired`.  When every option is
    answered, :meth:`run` (or :meth:`resume`) returns the
    configuration-stamped pipeline.

    Attributes:
        pipeline: Translated pipeline IR after motif collapsing.
        motifs: Detected motifs for the pipeline.  Optional; only used to
            decide whether the Lakeflow Connect option applies.
        defaults: Baseline configuration applied when the caller skips a
            option.  Per-task overrides on this object are preserved
            verbatim when :meth:`build_configuration` composes the final
            snapshot.
    """

    pipeline: Pipeline
    motifs: list[DetectedMotif] = field(default_factory=list)
    defaults: TranslationConfiguration = DEFAULT_CONFIGURATION
    _answers: dict[str, str] = field(default_factory=dict)

    def pending(self) -> PendingOptions:
        """Returns the options still awaiting an answer.

        Returns:
            A :class:`PendingOptions` instance containing only the
            options whose preconditions are met by the IR and whose
            IDs are not yet in the answer set.
        """
        return gather_options(
            self.pipeline,
            self.motifs,
            answers=self._answers,
        )

    def answer(self, option_id: str, value: str) -> None:
        """Validates and records a single answer.

        Args:
            option_id: Stable option identifier from
                :class:`TranslationOption`.
            value: Caller-supplied answer string.

        Raises:
            ValueError: When *option_id* is unknown or *value* is not
                in the allowed set for the option.
        """
        self._answers[option_id] = validate_answer(option_id, value)

    def answer_many(self, answers: dict[str, str]) -> None:
        """Validates and records multiple answers atomically.

        Args:
            answers: Mapping of option_id to the caller-supplied answer.

        Raises:
            ValueError: When any pair fails validation.  No answers from
                the batch are recorded when the call raises.
        """
        validated = {qid: validate_answer(qid, value) for qid, value in answers.items()}
        self._answers.update(validated)

    def find_option(self, option_id: str) -> TranslationOption | None:
        """Looks up a pending option by its identifier.

        Args:
            option_id: Stable option identifier.

        Returns:
            The matching :class:`TranslationOption` if it is still
            pending, otherwise ``None``.
        """
        return next(
            (option for option in self.pending().options if option.option_id == option_id),
            None,
        )

    def build_configuration(self) -> TranslationConfiguration:
        """Composes the validated configuration snapshot from collected answers.

        Returns:
            A :class:`TranslationConfiguration` where every answered field
            takes the caller-supplied value and every unanswered field
            falls back to the corresponding value on ``defaults``.
        """
        return TranslationConfiguration(
            copy_activity_paradigm=CopyActivityParadigm(
                self._answers.get("copy_activity_paradigm", self.defaults.copy_activity_paradigm)
            ),
            non_databricks_task_compute=NonDatabricksTaskCompute(
                self._answers.get("non_databricks_task_compute", self.defaults.non_databricks_task_compute)
            ),
            use_lakeflow_connectors=UseLakeflowConnectors(
                self._answers.get("use_lakeflow_connectors", self.defaults.use_lakeflow_connectors)
            ),
            lakeflow_connector_type=LakeflowConnectorType(
                self._answers.get("lakeflow_connector_type", self.defaults.lakeflow_connector_type)
            ),
            metadata_driven_consolidate=MetadataDrivenConsolidate(
                self._answers.get("metadata_driven_consolidate", self.defaults.metadata_driven_consolidate)
            ),
            metadata_driven_access=MetadataDrivenAccess(
                self._answers.get("metadata_driven_access", self.defaults.metadata_driven_access)
            ),
            metadata_driven_size=MetadataDrivenSize(
                self._answers.get("metadata_driven_size", self.defaults.metadata_driven_size)
            ),
            metadata_driven_lookup_tool=MetadataDrivenLookupTool(
                self._answers.get("metadata_driven_lookup_tool", self.defaults.metadata_driven_lookup_tool)
            ),
            motif_consolidations=self._collect_motif_consolidations(),
            per_task=self.defaults.per_task,
        )

    def resume(self) -> Pipeline:
        """Returns the configuration-stamped pipeline IR.

        Returns:
            A new :class:`Pipeline` produced by applying the composed
            configuration to ``self.pipeline``.  The input pipeline is not
            mutated.
        """
        return apply_configuration(self.pipeline, self.build_configuration())

    def run(self) -> Pipeline:
        """Returns the modified pipeline, raising when input is still required.

        Returns:
            The configuration-stamped pipeline IR when every applicable
            option has an answer.

        Raises:
            TranslationInputRequired: When one or more options are
                still outstanding.  The exception carries the pending
                options so the agent can route them to the user.
        """
        pending = self.pending()
        if pending.options:
            raise TranslationInputRequired(pending)
        return self.resume()

    def _collect_motif_consolidations(self) -> dict[str, MotifConsolidate]:
        """Returns the per-motif consolidation answers gathered so far.

        Returns:
            Dict mapping ``motif_id`` to the user's :class:`MotifConsolidate` answer. Motifs the user
            did not answer fall back to ``self.defaults`` (default :data:`MotifConsolidate.KEEP`).
        """
        consolidations: dict[str, MotifConsolidate] = dict(self.defaults.motif_consolidations)
        for option_id, answer in self._answers.items():
            if not option_id.startswith(MOTIF_CONSOLIDATE_OPTION_PREFIX):
                continue
            motif_id = option_id[len(MOTIF_CONSOLIDATE_OPTION_PREFIX) :]
            consolidations[motif_id] = MotifConsolidate(answer)
        return consolidations


_DISCOVER_OPTIONS: tuple[MigrationInputOption, ...] = (
    MigrationInputOption(
        option_id=INPUT_ADF_SOURCE_PATH,
        prompt="Where are the ADF JSON exports?",
        description=(
            "Unity Catalog volume path (``/Volumes/<catalog>/<schema>/<volume>``) "
            "or a local directory containing the ADF ARM/JSON export."
        ),
        required=True,
    ),
    MigrationInputOption(
        option_id=INPUT_ADF_RESOURCE_URL,
        prompt="ADF resource URL?",
        description=(
            "Azure portal URL of the source Data Factory.  Captured for "
            "traceability and surfaced in the generated bundle README; "
            "leave blank when the source is exported from a local copy."
        ),
        default="",
        required=False,
    ),
    MigrationInputOption(
        option_id=INPUT_OUTPUT_DIR,
        prompt="Which migration output directory should flowx use?",
        description=(
            "Single shared migration directory used by every phase (default ``./flowx_output``). "
            "Discover writes ``metadata/inventory.json``, ``metadata/profile_report.csv``, and the "
            "verbatim ``metadata/<pipeline>.arm.json`` into it."
        ),
        default="./flowx_output",
        required=False,
    ),
)

_CONVERT_OPTIONS: tuple[MigrationInputOption, ...] = (
    MigrationInputOption(
        option_id=INPUT_INVENTORY_PATH,
        prompt="Path to the inventory.json from the discover phase?",
        description="Inventory produced by the discover phase (under the shared migration dir's metadata/).",
        default="./flowx_output/metadata/inventory.json",
        required=False,
    ),
    MigrationInputOption(
        option_id=INPUT_ADF_SOURCE_PATH,
        prompt="Path to the ADF JSON exports?",
        description="Same source directory the discover phase consumed; needed for cross-references.",
        required=True,
    ),
    MigrationInputOption(
        option_id=INPUT_OUTPUT_DIR,
        prompt="Which migration output directory should flowx use?",
        description=(
            "The same shared migration directory the discover phase used (default ``./flowx_output``). "
            "Convert writes its transient report and IR to the directory's ``.work/`` subfolder."
        ),
        default="./flowx_output",
        required=False,
    ),
    MigrationInputOption(
        option_id=INPUT_GLOBAL_PARAMETER_RESOLUTION,
        prompt="How should factory global parameters be resolved?",
        description=(
            "Applies to every pipeline. 'literal' bakes each @pipeline().globalParameters.X value in as a "
            "literal; 'bundle_variable' emits ${var.X} and declares the global as a DAB bundle variable with "
            "the factory value as its default, so it can be changed at deploy time."
        ),
        default="literal",
        required=False,
    ),
)

_PACKAGE_OPTIONS: tuple[MigrationInputOption, ...] = (
    MigrationInputOption(
        option_id=INPUT_TRANSLATION_REPORT_PATH,
        prompt="Path to the translation report?",
        description=(
            "Configuration-stamped report from `python -m flowx.adapter modify`, "
            "or the raw convert-phase report when no configuration were applied."
        ),
        default="./flowx_output/.work/translation_report.stamped.json",
        required=False,
    ),
    MigrationInputOption(
        option_id=INPUT_OUTPUT_BUNDLE_PATH,
        prompt="Which migration output directory should flowx use?",
        description=(
            "The same shared migration directory used by discover/convert (default ``./flowx_output``). "
            "Package writes the DAB bundle at its top level and prunes the transient ``.work/`` folder."
        ),
        default="./flowx_output",
        required=False,
    ),
    MigrationInputOption(
        option_id=INPUT_CATALOG,
        prompt="Target Unity Catalog catalog?",
        description="Default ``catalog`` bundle variable used by emitted notebooks and pipelines.",
        default="main",
        required=False,
    ),
    MigrationInputOption(
        option_id=INPUT_SCHEMA,
        prompt="Target Unity Catalog schema?",
        description="Default ``schema`` bundle variable used by emitted notebooks and pipelines.",
        default="default",
        required=False,
    ),
    MigrationInputOption(
        option_id=INPUT_BUNDLE_NAME,
        prompt="Bundle name override?",
        description="Defaults to the first translated pipeline's resource key when blank.",
        default="",
        required=False,
    ),
    MigrationInputOption(
        option_id=INPUT_DATABRICKS_PROFILE,
        prompt="Databricks CLI profile?",
        description=(
            "Profile used to download workspace-resident notebooks during the "
            "package phase.  Leave blank to use the default profile from "
            "``~/.databrickscfg`` or the active ``DATABRICKS_*`` env vars."
        ),
        default="",
        required=False,
    ),
    MigrationInputOption(
        option_id=INPUT_RESULTS_TABLE,
        prompt="Record migration coverage to a Unity Catalog table? If so, the table (catalog.schema.table)?",
        description=(
            "Optional. When set, the package phase writes one coverage row per pipeline to this UC "
            "table, stamped with a UUID run_id, run_date (CURRENT_TIMESTAMP()), and run_by "
            "(CURRENT_USER()). Leave blank to skip. Requires workspace auth (Genie Code / a profile)."
        ),
        default="",
        required=False,
    ),
    MigrationInputOption(
        option_id=INPUT_RESULTS_WAREHOUSE,
        prompt="SQL warehouse id for writing the results table / backing the dashboard?",
        description=(
            "Optional. Warehouse used to run the CREATE/INSERT and back the dashboard. Leave blank to "
            "auto-detect (prefers a running, serverless warehouse)."
        ),
        default="",
        required=False,
    ),
    MigrationInputOption(
        option_id=INPUT_INSTALL_DASHBOARD,
        prompt="Install a published AI/BI coverage dashboard over the results table? (yes/no)",
        description=(
            "Optional. When 'yes' (and a results table is set), installs and publishes a Lakeview "
            "dashboard that visualizes migration coverage from the table."
        ),
        default="no",
        required=False,
    ),
)

_OPTIONS_BY_PHASE: dict[str, tuple[MigrationInputOption, ...]] = {
    PHASE_DISCOVER: _DISCOVER_OPTIONS,
    PHASE_CONVERT: _CONVERT_OPTIONS,
    PHASE_PACKAGE: _PACKAGE_OPTIONS,
}


class UnknownMigrationPhaseError(ValueError):
    """Raised when a MigrationInputSession is constructed with an unrecognised phase."""


@dataclass(slots=True, kw_only=True)
class MigrationInputSession:
    """Coordinates the free-text input prompts at the top of an flowx phase.

    A session is single-use: the caller drives it by polling
    :meth:`pending` and recording answers via :meth:`answer`, then reads
    them out with :meth:`collected` once every required input has a
    value.  The session is intentionally distinct from
    :class:`TranslationSession` because the inputs it gathers are
    free-text paths and identifiers rather than enum-backed choices.

    Attributes:
        phase: One of ``"discover"``, ``"convert"``, ``"package"``.
    """

    phase: str
    _answers: dict[str, str] = field(default_factory=dict)

    def __post_init__(self) -> None:
        """Validates that *phase* is one of the supported migration phases.

        Raises:
            UnknownMigrationPhaseError: When *phase* is not registered in
                :data:`_OPTIONS_BY_PHASE`.
        """
        if self.phase not in _OPTIONS_BY_PHASE:
            raise UnknownMigrationPhaseError(
                f"Unknown migration phase {self.phase!r}; expected one of {sorted(_OPTIONS_BY_PHASE)}"
            )

    def pending(self) -> PendingMigrationInputs:
        """Returns the input options still awaiting an answer.

        Returns:
            A :class:`PendingMigrationInputs` with the unanswered
            options for ``self.phase`` in registration order.
        """
        options = [option for option in _OPTIONS_BY_PHASE[self.phase] if option.option_id not in self._answers]
        return PendingMigrationInputs(phase=self.phase, options=options)

    def answer(self, option_id: str, value: str) -> None:
        """Records an answer to one input option.

        Args:
            option_id: Stable identifier of the option.
            value: Caller-supplied string value.

        Raises:
            ValueError: When *option_id* is not a known input for the
                session's phase.
        """
        if not any(option.option_id == option_id for option in _OPTIONS_BY_PHASE[self.phase]):
            raise ValueError(f"Unknown input option {option_id!r} for phase {self.phase!r}")
        self._answers[option_id] = value

    def answer_many(self, answers: dict[str, str]) -> None:
        """Records multiple input answers atomically.

        Args:
            answers: Mapping of option_id to the caller-supplied value.

        Raises:
            ValueError: When any pair references an unknown option.
                No answers are recorded when the call raises.
        """
        known_ids = {option.option_id for option in _OPTIONS_BY_PHASE[self.phase]}
        unknown = set(answers) - known_ids
        if unknown:
            raise ValueError(f"Unknown input options for phase {self.phase!r}: {sorted(unknown)}")
        self._answers.update(answers)

    def collected(self) -> dict[str, str]:
        """Returns the collected answers merged with each option's default.

        Returns:
            A dict keyed by option_id covering every option for the
            phase: caller-supplied answers take precedence; otherwise
            the option's ``default`` value (which may be the empty
            string) is used.  Required options whose answers are
            missing are omitted so the caller can detect them.
        """
        collected: dict[str, str] = {}
        for option in _OPTIONS_BY_PHASE[self.phase]:
            if option.option_id in self._answers:
                collected[option.option_id] = self._answers[option.option_id]
            elif option.default is not None:
                collected[option.option_id] = option.default
        return collected
