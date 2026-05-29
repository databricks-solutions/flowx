"""Agent adapter that drives the ask-validate-resume loop.

:class:`TranslationSession` is the entry point an agent uses to
translate tool-call arguments into validated preferences.  When the IR
raises questions the agent cannot answer from context alone, the
session surfaces them as structured :class:`TranslationQuestion`
objects (and, via :exc:`TranslationInputRequired`, as exceptions) so
the agent can route them back to the user.  The pipeline modifier is
invoked only once every question has an answer.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from orchestra.adapter.constants import (
    INPUT_ADF_RESOURCE_URL,
    INPUT_ADF_SOURCE_PATH,
    INPUT_BUNDLE_NAME,
    INPUT_CATALOG,
    INPUT_DATABRICKS_PROFILE,
    INPUT_INVENTORY_PATH,
    INPUT_OUTPUT_BUNDLE_PATH,
    INPUT_OUTPUT_DIR,
    INPUT_SCHEMA,
    INPUT_TRANSLATION_REPORT_PATH,
    PHASE_INGEST,
    PHASE_PREPARE,
    PHASE_TRANSLATE,
)
from orchestra.adapter.models import (
    DEFAULT_PREFERENCES,
    CopyActivityParadigm,
    DatabricksTaskCompute,
    LakeflowConnectorType,
    MetadataDrivenAccess,
    MetadataDrivenConsolidate,
    MetadataDrivenLookupTool,
    MetadataDrivenSize,
    MigrationInputQuestion,
    NonDatabricksTaskCompute,
    PendingMigrationInputs,
    PendingQuestions,
    TranslationPreferences,
    TranslationQuestion,
    UseLakeflowConnectors,
)
from orchestra.adapter.operations import (
    apply_preferences,
    gather_questions,
    validate_answer,
)
from orchestra.models.ir import Pipeline
from orchestra.models.motifs import DetectedMotif


class TranslationInputRequired(Exception):
    """Raised by :meth:`TranslationSession.run` when answers are still missing.

    Attributes:
        pending: The outstanding questions the agent should route to the
            user before retrying :meth:`TranslationSession.run`.
    """

    def __init__(self, pending: PendingQuestions) -> None:
        """Stores the pending questions on the exception.

        Args:
            pending: Outstanding questions surfaced by the session.
        """
        super().__init__(
            f"{len(pending.questions)} translation question(s) require user input "
            f"for pipeline {pending.pipeline_name!r}"
        )
        self.pending = pending


@dataclass(slots=True, kw_only=True)
class TranslationSession:
    """Coordinates the ask-validate-resume loop for one translated pipeline.

    A session is single-use: the caller drives it by either polling via
    :meth:`pending` and :meth:`answer`, or calling :meth:`run` and
    handling :exc:`TranslationInputRequired`.  When every question is
    answered, :meth:`run` (or :meth:`resume`) returns the
    preference-stamped pipeline.

    Attributes:
        pipeline: Translated pipeline IR after motif collapsing.
        motifs: Detected motifs for the pipeline.  Optional; only used to
            decide whether the Lakeflow Connect question applies.
        defaults: Baseline preferences applied when the caller skips a
            question.  Per-task overrides on this object are preserved
            verbatim when :meth:`build_preferences` composes the final
            snapshot.
    """

    pipeline: Pipeline
    motifs: list[DetectedMotif] = field(default_factory=list)
    defaults: TranslationPreferences = DEFAULT_PREFERENCES
    _answers: dict[str, str] = field(default_factory=dict)

    def pending(self) -> PendingQuestions:
        """Returns the questions still awaiting an answer.

        Returns:
            A :class:`PendingQuestions` instance containing only the
            questions whose preconditions are met by the IR and whose
            IDs are not yet in the answer set.
        """
        return gather_questions(
            self.pipeline,
            self.motifs,
            answers=self._answers,
        )

    def answer(self, question_id: str, value: str) -> None:
        """Validates and records a single answer.

        Args:
            question_id: Stable question identifier from
                :class:`TranslationQuestion`.
            value: Caller-supplied answer string.

        Raises:
            ValueError: When *question_id* is unknown or *value* is not
                in the allowed set for the question.
        """
        self._answers[question_id] = validate_answer(question_id, value)

    def answer_many(self, answers: dict[str, str]) -> None:
        """Validates and records multiple answers atomically.

        Args:
            answers: Mapping of question_id to the caller-supplied answer.

        Raises:
            ValueError: When any pair fails validation.  No answers from
                the batch are recorded when the call raises.
        """
        validated = {qid: validate_answer(qid, value) for qid, value in answers.items()}
        self._answers.update(validated)

    def find_question(self, question_id: str) -> TranslationQuestion | None:
        """Looks up a pending question by its identifier.

        Args:
            question_id: Stable question identifier.

        Returns:
            The matching :class:`TranslationQuestion` if it is still
            pending, otherwise ``None``.
        """
        return next(
            (question for question in self.pending().questions if question.question_id == question_id),
            None,
        )

    def build_preferences(self) -> TranslationPreferences:
        """Composes the validated preferences snapshot from collected answers.

        Returns:
            A :class:`TranslationPreferences` where every answered field
            takes the caller-supplied value and every unanswered field
            falls back to the corresponding value on ``defaults``.
        """
        return TranslationPreferences(
            copy_activity_paradigm=CopyActivityParadigm(
                self._answers.get("copy_activity_paradigm", self.defaults.copy_activity_paradigm)
            ),
            non_databricks_task_compute=NonDatabricksTaskCompute(
                self._answers.get("non_databricks_task_compute", self.defaults.non_databricks_task_compute)
            ),
            use_lakeflow_connectors=UseLakeflowConnectors(
                self._answers.get("use_lakeflow_connectors", self.defaults.use_lakeflow_connectors)
            ),
            databricks_task_compute=DatabricksTaskCompute(
                self._answers.get("databricks_task_compute", self.defaults.databricks_task_compute)
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
            per_task=self.defaults.per_task,
        )

    def resume(self) -> Pipeline:
        """Returns the preference-stamped pipeline IR.

        Returns:
            A new :class:`Pipeline` produced by applying the composed
            preferences to ``self.pipeline``.  The input pipeline is not
            mutated.
        """
        return apply_preferences(self.pipeline, self.build_preferences())

    def run(self) -> Pipeline:
        """Returns the modified pipeline, raising when input is still required.

        Returns:
            The preference-stamped pipeline IR when every applicable
            question has an answer.

        Raises:
            TranslationInputRequired: When one or more questions are
                still outstanding.  The exception carries the pending
                questions so the agent can route them to the user.
        """
        pending = self.pending()
        if pending.questions:
            raise TranslationInputRequired(pending)
        return self.resume()


_INGEST_QUESTIONS: tuple[MigrationInputQuestion, ...] = (
    MigrationInputQuestion(
        question_id=INPUT_ADF_SOURCE_PATH,
        prompt="Where are the ADF JSON exports?",
        description=(
            "Unity Catalog volume path (``/Volumes/<catalog>/<schema>/<volume>``) "
            "or a local directory containing the ADF ARM/JSON export."
        ),
        required=True,
    ),
    MigrationInputQuestion(
        question_id=INPUT_ADF_RESOURCE_URL,
        prompt="ADF resource URL?",
        description=(
            "Azure portal URL of the source Data Factory.  Captured for "
            "traceability and surfaced in the generated bundle README; "
            "leave blank when the source is exported from a local copy."
        ),
        default="",
        required=False,
    ),
    MigrationInputQuestion(
        question_id=INPUT_OUTPUT_DIR,
        prompt="Where should orchestra write the ingest output?",
        description="Directory the ingest phase writes ``inventory.json`` and ``ast/`` into.",
        default="./orchestra_output/ingest",
        required=False,
    ),
)

_TRANSLATE_QUESTIONS: tuple[MigrationInputQuestion, ...] = (
    MigrationInputQuestion(
        question_id=INPUT_INVENTORY_PATH,
        prompt="Path to the inventory.json from the ingest phase?",
        description="Inventory produced by the ingest phase that the translator consumes.",
        default="./orchestra_output/ingest/inventory.json",
        required=False,
    ),
    MigrationInputQuestion(
        question_id=INPUT_ADF_SOURCE_PATH,
        prompt="Path to the ADF JSON exports?",
        description="Same source directory the ingest phase consumed; needed for cross-references.",
        required=True,
    ),
    MigrationInputQuestion(
        question_id=INPUT_OUTPUT_DIR,
        prompt="Where should orchestra write the translate output?",
        description="Directory the translate phase writes the report and IR into.",
        default="./orchestra_output/translate",
        required=False,
    ),
)

_PREPARE_QUESTIONS: tuple[MigrationInputQuestion, ...] = (
    MigrationInputQuestion(
        question_id=INPUT_TRANSLATION_REPORT_PATH,
        prompt="Path to the translation report?",
        description=(
            "Preference-stamped report from `python -m orchestra.adapter modify`, "
            "or the raw translate-phase report when no preferences were applied."
        ),
        default="./orchestra_output/translate/translation_report.stamped.json",
        required=False,
    ),
    MigrationInputQuestion(
        question_id=INPUT_OUTPUT_BUNDLE_PATH,
        prompt="Where should the generated DAB bundle be written?",
        description="Root directory for the emitted Databricks Declarative Automation Bundle.",
        default="./dab_output",
        required=False,
    ),
    MigrationInputQuestion(
        question_id=INPUT_CATALOG,
        prompt="Target Unity Catalog catalog?",
        description="Default ``catalog`` bundle variable used by emitted notebooks and pipelines.",
        default="main",
        required=False,
    ),
    MigrationInputQuestion(
        question_id=INPUT_SCHEMA,
        prompt="Target Unity Catalog schema?",
        description="Default ``schema`` bundle variable used by emitted notebooks and pipelines.",
        default="default",
        required=False,
    ),
    MigrationInputQuestion(
        question_id=INPUT_BUNDLE_NAME,
        prompt="Bundle name override?",
        description="Defaults to the first translated pipeline's resource key when blank.",
        default="",
        required=False,
    ),
    MigrationInputQuestion(
        question_id=INPUT_DATABRICKS_PROFILE,
        prompt="Databricks CLI profile?",
        description=(
            "Profile used to download workspace-resident notebooks during the "
            "prepare phase.  Leave blank to use the default profile from "
            "``~/.databrickscfg`` or the active ``DATABRICKS_*`` env vars."
        ),
        default="",
        required=False,
    ),
)

_QUESTIONS_BY_PHASE: dict[str, tuple[MigrationInputQuestion, ...]] = {
    PHASE_INGEST: _INGEST_QUESTIONS,
    PHASE_TRANSLATE: _TRANSLATE_QUESTIONS,
    PHASE_PREPARE: _PREPARE_QUESTIONS,
}


class UnknownMigrationPhaseError(ValueError):
    """Raised when a MigrationInputSession is constructed with an unrecognised phase."""


@dataclass(slots=True, kw_only=True)
class MigrationInputSession:
    """Coordinates the free-text input prompts at the top of an orchestra phase.

    A session is single-use: the caller drives it by polling
    :meth:`pending` and recording answers via :meth:`answer`, then reads
    them out with :meth:`collected` once every required input has a
    value.  The session is intentionally distinct from
    :class:`TranslationSession` because the inputs it gathers are
    free-text paths and identifiers rather than enum-backed choices.

    Attributes:
        phase: One of ``"ingest"``, ``"translate"``, ``"prepare"``.
    """

    phase: str
    _answers: dict[str, str] = field(default_factory=dict)

    def __post_init__(self) -> None:
        """Validates that *phase* is one of the supported migration phases.

        Raises:
            UnknownMigrationPhaseError: When *phase* is not registered in
                :data:`_QUESTIONS_BY_PHASE`.
        """
        if self.phase not in _QUESTIONS_BY_PHASE:
            raise UnknownMigrationPhaseError(
                f"Unknown migration phase {self.phase!r}; expected one of {sorted(_QUESTIONS_BY_PHASE)}"
            )

    def pending(self) -> PendingMigrationInputs:
        """Returns the input questions still awaiting an answer.

        Returns:
            A :class:`PendingMigrationInputs` with the unanswered
            questions for ``self.phase`` in registration order.
        """
        questions = [
            question for question in _QUESTIONS_BY_PHASE[self.phase] if question.question_id not in self._answers
        ]
        return PendingMigrationInputs(phase=self.phase, questions=questions)

    def answer(self, question_id: str, value: str) -> None:
        """Records an answer to one input question.

        Args:
            question_id: Stable identifier of the question.
            value: Caller-supplied string value.

        Raises:
            ValueError: When *question_id* is not a known input for the
                session's phase.
        """
        if not any(question.question_id == question_id for question in _QUESTIONS_BY_PHASE[self.phase]):
            raise ValueError(f"Unknown input question {question_id!r} for phase {self.phase!r}")
        self._answers[question_id] = value

    def answer_many(self, answers: dict[str, str]) -> None:
        """Records multiple input answers atomically.

        Args:
            answers: Mapping of question_id to the caller-supplied value.

        Raises:
            ValueError: When any pair references an unknown question.
                No answers are recorded when the call raises.
        """
        known_ids = {question.question_id for question in _QUESTIONS_BY_PHASE[self.phase]}
        unknown = set(answers) - known_ids
        if unknown:
            raise ValueError(f"Unknown input questions for phase {self.phase!r}: {sorted(unknown)}")
        self._answers.update(answers)

    def collected(self) -> dict[str, str]:
        """Returns the collected answers merged with each question's default.

        Returns:
            A dict keyed by question_id covering every question for the
            phase: caller-supplied answers take precedence; otherwise
            the question's ``default`` value (which may be the empty
            string) is used.  Required questions whose answers are
            missing are omitted so the caller can detect them.
        """
        collected: dict[str, str] = {}
        for question in _QUESTIONS_BY_PHASE[self.phase]:
            if question.question_id in self._answers:
                collected[question.question_id] = self._answers[question.question_id]
            elif question.default is not None:
                collected[question.question_id] = question.default
        return collected
