"""Standalone operations: question gathering, validation, and IR modification.

The agent adapter and the CLI bridge call into these functions; nothing
here is stateful.  Preference dataclasses, StrEnums, and question shapes
live in :mod:`orchestra.adapter.models`.
"""

from __future__ import annotations

import dataclasses
import json
from enum import StrEnum
from pathlib import Path
from typing import Any

from orchestra.adapter.constants import (
    COMPUTE_MODE_CLASSIC_MULTI_NODE,
    COMPUTE_MODE_CLASSIC_SINGLE_NODE,
    COMPUTE_MODE_INHERIT,
    COMPUTE_MODE_SERVERLESS,
    DATABASE_SOURCE_TYPE_HINT,
    LAKEFLOW_CONNECT_MOTIF_REPLACEMENTS,
    LAKEFLOW_CONNECT_REPLACEMENT,
    LAKEFLOW_CONNECTOR_TYPE_QUERY_BASED,
    METADATA_DRIVEN_MOTIF_ID,
    MOTIF_CONSOLIDATE_QUESTION_PREFIX,
    QUESTION_COPY_ACTIVITY_PARADIGM,
    QUESTION_METADATA_DRIVEN_ACCESS,
    QUESTION_METADATA_DRIVEN_CONSOLIDATE,
    QUESTION_METADATA_DRIVEN_LOOKUP_TOOL,
    QUESTION_METADATA_DRIVEN_SIZE,
    QUESTION_NON_DATABRICKS_TASK_COMPUTE,
    QUESTION_USE_LAKEFLOW_CONNECTORS,
)
from orchestra.adapter.models import (
    FIELD_TO_ENUM,
    CopyActivityParadigm,
    LakeflowConnectorType,
    MetadataDrivenAccess,
    MetadataDrivenConsolidate,
    MetadataDrivenLookupTool,
    MetadataDrivenSize,
    MotifConsolidate,
    NonDatabricksTaskCompute,
    PendingQuestions,
    QuestionOption,
    TranslationPreferences,
    TranslationQuestion,
    UseLakeflowConnectors,
)
from orchestra.adapter.predicates import (
    copy_eligible_for_any_lfc_connector,
    copy_eligible_for_lfc_query_based,
    copy_query_unfit_for_lfc,
    copy_targets_delta,
    is_non_databricks_task,
    walk_activities,
)
from orchestra.models.ir import (
    Activity,
    CopyActivity,
    ForEachActivity,
    IfConditionActivity,
    MotifActivity,
    Pipeline,
    SwitchActivity,
    SwitchCase,
)
from orchestra.models.motifs import MOTIF_LAKEFLOW_CONNECT_DATABASE


def enum_for(question_id: str) -> type[StrEnum] | None:
    """Returns the enum class backing a preference field.

    Args:
        question_id: Field name (e.g. ``"copy_activity_paradigm"``) or
            per-motif id (e.g. ``"consolidate_motif:rest_api_pagination"``).

    Returns:
        The :class:`StrEnum` subclass that defines the allowed values, or
        ``None`` when the question_id is unknown.
    """
    if question_id.startswith(MOTIF_CONSOLIDATE_QUESTION_PREFIX):
        return MotifConsolidate
    return FIELD_TO_ENUM.get(question_id)


def allowed_values_for(question_id: str) -> tuple[str, ...]:
    """Returns the allowed string values for a preference field.

    Args:
        question_id: Field name (e.g. ``"copy_activity_paradigm"``).

    Returns:
        Tuple of allowed string values in declaration order.  Empty when
        the field is unknown.
    """
    enum_cls = enum_for(question_id)
    return tuple(member.value for member in enum_cls) if enum_cls else ()


def validate_answer(question_id: str, value: str) -> str:
    """Returns *value* when it is an allowed answer for *question_id*.

    Args:
        question_id: Stable question identifier.
        value: Caller-supplied answer string.

    Returns:
        The validated value, unchanged.

    Raises:
        ValueError: When *question_id* is not known or *value* is not in
            the allowed set for the question.
    """
    allowed = allowed_values_for(question_id)
    if not allowed:
        raise ValueError(f"Unknown question_id {question_id!r}")
    if value not in allowed:
        raise ValueError(f"Invalid answer {value!r} for {question_id!r}; allowed: {sorted(allowed)}")
    return value


def collect_workspace_artifact_paths(report_path: Path) -> list[str]:
    """Returns absolute workspace and DBFS paths referenced by a translation report.

    Args:
        report_path: Path to a translation report (single pipeline IR or
            an aggregated translation report).

    Returns:
        List of paths the bundle would need to download to be
        self-contained: notebook paths starting with ``/``, SparkPython
        files under DBFS or absolute workspace paths, and SparkJar
        library JARs under DBFS or absolute workspace paths.  Returns
        an empty list when the report cannot be read or contains no
        such paths.
    """
    try:
        with open(report_path, encoding="utf-8") as report_file:
            report = json.load(report_file)
    except (OSError, json.JSONDecodeError):
        return []
    candidates: list[str] = []
    if "tasks" in report:
        _walk_workspace_paths(report.get("tasks"), candidates)
    for translation in report.get("translations") or []:
        ir = translation.get("ir") or {}
        _walk_workspace_paths(ir.get("tasks"), candidates)
    return candidates


def detect_databricks_hosts(source_dir: Path) -> list[str]:
    """Returns unique Databricks workspace hosts referenced by an ADF export.

    Args:
        source_dir: Root directory of the ADF JSON export.  Expected to
            contain a ``linked_services/`` subdirectory.

    Returns:
        Sorted, unique list of workspace hosts pulled from every
        ``AzureDatabricks``-typed linked service whose ``domain`` field
        is populated.  Returns an empty list when no such linked
        services are present or the directory does not exist.
    """
    linked_services_dir = source_dir / "linked_services"
    if not linked_services_dir.exists():
        return []
    hosts: set[str] = set()
    for path in sorted(linked_services_dir.glob("*.json")):
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
        properties = data.get("properties") if isinstance(data.get("properties"), dict) else data
        if not isinstance(properties, dict):
            continue
        if properties.get("type") not in {"AzureDatabricks", "Databricks"}:
            continue
        domain = properties.get("domain") or properties.get("workspaceUrl")
        if isinstance(domain, str) and domain.strip():
            hosts.add(domain.strip().rstrip("/"))
    return sorted(hosts)


def _walk_workspace_paths(tasks: list[dict[str, Any]] | None, candidates: list[str]) -> None:
    """Appends workspace-resident artifact paths from *tasks* into *candidates*.

    Args:
        tasks: List of task dicts (top-level or nested in control flow).
        candidates: List that the caller mutates with discovered paths.
    """
    for task in tasks or []:
        task_type = task.get("type")
        if task_type == "NotebookActivity":
            path = task.get("notebook_path") or ""
            if isinstance(path, str) and path.startswith("/") and not path.startswith("../"):
                candidates.append(path)
        elif task_type == "SparkPythonActivity":
            path = task.get("python_file") or ""
            if isinstance(path, str) and (path.startswith("dbfs:") or path.startswith("/")):
                candidates.append(path)
        elif task_type == "SparkJarActivity":
            for lib in task.get("libraries") or []:
                jar = lib.get("jar") if isinstance(lib, dict) else None
                if isinstance(jar, str) and (jar.startswith("dbfs:") or jar.startswith("/")):
                    candidates.append(jar)
        _walk_workspace_paths(task.get("inner_activities"), candidates)
        _walk_workspace_paths(task.get("if_true_activities"), candidates)
        _walk_workspace_paths(task.get("if_false_activities"), candidates)
        for case in task.get("cases") or []:
            _walk_workspace_paths(case.get("activities"), candidates)
        _walk_workspace_paths(task.get("default_activities"), candidates)


def gather_questions(
    pipeline: Pipeline,
    motifs: list | None = None,
    *,
    answers: dict[str, str] | None = None,
) -> PendingQuestions:
    """Walks the IR and returns the questions that apply to *pipeline*.

    Args:
        pipeline: Translated pipeline IR after motif collapsing.
        motifs: Detected motifs, used to surface the Lakeflow Connect
            question for multi-step database ingestion patterns.
        answers: Answers the caller has already collected.  Questions
            whose ``question_id`` is in this mapping are filtered out,
            and questions whose ``conditions`` reference earlier answers
            are evaluated against this mapping.

    Returns:
        A :class:`PendingQuestions` instance carrying the questions whose
        IR preconditions and answer-dependent conditions are met but
        whose ``question_id`` has not yet been answered.
    """
    motif_list = motifs or []
    answer_map = answers or {}
    builders = (
        _build_use_lakeflow_connectors_question,
        _build_lakeflow_connector_type_question,
        _build_copy_activity_paradigm_question,
        _build_non_databricks_task_compute_question,
        _build_metadata_driven_consolidate_question,
        _build_metadata_driven_access_question,
        _build_metadata_driven_size_question,
        _build_metadata_driven_lookup_tool_question,
    )
    candidates = (builder(pipeline, motif_list, answers=answer_map) for builder in builders)
    pending = [
        question
        for question in candidates
        if question is not None
        and question.question_id not in answer_map
        and _conditions_met(question.conditions, answer_map)
    ]
    # Per-motif "consolidate?" questions: one per detected motif.  Each
    # gets its own question_id ``consolidate_motif:<motif_id>`` so the
    # adapter can solicit and validate them independently.  Default is
    # ``keep`` -- nothing is collapsed without an explicit yes.
    for motif_question in _build_motif_consolidation_questions(motif_list):
        if motif_question.question_id in answer_map:
            continue
        pending.append(motif_question)
    return PendingQuestions(pipeline_name=pipeline.name, questions=pending)


def _conditions_met(conditions: tuple[tuple[str, str], ...], answers: dict[str, str]) -> bool:
    """Returns True when every condition is satisfied by *answers*.

    Args:
        conditions: Tuples of ``(question_id, expected_value)`` from a
            :class:`TranslationQuestion`.
        answers: Mapping of question_id to the caller-supplied answer.

    Returns:
        ``True`` when every condition's question has been answered with
        the expected value (or when *conditions* is empty); ``False``
        otherwise.
    """
    return all(answers.get(qid) == expected for qid, expected in conditions)


def apply_preferences(pipeline: Pipeline, pipeline_preferences: TranslationPreferences) -> Pipeline:
    """Returns a copy of *pipeline* with preferences stamped onto each activity.

    Args:
        pipeline: Translated pipeline IR after motif collapsing.
        pipeline_preferences: Validated pipeline-wide preferences.

    Returns:
        A new :class:`Pipeline` whose activities carry concrete decisions
        about compute, target format, and Lakeflow Connect replacement.
        The input pipeline is not mutated.
    """
    stamped_tasks = [_stamp_activity(activity, pipeline_preferences) for activity in pipeline.tasks]
    return dataclasses.replace(
        pipeline,
        tasks=stamped_tasks,
        translation_preferences=pipeline_preferences,
    )


def _build_copy_activity_paradigm_question(
    pipeline: Pipeline, motifs: list, answers: dict[str, str] | None = None
) -> TranslationQuestion | None:
    """Builds the SDP-vs-notebook question for Copy activities targeting Delta.

    Args:
        pipeline: Translated pipeline IR.
        motifs: Detected motifs (unused; accepted for builder uniformity).
        answers: Answers already supplied for prior prompts.  When the
            user opted into Lakeflow Connect, this question only fires
            for Copy activities that are *not* LFC-eligible -- the
            paradigm choice is moot for Copies that will become
            managed LFC pipelines.

    Returns:
        The constructed :class:`TranslationQuestion`, or ``None`` when no
        Copy activity needs a paradigm choice.  Copies whose source
        query is unfit for both LFC and SDP (joins, aggregates, etc.)
        are forced to PySpark notebook and excluded from the affected
        set.
    """
    answers = answers or {}
    going_to_lfc = answers.get(QUESTION_USE_LAKEFLOW_CONNECTORS) == UseLakeflowConnectors.LAKEFLOW_CONNECT.value
    affected = tuple(
        activity.task_key
        for activity in walk_activities(pipeline.tasks)
        if isinstance(activity, CopyActivity)
        and copy_targets_delta(activity)
        and not _copy_paradigm_decided_by_lfc(activity, going_to_lfc)
    )
    if not affected:
        return None
    return TranslationQuestion(
        question_id=QUESTION_COPY_ACTIVITY_PARADIGM,
        prompt="How should Copy Data activities targeting Delta be implemented?",
        rationale=(
            "One or more Copy Data activities write to a Delta table.  "
            "Lakeflow Spark Declarative Pipelines define tables declaratively; "
            "a PySpark notebook stays closer to the original ADF activity shape."
        ),
        options=(
            QuestionOption(
                value=CopyActivityParadigm.NOTEBOOK.value,
                label="PySpark notebook",
                description="Generates a notebook task that reads the source and writes Delta directly.",
            ),
            QuestionOption(
                value=CopyActivityParadigm.SDP.value,
                label="Lakeflow Spark Declarative Pipeline",
                description="Emits an SDP pipeline resource with declarative table definitions.",
            ),
        ),
        affected_task_keys=affected,
        default=CopyActivityParadigm.NOTEBOOK.value,
    )


def _build_non_databricks_task_compute_question(
    pipeline: Pipeline, motifs: list, answers: dict[str, str] | None = None
) -> TranslationQuestion | None:
    """Builds the serverless-vs-classic question for non-Databricks tasks.

    Args:
        pipeline: Translated pipeline IR.
        motifs: Detected motifs (unused; accepted for builder uniformity).
        answers: Answers already supplied for prior prompts.  Copies
            that will become managed LFC pipelines are excluded
            because LFC pipelines always use serverless compute.

    Returns:
        The constructed :class:`TranslationQuestion`, or ``None`` when
        every non-Databricks task in the pipeline is going to LFC (no
        compute choice to make).
    """
    answers = answers or {}
    going_to_lfc = answers.get(QUESTION_USE_LAKEFLOW_CONNECTORS) == UseLakeflowConnectors.LAKEFLOW_CONNECT.value
    affected = tuple(
        activity.task_key
        for activity in walk_activities(pipeline.tasks)
        if is_non_databricks_task(activity) and not _task_compute_decided_by_lfc(activity, going_to_lfc)
    )
    if not affected:
        return None
    return TranslationQuestion(
        question_id=QUESTION_NON_DATABRICKS_TASK_COMPUTE,
        prompt="What compute should the non-Databricks tasks use?",
        rationale=(
            "Tasks such as Copy Data, Web, Lookup, and Wait can run on serverless "
            "or classic compute.  Classic provisions a single-node cluster for most "
            "tasks and a larger fixed-size cluster for Copy Data."
        ),
        options=(
            QuestionOption(
                value=NonDatabricksTaskCompute.SERVERLESS.value,
                label="Serverless",
                description="Runs every non-Databricks task on serverless compute.",
            ),
            QuestionOption(
                value=NonDatabricksTaskCompute.CLASSIC.value,
                label="Classic job_cluster",
                description="Provisions classic job_clusters sized per task type.",
            ),
        ),
        affected_task_keys=affected,
        default=NonDatabricksTaskCompute.SERVERLESS.value,
    )


def _build_use_lakeflow_connectors_question(
    pipeline: Pipeline, motifs: list, answers: dict[str, str] | None = None
) -> TranslationQuestion | None:
    """Builds the Lakeflow Connect question for eligible database ingestions.

    Args:
        pipeline: Translated pipeline IR.
        motifs: Detected motifs, scanned for database-source ingestion patterns.

    Returns:
        The constructed :class:`TranslationQuestion`, or ``None`` when no
        Copy activity or motif qualifies for Lakeflow Connect.
    """
    affected = _affected_task_keys_for_lakeflow_connect(pipeline, motifs)
    if not affected:
        return None
    return TranslationQuestion(
        question_id=QUESTION_USE_LAKEFLOW_CONNECTORS,
        prompt="Migrate eligible SQL Server, MySQL, and PostgreSQL ingestions to Lakeflow Connect?",
        rationale=(
            "One or more Copy Data activities ingest from SQL Server, MySQL, or "
            "PostgreSQL into Delta.  Managed Lakeflow Connect replaces the bespoke "
            "ingestion with a declarative pipeline; the existing translation keeps "
            "the ADF-shaped activity intact."
        ),
        options=(
            QuestionOption(
                value=UseLakeflowConnectors.EXISTING.value,
                label="Keep existing translation",
                description="Preserves the Copy Data activity as a notebook or SDP task.",
            ),
            QuestionOption(
                value=UseLakeflowConnectors.LAKEFLOW_CONNECT.value,
                label="Use Lakeflow Connect",
                description="Replaces eligible ingestions with a managed Lakeflow Connect pipeline.",
            ),
        ),
        affected_task_keys=affected,
        default=UseLakeflowConnectors.EXISTING.value,
    )


def _build_lakeflow_connector_type_question(
    pipeline: Pipeline, motifs: list, answers: dict[str, str] | None = None
) -> TranslationQuestion | None:
    """Builds the CDC-vs-query connector question, suppressed when not actionable.

    Args:
        pipeline: Translated pipeline IR.
        motifs: Detected motifs (unused; accepted for builder uniformity).
        answers: Answers already supplied for prior prompts (unused).

    Returns:
        Currently always ``None``.  The per-Copy LFC eligibility rules
        ensure each Copy can only be routed through one connector type
        (table-based reads → CDC because the query-based connector
        requires a cursor column; queries with a cursor → query-based
        because CDC requires direct table access).  A pipeline-wide
        preference between CDC and query-based therefore has no
        actionable effect; the modifier picks the eligible connector
        per Copy.
    """
    del pipeline, motifs, answers
    return None


def _build_metadata_driven_consolidate_question(
    pipeline: Pipeline,
    motifs: list,
    answers: dict[str, str] | None = None,
) -> TranslationQuestion | None:
    """Builds the consolidate-or-keep question for metadata-driven motifs.

    Args:
        pipeline: Translated pipeline IR.
        motifs: Detected motifs; the question only surfaces when at
            least one matches the metadata-driven bulk copy pattern.

    Returns:
        The constructed :class:`TranslationQuestion`, or ``None`` when
        the pipeline contains no metadata-driven motif.
    """
    affected = _metadata_driven_motif_task_keys(pipeline, motifs)
    if not affected:
        return None
    return TranslationQuestion(
        question_id=QUESTION_METADATA_DRIVEN_CONSOLIDATE,
        prompt="Consolidate the metadata-driven ingestions into one managed pipeline?",
        rationale=(
            "A Lookup feeds a ForEach that copies each row's table.  Consolidating "
            "replaces this loop with a single Lakeflow Connect or Lakeflow Spark "
            "Declarative Pipeline whose objects list materialises each source as "
            "its own streaming table.  Keeping the loop preserves the existing "
            "per-row Copy translation."
        ),
        options=(
            QuestionOption(
                value=MetadataDrivenConsolidate.KEEP.value,
                label="Keep the per-row loop",
                description="Preserves the ForEach + Copy translation as a motif scaffold.",
            ),
            QuestionOption(
                value=MetadataDrivenConsolidate.CONSOLIDATE.value,
                label="Consolidate into one pipeline",
                description="Emits one pipeline resource that ingests every source from the lookup.",
            ),
        ),
        affected_task_keys=affected,
        default=MetadataDrivenConsolidate.KEEP.value,
    )


def _build_metadata_driven_access_question(
    pipeline: Pipeline,
    motifs: list,
    answers: dict[str, str] | None = None,
) -> TranslationQuestion | None:
    """Builds the metadata-source access question, gated on consolidate=yes.

    Args:
        pipeline: Translated pipeline IR.
        motifs: Detected motifs.

    Returns:
        The constructed :class:`TranslationQuestion`, or ``None`` when no
        metadata-driven motif applies.
    """
    affected = _metadata_driven_motif_task_keys(pipeline, motifs)
    if not affected:
        return None
    return TranslationQuestion(
        question_id=QUESTION_METADATA_DRIVEN_ACCESS,
        prompt="Do you have access to query the metadata source and approve doing so?",
        rationale=(
            "Consolidating a metadata-driven ingestion requires materialising the "
            "lookup query at translation time so each row becomes a pipeline object.  "
            "Answering yes confirms the metadata source can be queried during this "
            "translation pass; answering no falls back to the per-row scaffold."
        ),
        options=(
            QuestionOption(
                value=MetadataDrivenAccess.YES.value,
                label="Yes, query is allowed",
                description="The metadata source is reachable and approved for read during translation.",
            ),
            QuestionOption(
                value=MetadataDrivenAccess.NO.value,
                label="No, skip materialising the lookup",
                description="Keeps the per-row motif scaffold without inlining the configuration.",
            ),
        ),
        affected_task_keys=affected,
        default=MetadataDrivenAccess.NO.value,
        conditions=((QUESTION_METADATA_DRIVEN_CONSOLIDATE, MetadataDrivenConsolidate.CONSOLIDATE.value),),
    )


def _build_metadata_driven_size_question(
    pipeline: Pipeline,
    motifs: list,
    answers: dict[str, str] | None = None,
) -> TranslationQuestion | None:
    """Builds the t-shirt sizing question, gated on consolidate=yes.

    Args:
        pipeline: Translated pipeline IR.
        motifs: Detected motifs.

    Returns:
        The constructed :class:`TranslationQuestion`, or ``None`` when no
        metadata-driven motif applies.
    """
    affected = _metadata_driven_motif_task_keys(pipeline, motifs)
    if not affected:
        return None
    return TranslationQuestion(
        question_id=QUESTION_METADATA_DRIVEN_SIZE,
        prompt="Roughly how many configuration rows feed the metadata-driven ingestion?",
        rationale=(
            "The size determines whether the modifier inlines every lookup row into "
            "one consolidated pipeline.  Small and medium-sized configurations are "
            "expanded inline; large configurations keep the per-row scaffold to "
            "avoid generating an unwieldy pipeline definition."
        ),
        options=(
            QuestionOption(
                value=MetadataDrivenSize.SMALL.value,
                label="S (under 50 rows)",
                description="Lookup feeds fewer than 50 ingestion targets.",
            ),
            QuestionOption(
                value=MetadataDrivenSize.MEDIUM.value,
                label="M (under 250 rows)",
                description="Lookup feeds 50 to 249 ingestion targets.",
            ),
            QuestionOption(
                value=MetadataDrivenSize.LARGE.value,
                label="L (250 or more rows)",
                description="Lookup feeds 250+ targets; skip inline consolidation.",
            ),
        ),
        affected_task_keys=affected,
        default=MetadataDrivenSize.LARGE.value,
        conditions=((QUESTION_METADATA_DRIVEN_CONSOLIDATE, MetadataDrivenConsolidate.CONSOLIDATE.value),),
    )


def _build_metadata_driven_lookup_tool_question(
    pipeline: Pipeline,
    motifs: list,
    answers: dict[str, str] | None = None,
) -> TranslationQuestion | None:
    """Builds the agent-tool question for the lookup query, gated on size != L.

    Args:
        pipeline: Translated pipeline IR.
        motifs: Detected motifs.

    Returns:
        The constructed :class:`TranslationQuestion`, or ``None`` when no
        metadata-driven motif applies.
    """
    affected = _metadata_driven_motif_task_keys(pipeline, motifs)
    if not affected:
        return None
    return TranslationQuestion(
        question_id=QUESTION_METADATA_DRIVEN_LOOKUP_TOOL,
        prompt="Does the agent have a tool that can run the lookup query?",
        rationale=(
            "When the agent has a Genie skill, an MCP database tool, or a SQL "
            "warehouse it can call, the modifier asks the agent to execute the "
            "lookup query directly and reuses the rows.  When no tool is "
            "available the agent prompts the user for a CSV file or "
            "comma-separated string of values and the modifier ingests that."
        ),
        options=(
            QuestionOption(
                value=MetadataDrivenLookupTool.HAVE.value,
                label="Yes, the agent can run the lookup",
                description="Agent executes the lookup query via its own tool.",
            ),
            QuestionOption(
                value=MetadataDrivenLookupTool.NONE.value,
                label="No, ask the user for the values",
                description="Agent prompts the user for a CSV file or string of values.",
            ),
        ),
        affected_task_keys=affected,
        default=MetadataDrivenLookupTool.NONE.value,
        conditions=(
            (QUESTION_METADATA_DRIVEN_CONSOLIDATE, MetadataDrivenConsolidate.CONSOLIDATE.value),
            (QUESTION_METADATA_DRIVEN_ACCESS, MetadataDrivenAccess.YES.value),
        ),
    )


def _build_motif_consolidation_questions(motifs: list) -> list[TranslationQuestion]:
    """Builds one ``consolidate_motif:<id>`` question per detected motif.

    Args:
        motifs: Detected :class:`~orchestra.models.motifs.DetectedMotif`
            instances from :func:`orchestra.motifs.detector.detect_motifs`.

    Returns:
        A list of :class:`TranslationQuestion` instances, one per
        detected motif.  Each question uses a unique question_id of the
        form ``consolidate_motif:<motif_id>`` so multiple distinct motif
        types in the same pipeline (e.g. ``rest_api_pagination`` *and*
        ``metadata_driven_bulk_copy``) each get their own prompt.
        Returns an empty list when no motifs were detected.

    Notes:
        - The default for every motif is ``keep``.  Motif detection is
          a heuristic match and over-collapsing silently rewrites
          pipelines; requiring an explicit ``consolidate`` answer is
          the safer default.
        - When the same motif type is detected more than once in the
          same pipeline (rare in practice but possible) the builder
          emits a single question covering all instances of that type.
          Per-instance overrides can still be expressed by adding more
          fine-grained gating in :class:`MotifActivity`.
        - The ``affected_task_keys`` field lists the *underlying* ADF
          activity names so the agent can quote them when asking the
          user, e.g. ``"Consolidate REST API Pagination motif spanning
          GetToken, InitCursor, PollLoop into a single notebook?"``.
    """
    if not motifs:
        return []
    seen: set[str] = set()
    questions: list[TranslationQuestion] = []
    for motif in motifs:
        definition = motif.definition
        motif_id = definition.motif_id
        if motif_id in seen:
            continue
        seen.add(motif_id)
        affected = tuple(motif.matched_activities)
        question_id = f"{MOTIF_CONSOLIDATE_QUESTION_PREFIX}{motif_id}"
        confidence_suffix = ""
        if motif.confidence_notes:
            confidence_suffix = "  Detector notes: " + " | ".join(motif.confidence_notes)
        questions.append(
            TranslationQuestion(
                question_id=question_id,
                prompt=f"Consolidate the {definition.display_name!r} motif into a single task?",
                rationale=(
                    f"{definition.description} "
                    f"Affected activities: {', '.join(affected) if affected else '(none)'}.{confidence_suffix} "
                    "Keep preserves the activity-by-activity translation; consolidate replaces "
                    f"them with a single {definition.databricks_replacement!r} task."
                ),
                options=(
                    QuestionOption(
                        value=MotifConsolidate.KEEP.value,
                        label="Keep individual activities",
                        description="Preserves the per-activity translation; no motif collapse.",
                    ),
                    QuestionOption(
                        value=MotifConsolidate.CONSOLIDATE.value,
                        label="Consolidate into one task",
                        description=f"Replaces matched activities with a {definition.databricks_replacement!r} task.",
                    ),
                ),
                affected_task_keys=affected,
                default=MotifConsolidate.KEEP.value,
            )
        )
    return questions


def _metadata_driven_motif_task_keys(
    pipeline: Pipeline, motifs: list, answers: dict[str, str] | None = None
) -> tuple[str, ...]:
    """Returns the task keys of metadata-driven bulk-copy motifs in *pipeline*.

    Args:
        pipeline: Translated pipeline IR after motif collapsing.
        motifs: Detected motifs for the pipeline.

    Returns:
        Tuple of motif activity task keys whose source motif is the
        metadata-driven bulk-copy pattern.  Empty when no such motif is
        present.
    """
    del motifs
    return tuple(
        activity.task_key
        for activity in pipeline.tasks
        if isinstance(activity, MotifActivity) and activity.motif_id == METADATA_DRIVEN_MOTIF_ID
    )


def _affected_task_keys_for_lakeflow_connect(
    pipeline: Pipeline, motifs: list, answers: dict[str, str] | None = None
) -> tuple[str, ...]:
    """Returns the task keys eligible for Lakeflow Connect replacement.

    Args:
        pipeline: Translated pipeline IR after motif collapsing.
        motifs: Detected motifs for the pipeline.

    Returns:
        Unique tuple combining standalone Copy activities with database
        sources targeting Delta and motif activities representing
        database ingestion patterns.
    """
    copy_keys = [
        activity.task_key
        for activity in walk_activities(pipeline.tasks)
        if isinstance(activity, CopyActivity) and copy_eligible_for_any_lfc_connector(activity)
    ]
    motif_keys = _motif_task_keys_for_lakeflow_connect(pipeline, motifs)
    return tuple(dict.fromkeys(copy_keys + motif_keys))


def _copy_paradigm_decided_by_lfc(activity: CopyActivity, going_to_lfc: bool) -> bool:
    """Reports whether a Copy's paradigm is already decided without a prompt.

    Args:
        activity: Copy activity to inspect.
        going_to_lfc: ``True`` when the caller answered the LFC question
            with ``lakeflow_connect``.

    Returns:
        ``True`` when the Copy is going to LFC (paradigm = managed
        ingestion pipeline), or when its source query is unfit for both
        LFC and SDP (paradigm forced to PySpark notebook).  ``False``
        when the user still needs to pick between SDP and notebook.
    """
    if going_to_lfc and copy_eligible_for_any_lfc_connector(activity):
        return True
    return copy_query_unfit_for_lfc(activity)


def _task_compute_decided_by_lfc(activity, going_to_lfc: bool) -> bool:
    """Reports whether a task's compute mode is already decided by an LFC routing.

    Args:
        activity: Activity to inspect.
        going_to_lfc: ``True`` when the caller answered the LFC question
            with ``lakeflow_connect``.

    Returns:
        ``True`` for Copy activities that will become LFC pipelines
        (LFC manages its own serverless compute).  ``False`` for every
        other non-Databricks task -- those still need a compute choice.
    """
    if not going_to_lfc:
        return False
    return isinstance(activity, CopyActivity) and copy_eligible_for_any_lfc_connector(activity)


def _motif_task_keys_for_lakeflow_connect(
    pipeline: Pipeline, motifs: list, answers: dict[str, str] | None = None
) -> list[str]:
    """Returns motif task keys eligible for Lakeflow Connect replacement.

    When the caller supplies the original :class:`DetectedMotif` list
    (in-process translator usage) the eligibility check uses the motif
    definition.  When the list is empty (CLI usage that only sees the
    serialised IR), eligibility is derived directly from each
    :class:`MotifActivity`'s ``source_type_hint`` and
    ``databricks_replacement`` fields.

    Args:
        pipeline: Translated pipeline IR after motif collapsing.
        motifs: Detected motifs for the pipeline.  May be empty.

    Returns:
        Task keys of motif activities whose source hint is ``database``
        and whose Databricks replacement is a known ingestion pattern
        that Lakeflow Connect can take over.
    """
    motif_tasks_by_id = {
        activity.motif_id: activity for activity in pipeline.tasks if isinstance(activity, MotifActivity)
    }
    if motifs:
        return [
            motif_tasks_by_id[detected.definition.motif_id].task_key
            for detected in motifs
            if detected.source_type_hint == DATABASE_SOURCE_TYPE_HINT
            and detected.definition.databricks_replacement in LAKEFLOW_CONNECT_MOTIF_REPLACEMENTS
            and detected.definition.motif_id in motif_tasks_by_id
        ]
    return [
        activity.task_key
        for activity in motif_tasks_by_id.values()
        if activity.source_type_hint == DATABASE_SOURCE_TYPE_HINT
        and activity.databricks_replacement in LAKEFLOW_CONNECT_MOTIF_REPLACEMENTS
    ]


def _stamp_activity(activity: Activity, pipeline_preferences: TranslationPreferences) -> Activity:
    """Stamps preference-derived decisions onto an activity.

    Args:
        activity: Source activity from the IR.
        pipeline_preferences: Pipeline-wide preferences; per-task overrides
            apply via :meth:`TranslationPreferences.effective_for`.

    Returns:
        A new activity instance with ``compute_mode``, ``target_format``,
        and motif-replacement updates applied as appropriate.  Control-
        flow activities are recursed into so their inner bodies are
        stamped too.
    """
    activity_preferences = pipeline_preferences.effective_for(activity.task_key)
    if isinstance(activity, ForEachActivity):
        return _stamp_for_each_activity(activity, pipeline_preferences, activity_preferences)
    if isinstance(activity, IfConditionActivity):
        return _stamp_if_condition_activity(activity, pipeline_preferences, activity_preferences)
    if isinstance(activity, SwitchActivity):
        return _stamp_switch_activity(activity, pipeline_preferences, activity_preferences)
    if isinstance(activity, CopyActivity):
        return _stamp_copy_activity(activity, activity_preferences)
    if isinstance(activity, MotifActivity):
        return _stamp_motif_activity(activity, activity_preferences)
    return dataclasses.replace(activity, compute_mode=_resolve_compute_mode(activity, activity_preferences))


def _stamp_for_each_activity(
    activity: ForEachActivity,
    pipeline_preferences: TranslationPreferences,
    activity_preferences: TranslationPreferences,
) -> ForEachActivity:
    """Stamps a ForEach activity and recurses into its inner body.

    Args:
        activity: Source ForEach activity.
        pipeline_preferences: Pipeline-wide preferences threaded into
            inner activities so they re-resolve their own overrides.
        activity_preferences: Preferences after per-task overrides for
            *activity*.

    Returns:
        A new :class:`ForEachActivity` with inner activities stamped.
    """
    return dataclasses.replace(
        activity,
        inner_activities=[_stamp_activity(inner, pipeline_preferences) for inner in activity.inner_activities],
        compute_mode=_resolve_compute_mode(activity, activity_preferences),
    )


def _stamp_if_condition_activity(
    activity: IfConditionActivity,
    pipeline_preferences: TranslationPreferences,
    activity_preferences: TranslationPreferences,
) -> IfConditionActivity:
    """Stamps an IfCondition activity and recurses into both branches.

    Args:
        activity: Source IfCondition activity.
        pipeline_preferences: Pipeline-wide preferences threaded into
            inner activities so they re-resolve their own overrides.
        activity_preferences: Preferences after per-task overrides for
            *activity*.

    Returns:
        A new :class:`IfConditionActivity` with both branches stamped.
    """
    return dataclasses.replace(
        activity,
        if_true_activities=[_stamp_activity(inner, pipeline_preferences) for inner in activity.if_true_activities],
        if_false_activities=[_stamp_activity(inner, pipeline_preferences) for inner in activity.if_false_activities],
        compute_mode=_resolve_compute_mode(activity, activity_preferences),
    )


def _stamp_switch_activity(
    activity: SwitchActivity,
    pipeline_preferences: TranslationPreferences,
    activity_preferences: TranslationPreferences,
) -> SwitchActivity:
    """Stamps a Switch activity and recurses into every case and the default.

    Args:
        activity: Source Switch activity.
        pipeline_preferences: Pipeline-wide preferences threaded into
            inner activities so they re-resolve their own overrides.
        activity_preferences: Preferences after per-task overrides for
            *activity*.

    Returns:
        A new :class:`SwitchActivity` with every case body stamped.
    """
    stamped_cases = [
        SwitchCase(
            value=case.value,
            activities=[_stamp_activity(inner, pipeline_preferences) for inner in case.activities],
        )
        for case in activity.cases
    ]
    return dataclasses.replace(
        activity,
        cases=stamped_cases,
        default_activities=[_stamp_activity(inner, pipeline_preferences) for inner in activity.default_activities],
        compute_mode=_resolve_compute_mode(activity, activity_preferences),
    )


def _stamp_copy_activity(
    activity: CopyActivity,
    activity_preferences: TranslationPreferences,
) -> CopyActivity:
    """Stamps a Copy activity with paradigm, compute, and Lakeflow Connect flags.

    Args:
        activity: Source Copy activity.
        activity_preferences: Effective preferences for this activity.

    Returns:
        A new :class:`CopyActivity` whose ``target_format``,
        ``compute_mode``, and ``use_lakeflow_connector`` fields reflect
        the user's choices.  Copies whose query is unfit for LFC and
        SDP (joins, aggregates, etc.) are forced to the notebook
        paradigm regardless of preference because the alternative
        paradigms cannot represent arbitrary SQL.
    """
    user_picked_lfc = activity_preferences.use_lakeflow_connectors is UseLakeflowConnectors.LAKEFLOW_CONNECT
    use_lakeflow_connector = user_picked_lfc and copy_eligible_for_any_lfc_connector(activity)
    paradigm = _resolve_paradigm(activity, activity_preferences, use_lakeflow_connector)
    connector_type = (
        _resolve_lakeflow_connector_type(activity, activity_preferences) if use_lakeflow_connector else None
    )
    return dataclasses.replace(
        activity,
        target_format=paradigm.value,
        compute_mode=_resolve_compute_mode(activity, activity_preferences),
        use_lakeflow_connector=use_lakeflow_connector,
        lakeflow_connector_type=connector_type,
    )


def _resolve_paradigm(
    activity: CopyActivity,
    activity_preferences: TranslationPreferences,
    use_lakeflow_connector: bool,
) -> CopyActivityParadigm:
    """Resolves the paradigm (notebook vs SDP) for a Copy that won't go to LFC.

    Args:
        activity: Source Copy activity.
        activity_preferences: Effective preferences for this activity.
        use_lakeflow_connector: ``True`` when the modifier already
            routed the Copy to a managed LFC pipeline; the paradigm is
            informational in that case.

    Returns:
        ``CopyActivityParadigm.NOTEBOOK`` when the activity's source
        query is unfit for SDP (joins, aggregates, etc.) so the
        notebook PySpark + JDBC path is the only viable alternative.
        Otherwise the user's preferred paradigm when the Copy targets
        Delta, falling back to ``NOTEBOOK`` for non-Delta sinks.
    """
    if copy_query_unfit_for_lfc(activity):
        return CopyActivityParadigm.NOTEBOOK
    if not copy_targets_delta(activity):
        return CopyActivityParadigm.NOTEBOOK
    return activity_preferences.copy_activity_paradigm


def _resolve_lakeflow_connector_type(activity: CopyActivity, activity_preferences: TranslationPreferences) -> str:
    """Resolves which Lakeflow Connect connector to use for an eligible Copy.

    Args:
        activity: Source Copy activity (already known to be LFC-eligible).
        activity_preferences: Effective preferences for this activity.

    Returns:
        Always the connector flavour the Copy is actually eligible for.
        Query-based eligibility (parseable query + cursor column) wins
        over the user's CDC preference because no cursor candidate
        exists for a CDC connector to use on a query-only Copy.
        Table-based Copies route to CDC because the query-based
        connector requires a cursor column and there is none.
    """
    if copy_eligible_for_lfc_query_based(activity):
        return LAKEFLOW_CONNECTOR_TYPE_QUERY_BASED
    return LakeflowConnectorType.CDC.value


def _stamp_motif_activity(
    activity: MotifActivity,
    activity_preferences: TranslationPreferences,
) -> MotifActivity:
    """Stamps a Motif activity, swapping in Lakeflow Connect when eligible.

    Args:
        activity: Source motif activity.
        activity_preferences: Effective preferences for this activity.

    Returns:
        A new :class:`MotifActivity` whose ``databricks_replacement`` is
        set to ``lakeflow_connect_database`` when the motif represents a
        database ingestion and the user opted into Lakeflow Connect.
        Metadata-driven motifs also pick up the
        ``consolidate_metadata_driven`` flag when the user approved
        consolidation, granted access, and the size bucket is S or M.
    """
    qualifies_for_lakeflow_connect = (
        activity_preferences.use_lakeflow_connectors is UseLakeflowConnectors.LAKEFLOW_CONNECT
        and activity.source_type_hint == DATABASE_SOURCE_TYPE_HINT
    )
    replacement = LAKEFLOW_CONNECT_REPLACEMENT if qualifies_for_lakeflow_connect else activity.databricks_replacement
    notebook_template = (
        MOTIF_LAKEFLOW_CONNECT_DATABASE.notebook_template
        if qualifies_for_lakeflow_connect
        else activity.notebook_template
    )
    consolidate = _should_consolidate_metadata_driven(activity, activity_preferences)
    return dataclasses.replace(
        activity,
        databricks_replacement=replacement,
        notebook_template=notebook_template,
        compute_mode=_resolve_compute_mode(activity, activity_preferences),
        consolidate_metadata_driven=consolidate,
    )


def _should_consolidate_metadata_driven(
    activity: MotifActivity,
    activity_preferences: TranslationPreferences,
) -> bool:
    """Returns True when the modifier should consolidate a metadata-driven motif.

    Args:
        activity: Source motif activity.
        activity_preferences: Effective preferences for this activity.

    Returns:
        ``True`` when the motif matches the metadata-driven bulk-copy
        pattern, the user opted to consolidate, granted access to the
        metadata source, and the configuration size is S or M.
        ``False`` otherwise -- including when the IR was not stamped by
        the metadata-driven prompts.
    """
    if activity.motif_id != "metadata_driven_bulk_copy":
        return False
    if activity_preferences.metadata_driven_consolidate is not MetadataDrivenConsolidate.CONSOLIDATE:
        return False
    if activity_preferences.metadata_driven_access is not MetadataDrivenAccess.YES:
        return False
    return activity_preferences.metadata_driven_size is not MetadataDrivenSize.LARGE


def _resolve_compute_mode(activity: Activity, activity_preferences: TranslationPreferences) -> str:
    """Resolves the compute mode an activity should run on.

    Args:
        activity: Source activity.
        activity_preferences: Effective preferences for this activity.

    Returns:
        One of :data:`COMPUTE_MODE_SERVERLESS`,
        :data:`COMPUTE_MODE_CLASSIC_SINGLE_NODE`,
        :data:`COMPUTE_MODE_CLASSIC_MULTI_NODE`, or
        :data:`COMPUTE_MODE_INHERIT`.

    DatabricksNotebook and DatabricksSparkPython activities always
    inherit the linked-service-derived cluster binding; serverless is
    no longer offered as a replacement for source-defined clusters.
    """
    if not is_non_databricks_task(activity):
        return COMPUTE_MODE_INHERIT
    if activity_preferences.non_databricks_task_compute is NonDatabricksTaskCompute.SERVERLESS:
        return COMPUTE_MODE_SERVERLESS
    if isinstance(activity, CopyActivity):
        return COMPUTE_MODE_CLASSIC_MULTI_NODE
    return COMPUTE_MODE_CLASSIC_SINGLE_NODE
