"""Standalone operations: option gathering, validation, and IR modification.

The agent adapter and the CLI bridge call into these functions; nothing
here is stateful.  Configuration dataclasses, StrEnums, and option shapes
live in :mod:`flowx.adapter.models`.
"""

from __future__ import annotations

import dataclasses
import json
from enum import StrEnum
from pathlib import Path
from typing import Any

from flowx.adapter.constants import (
    COMPUTE_MODE_CLASSIC_MULTI_NODE,
    COMPUTE_MODE_CLASSIC_SINGLE_NODE,
    COMPUTE_MODE_INHERIT,
    COMPUTE_MODE_SERVERLESS,
    DATABASE_SOURCE_TYPE_HINT,
    LAKEFLOW_CONNECT_MOTIF_REPLACEMENTS,
    LAKEFLOW_CONNECT_REPLACEMENT,
    LAKEFLOW_CONNECTOR_TYPE_QUERY_BASED,
    METADATA_DRIVEN_MOTIF_ID,
    MOTIF_CONSOLIDATE_OPTION_PREFIX,
    OPTION_COPY_ACTIVITY_PARADIGM,
    OPTION_METADATA_DRIVEN_ACCESS,
    OPTION_METADATA_DRIVEN_CONSOLIDATE,
    OPTION_METADATA_DRIVEN_LOOKUP_TOOL,
    OPTION_METADATA_DRIVEN_SIZE,
    OPTION_NON_DATABRICKS_TASK_COMPUTE,
    OPTION_NOTIFY_DESTINATION,
    OPTION_NOTIFY_DESTINATION_NAME,
    OPTION_NOTIFY_EVENTS,
    OPTION_USE_LAKEFLOW_CONNECTORS,
)
from flowx.adapter.models import (
    FIELD_TO_ENUM,
    CopyActivityParadigm,
    LakeflowConnectorType,
    MetadataDrivenAccess,
    MetadataDrivenConsolidate,
    MetadataDrivenLookupTool,
    MetadataDrivenSize,
    MotifConsolidate,
    NonDatabricksTaskCompute,
    NotifyDestination,
    NotifyEvents,
    OptionChoice,
    PendingOptions,
    TranslationConfiguration,
    TranslationOption,
    UseLakeflowConnectors,
)
from flowx.adapter.predicates import (
    copy_eligible_for_any_lfc_connector,
    copy_eligible_for_lfc_query_based,
    copy_query_unfit_for_lfc,
    copy_targets_delta,
    is_non_databricks_task,
    walk_activities,
)
from flowx.models.ir import (
    Activity,
    CopyActivity,
    Dependency,
    ForEachActivity,
    IfConditionActivity,
    MotifActivity,
    Pipeline,
    SwitchActivity,
    SwitchCase,
    WebActivity,
)
from flowx.models.motifs import MOTIF_LAKEFLOW_CONNECT_DATABASE

# Free-text option ids (no backing enum) that validate_answer accepts with any value, vs a genuinely
# unknown id which it rejects -- the set is the NOTIFY_FREE_TEXT_OPTION_IDS registry defined below.


def enum_for(option_id: str) -> type[StrEnum] | None:
    """Returns the enum class backing a configuration field.

    Args:
        option_id: Field name (e.g. ``"copy_activity_paradigm"``) or
            per-motif id (e.g. ``"consolidate_motif:rest_api_pagination"``).

    Returns:
        The :class:`StrEnum` subclass that defines the allowed values, or
        ``None`` when the option_id is unknown.
    """
    if option_id.startswith(MOTIF_CONSOLIDATE_OPTION_PREFIX):
        return MotifConsolidate
    return FIELD_TO_ENUM.get(option_id)


def allowed_values_for(option_id: str) -> tuple[str, ...]:
    """Returns the allowed string values for a configuration field.

    Args:
        option_id: Field name (e.g. ``"copy_activity_paradigm"``).

    Returns:
        Tuple of allowed string values in declaration order.  Empty when
        the field is unknown.
    """
    enum_cls = enum_for(option_id)
    return tuple(member.value for member in enum_cls) if enum_cls else ()


def validate_answer(option_id: str, value: str) -> str:
    """Returns *value* when it is an allowed answer for *option_id*.

    Args:
        option_id: Stable option identifier.
        value: Caller-supplied answer string.

    Returns:
        The validated value, unchanged.

    Raises:
        ValueError: When *option_id* is not known or *value* is not in
            the allowed set for the option.
    """
    allowed = allowed_values_for(option_id)
    if allowed:
        if value not in allowed:
            raise ValueError(f"Invalid answer {value!r} for {option_id!r}; allowed: {sorted(allowed)}")
        return value
    if option_id in NOTIFY_FREE_TEXT_OPTION_IDS:
        return value
    raise ValueError(f"Unknown option_id {option_id!r}")
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


def gather_options(
    pipeline: Pipeline,
    motifs: list | None = None,
    *,
    answers: dict[str, str] | None = None,
) -> PendingOptions:
    """Walks the IR and returns the options that apply to *pipeline*.

    Args:
        pipeline: Translated pipeline IR after motif collapsing.
        motifs: Detected motifs, used to surface the Lakeflow Connect
            option for multi-step database ingestion patterns.
        answers: Answers the caller has already collected.  Options
            whose ``option_id`` is in this mapping are filtered out,
            and options whose ``conditions`` reference earlier answers
            are evaluated against this mapping.

    Returns:
        A :class:`PendingOptions` instance carrying the options whose
        IR preconditions and answer-dependent conditions are met but
        whose ``option_id`` has not yet been answered.
    """
    motif_list = motifs or []
    answer_map = answers or {}
    builders = (
        _build_use_lakeflow_connectors_option,
        _build_lakeflow_connector_type_option,
        _build_copy_activity_paradigm_option,
        _build_non_databricks_task_compute_option,
        _build_metadata_driven_consolidate_option,
        _build_metadata_driven_access_option,
        _build_metadata_driven_size_option,
        _build_metadata_driven_lookup_tool_option,
    )
    candidates = (builder(pipeline, motif_list, answers=answer_map) for builder in builders)
    pending = [
        option
        for option in candidates
        if option is not None and option.option_id not in answer_map and _conditions_met(option.conditions, answer_map)
    ]
    # Per-motif consolidate options: one per detected motif (consolidate_motif:<id>), default keep.
    for motif_option in _build_motif_consolidation_options(motif_list):
        if motif_option.option_id in answer_map:
            continue
        pending.append(motif_option)
    # activity->Notify chain: destination choice, then one follow-up per SDK field once chosen.
    for notify_option in _build_notify_options(pipeline, answer_map):
        if notify_option.option_id in answer_map:
            continue
        pending.append(notify_option)
    return PendingOptions(pipeline_name=pipeline.name, options=pending)


def _conditions_met(conditions: tuple[tuple[str, str], ...], answers: dict[str, str]) -> bool:
    """Returns True when every condition is satisfied by *answers*.

    Args:
        conditions: Tuples of ``(option_id, expected_value)`` from a
            :class:`TranslationOption`.
        answers: Mapping of option_id to the caller-supplied answer.

    Returns:
        ``True`` when every condition's option has been answered with
        the expected value (or when *conditions* is empty); ``False``
        otherwise.
    """
    return all(answers.get(qid) == expected for qid, expected in conditions)


# Notify destinations other than KEEP — the destination-name and events follow-ups apply to all of them.
_NOTIFY_NON_KEEP: tuple[str, ...] = tuple(d.value for d in NotifyDestination if d is not NotifyDestination.KEEP)


def _show_when_from_conditions(conditions: tuple[tuple[str, str], ...]) -> list[dict[str, Any]]:
    """Translates a TranslationOption's equality ``conditions`` into ``show_when`` clauses."""
    return [{"option_id": qid, "in": [value]} for qid, value in conditions]


def _option_schema(option: TranslationOption, show_when: list[dict[str, Any]]) -> dict[str, Any]:
    """Serialises one option into a declarative schema entry the agent can walk locally."""
    return {
        "option_id": option.option_id,
        "prompt": option.prompt,
        "rationale": option.rationale,
        "choices": [
            {"value": choice.value, "label": choice.label, "description": choice.description}
            for choice in option.options
        ],
        "free_text": not option.options,
        "default": option.default,
        "affected_task_keys": list(option.affected_task_keys),
        "show_when": show_when,
    }


def _build_notify_schema(pipeline: Pipeline) -> list[dict[str, Any]]:
    """The full activity_and_notify chain as schema entries (every destination's follow-ups)."""
    affected = _notify_present(pipeline)
    if not affected:
        return []
    entries = [_option_schema(_build_notify_destination_option(affected), [])]
    for dest, fields in _NOTIFY_FIELDS.items():
        dest_clause = [{"option_id": OPTION_NOTIFY_DESTINATION, "in": [dest]}]
        entries.extend(_option_schema(_notify_field_option(dest, field, affected), dest_clause) for field in fields)
    non_keep_clause = [{"option_id": OPTION_NOTIFY_DESTINATION, "in": list(_NOTIFY_NON_KEEP)}]
    entries.append(_option_schema(_notify_name_option(affected), non_keep_clause))
    entries.append(_option_schema(_build_notify_events_option(affected), non_keep_clause))
    return entries


def build_option_schema(pipeline: Pipeline, motifs: list | None = None) -> list[dict[str, Any]]:
    """Returns the full declarative option tree for *pipeline* as schema dicts.

    Unlike :func:`gather_options` -- which filters to the options *currently* pending given the
    answers collected so far -- this returns **every** option the pipeline can raise, each annotated
    with a ``show_when`` condition: a conjunction of ``{"option_id", "in": [values]}`` clauses (empty
    list = always shown).  The agent walks this tree locally, asking only the options whose
    ``show_when`` clauses are all satisfied by the answers gathered so far, so a multi-step chain
    (e.g. notify destination -> per-field follow-ups, or metadata-driven consolidate -> access ->
    size -> lookup-tool) needs no per-follow-up round trip back to the server.  ``apply_answers``
    remains the single validate-and-apply path.
    """
    motif_list = motifs or []
    schema: list[dict[str, Any]] = []
    builders = (
        _build_use_lakeflow_connectors_option,
        _build_lakeflow_connector_type_option,
        _build_copy_activity_paradigm_option,
        _build_non_databricks_task_compute_option,
        _build_metadata_driven_consolidate_option,
        _build_metadata_driven_access_option,
        _build_metadata_driven_size_option,
        _build_metadata_driven_lookup_tool_option,
    )
    for builder in builders:
        option = builder(pipeline, motif_list, answers={})
        if option is not None:
            schema.append(_option_schema(option, _show_when_from_conditions(option.conditions)))
    for motif_option in _build_motif_consolidation_options(motif_list):
        schema.append(_option_schema(motif_option, _show_when_from_conditions(motif_option.conditions)))
    schema.extend(_build_notify_schema(pipeline))
    return schema


def apply_configuration(pipeline: Pipeline, pipeline_configuration: TranslationConfiguration) -> Pipeline:
    """Returns a copy of *pipeline* with configuration stamped onto each activity.

    Args:
        pipeline: Translated pipeline IR after motif collapsing.
        pipeline_configuration: Validated pipeline-wide configuration.

    Returns:
        A new :class:`Pipeline` whose activities carry concrete decisions
        about compute, target format, and Lakeflow Connect replacement.
        The input pipeline is not mutated.
    """
    stamped_tasks = [_stamp_activity(activity, pipeline_configuration) for activity in pipeline.tasks]
    stamped = dataclasses.replace(
        pipeline,
        tasks=stamped_tasks,
        translation_configuration=pipeline_configuration,
    )
    if pipeline_configuration.notify_destination is not NotifyDestination.KEEP:
        stamped = _collapse_notify(stamped, pipeline_configuration)
    return stamped


def provision_notification_destinations(pipeline: Pipeline) -> tuple[Pipeline, list[str]]:
    """Create the Databricks notification destinations for *pipeline* at prompt time.

    Walks the collapsed tasks and, for every non-email ``activity_and_notify``
    notification spec, creates (or reuses) the destination via the SDK now and stamps
    the resolved ``destination_id`` back onto the task.  Email specs are left untouched
    -- email wires raw ``email_notifications`` and needs no destination.  This runs in
    the adapter ``modify`` phase so the destination exists (and validates) as soon as
    the user answers, rather than at prepare time.

    Returns:
        ``(pipeline, messages)`` -- a copy of *pipeline* with resolved ids stamped in,
        and human-readable status lines (one per destination) for surfacing to the user.
        When no non-email notifications are present the pipeline is returned unchanged
        with an empty message list and no SDK call is made.
    """
    from flowx.preparer.notifications import provision_destination

    messages: list[str] = []
    new_tasks: list = []
    changed = False
    for task in pipeline.tasks:
        spec = getattr(task, "notifications", None)
        if spec and spec.get("destination") not in (None, "", "email"):
            new_spec, message = provision_destination(spec)
            if message:
                messages.append(message)
            if new_spec is not spec:
                task = dataclasses.replace(task, notifications=new_spec)
                changed = True
        new_tasks.append(task)
    if not changed:
        return pipeline, messages
    return dataclasses.replace(pipeline, tasks=new_tasks), messages


_NOTIFY_WEBHOOK_DESTS: frozenset[str] = frozenset({"slack", "teams", "webhook"})


@dataclasses.dataclass(frozen=True, slots=True)
class _NotifyField:
    """One Databricks-SDK config field of a notification destination.

    Surfaced as a chained follow-up option after the user picks a destination.

    Attributes:
        option_id: Adapter option id for the follow-up question.
        sdk_arg: The kwarg on the SDK config class (e.g. ``url``, ``addresses``).
        prompt: The question text.
        required: Whether the SDK config needs this field for the destination.
        is_list: When True the comma-separated answer is split into a list
            (used for email ``addresses``).
    """

    option_id: str
    sdk_arg: str
    prompt: str
    required: bool = True
    is_list: bool = False


# Per-destination SDK config fields (required first), from the SDK notification-destination config
# classes; the adapter chains one follow-up option per field after the destination is chosen.
_NOTIFY_FIELDS: dict[str, tuple[_NotifyField, ...]] = {
    NotifyDestination.EMAIL.value: (
        _NotifyField(
            "notify_email_recipients",
            "addresses",
            "Recipient email address(es)? (comma-separated)",
            is_list=True,
        ),
    ),
    NotifyDestination.SLACK.value: (
        _NotifyField("notify_slack_url", "url", "Slack incoming webhook URL?"),
        _NotifyField("notify_slack_channel_id", "channel_id", "Slack channel id? (optional)", required=False),
        _NotifyField("notify_slack_oauth_token", "oauth_token", "Slack OAuth token? (optional)", required=False),
    ),
    NotifyDestination.TEAMS.value: (_NotifyField("notify_teams_url", "url", "Microsoft Teams incoming webhook URL?"),),
    NotifyDestination.PAGERDUTY.value: (
        _NotifyField("notify_pagerduty_integration_key", "integration_key", "PagerDuty integration key?"),
    ),
    NotifyDestination.WEBHOOK.value: (
        _NotifyField("notify_webhook_url", "url", "Generic webhook URL?"),
        _NotifyField("notify_webhook_username", "username", "Webhook basic-auth username? (optional)", required=False),
        _NotifyField("notify_webhook_password", "password", "Webhook basic-auth password? (optional)", required=False),
    ),
}

# Free-text follow-up option ids (no backing enum).  ``validate_answer`` accepts
# any value for these, distinct from a genuinely unknown option id.
NOTIFY_FREE_TEXT_OPTION_IDS: frozenset[str] = frozenset(
    {field.option_id for fields in _NOTIFY_FIELDS.values() for field in fields} | {OPTION_NOTIFY_DESTINATION_NAME}
)


def _find_notify_groups(tasks: list) -> dict[str, list[tuple[Any, str]]]:
    """Find activity->notify groups in the IR: any non-Web task with WebActivity dependents.

    Returns a mapping of ``upstream_task_key`` -> list of ``(web_activity, outcome)`` where
    outcome is the dependency condition (``Succeeded`` / ``Failed`` / ...).

    Any activity -- Copy, Notebook, Lookup, SparkPython, a stored procedure, etc. -- that is directly
    followed by one or more WebActivity calls is a candidate: collapsing turns those Web calls into
    native Databricks job-task notifications on the upstream task. The upstream is any non-Web task
    (a WebActivity that merely follows another WebActivity is not treated as a notify target).
    Mirrors the activity_and_notify motif on the (non-collapsed) IR.
    """
    upstream = {t.task_key for t in tasks if not isinstance(t, WebActivity)}
    groups: dict[str, list[tuple[Any, str]]] = {}
    for task in tasks:
        if isinstance(task, WebActivity) and task.depends_on:
            for dep in task.depends_on:
                if dep.task_key in upstream:
                    groups.setdefault(dep.task_key, []).append((task, dep.outcome or "Succeeded"))
                    break
    return groups


def _notify_present(pipeline: Pipeline) -> tuple[str, ...]:
    """Task keys of activities that have notify WebActivity dependents."""
    return tuple(sorted(_find_notify_groups(pipeline.tasks).keys()))


def _notify_dest(answers: dict[str, str] | None) -> str:
    return (answers or {}).get(OPTION_NOTIFY_DESTINATION, "")


def _freetext_option(option_id: str, prompt: str, rationale: str, affected: tuple[str, ...]) -> TranslationOption:
    """Builds a free-text option (no enum choices)."""
    return TranslationOption(
        option_id=option_id,
        prompt=prompt,
        rationale=rationale,
        options=(),
        affected_task_keys=affected,
        default="",
    )


def _build_notify_destination_option(affected: tuple[str, ...]) -> TranslationOption:
    """Asks whether/how to route an activity->Notify motif to a Databricks destination."""
    return TranslationOption(
        option_id=OPTION_NOTIFY_DESTINATION,
        prompt=(
            "One or more activities are followed by notification Web activities. "
            "Route these to a Databricks destination?"
        ),
        rationale=(
            "Choosing a destination collapses the pattern: the upstream activity becomes the task "
            "and the downstream notifications become Databricks job-task success/failure "
            "notifications (email_notifications or webhook_notifications). The ADF Web activity's own "
            "URL/body is not used. Keeping preserves the current per-activity Web activity translation."
        ),
        options=(
            OptionChoice(
                value=NotifyDestination.KEEP.value,
                label="Keep current behavior",
                description="Do not collapse; translate the Web activities directly.",
            ),
            OptionChoice(
                value=NotifyDestination.EMAIL.value,
                label="Email",
                description="Wire email_notifications with recipient addresses.",
            ),
            OptionChoice(
                value=NotifyDestination.SLACK.value,
                label="Slack",
                description="Create a Slack notification destination and wire webhook_notifications.",
            ),
            OptionChoice(
                value=NotifyDestination.TEAMS.value,
                label="Microsoft Teams",
                description="Create a Teams notification destination and wire webhook_notifications.",
            ),
            OptionChoice(
                value=NotifyDestination.PAGERDUTY.value,
                label="PagerDuty",
                description="Create a PagerDuty notification destination and wire webhook_notifications.",
            ),
            OptionChoice(
                value=NotifyDestination.WEBHOOK.value,
                label="Generic Webhook",
                description="Create a generic webhook destination and wire webhook_notifications.",
            ),
        ),
        affected_task_keys=affected,
        default=NotifyDestination.KEEP.value,
    )


def _build_notify_events_option(affected: tuple[str, ...]) -> TranslationOption:
    return TranslationOption(
        option_id=OPTION_NOTIFY_EVENTS,
        prompt="Which events should notify?",
        rationale="Defaults to both (whatever the source notify activities covered). Restrict if desired.",
        options=(
            OptionChoice(
                value=NotifyEvents.BOTH.value,
                label="Both success and failure",
                description="Wire on_success and on_failure (as the source activities had).",
            ),
            OptionChoice(
                value=NotifyEvents.ON_FAILURE.value, label="On failure only", description="Only wire on_failure."
            ),
            OptionChoice(
                value=NotifyEvents.ON_SUCCESS.value, label="On success only", description="Only wire on_success."
            ),
        ),
        affected_task_keys=affected,
        default=NotifyEvents.BOTH.value,
    )


def _notify_field_option(dest: str, field: _NotifyField, affected: tuple[str, ...]) -> TranslationOption:
    """Builds the free-text follow-up for one SDK config field of a notification destination."""
    suffix = "" if field.required else " (optional -- leave blank to skip)"
    return _freetext_option(
        field.option_id,
        field.prompt,
        f"Maps to the Databricks SDK {dest} config field `{field.sdk_arg}`.{suffix}",
        affected,
    )


def _notify_name_option(affected: tuple[str, ...]) -> TranslationOption:
    """Builds the optional destination-display-name follow-up."""
    return _freetext_option(
        OPTION_NOTIFY_DESTINATION_NAME,
        "Display name for the notification destination? (optional; default derived)",
        "Reused if a destination with this name already exists, so prepare is idempotent.",
        affected,
    )


def _build_notify_options(pipeline: Pipeline, answers: dict[str, str] | None) -> list[TranslationOption]:
    """Returns the chained activity_and_notify options.

    The first prompt is the destination choice.  Once a (non-keep) destination is
    answered, one follow-up option is surfaced per SDK field of that destination
    (required first; optional fields flagged), so the agent prompts for each field
    sequentially, followed by an optional display name and the events selector.
    """
    affected = _notify_present(pipeline)
    if not affected:
        return []
    options: list[TranslationOption] = [_build_notify_destination_option(affected)]
    dest = _notify_dest(answers)
    if dest in ("", NotifyDestination.KEEP.value):
        return options
    options.extend(_notify_field_option(dest, field, affected) for field in _NOTIFY_FIELDS.get(dest, ()))
    options.append(_notify_name_option(affected))
    options.append(_build_notify_events_option(affected))
    return options


def collect_notify_args(answers: dict[str, str]) -> dict[str, str]:
    """Collect the per-field notification answers into ``{sdk_arg: value}``.

    Reads only the fields belonging to the chosen destination so that, e.g., the
    Slack ``url`` answer and a Webhook ``url`` answer never collide.
    """
    dest = answers.get(OPTION_NOTIFY_DESTINATION, "")
    args: dict[str, str] = {}
    for field in _NOTIFY_FIELDS.get(dest, ()):
        value = answers.get(field.option_id)
        if value:
            args[field.sdk_arg] = value
    return args


def _notification_spec(config: TranslationConfiguration) -> dict[str, Any]:
    """Builds the notification spec stamped onto the collapsed Copy task.

    ``args`` carries the resolved SDK config kwargs for the destination; email
    ``addresses`` is split into a list, other fields pass through as strings.
    """
    dest = config.notify_destination.value
    spec: dict[str, Any] = {
        "destination": dest,
        "destination_name": config.notify_destination_name or f"flowx-{dest}",
        "args": {},
    }
    for field in _NOTIFY_FIELDS.get(dest, ()):
        raw = config.notify_args.get(field.sdk_arg, "")
        if not raw:
            continue
        spec["args"][field.sdk_arg] = (
            [item.strip() for item in raw.split(",") if item.strip()] if field.is_list else raw
        )
    return spec


def _collapse_notify(pipeline: Pipeline, config: TranslationConfiguration) -> Pipeline:
    """Collapse activity->notify groups: drop the notify Web activities and stamp a
    notification spec (events + chosen destination) onto each upstream task.

    Works for any upstream activity type (Copy, Notebook, Lookup, …). Dependents of a dropped
    notify activity are rewired to the upstream task so the DAG stays connected. The destination is
    created (and its id resolved) later, at prepare time.
    """
    groups = _find_notify_groups(pipeline.tasks)
    if not groups:
        return pipeline
    spec_base = _notification_spec(config)
    notify_to_upstream: dict[str, str] = {}
    events_by_task: dict[str, list[str]] = {}
    drop: set[str] = set()
    for upstream_key, web_list in groups.items():
        events: set[str] = set()
        for web, outcome in web_list:
            events.add("on_failure" if outcome == "Failed" else "on_success")
            drop.add(web.task_key)
            notify_to_upstream[web.task_key] = upstream_key
        chosen = config.notify_events
        if chosen is NotifyEvents.ON_FAILURE:
            events &= {"on_failure"}
        elif chosen is NotifyEvents.ON_SUCCESS:
            events &= {"on_success"}
        events_by_task[upstream_key] = sorted(events) or ["on_failure"]

    new_tasks: list = []
    for task in pipeline.tasks:
        if task.task_key in drop:
            continue
        deps = task.depends_on
        if deps:
            rewired: list = []
            seen: set[str] = set()
            for dep in deps:
                key = notify_to_upstream.get(dep.task_key, dep.task_key)
                if key not in seen:
                    seen.add(key)
                    rewired.append(Dependency(task_key=key, outcome=dep.outcome))
            deps = rewired
        if task.task_key in events_by_task:
            spec = {**spec_base, "events": events_by_task[task.task_key]}
            task = dataclasses.replace(task, depends_on=deps, notifications=spec)
        else:
            task = dataclasses.replace(task, depends_on=deps)
        new_tasks.append(task)
    return dataclasses.replace(pipeline, tasks=new_tasks)


def _build_copy_activity_paradigm_option(
    pipeline: Pipeline, motifs: list, answers: dict[str, str] | None = None
) -> TranslationOption | None:
    """Builds the SDP-vs-notebook option for Copy activities targeting Delta.

    Args:
        pipeline: Translated pipeline IR.
        motifs: Detected motifs (unused; accepted for builder uniformity).
        answers: Answers already supplied for prior prompts.  When the
            user opted into Lakeflow Connect, this option only fires
            for Copy activities that are *not* LFC-eligible -- the
            paradigm choice is moot for Copies that will become
            managed LFC pipelines.

    Returns:
        The constructed :class:`TranslationOption`, or ``None`` when no
        Copy activity needs a paradigm choice.  Copies whose source
        query is unfit for both LFC and SDP (joins, aggregates, etc.)
        are forced to PySpark notebook and excluded from the affected
        set.
    """
    answers = answers or {}
    going_to_lfc = answers.get(OPTION_USE_LAKEFLOW_CONNECTORS) == UseLakeflowConnectors.LAKEFLOW_CONNECT.value
    affected = tuple(
        activity.task_key
        for activity in walk_activities(pipeline.tasks)
        if isinstance(activity, CopyActivity)
        and copy_targets_delta(activity)
        and not _copy_paradigm_decided_by_lfc(activity, going_to_lfc)
    )
    if not affected:
        return None
    return TranslationOption(
        option_id=OPTION_COPY_ACTIVITY_PARADIGM,
        prompt="How should Copy Data activities targeting Delta be implemented?",
        rationale=(
            "One or more Copy Data activities write to a Delta table.  "
            "Lakeflow Spark Declarative Pipelines define tables declaratively; "
            "a PySpark notebook stays closer to the original ADF activity shape."
        ),
        options=(
            OptionChoice(
                value=CopyActivityParadigm.NOTEBOOK.value,
                label="PySpark notebook",
                description="Generates a notebook task that reads the source and writes Delta directly.",
            ),
            OptionChoice(
                value=CopyActivityParadigm.SDP.value,
                label="Lakeflow Spark Declarative Pipeline",
                description="Emits an SDP pipeline resource with declarative table definitions.",
            ),
        ),
        affected_task_keys=affected,
        default=CopyActivityParadigm.NOTEBOOK.value,
    )


def _build_non_databricks_task_compute_option(
    pipeline: Pipeline, motifs: list, answers: dict[str, str] | None = None
) -> TranslationOption | None:
    """Builds the serverless-vs-classic option for non-Databricks tasks.

    Args:
        pipeline: Translated pipeline IR.
        motifs: Detected motifs (unused; accepted for builder uniformity).
        answers: Answers already supplied for prior prompts.  Copies
            that will become managed LFC pipelines are excluded
            because LFC pipelines always use serverless compute.

    Returns:
        The constructed :class:`TranslationOption`, or ``None`` when
        every non-Databricks task in the pipeline is going to LFC (no
        compute choice to make).
    """
    answers = answers or {}
    going_to_lfc = answers.get(OPTION_USE_LAKEFLOW_CONNECTORS) == UseLakeflowConnectors.LAKEFLOW_CONNECT.value
    affected = tuple(
        activity.task_key
        for activity in walk_activities(pipeline.tasks)
        if is_non_databricks_task(activity) and not _task_compute_decided_by_lfc(activity, going_to_lfc)
    )
    if not affected:
        return None
    return TranslationOption(
        option_id=OPTION_NON_DATABRICKS_TASK_COMPUTE,
        prompt="What compute should the non-Databricks tasks use?",
        rationale=(
            "Tasks such as Copy Data, Web, Lookup, and Wait can run on serverless "
            "or classic compute.  Classic provisions a single-node cluster for most "
            "tasks and a larger fixed-size cluster for Copy Data."
        ),
        options=(
            OptionChoice(
                value=NonDatabricksTaskCompute.SERVERLESS.value,
                label="Serverless",
                description="Runs every non-Databricks task on serverless compute.",
            ),
            OptionChoice(
                value=NonDatabricksTaskCompute.CLASSIC.value,
                label="Classic job_cluster",
                description="Provisions classic job_clusters sized per task type.",
            ),
        ),
        affected_task_keys=affected,
        default=NonDatabricksTaskCompute.SERVERLESS.value,
    )


def _build_use_lakeflow_connectors_option(
    pipeline: Pipeline, motifs: list, answers: dict[str, str] | None = None
) -> TranslationOption | None:
    """Builds the Lakeflow Connect option for eligible database ingestions.

    Args:
        pipeline: Translated pipeline IR.
        motifs: Detected motifs, scanned for database-source ingestion patterns.

    Returns:
        The constructed :class:`TranslationOption`, or ``None`` when no
        Copy activity or motif qualifies for Lakeflow Connect.
    """
    affected = _affected_task_keys_for_lakeflow_connect(pipeline, motifs)
    if not affected:
        return None
    return TranslationOption(
        option_id=OPTION_USE_LAKEFLOW_CONNECTORS,
        prompt="Migrate eligible SQL Server, MySQL, and PostgreSQL ingestions to Lakeflow Connect?",
        rationale=(
            "One or more Copy Data activities ingest from SQL Server, MySQL, or "
            "PostgreSQL into Delta.  Managed Lakeflow Connect replaces the bespoke "
            "ingestion with a declarative pipeline; the existing translation keeps "
            "the ADF-shaped activity intact."
        ),
        options=(
            OptionChoice(
                value=UseLakeflowConnectors.EXISTING.value,
                label="Keep existing translation",
                description="Preserves the Copy Data activity as a notebook or SDP task.",
            ),
            OptionChoice(
                value=UseLakeflowConnectors.LAKEFLOW_CONNECT.value,
                label="Use Lakeflow Connect",
                description="Replaces eligible ingestions with a managed Lakeflow Connect pipeline.",
            ),
        ),
        affected_task_keys=affected,
        default=UseLakeflowConnectors.EXISTING.value,
    )


def _build_lakeflow_connector_type_option(
    pipeline: Pipeline, motifs: list, answers: dict[str, str] | None = None
) -> TranslationOption | None:
    """Builds the CDC-vs-query connector option, suppressed when not actionable.

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
        configuration between CDC and query-based therefore has no
        actionable effect; the modifier picks the eligible connector
        per Copy.
    """
    del pipeline, motifs, answers
    return None


def _build_metadata_driven_consolidate_option(
    pipeline: Pipeline,
    motifs: list,
    answers: dict[str, str] | None = None,
) -> TranslationOption | None:
    """Builds the consolidate-or-keep option for metadata-driven motifs.

    Args:
        pipeline: Translated pipeline IR.
        motifs: Detected motifs; the option only surfaces when at
            least one matches the metadata-driven bulk copy pattern.

    Returns:
        The constructed :class:`TranslationOption`, or ``None`` when
        the pipeline contains no metadata-driven motif.
    """
    affected = _metadata_driven_motif_task_keys(pipeline, motifs)
    if not affected:
        return None
    return TranslationOption(
        option_id=OPTION_METADATA_DRIVEN_CONSOLIDATE,
        prompt="Consolidate the metadata-driven ingestions into one managed pipeline?",
        rationale=(
            "A Lookup feeds a ForEach that copies each row's table.  Consolidating "
            "replaces this loop with a single Lakeflow Connect or Lakeflow Spark "
            "Declarative Pipeline whose objects list materialises each source as "
            "its own streaming table.  Keeping it emits a Databricks for-each task "
            "that runs one Spark JDBC read per source table (no managed pipeline)."
        ),
        options=(
            OptionChoice(
                value=MetadataDrivenConsolidate.KEEP.value,
                label="Keep the per-table loop",
                description="Emits a for-each task running one Spark JDBC read per source table.",
            ),
            OptionChoice(
                value=MetadataDrivenConsolidate.CONSOLIDATE.value,
                label="Consolidate into one pipeline",
                description="Emits one pipeline resource that ingests every source from the lookup.",
            ),
        ),
        affected_task_keys=affected,
        default=MetadataDrivenConsolidate.KEEP.value,
    )


def _build_metadata_driven_access_option(
    pipeline: Pipeline,
    motifs: list,
    answers: dict[str, str] | None = None,
) -> TranslationOption | None:
    """Builds the metadata-source access option, gated on consolidate=yes.

    Args:
        pipeline: Translated pipeline IR.
        motifs: Detected motifs.

    Returns:
        The constructed :class:`TranslationOption`, or ``None`` when no
        metadata-driven motif applies.
    """
    affected = _metadata_driven_motif_task_keys(pipeline, motifs)
    if not affected:
        return None
    return TranslationOption(
        option_id=OPTION_METADATA_DRIVEN_ACCESS,
        prompt="Do you have access to query the metadata source and approve doing so?",
        rationale=(
            "Consolidating a metadata-driven ingestion requires materialising the "
            "lookup query at translation time so each row becomes a pipeline object.  "
            "Answering yes confirms the metadata source can be queried during this "
            "translation pass; answering no falls back to the per-row scaffold."
        ),
        options=(
            OptionChoice(
                value=MetadataDrivenAccess.YES.value,
                label="Yes, query is allowed",
                description="The metadata source is reachable and approved for read during translation.",
            ),
            OptionChoice(
                value=MetadataDrivenAccess.NO.value,
                label="No, skip materialising the lookup",
                description="Keeps the per-row motif scaffold without inlining the configuration.",
            ),
        ),
        affected_task_keys=affected,
        default=MetadataDrivenAccess.NO.value,
        conditions=((OPTION_METADATA_DRIVEN_CONSOLIDATE, MetadataDrivenConsolidate.CONSOLIDATE.value),),
    )


def _build_metadata_driven_size_option(
    pipeline: Pipeline,
    motifs: list,
    answers: dict[str, str] | None = None,
) -> TranslationOption | None:
    """Builds the t-shirt sizing option, gated on consolidate=yes.

    Args:
        pipeline: Translated pipeline IR.
        motifs: Detected motifs.

    Returns:
        The constructed :class:`TranslationOption`, or ``None`` when no
        metadata-driven motif applies.
    """
    affected = _metadata_driven_motif_task_keys(pipeline, motifs)
    if not affected:
        return None
    return TranslationOption(
        option_id=OPTION_METADATA_DRIVEN_SIZE,
        prompt="Roughly how many configuration rows feed the metadata-driven ingestion?",
        rationale=(
            "The size determines whether the modifier inlines every lookup row into "
            "one consolidated pipeline.  Small and medium-sized configurations are "
            "expanded inline; large configurations keep the per-row scaffold to "
            "avoid generating an unwieldy pipeline definition."
        ),
        options=(
            OptionChoice(
                value=MetadataDrivenSize.SMALL.value,
                label="S (under 50 rows)",
                description="Lookup feeds fewer than 50 ingestion targets.",
            ),
            OptionChoice(
                value=MetadataDrivenSize.MEDIUM.value,
                label="M (under 250 rows)",
                description="Lookup feeds 50 to 249 ingestion targets.",
            ),
            OptionChoice(
                value=MetadataDrivenSize.LARGE.value,
                label="L (250 or more rows)",
                description="Lookup feeds 250+ targets; skip inline consolidation.",
            ),
        ),
        affected_task_keys=affected,
        default=MetadataDrivenSize.LARGE.value,
        conditions=((OPTION_METADATA_DRIVEN_CONSOLIDATE, MetadataDrivenConsolidate.CONSOLIDATE.value),),
    )


def _build_metadata_driven_lookup_tool_option(
    pipeline: Pipeline,
    motifs: list,
    answers: dict[str, str] | None = None,
) -> TranslationOption | None:
    """Builds the agent-tool option for the lookup query, gated on size != L.

    Args:
        pipeline: Translated pipeline IR.
        motifs: Detected motifs.

    Returns:
        The constructed :class:`TranslationOption`, or ``None`` when no
        metadata-driven motif applies.
    """
    affected = _metadata_driven_motif_task_keys(pipeline, motifs)
    if not affected:
        return None
    return TranslationOption(
        option_id=OPTION_METADATA_DRIVEN_LOOKUP_TOOL,
        prompt="Does the agent have a tool that can run the lookup query?",
        rationale=(
            "When the agent has a Genie skill, an MCP database tool, or a SQL "
            "warehouse it can call, the modifier asks the agent to execute the "
            "lookup query directly and reuses the rows.  When no tool is "
            "available the agent prompts the user for a CSV file or "
            "comma-separated string of values and the modifier ingests that."
        ),
        options=(
            OptionChoice(
                value=MetadataDrivenLookupTool.HAVE.value,
                label="Yes, the agent can run the lookup",
                description="Agent executes the lookup query via its own tool.",
            ),
            OptionChoice(
                value=MetadataDrivenLookupTool.NONE.value,
                label="No, ask the user for the values",
                description="Agent prompts the user for a CSV file or string of values.",
            ),
        ),
        affected_task_keys=affected,
        default=MetadataDrivenLookupTool.NONE.value,
        conditions=(
            (OPTION_METADATA_DRIVEN_CONSOLIDATE, MetadataDrivenConsolidate.CONSOLIDATE.value),
            (OPTION_METADATA_DRIVEN_ACCESS, MetadataDrivenAccess.YES.value),
        ),
    )


def _build_motif_consolidation_options(motifs: list) -> list[TranslationOption]:
    """Builds one ``consolidate_motif:<id>`` option per detected motif.

    Args:
        motifs: Detected :class:`~flowx.models.motifs.DetectedMotif`
            instances from :func:`flowx.motifs.detector.detect_motifs`.

    Returns:
        A list of :class:`TranslationOption` instances, one per
        detected motif.  Each option uses a unique option_id of the
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
          emits a single option covering all instances of that type.
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
    options: list[TranslationOption] = []
    for motif in motifs:
        definition = motif.definition
        motif_id = definition.motif_id
        if motif_id in seen:
            continue
        seen.add(motif_id)
        affected = tuple(motif.matched_activities)
        option_id = f"{MOTIF_CONSOLIDATE_OPTION_PREFIX}{motif_id}"
        confidence_suffix = ""
        if motif.confidence_notes:
            confidence_suffix = "  Detector notes: " + " | ".join(motif.confidence_notes)
        options.append(
            TranslationOption(
                option_id=option_id,
                prompt=f"Consolidate the {definition.display_name!r} motif into a single task?",
                rationale=(
                    f"{definition.description} "
                    f"Affected activities: {', '.join(affected) if affected else '(none)'}.{confidence_suffix} "
                    "Keep preserves the activity-by-activity translation; consolidate replaces "
                    f"them with a single {definition.databricks_replacement!r} task."
                ),
                options=(
                    OptionChoice(
                        value=MotifConsolidate.KEEP.value,
                        label="Keep individual activities",
                        description="Preserves the per-activity translation; no motif collapse.",
                    ),
                    OptionChoice(
                        value=MotifConsolidate.CONSOLIDATE.value,
                        label="Consolidate into one task",
                        description=f"Replaces matched activities with a {definition.databricks_replacement!r} task.",
                    ),
                ),
                affected_task_keys=affected,
                default=MotifConsolidate.KEEP.value,
            )
        )
    return options


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
        going_to_lfc: ``True`` when the caller answered the LFC option
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
        going_to_lfc: ``True`` when the caller answered the LFC option
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


def _stamp_activity(activity: Activity, pipeline_configuration: TranslationConfiguration) -> Activity:
    """Stamps configuration-derived decisions onto an activity.

    Args:
        activity: Source activity from the IR.
        pipeline_configuration: Pipeline-wide configuration; per-task overrides
            apply via :meth:`TranslationConfiguration.effective_for`.

    Returns:
        A new activity instance with ``compute_mode``, ``target_format``,
        and motif-replacement updates applied as appropriate.  Control-
        flow activities are recursed into so their inner bodies are
        stamped too.
    """
    activity_configuration = pipeline_configuration.effective_for(activity.task_key)
    if isinstance(activity, ForEachActivity):
        return _stamp_for_each_activity(activity, pipeline_configuration, activity_configuration)
    if isinstance(activity, IfConditionActivity):
        return _stamp_if_condition_activity(activity, pipeline_configuration, activity_configuration)
    if isinstance(activity, SwitchActivity):
        return _stamp_switch_activity(activity, pipeline_configuration, activity_configuration)
    if isinstance(activity, CopyActivity):
        return _stamp_copy_activity(activity, activity_configuration)
    if isinstance(activity, MotifActivity):
        return _stamp_motif_activity(activity, activity_configuration)
    return dataclasses.replace(activity, compute_mode=_resolve_compute_mode(activity, activity_configuration))


def _stamp_for_each_activity(
    activity: ForEachActivity,
    pipeline_configuration: TranslationConfiguration,
    activity_configuration: TranslationConfiguration,
) -> ForEachActivity:
    """Stamps a ForEach activity and recurses into its inner body.

    Args:
        activity: Source ForEach activity.
        pipeline_configuration: Pipeline-wide configuration threaded into
            inner activities so they re-resolve their own overrides.
        activity_configuration: Configuration after per-task overrides for
            *activity*.

    Returns:
        A new :class:`ForEachActivity` with inner activities stamped.
    """
    return dataclasses.replace(
        activity,
        inner_activities=[_stamp_activity(inner, pipeline_configuration) for inner in activity.inner_activities],
        compute_mode=_resolve_compute_mode(activity, activity_configuration),
    )


def _stamp_if_condition_activity(
    activity: IfConditionActivity,
    pipeline_configuration: TranslationConfiguration,
    activity_configuration: TranslationConfiguration,
) -> IfConditionActivity:
    """Stamps an IfCondition activity and recurses into both branches.

    Args:
        activity: Source IfCondition activity.
        pipeline_configuration: Pipeline-wide configuration threaded into
            inner activities so they re-resolve their own overrides.
        activity_configuration: Configuration after per-task overrides for
            *activity*.

    Returns:
        A new :class:`IfConditionActivity` with both branches stamped.
    """
    return dataclasses.replace(
        activity,
        if_true_activities=[_stamp_activity(inner, pipeline_configuration) for inner in activity.if_true_activities],
        if_false_activities=[_stamp_activity(inner, pipeline_configuration) for inner in activity.if_false_activities],
        compute_mode=_resolve_compute_mode(activity, activity_configuration),
    )


def _stamp_switch_activity(
    activity: SwitchActivity,
    pipeline_configuration: TranslationConfiguration,
    activity_configuration: TranslationConfiguration,
) -> SwitchActivity:
    """Stamps a Switch activity and recurses into every case and the default.

    Args:
        activity: Source Switch activity.
        pipeline_configuration: Pipeline-wide configuration threaded into
            inner activities so they re-resolve their own overrides.
        activity_configuration: Configuration after per-task overrides for
            *activity*.

    Returns:
        A new :class:`SwitchActivity` with every case body stamped.
    """
    stamped_cases = [
        SwitchCase(
            value=case.value,
            activities=[_stamp_activity(inner, pipeline_configuration) for inner in case.activities],
        )
        for case in activity.cases
    ]
    return dataclasses.replace(
        activity,
        cases=stamped_cases,
        default_activities=[_stamp_activity(inner, pipeline_configuration) for inner in activity.default_activities],
        compute_mode=_resolve_compute_mode(activity, activity_configuration),
    )


def _stamp_copy_activity(
    activity: CopyActivity,
    activity_configuration: TranslationConfiguration,
) -> CopyActivity:
    """Stamps a Copy activity with paradigm, compute, and Lakeflow Connect flags.

    Args:
        activity: Source Copy activity.
        activity_configuration: Effective configuration for this activity.

    Returns:
        A new :class:`CopyActivity` whose ``target_format``,
        ``compute_mode``, and ``use_lakeflow_connector`` fields reflect
        the user's choices.  Copies whose query is unfit for LFC and
        SDP (joins, aggregates, etc.) are forced to the notebook
        paradigm regardless of configuration because the alternative
        paradigms cannot represent arbitrary SQL.
    """
    user_picked_lfc = activity_configuration.use_lakeflow_connectors is UseLakeflowConnectors.LAKEFLOW_CONNECT
    use_lakeflow_connector = user_picked_lfc and copy_eligible_for_any_lfc_connector(activity)
    paradigm = _resolve_paradigm(activity, activity_configuration, use_lakeflow_connector)
    connector_type = (
        _resolve_lakeflow_connector_type(activity, activity_configuration) if use_lakeflow_connector else None
    )
    return dataclasses.replace(
        activity,
        target_format=paradigm.value,
        compute_mode=_resolve_compute_mode(activity, activity_configuration),
        use_lakeflow_connector=use_lakeflow_connector,
        lakeflow_connector_type=connector_type,
    )


def _resolve_paradigm(
    activity: CopyActivity,
    activity_configuration: TranslationConfiguration,
    use_lakeflow_connector: bool,
) -> CopyActivityParadigm:
    """Resolves the paradigm (notebook vs SDP) for a Copy that won't go to LFC.

    Args:
        activity: Source Copy activity.
        activity_configuration: Effective configuration for this activity.
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
    return activity_configuration.copy_activity_paradigm


def _resolve_lakeflow_connector_type(activity: CopyActivity, activity_configuration: TranslationConfiguration) -> str:
    """Resolves which Lakeflow Connect connector to use for an eligible Copy.

    Args:
        activity: Source Copy activity (already known to be LFC-eligible).
        activity_configuration: Effective configuration for this activity.

    Returns:
        Always the connector flavour the Copy is actually eligible for.
        Query-based eligibility (parseable query + cursor column) wins
        over the user's CDC configuration because no cursor candidate
        exists for a CDC connector to use on a query-only Copy.
        Table-based Copies route to CDC because the query-based
        connector requires a cursor column and there is none.
    """
    if copy_eligible_for_lfc_query_based(activity):
        return LAKEFLOW_CONNECTOR_TYPE_QUERY_BASED
    return LakeflowConnectorType.CDC.value


def _stamp_motif_activity(
    activity: MotifActivity,
    activity_configuration: TranslationConfiguration,
) -> MotifActivity:
    """Stamps a Motif activity, swapping in Lakeflow Connect when eligible.

    Args:
        activity: Source motif activity.
        activity_configuration: Effective configuration for this activity.

    Returns:
        A new :class:`MotifActivity` whose ``databricks_replacement`` is
        set to ``lakeflow_connect_database`` when the motif represents a
        database ingestion and the user opted into Lakeflow Connect.
        Metadata-driven motifs also pick up the
        ``consolidate_metadata_driven`` flag when the user approved
        consolidation, granted access, and the size bucket is S or M.
    """
    qualifies_for_lakeflow_connect = (
        activity_configuration.use_lakeflow_connectors is UseLakeflowConnectors.LAKEFLOW_CONNECT
        and activity.source_type_hint == DATABASE_SOURCE_TYPE_HINT
    )
    replacement = LAKEFLOW_CONNECT_REPLACEMENT if qualifies_for_lakeflow_connect else activity.databricks_replacement
    notebook_template = (
        MOTIF_LAKEFLOW_CONNECT_DATABASE.notebook_template
        if qualifies_for_lakeflow_connect
        else activity.notebook_template
    )
    consolidate = _should_consolidate_metadata_driven(activity, activity_configuration)
    return dataclasses.replace(
        activity,
        databricks_replacement=replacement,
        notebook_template=notebook_template,
        compute_mode=_resolve_compute_mode(activity, activity_configuration),
        consolidate_metadata_driven=consolidate,
    )


def _should_consolidate_metadata_driven(
    activity: MotifActivity,
    activity_configuration: TranslationConfiguration,
) -> bool:
    """Returns True when the modifier should consolidate a metadata-driven motif.

    Args:
        activity: Source motif activity.
        activity_configuration: Effective configuration for this activity.

    Returns:
        ``True`` when the motif matches the metadata-driven bulk-copy
        pattern, the user opted to consolidate, granted access to the
        metadata source, and the configuration size is S or M.
        ``False`` otherwise -- including when the IR was not stamped by
        the metadata-driven prompts.
    """
    if activity.motif_id != "metadata_driven_bulk_copy":
        return False
    if activity_configuration.metadata_driven_consolidate is not MetadataDrivenConsolidate.CONSOLIDATE:
        return False
    if activity_configuration.metadata_driven_access is not MetadataDrivenAccess.YES:
        return False
    return activity_configuration.metadata_driven_size is not MetadataDrivenSize.LARGE


def _resolve_compute_mode(activity: Activity, activity_configuration: TranslationConfiguration) -> str:
    """Resolves the compute mode an activity should run on.

    Args:
        activity: Source activity.
        activity_configuration: Effective configuration for this activity.

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
    if activity_configuration.non_databricks_task_compute is NonDatabricksTaskCompute.SERVERLESS:
        return COMPUTE_MODE_SERVERLESS
    if isinstance(activity, CopyActivity):
        return COMPUTE_MODE_CLASSIC_MULTI_NODE
    return COMPUTE_MODE_CLASSIC_SINGLE_NODE
