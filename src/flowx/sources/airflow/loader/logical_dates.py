"""Airflow logical-date semantics for one DAG, and the generated task that resolves them at run time.

Airflow's interval macros (``ds``, ``ts``, ``data_interval_start``, ...) come from one logical instant
whose relation to the fire time depends on the Airflow version and timetable. This module decides, from
source alone, which timetable family a DAG uses, and builds the notebook task that computes the macros
for each Databricks run. When the family cannot be decided statically, consumers become gaps instead of
guessing a date.
"""

from __future__ import annotations

import ast
import dataclasses
import inspect
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from flowx.models.ir import Activity, NotebookActivity
from flowx.sources.airflow import logical_date_runtime as runtime
from flowx.sources.airflow import operators as ops
from flowx.sources.airflow import templating
from flowx.sources.airflow.loader.ast_utils import airflow_generation
from flowx.sources.airflow.loader.schedule import _extract_timezone, _timedelta_seconds

EVENT = "event"
UNDETERMINABLE = "undeterminable"

_EVENT_SCHEDULE_KINDS = frozenset({"file_arrival", "table_update", "continuous"})
_TIMETABLE_NAMES = frozenset(
    {"CronDataIntervalTimetable", "CronTriggerTimetable", "DeltaDataIntervalTimetable", "DeltaTriggerTimetable"}
)


@dataclass(frozen=True, slots=True, kw_only=True)
class LogicalDateSemantics:
    """How one DAG's interval macros relate to a Databricks run's trigger instant.

    Attributes:
        kind: A ``logical_date_runtime`` semantics constant, ``event`` (no scheduled instant), or
            ``undeterminable``.
        generation: ``"2"``, ``"3"``, ``"1.10"``, or ``"unknown"`` as inferred from source.
        cron: The five-field cron or preset, for cron semantics.
        timezone: The DAG timezone cron ticks are evaluated in.
        delta_seconds: The interval length, for delta semantics.
        reason: Why the semantics were chosen, for the reconciliation ledger or the gap message.
        disclosures: Assumptions a reviewer must confirm, as ``(code, message)`` pairs.
    """

    kind: str
    generation: str
    cron: str | None = None
    timezone: str = "UTC"
    delta_seconds: int = 0
    anchor_time: str = ""
    reason: str = ""
    disclosures: tuple[tuple[str, str], ...] = ()

    @property
    def resolvable(self) -> bool:
        """Whether a run-time resolver can reproduce Airflow's rendering for this DAG."""
        return self.kind in runtime.RESOLVABLE_SEMANTICS

    @property
    def publishes_neighbors(self) -> bool:
        """Whether ``prev_ds`` / ``next_ds`` exist: Airflow 2 (or 1.10) data-interval timetables only."""
        return self.generation in ("2", "1.10") and self.kind in (
            runtime.DATA_INTERVAL_CRON,
            runtime.DATA_INTERVAL_DELTA,
        )

    def unavailable_reason(self, value_names: set[str]) -> str | None:
        """Returns why some of *value_names* cannot be resolved for this DAG, or None when all can."""
        if not self.resolvable:
            return self.reason
        neighbors = value_names & templating.LOGICAL_DATE_NEIGHBOR_VALUES
        if neighbors and not self.publishes_neighbors:
            return (
                f"{', '.join(sorted(neighbors))} exist only for Airflow 2 data-interval timetables "
                f"(this DAG: Airflow {self.generation}, {self.kind})"
            )
        return None


def _valid_cron(expression: str) -> bool:
    try:
        runtime.CronSchedule(expression)
    except ValueError:
        return False
    return True


def _timetable_semantics(call: ast.Call, generation: str, timezone: str) -> LogicalDateSemantics:
    name = call.func.attr if isinstance(call.func, ast.Attribute) else getattr(call.func, "id", "")
    arguments = {keyword.arg: keyword.value for keyword in call.keywords if keyword.arg}
    first = call.args[0] if call.args else None
    timezone_node = arguments.get("timezone")
    if timezone_node is None and name == "CronDataIntervalTimetable" and len(call.args) > 1:
        timezone_node = call.args[1]
    explicit_zone = _extract_timezone(timezone_node)
    if timezone_node is not None and explicit_zone is None:
        return LogicalDateSemantics(
            kind=UNDETERMINABLE, generation=generation, reason=f"{name} timezone is not a literal IANA zone"
        )
    zone = explicit_zone or timezone
    if name == "timedelta":
        seconds = _timedelta_seconds(call)
        if seconds <= 0:
            return LogicalDateSemantics(kind=UNDETERMINABLE, generation=generation, reason="non-literal timedelta")
        if generation in ("2", "1.10"):
            return LogicalDateSemantics(
                kind=runtime.DATA_INTERVAL_DELTA,
                generation=generation,
                delta_seconds=seconds,
                timezone=zone,
                reason=f"Airflow {generation} timedelta schedule uses DeltaDataIntervalTimetable",
            )
        if generation == "3":
            return LogicalDateSemantics(
                kind=runtime.TRIGGER_DELTA,
                generation=generation,
                delta_seconds=seconds,
                timezone=zone,
                reason="Airflow 3 timedelta schedule uses DeltaTriggerTimetable",
                disclosures=(
                    (
                        "airflow3_delta_data_intervals_assumed_false",
                        "Airflow 3 renders a timedelta schedule with DeltaTriggerTimetable unless the deployment "
                        "enables data intervals in its [scheduler] settings (create_delta_data_intervals, which "
                        "current Airflow releases read through create_cron_data_intervals), and that setting is not "
                        "visible in DAG source. flowx assumed the default (False): ds is the fire date, not the "
                        "previous interval.",
                    ),
                ),
            )
        return LogicalDateSemantics(
            kind=UNDETERMINABLE,
            generation=generation,
            reason=(
                "the Airflow version cannot be determined from source, and a timedelta schedule's logical date is "
                "one interval behind the fire time on Airflow 2 but equals it on Airflow 3"
            ),
        )
    if name == "DeltaTriggerTimetable":
        return LogicalDateSemantics(
            kind=UNDETERMINABLE,
            generation=generation,
            reason="DeltaTriggerTimetable fire times depend on the deployment's first run, not on source",
        )
    if name == "DeltaDataIntervalTimetable":
        seconds = _timedelta_seconds(first if first is not None else arguments.get("delta"))
        if seconds <= 0:
            return LogicalDateSemantics(kind=UNDETERMINABLE, generation=generation, reason=f"non-literal {name}")
        return LogicalDateSemantics(
            kind=runtime.DATA_INTERVAL_DELTA,
            generation=generation,
            delta_seconds=seconds,
            timezone=zone,
            reason=f"explicit {name}",
        )
    cron = ops.literal_str(first if first is not None else arguments.get("cron"))
    if cron is None or not _valid_cron(cron):
        return LogicalDateSemantics(kind=UNDETERMINABLE, generation=generation, reason=f"non-literal {name} cron")
    if name == "CronTriggerTimetable" and "interval" in arguments:
        return LogicalDateSemantics(
            kind=UNDETERMINABLE,
            generation=generation,
            reason="CronTriggerTimetable with an explicit interval has no static data-interval mapping",
        )
    kind = runtime.DATA_INTERVAL_CRON if name == "CronDataIntervalTimetable" else runtime.TRIGGER_CRON
    return LogicalDateSemantics(kind=kind, generation=generation, cron=cron, timezone=zone, reason=f"explicit {name}")


def classify(
    module: ast.Module,
    *,
    dag_kwargs: dict[str, ast.expr],
    schedule_node: ast.expr | None,
    schedule_interval: str | None,
    timezone: str | None,
    schedule: dict[str, object] | None,
    start_date: datetime | None = None,
) -> LogicalDateSemantics:
    """Decides which timetable family governs this DAG's interval macros, from source alone.

    A timedelta schedule is only resolvable when its Databricks schedule fires on Airflow's interval
    boundaries, which needs the Quartz cron anchored to a literal ``start_date``. A periodic trigger
    fires relative to when the job was deployed, so its runs cannot be mapped back to Airflow intervals.
    """
    semantics = _classify_timetable(
        module,
        dag_kwargs=dag_kwargs,
        schedule_node=schedule_node,
        schedule_interval=schedule_interval,
        timezone=timezone,
        schedule=schedule,
    )
    if semantics.kind not in (runtime.DATA_INTERVAL_DELTA, runtime.TRIGGER_DELTA) or schedule is None:
        return semantics
    if schedule.get("kind") != "schedule" or start_date is None:
        return LogicalDateSemantics(
            kind=UNDETERMINABLE,
            generation=semantics.generation,
            reason=(
                "the timedelta schedule became a Databricks periodic trigger, which fires relative to when the "
                "job was deployed rather than on Airflow's interval boundaries from start_date"
            ),
        )
    return dataclasses.replace(semantics, anchor_time=start_date.isoformat())


def _classify_timetable(
    module: ast.Module,
    *,
    dag_kwargs: dict[str, ast.expr],
    schedule_node: ast.expr | None,
    schedule_interval: str | None,
    timezone: str | None,
    schedule: dict[str, object] | None,
) -> LogicalDateSemantics:
    generation, evidence = airflow_generation(module, dag_kwargs)
    zone = timezone or "UTC"
    if schedule is not None and schedule.get("kind") in _EVENT_SCHEDULE_KINDS:
        return LogicalDateSemantics(
            kind=EVENT,
            generation=generation,
            reason=(
                f"the job is {schedule.get('kind')}-triggered, so a run has no scheduled logical instant; "
                "derive the date from the triggering event instead"
            ),
        )
    has_schedule_argument = "schedule" in dag_kwargs or "schedule_interval" in dag_kwargs
    if isinstance(schedule_node, ast.Constant) and schedule_node.value is None:
        return LogicalDateSemantics(kind=runtime.MANUAL_ONLY, generation=generation, reason="schedule=None")
    if isinstance(schedule_node, ast.Call):
        name = schedule_node.func.attr if isinstance(schedule_node.func, ast.Attribute) else ""
        name = name or getattr(schedule_node.func, "id", "")
        if name == "timedelta" or name in _TIMETABLE_NAMES:
            return _timetable_semantics(schedule_node, generation, zone)
        return LogicalDateSemantics(
            kind=UNDETERMINABLE, generation=generation, reason=f"custom timetable {name or ast.unparse(schedule_node)}"
        )
    if schedule_interval is not None:
        preset = schedule_interval.strip()
        if preset == "@once":
            return LogicalDateSemantics(kind=runtime.MANUAL_ONLY, generation=generation, reason="@once")
        if not _valid_cron(preset):
            return LogicalDateSemantics(
                kind=UNDETERMINABLE, generation=generation, reason=f"schedule {preset!r} is not a five-field cron"
            )
        if generation in ("2", "1.10"):
            return LogicalDateSemantics(
                kind=runtime.DATA_INTERVAL_CRON,
                generation=generation,
                cron=preset,
                timezone=zone,
                reason=f"Airflow {generation} cron schedule ({evidence}) uses a data-interval timetable",
            )
        if generation == "3":
            return LogicalDateSemantics(
                kind=runtime.TRIGGER_CRON,
                generation=generation,
                cron=preset,
                timezone=zone,
                reason=f"Airflow 3 cron schedule ({evidence}) uses CronTriggerTimetable",
                disclosures=(
                    (
                        "airflow3_cron_data_intervals_assumed_false",
                        "Airflow 3 renders a raw cron schedule with CronTriggerTimetable unless the deployment "
                        "sets [scheduler] create_cron_data_intervals = True, which is not visible in DAG source. "
                        "flowx assumed the default (False): ds is the fire date, not the previous interval.",
                    ),
                ),
            )
        return LogicalDateSemantics(
            kind=UNDETERMINABLE,
            generation=generation,
            reason=(
                f"the Airflow version cannot be determined from source ({evidence}), and a cron schedule's "
                "logical date is one interval behind the fire time on Airflow 2 but equals it on Airflow 3"
            ),
        )
    if schedule_node is not None:
        return LogicalDateSemantics(
            kind=UNDETERMINABLE, generation=generation, reason=f"non-literal schedule {ast.unparse(schedule_node)}"
        )
    if not has_schedule_argument:
        if generation == "2":
            return LogicalDateSemantics(
                kind=runtime.DATA_INTERVAL_DELTA,
                generation=generation,
                delta_seconds=86400,
                timezone=zone,
                reason="Airflow 2 default schedule timedelta(days=1)",
                disclosures=(
                    (
                        "airflow2_default_schedule_assumed",
                        "The DAG sets no schedule, so Airflow 2 ran it daily with a timedelta(days=1) data "
                        "interval. flowx emits no Databricks schedule for it; interval macros resolve with that "
                        "timetable for manual and backfill runs.",
                    ),
                ),
            )
        if generation == "3":
            return LogicalDateSemantics(
                kind=runtime.MANUAL_ONLY, generation=generation, reason="Airflow 3 default None"
            )
    return LogicalDateSemantics(
        kind=UNDETERMINABLE,
        generation=generation,
        reason=f"the DAG's default schedule depends on the Airflow version, which is {generation}",
    )


def _strings(value: Any) -> Iterator[str]:
    """Yields every string inside *value*: dataclass fields, mapping keys and values, and list items."""
    if isinstance(value, str):
        yield value
    elif dataclasses.is_dataclass(value) and not isinstance(value, type):
        for field in dataclasses.fields(value):
            yield from _strings(getattr(value, field.name))
    elif isinstance(value, dict):
        for key, item in value.items():
            yield from _strings(key)
            yield from _strings(item)
    elif isinstance(value, (list, tuple, set, frozenset)):
        for item in value:
            yield from _strings(item)


def logical_date_values(activity: Activity) -> set[str]:
    """Returns the resolver task values *activity* (including nested tasks) reads."""
    return {name for text in _strings(activity) for name in templating.LOGICAL_DATE_VALUE_REF.findall(text)}


def resolver_source(semantics: LogicalDateSemantics) -> str:
    """Renders the resolver notebook: the runtime module verbatim, the DAG's timetable, then one call."""
    configuration = {
        "semantics": semantics.kind,
        "cron": semantics.cron,
        "zone_name": semantics.timezone,
        "delta_seconds": semantics.delta_seconds,
        "anchor_time": semantics.anchor_time,
        "publish_neighbors": semantics.publishes_neighbors,
    }
    main = (
        "\n\n# Timetable captured from the Airflow DAG source.\n"
        f"_TIMETABLE = {configuration!r}\n\n\n"
        "def _publish() -> None:\n"
        '    """Resolves this run\'s Airflow interval macros and publishes them as task values."""\n'
        "    for name in ('trigger_time', 'trigger_type', 'logical_date_override'):\n"
        "        dbutils.widgets.text(name, '')\n"
        "    values = resolve(\n"
        "        trigger_time=dbutils.widgets.get('trigger_time'),\n"
        "        trigger_type=dbutils.widgets.get('trigger_type'),\n"
        "        override=dbutils.widgets.get('logical_date_override'),\n"
        "        **_TIMETABLE,\n"
        "    )\n"
        "    for name, value in values.items():\n"
        "        dbutils.jobs.taskValues.set(key=name, value=value)\n"
        "    print(values)\n\n\n"
        "if 'dbutils' in globals():\n"
        "    _publish()\n"
    )
    return "# Databricks notebook source\n" + inspect.getsource(runtime) + main


def build_resolver(semantics: LogicalDateSemantics) -> NotebookActivity:
    """Builds the generated first task that publishes this DAG's interval macros."""
    key = templating.LOGICAL_DATE_RESOLVER_TASK_KEY
    return NotebookActivity(
        name="Resolve Airflow logical dates",
        task_key=key,
        notebook_path=f"notebooks/{key}.py",
        generated_source=resolver_source(semantics),
        base_parameters={
            "trigger_time": "{{job.parameters." + templating.LOGICAL_DATE_TRIGGER_TIME_PARAMETER + "}}",
            "trigger_type": "{{job.parameters." + templating.LOGICAL_DATE_TRIGGER_TYPE_PARAMETER + "}}",
            "logical_date_override": "{{job.parameters." + templating.LOGICAL_DATE_OVERRIDE_PARAMETER + "}}",
        },
    )


def resolver_proof(semantics: LogicalDateSemantics, consumers: list[str]) -> dict[str, Any]:
    """Returns the ledger entry for the synthetic resolver task; reconcile adds the edges it attaches."""
    return {
        "code": "logical_date_resolver_emitted",
        "task_key": templating.LOGICAL_DATE_RESOLVER_TASK_KEY,
        "semantics": semantics.kind,
        "airflow_generation": semantics.generation,
        "cron": semantics.cron,
        "timezone": semantics.timezone,
        "delta_seconds": semantics.delta_seconds,
        "rationale": semantics.reason,
        "consumer_task_keys": sorted(consumers),
    }
