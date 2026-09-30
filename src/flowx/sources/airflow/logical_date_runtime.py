"""Airflow logical-date arithmetic evaluated at run time by the generated date-resolver task.

Databricks dynamic value references cannot do date arithmetic, so a migrated job computes Airflow's
``{{ ds }}`` family in a small generated first task. That notebook embeds this module's source
verbatim, which is why it uses only the standard library and reads no flowx state.

Airflow renders every interval macro in UTC from one logical instant. Which instant that is depends on
the DAG's timetable: a data-interval timetable (the Airflow 2 cron/preset and ``timedelta`` default)
names the start of the interval that just closed, one schedule tick before the run fires, while a
trigger timetable (the Airflow 3 raw-cron default) names the fire time itself. Manual runs always use
the trigger time.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

DATA_INTERVAL_CRON = "data_interval_cron"
DATA_INTERVAL_DELTA = "data_interval_delta"
TRIGGER_CRON = "trigger_cron"
TRIGGER_DELTA = "trigger_delta"
MANUAL_ONLY = "manual_only"
RESOLVABLE_SEMANTICS = frozenset({DATA_INTERVAL_CRON, DATA_INTERVAL_DELTA, TRIGGER_CRON, TRIGGER_DELTA, MANUAL_ONLY})

# Databricks trigger types that correspond to an Airflow manual or triggered-DAG run.
MANUAL_TRIGGER_TYPES = frozenset({"one_time", "run_job_task"})

CRON_PRESETS = {
    "@hourly": "0 * * * *",
    "@daily": "0 0 * * *",
    "@midnight": "0 0 * * *",
    "@weekly": "0 0 * * 0",
    "@monthly": "0 0 1 * *",
    "@quarterly": "0 0 1 */3 *",
    "@yearly": "0 0 1 1 *",
    "@annually": "0 0 1 1 *",
}

_MONTH_NAMES = {
    name: number
    for number, name in enumerate(
        ["jan", "feb", "mar", "apr", "may", "jun", "jul", "aug", "sep", "oct", "nov", "dec"], start=1
    )
}
_DAY_NAMES = {name: number for number, name in enumerate(["sun", "mon", "tue", "wed", "thu", "fri", "sat"])}

# A tick search that walks further than this has no reachable tick (for example ``0 0 31 2 *``).
_MAXIMUM_SEARCH_SPAN = timedelta(days=366 * 30)


def _parse_value(token: str, names: dict[str, int]) -> int:
    lowered = token.lower()
    if lowered in names:
        return names[lowered]
    if not token.isdigit():
        raise ValueError(f"invalid cron value {token!r}")
    return int(token)


def _parse_field(field: str, low: int, high: int, names: dict[str, int]) -> frozenset[int]:
    values: set[int] = set()
    for part in field.split(","):
        base, _, step_text = part.partition("/")
        step = int(step_text) if step_text else 1
        if step < 1:
            raise ValueError(f"invalid cron step in {part!r}")
        if base == "*":
            start, end = low, high
        elif "-" in base:
            start_text, _, end_text = base.partition("-")
            start, end = _parse_value(start_text, names), _parse_value(end_text, names)
        else:
            start = _parse_value(base, names)
            end = high if step_text else start
        if not (low <= start <= high and low <= end <= high and start <= end):
            raise ValueError(f"cron field {field!r} is out of range {low}-{high}")
        values.update(range(start, end + 1, step))
    return frozenset(values)


_ALL_HOURS = frozenset(range(24))
_ALL_DAYS = frozenset(range(1, 32))
_ALL_WEEKDAYS = frozenset(range(7))


class CronSchedule:
    """A five-field Unix cron expression evaluated the way Airflow evaluates it through croniter.

    A day-of-month or day-of-week field is unrestricted when it is written as a bare ``*``, or when it
    lists every value and the other day field is written with a ``*``; this is croniter's expansion
    rule, so ``*/2`` in either day field is restricted. When both day fields are restricted, a day
    matches if either field matches.
    """

    def __init__(self, expression: str) -> None:
        fields = CRON_PRESETS.get(expression.strip().lower(), expression).split()
        if len(fields) != 5:
            raise ValueError(f"expected a five-field cron expression, got {expression!r}")
        self.expression = expression
        self.minutes = _parse_field(fields[0], 0, 59, {})
        self.hours = _parse_field(fields[1], 0, 23, {})
        self.days = _parse_field(fields[2], 1, 31, {})
        self.months = _parse_field(fields[3], 1, 12, _MONTH_NAMES)
        self.weekdays = frozenset(day % 7 for day in _parse_field(fields[4], 0, 7, _DAY_NAMES))
        self.days_unrestricted = fields[2] == "*" or (self.days == _ALL_DAYS and "*" in fields[4])
        self.weekdays_unrestricted = fields[4] == "*" or (self.weekdays == _ALL_WEEKDAYS and "*" in fields[2])
        self.covers_every_hour = self.hours == _ALL_HOURS

    def day_matches(self, moment: datetime) -> bool:
        """Returns whether *moment*'s calendar day can hold a tick."""
        if moment.month not in self.months:
            return False
        day_ok = moment.day in self.days
        weekday_ok = (moment.weekday() + 1) % 7 in self.weekdays
        if self.days_unrestricted or self.weekdays_unrestricted:
            return day_ok and weekday_ok
        return day_ok or weekday_ok


def _wall_clock_previous(schedule: CronSchedule, wall_clock: datetime) -> datetime:
    """Returns the latest matching naive wall-clock minute strictly before *wall_clock*."""
    candidate = wall_clock.replace(second=0, microsecond=0)
    if candidate == wall_clock:
        candidate -= timedelta(minutes=1)
    floor = candidate - _MAXIMUM_SEARCH_SPAN
    while candidate >= floor:
        if not schedule.day_matches(candidate):
            candidate = candidate.replace(hour=23, minute=59) - timedelta(days=1)
        elif candidate.hour not in schedule.hours:
            candidate = candidate.replace(minute=59) - timedelta(hours=1)
        elif candidate.minute not in schedule.minutes:
            candidate -= timedelta(minutes=1)
        else:
            return candidate
    raise RuntimeError(f"cron {schedule.expression!r} has no tick before {wall_clock.isoformat()}")


def _wall_clock_next(schedule: CronSchedule, wall_clock: datetime) -> datetime:
    """Returns the earliest matching naive wall-clock minute strictly after *wall_clock*."""
    candidate = wall_clock.replace(second=0, microsecond=0) + timedelta(minutes=1)
    ceiling = candidate + _MAXIMUM_SEARCH_SPAN
    while candidate <= ceiling:
        if not schedule.day_matches(candidate):
            candidate = candidate.replace(hour=0, minute=0) + timedelta(days=1)
        elif candidate.hour not in schedule.hours:
            candidate = candidate.replace(minute=0) + timedelta(hours=1)
        elif candidate.minute not in schedule.minutes:
            candidate += timedelta(minutes=1)
        else:
            return candidate
    raise RuntimeError(f"cron {schedule.expression!r} has no tick after {wall_clock.isoformat()}")


def _to_wall_clock(instant: datetime, zone: ZoneInfo) -> datetime:
    return instant.astimezone(zone).replace(tzinfo=None)


def _localize(wall_clock: datetime, zone: ZoneInfo) -> datetime:
    """Attaches *zone* to a naive wall-clock time exactly as Airflow's ``make_aware`` does.

    Airflow marks the time as the second occurrence and lets pendulum resolve it: a time skipped by a
    daylight-saving jump moves forward by the length of the jump, and a repeated time is its second
    occurrence.
    """
    offset_before = wall_clock.replace(tzinfo=zone, fold=0).utcoffset()
    offset_after = wall_clock.replace(tzinfo=zone, fold=1).utcoffset()
    if offset_before is not None and offset_after is not None and offset_after > offset_before:
        wall_clock += offset_after - offset_before
    return wall_clock.replace(tzinfo=zone, fold=1).astimezone(timezone.utc)


def _shift(instant: datetime, delta: timedelta, zone: ZoneInfo) -> datetime:
    """Adds *delta* to *instant* the way a pendulum datetime does.

    A change shorter than a day is exact elapsed time; one with whole days is applied to the local
    wall clock and then localized again.
    """
    if abs(delta).days == 0:
        return (instant + delta).astimezone(timezone.utc)
    return _localize(_to_wall_clock(instant, zone) + delta, zone)


def previous_tick(schedule: CronSchedule, instant: datetime, zone: ZoneInfo) -> datetime:
    """Returns the tick before *instant*, in UTC (Airflow's ``CronMixin._get_prev``)."""
    wall_clock = _to_wall_clock(instant, zone)
    scheduled = _wall_clock_previous(schedule, wall_clock)
    if not schedule.covers_every_hour:
        return _localize(scheduled, zone)
    return _shift(instant, -(wall_clock - scheduled), zone)


def next_tick(schedule: CronSchedule, instant: datetime, zone: ZoneInfo) -> datetime:
    """Returns the tick after *instant*, in UTC (Airflow's ``CronMixin._get_next``)."""
    wall_clock = _to_wall_clock(instant, zone)
    scheduled = _wall_clock_next(schedule, wall_clock)
    if not schedule.covers_every_hour:
        return _localize(scheduled, zone)
    return _shift(instant, scheduled - wall_clock, zone)


def latest_tick_at_or_before(schedule: CronSchedule, instant: datetime, zone: ZoneInfo) -> datetime:
    """Returns *instant* when it is a tick, otherwise the tick before it (Airflow's ``_align_to_prev``)."""
    previous = previous_tick(schedule, instant, zone)
    return instant if next_tick(schedule, previous, zone) == instant else previous


def scheduled_interval_start(schedule: CronSchedule, end: datetime, zone: ZoneInfo) -> datetime:
    """Returns the tick whose next tick is *end*, the start of the scheduled interval ending at *end*.

    Airflow chains scheduled intervals forward, each ending at the next tick after its start, so this
    is the tick before *end* in that chain. It differs from :func:`previous_tick` only when *end* is a
    time moved forward by a daylight-saving jump, where Airflow's own previous-tick lookup returns *end*.
    """
    start = previous_tick(schedule, end, zone)
    if start >= end:
        start = previous_tick(schedule, end - timedelta(minutes=1), zone)
    return start


def nearest_tick(schedule: CronSchedule, instant: datetime, zone: ZoneInfo) -> datetime:
    """Returns the tick closest to a scheduled run's fire instant, preferring the earlier one on a tie.

    A scheduled run fires on a tick, but its reported trigger time can lag by a few seconds, and on a
    daylight-saving jump the scheduler may fire before the tick Airflow would have produced.
    """
    earlier = latest_tick_at_or_before(schedule, instant, zone)
    if earlier == instant:
        return earlier
    later = next_tick(schedule, instant, zone)
    return later if later - instant < instant - earlier else earlier


def parse_instant(value: str) -> datetime:
    """Parses an ISO date or datetime; a value without an offset is read as UTC."""
    text = value.strip()
    if not text:
        raise ValueError("an empty instant cannot be resolved")
    if text.endswith(("Z", "z")):
        text = text[:-1] + "+00:00"
    parsed = datetime.fromisoformat(text)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _render_timestamp(moment: datetime) -> str:
    return moment.astimezone(timezone.utc).isoformat()


def _render_date(moment: datetime) -> str:
    return moment.astimezone(timezone.utc).strftime("%Y-%m-%d")


def _render_date_nodash(moment: datetime) -> str:
    return moment.astimezone(timezone.utc).strftime("%Y%m%d")


def resolve(
    *,
    semantics: str,
    trigger_time: str,
    trigger_type: str,
    override: str = "",
    cron: str | None = None,
    zone_name: str = "UTC",
    delta_seconds: int = 0,
    publish_neighbors: bool = False,
) -> dict[str, str]:
    """Computes the Airflow interval macros for one Databricks run, rendered as Airflow renders them.

    Args:
        semantics: The DAG's timetable family (one of ``RESOLVABLE_SEMANTICS``).
        trigger_time: The run's fire instant, normally ``{{job.trigger.time.iso_datetime}}``; a native
            backfill overrides it with ``{{backfill.iso_datetime}}``.
        trigger_type: ``{{job.trigger.type}}``; manual and triggered-job runs keep the trigger time as
            their logical date, as Airflow does for manual runs.
        override: An explicit logical date for an exact partition replay. When set it wins and is not
            shifted.
        cron: The source cron expression or preset, for cron semantics.
        zone_name: The DAG timezone that cron ticks are evaluated in.
        delta_seconds: The ``timedelta`` schedule length, for delta semantics.
        publish_neighbors: Whether to publish ``prev_ds`` / ``next_ds`` (Airflow 2 only).

    Returns:
        Macro name -> rendered string.
    """
    if semantics not in RESOLVABLE_SEMANTICS:
        raise ValueError(f"unsupported logical-date semantics {semantics!r}")
    zone = ZoneInfo(zone_name)
    uses_cron = semantics in (DATA_INTERVAL_CRON, TRIGGER_CRON)
    schedule = CronSchedule(cron or "") if uses_cron else None
    delta = timedelta(seconds=delta_seconds)
    if semantics == DATA_INTERVAL_DELTA and delta <= timedelta(0):
        raise ValueError("a delta timetable needs a positive interval")

    if override.strip():
        logical = parse_instant(override)
        start = logical
        if semantics == DATA_INTERVAL_CRON and schedule is not None:
            end = next_tick(schedule, logical, zone)
        elif semantics == DATA_INTERVAL_DELTA:
            end = logical + delta
        else:
            end = logical
    else:
        fired = parse_instant(trigger_time)
        manual = trigger_type.strip().lower() in MANUAL_TRIGGER_TYPES
        if semantics == DATA_INTERVAL_CRON and schedule is not None:
            if manual:
                end = latest_tick_at_or_before(schedule, fired, zone)
                start = previous_tick(schedule, end, zone)
            else:
                end = nearest_tick(schedule, fired, zone)
                start = scheduled_interval_start(schedule, end, zone)
            logical = fired if manual else start
        elif semantics == DATA_INTERVAL_DELTA:
            start, end = fired - delta, fired
            logical = fired if manual else start
        else:
            logical = start = end = fired

    values = {
        "ds": _render_date(logical),
        "ds_nodash": _render_date_nodash(logical),
        "ts": _render_timestamp(logical),
        "ts_nodash": logical.astimezone(timezone.utc).strftime("%Y%m%dT%H%M%S"),
        "logical_date": _render_timestamp(logical),
        "execution_date": _render_timestamp(logical),
        "data_interval_start": _render_timestamp(start),
        "data_interval_end": _render_timestamp(end),
    }
    if publish_neighbors:
        if semantics == DATA_INTERVAL_CRON and schedule is not None:
            previous_logical = previous_tick(schedule, logical, zone)
            next_logical = next_tick(schedule, logical, zone)
        elif semantics == DATA_INTERVAL_DELTA:
            previous_logical, next_logical = logical - delta, logical + delta
        else:
            raise ValueError("prev_ds and next_ds need a data-interval timetable")
        values.update(
            {
                "prev_ds": _render_date(previous_logical),
                "next_ds": _render_date(next_logical),
                "prev_ds_nodash": _render_date_nodash(previous_logical),
                "next_ds_nodash": _render_date_nodash(next_logical),
            }
        )
    return values
