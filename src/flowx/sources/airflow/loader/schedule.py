"""Airflow schedule handling: cron->Quartz, timedelta, timezone, Asset/Dataset triggers."""

from __future__ import annotations

import ast

from flowx.sources.airflow import operators as ops
from flowx.sources.airflow.loader.ast_utils import _construct_name

_CRON_PRESETS: dict[str, str] = {
    "@hourly": "0 0 * * * ?",
    "@daily": "0 0 0 * * ?",
    "@midnight": "0 0 0 * * ?",
    "@weekly": "0 0 0 ? * SUN",
    "@monthly": "0 0 0 1 * ?",
    "@yearly": "0 0 0 1 1 ?",
    "@annually": "0 0 0 1 1 ?",
}


_TIMEDELTA_UNIT_SECONDS: dict[str, int] = {
    "weeks": 604800,
    "days": 86400,
    "hours": 3600,
    "minutes": 60,
    "seconds": 1,
}


def _shift_weekday_field(dow: str) -> str:
    """Shifts Unix-cron day-of-week numbering (0-6, Sun=0) to Quartz (1-7, Sun=1).

    Airflow/Unix: 0=Sun..6=Sat (7 also = Sun). Quartz: 1=Sun..7=Sat. Each numeric token is
    shifted +1, with 7 -> 1. Ranges/lists/steps (e.g. ``1-5``, ``0,3``, ``*/2``) have their
    numeric components shifted individually; ``*`` / ``?`` and named days pass through.
    """

    def _shift_token(token: str) -> str:
        if token.isdigit():
            n = int(token)
            return "1" if n == 7 else str(n + 1) if 0 <= n <= 6 else token
        return token

    # Split on commas (lists), then on '/' (steps) and '-' (ranges), shifting numeric pieces.
    def _shift_part(part: str) -> str:
        step = ""
        if "/" in part:
            part, _, step = part.partition("/")
            step = "/" + step
        if "-" in part:
            lo, _, hi = part.partition("-")
            shifted_lo, shifted_hi = _shift_token(lo), _shift_token(hi)
            # A range that wraps the week in Unix numbering (e.g. 5-0, Fri-Sun) shifts to a descending
            # range Quartz reads as empty; split it at the week boundary instead (6-7,1).
            if shifted_lo.isdigit() and shifted_hi.isdigit() and int(shifted_lo) > int(shifted_hi):
                head = shifted_lo if shifted_lo == "7" else f"{shifted_lo}-7"
                tail = shifted_hi if shifted_hi == "1" else f"1-{shifted_hi}"
                return f"{head},{tail}{step}"
            return f"{shifted_lo}-{shifted_hi}{step}"
        return f"{_shift_token(part)}{step}"

    return ",".join(_shift_part(p) for p in dow.split(","))


def _cron_to_quartz(cron: str) -> str | None:
    """Converts a 5-field Unix cron to a 6-field Quartz expression.

    Quartz is ``second minute hour day-of-month month day-of-week``; Unix cron
    is ``minute hour day-of-month month day-of-week``. Prepend the seconds
    field, shift the day-of-week from Unix (0-6) to Quartz (1-7) numbering, and
    reconcile the day-of-month / day-of-week wildcard (Quartz rejects ``*`` in
    both simultaneously -- one must be ``?``).
    """
    fields = cron.split()
    if len(fields) != 5:
        return None
    minute, hour, dom, month, dow = fields
    if dow not in ("*", "?"):
        dow = _shift_weekday_field(dow)
    if dom != "*" and dow not in ("*", "?"):
        # Unix cron ORs a restricted day-of-month with a restricted day-of-week; Quartz cannot express
        # both (it rejects the expression outright). Keep the day-of-week and drop the day-of-month so
        # the job is still valid -- narrower than the Airflow schedule, and flagged for review.
        dom = "?"
    elif dow == "*" and dom != "*":
        dow = "?"
    elif dom == "*":
        dom = "?"
    return f"0 {minute} {hour} {dom} {month} {dow}"


def _extract_timezone(node: ast.expr | None) -> str | None:
    """Extracts an IANA timezone from a ``pendulum.timezone("…")`` call or a tz string kwarg.

    Handles ``start_date=datetime(..., tzinfo=pendulum.timezone("Europe/Madrid"))``,
    ``timezone="Europe/Madrid"``, and ``pendulum.timezone("…")`` directly. Returns None
    when no literal timezone is present (caller falls back to UTC).
    """
    if node is None:
        return None
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        return node.value
    if isinstance(node, ast.Call):
        func = node.func
        name = func.attr if isinstance(func, ast.Attribute) else (func.id if isinstance(func, ast.Name) else "")
        if name in ("timezone", "timezone_") and node.args:
            return ops.literal_str(node.args[0])
        # datetime(..., tzinfo=pendulum.timezone("…")) / tz=...
        for kw in node.keywords:
            if kw.arg in ("tzinfo", "tz"):
                return _extract_timezone(kw.value)
    return None


def _timedelta_to_periodic(node: ast.expr | None) -> dict[str, object] | None:
    """Maps a ``timedelta(...)`` schedule to a ``trigger.periodic`` spec.

    Databricks periodic units are DAYS/HOURS/WEEKS. A timedelta of whole weeks/days/hours
    maps to the largest exact unit; anything finer (minutes/seconds) is expressed as a
    cron in the caller, so this returns None for those.
    """
    total = _timedelta_seconds(node)
    if total <= 0:
        return None
    for unit, unit_seconds in (("WEEKS", 604800), ("DAYS", 86400), ("HOURS", 3600)):
        if total % unit_seconds == 0:
            return {"kind": "periodic", "interval": total // unit_seconds, "unit": unit, "pause_status": "UNPAUSED"}
    return None


def _timedelta_seconds(node: ast.expr | None) -> int:
    """Returns the number of seconds in a literal timedelta call, or zero."""
    if not isinstance(node, ast.Call):
        return 0
    func = node.func
    name = func.attr if isinstance(func, ast.Attribute) else (func.id if isinstance(func, ast.Name) else "")
    if name != "timedelta":
        return 0
    total = 0
    for keyword in node.keywords:
        if keyword.arg in _TIMEDELTA_UNIT_SECONDS and isinstance(keyword.value, ast.Constant):
            if isinstance(keyword.value.value, int):
                total += keyword.value.value * _TIMEDELTA_UNIT_SECONDS[keyword.arg]
    return total


def _schedule_from_interval(
    interval: str | None,
    *,
    node: ast.expr | None = None,
    timezone: str | None = None,
) -> dict[str, object] | None:
    """Builds a Pipeline.schedule spec from an Airflow schedule.

    A string cron / preset -> ``kind: schedule`` (Quartz) with the DAG timezone;
    a ``timedelta(...)`` -> ``kind: periodic``. Returns None when neither applies.
    """
    if interval:
        if interval == "@continuous":
            return {"kind": "continuous", "pause_status": "UNPAUSED"}
        quartz: str | None = _CRON_PRESETS.get(interval) or _cron_to_quartz(interval)
        if quartz is not None:
            return {
                "kind": "schedule",
                "quartz_cron_expression": quartz,
                "timezone_id": timezone or "UTC",
                "pause_status": "UNPAUSED",
            }
    periodic = _timedelta_to_periodic(node)
    if periodic is not None:
        return periodic
    total_seconds = _timedelta_seconds(node)
    if 0 < total_seconds < 60 and 60 % total_seconds == 0:
        return {
            "kind": "schedule",
            "quartz_cron_expression": f"0/{total_seconds} * * * * ?",
            "timezone_id": timezone or "UTC",
            "pause_status": "UNPAUSED",
        }
    if total_seconds % 60 == 0:
        minutes = total_seconds // 60
        if 0 < minutes < 60 and 60 % minutes == 0:
            return {
                "kind": "schedule",
                "quartz_cron_expression": f"0 0/{minutes} * * * ?",
                "timezone_id": timezone or "UTC",
                "pause_status": "UNPAUSED",
            }
    return None


def _asset_definitions(module: ast.Module, aliases: dict[str, str]) -> dict[str, ast.Call]:
    """Returns module-level Asset/Dataset objects that a DAG schedule may reference."""
    definitions: dict[str, ast.Call] = {}
    for statement in module.body:
        if not (
            isinstance(statement, ast.Assign)
            and len(statement.targets) == 1
            and isinstance(statement.targets[0], ast.Name)
            and isinstance(statement.value, ast.Call)
        ):
            continue
        if _construct_name(statement.value.func, aliases) in {"Asset", "Dataset"}:
            definitions[statement.targets[0].id] = statement.value
    return definitions


def _asset_table_name(call: ast.Call) -> str | None:
    kwargs = {keyword.arg: keyword.value for keyword in call.keywords if keyword.arg}
    extra = ops.literal_value(kwargs.get("extra"))
    if isinstance(extra, dict):
        table_name = extra.get("databricks_table")
        if isinstance(table_name, str) and table_name.strip():
            return table_name.strip()
    uri = ops.literal_str(call.args[0]) if call.args else ops.literal_str(kwargs.get("uri"))
    prefix = "x-databricks-table:"
    if uri and uri.startswith(prefix):
        table_name = uri[len(prefix) :].lstrip("/").strip()
        return table_name or None
    return None


def _asset_expression(
    node: ast.expr,
    aliases: dict[str, str],
    definitions: dict[str, ast.Call],
) -> tuple[list[str], str, str | None] | None:
    if isinstance(node, ast.Name):
        definition = definitions.get(node.id)
        return _asset_expression(definition, aliases, definitions) if definition is not None else None
    if isinstance(node, ast.Call) and _construct_name(node.func, aliases) in {"Asset", "Dataset"}:
        table_name = _asset_table_name(node)
        return ([table_name], "leaf", None) if table_name else ([], "leaf", "unresolved_asset_schedule")
    if isinstance(node, (ast.List, ast.Tuple)):
        children = [_asset_expression(item, aliases, definitions) for item in node.elts]
        if not children or any(child is None for child in children):
            return None
        resolved = [child for child in children if child is not None]
        error = next((child[2] for child in resolved if child[2] is not None), None)
        if error:
            return [], "all", error
        if any(child[1] == "any" for child in resolved):
            return [], "all", "unsupported_asset_schedule_expression"
        return [table for child in resolved for table in child[0]], "all", None
    if isinstance(node, ast.BinOp) and isinstance(node.op, (ast.BitAnd, ast.BitOr)):
        left = _asset_expression(node.left, aliases, definitions)
        right = _asset_expression(node.right, aliases, definitions)
        if left is None or right is None:
            return None
        error = left[2] or right[2]
        mode = "all" if isinstance(node.op, ast.BitAnd) else "any"
        if error:
            return [], mode, error
        if any(child_mode not in {"leaf", mode} for child_mode in (left[1], right[1])):
            return [], mode, "unsupported_asset_schedule_expression"
        return [*left[0], *right[0]], mode, None
    return None


def _asset_schedule_from_node(
    node: ast.expr,
    aliases: dict[str, str],
    definitions: dict[str, ast.Call],
) -> tuple[dict[str, object] | None, str | None]:
    if any(
        isinstance(candidate, ast.Call) and _construct_name(candidate.func, aliases) == "AssetOrTimeSchedule"
        for candidate in ast.walk(node)
    ):
        return None, "unsupported_asset_or_time_schedule"
    expression = _asset_expression(node, aliases, definitions)
    if expression is None:
        return None, "unsupported_dag_schedule"
    table_names, mode, error = expression
    if error:
        return None, error
    table_names = list(dict.fromkeys(table_names))
    if not table_names:
        return None, "unresolved_asset_schedule"
    return (
        {
            "kind": "table_update",
            "table_names": table_names,
            "condition": "ANY_UPDATED" if mode in {"leaf", "any"} else "ALL_UPDATED",
            "pause_status": "UNPAUSED",
        },
        None,
    )
