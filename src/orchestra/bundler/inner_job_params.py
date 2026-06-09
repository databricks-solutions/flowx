"""Collects and normalize parameters for ForEach inner jobs."""

from __future__ import annotations

import re
from typing import Any

# The leading @ is optional -- ADF only puts it on the outermost expression;
# inner references like concat('x', pipeline().parameters.Y) are bare.
_ITEM_BARE_RE = re.compile(r"@?item\(\s*\)", re.IGNORECASE)
_ITEM_FIELD_RE = re.compile(r"@?item\(\s*\)\.(\w+)", re.IGNORECASE)
_PIPELINE_PARAM_RE = re.compile(r"@?pipeline\(\s*\)\.parameters\.(\w+)", re.IGNORECASE)
_PIPELINE_RUNID_RE = re.compile(r"@?pipeline\(\s*\)\.RunId", re.IGNORECASE)
_PIPELINE_TRIGGER_TIME_RE = re.compile(r"@?pipeline\(\s*\)\.TriggerTime", re.IGNORECASE)
_PIPELINE_NAME_RE = re.compile(r"@?pipeline\(\s*\)\.Pipeline", re.IGNORECASE)
_PIPELINE_GROUPID_RE = re.compile(r"@?pipeline\(\s*\)\.GroupId", re.IGNORECASE)
_ACTIVITY_OUTPUT_RE = re.compile(
    r"@?activity\(\s*'([^']+)'\s*\)\.output(?:\.firstRow)?\.(\w+)",
    re.IGNORECASE,
)
_VARIABLES_RE = re.compile(r"@?variables\(\s*'([^']+)'\s*\)", re.IGNORECASE)
_UTCNOW_RE = re.compile(r"@?utcNow\(\s*\)", re.IGNORECASE)
_DAB_JOB_PARAM_RE = re.compile(r"\{\{job\.parameters\.(\w+)\}\}")
_DAB_INPUT_FIELD_RE = re.compile(r"\{\{input\.(\w+)\}\}")

_CONCAT_RE = re.compile(r"@?concat\((.+)\)$", re.IGNORECASE | re.DOTALL)

# ADF type-conversion wrappers: @string(expr), @int(expr), @bool(expr), etc.
# These are no-ops in DAB string parameter context -- strip them.
_TYPE_CAST_RE = re.compile(
    r"^@?(string|int|float|bool|decimal|json|xml|base64|binary|uriComponent|"
    r"ticks|dataUri|dataUriToBinary|dataUriToString|uriComponentToString)\((.+)\)$",
    re.IGNORECASE | re.DOTALL,
)


def collect_inner_job_params(
    tasks: list[dict[str, Any]],
    *,
    raw_ir_tasks: list[dict[str, Any]] | None = None,
    variable_task_keys: dict[str, str] | None = None,
) -> tuple[list[dict[str, Any]], dict[str, str]]:
    """Scans task dicts for parameter references and return declarations + pass-through map.

    Args:
        tasks: The inner job's DAB task dicts (may be nested via condition_task).
        raw_ir_tasks: Optional raw IR dicts (before DAB conversion) to scan
            for references in fields that are consumed during conversion
            (e.g. WebActivity ``url``, ``body``).
        variable_task_keys: C-06 (VAREX-004): mapping of pipeline-variable
            names to the setter task_key on the parent job that owns the
            variable.  Names that match a variable do NOT get declared as
            inner-job parameters; instead the parent passes the variable's
            task-value reference (``{{tasks.<setter>.values.<var>}}``)
            through the ``job_parameters`` map.  Without this the parent
            would emit ``{{job.parameters.<var>}}`` referring to a name
            that's never declared on the parent job, so the inner job
            would receive an empty string.

    Returns:
        Tuple of:
        - ``parameters``: list of ``{"name": ..., "default": ...}`` dicts for
          the inner job definition.
        - ``job_parameters``: dict mapping param name -> parent expression,
          suitable for the ``run_job_task.job_parameters`` block.  ``item``
          always maps to ``"{{input}}"``, pipeline params map to
          ``"{{job.parameters.<name>}}"``, variables resolved via the
          *variable_task_keys* map route through ``{{tasks.X.values.Y}}``.
    """
    param_names: set[str] = set()
    item_field_names: set[str] = set()
    variable_names: set[str] = set()

    _scan_tasks(tasks, param_names, item_field_names=item_field_names, variable_names=variable_names)

    if raw_ir_tasks:
        _scan_ir_tasks(raw_ir_tasks, param_names, item_field_names=item_field_names, variable_names=variable_names)

    var_task_keys = variable_task_keys or {}

    parameters: list[dict[str, Any]] = []
    for name in sorted(param_names):
        # C-06: variables with a known setter task on the parent job route
        # via {{tasks.X.values.Y}} -- they must NOT show up as inner-job
        # parameter declarations.
        if name in variable_names and name in var_task_keys:
            continue
        param: dict[str, Any] = {"name": name}
        if name != "item":
            param["default"] = ""
        parameters.append(param)

    # "item" (bare @item()) maps to {{input}} (the full iteration value);
    # item field names (@item().field) map to {{input.<field>}};
    # pipeline params / variables map to {{job.parameters.<name>}}.
    job_parameters: dict[str, str] = {}
    for name in sorted(param_names):
        if name == "item":
            job_parameters[name] = "{{input}}"
        elif name in item_field_names:
            job_parameters[name] = "{{input." + name + "}}"
        elif name in variable_names and name in var_task_keys:
            setter = var_task_keys[name]
            job_parameters[name] = "{{tasks." + setter + ".values." + name + "}}"
        else:
            job_parameters[name] = "{{job.parameters." + name + "}}"

    return parameters, job_parameters


def normalize_inner_task_params(tasks: list[dict[str, Any]]) -> None:
    """Normalise ADF expressions in task dicts for an inner job context.

    Args:
        tasks: The inner job's task dicts.
    """
    for task in tasks:
        notebook_task = task.get("notebook_task")
        if notebook_task and "base_parameters" in notebook_task:
            notebook_task["base_parameters"] = {
                key: normalize_value(value) for key, value in notebook_task["base_parameters"].items()
            }

        condition_task = task.get("condition_task")
        if condition_task:
            if "left" in condition_task:
                condition_task["left"] = normalize_value(condition_task["left"])
            if "right" in condition_task:
                condition_task["right"] = normalize_value(condition_task["right"])
            normalize_inner_task_params(condition_task.get("if_true", []))
            normalize_inner_task_params(condition_task.get("if_false", []))

        for_each_task = task.get("for_each_task", {})
        body = for_each_task.get("task")
        if body:
            normalize_inner_task_params([body])


def _scan_tasks(
    tasks: list[dict[str, Any]],
    param_names: set[str],
    *,
    item_field_names: set[str] | None = None,
    variable_names: set[str] | None = None,
) -> None:
    """Recursively scan task dicts for ADF parameter references.

    Args:
        tasks: List of task dicts to scan.
        param_names: Accumulator set of discovered parameter names.
        item_field_names: Optional accumulator for field names from item().field refs.
        variable_names: Optional accumulator for names sourced from
            ``variables('X')`` references (separate from pipeline params).
    """
    kw: dict[str, Any] = {"item_field_names": item_field_names, "variable_names": variable_names}
    for task in tasks:
        notebook_task = task.get("notebook_task", {})
        params = notebook_task.get("base_parameters", {})
        for value in params.values():
            _extract_refs(value, param_names, **kw)

        run_job_task = task.get("run_job_task", {})
        for value in run_job_task.get("job_parameters", {}).values():
            _extract_refs(value, param_names, **kw)

        condition_task = task.get("condition_task", {})
        if condition_task:
            _extract_refs(condition_task.get("left", ""), param_names, **kw)
            _extract_refs(condition_task.get("right", ""), param_names, **kw)
            _scan_tasks(condition_task.get("if_true", []), param_names, **kw)
            _scan_tasks(condition_task.get("if_false", []), param_names, **kw)

        for_each_task = task.get("for_each_task", {})
        body = for_each_task.get("task")
        if body:
            _scan_tasks([body], param_names, **kw)


def _scan_ir_tasks(
    ir_tasks: list[dict[str, Any]],
    param_names: set[str],
    *,
    item_field_names: set[str] | None = None,
    variable_names: set[str] | None = None,
) -> None:
    """Scans raw IR task dicts for parameter references in all fields.

    Args:
        ir_tasks: Raw serialised IR task dicts.
        param_names: Accumulator set of discovered parameter names.
        item_field_names: Optional accumulator for field names from item().field refs.
        variable_names: Optional accumulator for variable names.
    """
    field_name_kwargs = {"item_field_names": item_field_names, "variable_names": variable_names}
    for task_dict in ir_tasks:
        _extract_refs(task_dict.get("url", ""), param_names, **field_name_kwargs)
        _extract_refs(task_dict.get("body"), param_names, **field_name_kwargs)
        if isinstance(task_dict.get("headers"), dict):
            for value in task_dict["headers"].values():
                _extract_refs(value, param_names, **field_name_kwargs)

        base_parameters = task_dict.get("base_parameters")
        if isinstance(base_parameters, dict):
            for value in base_parameters.values():
                _extract_refs(value, param_names, **field_name_kwargs)

        _extract_refs(task_dict.get("on_expression", ""), param_names, **field_name_kwargs)
        for case in task_dict.get("cases", []):
            _scan_ir_tasks(case.get("activities", []), param_names, **field_name_kwargs)
        _scan_ir_tasks(task_dict.get("default_activities", []), param_names, **field_name_kwargs)

        _extract_refs(task_dict.get("op", ""), param_names, **field_name_kwargs)
        _extract_refs(task_dict.get("left", ""), param_names, **field_name_kwargs)
        _extract_refs(task_dict.get("right", ""), param_names, **field_name_kwargs)
        _scan_ir_tasks(task_dict.get("if_true_activities", []), param_names, **field_name_kwargs)
        _scan_ir_tasks(task_dict.get("if_false_activities", []), param_names, **field_name_kwargs)

        _scan_ir_tasks(task_dict.get("inner_activities", []), param_names, **field_name_kwargs)


def _extract_refs(
    value: Any,
    param_names: set[str],
    *,
    item_field_names: set[str] | None = None,
    variable_names: set[str] | None = None,
) -> None:
    """Extracts parameter names from a single value that may be a string or ADF expression dict."""
    text = ""
    if isinstance(value, str):
        text = value
    elif isinstance(value, dict):
        text = value.get("value", "") if value.get("type") == "Expression" else ""

    if not text:
        return

    for match in _PIPELINE_PARAM_RE.finditer(text):
        param_names.add(match.group(1))

    for match in _VARIABLES_RE.finditer(text):
        param_names.add(match.group(1))
        if variable_names is not None:
            variable_names.add(match.group(1))

    for match in _ITEM_FIELD_RE.finditer(text):
        field_name = match.group(1)
        param_names.add(field_name)
        if item_field_names is not None:
            item_field_names.add(field_name)

    # Only add bare "item" if there's an item() that isn't part of item().field
    bare_text = _ITEM_FIELD_RE.sub("", text)
    if _ITEM_BARE_RE.search(bare_text):
        param_names.add("item")

    for match in _DAB_INPUT_FIELD_RE.finditer(text):
        field_name = match.group(1)
        param_names.add(field_name)
        if item_field_names is not None:
            item_field_names.add(field_name)

    for match in _DAB_JOB_PARAM_RE.finditer(text):
        param_names.add(match.group(1))


def normalize_value(value: Any) -> str:
    """Normalize a single parameter value from ADF expression to DAB reference.

    Args:
        value: A string or ``{type: Expression, value: ...}`` dict.

    Returns:
        A normalized string with ``{{job.parameters.*}}`` references.
    """
    if isinstance(value, dict) and value.get("type") == "Expression":
        text = value.get("value", "")
    elif isinstance(value, str):
        text = value
    else:
        return str(value)

    # Strip ADF type-conversion wrappers (@string, @int, etc.) first so the
    # inner expression can be resolved by subsequent steps.
    match = _TYPE_CAST_RE.match(text.strip())
    if match:
        text = match.group(2)
        # Re-add @ prefix if the inner expression is a function call or
        # reference that needs it for pattern matching.
        if not text.startswith("@") and not text.startswith("{{"):
            text = "@" + text

    text = _replace_refs(text)
    text = _resolve_concat(text)

    return text


def _replace_refs(text: str) -> str:
    """Replaces ADF references with {{job.parameters.*}} or {{job.*}} refs.

    Args:
        text: Expression text potentially containing ADF references.

    Returns:
        Text with references replaced.
    """
    text = _DAB_INPUT_FIELD_RE.sub(r"{{job.parameters.\1}}", text)
    text = _ITEM_FIELD_RE.sub(r"{{job.parameters.\1}}", text)
    text = _ITEM_BARE_RE.sub("{{job.parameters.item}}", text)
    text = _PIPELINE_PARAM_RE.sub(r"{{job.parameters.\1}}", text)
    text = _PIPELINE_RUNID_RE.sub("{{job.run_id}}", text)
    text = _PIPELINE_TRIGGER_TIME_RE.sub("{{job.start_time.iso_datetime}}", text)
    text = _PIPELINE_NAME_RE.sub("{{job.name}}", text)
    text = _PIPELINE_GROUPID_RE.sub("{{job.run_id}}", text)
    text = _ACTIVITY_OUTPUT_RE.sub(r"{{tasks.\1.values.\2}}", text)
    text = _VARIABLES_RE.sub(r"{{job.parameters.\1}}", text)
    text = _UTCNOW_RE.sub("{{job.start_time.iso_datetime}}", text)

    return text


def _resolve_concat(text: str) -> str:
    """Resolves ``@concat(arg1, arg2, ...)`` to a plain concatenated string.

    Args:
        text: Expression that may be a @concat(...) call.

    Returns:
        Resolved string, or original text if not resolvable.
    """
    match = _CONCAT_RE.match(text.strip())
    if not match:
        return text

    args_str = match.group(1)
    parts = _split_concat_args(args_str)
    if parts is None:
        return text

    resolved: list[str] = []
    for part in parts:
        part = part.strip()
        if part.startswith("'") and part.endswith("'"):
            resolved.append(part[1:-1])
        elif part.startswith("{{"):
            resolved.append(part)
        else:
            return text

    return "".join(resolved)


def _split_concat_args(args_str: str) -> list[str] | None:
    """Splits concat arguments respecting nested parentheses and quotes.

    Args:
        args_str: The argument string inside concat(...).

    Returns:
        List of argument strings, or None if parsing fails.
    """
    parts: list[str] = []
    depth = 0
    current: list[str] = []
    in_quote = False

    for char in args_str:
        if char == "'" and depth == 0:
            in_quote = not in_quote
            current.append(char)
        elif in_quote:
            current.append(char)
        elif char == "(":
            depth += 1
            current.append(char)
        elif char == ")":
            if depth == 0:
                return None
            depth -= 1
            current.append(char)
        elif char == "," and depth == 0:
            parts.append("".join(current).strip())
            current = []
        else:
            current.append(char)

    if current:
        parts.append("".join(current).strip())

    if depth != 0 or in_quote:
        return None

    return parts
