"""Generates Python notebook content for activities that need custom notebooks."""

from __future__ import annotations

import ast
import json
import re
import textwrap
from typing import TYPE_CHECKING, Any

from orchestra.models.ir import TranslationContext
from orchestra.models.source_types import FILE_SOURCE_TYPES, JDBC_SOURCE_TYPES, REST_SOURCE_TYPES
from orchestra.parser.expression_parser import (
    resolve_expression,
    resolve_interpolated_string_for_notebook,
)

if TYPE_CHECKING:
    from orchestra.models.ir import (
        AppendVariableActivity,
        CopyActivity,
        DeleteActivity,
        FilterActivity,
        LookupActivity,
        MotifActivity,
        SetVariableActivity,
        WaitActivity,
        WebActivity,
    )


def generate_lookup_notebook(activity: LookupActivity, *, scope: str = "") -> str:
    """Generates a Python notebook that executes a lookup query.

    Args:
        activity: The LookupActivity IR node.
        scope: Secret scope name (defaults to task_key if empty).

    Returns:
        Complete notebook source code as a string.
    """
    header = _notebook_header(f"Lookup: {activity.name}")
    source_type = activity.source_type or ""
    query = activity.source_query or ""

    query_assignment = _render_query_assignment(query)

    if source_type in JDBC_SOURCE_TYPES:
        scope = scope or activity.task_key

        body = textwrap.dedent(f"""\
            import json

            # Parameters
            first_row_only = dbutils.widgets.get("first_row_only") == "true"

            # Credentials
            jdbc_url = dbutils.secrets.get(scope="{scope}", key="jdbc-url")
            jdbc_password = dbutils.secrets.get(scope="{scope}", key="jdbc-password")
            jdbc_user = dbutils.secrets.get(scope="{scope}", key="jdbc-user")

            # Execute lookup query
            {query_assignment}

            df = (
                spark.read.format("jdbc")
                .option("url", jdbc_url)
                .option("user", jdbc_user)
                .option("password", jdbc_password)
                .option("query", query)
                .load()
            )

            if first_row_only:
                result = df.first()
                output = result.asDict() if result else {{}}
            else:
                output = [row.asDict() for row in df.collect()]

            # Set task values so downstream tasks can reference them via
            # {{{{tasks.{activity.task_key}.values.<key>}}}}.
            # The full result is stored under "result" as a JSON string so
            # for_each_task can consume it as `inputs`.  For firstRow lookups,
            # each column is also stored as an individual task value so
            # condition_task can reference e.g. {{{{tasks.{activity.task_key}.values.cnt}}}}.
            dbutils.jobs.taskValues.set(key="result", value=json.dumps(output))
            if first_row_only and isinstance(output, dict):
                for col_name, col_value in output.items():
                    dbutils.jobs.taskValues.set(key=col_name, value=col_value)
        """)
    else:
        body = textwrap.dedent(f"""\
            import json

            # Parameters
            first_row_only = dbutils.widgets.get("first_row_only") == "true"

            # Execute lookup query via Spark SQL
            {query_assignment}
            df = spark.sql(query)

            if first_row_only:
                result = df.first()
                output = result.asDict() if result else {{}}
            else:
                output = [row.asDict() for row in df.collect()]

            # Set task values so downstream tasks can reference them via
            # {{{{tasks.{activity.task_key}.values.<key>}}}}.
            dbutils.jobs.taskValues.set(key="result", value=json.dumps(output))
            if first_row_only and isinstance(output, dict):
                for col_name, col_value in output.items():
                    dbutils.jobs.taskValues.set(key=col_name, value=col_value)
        """)

    return header + _command_separator() + body


def generate_web_activity_notebook(activity: WebActivity, *, scope: str = "") -> str:
    """Generates a Python notebook that makes an HTTP request.

    Args:
        activity: The WebActivity IR node.
        scope: Secret scope name (defaults to task_key if empty).

    Returns:
        Complete notebook source code as a string.
    """
    header = _notebook_header(f"Web Activity: {activity.name}")

    # Resolve header values — some may contain ADF expressions like
    # {"Authorization": {"type": "Expression", "value": "@concat('Bearer ', ...)"}}
    headers_literal, headers_preamble = _resolve_headers(activity.headers)

    auth_block = ""
    auth = activity.authentication
    if auth:
        scope = scope or activity.task_key
        auth_type = auth.get("type", "")
        if auth_type in ("ServicePrincipal", "MSI", "ManagedServiceIdentity"):
            auth_block = textwrap.dedent(f"""\
                # Authentication ({auth_type})
                auth_token = dbutils.secrets.get(scope="{scope}", key="auth-credential")
                headers["Authorization"] = f"Bearer {{auth_token}}"
            """)
        elif auth_type == "Basic":
            auth_block = textwrap.dedent(f"""\
                # Authentication (Basic)
                import base64
                username = dbutils.secrets.get(scope="{scope}", key="auth-username")
                password = dbutils.secrets.get(scope="{scope}", key="auth-credential")
                token = base64.b64encode(f"{{username}}:{{password}}".encode()).decode()
                headers["Authorization"] = f"Basic {{token}}"
            """)
        else:
            auth_block = textwrap.dedent(f"""\
                # Authentication
                auth_credential = dbutils.secrets.get(scope="{scope}", key="auth-credential")
                headers["Authorization"] = f"Bearer {{auth_credential}}"
            """)

    body_block = ""
    request_call = ""
    if activity.method in ("POST", "PUT", "PATCH"):
        raw_body = activity.body
        # If the body was pre-resolved to Python code by the translator
        # (contains function calls like __import__ or json.loads), embed directly.
        if isinstance(raw_body, str) and ("__import__" in raw_body or "json.loads" in raw_body):
            body_block = f"body = {raw_body}\n"
        else:
            body_str = _resolve_body(raw_body)
            # ``_resolve_body`` may return either a JSON literal, a Python
            # dict literal, or a ``repr()``'d string containing Python-like
            # concat syntax.  Parse strings as JSON when possible so the
            # downstream ``requests.request(json=...)`` gets a real object.
            body_block = textwrap.dedent(f"""\
                body_raw = dbutils.widgets.get("body") or {body_str}
                if isinstance(body_raw, str):
                    try:
                        body = json.loads(body_raw)
                    except (ValueError, TypeError):
                        body = body_raw
                else:
                    body = body_raw
            """)
        # ``json=`` encodes dicts; string bodies go over ``data=`` so we don't
        # double-encode them as JSON strings.
        request_call = textwrap.dedent(
            """\
            if isinstance(body, (dict, list)):
                response = requests.request(method, url, headers=headers, json=body, timeout=300)
            else:
                response = requests.request(method, url, headers=headers, data=body, timeout=300)
            """
        )
    else:
        request_call = "response = requests.request(method, url, headers=headers, timeout=300)\n"

    # When the URL is a DAB dynamic ref ({{...}}), it will be resolved and
    # passed via base_parameters at runtime — just read from the widget.
    if "{{" in activity.url:
        url_line = 'url = dbutils.widgets.get("url")'
    else:
        url_line = f'url = dbutils.widgets.get("url") or "{activity.url}"'

    if "{{" in activity.method:
        method_line = 'method = dbutils.widgets.get("method")'
    else:
        method_line = f'method = dbutils.widgets.get("method") or "{activity.method}"'

    body = textwrap.dedent(f"""\
        import json
        import requests

        # Parameters
        {url_line}
        {method_line}
        headers = {headers_literal}

    """)
    body += headers_preamble
    body += auth_block
    body += body_block
    body += request_call
    body += textwrap.dedent("""\

        response.raise_for_status()

        # Return response
        try:
            result = response.json()
        except ValueError:
            result = {"status_code": response.status_code, "text": response.text}

    """)

    return header + _command_separator() + body


def generate_delete_notebook(activity: DeleteActivity) -> str:
    """Generates a notebook using dbutils.fs.rm().

    Args:
        activity: The DeleteActivity IR node.

    Returns:
        Complete notebook source code as a string.
    """
    header = _notebook_header(f"Delete: {activity.name}")
    folder_path = activity.folder_path or ""

    body = textwrap.dedent(f"""\
        # Parameters
        dataset_name = dbutils.widgets.get("dataset_name") or "{activity.dataset_name}"
        folder_path = dbutils.widgets.get("folder_path") or "{folder_path}"
        recursive = dbutils.widgets.get("recursive") == "true"

        # Build the full path to delete
        target_path = folder_path if folder_path else dataset_name
        print(f"Deleting: {{target_path}} (recursive={{recursive}})")
        result = dbutils.fs.rm(target_path, recurse=recursive)

    """)

    return header + _command_separator() + body


def generate_set_variable_notebook(activity: SetVariableActivity) -> str:
    """Generates a notebook that sets a task value.

    Args:
        activity: The SetVariableActivity IR node.

    Returns:
        Complete notebook source code as a string.
    """
    header = _notebook_header(f"Set Variable: {activity.name}")

    if activity.value_kind == "notebook_code" and activity.notebook_code:
        # Embed imports and Python code directly in the notebook
        import_lines = "\n".join(activity.notebook_imports) if activity.notebook_imports else ""
        if import_lines:
            import_block = import_lines + "\n"
        else:
            import_block = ""

        # Build body lines list to avoid textwrap.dedent issues when
        # import_block starts at column 0 (which would prevent dedent
        # from stripping the common leading whitespace).
        lines = ["import json"]
        if import_block:
            lines.append(import_block.rstrip("\n"))
        lines.append("")
        lines.append(f'variable_name = "{activity.variable_name}"')
        lines.append("")
        lines.append("# Compute value at runtime.")
        lines.append("# Original ADF expression resolved to notebook code.")
        lines.append(f"value = {activity.notebook_code}")
        lines.append("")
        lines.append("# Set task value so downstream tasks can reference it via")
        lines.append(f"# {{{{tasks.{activity.task_key}.values.{activity.variable_name}}}}}.")
        lines.append("dbutils.jobs.taskValues.set(key=variable_name, value=value)")
        lines.append("print(f\"Set task value '{variable_name}' = '{value}'\")")
        lines.append("")
        body = "\n".join(lines) + "\n"
    else:
        body = textwrap.dedent(f"""\
            import json

            variable_name = "{activity.variable_name}"

            # Read value from widget parameter (set via base_parameters).
            # DAB resolves dynamic references (e.g. {{{{job.run_id}}}}) before passing.
            value = dbutils.widgets.get("value")

            # Set task value so downstream tasks can reference it via
            # {{{{tasks.{activity.task_key}.values.{activity.variable_name}}}}}.
            dbutils.jobs.taskValues.set(key=variable_name, value=value)
            print(f"Set task value '{{variable_name}}' = '{{value}}'")

        """)

    return header + _command_separator() + body


def generate_wait_notebook(activity: WaitActivity) -> str:
    """Generates a notebook that sleeps for a specified duration.

    Args:
        activity: The WaitActivity IR node.

    Returns:
        Complete notebook source code as a string.
    """
    header = _notebook_header(f"Wait: {activity.name}")

    body = textwrap.dedent(f"""\
        import time

        # Parameters
        default_wait = {activity.wait_time_seconds}
        param = dbutils.widgets.get("wait_seconds")
        wait_seconds = int(param) if param else default_wait

        print(f"Waiting for {{wait_seconds}} seconds...")
        time.sleep(wait_seconds)
        print("Wait complete.")

    """)

    return header + _command_separator() + body


def generate_copy_notebook(activity: CopyActivity, *, scope: str = "") -> str:
    """Generates a notebook for copy operations (Auto Loader, COPY INTO, or JDBC).

    Args:
        activity: The CopyActivity IR node.
        scope: Secret scope name (defaults to task_key if empty).

    Returns:
        Complete notebook source code as a string.  When the IR is
        stamped with ``use_lakeflow_connector=True`` the body is a
        Lakeflow Connect scaffold; when ``target_format='sdp'`` it is a
        Lakeflow Spark Declarative Pipeline scaffold; otherwise it is
        the PySpark notebook the legacy translator emits.
    """
    if activity.target_format == "sdp":
        return _generate_sdp_copy_notebook(activity, scope=scope)

    header = _notebook_header(f"Copy: {activity.name}")
    source_type = activity.source_type or ""

    if source_type in FILE_SOURCE_TYPES:
        body = _generate_autoloader_body(activity)
    elif source_type in JDBC_SOURCE_TYPES:
        body = _generate_jdbc_body(activity, scope=scope)
    elif source_type in REST_SOURCE_TYPES:
        body = _generate_rest_copy_body(activity)
    else:
        body = _generate_generic_copy_body(activity)

    # Hoist any imports the body needs into a single cell at the top of
    # the notebook.  ``_render_sink_write`` and a few other helpers used
    # to inline ``from datetime import ...`` next to the call site, which
    # produced an awkward block sandwiched between two comment groups.
    imports = _detect_imports(body)
    if imports:
        body = _strip_inline_imports(body, imports)
        return header + _command_separator() + "\n".join(imports) + "\n" + _command_separator() + body
    return header + _command_separator() + body


def _generate_sdp_copy_notebook(activity: CopyActivity, *, scope: str = "") -> str:
    """Generates an SDP scaffold notebook for a Copy activity targeting Delta.

    Args:
        activity: The CopyActivity IR node.
        scope: Secret scope name (defaults to task_key if empty).

    Returns:
        Notebook source code that defines a Lakeflow Spark Declarative
        Pipeline table reading from the resolved source and materialising
        into the resolved sink table.
    """
    header = _notebook_header(f"SDP Copy: {activity.name}")
    sink_table = _resolve_sink_table_reference(activity)
    source_descriptor = _resolve_source_descriptor(activity, scope=scope)
    body = textwrap.dedent(f"""\
        from pyspark import pipelines as sdp
        from pyspark.sql import functions as F


        @sdp.table(
            name="{sink_table}",
            comment="Generated by orchestra from ADF Copy activity '{activity.name}'.",
        )
        def {_safe_identifier(activity.task_key)}():
            return {source_descriptor}
    """)
    return header + _command_separator() + body


def _resolve_sink_table_reference(activity: CopyActivity) -> str:
    """Resolves the fully-qualified Delta table reference for a Copy sink.

    Args:
        activity: Copy activity to inspect.

    Returns:
        Catalog/schema/table reference using DAB bundle variables when
        only the bare table name is known, or a literal reference when
        the IR already carries one.
    """
    sink_properties = activity.sink_properties or {}
    table = sink_properties.get("table") or activity.task_key
    if "." in table:
        return table
    return f"${{var.catalog}}.${{var.schema}}.{table}"


def _resolve_source_descriptor(activity: CopyActivity, *, scope: str) -> str:
    """Resolves a Spark read expression for an SDP scaffold body.

    Args:
        activity: Copy activity whose source the scaffold reads.
        scope: Secret scope name used for JDBC-style sources.

    Returns:
        Python expression that returns a Spark DataFrame for the source.
        Falls back to a placeholder ``spark.read.table(...)`` when the
        source type is unrecognised so the scaffold still parses.
    """
    source_properties = activity.source_properties or {}
    source_path = source_properties.get("resolved_path")
    source_type = activity.source_type or ""
    if source_type in FILE_SOURCE_TYPES and source_path:
        return f'spark.readStream.format("cloudFiles").load("{source_path}")'
    if source_type in JDBC_SOURCE_TYPES:
        secret_scope = scope or activity.task_key
        return (
            'spark.read.format("jdbc")\n'
            f'        .option("url", dbutils.secrets.get(scope="{secret_scope}", key="jdbc-url"))\n'
            f'        .option("user", dbutils.secrets.get(scope="{secret_scope}", key="jdbc-user"))\n'
            f'        .option("password", dbutils.secrets.get(scope="{secret_scope}", key="jdbc-password"))\n'
            "        .load()"
        )
    if source_path:
        return f'spark.read.load("{source_path}")'
    return 'spark.read.table("source_placeholder")'


def _safe_identifier(value: str) -> str:
    """Coerces a string into a Python identifier safe for use as a function name.

    Args:
        value: Source string, typically a sanitised task key.

    Returns:
        The input with non-identifier characters replaced by underscores,
        falling back to ``ingest`` when the result would be empty.
    """
    cleaned = "".join(ch if ch.isalnum() or ch == "_" else "_" for ch in value)
    cleaned = cleaned.strip("_")
    if not cleaned or cleaned[0].isdigit():
        cleaned = f"ingest_{cleaned}" if cleaned else "ingest"
    return cleaned


def _detect_imports(body: str) -> list[str]:
    """Return import lines required by code references found in *body*."""
    needed: list[str] = []
    if "datetime." in body:
        needed.append("from datetime import datetime, timezone, timedelta")
    if "ZoneInfo(" in body:
        needed.append("from zoneinfo import ZoneInfo")
    return needed


def _strip_inline_imports(body: str, hoisted: list[str]) -> str:
    """Remove inline import lines that match the ones we hoisted to the top."""
    lines = body.splitlines()
    keep: list[str] = []
    skip_prefixes = (
        "from datetime import",
        "from zoneinfo import",
        "import datetime",
    )
    for line in lines:
        stripped = line.lstrip()
        if any(stripped.startswith(prefix) for prefix in skip_prefixes):
            continue
        keep.append(line)
    return "\n".join(keep) + ("\n" if body.endswith("\n") else "")


def generate_filter_notebook(activity: FilterActivity) -> str:
    """Generates a notebook that filters an array and stores the result as a task value.

    The condition is pre-translated at translate-time when possible; the
    notebook never calls ``eval()`` on widget input.  Conditions the
    translator could not safely lower fall back to a TODO placeholder
    notebook.
    """
    header = _notebook_header(f"Filter: {activity.name}")
    if activity.condition_code is None:
        return header + _command_separator() + _filter_placeholder_body(activity)
    return header + _command_separator() + _filter_resolved_body(activity)


def _filter_resolved_body(activity: FilterActivity) -> str:
    """Returns the notebook body for a Filter whose condition was resolved to Python."""
    import_block = "\n".join(activity.condition_imports or [])
    items_default = repr(activity.items_expression)
    original_expression_comment = _safe_inline_comment(activity.condition_expression)
    return textwrap.dedent(f"""\
        import json
        {import_block}

        # ``items_expression`` carries either a JSON-encoded array (when DAB
        # substitutes a {{{{tasks.X.values.Y}}}} reference) or a raw JSON literal
        # -- both parse via ``json.loads``.
        items_expression = dbutils.widgets.get("items_expression") or {items_default}
        items = json.loads(items_expression) if items_expression else []
        if not isinstance(items, list):
            items = [items]

        # Pre-translated condition (no eval on widget input).
        # Original ADF expression: {original_expression_comment}
        filtered = [item for item in items if {activity.condition_code}]

        dbutils.jobs.taskValues.set(key="output", value=json.dumps(filtered))
        print(f"Filtered {{len(items)}} items to {{len(filtered)}} items")
    """)


def _filter_placeholder_body(activity: FilterActivity) -> str:
    """Returns a TODO placeholder body for a Filter whose condition didn't translate."""
    items_default = repr(activity.items_expression)
    original_expression_comment = _safe_inline_comment(activity.condition_expression)
    activity_name_literal = repr(activity.name)
    return textwrap.dedent(f"""\
        import json

        items_expression = dbutils.widgets.get("items_expression") or {items_default}
        items = json.loads(items_expression) if items_expression else []
        if not isinstance(items, list):
            items = [items]

        # The translator could not safely lower the original ADF condition
        # to Python without invoking ``eval()`` on widget input.  Implement
        # the per-item check below by hand, then drop this NotImplementedError.
        # Original ADF expression: {original_expression_comment}
        def _matches(item):
            raise NotImplementedError(
                f"Implement filter condition for activity {activity_name_literal}"
            )

        filtered = [item for item in items if _matches(item)]

        dbutils.jobs.taskValues.set(key="output", value=json.dumps(filtered))
        print(f"Filtered {{len(items)}} items to {{len(filtered)}} items")
    """)


def _safe_inline_comment(text: str) -> str:
    """Returns *text* on a single line, suitable for an inline ``#`` comment."""
    return text.replace("\n", " ").replace("\r", " ").strip()


def generate_append_variable_notebook(activity: AppendVariableActivity) -> str:
    """Generates a notebook that appends a value to an array task value.

    Args:
        activity: The AppendVariableActivity IR node.

    Returns:
        Complete notebook source code as a string.
    """
    header = _notebook_header(f"Append Variable: {activity.name}")

    if activity.value_kind == "notebook_code" and activity.notebook_code:
        import_lines = "\n".join(activity.notebook_imports) if activity.notebook_imports else ""
        if import_lines:
            import_block = import_lines + "\n"
        else:
            import_block = ""

        # Build body lines list to avoid textwrap.dedent issues when
        # import_block starts at column 0.
        lines = ["import json"]
        if import_block:
            lines.append(import_block.rstrip("\n"))
        lines.append("")
        lines.append("# Parameters")
        lines.append(f'variable_name = dbutils.widgets.get("variable_name") or "{activity.variable_name}"')
        lines.append("")
        lines.append("# Compute value at runtime.")
        lines.append(f"value = {activity.notebook_code}")
        lines.append("")
        lines.append("# Read the current array from task values (or start with empty list)")
        lines.append("# `source_task_key` is populated at deploy time with the task that most")
        lines.append("# recently set this variable.  An empty value falls back to [].")
        lines.append("source_task_key = dbutils.widgets.get('source_task_key')")
        lines.append("current: list = []")
        lines.append("if source_task_key:")
        lines.append("    try:")
        lines.append("        current_raw = dbutils.jobs.taskValues.get(taskKey=source_task_key, key=variable_name)")
        lines.append("        if isinstance(current_raw, str):")
        lines.append("            current = json.loads(current_raw)")
        lines.append("        elif isinstance(current_raw, list):")
        lines.append("            current = current_raw")
        lines.append("        elif current_raw is not None:")
        lines.append("            current = [current_raw]")
        lines.append("    except Exception:")
        lines.append("        current = []")
        lines.append("")
        lines.append("if not isinstance(current, list):")
        lines.append("    current = [current] if current else []")
        lines.append("")
        lines.append("# Append and write back")
        lines.append("current.append(value)")
        lines.append("result = json.dumps(current)")
        lines.append("dbutils.jobs.taskValues.set(key=variable_name, value=result)")
        lines.append("print(f\"Appended to '{variable_name}': array now has {len(current)} item(s)\")")
        lines.append("")
        body = "\n".join(lines) + "\n"
    else:
        body = textwrap.dedent(f"""\
            import json

            # Parameters
            variable_name = dbutils.widgets.get("variable_name") or "{activity.variable_name}"
            value = dbutils.widgets.get("value") or {json.dumps(activity.append_value)}

            # Read the current array from a prior task's values.
            # `source_task_key` is passed via base_parameters and points at the
            # task that most recently set this variable; empty → start with [].
            source_task_key = dbutils.widgets.get("source_task_key")
            current: list = []
            if source_task_key:
                try:
                    current_raw = dbutils.jobs.taskValues.get(taskKey=source_task_key, key=variable_name)
                    if isinstance(current_raw, str):
                        current = json.loads(current_raw)
                    elif isinstance(current_raw, list):
                        current = current_raw
                    elif current_raw is not None:
                        current = [current_raw]
                except Exception:
                    current = []

            # Evaluate the value to append
            try:
                starts = ('{{', '[', '"')
                append_val = json.loads(value) if isinstance(value, str) and value.startswith(starts) else value
            except (json.JSONDecodeError, ValueError):
                append_val = value

            # Append and write back
            current.append(append_val)
            result = json.dumps(current)
            dbutils.jobs.taskValues.set(key=variable_name, value=result)
            print(f"Appended to '{{variable_name}}': array now has {{len(current)}} item(s)")

        """)

    return header + _command_separator() + body


def _notebook_header(title: str) -> str:
    """Return the standard Databricks notebook header block."""
    return textwrap.dedent(f"""\
        # Databricks notebook source
        # MAGIC %md
        # MAGIC # {title}
        # MAGIC
        # MAGIC *Auto-generated by Orchestra. Do not edit manually unless necessary.*
    """)


def _command_separator() -> str:
    """Return the Databricks cell separator comment."""
    return "\n# COMMAND ----------\n\n"


def _render_query_assignment(query: str) -> str:
    """Returns ``query = <expr>`` source for embedding a lookup query in a notebook.

    The orchestra expression resolver may have pre-translated the original
    ADF query into Python source that builds the SQL string at runtime
    (e.g. ``"SELECT ... FROM " + dbutils.widgets.get('table')``).  When
    that's the case the embedded text is *code*, not data, so we
    defensively parse it as a Python expression before splicing it in --
    if it doesn't parse, fall back to a safe string literal.

    All other queries are embedded as ``repr()`` string literals so any
    embedded quotes (including ``\"\"\"``) round-trip correctly.
    """
    if "dbutils.widgets.get" in query or "dbutils.jobs.taskValues" in query:
        try:
            ast.parse(query, mode="eval")
        except SyntaxError:
            return f"query = {query!r}"
        return f"query = {query}"
    return f"query = {query!r}"


_DAB_REF_PARAMETER_RE = re.compile(r"\{\{job\.parameters\.(\w+)\}\}")
_DAB_REF_RUN_ID_RE = re.compile(r"\{\{job\.run_id\}\}")
_DAB_REF_JOB_NAME_RE = re.compile(r"\{\{job\.name\}\}")
_DAB_REF_START_TIME_RE = re.compile(r"\{\{job\.start_time\.iso_datetime\}\}")
_DAB_REF_TASK_VALUE_RE = re.compile(r"\{\{tasks\.([^.]+)\.values\.(\w+)\}\}")


def _dab_ref_to_fstring_expr(ref: str) -> str:
    """Converts a DAB ref (``{{job.name}}``) to a Python f-string expression."""
    parameter_match = _DAB_REF_PARAMETER_RE.match(ref)
    if parameter_match:
        return "{dbutils.widgets.get('" + parameter_match.group(1) + "')}"
    if _DAB_REF_RUN_ID_RE.match(ref):
        return "{spark.conf.get('spark.databricks.job.runId', 'unknown')}"
    if _DAB_REF_JOB_NAME_RE.match(ref):
        return "{spark.conf.get('spark.databricks.job.parentName', 'unknown')}"
    if _DAB_REF_START_TIME_RE.match(ref):
        return "{spark.conf.get('spark.databricks.job.triggerTime', 'unknown')}"
    task_value_match = _DAB_REF_TASK_VALUE_RE.match(ref)
    if task_value_match:
        task_key, value_key = task_value_match.group(1), task_value_match.group(2)
        return "{dbutils.jobs.taskValues.get(taskKey='" + task_key + "', key='" + value_key + "')}"
    return ref


def _resolve_body(body: Any) -> str:
    """Resolves ADF expressions in a request body and return a Python expression.

    Args:
        body: The raw body from the WebActivity IR.

    Returns:
        A Python expression string suitable for embedding in generated code.
    """
    if body is None:
        return "None"
    if isinstance(body, str):
        return _resolve_string_body(body)
    if isinstance(body, dict):
        return _resolve_dict_body(body)
    return json.dumps(body) if body else "''"


def _resolve_string_body(body: str) -> str:
    """Renders a string-shaped WebActivity body as Python source."""
    context = TranslationContext()
    if "@{" in body:
        resolved_str = resolve_interpolated_string_for_notebook(body, context)
        return f"f{json.dumps(resolved_str)}"
    if not body.startswith("@"):
        return json.dumps(body)

    result = resolve_expression(body, context)
    if result is None:
        return json.dumps(body)
    if result.kind == "notebook_code":
        return result.value
    return json.dumps(result.value)


def _resolve_dict_body(body: dict[str, Any]) -> str:
    """Renders a dict-shaped WebActivity body as Python source."""
    if body.get("type") == "Expression" and "value" in body:
        return _resolve_body(body["value"])

    context = TranslationContext()
    needs_fstring = False
    resolved: dict[str, Any] = {}
    for key, value in body.items():
        new_value, value_needs_fstring = _resolve_dict_value(value, context)
        resolved[key] = new_value
        needs_fstring = needs_fstring or value_needs_fstring

    if not needs_fstring:
        return json.dumps(resolved)

    parts: list[str] = []
    for key, value in resolved.items():
        if isinstance(value, str) and "{" in value and "dbutils" in value:
            parts.append(f'"{key}": f"{value}"')
        else:
            parts.append(f'"{key}": {json.dumps(value)}')
    return "{" + ", ".join(parts) + "}"


def _resolve_dict_value(value: Any, context: TranslationContext) -> tuple[Any, bool]:
    """Resolves a single dict value; return ``(new_value, needs_fstring)``."""
    if isinstance(value, str) and "@{" in value:
        return resolve_interpolated_string_for_notebook(value, context), True
    if isinstance(value, str) and value.startswith("@"):
        return _resolve_expression_value(value, context, fallback=value)
    if isinstance(value, dict) and value.get("type") == "Expression":
        fallback = value.get("value", str(value))
        return _resolve_expression_value(value, context, fallback=fallback)
    return value, False


def _resolve_expression_value(raw: Any, context: TranslationContext, *, fallback: Any) -> tuple[Any, bool]:
    """Resolves a string/dict expression to either an f-string or a literal."""
    result = resolve_expression(raw, context)
    if result is None:
        return fallback, False
    if result.kind == "dab_ref":
        return _dab_ref_to_fstring_expr(result.value), True
    if result.kind == "literal":
        return result.value, False
    return fallback, False


def _resolve_headers(headers: dict[str, str] | None) -> tuple[str, str]:
    """Resolves ADF expressions in HTTP header values.

    Returns:
        A tuple of ``(headers_literal, preamble_code)``:
        - ``headers_literal`` is a Python dict literal for the initial headers
        - ``preamble_code`` is Python code to execute after the headers dict
          is created, adding dynamically computed header values
    """
    if not headers:
        return "{}", ""

    context = TranslationContext()
    static_headers: dict[str, str] = {}
    preamble_lines: list[str] = []

    for key, value in headers.items():
        result = resolve_expression(value, context)
        if result is None:
            static_headers[key] = value if isinstance(value, str) else str(value)
        elif result.kind == "literal":
            static_headers[key] = result.value
        elif result.kind == "dab_ref":
            preamble_lines.append(f'headers["{key}"] = dbutils.widgets.get("{key}")')
        elif result.kind == "notebook_code":
            for import_line in result.imports:
                preamble_lines.insert(0, import_line)
            preamble_lines.append(f'headers["{key}"] = {result.value}')

    headers_literal = json.dumps(static_headers) if static_headers else "{}"
    preamble = ""
    if preamble_lines:
        seen_lines: set[str] = set()
        unique_lines: list[str] = []
        for line in preamble_lines:
            if line not in seen_lines:
                seen_lines.add(line)
                unique_lines.append(line)
        preamble = "\n".join(unique_lines) + "\n"

    return headers_literal, preamble


def _render_sink_write(
    activity: CopyActivity,
    df_var: str = "df",
    *,
    mode: str = "overwrite",
    indent: str = "",
) -> str:
    """Return the Python ``df.write.*`` expression for the activity's sink.

    Args:
        activity: The CopyActivity.  Reads ``sink_format``,
            ``sink_resolved_path``, and ``sink_dataset_type``.
        df_var: The Python identifier of the DataFrame to write.
        mode: Spark write mode.  ``overwrite`` for full reads, ``append``
            for incremental ones (e.g. inside ForEach loops).
        indent: Prefix prepended to every emitted line, so the snippet drops
            cleanly into already-indented bodies.

    Returns:
        A multi-line Python snippet.  Trailing newline included.
    """
    fmt = activity.sink_format
    sink_props = activity.sink_properties or {}

    # File-format sink — write the actual format declared by the ADF
    # output dataset.  Delta files written with ``.save(path)`` skip the
    # metastore, which matches the ADF semantic of a path-based dataset.
    if fmt and fmt != "delta":
        opts: list[str] = []
        format_settings = sink_props.get("formatSettings") or {}
        if fmt == "csv":
            if format_settings.get("firstRowAsHeader") is not False:
                opts.append('.option("header", "true")')
            if format_settings.get("columnDelimiter"):
                delim = format_settings["columnDelimiter"]
                opts.append(f'.option("delimiter", "{delim}")')
        opts_str = "".join(opts)

        volume_relative = sink_props.get("volume_relative_path")
        if volume_relative is not None:
            # Volume-rooted sink: ``output_path_root`` is set by the bundler
            # as a base_parameter (with DAB-substituted ``${var.catalog}``
            # / ``${var.schema}``).  Any ``@{...}`` expressions in the
            # ADF dataset's folderPath / fileName have already been
            # rewritten to Python f-string fragments, so we wrap the
            # relative path in an f-string and join.
            rel_literal = volume_relative.replace('"', '\\"')
            preamble = ""
            # Pull in any modules the rewritten f-string fragments reference
            # so the notebook is runnable as-is.  Today the only one is
            # ``datetime`` (from ``@{formatDateTime(...)}`` rewrites).
            if "datetime." in rel_literal:
                preamble = f"{indent}from datetime import datetime\n"
            return (
                f"{preamble}"
                f"{indent}# Volume root is bound by the task's ``output_path_root`` parameter\n"
                f"{indent}# (resolved by DAB to /Volumes/<catalog>/<schema>/<volume>).\n"
                f'{indent}output_path_root = dbutils.widgets.get("output_path_root")\n'
                f'{indent}output_path = f"{{output_path_root}}/{rel_literal}"\n'
                f'{indent}{df_var}.write.format("{fmt}"){opts_str}.mode("{mode}").save(output_path)\n'
            )

        # No structured sink volume — fall back to a single ``output_path``
        # widget the user fills in.  Common when the linked service uses
        # a masked connection string and we can't reconstruct any path.
        return (
            f"{indent}# The ADF output dataset path could not be resolved at translation time.\n"
            f"{indent}# Set ``output_path`` on this task to the destination URI.\n"
            f'{indent}output_path = dbutils.widgets.get("output_path")\n'
            f'{indent}{df_var}.write.format("{fmt}"){opts_str}.mode("{mode}").save(output_path)\n'
        )

    # Delta sink (or unclassified fallback) — keep existing behaviour.
    if fmt == "delta" or fmt is None:
        if mode == "append":
            return (
                f'{indent}{df_var}.write.format("delta").mode("append")'
                f'.option("mergeSchema", "true").saveAsTable(target_table)\n'
            )
        return (
            f'{indent}{df_var}.write.format("delta").mode("{mode}")'
            f'.option("overwriteSchema", "true").saveAsTable(target_table)\n'
        )

    raise ValueError("Invalid fmt string")


def _infer_file_format(source_type: str | None, source_properties: dict | None) -> str:
    """Infer the file format from the source type string and source properties.

    Args:
        source_type: The ADF source type string (e.g. ``"DelimitedTextSource"``).
        source_properties: The source properties dict from the CopyActivity.

    Returns:
        File format string (e.g. ``"csv"``, ``"json"``, ``"parquet"``).
    """
    if source_type:
        type_lower = source_type.lower()
        if "delimitedtext" in type_lower or "csv" in type_lower:
            return "csv"
        if "json" in type_lower:
            return "json"
        if "parquet" in type_lower:
            return "parquet"
        if "avro" in type_lower:
            return "avro"
        if "orc" in type_lower:
            return "orc"

    if source_properties:
        fmt_settings = source_properties.get("formatSettings", {})
        fmt_type = fmt_settings.get("type", "")
        if "Csv" in fmt_type or "Delimited" in fmt_type:
            return "csv"
        if "Json" in fmt_type:
            return "json"
        if "Parquet" in fmt_type:
            return "parquet"
        if "Avro" in fmt_type:
            return "avro"
        if "Orc" in fmt_type:
            return "orc"
        store = source_properties.get("storeSettings", {})
        store_type = store.get("type", "")
        if "BinaryRead" in store_type:
            return "binaryFile"

    return "parquet"


def _generate_autoloader_body(activity: CopyActivity) -> str:
    """Generates Auto Loader ingestion body for file-based sources."""
    source_properties = activity.source_properties or {}
    sink_properties = activity.sink_properties or {}

    # Prefer the UC volume path when available (set by the copy preparer
    # when an external volume setup task is created); otherwise fall back
    # to the resolved abfss:// path or raw dataset path.
    source_path = source_properties.get(
        "volume_path",
        source_properties.get(
            "resolved_path",
            source_properties.get("path", source_properties.get("filePath", "/mnt/source")),
        ),
    )
    sink_table = sink_properties.get("table", sink_properties.get("tableName", f"{activity.task_key}_raw"))
    file_format = _infer_file_format(activity.source_type, source_properties)

    # Use the volume for checkpoints and schema evolution storage instead of
    # /tmp.  This ensures state persists across cluster restarts and is
    # visible in Unity Catalog.
    volume_base = source_properties.get("volume_base", "")
    if volume_base:
        checkpoint = f"{volume_base}/_checkpoints/{activity.task_key}"
    else:
        checkpoint = f"/tmp/checkpoints/{activity.task_key}"

    return textwrap.dedent(f"""\
        # Parameters
        source_path = dbutils.widgets.get("source_path") if dbutils.widgets.get("source_path") else "{source_path}"
        target_table = dbutils.widgets.get("target_table") if dbutils.widgets.get("target_table") else "{sink_table}"
        checkpoint_path = "{checkpoint}"

        # Auto Loader: stream file-based source into Delta table
        df = (
            spark.readStream.format("cloudFiles")
            .option("cloudFiles.format", "{file_format}")
            .option("cloudFiles.schemaLocation", checkpoint_path + "/_schema")
            .option("cloudFiles.inferColumnTypes", "true")
            .load(source_path)
        )

        # Write to Delta table
        (
            df.writeStream.format("delta")
            .option("checkpointLocation", checkpoint_path)
            .option("mergeSchema", "true")
            .outputMode("append")
            .trigger(availableNow=True)
            .toTable(target_table)
        )

    """)


def _adf_timeout_to_seconds(value: Any) -> int | None:
    """Parses ADF duration strings (e.g. ``"02:00:00"`` / ``"0.01:30:00"``) to seconds."""
    if not value:
        return None
    text = str(value).strip()
    if not text:
        return None
    if "." in text and text.split(".", 1)[0].isdigit():
        days_part, hms_part = text.split(".", 1)
        days = int(days_part)
    else:
        days = 0
        hms_part = text
    parts = hms_part.split(":")
    if len(parts) != 3:
        return None
    try:
        hours, minutes, seconds = (int(part) for part in parts)
    except ValueError:
        return None
    return days * 86400 + hours * 3600 + minutes * 60 + seconds


def _generate_jdbc_body(activity: CopyActivity, *, scope: str = "") -> str:
    """Generates JDBC ingestion body for database sources."""
    scope = scope or activity.task_key
    source_properties = activity.source_properties or {}
    sink_properties = activity.sink_properties or {}

    sink_table = sink_properties.get("table", sink_properties.get("tableName", f"{activity.task_key}_raw"))
    table_name = source_properties.get("tableName", source_properties.get("table", ""))
    query_raw = source_properties.get("sqlReaderQuery", source_properties.get("query", ""))
    query_timeout_seconds = _adf_timeout_to_seconds(source_properties.get("queryTimeout"))
    query_timeout_option = (
        f'\n            .option("queryTimeout", "{query_timeout_seconds}")' if query_timeout_seconds else ""
    )

    query = ""
    is_expression = False
    if isinstance(query_raw, dict) and query_raw.get("type") == "Expression":
        is_expression = True
        query = query_raw.get("value", "")
    elif isinstance(query_raw, str):
        query = query_raw
        if query.startswith("@"):
            is_expression = True

    if is_expression:
        # The query is an ADF expression (e.g. @concat('SELECT * FROM ', item().schema_name, ...)).
        # Generate a notebook that reads the current ForEach item from the
        # "item" widget (set to {{input}} by the for_each_task) and builds
        # the SQL query dynamically.
        return (
            textwrap.dedent(f"""\
            import json

            # Parameters
            default_table = "{sink_table}"
            target_table = dbutils.widgets.get("target_table") or default_table

            # The ForEach task passes the current item as the "item" widget
            # parameter via {{{{input}}}}.  Parse it as JSON to access fields.
            item_raw = dbutils.widgets.get("item")
            item = json.loads(item_raw) if item_raw else {{}}

            # Credentials
            jdbc_url = dbutils.secrets.get(scope="{scope}", key="jdbc-url")
            jdbc_password = dbutils.secrets.get(scope="{scope}", key="jdbc-password")
            jdbc_user = dbutils.secrets.get(scope="{scope}", key="jdbc-user")

            # Build the SQL query from the ForEach item fields.
            # Original ADF expression: {query}
            schema_name = item.get("schema_name", "dbo")
            table_name = item.get("table_name", "UNKNOWN_TABLE")
            query = f"SELECT * FROM {{schema_name}}.{{table_name}}"
            print(f"Executing query: {{query}}")

            # Read from source database via JDBC
            df = (
                spark.read.format("jdbc")
                .option("url", jdbc_url)
                .option("user", jdbc_user)
                .option("password", jdbc_password)
                .option("query", query){query_timeout_option}
                .load()
            )

            # Write to the sink defined by the ADF output dataset.  No
            # count/print: those trigger an extra Spark action and can
            # double the read cost.
        """)
            + _render_sink_write(activity, mode="append", indent="")
            + textwrap.dedent("""\

        """)
        )

    if table_name:
        read_option = f'    .option("dbtable", "{table_name}")'
    elif query and "@{" in query:
        # ``@{...}`` interpolation in a SQL query becomes an f-string so
        # ``dbutils.widgets.get(...)`` resolves at runtime.
        resolved_query = resolve_interpolated_string_for_notebook(query, TranslationContext())
        read_option = f'    .option("query", f"""{resolved_query}""")'
    elif query:
        read_option = f'    .option("query", """{query}""")'
    else:
        read_option = '    .option("dbtable", "REPLACE_WITH_TABLE_NAME")'

    return (
        textwrap.dedent(f"""\
        # Parameters
        target_table = dbutils.widgets.get("target_table") if dbutils.widgets.get("target_table") else "{sink_table}"

        # Credentials
        jdbc_url = dbutils.secrets.get(scope="{scope}", key="jdbc-url")
        jdbc_password = dbutils.secrets.get(scope="{scope}", key="jdbc-password")
        jdbc_user = dbutils.secrets.get(scope="{scope}", key="jdbc-user")

        # Read from source database via JDBC
        df = (
            spark.read.format("jdbc")
            .option("url", jdbc_url)
            .option("user", jdbc_user)
            .option("password", jdbc_password)
        {read_option}{query_timeout_option}
            .load()
        )

        # Write to the sink defined by the ADF output dataset.  No count/
        # print: those trigger an extra Spark action and can double the
        # read cost.
    """)
        + _render_sink_write(activity, mode="overwrite", indent="")
        + textwrap.dedent("""\

    """)
    )


def _generate_rest_copy_body(activity: CopyActivity) -> str:
    """Generates REST API ingestion body."""
    source_properties = activity.source_properties or {}
    sink_properties = activity.sink_properties or {}

    url = source_properties.get("url", source_properties.get("relativeUrl", ""))
    sink_table = sink_properties.get("table", sink_properties.get("tableName", f"{activity.task_key}_raw"))

    return (
        textwrap.dedent(f"""\
        import json
        import requests

        # Parameters
        url = dbutils.widgets.get("url") if dbutils.widgets.get("url") else "{url}"
        target_table = dbutils.widgets.get("target_table") if dbutils.widgets.get("target_table") else "{sink_table}"
        headers = {{"Content-Type": "application/json"}}

        # Fetch data from REST API
        response = requests.get(url, headers=headers, timeout=300)
        response.raise_for_status()
        data = response.json()

        # Normalize to list of records
        if isinstance(data, dict):
            for key in ("value", "data", "results", "items", "records"):
                if key in data and isinstance(data[key], list):
                    data = data[key]
                    break
            else:
                data = [data]

        # Write to the sink defined by the ADF output dataset.
        df = spark.createDataFrame(data)
    """)
        + _render_sink_write(activity, mode="overwrite", indent="")
        + textwrap.dedent("""\

    """)
    )


def _generate_generic_copy_body(activity: CopyActivity) -> str:
    """Generates a generic Spark read/write copy body as fallback."""
    source_properties = activity.source_properties or {}
    sink_properties = activity.sink_properties or {}

    # Use the resolved path from dataset if available, otherwise fall back
    source_path = source_properties.get(
        "resolved_path",
        source_properties.get("path", source_properties.get("filePath", "/mnt/source")),
    )
    sink_table = sink_properties.get("table", sink_properties.get("tableName", f"{activity.task_key}_raw"))
    file_format = _infer_file_format(activity.source_type, source_properties)

    return (
        textwrap.dedent(f"""\
        # Parameters
        source_path = dbutils.widgets.get("source_path") if dbutils.widgets.get("source_path") else "{source_path}"
        target_table = dbutils.widgets.get("target_table") if dbutils.widgets.get("target_table") else "{sink_table}"

        # Read source data
        df = spark.read.format("{file_format}").load(source_path)

        # Write to the sink defined by the ADF output dataset.
    """)
        + _render_sink_write(activity, mode="overwrite", indent="")
        + textwrap.dedent("""\

    """)
    )


def generate_motif_notebook(activity: MotifActivity) -> str:
    """Generates a notebook scaffold for a collapsed motif activity."""
    return _build_motif_notebook(
        task_key=activity.task_key,
        activity_name=activity.name,
        motif_id=activity.motif_id,
        databricks_replacement=activity.databricks_replacement,
        matched_activity_names=list(activity.matched_activity_names),
        source_type_hint=activity.source_type_hint or "",
        confidence_notes=list(activity.confidence_notes),
        motif_config=dict(activity.motif_config) if activity.motif_config else None,
    )


def _build_motif_notebook(
    *,
    task_key: str,
    activity_name: str,
    motif_id: str,
    databricks_replacement: str,
    matched_activity_names: list[str],
    source_type_hint: str,
    confidence_notes: list[str],
    motif_config: dict[str, Any] | None = None,
) -> str:
    """Builds the motif notebook scaffold body shared by both preparation paths."""
    matched_list = "\n".join(f"# MAGIC - `{name}`" for name in matched_activity_names)
    notes_list = "\n".join(f"# MAGIC - {note}" for note in confidence_notes) if confidence_notes else "# MAGIC   (none)"

    source_line = f"# MAGIC **Source type**: `{source_type_hint}`" if source_type_hint else ""

    lines = [
        "# Databricks notebook source",
        "# MAGIC %md",
        f"# MAGIC # Motif: {activity_name}",
        "# MAGIC",
        f"# MAGIC **Pattern**: `{motif_id}`",
        f"# MAGIC **Databricks replacement**: `{databricks_replacement}`",
    ]
    if source_line:
        lines.append(source_line)
    lines.extend(
        [
            "# MAGIC",
            "# MAGIC ## Collapsed ADF Activities",
            "# MAGIC",
            matched_list,
            "# MAGIC",
            "# MAGIC ## Detection Notes",
            "# MAGIC",
            notes_list,
            "# MAGIC",
            "# MAGIC *Auto-generated by Orchestra motif collapser.*",
            "",
            "# COMMAND ----------",
            "",
        ]
    )

    if databricks_replacement == "auto_loader":
        lines.extend(_auto_loader_motif_body(task_key))
    elif databricks_replacement == "dlt_apply_changes":
        lines.extend(_dlt_apply_changes_motif_body(motif_id))
    elif databricks_replacement == "for_each_ingestion":
        lines.extend(_for_each_ingestion_motif_body(task_key, motif_config or {}))
    elif databricks_replacement == "python_rest_ingestion":
        lines.extend(_python_rest_ingestion_motif_body())
    elif databricks_replacement == "auto_loader_file_notification":
        lines.extend(_auto_loader_file_notification_motif_body(task_key))
    else:
        lines.extend(
            [
                f"# TODO: Implement Databricks-native replacement for motif '{motif_id}'",
                f"# Strategy: {databricks_replacement}",
                f"raise NotImplementedError('Motif {motif_id}: implement {databricks_replacement}')",
            ]
        )

    lines.append("")
    return "\n".join(lines)


def _auto_loader_motif_body(task_key: str) -> list[str]:
    return [
        "# Auto Loader ingestion -- replaces Lookup/Copy/StoredProcedure watermark chain",
        "source_path = dbutils.widgets.get('source_path')",
        "target_table = dbutils.widgets.get('target_table')",
        f"checkpoint_path = '/tmp/checkpoints/{task_key}'",
        "",
        "df = (",
        '    spark.readStream.format("cloudFiles")',
        '    .option("cloudFiles.format", "parquet")',
        '    .option("cloudFiles.schemaLocation", checkpoint_path + "/_schema")',
        "    .load(source_path)",
        ")",
        "",
        "(",
        '    df.writeStream.format("delta")',
        '    .option("checkpointLocation", checkpoint_path)',
        '    .option("mergeSchema", "true")',
        "    .outputMode('append')",
        "    .trigger(availableNow=True)",
        "    .toTable(target_table)",
        ")",
    ]


def _dlt_apply_changes_motif_body(motif_id: str) -> list[str]:
    return [
        "# DLT APPLY CHANGES -- replaces Copy/DataFlow SCD or CDC chain",
        "# This motif is best implemented as a DLT pipeline definition.",
        "# See: https://docs.databricks.com/en/delta-live-tables/cdc.html",
        "",
        "# import dlt",
        "# @dlt.table",
        "# def target_table():",
        "#     return spark.readStream.table('staging_table')",
        "#",
        "# dlt.apply_changes(",
        "#     target='target_table',",
        "#     source='staging_table',",
        "#     keys=['id'],",
        "#     sequence_by='updated_at',",
        "# )",
        "",
        f"raise NotImplementedError('Motif {motif_id}: implement as DLT pipeline')",
    ]


def _for_each_ingestion_motif_body(task_key: str, motif_config: dict[str, Any]) -> list[str]:
    lookup_query = motif_config.get("lookup_query", "")
    lookup_scope = motif_config.get("lookup_scope") or task_key
    copy_scope = motif_config.get("copy_scope") or task_key
    sink_table_pattern = motif_config.get("sink_table") or "raw.{schema_name}_{table_name}"
    return [
        "# Parameterised bulk ingestion -- replaces the collapsed Lookup/ForEach/Copy chain.",
        "import json",
        "",
        f"lookup_jdbc_url = dbutils.secrets.get(scope='{lookup_scope}', key='jdbc-url')",
        f"lookup_jdbc_user = dbutils.secrets.get(scope='{lookup_scope}', key='jdbc-user')",
        f"lookup_jdbc_password = dbutils.secrets.get(scope='{lookup_scope}', key='jdbc-password')",
        "",
        "items_override = dbutils.widgets.get('items')",
        "if items_override:",
        "    items = json.loads(items_override)",
        "else:",
        f"    control_query = {lookup_query!r}",
        "    control_df = (",
        "        spark.read.format('jdbc')",
        "        .option('url', lookup_jdbc_url)",
        "        .option('user', lookup_jdbc_user)",
        "        .option('password', lookup_jdbc_password)",
        "        .option('query', control_query)",
        "        .load()",
        "    )",
        "    items = [row.asDict() for row in control_df.collect()]",
        "",
        f"copy_jdbc_url = dbutils.secrets.get(scope='{copy_scope}', key='jdbc-url')",
        f"copy_jdbc_user = dbutils.secrets.get(scope='{copy_scope}', key='jdbc-user')",
        f"copy_jdbc_password = dbutils.secrets.get(scope='{copy_scope}', key='jdbc-password')",
        "",
        "for item in items:",
        "    table_name = item.get('table_name') or item.get('name') or 'UNKNOWN_TABLE'",
        "    schema_name = item.get('schema_name', 'dbo')",
        f"    target = {sink_table_pattern!r}.format(schema_name=schema_name, table_name=table_name)",
        "    query = f'SELECT * FROM {schema_name}.{table_name}'",
        "    (",
        "        spark.read.format('jdbc')",
        "        .option('url', copy_jdbc_url)",
        "        .option('user', copy_jdbc_user)",
        "        .option('password', copy_jdbc_password)",
        "        .option('query', query)",
        "        .load()",
        "        .write.format('delta')",
        "        .mode('overwrite')",
        "        .option('overwriteSchema', 'true')",
        "        .saveAsTable(target)",
        "    )",
        "",
        "dbutils.notebook.exit(json.dumps({'ingested_tables': len(items)}))",
    ]


def _python_rest_ingestion_motif_body() -> list[str]:
    return [
        "# REST API pagination -- replaces WebActivity/Until/SetVariable chain",
        "import json",
        "import requests",
        "",
        "base_url = dbutils.widgets.get('api_url')",
        "auth_token = dbutils.secrets.get(scope='rest_api', key='token')",
        "headers = {'Authorization': f'Bearer {auth_token}'}",
        "",
        "all_records = []",
        "next_url = base_url",
        "",
        "while next_url:",
        "    response = requests.get(next_url, headers=headers, timeout=60)",
        "    response.raise_for_status()",
        "    data = response.json()",
        "    records = data.get('value', data.get('data', []))",
        "    all_records.extend(records)",
        "    next_url = data.get('nextLink') or data.get('@odata.nextLink')",
        "",
        "df = spark.createDataFrame(all_records)",
        "target_table = dbutils.widgets.get('target_table')",
        "df.write.format('delta').mode('overwrite').saveAsTable(target_table)",
        "print(f'Ingested {len(all_records)} records')",
    ]


def _auto_loader_file_notification_motif_body(task_key: str) -> list[str]:
    return [
        "# Auto Loader with file notification -- replaces GetMetadata/ForEach/Copy/Delete chain",
        "source_path = dbutils.widgets.get('source_path')",
        "target_table = dbutils.widgets.get('target_table')",
        f"checkpoint_path = '/tmp/checkpoints/{task_key}'",
        "",
        "df = (",
        '    spark.readStream.format("cloudFiles")',
        '    .option("cloudFiles.format", "parquet")',
        '    .option("cloudFiles.useNotifications", "true")',
        '    .option("cloudFiles.schemaLocation", checkpoint_path + "/_schema")',
        "    .load(source_path)",
        ")",
        "",
        "(",
        '    df.writeStream.format("delta")',
        '    .option("checkpointLocation", checkpoint_path)',
        "    .outputMode('append')",
        "    .trigger(availableNow=True)",
        "    .toTable(target_table)",
        ")",
    ]
