"""Airflow PapermillOperator support: find the notebook a task runs and turn it into a Databricks notebook.

Papermill runs a Jupyter notebook top to bottom. It overrides the values in the cell tagged ``parameters``
by inserting a new cell after it, and it saves the executed copy to ``output_nb``. A Databricks notebook task
does the same job natively: ``base_parameters`` arrive as widgets, and every job run keeps the executed
notebook with its outputs. So a PapermillOperator task becomes a notebook task whose notebook is the
original ``.ipynb`` converted to Databricks source format, with the ``parameters`` cell rewritten to read
widgets.

The notebook path a DAG names usually lives on the JupyterHub or Airflow machine (``/home/<user>/...``), so
it is found the way flowx finds a dbt project: at that path when it lies inside the folder discover reads,
otherwise as the one file of the same name under that folder.
"""

from __future__ import annotations

import ast
import posixpath
from dataclasses import dataclass, field
from pathlib import Path, PurePosixPath
from typing import Any

from flowx.sources.airflow.template_files import find_exported_file

_UNRESOLVED = object()

_PATH_CONSTRUCTORS = frozenset(
    {
        "Path",
        "PurePath",
        "PosixPath",
        "PurePosixPath",
        "pathlib.Path",
        "pathlib.PurePath",
        "pathlib.PosixPath",
        "pathlib.PurePosixPath",
    }
)
_STRING_WRAPPERS = frozenset({"str", "os.fspath", "os.path.expanduser", "path.expanduser"})
_JOIN_FUNCTIONS = frozenset({"os.path.join", "path.join", "posixpath.join"})

# Cell magics with a direct Databricks equivalent.
_CELL_MAGICS = {"%%sql": "%sql", "%%sh": "%sh", "%%bash": "%sh", "%%markdown": "%md", "%%md": "%md"}
# Magics that only change how an interactive session displays or times work, so a job run can drop them.
# %%capture is droppable only without a variable name (see _capture_stores_output).
_DROPPABLE_CELL_MAGICS = frozenset({"%%time", "%%timeit", "%%python"})
_DROPPABLE_LINE_MAGICS = frozenset({"%matplotlib", "%autoreload"})
# %load_ext and %config are droppable only for the extensions and settings listed here; any other one changes
# behavior, so it stays a gap.
_DROPPABLE_EXTENSIONS = frozenset({"autoreload", "sql"})
_DROPPABLE_CONFIG_PREFIXES = ("InlineBackend.", "Completer.", "IPCompleter.", "SqlMagic.")
_TIMING_LINE_MAGICS = ("%time ", "%timeit ")
_PIP_PREFIXES = ("!pip ", "%pip ", "!pip3 ", "!python -m pip ", "!python3 -m pip ")

_PYTHON_KERNEL_PREFIXES = ("python", "py", "ipykernel", "pyspark", "spark")
_CELL_SEPARATOR = "\n# COMMAND ----------\n\n"
_PARAMETER_HELPER = "_papermill_parameter"


class NotebookConversionError(ValueError):
    """Raised when a notebook holds something with no deterministic Databricks equivalent."""


@dataclass(slots=True, kw_only=True)
class ConvertedNotebook:
    """A Jupyter notebook rendered as Databricks notebook source.

    Attributes:
        source: The notebook in Databricks source format, ready to write into the bundle.
        parameter_names: Names the rewritten ``parameters`` cell reads from widgets, in order.
        notes: Plain-language notes on each change the conversion made, for the task's disclosure.
    """

    source: str
    parameter_names: list[str] = field(default_factory=list)
    notes: list[str] = field(default_factory=list)


# --------------------------------------------------------------------------------------
# Reading the operator's arguments
# --------------------------------------------------------------------------------------


def module_constants(source: str) -> dict[str, ast.expr]:
    """Collects the ``NAME = expression`` assignments at the top level of a DAG module.

    A DAG usually builds a notebook path from a few module-level names (``home_dir = Path("/home", user)``).
    A name assigned more than once is left out, since which value a task sees depends on ordering.

    Args:
        source: The DAG module's source text.

    Returns:
        Each singly assigned top-level name mapped to the expression assigned to it.
    """
    try:
        module = ast.parse(source)
    except SyntaxError:
        return {}
    assignments: dict[str, ast.expr] = {}
    reassigned: set[str] = set()
    for statement in module.body:
        if isinstance(statement, ast.Assign) and len(statement.targets) == 1:
            target, value = statement.targets[0], statement.value
        elif isinstance(statement, ast.AnnAssign) and statement.value is not None:
            target, value = statement.target, statement.value
        else:
            continue
        if not isinstance(target, ast.Name):
            continue
        if target.id in assignments:
            reassigned.add(target.id)
        assignments[target.id] = value
    return {name: value for name, value in assignments.items() if name not in reassigned}


def static_value(node: ast.expr | None, constants: dict[str, ast.expr]) -> Any:
    """Evaluates a literal, a module-level name, or a path or string built from them.

    Args:
        node: The expression to evaluate, such as an operator argument.
        constants: Module-level names from :func:`module_constants`.

    Returns:
        The value, or the module's unresolved marker (test with :func:`is_resolved`) for anything that would
        need the DAG to run, such as a function call other than the path and string helpers.
    """
    return _evaluate(node, constants, frozenset()) if node is not None else _UNRESOLVED


def is_resolved(value: Any) -> bool:
    """Returns whether :func:`static_value` produced a value."""
    return value is not _UNRESOLVED


def _evaluate(node: ast.expr, constants: dict[str, ast.expr], visiting: frozenset[str]) -> Any:
    if isinstance(node, ast.Constant):
        return node.value
    if isinstance(node, ast.Name):
        if node.id in visiting or node.id not in constants:
            return _UNRESOLVED
        return _evaluate(constants[node.id], constants, visiting | {node.id})
    if isinstance(node, (ast.List, ast.Tuple)):
        items = [_evaluate(item, constants, visiting) for item in node.elts]
        if not all(is_resolved(item) for item in items):
            return _UNRESOLVED
        return items if isinstance(node, ast.List) else tuple(items)
    if isinstance(node, ast.Dict):
        if any(key is None for key in node.keys):
            return _UNRESOLVED
        keys = [_evaluate(key, constants, visiting) for key in node.keys if key is not None]
        values = [_evaluate(value, constants, visiting) for value in node.values]
        if not all(is_resolved(item) for item in [*keys, *values]):
            return _UNRESOLVED
        return dict(zip(keys, values))
    if isinstance(node, ast.JoinedStr):
        parts: list[str] = []
        for item in node.values:
            if isinstance(item, ast.Constant) and isinstance(item.value, str):
                parts.append(item.value)
            elif isinstance(item, ast.FormattedValue) and item.conversion == -1 and item.format_spec is None:
                value = _evaluate(item.value, constants, visiting)
                if not is_resolved(value):
                    return _UNRESOLVED
                parts.append(str(value))
            else:
                return _UNRESOLVED
        return "".join(parts)
    if isinstance(node, ast.BinOp):
        left = _evaluate(node.left, constants, visiting)
        right = _evaluate(node.right, constants, visiting)
        if not (is_resolved(left) and is_resolved(right)):
            return _UNRESOLVED
        if isinstance(node.op, ast.Add) and isinstance(left, str) and isinstance(right, str):
            return left + right
        if isinstance(node.op, ast.Div) and isinstance(left, str) and isinstance(right, str):
            return str(PurePosixPath(left) / right)
        return _UNRESOLVED
    if isinstance(node, ast.Attribute) and node.attr in ("parent", "name", "stem"):
        inner = _evaluate(node.value, constants, visiting)
        return str(getattr(PurePosixPath(inner), node.attr)) if isinstance(inner, str) else _UNRESOLVED
    if isinstance(node, ast.Call) and not node.keywords:
        return _evaluate_call(node, constants, visiting)
    return _UNRESOLVED


def _evaluate_call(node: ast.Call, constants: dict[str, ast.expr], visiting: frozenset[str]) -> Any:
    name = _dotted_name(node.func)
    arguments = [_evaluate(argument, constants, visiting) for argument in node.args]
    if not all(is_resolved(argument) and isinstance(argument, str) for argument in arguments):
        return _UNRESOLVED
    if name in _PATH_CONSTRUCTORS and arguments:
        return str(PurePosixPath(*arguments))
    if name in _STRING_WRAPPERS and len(arguments) == 1:
        return arguments[0]
    if name in _JOIN_FUNCTIONS and arguments:
        return posixpath.join(*arguments)
    return _UNRESOLVED


def _dotted_name(node: ast.expr) -> str:
    if isinstance(node, ast.Name):
        return node.id
    if isinstance(node, ast.Attribute):
        inner = _dotted_name(node.value)
        return f"{inner}.{node.attr}" if inner else ""
    return ""


def is_python_kernel(kernel_name: str | None) -> bool:
    """Returns whether a Jupyter kernel name runs Python, so the notebook can run as a Python notebook."""
    return kernel_name is None or kernel_name.lower().startswith(_PYTHON_KERNEL_PREFIXES)


def parameter_text(value: Any) -> str:
    """Renders a papermill parameter value as the text a Databricks widget carries."""
    return value if isinstance(value, str) else repr(value)


# --------------------------------------------------------------------------------------
# Finding the notebook
# --------------------------------------------------------------------------------------


def locate_notebook(
    notebook_path: str, search_paths: tuple[Path, ...], source_root: Path | None
) -> tuple[Path | None, str]:
    """Finds the notebook a task names, reading only inside the folders discover was given.

    See :func:`~flowx.sources.airflow.template_files.find_exported_file` for how a path on the Airflow or
    JupyterHub machine is matched to the export.

    Args:
        notebook_path: The ``input_nb`` path as the DAG names it.
        search_paths: The DAG folder, then the DAG's ``template_searchpath``.
        source_root: The folder discover was pointed at.

    Returns:
        The notebook's local path and that path as text, or None and the reason it was not found.
    """
    if "://" in notebook_path:
        scheme = notebook_path.split("://", 1)[0]
        return None, f"is a remote {scheme}:// path; copy the notebook into the folder discover reads"
    if PurePosixPath(notebook_path).suffix != ".ipynb":
        return None, "does not name a .ipynb notebook"
    return find_exported_file(notebook_path, search_paths, source_root)


# --------------------------------------------------------------------------------------
# Converting the notebook
# --------------------------------------------------------------------------------------


def convert_notebook(
    document: dict[str, Any], *, task_id: str, notebook_name: str, passed_parameters: dict[str, Any]
) -> ConvertedNotebook:
    """Renders a Jupyter notebook (nbformat 4) as a Databricks source notebook.

    Markdown cells become ``%md`` cells and code cells stay Python. ``%%sql`` and ``%%bash`` cells become
    ``%sql`` and ``%sh`` cells, ``!pip install`` becomes ``%pip install``, and magics that only affect an
    interactive session (``%matplotlib``, ``%%time``) are dropped. The cell tagged ``parameters`` is
    rewritten to read each value from a widget, keeping its value as the widget default. Parameters the DAG
    passes that the cell does not declare get a widget too, as papermill would inject them. A cell papermill
    itself injected into an earlier executed copy (tagged ``injected-parameters``) is dropped.

    Args:
        document: The notebook's parsed JSON.
        task_id: The Airflow task that runs it, named in the notebook header.
        notebook_name: The notebook's file name, named in the notebook header.
        passed_parameters: The ``parameters`` the DAG passes, with their Python values.

    Returns:
        The Databricks source, the widget names it reads, and notes on each change made.

    Raises:
        NotebookConversionError: for an older notebook format, a non-Python kernel, or a magic or shell
            command with no deterministic Databricks equivalent.
    """
    if document.get("nbformat", 4) < 4 or not isinstance(document.get("cells"), list):
        raise NotebookConversionError("is not an nbformat 4 notebook; upgrade it with nbconvert first")
    language = _notebook_language(document)
    if language not in ("", "python"):
        raise NotebookConversionError(f"runs on a {language} kernel; only Python notebooks convert deterministically")

    converted = ConvertedNotebook(source="")
    cells: list[str] = []
    declared: list[str] = []
    saw_parameters_cell = False
    for index, cell in enumerate(document["cells"]):
        kind = cell.get("cell_type")
        text = _cell_text(cell)
        tags = set(cell.get("metadata", {}).get("tags", []) or [])
        if not text.strip():
            continue
        if "injected-parameters" in tags:
            converted.notes.append("dropped the injected-parameters cell left by an earlier papermill run")
            continue
        if kind == "markdown":
            cells.append(_markdown_cell(text))
        elif kind == "raw":
            converted.notes.append(f"kept raw cell {index + 1} as a comment")
            cells.append("".join(f"# {line}\n" if line else "#\n" for line in text.splitlines()))
        elif kind == "code" and "parameters" in tags and not saw_parameters_cell:
            saw_parameters_cell = True
            cell_source, names = _parameters_cell(text, passed_parameters)
            declared.extend(names)
            cells.append(cell_source)
        elif kind == "code":
            code_cells, notes = _code_cells(text, index)
            cells.extend(code_cells)
            converted.notes.extend(notes)

    undeclared = [name for name in passed_parameters if name not in declared]
    if undeclared:
        injected = _injected_parameters_cell(undeclared, passed_parameters)
        if saw_parameters_cell:
            parameters_position = next(
                position for position, cell in enumerate(cells) if _PARAMETER_HELPER in cell and "def " in cell
            )
            cells.insert(parameters_position + 1, injected)
        else:
            cells.insert(0, _helper_definition() + "\n" + injected)
        declared.extend(undeclared)
        converted.notes.append(
            "added widgets for parameters the DAG passes that the notebook does not declare: " + ", ".join(undeclared)
        )
    if saw_parameters_cell:
        converted.notes.append("rewrote the parameters cell to read widgets, keeping its values as widget defaults")

    # The provenance comment gets a cell of its own: Databricks reads a cell as markdown or SQL only when the
    # magic is the cell's first line, so a comment above it turns the whole cell back into Python.
    provenance = f"# Migrated from Airflow PapermillOperator '{task_id}' (notebook {notebook_name}).\n"
    converted.source = "# Databricks notebook source\n" + _CELL_SEPARATOR.join(
        cell.rstrip("\n") + "\n" for cell in [provenance, *cells]
    )
    converted.parameter_names = declared
    return converted


def _notebook_language(document: dict[str, Any]) -> str:
    metadata = document.get("metadata", {}) or {}
    language = (metadata.get("kernelspec", {}) or {}).get("language") or (metadata.get("language_info", {}) or {}).get(
        "name"
    )
    return str(language or "").lower()


def _cell_text(cell: dict[str, Any]) -> str:
    source = cell.get("source", "")
    return "".join(source) if isinstance(source, list) else str(source)


def _markdown_cell(text: str) -> str:
    lines = text.splitlines()
    rendered = ["# MAGIC %md"] + [f"# MAGIC {_inert(line)}" if line else "# MAGIC" for line in lines]
    return "\n".join(rendered) + "\n"


def _magic_cell(magic: str, lines: list[str]) -> str:
    rendered = [f"# MAGIC {magic}"] + [f"# MAGIC {_inert(line)}" if line else "# MAGIC" for line in lines]
    return "\n".join(rendered) + "\n"


def _inert(line: str) -> str:
    """Keeps notebook source directives inside a cell from being read as cell boundaries."""
    stripped = line.lstrip()
    if stripped.startswith("# MAGIC") or stripped.startswith("# COMMAND ----------"):
        return line[: len(line) - len(stripped)] + "#" + stripped
    return line


def _code_cells(text: str, index: int) -> tuple[list[str], list[str]]:
    """Converts one Jupyter code cell into Databricks cells, returning them with notes on what changed."""
    notes: list[str] = []
    lines = text.splitlines()
    first = lines[0].strip()
    if first.startswith("%%"):
        magic = first.split()[0]
        if magic in _CELL_MAGICS:
            return [_magic_cell(_CELL_MAGICS[magic], lines[1:])], notes
        if magic == "%%capture" and _capture_stores_output(first):
            raise NotebookConversionError(
                f"cell {index + 1} uses %%capture to store its output in a variable, which has no Databricks form"
            )
        if magic in _DROPPABLE_CELL_MAGICS or magic == "%%capture":
            notes.append(f"dropped the {magic} magic from cell {index + 1}")
            lines = lines[1:]
        else:
            raise NotebookConversionError(f"cell {index + 1} uses the {magic} cell magic, which has no Databricks form")

    pip_lines: list[str] = []
    python_lines: list[str] = []
    inside_strings = _lines_inside_strings(lines)
    for position, line in enumerate(lines):
        stripped = line.strip()
        if position in inside_strings:
            python_lines.append(_inert(line))
        elif stripped.startswith(_PIP_PREFIXES):
            arguments = stripped.split(" pip ", 1)[-1] if " pip " in stripped else stripped.split(" ", 1)[1]
            pip_lines.append(f"%pip {arguments}")
        elif stripped.startswith(_TIMING_LINE_MAGICS):
            notes.append(f"dropped a timing magic in cell {index + 1}")
            python_lines.append(line.replace(stripped, stripped.split(" ", 1)[1]))
        elif stripped.startswith("%") and _is_droppable_line_magic(stripped):
            notes.append(f"dropped {stripped.split()[0]} in cell {index + 1}")
        elif stripped.startswith(("%", "!")):
            raise NotebookConversionError(
                f"cell {index + 1} runs {stripped.split()[0]!r}, a magic or shell command with no deterministic "
                "Databricks form; move it to its own %sh cell or a separate task"
            )
        else:
            python_lines.append(_inert(line))
    cells: list[str] = []
    if pip_lines:
        notes.append(f"moved pip installs in cell {index + 1} into a %pip cell")
        cells.append(_magic_cell(pip_lines[0], pip_lines[1:]) if len(pip_lines) == 1 else _pip_cell(pip_lines))
    if any(line.strip() for line in python_lines):
        cells.append("\n".join(python_lines) + "\n")
    return cells, notes


def _capture_stores_output(first_line: str) -> bool:
    """Returns whether a ``%%capture`` line names a variable. Its flags (``--no-stderr`` and so on) do not."""
    return any(not token.startswith("-") for token in first_line.split()[1:])


def _is_droppable_line_magic(line: str) -> bool:
    """Returns whether a line magic only changes an interactive session, so a job run can drop it."""
    parts = line.split()
    magic = parts[0]
    if magic in _DROPPABLE_LINE_MAGICS:
        return True
    if magic == "%load_ext":
        return len(parts) == 2 and parts[1] in _DROPPABLE_EXTENSIONS
    if magic == "%config":
        # A bare %config only lists the settings, so it changes nothing.
        return len(parts) == 1 or parts[1].startswith(_DROPPABLE_CONFIG_PREFIXES)
    return False


def _lines_inside_strings(lines: list[str]) -> set[int]:
    """Returns the positions of lines that begin inside a triple-quoted string.

    A ``%`` or ``!`` at the start of such a line is text, not a magic, so it must be left alone. This is a
    line scanner rather than a tokenizer because a cell with magics is not valid Python.
    """
    inside: set[int] = set()
    open_quote: str | None = None
    for position, line in enumerate(lines):
        if open_quote is not None:
            inside.add(position)
        index = 0
        while index < len(line):
            if open_quote is None and line[index] == "#":
                break
            quote = line[index : index + 3]
            if quote in ('"""', "'''") and open_quote in (None, quote):
                open_quote = None if open_quote == quote else quote
                index += 3
                continue
            index += 1
    return inside


def _pip_cell(pip_lines: list[str]) -> str:
    # Databricks runs one %pip command per cell, so each install keeps its own cell.
    return _CELL_SEPARATOR.join(f"# MAGIC {line}\n" for line in pip_lines)


def _helper_definition() -> str:
    # The widget name is a variable here on purpose: flowx declares every dbutils.widgets.get('literal')
    # it sees in generated source as a job parameter, and a job parameter would override the task's
    # base_parameters, replacing the value the DAG passes with an empty default.
    return (
        "import ast\n\n"
        f"def {_PARAMETER_HELPER}(name, default, as_text):\n"
        '    """Reads a papermill parameter from its widget, keeping the notebook value as the default."""\n'
        "    dbutils.widgets.text(name, default)\n"
        "    value = dbutils.widgets.get(name)\n"
        "    if as_text:\n"
        "        return value\n"
        "    try:\n"
        "        return ast.literal_eval(value)\n"
        "    except (ValueError, SyntaxError):\n"
        "        return value\n"
    )


def _parameters_cell(text: str, passed_parameters: dict[str, Any]) -> tuple[str, list[str]]:
    """Rewrites the ``parameters`` cell so each simple assignment reads its value from a widget."""
    try:
        module = ast.parse(text)
    except SyntaxError as error:
        raise NotebookConversionError(f"the parameters cell is not valid Python ({error.msg})") from error
    if "dbutils.widgets" in text:
        # A cell already rewritten by hand would get a second set of widgets, and the DAG's values could be lost.
        raise NotebookConversionError(
            "has a parameters cell that already reads dbutils widgets; restore its plain assignments "
            "(name = value) so the DAG's parameters map to widgets"
        )
    lines = [_helper_definition()]
    names: list[str] = []
    for statement in module.body:
        target = _assignment_target(statement)
        value_node = getattr(statement, "value", None)
        if target is None or value_node is None:
            lines.append(ast.get_source_segment(text, statement) or ast.unparse(statement))
            continue
        try:
            default = ast.literal_eval(value_node)
        except (ValueError, SyntaxError):
            lines.append(ast.get_source_segment(text, statement) or ast.unparse(statement))
            continue
        names.append(target)
        # Papermill injects the DAG's value with its own type, so that type wins over the notebook default's.
        as_text = isinstance(passed_parameters.get(target, default), str)
        lines.append(f"{target} = {_PARAMETER_HELPER}({target!r}, {parameter_text(default)!r}, as_text={as_text})")
    return "\n".join(lines) + "\n", names


def _assignment_target(statement: ast.stmt) -> str | None:
    if isinstance(statement, ast.Assign) and len(statement.targets) == 1 and isinstance(statement.targets[0], ast.Name):
        return statement.targets[0].id
    if isinstance(statement, ast.AnnAssign) and isinstance(statement.target, ast.Name):
        return statement.target.id
    return None


def _injected_parameters_cell(names: list[str], passed_parameters: dict[str, Any]) -> str:
    lines = [
        f"{name} = {_PARAMETER_HELPER}({name!r}, '', as_text={isinstance(passed_parameters[name], str)})"
        for name in names
    ]
    return "\n".join(lines) + "\n"
