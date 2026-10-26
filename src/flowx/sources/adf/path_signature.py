"""Structural path signature of an ADF dataset (the weak lineage key).

The folder and file name with only provably literal text kept, and one ``<P>``
slot for every run-time value, so two references join only on the same literal
folder layout.
"""

from __future__ import annotations

import re
from typing import Any

from flowx.models.adf_ast import AdfDatasetReference, AdfDefinitions
from flowx.sources.adf.dataset_identity import (
    _DATASET_PARAMETER_RE,
    _call_site_parameters,
    _dataset_props,
    _effective_dataset_params,
    _expression_value,
    _is_arm_expression,
)

_QUOTED_LITERAL_RE = re.compile(r"'((?:[^']|'')*)'", re.DOTALL)
_CONCAT_CALL_RE = re.compile(r"concat\s*\((?P<arguments>.*)\)", re.IGNORECASE | re.DOTALL)
_NUMBER_LITERAL_RE = re.compile(r"-?\d+(?:\.\d+)?")
_RUNTIME_SLOT_RE = re.compile(r"<P(?::(?:[^<>]|<P>)*)?>")


# The rule: only provably literal text forms the path. Any other value, whatever
# function or reference it is, is one placeholder slot where it sits, so no link is
# ever made from a non-literal value standing in for a folder. A function over a bound
# dataset() value is a slot keyed by that value, never path text.
# tests/unit/test_adf_path_signature_corpus.py checks this over generated
# expressions: add a new ADF construct there rather than patching the shape for it.


def _path_signature(dataset_ref: AdfDatasetReference, definitions: AdfDefinitions) -> str | None:
    """Structural signature of the folder and file name a parameterised reference points at.

    Both of these must hold, or the result is ``None``:

    1. The reference passes a ``folderPath`` at the call site with literal text in it:
       quoted text or a number in an expression, or text outside ``@{...}``, so
       ``landing/@{item().run}`` qualifies just as ``@concat('landing/', item().run)``
       does. The dataset's own location is not enough on its own: a reference that
       passes no anchored ``folderPath`` gets no signature. A file name alone never
       qualifies, since many unrelated activities write ``.csv`` files to opaque folders.
    2. The folder the reference resolves to has a literal segment. That folder is the
       dataset's own ``location.folderPath`` with each ``dataset().<name>`` filled in
       from the reference's parameters (call-site values, then declared defaults), so
       ``bronze/`` and ``silver/`` datasets called with the same value stay apart. When
       the location sets no folder, or the dataset is not in the export, the call-site
       ``folderPath`` is used instead.

    The file name is read the same way; one with no literal text gives ``FN[]``. A
    location folder or file name the code cannot read (a list, or an object of a type
    other than Expression or SecureString) is unknown, so it gives ``None`` rather than
    borrowing the call site's. An untyped object is read by its ``value``.
    """
    call_site = _call_site_parameters(dataset_ref)
    passed_folder = _path_shape(call_site.get("folderPath"), {}, require_physical=True)
    if passed_folder is None or not _has_literal_anchor(passed_folder):
        return None
    properties = _dataset_props(dataset_ref, definitions) or {}
    type_props = properties.get("typeProperties") or properties
    written_location = type_props.get("location")
    location: dict[str, Any] = written_location if isinstance(written_location, dict) else {}
    written_values = {key: _expression_value(location.get(key)) for key in ("folderPath", "fileName")}
    if any(value is not None and not isinstance(value, str) for value in written_values.values()):
        return None
    bindings = {
        name: value
        for name, value in _effective_dataset_params(dataset_ref, properties).items()
        if _reads_as_text(value)
    }

    def _shape_of(key: str, *, require_physical: bool) -> str | None:
        written = written_values[key]
        if isinstance(written, str) and written.strip():
            return _path_shape(written, bindings, require_physical=require_physical)
        return _path_shape(call_site.get(key), {}, require_physical=require_physical)

    folder_shape = _shape_of("folderPath", require_physical=True)
    if folder_shape is None or not _has_literal_anchor(folder_shape):
        return None
    file_shape = _shape_of("fileName", require_physical=False)
    names_a_file = file_shape and (_has_literal_anchor(file_shape) or "<P:" in file_shape)
    return f"FP[{folder_shape}]/FN[{file_shape if names_a_file else ''}]"


def _reads_as_text(value: Any) -> bool:
    """``True`` when a bound parameter *value* is text ADF can splice into a path, even ``""``.

    That is a string, number or boolean, given bare or as the ``value`` of an Expression
    or SecureString object (or of an untyped one). A
    ``null``, a list, or an Expression with no ``value`` says nothing about the path,
    so it is no more known than a parameter the call site never bound.
    """
    return isinstance(_expression_value(value), (str, bool, int, float))


def _path_shape(expression: Any, bindings: dict[str, Any], *, require_physical: bool) -> str | None:
    """Reduce a (possibly parameterised) ADF path expression to its structural shape.

    Keeps the literal path text in place and writes ``<P>`` where each runtime value
    sits, so ``landing/<P>`` and ``<P>/landing`` stay apart, without guessing the
    runtime value. A ``dataset().<name>`` bound in *bindings* is replaced by its bound
    value's shape instead. Repeated slashes collapse, and leading and trailing ones go,
    only once the whole shape is put together, so a bound ``bronze/`` followed by
    ``orders`` reads ``bronze/orders``. Gives ``""`` for a missing or empty value; see
    :func:`_raw_path_shape` for how each part of the value is read.
    """
    shape = _raw_path_shape(expression, bindings, require_physical=require_physical)
    return re.sub(r"/+", "/", shape).strip("/") if shape is not None else None


def _raw_path_shape(expression: Any, bindings: dict[str, Any], *, require_physical: bool) -> str | None:
    """Shape of an ADF path value with its slashes left exactly as written.

    A value that opens with ``@`` (but not ``@{``) is one whole ADF expression, so a
    quoted ``'@{x}'`` inside it is text. A value holding ``@{`` is interpolated: the
    text outside each ``@{...}`` is literal and each block is an expression. A value
    that opens with ``@@`` is the literal text after the first ``@``, as ADF reads it.
    A number or boolean is its text. Any other value, and an unevaluated ARM
    expression, is kept verbatim.

    With *require_physical*, an unevaluated ARM expression such as
    ``[parameters('folder')]`` or a ``{{`` placeholder, here or in a bound value,
    gives ``None``, even when a quoted literal inside it holds ``@``; an ADF ``@``
    expression is still reduced to its shape. Without it that value is kept verbatim,
    and so is an ARM expression holding a quoted ``@``, so two different ones never
    match.
    """
    if isinstance(expression, dict):
        expression = expression.get("value", "")
    if isinstance(expression, (bool, int, float)):
        return str(expression)
    if not isinstance(expression, str):
        return ""
    text = expression.strip()
    is_arm_expression = bool(_is_arm_expression(text))
    if require_physical and ("{{" in text or is_arm_expression):
        return None
    if is_arm_expression:
        return text
    if text.startswith("@@"):
        return text[1:]
    if text.startswith("@") and not text.startswith("@{"):
        return _expression_shape(text[1:], bindings, require_physical=require_physical)
    if "@{" in text:
        return _interpolation_shape(text, bindings, require_physical=require_physical)
    return text


def _interpolation_shape(text: str, bindings: dict[str, Any], *, require_physical: bool) -> str | None:
    """Shape of an interpolated string: text outside ``@{...}`` is literal, each ``@{...}`` is an expression."""
    pieces: list[str] = []
    literal_start = 0
    for block_start, block_end in _interpolation_blocks(text):
        expression_shape = _expression_shape(
            text[block_start + 2 : block_end - 1], bindings, require_physical=require_physical
        )
        if expression_shape is None:
            return None
        pieces.extend((text[literal_start:block_start], expression_shape))
        literal_start = block_end
    pieces.append(text[literal_start:])
    return "".join(pieces)


def _interpolation_blocks(text: str) -> list[tuple[int, int]]:
    """The ``(start, end)`` of each ``@{...}`` block in *text*, where a ``}`` inside quoted text does not close one.

    An ``@{`` with no closing ``}`` is left as literal text, as ADF would print it.
    """
    blocks: list[tuple[int, int]] = []
    search_from = 0
    while (block_start := text.find("@{", search_from)) != -1:
        in_quote = False
        for index in range(block_start + 2, len(text)):
            if text[index] == "'":
                in_quote = not in_quote
            elif text[index] == "}" and not in_quote:
                blocks.append((block_start, index + 1))
                search_from = index + 1
                break
        else:
            break
    return blocks


def _expression_shape(expression: str, bindings: dict[str, Any], *, require_physical: bool) -> str | None:
    """Shape of one ADF expression body, read so that no run-time value is ever dropped.

    A quoted ``'text'`` (``''`` is an escaped quote) is that text, and a number is its
    digits. ``concat(...)`` is its arguments' shapes joined in order. A
    ``dataset().<name>`` (or ``dataset()?.<name>``) bound in *bindings* is its bound
    value's shape, slashes and all. Anything else -- any other function call,
    ``pipeline()``, ``item()``, ``activity(...)``, a subscript -- is one slot (see
    :func:`_runtime_slot`), so none of its argument text ever reads as part of the
    path.
    """
    expression = expression.strip()
    literal = _QUOTED_LITERAL_RE.fullmatch(expression)
    if literal is not None:
        return literal.group(1).replace("''", "'")
    if _NUMBER_LITERAL_RE.fullmatch(expression):
        return expression
    arguments = _concat_arguments(expression)
    if arguments is not None:
        pieces: list[str] = []
        for argument in arguments:
            argument_shape = _expression_shape(argument, bindings, require_physical=require_physical)
            if argument_shape is None:
                return None
            pieces.append(argument_shape)
        return "".join(pieces)
    dataset_parameter = _DATASET_PARAMETER_RE.fullmatch(expression)
    if dataset_parameter is None or dataset_parameter.group(1) not in bindings:
        return _runtime_slot(expression, bindings, require_physical=require_physical)
    return _raw_path_shape(bindings[dataset_parameter.group(1)], {}, require_physical=require_physical)


def _runtime_slot(expression: str, bindings: dict[str, Any], *, require_physical: bool) -> str | None:
    """The one slot a run-time expression leaves, keyed by any bound ``dataset()`` value it uses.

    A plain run-time value is ``<P>``. A call over a ``dataset().<name>`` the call site
    bound, such as ``toLower(dataset().folderPath)`` called with ``orders``, is
    ``<P:tolower('orders')>``: still one slot that is never path text, but two of them
    match only when the call and its bound inputs match, so ``orders`` and
    ``customers`` never join. ``None`` when a bound value is not physical.
    """
    pieces: list[str] = []
    piece_start = 0
    for match in _DATASET_PARAMETER_RE.finditer(expression):
        if match.group(1) not in bindings or expression.count("'", 0, match.start()) % 2:
            continue
        bound_shape = _raw_path_shape(bindings[match.group(1)], {}, require_physical=require_physical)
        if bound_shape is None:
            return None
        quoted_bound_shape = bound_shape.replace("'", "''")
        pieces.extend((expression[piece_start : match.start()], f"'{quoted_bound_shape}'"))
        piece_start = match.end()
    if not pieces:
        return "<P>"
    pieces.append(expression[piece_start:])
    return f"<P:{_canonical_slot_key(''.join(pieces))}>"


def _canonical_slot_key(text: str) -> str:
    """*text* lower-cased and without whitespace outside quotes.

    So neither spacing nor the case of an ADF function name (ADF reads ``toLower`` and
    ``TOLOWER`` alike) tells two keyed slots apart; quoted text keeps its case.
    """
    return "".join(
        part if index % 2 else re.sub(r"\s+", "", part).lower()
        for index, part in enumerate(re.split(r"('(?:[^']|'')*')", text))
    )


def _concat_arguments(expression: str) -> list[str] | None:
    """The top-level arguments of *expression* when it is exactly one ``concat(...)`` call, else ``None``.

    Commas split arguments only outside quotes and outside any nested parentheses or
    brackets, so ``concat('a,b', f(x, y))`` has two arguments.
    """
    match = _CONCAT_CALL_RE.fullmatch(expression)
    if match is None:
        return None
    body = match.group("arguments")
    arguments: list[str] = []
    argument_start = 0
    depth = 0
    in_quote = False
    for index, character in enumerate(body):
        if character == "'":
            in_quote = not in_quote
        elif in_quote:
            continue
        elif character in "([":
            depth += 1
        elif character in ")]":
            depth -= 1
            if depth < 0:
                return None
        elif character == "," and depth == 0:
            arguments.append(body[argument_start:index])
            argument_start = index + 1
    if in_quote or depth:
        return None
    arguments.append(body[argument_start:])
    return arguments


def _has_literal_anchor(shape: str) -> bool:
    """``True`` when *shape* holds literal text besides slots and slashes; only slots is too weak a key.

    A keyed slot such as ``<P:tolower('orders')>`` is still a slot, so its key never counts as literal text.
    """
    return bool(_RUNTIME_SLOT_RE.sub("", shape).replace("/", "").strip())
