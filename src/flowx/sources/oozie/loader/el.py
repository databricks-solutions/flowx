"""Parse Oozie EL, the JSP expression language Oozie evaluates inside ``${...}``.

A value such as ``/data/${wf:conf('day')}/out`` is a *template*: literal text with embedded expressions.
This module only builds the syntax tree; :mod:`flowx.sources.oozie.loader.el_render` decides what each
piece becomes on Databricks. Identifiers may contain dots because Oozie property names do
(``${oozie.wf.application.path}``).
"""

from __future__ import annotations

import re
from dataclasses import dataclass


class ElSyntaxError(ValueError):
    """Raised when an EL template or expression can't be parsed."""


@dataclass(frozen=True, slots=True)
class Literal:
    value: str | int | float | bool | None


@dataclass(frozen=True, slots=True)
class Identifier:
    name: str


@dataclass(frozen=True, slots=True)
class Call:
    namespace: str | None
    function: str
    arguments: tuple[Expression, ...]


@dataclass(frozen=True, slots=True)
class Index:
    target: Expression
    key: Expression


@dataclass(frozen=True, slots=True)
class Unary:
    operator: str
    operand: Expression


@dataclass(frozen=True, slots=True)
class Binary:
    operator: str
    left: Expression
    right: Expression


@dataclass(frozen=True, slots=True)
class Ternary:
    condition: Expression
    if_true: Expression
    if_false: Expression


Expression = Literal | Identifier | Call | Index | Unary | Binary | Ternary


@dataclass(frozen=True, slots=True)
class Template:
    parts: tuple[str | Expression, ...]


_OPERATOR_ALIASES = {
    "&&": "and",
    "||": "or",
    "!": "not",
    "==": "eq",
    "!=": "ne",
    "<": "lt",
    ">": "gt",
    "<=": "le",
    ">=": "ge",
    "div": "/",
    "mod": "%",
}
_BINARY_PRECEDENCE = {
    "or": 1,
    "and": 2,
    "eq": 3,
    "ne": 3,
    "lt": 4,
    "gt": 4,
    "le": 4,
    "ge": 4,
    "+": 5,
    "-": 5,
    "*": 6,
    "/": 6,
    "%": 6,
}
_WORD_OPERATORS = frozenset({"and", "or", "not", "eq", "ne", "lt", "gt", "le", "ge", "div", "mod", "empty"})
_TOKEN = re.compile(
    r"\s*(?:(?P<number>\d+\.\d*(?:[eE][+-]?\d+)?|\d+(?:[eE][+-]?\d+)?)"
    r"|(?P<name>[A-Za-z_][A-Za-z0-9_.]*)"
    r"|(?P<string>'(?:[^'\\]|\\.)*'|\"(?:[^\"\\]|\\.)*\")"
    r"|(?P<operator>&&|\|\||==|!=|<=|>=|[<>!+\-*/%?:()\[\],]))"
)


def _find_expression_end(text: str, start: int) -> int:
    """Returns the index of the ``}`` that closes the expression starting at *start*."""
    index, depth, quote = start, 0, ""
    while index < len(text):
        character = text[index]
        if quote:
            if character == "\\":
                index += 2
                continue
            if character == quote:
                quote = ""
        elif character in "'\"":
            quote = character
        elif character == "{":
            depth += 1
        elif character == "}":
            if depth == 0:
                return index
            depth -= 1
        index += 1
    raise ElSyntaxError(f"unterminated ${{...}} in {text!r}")


def parse_template(text: str) -> Template:
    """Splits *text* into literal strings and parsed ``${...}`` expressions."""
    parts: list[str | Expression] = []
    buffer: list[str] = []
    index = 0
    while index < len(text):
        if text.startswith("${", index):
            end = _find_expression_end(text, index + 2)
            if buffer:
                parts.append("".join(buffer))
                buffer = []
            parts.append(parse_expression(text[index + 2 : end]))
            index = end + 1
            continue
        buffer.append(text[index])
        index += 1
    if buffer:
        parts.append("".join(buffer))
    return Template(parts=tuple(parts))


def _tokenize(text: str) -> tuple[list[tuple[str, str]], list[tuple[int, int]]]:
    """Returns the tokens and, in parallel, the ``(start, end)`` character span of each one."""
    tokens: list[tuple[str, str]] = []
    spans: list[tuple[int, int]] = []
    position = 0
    while position < len(text):
        if text[position:].strip() == "":
            break
        match = _TOKEN.match(text, position)
        if match is None or match.end() == position:
            raise ElSyntaxError(f"unexpected character in EL {text!r} at {position}")
        kind = match.lastgroup or ""
        value = match.group(kind)
        if kind == "name" and value in _WORD_OPERATORS:
            kind = "operator"
        tokens.append((kind, value))
        spans.append(match.span(match.lastgroup or ""))
        position = match.end()
    tokens.append(("end", ""))
    spans.append((len(text), len(text)))
    return tokens, spans


def _unquote(token: str) -> str:
    body = token[1:-1]
    return re.sub(r"\\(.)", r"\1", body)


class _Parser:
    def __init__(self, text: str) -> None:
        self.text = text
        self.tokens, self.spans = _tokenize(text)
        self.position = 0

    def peek(self, offset: int = 0) -> tuple[str, str]:
        return self.tokens[min(self.position + offset, len(self.tokens) - 1)]

    def advance(self) -> tuple[str, str]:
        token = self.peek()
        self.position += 1
        return token

    def expect(self, value: str) -> None:
        kind, actual = self.advance()
        if actual != value:
            raise ElSyntaxError(f"expected {value!r} in EL {self.text!r}, found {actual or 'end'!r}")

    def parse(self) -> Expression:
        if self.peek()[0] == "end":
            raise ElSyntaxError("empty EL expression")
        expression = self.ternary()
        if self.peek()[0] != "end":
            raise ElSyntaxError(f"unexpected {self.peek()[1]!r} in EL {self.text!r}")
        return expression

    def ternary(self) -> Expression:
        condition = self.binary(1)
        if self.peek() == ("operator", "?"):
            self.advance()
            if_true = self.ternary()
            self.expect(":")
            return Ternary(condition=condition, if_true=if_true, if_false=self.ternary())
        return condition

    def binary(self, minimum_precedence: int) -> Expression:
        left = self.unary()
        while True:
            kind, value = self.peek()
            operator = _OPERATOR_ALIASES.get(value, value) if kind == "operator" else ""
            precedence = _BINARY_PRECEDENCE.get(operator, 0)
            if precedence < minimum_precedence or precedence == 0:
                return left
            self.advance()
            left = Binary(operator=operator, left=left, right=self.binary(precedence + 1))

    def unary(self) -> Expression:
        kind, value = self.peek()
        operator = _OPERATOR_ALIASES.get(value, value) if kind == "operator" else ""
        if operator in ("not", "-", "empty"):
            self.advance()
            return Unary(operator=operator, operand=self.unary())
        return self.postfix(self.primary())

    def postfix(self, expression: Expression) -> Expression:
        while self.peek() == ("operator", "["):
            self.advance()
            key = self.ternary()
            self.expect("]")
            expression = Index(target=expression, key=key)
        return expression

    def arguments(self) -> tuple[Expression, ...]:
        self.expect("(")
        values: list[Expression] = []
        if self.peek() != ("operator", ")"):
            values.append(self.ternary())
            while self.peek() == ("operator", ","):
                self.advance()
                values.append(self.ternary())
        self.expect(")")
        return tuple(values)

    def _at_namespace_separator(self) -> bool:
        """True when the upcoming tokens read ``name:name(`` with no whitespace, which is how Oozie writes namespaces.

        The spacing check is what keeps ``x ? a : f(1)`` from being read as a namespaced call ``a:f(``.
        """
        if self.peek() != ("operator", ":") or self.peek(1)[0] != "name" or self.peek(2) != ("operator", "("):
            return False
        name_span = self.spans[self.position - 1]
        colon_span = self.spans[self.position]
        function_span = self.spans[self.position + 1]
        return name_span[1] == colon_span[0] and colon_span[1] == function_span[0]

    def primary(self) -> Expression:
        kind, value = self.advance()
        if kind == "number":
            return Literal(value=float(value) if any(mark in value for mark in ".eE") else int(value))
        if kind == "string":
            return Literal(value=_unquote(value))
        if kind == "name":
            if value in ("true", "false"):
                return Literal(value=value == "true")
            if value == "null":
                return Literal(value=None)
            if self._at_namespace_separator():
                self.advance()
                function = self.advance()[1]
                return Call(namespace=value, function=function, arguments=self.arguments())
            if self.peek() == ("operator", "("):
                return Call(namespace=None, function=value, arguments=self.arguments())
            return Identifier(name=value)
        if (kind, value) == ("operator", "("):
            expression = self.ternary()
            self.expect(")")
            return expression
        raise ElSyntaxError(f"unexpected {value or 'end'!r} in EL {self.text!r}")


def parse_expression(text: str) -> Expression:
    """Parses the inside of one ``${...}``."""
    return _Parser(text).parse()
