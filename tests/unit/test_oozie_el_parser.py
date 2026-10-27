"""Parsing Oozie's JSP-EL: templates with embedded ${...} and the expression grammar inside them."""

from __future__ import annotations

import pytest

from flowx.sources.oozie.loader.el import (
    Binary,
    Call,
    ElSyntaxError,
    Identifier,
    Index,
    Literal,
    Template,
    Ternary,
    Unary,
    parse_expression,
    parse_template,
)


def test_template_splits_literal_text_and_expressions():
    assert parse_template("/data/${day}/out") == Template(parts=("/data/", Identifier(name="day"), "/out"))
    assert parse_template("plain") == Template(parts=("plain",))
    assert parse_template("") == Template(parts=())


def test_braces_and_quotes_inside_expressions_do_not_end_them():
    template = parse_template('${concat(\'a}\', "b\\"}")}x')
    assert template.parts == (
        Call(namespace=None, function="concat", arguments=(Literal(value="a}"), Literal(value='b"}'))),
        "x",
    )


@pytest.mark.parametrize(
    "text, expected",
    [
        ("a eq 'x'", Binary(operator="eq", left=Identifier(name="a"), right=Literal(value="x"))),
        ("a == 'x'", Binary(operator="eq", left=Identifier(name="a"), right=Literal(value="x"))),
        ("a != 1", Binary(operator="ne", left=Identifier(name="a"), right=Literal(value=1))),
        ("n ge 2.5", Binary(operator="ge", left=Identifier(name="n"), right=Literal(value=2.5))),
        ("!flag", Unary(operator="not", operand=Identifier(name="flag"))),
        ("not flag", Unary(operator="not", operand=Identifier(name="flag"))),
        ("empty x", Unary(operator="empty", operand=Identifier(name="x"))),
        ("-n", Unary(operator="-", operand=Identifier(name="n"))),
        ("true", Literal(value=True)),
        ("null", Literal(value=None)),
        ("oozie.wf.application.path", Identifier(name="oozie.wf.application.path")),
        ("wf:id()", Call(namespace="wf", function="id", arguments=())),
        (
            "wf:actionData('load')['rows']",
            Index(
                target=Call(namespace="wf", function="actionData", arguments=(Literal(value="load"),)),
                key=Literal(value="rows"),
            ),
        ),
        (
            "a ? 'y' : 'n'",
            Ternary(condition=Identifier(name="a"), if_true=Literal(value="y"), if_false=Literal(value="n")),
        ),
        (
            "10 div 3 mod 2",
            Binary(
                operator="%",
                left=Binary(operator="/", left=Literal(value=10), right=Literal(value=3)),
                right=Literal(value=2),
            ),
        ),
    ],
)
def test_expression_grammar(text: str, expected):
    assert parse_expression(text) == expected


def test_precedence_and_over_or_and_comparison_over_and():
    assert parse_expression("a or b and c eq 1") == Binary(
        operator="or",
        left=Identifier(name="a"),
        right=Binary(
            operator="and",
            left=Identifier(name="b"),
            right=Binary(operator="eq", left=Identifier(name="c"), right=Literal(value=1)),
        ),
    )


def test_ternary_colon_is_not_mistaken_for_a_namespace():
    assert parse_expression("x ? a : b") == Ternary(
        condition=Identifier(name="x"), if_true=Identifier(name="a"), if_false=Identifier(name="b")
    )


def test_spaced_ternary_colon_before_a_call_is_not_a_namespace():
    assert parse_expression("x ? b : concat(a, c)") == Ternary(
        condition=Identifier(name="x"),
        if_true=Identifier(name="b"),
        if_false=Call(namespace=None, function="concat", arguments=(Identifier(name="a"), Identifier(name="c"))),
    )


def test_namespace_call_after_a_ternary_colon_is_still_a_namespace():
    assert parse_expression("x ? 'y' : wf:conf('z')") == Ternary(
        condition=Identifier(name="x"),
        if_true=Literal(value="y"),
        if_false=Call(namespace="wf", function="conf", arguments=(Literal(value="z"),)),
    )


@pytest.mark.parametrize("text", ["${a", "${a eq}", "${(a}", "${'unterminated}", "${a b}", "${}"])
def test_syntax_errors(text: str):
    with pytest.raises(ElSyntaxError):
        parse_template(text)
