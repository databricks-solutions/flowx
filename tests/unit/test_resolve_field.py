"""Unit tests for the resolve.py helpers (resolve_field, resolve_field_int, resolve_dict_values)."""

from __future__ import annotations

from types import MappingProxyType

from orchestra.models.ir import TranslationContext
from orchestra.translator.activity_translators.resolve import (
    resolve_dict_values,
    resolve_field,
    resolve_field_int,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _ctx(**variable_mappings: str) -> TranslationContext:
    """Build a context with optional variable -> task_key mappings."""
    vc = MappingProxyType(variable_mappings) if variable_mappings else MappingProxyType({})
    return TranslationContext(variable_cache=vc)


# ---------------------------------------------------------------------------
# resolve_field
# ---------------------------------------------------------------------------


class TestResolveField:
    def test_none_returns_empty_string(self):
        assert resolve_field(None, _ctx()) == ""

    def test_plain_string_literal(self):
        assert resolve_field("hello", _ctx()) == "hello"

    def test_integer_literal(self):
        result = resolve_field(42, _ctx())
        assert result == "42"

    def test_expression_dict_pipeline_param(self):
        value = {"type": "Expression", "value": "@pipeline().parameters.env"}
        result = resolve_field(value, _ctx())
        assert result == "{{job.parameters.env}}"

    def test_pipeline_run_id(self):
        result = resolve_field("@pipeline().RunId", _ctx())
        assert result == "{{job.run_id}}"

    def test_interpolation_string(self):
        result = resolve_field("@{pipeline().parameters.env}", _ctx())
        assert "env" in result

    def test_expression_dict_with_unsupported_expression(self):
        """Unsupported expression dict falls back to the raw value string."""
        value = {"type": "Expression", "value": "@dataUri('hello')"}
        result = resolve_field(value, _ctx())
        assert result == "@dataUri('hello')"

    def test_non_expression_dict_returns_str(self):
        """Non-expression dict without resolve falls back to str."""
        value = {"type": "Other"}
        result = resolve_field(value, _ctx())
        assert isinstance(result, str)

    def test_activity_output_ref(self):
        result = resolve_field("@activity('Lookup').output.firstRow.cnt", _ctx())
        assert "tasks.Lookup.values.cnt" in result

    def test_boolean_value(self):
        # VAREX3-002: Python bool renders lowercase 'true'/'false' to match ADF.
        assert resolve_field(True, _ctx()) == "true"
        assert resolve_field(False, _ctx()) == "false"

    def test_variables_with_context(self):
        result = resolve_field("@variables('runDate')", _ctx(runDate="SetRunDate"))
        assert "tasks.SetRunDate.values.runDate" in result


# ---------------------------------------------------------------------------
# resolve_field_int
# ---------------------------------------------------------------------------


class TestResolveFieldInt:
    def test_integer_value(self):
        assert resolve_field_int(42, _ctx()) == 42

    def test_integer_as_string(self):
        assert resolve_field_int("100", _ctx()) == 100

    def test_expression_resolving_to_literal(self):
        """A literal string that happens to be an integer."""
        assert resolve_field_int("5", _ctx()) == 5

    def test_non_numeric_returns_default(self):
        assert resolve_field_int("not_a_number", _ctx()) == 0

    def test_custom_default(self):
        assert resolve_field_int("not_a_number", _ctx(), default=99) == 99

    def test_none_returns_default(self):
        # resolve_field(None) returns "", int("") fails, so default is returned
        assert resolve_field_int(None, _ctx()) == 0

    def test_expression_dict_non_numeric(self):
        """Expression dict that resolves to a DAB ref (non-numeric) returns default."""
        value = {"type": "Expression", "value": "@pipeline().parameters.env"}
        assert resolve_field_int(value, _ctx(), default=10) == 10


# ---------------------------------------------------------------------------
# resolve_dict_values
# ---------------------------------------------------------------------------


class TestResolveDictValues:
    def test_none_returns_empty_dict(self):
        assert resolve_dict_values(None, _ctx()) == {}

    def test_empty_dict_returns_empty(self):
        assert resolve_dict_values({}, _ctx()) == {}

    def test_literal_values(self):
        result = resolve_dict_values({"env": "dev", "mode": "batch"}, _ctx())
        assert result == {"env": "dev", "mode": "batch"}

    def test_mixed_literal_and_expression(self):
        result = resolve_dict_values(
            {
                "env": "dev",
                "run_id": "@pipeline().RunId",
                "date": {"type": "Expression", "value": "@pipeline().parameters.date"},
            },
            _ctx(),
        )
        assert result["env"] == "dev"
        assert result["run_id"] == "{{job.run_id}}"
        assert result["date"] == "{{job.parameters.date}}"

    def test_all_expressions(self):
        result = resolve_dict_values(
            {
                "run_id": "@pipeline().RunId",
                "name": "@pipeline().Pipeline",
            },
            _ctx(),
        )
        assert result["run_id"] == "{{job.run_id}}"
        assert result["name"] == "{{job.name}}"
