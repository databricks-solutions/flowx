"""Unit tests for the unified resolve_expression() function."""

from __future__ import annotations

from types import MappingProxyType

from orchestra.models.ir import TranslationContext
from orchestra.parser.expression_parser import (
    parse_expression,
    parse_expression_for_dab,
    resolve_expression,
)


def _context(**variable_mappings: str) -> TranslationContext:
    """Build a context with optional variable -> task_key mappings."""
    variable_cache = MappingProxyType(variable_mappings) if variable_mappings else MappingProxyType({})
    return TranslationContext(variable_cache=variable_cache)


class TestLiterals:
    def test_plain_string(self):
        result = resolve_expression("hello", _context())
        assert result is not None
        assert result.kind == "literal"
        assert result.value == "hello"

    def test_integer(self):
        result = resolve_expression(42, _context())
        assert result is not None
        assert result.kind == "literal"
        assert result.value == "42"

    def test_float(self):
        result = resolve_expression(3.14, _context())
        assert result is not None
        assert result.kind == "literal"
        assert result.value == "3.14"

    def test_boolean(self):
        # VAREX3-002: Python bool renders lowercase to match ADF semantics.
        result_t = resolve_expression(True, _context())
        assert result_t is not None
        assert result_t.kind == "literal"
        assert result_t.value == "true"

        result_f = resolve_expression(False, _context())
        assert result_f is not None
        assert result_f.kind == "literal"
        assert result_f.value == "false"

    def test_expression_dict_wrapping(self):
        result = resolve_expression({"type": "Expression", "value": "hello"}, _context())
        assert result is not None
        assert result.kind == "literal"
        assert result.value == "hello"


class TestPipelineProperties:
    def test_run_id(self):
        result = resolve_expression("@pipeline().RunId", _context())
        assert result is not None
        assert result.kind == "dab_ref"
        assert result.value == "{{job.run_id}}"

    def test_pipeline_name(self):
        result = resolve_expression("@pipeline().Pipeline", _context())
        assert result is not None
        assert result.kind == "dab_ref"
        assert result.value == "{{job.name}}"

    def test_trigger_time(self):
        result = resolve_expression("@pipeline().TriggerTime", _context())
        assert result is not None
        assert result.kind == "dab_ref"
        assert result.value == "{{job.start_time.iso_datetime}}"

    def test_group_id(self):
        result = resolve_expression("@pipeline().GroupId", _context())
        assert result is not None
        assert result.kind == "dab_ref"
        assert result.value == "{{job.run_id}}"


class TestPipelineParameters:
    def test_parameter(self):
        result = resolve_expression("@pipeline().parameters.environment", _context())
        assert result is not None
        assert result.kind == "dab_ref"
        assert result.value == "{{job.parameters.environment}}"

    def test_parameter_expression_dict(self):
        result = resolve_expression(
            {"type": "Expression", "value": "@pipeline().parameters.date"},
            _context(),
        )
        assert result is not None
        assert result.kind == "dab_ref"
        assert result.value == "{{job.parameters.date}}"


class TestActivityOutput:
    def test_firstrow_column(self):
        result = resolve_expression("@activity('Lookup').output.firstRow.cnt", _context())
        assert result is not None
        assert result.kind == "dab_ref"
        assert result.value == "{{tasks.Lookup.values.cnt}}"

    def test_output_value(self):
        result = resolve_expression("@activity('GetList').output.value", _context())
        assert result is not None
        assert result.kind == "dab_ref"
        assert result.value == "{{tasks.GetList.values.result}}"

    def test_output_no_path(self):
        result = resolve_expression("@activity('Task').output", _context())
        assert result is not None
        assert result.kind == "dab_ref"
        assert result.value == "{{tasks.Task.values.result}}"

    def test_sanitizes_task_key(self):
        result = resolve_expression("@activity('Lookup Row Count').output.firstRow.row_count", _context())
        assert result is not None
        assert result.kind == "dab_ref"
        assert "Lookup_Row_Count" in result.value


class TestVariables:
    def test_variable_with_context(self):
        result = resolve_expression("@variables('outputPath')", _context(outputPath="SetOutputPath"))
        assert result is not None
        assert result.kind == "dab_ref"
        assert result.value == "{{tasks.SetOutputPath.values.outputPath}}"

    def test_variable_with_explicit_task_keys(self):
        result = resolve_expression(
            "@variables('runDate')",
            _context(),
            variable_task_keys={"runDate": "SetRunDate"},
        )
        assert result is not None
        assert result.kind == "dab_ref"
        assert result.value == "{{tasks.SetRunDate.values.runDate}}"

    def test_variable_returns_none_when_no_setter(self):
        """C-05 (VAREX-002): unknown variables resolve to ``None`` instead of
        a self-referential dangling ``{{tasks.X.values.X}}`` placeholder
        that never gets satisfied at runtime."""
        result = resolve_expression("@variables('unknown')", _context())
        assert result is None


class TestItem:
    def test_item(self):
        result = resolve_expression("@item()", _context())
        assert result is not None
        assert result.kind == "dab_ref"
        assert result.value == "{{input}}"

    def test_item_safe_nav_single_segment(self):
        """C-16 (CF3-005 / VAREX3-005): single-segment item()?.X must lower to
        notebook_code so bridge lowering can fire downstream (Switch on-expr,
        SetVariable expressions wrapping the safe-nav)."""
        result = resolve_expression("@item()?.foo", _context())
        assert result is not None
        assert result.kind == "notebook_code"
        assert "json" in result.value
        assert "get('foo')" in result.value

    def test_item_safe_nav_multi_segment_unchanged(self):
        """Two-segment item()?.a?.b continues to lower to notebook_code."""
        result = resolve_expression("@item()?.foo?.bar", _context())
        assert result is not None
        assert result.kind == "notebook_code"
        assert "get('foo')" in result.value
        assert "get('bar')" in result.value

    def test_item_field_no_safe_nav_remains_dab_ref(self):
        """Plain item().foo with no safe-nav operator stays a dab_ref."""
        result = resolve_expression("@item().foo", _context())
        assert result is not None
        assert result.kind == "dab_ref"
        assert result.value == "{{input.foo}}"

    def test_item_field_multi_segment_lowers_to_notebook_code(self):
        """C-35 (CF4-004): ``item().condition.name`` must walk both
        ``.condition`` and ``.name`` instead of truncating to
        ``{{input.condition}}``.  Previously ``_ITEM_FIELD_RE`` matched the
        first segment without an end-anchor so the trailing ``.name``
        was silently dropped."""
        result = resolve_expression("@item().condition.name", _context())
        assert result is not None
        assert result.kind == "notebook_code"
        assert "get('condition')" in result.value
        assert "get('name')" in result.value


class TestUtcNow:
    def test_utcnow_no_format(self):
        # ``@utcNow()`` maps to the Databricks job start time so the result
        # lands directly in DAB YAML.  The translator attaches a note so
        # the bundler can surface the activity-vs-job-start skew caveat in
        # SETUP.md.
        result = resolve_expression("@utcNow()", _context())
        assert result is not None
        assert result.kind == "dab_ref"
        assert result.value == "{{job.start_time.iso_datetime}}"
        assert any("utcnow" in note.lower() for note in result.notes)

    def test_utcnow_with_known_iso_format(self):
        result = resolve_expression("@utcNow('yyyy-MM-dd')", _context())
        assert result is not None
        assert result.kind == "dab_ref"
        assert result.value == "{{job.start_time.iso_date}}"
        assert any("utcnow" in note.lower() for note in result.notes)

    def test_utcnow_with_unknown_format_falls_back_to_notebook_code(self):
        # ``yyyyMMdd`` (no separators) is not in the DAB dynamic-value
        # vocabulary, so orchestra keeps the legacy Python strftime path.
        result = resolve_expression("@utcNow('yyyyMMdd')", _context())
        assert result is not None
        assert result.kind == "notebook_code"
        assert "strftime" in result.value
        assert "%Y%m%d" in result.value
        assert result.notes == []

    def test_utcnow_expression_dict(self):
        result = resolve_expression(
            {"type": "Expression", "value": "@utcNow('yyyy-MM-dd')"},
            _context(),
        )
        assert result is not None
        assert result.kind == "dab_ref"
        assert result.value == "{{job.start_time.iso_date}}"


class TestConcat:
    def test_concat_literals(self):
        # C-01: when every part is a literal, the whole concat collapses to
        # a single literal so consumers (cluster fields, library jar paths,
        # ...) receive a plain string instead of Python source.
        result = resolve_expression("@concat('hello', ' ', 'world')", _context())
        assert result is not None
        assert result.kind == "literal"
        assert result.value == "hello world"

    def test_concat_collapses_when_all_parts_resolve_to_literals(self):
        # C-01: factory globals collapse @concat parts to literal kinds,
        # so the whole concat should likewise be a literal string.
        ctx = TranslationContext(
            global_parameters=MappingProxyType({"env_variable": "t", "deequLibFileName": "deequ-3.5.6.jar"}),
        )
        result = resolve_expression(
            "@concat('/Volumes/datahub01', pipeline().globalParameters.env_variable, "
            "'/lib/', pipeline().globalParameters.deequLibFileName)",
            ctx,
        )
        assert result is not None
        assert result.kind == "literal"
        assert result.value == "/Volumes/datahub01t/lib/deequ-3.5.6.jar"

    def test_concat_with_variable(self):
        result = resolve_expression(
            "@concat('output/', variables('runDate'), '/processed')",
            _context(runDate="SetRunDate"),
        )
        assert result is not None
        assert result.kind == "notebook_code"
        assert "runDate" in result.value

    def test_concat_with_utcnow(self):
        # ``utcNow('yyyy-MM-dd')`` is now a DAB ref, so concat wraps it as a
        # widget read.  The result is still notebook_code because concat
        # composes Python strings.
        result = resolve_expression("@concat('date_', utcNow('yyyy-MM-dd'))", _context())
        assert result is not None
        assert result.kind == "notebook_code"
        assert "dbutils.widgets.get('iso_date')" in result.value

    def test_concat_with_pipeline_param(self):
        result = resolve_expression(
            "@concat(variables('catalogName'), '.', pipeline().parameters.schemaPrefix, '_data')",
            _context(catalogName="SetCatalogName"),
        )
        assert result is not None
        assert result.kind == "notebook_code"


class TestUnsupported:
    def test_non_dict_non_scalar(self):
        result = resolve_expression({"type": "Other"}, _context())
        assert result is None

    def test_agentic_data_uri(self):
        result = resolve_expression("@dataUri('hello')", _context())
        assert result is None

    def test_agentic_xml(self):
        result = resolve_expression("@xml('<root/>')", _context())
        assert result is None

    def test_agentic_xpath(self):
        result = resolve_expression("@xpath(xml('<r/>'), '/')", _context())
        assert result is None

    def test_convert_from_utc_resolves_to_notebook_code(self):
        result = resolve_expression("@convertFromUtc('2024-01-01T00:00:00Z', 'Pacific Standard Time')", _context())
        assert result is not None
        assert result.kind == "notebook_code"
        assert "ZoneInfo" in result.value

    def test_ticks_resolves_to_notebook_code(self):
        result = resolve_expression("@ticks('2024-01-01T00:00:00Z')", _context())
        assert result is not None
        assert result.kind == "notebook_code"


class TestStringFunctions:
    def test_ends_with(self):
        result = resolve_expression("@endsWith('hello world', 'world')", _context())
        assert result is not None
        assert result.kind == "notebook_code"
        assert "endswith" in result.value

    def test_ends_with_nested(self):
        result = resolve_expression("@endsWith(toLower('HELLO'), 'hello')", _context())
        assert result is not None
        assert result.kind == "notebook_code"
        assert "endswith" in result.value
        assert "lower" in result.value

    def test_guid_no_args(self):
        result = resolve_expression("@guid()", _context())
        assert result is not None
        assert result.kind == "notebook_code"
        assert "uuid4" in result.value

    def test_guid_format_n(self):
        result = resolve_expression("@guid('N')", _context())
        assert result is not None
        assert "replace" in result.value

    def test_index_of(self):
        result = resolve_expression("@indexOf('hello world', 'world')", _context())
        assert result is not None
        assert "find" in result.value

    def test_last_index_of(self):
        result = resolve_expression("@lastIndexOf('hello hello', 'hello')", _context())
        assert result is not None
        assert "rfind" in result.value

    def test_replace(self):
        result = resolve_expression("@replace('hello world', 'world', 'python')", _context())
        assert result is not None
        assert "replace" in result.value

    def test_split(self):
        result = resolve_expression("@split('a,b,c', ',')", _context())
        assert result is not None
        assert "split" in result.value

    def test_starts_with(self):
        result = resolve_expression("@startsWith('hello world', 'hello')", _context())
        assert result is not None
        assert "startswith" in result.value

    def test_substring(self):
        result = resolve_expression("@substring('hello', 0, 3)", _context())
        assert result is not None
        assert result.kind == "notebook_code"
        # Should produce a slice expression
        assert "[" in result.value

    def test_equals_quoted_string_emits_repr(self):
        """C-34 (VAREX4-002): a quoted ``'12'`` argument must keep its
        quotedness through codegen so the comparison emits ``... == '12'``
        rather than the bare token ``12`` (which silently compares against
        a numeric value)."""
        result = resolve_expression(
            "@equals(variables('month'), '12')",
            _context(month="set_month"),
        )
        assert result is not None
        assert "== '12'" in result.value

    def test_less_quoted_leading_zero_is_valid_python(self):
        """C-34 (VAREX4-002): a leading-zero quoted argument like ``'09'``
        must round-trip as a Python string literal, not the bare token
        ``09`` (which is a SyntaxError in modern Python)."""
        result = resolve_expression(
            "@less(variables('month'), '09')",
            _context(month="set_month"),
        )
        assert result is not None
        # Result must parse as valid Python.
        compile(result.value, "<test>", "eval")
        assert "< '09'" in result.value

    def test_equals_bool_literal_emits_lowercase_string(self):
        """C-34 (VAREX4-003): an ADF Boolean ``true`` argument lowers to
        the lowercase string literal ``'true'`` so the comparison matches
        what C-21 SetVariable writes on the consumer side."""
        result = resolve_expression(
            "@equals(variables('X'), true)",
            _context(X="set_x"),
        )
        assert result is not None
        assert "== 'true'" in result.value

    def test_substring_two_arg_form(self):
        """C-33 (VAREX4-001): ADF accepts substring(text, start) without an
        explicit length argument."""
        result = resolve_expression("@substring(string(pipeline().parameters.params), 1)", _context())
        assert result is not None
        assert result.kind == "notebook_code"
        assert "[int(" in result.value
        assert "):]" in result.value

    def test_split_with_subscript(self):
        """C-33 (VAREX4-001): a trailing ``[N]`` on a function call lowers
        to notebook_code Python source so SetVariable activities wrapping
        ``@split(...)[0]`` actually resolve."""
        result = resolve_expression("@split(pipeline().parameters.referenceDate,'/')[0]", _context())
        assert result is not None
        assert result.kind == "notebook_code"
        assert ".split(str('/'))" in result.value
        assert ")[0]" in result.value

    def test_to_lower(self):
        result = resolve_expression("@toLower('HELLO')", _context())
        assert result is not None
        assert "lower" in result.value

    def test_to_upper(self):
        result = resolve_expression("@toUpper('hello')", _context())
        assert result is not None
        assert "upper" in result.value

    def test_trim(self):
        result = resolve_expression("@trim('  hello  ')", _context())
        assert result is not None
        assert "strip" in result.value


class TestCollectionFunctions:
    def test_contains(self):
        result = resolve_expression("@contains('hello world', 'hello')", _context())
        assert result is not None
        assert result.kind == "notebook_code"
        assert "in" in result.value

    def test_empty(self):
        result = resolve_expression("@empty('')", _context())
        assert result is not None
        assert "len" in result.value

    def test_first(self):
        result = resolve_expression("@first(createArray(1, 2, 3))", _context())
        assert result is not None
        assert "[0]" in result.value

    def test_join(self):
        result = resolve_expression("@join(createArray('a', 'b', 'c'), ',')", _context())
        assert result is not None
        assert "join" in result.value

    def test_last(self):
        result = resolve_expression("@last(createArray(1, 2, 3))", _context())
        assert result is not None
        assert "[-1]" in result.value

    def test_length(self):
        result = resolve_expression("@length('hello')", _context())
        assert result is not None
        assert "len" in result.value

    def test_skip(self):
        result = resolve_expression("@skip(createArray(1, 2, 3), 1)", _context())
        assert result is not None
        assert result.kind == "notebook_code"

    def test_take(self):
        result = resolve_expression("@take(createArray(1, 2, 3), 2)", _context())
        assert result is not None
        assert result.kind == "notebook_code"

    def test_intersection(self):
        result = resolve_expression("@intersection(createArray(1, 2, 3), createArray(2, 3, 4))", _context())
        assert result is not None
        assert "set" in result.value

    def test_union(self):
        result = resolve_expression("@union(createArray(1, 2), createArray(3, 4))", _context())
        assert result is not None
        assert "set" in result.value


class TestLogicalFunctions:
    def test_and(self):
        result = resolve_expression("@and(true, false)", _context())
        assert result is not None
        assert result.kind == "notebook_code"
        assert "and" in result.value

    def test_equals(self):
        result = resolve_expression("@equals(1, 1)", _context())
        assert result is not None
        assert "==" in result.value

    def test_greater(self):
        result = resolve_expression("@greater(5, 3)", _context())
        assert result is not None
        assert ">" in result.value

    def test_greater_or_equals(self):
        result = resolve_expression("@greaterOrEquals(5, 5)", _context())
        assert result is not None
        assert ">=" in result.value

    def test_if(self):
        result = resolve_expression("@if(equals(1, 1), 'yes', 'no')", _context())
        assert result is not None
        assert result.kind == "notebook_code"
        assert "if" in result.value
        assert "else" in result.value

    def test_less(self):
        result = resolve_expression("@less(3, 5)", _context())
        assert result is not None
        assert "<" in result.value

    def test_less_or_equals(self):
        result = resolve_expression("@lessOrEquals(3, 3)", _context())
        assert result is not None
        assert "<=" in result.value

    def test_not(self):
        result = resolve_expression("@not(true)", _context())
        assert result is not None
        assert "not" in result.value

    def test_or(self):
        result = resolve_expression("@or(true, false)", _context())
        assert result is not None
        assert "or" in result.value


class TestConversionFunctions:
    def test_array(self):
        result = resolve_expression("@array('hello')", _context())
        assert result is not None
        assert result.kind == "notebook_code"
        assert "[" in result.value

    def test_base64(self):
        result = resolve_expression("@base64('hello')", _context())
        assert result is not None
        assert "b64encode" in result.value

    def test_base64_to_string(self):
        result = resolve_expression("@base64ToString('aGVsbG8=')", _context())
        assert result is not None
        assert "b64decode" in result.value
        assert "decode" in result.value

    def test_base64_to_binary(self):
        result = resolve_expression("@base64ToBinary('aGVsbG8=')", _context())
        assert result is not None
        assert "b64decode" in result.value

    def test_binary(self):
        result = resolve_expression("@binary('hello')", _context())
        assert result is not None
        assert "encode" in result.value

    def test_bool(self):
        result = resolve_expression("@bool(1)", _context())
        assert result is not None
        assert "bool" in result.value

    def test_coalesce(self):
        result = resolve_expression("@coalesce(null, 'fallback')", _context())
        assert result is not None
        assert "next" in result.value
        assert "None" in result.value

    def test_create_array(self):
        result = resolve_expression("@createArray(1, 2, 3)", _context())
        assert result is not None
        assert result.kind == "notebook_code"
        assert "[" in result.value

    def test_decode_base64_alias(self):
        result = resolve_expression("@decodeBase64('aGVsbG8=')", _context())
        assert result is not None
        assert "b64decode" in result.value

    def test_decode_uri_component(self):
        result = resolve_expression("@decodeUriComponent('hello%20world')", _context())
        assert result is not None
        assert "unquote" in result.value

    def test_encode_uri_component(self):
        result = resolve_expression("@encodeUriComponent('hello world')", _context())
        assert result is not None
        assert "quote" in result.value

    def test_float(self):
        result = resolve_expression("@float('3.14')", _context())
        assert result is not None
        assert "float" in result.value

    def test_int(self):
        result = resolve_expression("@int('42')", _context())
        assert result is not None
        assert "int" in result.value

    def test_json(self):
        result = resolve_expression('@json(\'{"key": "value"}\')', _context())
        assert result is not None
        assert "loads" in result.value

    def test_string(self):
        result = resolve_expression("@string(42)", _context())
        assert result is not None
        assert "str" in result.value

    def test_uri_component_alias(self):
        result = resolve_expression("@uriComponent('hello world')", _context())
        assert result is not None
        assert "quote" in result.value

    def test_uri_component_to_string_alias(self):
        result = resolve_expression("@uriComponentToString('hello%20world')", _context())
        assert result is not None
        assert "unquote" in result.value


class TestMathFunctions:
    def test_add(self):
        result = resolve_expression("@add(1, 2)", _context())
        assert result is not None
        assert result.kind == "notebook_code"
        assert "+" in result.value

    def test_div(self):
        result = resolve_expression("@div(10, 3)", _context())
        assert result is not None
        assert "//" in result.value

    def test_max(self):
        result = resolve_expression("@max(1, 5, 3)", _context())
        assert result is not None
        assert "max" in result.value

    def test_min(self):
        result = resolve_expression("@min(1, 5, 3)", _context())
        assert result is not None
        assert "min" in result.value

    def test_mod(self):
        result = resolve_expression("@mod(7, 3)", _context())
        assert result is not None
        assert "%" in result.value

    def test_mul(self):
        result = resolve_expression("@mul(3, 4)", _context())
        assert result is not None
        assert "*" in result.value

    def test_rand(self):
        result = resolve_expression("@rand(1, 100)", _context())
        assert result is not None
        assert "randint" in result.value

    def test_range(self):
        result = resolve_expression("@range(0, 10)", _context())
        assert result is not None
        assert "range" in result.value

    def test_sub(self):
        result = resolve_expression("@sub(10, 3)", _context())
        assert result is not None
        assert "-" in result.value


class TestDateTimeFunctions:
    def test_add_days(self):
        result = resolve_expression("@addDays('2024-01-01T00:00:00', 5)", _context())
        assert result is not None
        assert result.kind == "notebook_code"
        assert "timedelta" in result.value
        assert "days" in result.value

    def test_add_days_with_format(self):
        result = resolve_expression("@addDays('2024-01-01T00:00:00', 5, 'yyyy-MM-dd')", _context())
        assert result is not None
        assert "strftime" in result.value
        assert "%Y-%m-%d" in result.value

    def test_add_hours(self):
        result = resolve_expression("@addHours('2024-01-01T00:00:00', 3)", _context())
        assert result is not None
        assert "hours" in result.value

    def test_add_minutes(self):
        result = resolve_expression("@addMinutes('2024-01-01T00:00:00', 30)", _context())
        assert result is not None
        assert "minutes" in result.value

    def test_add_seconds(self):
        result = resolve_expression("@addSeconds('2024-01-01T00:00:00', 90)", _context())
        assert result is not None
        assert "seconds" in result.value

    def test_add_to_time(self):
        result = resolve_expression("@addToTime('2024-01-01T00:00:00', 2, 'Hour')", _context())
        assert result is not None
        assert "timedelta" in result.value
        assert "hours" in result.value

    def test_day_of_month(self):
        result = resolve_expression("@dayOfMonth('2024-01-15T00:00:00')", _context())
        assert result is not None
        assert ".day" in result.value

    def test_day_of_week(self):
        result = resolve_expression("@dayOfWeek('2024-01-15T00:00:00')", _context())
        assert result is not None
        assert "isoweekday" in result.value

    def test_day_of_year(self):
        result = resolve_expression("@dayOfYear('2024-01-15T00:00:00')", _context())
        assert result is not None
        assert "tm_yday" in result.value

    def test_format_date_time(self):
        result = resolve_expression("@formatDateTime('2024-01-01T00:00:00', 'yyyy-MM-dd')", _context())
        assert result is not None
        assert "strftime" in result.value
        assert "%Y-%m-%d" in result.value

    def test_format_date_time_no_format(self):
        result = resolve_expression("@formatDateTime('2024-01-01T00:00:00')", _context())
        assert result is not None
        assert "isoformat" in result.value

    def test_get_future_time(self):
        result = resolve_expression("@getFutureTime(5, 'Day')", _context())
        assert result is not None
        assert "timedelta" in result.value
        assert "days" in result.value
        assert "from datetime import datetime, timezone, timedelta" in result.imports

    def test_get_past_time(self):
        result = resolve_expression("@getPastTime(3, 'Hour')", _context())
        assert result is not None
        assert "timedelta" in result.value
        assert "hours" in result.value

    def test_start_of_day(self):
        result = resolve_expression("@startOfDay('2024-01-15T14:30:00')", _context())
        assert result is not None
        assert "hour=0" in result.value

    def test_start_of_hour(self):
        result = resolve_expression("@startOfHour('2024-01-15T14:30:00')", _context())
        assert result is not None
        assert "minute=0" in result.value

    def test_start_of_month(self):
        result = resolve_expression("@startOfMonth('2024-01-15T14:30:00')", _context())
        assert result is not None
        assert "day=1" in result.value

    def test_subtract_from_time(self):
        result = resolve_expression("@subtractFromTime('2024-01-15T00:00:00', 5, 'Day')", _context())
        assert result is not None
        assert "timedelta" in result.value
        assert " - " in result.value


class TestNestedFunctions:
    def test_concat_with_toLower_and_toUpper(self):
        result = resolve_expression("@concat(toLower('Hello'), toUpper('world'))", _context())
        assert result is not None
        assert result.kind == "notebook_code"
        assert "lower" in result.value
        assert "upper" in result.value

    def test_if_with_equals(self):
        result = resolve_expression("@if(equals(1, 1), 'yes', 'no')", _context())
        assert result is not None
        assert result.kind == "notebook_code"
        assert "==" in result.value
        assert "if" in result.value

    def test_deeply_nested(self):
        result = resolve_expression("@concat(toLower(trim(' HELLO ')), '_', toUpper('world'))", _context())
        assert result is not None
        assert result.kind == "notebook_code"
        assert "lower" in result.value
        assert "strip" in result.value
        assert "upper" in result.value

    def test_first_of_create_array(self):
        result = resolve_expression("@first(createArray('a', 'b', 'c'))", _context())
        assert result is not None
        assert result.kind == "notebook_code"
        assert "[0]" in result.value

    def test_length_of_split(self):
        result = resolve_expression("@length(split('a,b,c', ','))", _context())
        assert result is not None
        assert "len" in result.value
        assert "split" in result.value

    def test_nested_math(self):
        result = resolve_expression("@add(mul(2, 3), sub(10, 4))", _context())
        assert result is not None
        assert result.kind == "notebook_code"

    def test_replace_with_pipeline_param(self):
        result = resolve_expression(
            "@replace(pipeline().parameters.path, '/old/', '/new/')",
            _context(),
        )
        assert result is not None
        assert result.kind == "notebook_code"
        assert "replace" in result.value
        assert "dbutils.widgets.get" in result.value


class TestFunctionsWithDabRefs:
    def test_to_lower_with_pipeline_param(self):
        result = resolve_expression("@toLower(pipeline().parameters.env)", _context())
        assert result is not None
        assert result.kind == "notebook_code"
        assert "lower" in result.value
        assert "dbutils.widgets.get" in result.value

    def test_concat_with_variable_and_literal(self):
        result = resolve_expression(
            "@concat(variables('prefix'), '_suffix')",
            _context(prefix="SetPrefix"),
        )
        assert result is not None
        assert result.kind == "notebook_code"
        assert "dbutils.widgets.get" in result.value

    def test_equals_with_activity_output(self):
        result = resolve_expression(
            "@equals(activity('Check').output.firstRow.status, 'done')",
            _context(),
        )
        assert result is not None
        assert result.kind == "notebook_code"
        assert "==" in result.value
        assert "dbutils.widgets.get" in result.value


class TestBackwardCompat:
    def test_parse_expression_returns_value(self):
        result = parse_expression("@pipeline().RunId", _context())
        assert result == "{{job.run_id}}"

    def test_parse_expression_returns_none_for_unsupported(self):
        result = parse_expression("@dataUri('hello')", _context())
        assert result is None

    def test_parse_expression_for_dab_returns_ref(self):
        result = parse_expression_for_dab("@pipeline().RunId")
        assert result == "{{job.run_id}}"

    def test_parse_expression_for_dab_returns_ref_for_utcnow(self):
        # ``@utcNow()`` maps to the Databricks job start time dynamic value
        # so it can land in DAB YAML directly.
        result = parse_expression_for_dab("@utcNow()")
        assert result == "{{job.start_time.iso_datetime}}"

    def test_parse_expression_for_dab_returns_none_for_non_expression(self):
        result = parse_expression_for_dab("plain_string")
        assert result is None


class TestGlobalParameters:
    """Change expr-resolver-globalparams-and-wrappers (P0)."""

    def _ctx_with_globals(self, **globals_) -> TranslationContext:
        return TranslationContext(
            global_parameters=MappingProxyType(dict(globals_)),
        )

    def test_global_parameter_resolves_to_literal(self):
        ctx = self._ctx_with_globals(env_variable="t")
        result = resolve_expression("@pipeline().globalParameters.env_variable", ctx)
        assert result is not None
        assert result.kind == "literal"
        assert result.value == "t"

    def test_global_parameter_value_dict(self):
        ctx = self._ctx_with_globals(
            env_variable={"type": "string", "value": "t"},
        )
        result = resolve_expression("@pipeline().globalParameters.env_variable", ctx)
        assert result is not None
        assert result.kind == "literal"
        assert result.value == "t"

    def test_global_parameter_missing_falls_back_to_dab_ref(self):
        ctx = TranslationContext()
        result = resolve_expression("@pipeline().globalParameters.something", ctx)
        assert result is not None
        assert result.kind == "dab_ref"
        assert result.value == "{{job.parameters.something}}"

    def test_concat_with_globals_resolves_fully(self):
        # C-01: when every part collapses to a literal, the whole concat
        # is itself a literal so downstream consumers don't have to eval.
        ctx = self._ctx_with_globals(env_variable="t", libFileName="myjar.jar")
        expr = (
            "@concat('/Volumes/datahub01', pipeline().globalParameters.env_variable, "
            "'/x/', pipeline().globalParameters.libFileName)"
        )
        result = resolve_expression(expr, ctx)
        assert result is not None
        assert result.kind == "literal"
        assert result.value == "/Volumes/datahub01t/x/myjar.jar"


class TestNoopWrappers:
    """Change expr-resolver-globalparams-and-wrappers (P0): @json/@string/@array."""

    def test_json_wrapper_around_pipeline_param(self):
        ctx = TranslationContext()
        result = resolve_expression("@json(pipeline().parameters.items)", ctx)
        assert result is not None
        assert result.kind == "dab_ref"
        assert result.value == "{{job.parameters.items}}"

    def test_string_wrapper_around_pipeline_param(self):
        ctx = TranslationContext()
        result = resolve_expression("@string(pipeline().parameters.value)", ctx)
        assert result is not None
        assert result.kind == "dab_ref"
        assert result.value == "{{job.parameters.value}}"

    def test_array_wrapper_around_pipeline_param(self):
        ctx = TranslationContext()
        result = resolve_expression("@array(pipeline().parameters.lst)", ctx)
        assert result is not None
        assert result.kind == "dab_ref"
        assert result.value == "{{job.parameters.lst}}"


class TestTrailingWhitespaceFunctionCall:
    """Change expr-resolver-globalparams-and-wrappers (P0): VAR-003 regex anchor bug."""

    def test_string_wrapper_with_trailing_newlines(self):
        ctx = TranslationContext()
        # Previously the regex anchor at end refused trailing whitespace.
        result = resolve_expression(
            "@string(activity('X').output.runOutput.year)\n\n",
            ctx,
        )
        assert result is not None
        # Should hit the function-call branch (string wraps activity output).
        assert result.kind == "dab_ref"
        assert "tasks.X.values" in result.value


class TestItemSafeNav:
    """Change expr-resolver-globalparams-and-wrappers (P0): VAR-005."""

    def test_item_safe_nav_chain_resolves(self):
        ctx = TranslationContext()
        result = resolve_expression(
            "@coalesce(item()?.condition?.name, 'fallback')",
            ctx,
        )
        assert result is not None
        assert result.kind == "notebook_code"
        # The chain walk should emit nested .get() calls
        assert ".get('condition')" in result.value
        assert ".get('name')" in result.value
        assert "'fallback'" in result.value


class TestLinkedServiceParameter:
    """Change linked-service-parameter-resolution (P0): NB-4, LSC-001."""

    def test_linked_service_param_resolves_with_supplied_value(self):
        ctx = TranslationContext(
            linked_service_parameters=MappingProxyType({"clusterVersion": "16.4.x-scala2.12"}),
        )
        result = resolve_expression("@linkedService().clusterVersion", ctx)
        assert result is not None
        assert result.kind == "literal"
        assert result.value == "16.4.x-scala2.12"

    def test_linked_service_param_missing_returns_none(self):
        ctx = TranslationContext()
        result = resolve_expression("@linkedService().clusterVersion", ctx)
        # Without a value, we can't deterministically resolve; caller must
        # supply via with_linked_service_parameters or accept None.
        assert result is None


class TestFunctionCallWithAttribute:
    """Change fix-attribute-access-on-function-results (P1): CF3-004."""

    def test_json_call_with_trailing_attribute_lowers_to_notebook_code(self):
        ctx = TranslationContext()
        result = resolve_expression(
            "@toUpper(json(pipeline().parameters.items).type)",
            ctx,
        )
        assert result is not None
        # toUpper wraps the json(...).type expression; the inner
        # json(...).type chain must resolve to notebook_code so the bridge
        # can pick it up.
        assert result.kind == "notebook_code"

    def test_function_call_with_attribute_alone_lowers_to_notebook_code(self):
        ctx = TranslationContext()
        result = resolve_expression(
            "@json(pipeline().parameters.items).type",
            ctx,
        )
        assert result is not None
        assert result.kind == "notebook_code"
        # The lowered code chains .get('type') onto a json-loaded widget.
        assert ".get('type')" in result.value
