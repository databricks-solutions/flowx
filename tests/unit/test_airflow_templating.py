"""Unit tests for Airflow Jinja -> DAB reference conversion and cron -> Quartz translation."""

from __future__ import annotations

from flowx.sources.airflow.loader.schedule import _cron_to_quartz
from flowx.sources.airflow.templating import (
    convert_shell_template,
    convert_sql_template,
    convert_template,
    macro_param_default,
)


def test_interval_macros_read_the_date_resolver_task_values():
    # Interval macros are computed at run time by the generated resolver task, so consumers read its
    # task values and declare no job parameter of their own.
    assert convert_template("{{ ds }}") == ("{{tasks.__flowx_airflow_dates.values.ds}}", set())
    assert convert_template("{{ execution_date }}") == (
        "{{tasks.__flowx_airflow_dates.values.execution_date}}",
        set(),
    )
    assert convert_template("{{ logical_date }}") == ("{{tasks.__flowx_airflow_dates.values.logical_date}}", set())
    assert convert_template("{{ data_interval_end }}") == (
        "{{tasks.__flowx_airflow_dates.values.data_interval_end}}",
        set(),
    )


def test_run_id_macro_stays_inline():
    # run_id has no backfill relevance -- it maps to its inline dynamic ref and declares no parameter.
    assert convert_template("{{ run_id }}") == ("{{job.run_id}}", set())


def test_derived_interval_macros_resolve_through_the_resolver():
    # The resolver renders every interval macro from one logical instant, including the formats with
    # no dynamic-value equivalent.
    assert convert_template("{{ ds_nodash }}") == ("{{tasks.__flowx_airflow_dates.values.ds_nodash}}", set())
    assert convert_template("{{ ts_nodash }}") == ("{{tasks.__flowx_airflow_dates.values.ts_nodash}}", set())
    assert convert_template("{{ prev_ds }}") == ("{{tasks.__flowx_airflow_dates.values.prev_ds}}", set())
    assert convert_template("{{ macros.ds_add(ds, 1) }}") == ("{{ macros.ds_add(ds, 1) }}", set())


def test_sql_execution_date_binds_the_resolver_value():
    marked, params = convert_sql_template("SELECT * FROM t WHERE d = {{ ds }}")
    assert marked == "SELECT * FROM t WHERE d = :__flowx_airflow_date_ds"
    assert params == {"__flowx_airflow_date_ds": "{{tasks.__flowx_airflow_dates.values.ds}}"}


def test_sql_macro_as_entire_string_literal_removes_sql_quotes():
    marked, params = convert_sql_template("SELECT * FROM sales WHERE order_date = '{{ ds }}'")
    assert marked == "SELECT * FROM sales WHERE order_date = :__flowx_airflow_date_ds"
    assert params == {"__flowx_airflow_date_ds": "{{tasks.__flowx_airflow_dates.values.ds}}"}


def test_sql_macro_embedded_in_string_literal_remains_unresolved():
    marked, params = convert_sql_template("SELECT 'partition_{{ ds }}'")
    assert marked == "SELECT 'partition_{{ ds }}'"
    assert params == {}


def test_sql_macros_in_quoted_identifiers_and_adjacent_tokens_remain_unresolved():
    for sql in (
        'SELECT * FROM "{{ params.table }}"',
        "SELECT * FROM `{{ params.table }}`",
        "SELECT * FROM analytics.{{ params.table }}",
        "SELECT {{ params.column }}_suffix FROM source",
    ):
        assert convert_sql_template(sql) == (sql, {})


def test_sql_macros_in_typed_and_prefixed_literals_remain_unresolved():
    for sql in (
        "SELECT DATE '{{ ds }}'",
        "SELECT TIMESTAMP '{{ ts }}'",
        "SELECT INTERVAL '{{ params.hours }}' HOUR",
        "SELECT r'{{ params.pattern }}'",
    ):
        assert convert_sql_template(sql) == (sql, {})


def test_sql_quote_scanning_ignores_quotes_in_comments():
    sql = "-- owner's date\nSELECT '{{ ds }}'"
    assert convert_sql_template(sql) == (
        "-- owner's date\nSELECT :__flowx_airflow_date_ds",
        {"__flowx_airflow_date_ds": "{{tasks.__flowx_airflow_dates.values.ds}}"},
    )


def test_sql_unquoted_identifier_uses_identifier_parameter_marker():
    sql = "SELECT * FROM {{ params.table }} WHERE id = {{ params.id }}"
    assert convert_sql_template(sql) == (
        "SELECT * FROM IDENTIFIER(:table) WHERE id = :id",
        {
            "table": "{{job.parameters.table}}",
            "id": "{{job.parameters.id}}",
        },
    )


def test_sql_run_id_binds_inline_ref():
    marked, params = convert_sql_template("SELECT '{{ run_id }}'")
    assert marked == "SELECT :__flowx_airflow_run_id"
    assert params == {"__flowx_airflow_run_id": "{{job.run_id}}"}


def test_shell_template_threads_macros_through_named_vars():
    command, bindings = convert_shell_template("etl.py --date {{ ds }} --run {{ run_id }} --env {{ params.env }}")
    assert command == ("etl.py --date ${__flowx_airflow_date_ds} --run ${__flowx_airflow_run_id} --env ${env}")
    assert bindings == {
        "__flowx_airflow_date_ds": "{{tasks.__flowx_airflow_dates.values.ds}}",
        "__flowx_airflow_run_id": "{{job.run_id}}",
        "env": "{{job.parameters.env}}",
    }


def test_shell_template_braces_adjacent_macros_and_breaks_out_of_single_quotes():
    command, bindings = convert_shell_template("echo '/data/{{ ds }}_load.csv'")
    assert command == "echo '/data/'\"${__flowx_airflow_date_ds}\"'_load.csv'"
    assert bindings == {"__flowx_airflow_date_ds": "{{tasks.__flowx_airflow_dates.values.ds}}"}


def test_shell_template_leaves_nonexpanding_or_escaped_contexts_unresolved():
    for command in (
        "echo $'{{ ds }}'",
        "printf \\{{ ds }}",
        "cat <<'EOF'\n{{ ds }}\nEOF",
    ):
        assert convert_shell_template(command) == (command, {})


def test_shell_quote_scanning_ignores_quotes_in_comments():
    command = "# owner's note\necho {{ ds }}"
    assert convert_shell_template(command) == (
        "# owner's note\necho ${__flowx_airflow_date_ds}",
        {"__flowx_airflow_date_ds": "{{tasks.__flowx_airflow_dates.values.ds}}"},
    )


def test_template_namespaces_do_not_collapse_equal_source_names():
    converted, params = convert_template(
        "{{ ds }}|{{ params.run_date }}|{{ var.value.run_date }}|{{ dag_run.conf['run_date'] }}"
    )
    assert converted == (
        "{{tasks.__flowx_airflow_dates.values.ds}}|{{job.parameters.run_date}}|"
        "{{job.parameters.__flowx_airflow_variable_run_date}}|"
        "{{job.parameters.__flowx_airflow_conf_run_date}}"
    )
    assert params == {
        "run_date",
        "__flowx_airflow_variable_run_date",
        "__flowx_airflow_conf_run_date",
    }


def test_reserved_flowx_parameter_reference_remains_unresolved():
    value = "{{ params.__flowx_airflow_run_date }}"
    assert convert_template(value) == (value, set())


def test_bracket_parameter_names_must_be_valid_job_parameter_identifiers():
    value = "{{ params['bad-name'] }}"
    assert convert_template(value) == (value, set())


def test_shell_template_leaves_unknown_expressions():
    command, bindings = convert_shell_template("echo {{ some.unknown }}")
    assert command == "echo {{ some.unknown }}"
    assert bindings == {}


def test_macro_param_default_covers_resolver_inputs_run_id_and_none():
    assert macro_param_default("__flowx_airflow_trigger_time") == "{{job.trigger.time.iso_datetime}}"
    assert macro_param_default("__flowx_airflow_trigger_type") == "{{job.trigger.type}}"
    assert macro_param_default("__flowx_airflow_logical_date") == ""
    assert macro_param_default("__flowx_airflow_run_id") == "{{job.run_id}}"
    assert macro_param_default("env") is None  # a user param, not macro-derived


def test_quartz_never_restricts_both_day_of_month_and_day_of_week():
    # Unix cron ORs a restricted dom with a restricted dow; Quartz rejects an expression that sets
    # both, so one must become '?' or the emitted job fails to validate.
    assert _cron_to_quartz("0 0 1 * 1") == "0 0 0 ? * 2"
    assert _cron_to_quartz("0 0 15 * MON") == "0 0 0 ? * MON"
    # The single-restriction cases keep their field and '?' the other.
    assert _cron_to_quartz("0 0 1 * *") == "0 0 0 1 * ?"
    assert _cron_to_quartz("0 6 * * 1") == "0 0 6 ? * 2"
    assert _cron_to_quartz("0 0 * * *") == "0 0 0 ? * *"


def test_quartz_splits_week_wrapping_weekday_ranges():
    # Unix 5-0 (Fri-Sun) shifts to 6-1, which Quartz reads as a descending (empty) range.
    assert _cron_to_quartz("0 0 * * 5-0") == "0 0 0 ? * 6-7,1"
    assert _cron_to_quartz("0 0 * * 6-2") == "0 0 0 ? * 7,1-3"
    # A non-wrapping range is untouched apart from the +1 shift.
    assert _cron_to_quartz("0 0 * * 1-5") == "0 0 0 ? * 2-6"
