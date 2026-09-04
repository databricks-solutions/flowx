"""Unit tests for Airflow Jinja -> DAB reference conversion and cron -> Quartz translation."""

from __future__ import annotations

from flowx.sources.airflow.loader import _cron_to_quartz
from flowx.sources.airflow.templating import (
    convert_shell_template,
    convert_sql_template,
    convert_template,
    date_param_default,
    macro_param_default,
)


def test_execution_date_macros_route_through_job_parameters():
    # Logical-date macros become an overridable job parameter (not an inline start_time ref) so a
    # native Databricks backfill can override them per replayed window.
    assert convert_template("{{ ds }}") == (
        "{{job.parameters.__flowx_airflow_run_date}}",
        {"__flowx_airflow_run_date"},
    )
    assert convert_template("{{ execution_date }}") == (
        "{{job.parameters.__flowx_airflow_execution_date}}",
        {"__flowx_airflow_execution_date"},
    )
    assert convert_template("{{ logical_date }}") == (
        "{{job.parameters.__flowx_airflow_logical_date}}",
        {"__flowx_airflow_logical_date"},
    )


def test_run_id_macro_stays_inline():
    # run_id has no backfill relevance -- it maps to its inline dynamic ref and declares no parameter.
    assert convert_template("{{ run_id }}") == ("{{job.run_id}}", set())


def test_dashless_macro_left_untouched():
    # ds_nodash has no dynamic-value form; leave it as an (unresolved) reference rather than emitting
    # an invalid ref.
    assert convert_template("{{ ds_nodash }}") == ("{{ ds_nodash }}", set())


def test_sql_execution_date_binds_a_job_parameter():
    marked, params = convert_sql_template("SELECT * FROM t WHERE d = {{ ds }}")
    assert marked == "SELECT * FROM t WHERE d = :__flowx_airflow_run_date"
    assert params == {"__flowx_airflow_run_date": "{{job.parameters.__flowx_airflow_run_date}}"}


def test_sql_macro_as_entire_string_literal_removes_sql_quotes():
    marked, params = convert_sql_template("SELECT * FROM sales WHERE order_date = '{{ ds }}'")
    assert marked == "SELECT * FROM sales WHERE order_date = :__flowx_airflow_run_date"
    assert params == {"__flowx_airflow_run_date": "{{job.parameters.__flowx_airflow_run_date}}"}


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
        "-- owner's date\nSELECT :__flowx_airflow_run_date",
        {"__flowx_airflow_run_date": "{{job.parameters.__flowx_airflow_run_date}}"},
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


def test_date_param_default_is_schedule_aware():
    # Cron/periodic jobs have a scheduled trigger time (correct on normal runs, no start-time drift);
    # event-triggered or unscheduled jobs approximate with the run start time.
    assert date_param_default("iso_date", {"kind": "schedule"}) == "{{job.trigger.time.iso_date}}"
    assert date_param_default("iso_datetime", {"kind": "periodic"}) == "{{job.trigger.time.iso_datetime}}"
    assert date_param_default("iso_date", {"kind": "file_arrival"}) == "{{job.start_time.iso_date}}"
    assert date_param_default("iso_date", None) == "{{job.start_time.iso_date}}"


def test_shell_template_threads_macros_through_named_vars():
    command, bindings = convert_shell_template("etl.py --date {{ ds }} --run {{ run_id }} --env {{ params.env }}")
    assert command == ("etl.py --date ${__flowx_airflow_run_date} --run ${__flowx_airflow_run_id} --env ${env}")
    assert bindings == {
        "__flowx_airflow_run_date": "{{job.parameters.__flowx_airflow_run_date}}",
        "__flowx_airflow_run_id": "{{job.run_id}}",
        "env": "{{job.parameters.env}}",
    }


def test_shell_template_braces_adjacent_macros_and_breaks_out_of_single_quotes():
    command, bindings = convert_shell_template("echo '/data/{{ ds }}_load.csv'")
    assert command == "echo '/data/'\"${__flowx_airflow_run_date}\"'_load.csv'"
    assert bindings == {"__flowx_airflow_run_date": "{{job.parameters.__flowx_airflow_run_date}}"}


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
        "# owner's note\necho ${__flowx_airflow_run_date}",
        {"__flowx_airflow_run_date": "{{job.parameters.__flowx_airflow_run_date}}"},
    )


def test_template_namespaces_do_not_collapse_equal_source_names():
    converted, params = convert_template(
        "{{ ds }}|{{ params.run_date }}|{{ var.value.run_date }}|{{ dag_run.conf['run_date'] }}"
    )
    assert converted == (
        "{{job.parameters.__flowx_airflow_run_date}}|{{job.parameters.run_date}}|"
        "{{job.parameters.__flowx_airflow_variable_run_date}}|"
        "{{job.parameters.__flowx_airflow_conf_run_date}}"
    )
    assert params == {
        "__flowx_airflow_run_date",
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


def test_macro_param_default_covers_date_and_run_id_and_none():
    assert macro_param_default("__flowx_airflow_run_date", {"kind": "schedule"}) == ("{{job.trigger.time.iso_date}}")
    assert macro_param_default("__flowx_airflow_run_id", None) == "{{job.run_id}}"
    assert macro_param_default("env", None) is None  # a user param, not macro-derived


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
