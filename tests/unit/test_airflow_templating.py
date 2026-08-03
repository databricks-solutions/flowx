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
    assert convert_template("{{ ds }}") == ("{{job.parameters.run_date}}", {"run_date"})
    assert convert_template("{{ execution_date }}") == ("{{job.parameters.execution_date}}", {"execution_date"})
    assert convert_template("{{ logical_date }}") == ("{{job.parameters.logical_date}}", {"logical_date"})


def test_run_id_macro_stays_inline():
    # run_id has no backfill relevance -- it maps to its inline dynamic ref and declares no parameter.
    assert convert_template("{{ run_id }}") == ("{{job.run_id}}", set())


def test_dashless_macro_left_untouched():
    # ds_nodash has no dynamic-value form; leave it as an (unresolved) reference rather than emitting
    # an invalid ref.
    assert convert_template("{{ ds_nodash }}") == ("{{ ds_nodash }}", set())


def test_sql_execution_date_binds_a_job_parameter():
    marked, params = convert_sql_template("SELECT * FROM t WHERE d = {{ ds }}")
    assert marked == "SELECT * FROM t WHERE d = :run_date"
    assert params == {"run_date": "{{job.parameters.run_date}}"}


def test_sql_run_id_binds_inline_ref():
    marked, params = convert_sql_template("SELECT '{{ run_id }}'")
    assert marked == "SELECT ':run_id'"
    assert params == {"run_id": "{{job.run_id}}"}


def test_date_param_default_is_schedule_aware():
    # Cron/periodic jobs have a scheduled trigger time (correct on normal runs, no start-time drift);
    # event-triggered or unscheduled jobs approximate with the run start time.
    assert date_param_default("iso_date", {"kind": "schedule"}) == "{{job.trigger.time.iso_date}}"
    assert date_param_default("iso_datetime", {"kind": "periodic"}) == "{{job.trigger.time.iso_datetime}}"
    assert date_param_default("iso_date", {"kind": "file_arrival"}) == "{{job.start_time.iso_date}}"
    assert date_param_default("iso_date", None) == "{{job.start_time.iso_date}}"


def test_shell_template_threads_macros_through_named_vars():
    command, bindings = convert_shell_template("etl.py --date {{ ds }} --run {{ run_id }} --env {{ params.env }}")
    assert command == "etl.py --date $run_date --run $run_id --env $env"
    assert bindings == {
        "run_date": "{{job.parameters.run_date}}",
        "run_id": "{{job.run_id}}",
        "env": "{{job.parameters.env}}",
    }


def test_shell_template_leaves_unknown_expressions():
    command, bindings = convert_shell_template("echo {{ some.unknown }}")
    assert command == "echo {{ some.unknown }}"
    assert bindings == {}


def test_macro_param_default_covers_date_and_run_id_and_none():
    assert macro_param_default("run_date", {"kind": "schedule"}) == "{{job.trigger.time.iso_date}}"
    assert macro_param_default("run_id", None) == "{{job.run_id}}"
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
