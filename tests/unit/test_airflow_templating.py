"""Unit tests for Airflow Jinja -> DAB reference conversion (flowx.sources.airflow.templating)."""

from __future__ import annotations

from flowx.sources.airflow.templating import (
    convert_sql_template,
    convert_template,
    date_param_default,
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
