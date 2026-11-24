# flowx Changelog

All notable changes to flowx will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

## [Unreleased]

### Added
- Airflow `PapermillOperator` converts to a Databricks notebook task: the `.ipynb` is located under the
  discover folder, converted to a Databricks notebook with its `parameters` cell reading widgets, and packaged
  into the bundle; `parameters` become `base_parameters` and `output_nb` is dropped with a disclosure.
- A spark-submit Python application named by its Airflow-machine path is packaged from the export when discover's
  folders hold it, instead of an empty download placeholder.
- `days_ago(n, hour=, minute=, second=)` start dates anchor `timedelta` schedules, and `default_args.start_date`
  is a recognized DAG setting.
- `concurrency` / `max_active_tasks` on a single-task DAG maps to Job `max_concurrent_runs` (the lower of it and
  `max_active_runs`); on a multi-task DAG it stays a gap.

## [0.1.0]

### Added
- Initial release of the flowx library
