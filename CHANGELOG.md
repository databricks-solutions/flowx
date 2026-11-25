# flowx Changelog

All notable changes to flowx will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

## [Unreleased]

### Added
- Airflow `PapermillOperator` converts to a Databricks notebook task: the `.ipynb` is located under the
  discover folder, converted to a Databricks notebook with its `parameters` cell reading widgets, and packaged
  into the bundle; `parameters` become `base_parameters` and `output_nb` is dropped with a disclosure. Display-only
  magics are dropped (`%%time`, `%%timeit`, `%%python`, `%%capture` without a variable name, `%time`, `%timeit`,
  `%matplotlib`, `%autoreload`, `%load_ext autoreload` and `%load_ext sql`, and `%config` for display, completion
  and `SqlMagic` settings). A named `%%capture`, any other `%load_ext` or `%config`, and any other magic become gaps.
- A spark-submit Python application named by its Airflow-machine path is packaged from the export when discover's
  folders hold it. One the export does not hold (or that matches several files) is a placeholder asking for the
  script to be copied into the discover folder, instead of a download stub reported as verified.
- `days_ago(n, hour=, minute=, second=)` start dates anchor `timedelta` schedules, and `default_args.start_date`
  is a recognized DAG setting.
- `concurrency` / `max_active_tasks` on a single-task DAG maps to Job `max_concurrent_runs` (the lower of it and
  `max_active_runs`). On a multi-task DAG with one active run, a limit at least the DAG's parallel width is
  recorded as an intentional no-op, since it can never make a task wait; any other task limit stays a gap.
- A `spark_python_task` with no compute gets a serverless job environment, since a Python script task must name
  its compute.

### Fixed
- Two different spark-submit scripts in one job that share a file name (for example `/home/alice/load.py` and
  `/home/bob/load.py`) no longer overwrite each other in the bundle. Each distinct script packages to its own file:
  `scripts/<name>` when the name is used once, otherwise `scripts/<task_key>/<name>`. Tasks that run the same script
  still share one file.
- Generated notebooks start each `%md` and `%sh` cell with its magic, so the cell runs as markdown or shell
  instead of Python. This affected papermill notebooks and `BashOperator` `%sh` notebooks without macro widgets.
- A `{{ ... }}` expression in a spark-submit command stays one argument, so `--run-date {{ ds }}` converts like
  any other macro instead of becoming a gap.
- `{{ }}` text inside a packaged papermill notebook or spark-submit script is no longer reported as unresolved
  Airflow Jinja, since Airflow renders only the operator's arguments.
- A papermill notebook whose `parameters` cell already reads `dbutils.widgets` becomes a gap instead of getting
  a second set of widgets, which could lose the DAG's parameter values.

## [0.1.0]

### Added
- Initial release of the flowx library
