"""Static Airflow DAG parser (see the sibling modules for the split implementation)."""

from __future__ import annotations

from flowx.sources.airflow.loader.api import (
    detect_hosts,
    discover_dags,
    load_airflow_dag,
    load_airflow_dags,
    load_pipelines,
)

__all__ = [
    "detect_hosts",
    "discover_dags",
    "load_airflow_dag",
    "load_airflow_dags",
    "load_pipelines",
]
