"""Template-reference helpers over generated activities."""

from __future__ import annotations

import re
from typing import Any

from flowx.models.ir import (
    Activity,
    SqlActivity,
)
from flowx.sources.airflow import templating

_WIDGET_GET = re.compile(r"""dbutils\.widgets\.get\(\s*['"]([A-Za-z_][A-Za-z0-9_]*)['"]\s*\)""")


_JOB_PARAM_REF = re.compile(r"\{\{\s*job\.parameters\.([A-Za-z0-9_]+)\s*\}\}")


def _convert_activity_templates(activity: Activity) -> set[str]:
    """Converts Airflow Jinja in an activity's parameter fields to DAB refs.

    Mutates ``base_parameters`` (NotebookActivity), ``parameters`` (Spark/Sql/RunJob),
    ``job_parameters`` (RunJob), and ``sql`` (SqlActivity) in place, returning the set
    of ``{{job.parameters.X}}`` names referenced so the pipeline can declare them.
    """
    referenced: set[str] = set()
    for attr in ("base_parameters", "job_parameters", "parameters"):
        value = getattr(activity, attr, None)
        if value:
            converted, refs = templating.convert_params(value)
            setattr(activity, attr, converted)
            referenced |= refs
    if isinstance(activity, SqlActivity):
        # SQL dynamic refs must go through :name markers + sql_task.parameters, not inline text.
        marked_sql, sql_params = templating.convert_sql_template(activity.sql)
        activity.sql = marked_sql
        activity.parameters = {**(activity.parameters or {}), **sql_params}
        # sql_task.parameters values that resolve to {{job.parameters.X}} need X declared.
        for value in sql_params.values():
            referenced |= set(_JOB_PARAM_REF.findall(value))
    # generated_source was already rewritten (Variable.get -> dbutils.widgets.get); collect the
    # widget names so the pipeline declares them as job parameters. Airflow runtime widgets use the
    # reserved __flowx_airflow_* namespace and must be declared; other __flowx_* widgets are task-local.
    generated = getattr(activity, "generated_source", None)
    if isinstance(generated, str):
        referenced |= {
            name
            for name in _WIDGET_GET.findall(generated)
            if not name.startswith(templating.FLOWX_INTERNAL_PARAMETER_PREFIX)
            or name.startswith(templating.FLOWX_AIRFLOW_PARAMETER_PREFIX)
        }
    return referenced


def _unresolved_activity_templates(activity: Activity) -> set[str]:
    """Returns residual Airflow Jinja expressions in task parameter fields."""
    unresolved: set[str] = set()
    for attribute in ("base_parameters", "job_parameters", "parameters", "sql", "generated_source"):
        unresolved |= templating.unresolved_jinja_expressions(getattr(activity, attribute, None))
    return unresolved


def _declared_param_default(name: str, dag_params: dict[str, Any], schedule: dict[str, object] | None) -> Any:
    """Returns the Databricks-required default for a declared job parameter.

    A DAG ``params={...}`` default wins. A reserved macro-derived parameter gets its schedule-aware or
    inline default so the value resolves at run time and native backfills can override logical dates.
    Everything else defaults to an empty string.
    """
    if dag_params.get(name) is not None:
        return dag_params[name]
    macro_default = templating.macro_param_default(name, schedule)
    if macro_default is not None:
        return macro_default
    return ""
