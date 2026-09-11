"""DAG-level policy: job timeout, failure email, retry-email, per-setting disposition."""

from __future__ import annotations

import ast

from flowx.sources.airflow import operators as ops
from flowx.sources.airflow import templating
from flowx.sources.airflow.loader.visitor import _DagVisitor

_RECOGNIZED_DAG_SETTINGS = frozenset(
    {
        "dag_id",
        "schedule",
        "schedule_interval",
        "start_date",
        "timezone",
        "catchup",
        "default_args",
        "params",
        "default_args.retries",
        "default_args.retry_delay",
        "default_args.execution_timeout",
        "dagrun_timeout",
        "max_consecutive_failed_dag_runs",
        "sla_miss_callback",
        "default_args.depends_on_past",
        "default_args.email",
        "default_args.email_on_failure",
        "default_args.email_on_retry",
        "default_args.env",
        "tags",
        "description",
        "doc_md",
        "dag_display_name",
        "default_args.owner",
    }
)


_NON_EXECUTION_DAG_SETTINGS = frozenset({"tags", "description", "doc_md", "dag_display_name", "default_args.owner"})


def _job_timeout_seconds(visitor: _DagVisitor) -> int | None:
    return templating.timedelta_seconds(visitor.dag_kwargs.get("dagrun_timeout"))


def _job_email_notifications(visitor: _DagVisitor) -> dict[str, list[str]]:
    recipients = templating.literal_email_recipients(visitor.default_args.get("email"))
    on_failure = visitor.default_args.get("email_on_failure")
    failure_enabled = on_failure is None or (isinstance(on_failure, ast.Constant) and on_failure.value is True)
    if recipients and failure_enabled:
        return {"on_failure": recipients}
    return {}


def _retry_email_is_active(visitor: _DagVisitor) -> bool:
    """Returns whether any captured task can emit an Airflow retry email."""
    for _, _, kwargs in visitor.operators.values():
        retry_node = kwargs.get("retries", visitor.default_args.get("retries"))
        if retry_node is None:
            continue
        retry_count = ops.literal_value(retry_node)
        if isinstance(retry_count, int) and not isinstance(retry_count, bool) and retry_count <= 0:
            continue
        on_retry = kwargs.get("email_on_retry", visitor.default_args.get("email_on_retry"))
        if isinstance(on_retry, ast.Constant) and on_retry.value is False:
            continue
        return True
    return False


def _dag_setting_disposition(name: str, visitor: _DagVisitor) -> dict[str, str] | None:
    """Classifies recognized DAG settings as mapped, intentional no-ops, or runtime gaps."""
    if name not in _RECOGNIZED_DAG_SETTINGS:
        return {
            "status": "gap",
            "message": f"Airflow DAG setting {name!r} has no deterministic Databricks Jobs mapping.",
            "rationale": "no_deterministic_databricks_jobs_mapping",
        }
    if name == "dagrun_timeout":
        if _job_timeout_seconds(visitor) is not None:
            return {
                "status": "mapped",
                "target": "job.timeout_seconds",
                "rationale": "preserved_as_databricks_job_run_timeout",
            }
        return {
            "status": "gap",
            "message": (
                "Airflow dagrun_timeout must be a static positive timedelta before it can map to Job timeout_seconds."
            ),
            "rationale": "dag_run_timeout_not_statically_resolvable",
        }
    if name == "default_args.depends_on_past":
        value = visitor.default_args.get("depends_on_past")
        if isinstance(value, ast.Constant) and value.value in {False, None}:
            return {
                "status": "ignored",
                "rationale": "disabled_cross_run_dependency_has_no_runtime_effect",
            }
        return {
            "status": "gap",
            "message": (
                "Airflow depends_on_past requires each task instance to depend on the prior DAG run; "
                "Databricks Jobs has no equivalent cross-run task dependency."
            ),
            "rationale": "cross_run_task_state_not_representable",
        }
    if name == "max_consecutive_failed_dag_runs":
        value = visitor.dag_kwargs.get("max_consecutive_failed_dag_runs")
        if isinstance(value, ast.Constant) and value.value in {0, None}:
            return {
                "status": "ignored",
                "rationale": "automatic_pause_after_failures_is_disabled",
            }
        return {
            "status": "gap",
            "message": (
                "Airflow max_consecutive_failed_dag_runs can automatically pause a DAG after repeated failures; "
                "Databricks Jobs has no equivalent automatic-pause policy."
            ),
            "rationale": "automatic_pause_after_consecutive_failures_not_representable",
        }
    if name == "sla_miss_callback":
        value = visitor.dag_kwargs.get("sla_miss_callback")
        if isinstance(value, ast.Constant) and value.value is None:
            return {"status": "ignored", "rationale": "sla_callback_is_disabled"}
        return {
            "status": "gap",
            "message": (
                "Airflow sla_miss_callback executes an arbitrary SLA callback; configure a Databricks duration "
                "health rule and notification destination or migrate the callback explicitly."
            ),
            "rationale": "arbitrary_sla_callback_not_representable",
        }
    if name == "default_args.env":
        value = visitor.default_args.get("env")
        if (isinstance(value, ast.Constant) and value.value is None) or (
            isinstance(value, ast.Dict) and not value.keys
        ):
            return {"status": "ignored", "rationale": "empty_default_task_environment_has_no_runtime_effect"}
        return {
            "status": "gap",
            "message": (
                "Airflow default_args.env changes each task environment and may contain connection-derived values; "
                "map every value to Databricks task parameters or secrets before migration."
            ),
            "rationale": "default_task_environment_requires_runtime_secret_mapping",
        }
    if name in {"default_args.email", "default_args.email_on_failure", "default_args.email_on_retry"}:
        recipients = templating.literal_email_recipients(visitor.default_args.get("email"))
        on_failure = visitor.default_args.get("email_on_failure")
        on_retry = visitor.default_args.get("email_on_retry")
        sla_callback = visitor.dag_kwargs.get("sla_miss_callback")
        sla_callback_active = sla_callback is not None and not (
            isinstance(sla_callback, ast.Constant) and sla_callback.value is None
        )
        failure_disabled = isinstance(on_failure, ast.Constant) and on_failure.value is False
        retry_disabled = isinstance(on_retry, ast.Constant) and on_retry.value is False
        retry_active = _retry_email_is_active(visitor)
        if name == "default_args.email_on_failure":
            if failure_disabled:
                return {"status": "ignored", "rationale": "failure_email_notification_is_disabled"}
            if recipients is None:
                return {
                    "status": "gap",
                    "message": (
                        "Airflow failure email settings must be static before they can map to Job email notifications."
                    ),
                    "rationale": "failure_email_notification_not_statically_resolvable",
                }
            if not recipients:
                return {"status": "ignored", "rationale": "failure_email_has_no_recipients"}
            if not (isinstance(on_failure, ast.Constant) and on_failure.value is True):
                return {
                    "status": "gap",
                    "message": (
                        "Airflow failure email settings must be static before they can map to Job email notifications."
                    ),
                    "rationale": "failure_email_notification_not_statically_resolvable",
                }
            return {
                "status": "mapped",
                "target": "job.email_notifications.on_failure",
                "rationale": "preserved_as_databricks_job_failure_notification",
            }
        if name == "default_args.email_on_retry":
            if retry_disabled or not retry_active:
                return {"status": "ignored", "rationale": "retry_email_notification_has_no_runtime_effect"}
            if recipients is None:
                return {
                    "status": "gap",
                    "message": "Airflow retry email settings must be static before they can be migrated.",
                    "rationale": "retry_email_notification_not_statically_resolvable",
                }
            if not recipients:
                return {"status": "ignored", "rationale": "retry_email_has_no_recipients"}
            return {
                "status": "gap",
                "message": (
                    "Airflow email_on_retry sends a retry notification, but Databricks Jobs exposes start, "
                    "success, failure, and duration notifications rather than a retry notification event."
                ),
                "rationale": "retry_notification_event_not_available",
            }
        if failure_disabled and not retry_active:
            return {"status": "ignored", "rationale": "all_email_notification_events_are_disabled"}
        if recipients is None:
            return {
                "status": "gap",
                "message": "Airflow email recipients must be static strings before they can map to Job notifications.",
                "rationale": "email_recipients_not_statically_resolvable",
            }
        if retry_active:
            return {
                "status": "gap",
                "message": (
                    "Airflow retry email notifications have no Databricks retry event; failure recipients were "
                    "preserved, but the retry notification still requires an explicit replacement."
                ),
                "rationale": "email_recipients_include_unrepresented_retry_event",
            }
        if failure_disabled or not recipients:
            return {"status": "ignored", "rationale": "email_recipients_have_no_enabled_notification_event"}
        if sla_callback_active:
            return {
                "status": "gap",
                "target": "job.email_notifications.on_failure",
                "message": (
                    "Airflow email recipients were preserved for Job failure notifications, but SLA email and "
                    "callback delivery are not attached to a Databricks duration health rule."
                ),
                "rationale": "failure_email_preserved_but_sla_notification_requires_explicit_mapping",
            }
        return {
            "status": "mapped",
            "target": "job.email_notifications.on_failure",
            "rationale": "preserved_as_databricks_job_failure_notification",
        }
    return None
