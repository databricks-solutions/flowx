"""Lower agent-authored bundle components through the existing output channels."""

from __future__ import annotations

import base64
from pathlib import PurePosixPath, PureWindowsPath

from flowx.bundler.constants import DEFAULT_JOB_CLUSTER_KEY, MULTI_NODE_JOB_CLUSTER_KEY, SINGLE_NODE_JOB_CLUSTER_KEY
from flowx.models.dab import DabNotebook
from flowx.models.ir import AgenticComponentActivity
from flowx.preparer.workflow_preparer import PreparedActivity, build_common_task_fields
from flowx.utils import normalize_task_key

# flowx owns a task's identity, its upstream wiring, and its run policy; these come from
# the activity itself, so an authored task fragment may only say what the task runs.
FLOWX_OWNED_TASK_FIELDS = frozenset(
    {
        "task_key",
        "depends_on",
        "run_if",
        "timeout_seconds",
        "max_retries",
        "min_retry_interval_millis",
        "retry_on_timeout",
    }
)


_PAYLOADS_NEEDING_COMPUTE = frozenset(
    {"spark_python_task", "python_wheel_task", "spark_jar_task", "spark_submit_task", "dbt_task"}
)
_COMPUTE_KEYS = ("environment_key", "job_cluster_key", "existing_cluster_id", "new_cluster")
_BUNDLE_JOB_CLUSTER_KEYS = (DEFAULT_JOB_CLUSTER_KEY, SINGLE_NODE_JOB_CLUSTER_KEY, MULTI_NODE_JOB_CLUSTER_KEY)


def _source_relative_path(raw_path: object) -> str:
    """Return a safe path relative to the bundle's ``src`` directory."""
    relative_path = PurePosixPath(str(raw_path))
    windows_path = PureWindowsPath(str(raw_path))
    if (
        relative_path.is_absolute()
        or windows_path.is_absolute()
        # A drive or root without the other ("C:x.py", "\\tmp\\x.py") is not absolute on Windows but
        # still resolves outside the bundle once joined to the output directory.
        or windows_path.drive
        or windows_path.root
        or relative_path.as_posix() == "."
        or ".." in relative_path.parts
        or ".." in windows_path.parts
    ):
        raise ValueError(f"Agentic component file path {raw_path!r} must be relative to the bundle src directory")
    return relative_path.as_posix()


def _check_task_payload(task_key: str, task: dict[str, object]) -> None:
    """Require exactly one executable task payload (``notebook_task``, ``pipeline_task``, ...).

    Databricks names every task type ``<kind>_task``. A fragment with none packages fine but is
    rejected at deploy time, and one with two leaves which runs undefined, so both fail here.
    The same goes for a task that needs compute but names none, or names a job cluster the
    bundle does not define. A ``for_each_task`` body is checked the same way.
    """
    payloads = sorted(key for key in task if key.endswith("_task"))
    if len(payloads) != 1:
        found = ", ".join(payloads) if payloads else "none"
        raise ValueError(
            f"Agentic component {task_key!r} task must contain exactly one executable payload such as "
            f"pipeline_task or notebook_task (found: {found})"
        )
    payload = task[payloads[0]]
    if not isinstance(payload, dict):
        raise ValueError(f"Agentic component {task_key!r} task payload {payloads[0]} must be a mapping")
    # flowx binds a cluster only for notebooks, and these task types cannot run without compute.
    if payloads[0] in _PAYLOADS_NEEDING_COMPUTE and not any(key in task for key in _COMPUTE_KEYS):
        raise ValueError(
            f"Agentic component {task_key!r} {payloads[0]} must name its compute with {', '.join(_COMPUTE_KEYS)}"
        )
    if "job_cluster_key" in task and task["job_cluster_key"] not in _BUNDLE_JOB_CLUSTER_KEYS:
        raise ValueError(
            f"Agentic component {task_key!r} job_cluster_key {task['job_cluster_key']!r} must be one of the "
            f"bundle's job clusters: {', '.join(_BUNDLE_JOB_CLUSTER_KEYS)}"
        )
    if payloads[0] == "for_each_task" and isinstance(payload.get("task"), dict):
        _check_task_payload(task_key, payload["task"])


def _check_resource(task_key: str, resource: object) -> None:
    """Reject a resource that is not a mapping of a plain-identifier key to a definition mapping.

    Each resource is written to ``resources/<resource_key>.yml``, so a key with path characters
    could land outside the ``resources`` directory or overwrite another bundle file. The definition
    becomes the body of ``resources.pipelines.<resource_key>``, which the bundle expects to be a mapping.
    """
    if not isinstance(resource, dict):
        raise ValueError(
            f"Agentic component {task_key!r} resource {resource!r} must be a mapping with resource_key and definition"
        )
    resource_key = resource.get("resource_key")
    if not isinstance(resource_key, str) or not resource_key or resource_key != normalize_task_key(resource_key):
        raise ValueError(
            f"Agentic component {task_key!r} resource key {resource_key!r} must be a plain identifier "
            "of lowercase letters, digits, and single underscores"
        )
    if not isinstance(resource.get("definition"), dict):
        raise ValueError(f"Agentic component {task_key!r} resource {resource_key!r} definition must be a mapping")


def _check_environment(task_key: str, environment: object) -> None:
    """Reject an environment that is not exactly an ``environment_key`` string and a ``spec`` mapping.

    The entry is written as given into a job's ``environments`` list, whose items take only those two fields.
    """
    if (
        not isinstance(environment, dict)
        or environment.keys() != {"environment_key", "spec"}
        or not isinstance(environment["environment_key"], str)
        or not environment["environment_key"]
        or not isinstance(environment["spec"], dict)
    ):
        raise ValueError(
            f"Agentic component {task_key!r} environment {environment!r} must be a mapping of a non-empty "
            "environment_key string and a spec mapping"
        )


def _authored_file(task_key: str, file: object) -> DabNotebook:
    """Turn one authored ``files`` entry into a file the bundle writer keeps below ``src``.

    A file carries exactly one of text ``content`` or base64 ``binary_content``, and it must
    already be a string so the file is written byte for byte as authored.
    """
    if not isinstance(file, dict) or not isinstance(file.get("path"), str):
        raise ValueError(f"Agentic component {task_key!r} file {file!r} must be a mapping with a string path")
    relative_path = _source_relative_path(file["path"])
    if ("content" in file) == ("binary_content" in file):
        raise ValueError(
            f"Agentic component {task_key!r} file {relative_path!r} must set content or binary_content, "
            "exactly one of them"
        )
    if "binary_content" in file:
        encoded = file["binary_content"]
        if not isinstance(encoded, str):
            raise ValueError(
                f"Agentic component {task_key!r} file {relative_path!r} binary_content must be a base64 string"
            )
        try:
            decoded = base64.b64decode(encoded, validate=True)
        except ValueError as error:
            raise ValueError(
                f"Agentic component {task_key!r} file {relative_path!r} binary_content must be valid base64"
            ) from error
        return DabNotebook(relative_path=relative_path, binary_content=decoded, authored=True)
    content = file["content"]
    if not isinstance(content, str):
        raise ValueError(f"Agentic component {task_key!r} file {relative_path!r} content must be a string")
    return DabNotebook(relative_path=relative_path, content=content, authored=True)


def prepare(activity: AgenticComponentActivity, *, scope: str = "") -> PreparedActivity:
    """Pass authored files, resources, environments, and the task payload to the bundle writer.

    A serverless task names its compute with ``environment_key``; the component declares
    that environment in ``environments`` and the bundle writer adds it to the job that holds
    the task. Every referenced ``environment_key`` must be declared by some component, and
    two components may declare the same key only with an identical spec. A component cannot
    declare a job cluster, so a ``job_cluster_key`` must name one the bundle writer defines:
    ``default_cluster``, ``single_node_cluster`` or ``multi_node_cluster``. The bundle writer
    adds that cluster to the job's ``job_clusters``.

    The task's key, dependencies, run condition, timeout, and retries always come from
    the activity, never from the authored fragment, so an agent cannot rewire or re-time
    a task behind flowx's back. A fragment that tries to set any of them is rejected. When
    the component replaces a placeholder through ``merge_agentic``, the activity's own name,
    key, dependencies, timeout, and retries are taken from that placeholder.

    flowx does not override or remove any other value the fragment sets, except in three
    wiring passes that run on authored tasks exactly as on generated ones:

    - a ``{{tasks.X.values.Y}}`` reference to a task outside the same job is blanked,
      because task values do not cross job boundaries. Only notebook ``base_parameters``,
      ``run_job_task.job_parameters`` and ``condition_task`` operands are checked;
      SETUP.md lists the blanked notebook parameters and neutralised conditions but not
      blanked ``job_parameters``, and references in other payloads are left as they are;
    - inside a ForEach that runs as its own job, notebook ``base_parameters`` and
      ``condition_task`` operands are rewritten for that job: ``{{input.x}}`` becomes
      ``{{job.parameters.x}}``, ADF expressions become job-parameter references, and
      non-string values become strings;
    - a ``run_job_task`` whose ``job_id`` is ``${resources.jobs.X.id}`` for a job outside
      this bundle is pointed at an ``X_job_id`` bundle variable instead.

    Where the fragment leaves a value out, flowx adds only the plumbing the task needs:

    - the ForEach ``item`` parameter, only to a ``notebook_task`` (``base_parameters``) or
      ``run_job_task`` (``job_parameters``) that is the ForEach's only child; any other
      payload of an only child passes ``{{input}}`` itself. When the ForEach has several
      children, or its only child is an IfCondition or Switch holding the component, the
      children run as a ``<loop>_inner_tasks`` job where ``{{input}}`` does not resolve:
      reference ``{{job.parameters.item}}`` in a notebook ``base_parameters``, a
      ``run_job_task.job_parameters`` or a ``condition_task`` operand, which flowx forwards;
      other payloads are not scanned, so they cannot receive the item on their own;
    - the source activity's collapsed notifications, when the fragment sets neither
      ``email_notifications`` nor ``webhook_notifications``;
    - a job-cluster binding, only for a notebook task that names no compute and
      either has a classic compute mode, ships libraries, or (without a serverless compute
      mode) points at a workspace path outside the bundle. A bundle ``../src/`` notebook
      without libraries runs on serverless, and no other task type is given a cluster.

    The task is marked ``_authored`` so the bundle writer skips the parameter clean-ups
    meant for generated tasks; the marker is stripped before the job YAML is written.

    Raises:
        ValueError: The authored task fragment sets a flowx-owned field, a file entry is
            malformed or its path escapes the bundle's ``src`` directory, a resource is
            malformed or its key is not a plain identifier, an environment is malformed,
            a binary file is not valid base64, the task does not carry exactly one
            executable payload mapping, a ``spark_python_task``, ``python_wheel_task``,
            ``spark_jar_task``, ``spark_submit_task`` or ``dbt_task`` names no compute, or a
            ``job_cluster_key`` is not one of the bundle's job clusters.
    """
    del scope
    owned_fields = sorted(FLOWX_OWNED_TASK_FIELDS & activity.task.keys())
    if owned_fields:
        raise ValueError(
            f"Agentic component {activity.task_key!r} task wiring sets flowx-owned field(s) "
            f"{', '.join(owned_fields)}; set dependencies and run policy on the activity instead"
        )
    _check_task_payload(activity.task_key, activity.task)
    for resource in activity.resources:
        _check_resource(activity.task_key, resource)
    for environment in activity.environments:
        _check_environment(activity.task_key, environment)
    notebooks = [_authored_file(activity.task_key, file) for file in activity.files]
    owned_task_fields = {
        field: value for field, value in build_common_task_fields(activity).items() if field in FLOWX_OWNED_TASK_FIELDS
    }
    return PreparedActivity(
        task={**owned_task_fields, **activity.task, "_authored": True},
        notebooks=notebooks,
        pipeline_resources=list(activity.resources),
        environments=list(activity.environments),
    )
