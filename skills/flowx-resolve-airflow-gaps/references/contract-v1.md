# Airflow Agentic Gap Contract v1

The provider receives a `GapEnvelope` produced by flowx. It does not receive authority to alter the captured graph.

`capture_identity` is flowx's source-capture identity and may differ from the collision-safe Databricks `task_key`. `task_path` identifies the exact placeholder location, including a nested `for_each` body; providers must copy neither field into the replacement payload.

## Resolution shape

```json
{
  "contract_version": "1",
  "gap_id": "finding fingerprint from the envelope",
  "status": "resolved",
  "baseline_report_sha256": "copied from the envelope",
  "source_sha256": "copied from the envelope",
  "task_sha256": "copied from the envelope",
  "graph_sha256": "copied from the envelope",
  "provider_sha256": "copied from the envelope",
  "request_sha256": "copied from the envelope",
  "provider": {
    "name": "airflow-to-dabs",
    "version": "copied from GapEnvelope.provider.version",
    "repository": "https://github.com/park-peter/airflow-to-dabs"
  },
  "model": {"name": "model identifier"},
  "replacement": {"kind": "notebook", "file": "task.py", "base_parameters": {}},
  "generated_files": [
    {
      "path": "task.py",
      "language": "python",
      "content": "# Databricks notebook source\nprint('resolved')\n",
      "sha256": "SHA-256 of content bytes"
    }
  ],
  "argument_disposition": [
    {
      "name": "task_id",
      "disposition": "preserved_by_flowx",
      "rationale": "Flowx preserves the collision-safe task identity."
    }
  ],
  "prerequisites": [],
  "warnings": [],
  "semantic_deltas": []
}
```

SQL uses `{"kind": "sql", "file": "task.sql", "parameters": {}}` and a single generated file whose language is `sql`.

Spark Python uses `{"kind": "spark_python", "file": "task.py", "parameters": ["--arg", "value"]}` and a single generated Python file. It is emitted as a Databricks `spark_python_task`.

`needs_input` and `deferred` omit `replacement` and `generated_files` and add a non-empty `reason`. They are terminal reviewed outcomes: the linked `NotImplementedError` placeholder remains and no automatic retry occurs.

## Hard boundaries

- Only `notebook`, `sql`, and `spark_python` leaf replacements are allowed in v1.
- The replacement cannot express `name`, `task_key`, `depends_on`, retries, timeouts, compute, libraries, schedules, or control-flow fields.
- Generated file paths are relative and cannot contain `..`.
- Every generated file is inline and hash-bound; external workspace paths are not accepted.
- Python payloads may mention Airflow in comments, docstrings, and other inert string literals but may not import Airflow through import statements or statically identifiable dynamic imports with literal module names or executed source. This validation enforces runtime compatibility and hygiene; it is not a Python security sandbox, and every accepted payload remains reviewed arbitrary Python. Notebook payloads must start with `# Databricks notebook source`; Spark Python scripts are ordinary valid Python files.
- Notebook `base_parameters` keys must use letters, digits, underscores, dots, and hyphens, starting with a letter or underscore. Flowx-owned names beginning with `__flowx` and Databricks task identity, graph, policy, and task-type field names are reserved case-insensitively.
- Airflow Jinja is rejected. Databricks dynamic references such as `{{job.parameters.x}}`, `{{tasks.upstream.values.x}}`, and `{{input}}` are valid in replacement parameter values, not in uploaded notebook or SQL source files; source files must read widgets or SQL named parameters.
- Every source argument in the envelope has exactly one disposition and a non-empty rationale.
- The provider identity must match `GapEnvelope.provider` and the prepared workspace's pinned knowledge release.
