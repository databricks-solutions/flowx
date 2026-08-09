# Airflow Agentic Gap Contract v1

The provider receives a `GapEnvelope` produced by flowx. It does not receive authority to alter the
captured graph.

## Resolution shape

```json
{
  "contract_version": "1",
  "gap_id": "finding fingerprint from the envelope",
  "status": "resolved",
  "baseline_report_sha256": "copied from the envelope",
  "source_sha256": "copied from the envelope",
  "provider": {
    "name": "airflow-to-dabs",
    "version": "0.2.0",
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

SQL uses `{"kind": "sql", "file": "task.sql", "parameters": {}}` and a single generated file
whose language is `sql`.

`needs_input` and `deferred` omit `replacement` and `generated_files` and add a non-empty `reason`.
They are terminal reviewed outcomes: the linked `NotImplementedError` placeholder remains and no
automatic retry occurs.

## Hard boundaries

- Only `notebook` and `sql` leaf replacements are allowed in v1.
- The replacement cannot express `name`, `task_key`, `depends_on`, retries, timeouts, compute,
  libraries, schedules, or control-flow fields.
- Generated file paths are relative and cannot contain `..`.
- Every generated file is inline and hash-bound; external workspace paths are not accepted.
- Python payloads may mention Airflow in comments but may not contain `import airflow` or
  `from airflow ...` statements.
- Airflow Jinja is rejected. Databricks dynamic references such as `{{job.parameters.x}}`,
  `{{tasks.upstream.values.x}}`, and `{{input}}` remain valid.
- Every source argument in the envelope has exactly one disposition and a non-empty rationale.
- The provider identity must match the pinned `airflow-to-dabs` v0.2.0 knowledge release.
