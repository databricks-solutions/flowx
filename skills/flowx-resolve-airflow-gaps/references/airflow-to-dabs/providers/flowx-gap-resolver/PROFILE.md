# flowx Airflow Gap Resolver Profile

Resolve exactly one source-reconciled Airflow leaf gap supplied by flowx. flowx owns DAG parsing,
capture identity, task keys, dependencies, task policy, control flow, IR, and bundle packaging. Do
not reopen or parse the original DAG, construct another task graph, or generate a bundle.

This profile implements flowx Airflow agentic gap contract `1` with the pinned provider identity:

```json
{
  "name": "airflow-to-dabs",
  "version": "0.2.2",
  "repository": "https://github.com/park-peter/airflow-to-dabs"
}
```

## Inputs

Accept one `GapEnvelope` JSON object. Use only the captured source, arguments, surrounding task-key
context, DAG settings, and finding reason in that envelope. Reject an envelope when:

- `contract_version` is not `"1"`;
- `source` is not `"airflow"`;
- `knowledge_provider` does not match the pinned provider identity;
- the requested behavior cannot be determined without source or deployment information absent from
  the envelope.

Read `../../references/operator-mapping.md` for operator semantics. Read another knowledge file
listed in `provider.json` only when the gap involves that domain. These references inform a leaf
resolution; they do not grant authority to emit jobs, triggers, clusters, pipelines, or graph edits.

## Resolution procedure

1. Classify the operator's intent from `operator_fqn`, `raw_definition`, and `arguments`.
2. Decide the terminal status:
   - `resolved`: one self-contained Python notebook, SQL file, or Spark Python script preserves the represented behavior.
   - `needs_input`: a concrete deployment fact, credential mapping, runtime dependency, or semantic
     choice is required before a safe leaf implementation can be written.
   - `deferred`: a faithful migration requires graph, control-flow, schedule, compute, resource, or
     other changes outside the leaf-only contract.
3. Account for every envelope argument exactly once in `argument_disposition`:
   - `consumed`: the generated payload or resolution decision uses it;
   - `preserved_by_flowx`: flowx retains it as task identity or policy;
   - `ignored`: the resolution intentionally omits it and states the exact behavioral loss.
   - `needs_input`: the argument depends on a concrete fact the user must provide before resolution.
4. Enumerate prerequisites, warnings, and semantic deltas. Never hide a dropped behavior in prose or
   omit an argument from the disposition list.
5. Return one `AgenticResolution` JSON object and no bundle files or graph patches.

## Resolved payload rules

- Emit exactly one replacement with `kind` equal to `notebook` or `sql`.
- Emit exactly one inline generated file whose `path` matches `replacement.file` and whose `sha256`
  is the lowercase SHA-256 of the UTF-8 content bytes.
- For a notebook, emit syntactically valid Python with no `import airflow` or `from airflow ...`
  statements. Airflow may be named in comments.
- For SQL, use Databricks SQL syntax. Put dynamic values in named parameter markers and declare the
  corresponding string values in `replacement.parameters`.
- Do not emit unresolved Airflow Jinja. Databricks dynamic references such as
  `{{job.parameters.x}}`, `{{tasks.upstream.values.x}}`, `{{input}}`, and `{{backfill.iso_date}}` are
  allowed when valid for the captured context.
- Keep the replacement self-contained. Record required libraries, secrets, UC objects, network
  access, or user decisions in `prerequisites`; do not invent them.

## Forbidden authority

Never include task names, task keys, dependencies, retries, timeouts, clusters, libraries,
schedules, triggers, notifications, control-flow bodies, or graph mutations in `replacement`.
Return `deferred` when those changes are required. Return `needs_input` when a safe leaf result might
be possible after the user supplies missing information.

## Output shape

Use only these top-level fields:

- always: `contract_version`, `gap_id`, `status`, `baseline_report_sha256`, `source_sha256`,
  `task_sha256`, `graph_sha256`, `provider_sha256`, `request_sha256`,
  `provider`, `model`, `argument_disposition`, `prerequisites`, `warnings`, `semantic_deltas`;
- `resolved`: add `replacement` and `generated_files`;
- `needs_input` or `deferred`: add `reason` and omit `replacement` and `generated_files`.

Copy the gap, baseline, and source hashes verbatim from the envelope. Set `model.name` to the actual
model identifier. Do not retry `needs_input` or `deferred` automatically.

Use the paired files under `fixtures/` as contract examples. They are interoperability fixtures,
not permission to substitute their assumptions into another gap.
