---
name: flowx-resolve-airflow-gaps
description: >
  Resolve source-reconciled Airflow leaf gaps through the fingerprint-bound flowx contract. Use
  after Airflow conversion emits PlaceholderActivity tasks and before packaging the reviewed report.
---

# Resolve Airflow Leaf Gaps

Use this workflow only for Airflow reports whose deterministic conversion succeeded with gaps.
Flowx owns source parsing, task identity, dependencies, task policy, IR, and packaging. This skill
reasons about one prepared gap at a time using the migration knowledge from
[`park-peter/airflow-to-dabs` v0.2.0](https://github.com/park-peter/airflow-to-dabs/releases/tag/v0.2.0).
It must not parse the DAG independently or generate a second bundle.

Read [`references/contract-v1.md`](references/contract-v1.md) and the pinned
[`airflow-to-dabs-v0.2.0/PROFILE.md`](references/airflow-to-dabs-v0.2.0/PROFILE.md) before authoring a
resolution. The profile's `../../references/*.md` knowledge paths are relative to the upstream
v0.2.0 release. Resolve them against
`https://github.com/park-peter/airflow-to-dabs/tree/v0.2.0/references` or an exact local checkout of
that tag. If required knowledge is unavailable, return `needs_input` or `deferred`; never infer
missing operator semantics.

## 1. Prepare immutable gap envelopes

```bash
"$PY" -m flowx.adapter resolve-agentic prepare \
  --source airflow \
  --source-path <dag_file_or_directory> \
  --report <output_dir>/.work/translation_report.json \
  --output-dir <output_dir>
```

Preparation reparses the source, proves that it reproduces the deterministic report, and writes an
immutable baseline, source snapshot, manifest, and `GapEnvelope v1` objects under
`<output_dir>/.work/agentic/`. If the source or report no longer agrees, rerun convert first.

With MCP, call `flowx(command="resolve_agentic", parameters={"action": "prepare", "source":
"airflow", "airflow_source_path": ..., "report_path": ..., "output_dir": ...})`.

## 2. Produce one candidate per gap

Read the prepared envelope rather than reopening or reparsing the DAG. Return one of:

- `resolved`: exactly one self-contained Python notebook or SQL payload.
- `needs_input`: a concrete question or prerequisite blocks a safe migration.
- `deferred`: the gap is outside the leaf-only contract and remains a linked failing placeholder.

`KubernetesPodOperator` commonly returns `needs_input` when the image, secrets, storage, networking,
or compute assumptions cannot be preserved from the envelope alone. Do not present it as the default
successful example.

Every source argument must appear exactly once in `argument_disposition` as `consumed`,
`preserved_by_flowx`, or `ignored`. Every disposition needs a rationale; an ignored argument must
state the specific semantic loss. Never include task names, task keys, dependencies, retries,
timeouts, clusters, schedules, or other graph/policy fields in the replacement.

Generated code must be self-contained, contain no Airflow import statements, and contain no
unresolved Airflow Jinja. Comments may mention Airflow for provenance.

## 3. Stage candidates

```bash
"$PY" -m flowx.adapter resolve-agentic stage \
  --source airflow \
  --output-dir <output_dir> \
  --candidate <candidate.json> [--candidate <candidate.json> ...]
```

Stage validates fingerprints, source/report hashes, the pinned provider version, argument
disposition, generated-file hashes, Python imports, templates, and the constrained replacement
schema. Tampering after staging is a hard failure.

MCP accepts candidate objects inline with `action="stage"` and `candidates=[...]`.

## 4. Review and explicitly apply

Show the user each candidate's code, prerequisites, warnings, semantic deltas, ignored arguments,
provider version, and model provenance. Apply only the fingerprints the user accepts:

```bash
"$PY" -m flowx.adapter resolve-agentic apply \
  --source airflow \
  --output-dir <output_dir> \
  --accept-gap <fingerprint> [--accept-gap <fingerprint> ...]
```

`--accept-all` is only for replaying candidates already staged in a prior step; never combine it
with live candidate generation. Apply always rebuilds from the immutable deterministic baseline,
then proves task count, location, keys, dependencies, policy, and enclosing control flow are
unchanged. It writes `.work/translation_report.agentic.json` and keeps accepted evidence under
`metadata/agentic/` so package pruning does not destroy provenance.

Use a reduced `--accept-gap` allowlist to reject selected candidates while retaining others. Use
`--reset` to discard all accepted resolutions and start over from the deterministic baseline. A
source edit after prepare is a hard failure: rerun convert and prepare instead of applying stale
results.

Package the reviewed report explicitly:

```bash
"$PY" -m flowx.adapter package \
  --report <output_dir>/.work/translation_report.agentic.json \
  --output-dir <output_dir>
```

Package replays the kept baseline and accepted candidates before writing bundle files. Missing,
modified, or inconsistent evidence fails preflight.
