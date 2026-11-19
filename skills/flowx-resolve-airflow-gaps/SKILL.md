---
name: flowx-resolve-airflow-gaps
description: Resolve source-reconciled Airflow leaf gaps through the fingerprint-bound flowx contract. Use after Airflow conversion emits PlaceholderActivity tasks and before packaging the reviewed report.
---

# Resolve Airflow leaf gaps

The Airflow step between convert and package. It reads an Airflow report whose deterministic conversion
succeeded with gaps, and resolves one prepared gap at a time using the pinned migration knowledge from
[`park-peter/airflow-to-dabs`](https://github.com/park-peter/airflow-to-dabs/tree/main/providers/flowx-gap-resolver).
It writes `.work/translation_report.agentic.json` and keeps the accepted evidence under `metadata/agentic/`.
Flowx owns source parsing, task identity, dependencies, task policy, IR and packaging. Never parse the
DAG yourself or generate a second bundle.

## Before you start

- Convert has run for this Airflow source and the report has gaps.
- Read [`references/contract-v1.md`](references/contract-v1.md) and the pinned
  [`airflow-to-dabs/providers/flowx-gap-resolver/PROFILE.md`](references/airflow-to-dabs/providers/flowx-gap-resolver/PROFILE.md)
  before authoring a resolution. The profile and every knowledge file it references are vendored from
  the exact upstream tag and commit under `references/airflow-to-dabs/`. If the knowledge you need is not
  there, return `needs_input` or `deferred`; never infer missing operator semantics.

## How to run

- CLI: `"$PY" -m flowx.adapter resolve-agentic prepare|stage|apply --source airflow --output-dir <output_dir> ...`
  (the flags for each action are in the steps).
- MCP: `flowx(command="resolve_agentic", parameters={"action": "prepare", "source": "airflow", "airflow_source_path": ..., "report_path": ..., "output_dir": ...})`;
  `action="stage"` takes the candidate objects inline as `candidates=[...]`.

Common MCP and CLI rules: [`../shared/running-flowx.md`](../shared/running-flowx.md).

## Steps

### Step 1: Prepare immutable gap envelopes

```bash
"$PY" -m flowx.adapter resolve-agentic prepare \
  --source airflow \
  --source-path <dag_file_or_directory> \
  --report <output_dir>/.work/translation_report.json \
  --output-dir <output_dir>
```

Preparation reparses the source, proves that it reproduces the deterministic report, and writes an
immutable baseline, source snapshot, manifest, and `GapEnvelope v1` objects under
`<output_dir>/.work/agentic/`. If the source or report no longer agrees, run convert again first.

### Step 2: Produce one candidate per gap

Read the prepared envelope rather than reopening or reparsing the DAG. Return one of:

- `resolved`: exactly one self-contained Python notebook or SQL payload.
- `needs_input`: a concrete question or prerequisite blocks a safe migration.
- `deferred`: the gap is outside the leaf-only contract and remains a linked failing placeholder.

`KubernetesPodOperator` commonly returns `needs_input` when the image, secrets, storage, networking, or
compute assumptions cannot be preserved from the envelope alone. Do not present it as the default
successful example.

Every source argument must appear exactly once in `argument_disposition` as `consumed`,
`preserved_by_flowx`, `ignored`, or `needs_input`. Every disposition needs a rationale; an ignored
argument must state the specific semantic loss. Never include task names, task keys, dependencies,
retries, timeouts, clusters, schedules, or other graph or policy fields in the replacement.

Generated code must be self-contained, must not import Airflow through import statements or literal
dynamic imports, and must contain no template expressions. Python notebooks must start with
`# Databricks notebook source`. Put Databricks dynamic references in replacement parameters and read
them through notebook widgets or SQL named parameters. Notebook parameter keys must avoid Flowx and
Databricks task-schema namespaces. Comments, docstrings, and inert strings may mention Airflow for
provenance; static validation is runtime hygiene, not a Python security sandbox.

### Step 3: Stage candidates

```bash
"$PY" -m flowx.adapter resolve-agentic stage \
  --source airflow \
  --output-dir <output_dir> \
  --candidate <candidate.json> [--candidate <candidate.json> ...]
```

Stage validates fingerprints, source and report hashes, the pinned provider version, argument
disposition, generated-file hashes, Python imports, templates, and the constrained replacement schema.
Tampering after staging is a hard failure. Identical content is idempotent; use `--replace` to replace
different content for an already-staged gap. Stage returns an immutable, hash-addressed review-manifest
path for the complete staged candidate set.

### Step 4: Review and explicitly apply

Show the user each candidate's code, prerequisites, warnings, semantic deltas, ignored arguments,
provider version, and model provenance. Apply only the fingerprints the user accepts:

```bash
"$PY" -m flowx.adapter resolve-agentic apply \
  --source airflow \
  --output-dir <output_dir> \
  --accept-gap <fingerprint> [--accept-gap <fingerprint> ...]
```

Apply always rebuilds from the immutable deterministic baseline, then proves task count, location, keys,
dependencies, policy, and enclosing control flow are unchanged.

- `--accept-all` is only for replaying candidates already staged in a prior step; never combine it with
  live candidate generation. It requires `--review-manifest <path>` and rejects the operation if the
  reviewed candidate IDs or hashes no longer exactly match the staged set.
- A reduced `--accept-gap` allowlist rejects the candidates it leaves out and keeps the others.
- To decline every staged candidate after reviewing that exact set, run apply with `--review-complete
  --review-manifest <manifest-returned-by-stage>`. This records the staged candidates as declined.
  Prepared gaps without a staged candidate remain unreviewed; the flag makes no claim about artifacts
  that did not exist.
- `--reset` discards all accepted resolutions and starts over from the deterministic baseline. A normal
  apply after a source edit is a hard failure: run convert and prepare again instead of applying stale
  results. Reset is the recovery path and restores the durable baseline even after source drift or the
  usual pruning of `.work/`.

### Step 5: Package the reviewed report

```bash
"$PY" -m flowx.adapter package \
  --report <output_dir>/.work/translation_report.agentic.json \
  --output-dir <output_dir>
```

Package replays the kept baseline and accepted candidates before writing bundle files. Missing, modified,
or inconsistent evidence fails preflight. The rest of packaging is the `flowx-package` skill.

## Output

- `.work/agentic/`: the immutable baseline, source snapshot, manifest and gap envelopes (prepare).
- `.work/translation_report.agentic.json`: the report with the accepted resolutions (apply).
- `metadata/agentic/`: the accepted evidence, kept so the pruning of `.work/` by package does not destroy
  provenance.

The folder layout is in [`../shared/output-folder.md`](../shared/output-folder.md).

## Reference

- [`references/contract-v1.md`](references/contract-v1.md): the gap envelope and candidate contract.
- [`references/airflow-to-dabs/providers/flowx-gap-resolver/PROFILE.md`](references/airflow-to-dabs/providers/flowx-gap-resolver/PROFILE.md): the pinned upstream resolver profile and the knowledge files it uses (vendored, not edited here).
- [`../shared/running-flowx.md`](../shared/running-flowx.md): the common MCP and CLI rules.
