# Authoring an `AgenticComponentActivity` (the escape hatch)

Read this from Step 3 of the `flowx-route` skill, when the target can't be expressed by the typed engine.

When the target can't be expressed by the typed engine (e.g. a managed Lakeflow Connect ingestion
pipeline), emit an `AgenticComponentActivity` task inside the authored pipeline. The authored
**pipeline** carries the required `tags.source == "adf"`; the task carries the raw bundle components
the package phase writes verbatim:

```json
{
  "name": "IngestAll",
  "tags": {"source": "adf"},
  "tasks": [
    {
      "name": "IngestAll",
      "task_key": "ingest_all",
      "type": "AgenticComponentActivity",
      "files": [
        {"path": "ingest/lakeflow_connect.py", "content": "# authored pipeline source ..."}
      ],
      "resources": [
        {"resource_key": "ingest_all_pipeline", "definition": { "channel": "current", "...": "raw pipeline resource" }}
      ],
      "task": {"pipeline_task": {"pipeline_id": "${resources.pipelines.ingest_all_pipeline.id}"}},
      "raw_definition": { "...": "original source definitions, retained for auditing" }
    }
  ]
}
```

- `tags`: the authored pipeline **must** set `tags.source` to `"adf"` (routing/agentic is ADF-only);
  fill-agentic fails closed otherwise.
- `files`: files written below the bundle `src/`, so `path` is relative to it (a resource refers to
  one as `../src/<path>`); each is `path` + either UTF-8 `content` or base64 `binary_content`.
- `resources`: bundle resources in the `resource_key` + raw `definition` shape the bundle writer
  expects.
- `environments` (optional): job environments, each `environment_key` + `spec`, for a serverless task
  that names one.
- `task`: the raw Databricks task fragment (`pipeline_task` or `notebook_task`) wiring to an
  authored resource or file. Libraries and compute go here, never on the activity.
- `raw_definition`: the original source definition, retained for provenance.

The full contract, including every field package refuses, is the `AgenticComponentActivity` section of
`flowx-convert`'s [`references/adf.md`](../../flowx-convert/references/adf.md). fill-agentic runs each authored pipeline through package's prepare
and bundle-writing path, so a component package would refuse is refused at fill, with nothing written.

**Default the Lakeflow pipeline / Connect resource `channel` to `current` (stable/GA).** Do **not**
emit `channel: preview` by default. Let the enriched insight's **structured `release_state`** drive
the channel — the routing recommendation exposes each agentic option's states as a neutral
`release_disclosures` list (factual labels, no alarm). Surface the state as disclosure, tiered as:

- `ga` — `channel: current`; **silent** (not surfaced).
- `unknown`: the plain label **"Release state not verified"**, so a state nobody could verify never
  looks the same as a verified `ga`.
- `public_preview` — the informational label **"Public Preview (production-ready)"**: generally
  production-ready and supported per Databricks; still confirm workspace availability. Emit
  `channel: preview` only with the cited `release_state_source`.
- `private_preview` — the plain factual label **"Private Preview"**. Emit `channel: preview` only
  with the cited `release_state_source`.
- `beta` — the plain factual label **"Beta"**. Emit `channel: preview` only with the cited
  `release_state_source`.

As in `flowx-enrich`, the state is disclosure, not a gate: there is no `doNotSuggest` rule, so a
verified **Private Preview** connector may still be recommended. Show its label, say that Private
Preview typically needs workspace enrollment, and let the customer decide. And **verify GA-vs-Preview
status before recommending any connector or Lakeflow Connect pattern**: do not hardcode GA/Preview status or dates (release state changes), check
the feature's current release state **and** target-workspace availability against the current public
Databricks docs, and cite the source. (Same structured grounding rule the `flowx-enrich` skill
applies when authoring recommended patterns.)
