# Authoring an `AgenticComponentActivity` (the escape hatch)

Read this from Step 3 of the `flowx-route` skill, when a task of the authored pipeline is one no typed
activity produces.

When a task is one no typed activity produces (for example a pipeline resource of your own design), emit
an `AgenticComponentActivity` task inside the authored pipeline. The authored
**pipeline** carries the required `tags.source == "adf"`; the task carries the raw bundle components
the package phase writes verbatim:

```json
{
  "name": "ExamplePipeline",
  "tags": {"source": "adf"},
  "tasks": [
    {
      "name": "ExampleTask",
      "task_key": "example_task",
      "type": "AgenticComponentActivity",
      "files": [
        {"path": "example/pipeline.py", "content": "# authored pipeline source ..."}
      ],
      "resources": [
        {"resource_key": "example_pipeline", "definition": { "channel": "current", "...": "raw pipeline resource" }}
      ],
      "task": {"pipeline_task": {"pipeline_id": "${resources.pipelines.example_pipeline.id}"}},
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

**Set the Lakeflow pipeline / Connect resource `channel` to `current`** unless the official page for that
connector or feature says its pipelines must use `preview`, and cite that page in the plan's
`conversation`. What a channel does is on the pipeline settings page
(https://docs.databricks.com/aws/en/ldp/configure-pipeline); read the edition for the target workspace's
cloud (the page's cloud selector, or learn.microsoft.com for Azure Databricks). The routing
recommendation exposes each agentic option's release states as a neutral `release_disclosures` list.
Show the `label` route puts there for each pattern (`src/flowx/routing.py`); `ga` has none.

As in `flowx-enrich`, the state is disclosure, not a gate: there is no `doNotSuggest` rule, so a
verified **Private Preview** connector may still be recommended. Show its label, say what the release
types page says about access to that state (https://docs.databricks.com/aws/en/release-notes/release-types),
and let the customer decide. And **verify GA-vs-Preview
status before recommending any connector or Lakeflow Connect pattern**: do not hardcode GA/Preview status or dates (release state changes), check
the feature's current release state **and** target-workspace availability against the current public
Databricks docs, and cite the source. (Same structured grounding rule the `flowx-enrich` skill
applies when authoring recommended patterns.)
