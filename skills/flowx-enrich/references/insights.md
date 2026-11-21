# The agentic insights shape

Read this from Step 4 of the `flowx-enrich` skill, which covers when to skip enrich, how much to write
and how to run it. This file covers what to write and the rules the validator enforces. The
`flowx-route` step reads the block to present the agentic conversion option per component.

## Contents

- [The insights shape](#the-insights-shape)
- [Idempotency & safety](#idempotency--safety)

## The insights shape

You author these four content fields, plus `authored_against` by default: copy the
`source_graphs_sha256` value from the `inventory.json` you read. It is optional, but when you give it
enrich refuses the insights if it no longer matches, because that means discover ran again after you
wrote them. When the inventory records no `source_graphs_sha256`, leave it out: enrich rejects it
there. The library stamps `schema_version`, the `inventory_sha256` fingerprint, the
`source_graphs_sha256` it was checked against and its own `agentic_insights_sha256` when it records
them, binding your insights to the exact inventory and source graphs they describe. Do not author any
of those four keys yourself:

```json
{
  "authored_against": "<source_graphs_sha256 copied from the inventory.json you read>",
  "overview": "One short factory-wide narrative — what this collection of pipelines is.",
  "system_recommendation": {
    "headline": "The one decision a migrator must make before any per-pipeline work",
    "recommended_patterns": [
      {"pattern": "<an option you found in the current Databricks docs>", "fit": "<why it fits these pipelines>", "simplification_pattern": true, "release_state": "<verify live, one of ga|public_preview|private_preview|beta|unknown>", "release_state_source": "<cite the current Databricks doc you verified>"},
      {"pattern": "<the like-for-like port>", "fit": "<why it remains an option>", "simplification_pattern": false}
    ],
    "cascade": ["<which pipelines the decision replaces or changes>"],
    "decision_driver": "<the open fact or check that decides between the patterns>"
  },
  "pipeline_insights": [
    {
      "pipeline": "LoadSource",
      "intent": "<what the pipeline does, in a sentence>",
      "databricks_pattern": "<the target pattern, in plain words>",
      "recommended_patterns": [
        {"pattern": "<an option you found in the current Databricks docs>", "fit": "<why it fits this pipeline>", "simplification_pattern": true, "release_state": "<verify live, one of ga|public_preview|private_preview|beta|unknown>", "release_state_source": "<cite the current Databricks doc you verified>"}
      ],
      "conversion_notes": ["source: <system> via <source type>", "fact: <value> (source: code | data | user | assumed)"],
      "risk_if_ignored": "<what breaks or is lost if the insight is ignored>"
    }
  ],
  "pipeline_relationships": [
    {
      "from_pipeline": "Orchestrator",
      "to_pipeline": "LoadSource",
      "lineage_edge": {"edge_type": "control", "edge_identity": "<via_task_key from a real control edge>"},
      "relationship_summary": "Orchestrator invokes LoadSource"
    },
    {
      "from_pipeline": "LoadSource",
      "to_pipeline": "BuildMart",
      "lineage_edge": {
        "edge_type": "inferred",
        "edge_identity": "shared table sales.curated",
        "evidence": "Both notebooks read/write sales.curated, but the hand-off is inside notebook code the parser can't see",
        "confidence": "medium"
      }
    }
  ]
}
```

The order of the patterns in this example is not a ranking; order yours by the facts, as the rule on
`recommended_patterns` below says.

### Rules the validator enforces

- **Some content.** Set at least one of `overview`, `system_recommendation`, `pipeline_insights` or
  `pipeline_relationships`; an empty list does not count. Insights with nothing in them are refused.
- **Foreign keys.** Every `pipeline_insights[].pipeline` and every relationship
  `from_pipeline` / `to_pipeline` must be a pipeline name listed in the inventory's `pipelines`.
  The one exception is a `control` relationship whose edge resolves (see below): its endpoints may
  name a pipeline discover left unlisted, because the resolved edge proves it is in the export.
- **One insight per pipeline.** A pipeline may appear in `pipeline_insights` at most once; put
  everything you have to say about it in that one entry.
- **Field types.** Pipeline names are strings. When present, `pattern_name`, `intent`,
  `databricks_pattern` and `risk_if_ignored` (per pipeline) and `relationship_summary`,
  `databricks_pattern` and `risk_if_ignored` (per relationship) must be strings, and
  `conversion_notes` a list of strings. Leave out a field you have nothing for: an explicit `null` is
  refused wherever a field is optional.
- **Text length.** No text value may exceed 20,000 characters (`overview`, `intent`, `evidence`, a
  `cascade` step or any other); the violation names the field. Summarise rather than paste source code.
- **`recommended_patterns`** (per pipeline and system-wide): **1–4** patterns. The validator enforces
  the count and that each has a non-empty `pattern` and `fit` and a boolean `simplification_pattern`;
  it does **not** enforce ordering. Set `simplification_pattern: true` **only** for a distinctive
  capability that collapses a whole legacy pattern (a managed connector, declarative `AUTO CDC`, Auto
  Loader, system tables replacing a home-grown logging tier), not for a like-for-like port. Order the
  patterns by the facts you recorded, the one they favour first, and say why in `fit`. The review page
  (`src/flowx/reporting/routing_review.py`) shows the first system-wide pattern as the recommended
  approach and a component's first pattern as its suggested pattern, so put a like-for-like port first
  when the facts favour it.
- **`release_state` + `release_state_source`** (per recommended pattern): the pattern's *verified*
  Databricks release state and the doc that grounds it. `release_state` is one of `ga`,
  `public_preview`, `private_preview`, `beta`, `unknown`. It is **required** on any pattern with
  `simplification_pattern: true` (a distinctive capability must declare its verified release state);
  optional otherwise. `release_state_source` must be a string whenever it is set, and a non-empty
  citation is **required** when `release_state` is `public_preview` / `private_preview` / `beta`; not
  required for `ga` / `unknown`.
  Verify the state against the current public docs **before** recommending; see the SKILL.md
  release-state step.
- **`system_recommendation`** needs a non-empty `headline` and a `recommended_patterns` list;
  `cascade` (non-empty strings) and `decision_driver` are optional.
- **Relationship edges** come in two tiers:
  - `control` — an **annotation** of a proven control edge. `edge_identity` must be the
    `via_task_key` of a real `control_edges` entry, marked `"resolved": true`, whose
    `source_workflow`/`target_workflow` match
    your `from_pipeline`/`to_pipeline`. Do **not** set `evidence`/`confidence` — the proven edge is
    the evidence. Such an edge can be annotated even when its `target_workflow` is not a listed
    pipeline (discover ran with `--pipeline`, or the called pipeline has no activities).
  - `inferred` — a coupling the deterministic layer never found (data flow inside notebook code, an
    external trigger, a shared table the parser didn't resolve). There is nothing to resolve
    against, so `edge_identity` is your own non-empty descriptor of the coupling, and you **must** also
    supply a non-empty `evidence` string and a `confidence` of `high` / `medium` / `low`.
  - There is no deterministic cross-pipeline **data** tier in v1: the deterministic data edges are
    intra-pipeline and task-scoped, so a cross-pipeline data coupling rides the `inferred` tier.

#### Worked example: a cross-pipeline data coupling → `inferred` (not a `data` edge_type)

`edge_type` accepts only `control` or `inferred`. There is **no** `data` edge_type. When one pipeline
writes a table (or file) that another pipeline reads, record it on the `inferred` tier with the
`evidence` + `confidence` the tier requires:

```jsonc
// WRONG — the validator rejects this and tells you to use the 'inferred' tier
{
  "from_pipeline": "LoadSource",
  "to_pipeline": "BuildMart",
  "lineage_edge": {"edge_type": "data", "edge_identity": "sales.curated"}
}

// RIGHT — a cross-pipeline data coupling rides the 'inferred' tier
{
  "from_pipeline": "LoadSource",
  "to_pipeline": "BuildMart",
  "lineage_edge": {
    "edge_type": "inferred",
    "edge_identity": "shared table sales.curated",
    "evidence": "LoadSource writes sales.curated and BuildMart reads it; the hand-off is inside notebook code the parser can't see",
    "confidence": "medium"
  }
}
```

## Idempotency & safety

See "Idempotency and safety" under Output in the `flowx-enrich` SKILL.md: a validation failure writes nothing.
