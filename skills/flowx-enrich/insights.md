# The agentic insights shape

This is the reference for the `insights` JSON you author before calling `enrich`. The `flowx-enrich`
SKILL.md covers the workflow (no-LLM contract, the three authoring steps, how to run `enrich`, and
when a deterministic-only pass skips it); this file covers **what to write** and the rules the
validator enforces.

Author the insights when a human reader would benefit from a migration narrative — which pipelines
collapse onto a managed capability, how the factory hangs together, what the risky couplings are.
The `flowx-route` step reads this block to present the agentic conversion option per component.

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
      {"pattern": "<managed connector for your source>", "fit": "Replaces the child extractor family", "simplification_pattern": true, "release_state": "<verify live; do not copy — one of ga|public_preview|private_preview|beta|unknown>", "release_state_source": "<cite the current Databricks doc you verified>"},
      {"pattern": "Parameterised Lakeflow Job", "fit": "Like-for-like orchestration fallback", "simplification_pattern": false}
    ],
    "cascade": ["5 child extractors -> managed connector pipelines"],
    "decision_driver": "Is a managed connector available and its release state verified/approved for this source?"
  },
  "pipeline_insights": [
    {
      "pipeline": "IngestSalesforce",
      "intent": "Land Salesforce objects into the bronze layer nightly",
      "databricks_pattern": "Managed ingestion",
      "recommended_patterns": [
        {"pattern": "<managed CDC ingestion connector>", "fit": "Managed CDC ingestion replaces the copy loop", "simplification_pattern": true, "release_state": "<verify live; do not copy — one of ga|public_preview|private_preview|beta|unknown>", "release_state_source": "<cite the current Databricks doc you verified>"}
      ],
      "conversion_notes": ["Point the connector at the same source objects"],
      "risk_if_ignored": "Bespoke extractor code and its watermark table carry forward"
    }
  ],
  "pipeline_relationships": [
    {
      "from_pipeline": "Orchestrator",
      "to_pipeline": "IngestSalesforce",
      "lineage_edge": {"edge_type": "control", "edge_identity": "<via_task_key from a real control edge>"},
      "relationship_summary": "Orchestrator invokes IngestSalesforce"
    },
    {
      "from_pipeline": "IngestSalesforce",
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

### Rules the validator enforces

- **Foreign keys.** Every `pipeline_insights[].pipeline` and every relationship
  `from_pipeline` / `to_pipeline` must be a real pipeline name in the inventory.
- **One insight per pipeline.** A pipeline may appear in `pipeline_insights` at most once; put
  everything you have to say about it in that one entry.
- **Field types.** Pipeline names are strings. When present, `pattern_name`, `intent`,
  `databricks_pattern` and `risk_if_ignored` (per pipeline) and `relationship_summary`,
  `databricks_pattern` and `risk_if_ignored` (per relationship) must be strings, and
  `conversion_notes` a list of strings.
- **`recommended_patterns`** (per pipeline and system-wide): **1–4** patterns. The validator enforces
  the count and that each has a non-empty `pattern` and `fit` and a boolean `simplification_pattern`;
  it does **not** enforce ordering. Set `simplification_pattern: true` **only** for a distinctive
  capability that collapses a whole legacy pattern (a managed connector, declarative `AUTO CDC`, Auto
  Loader, system tables replacing a home-grown logging tier) — not for a like-for-like port. By
  convention (not validated), order them best-first and list the `simplification_pattern: true` ones
  ahead of like-for-like ports.
- **`release_state` + `release_state_source`** (per recommended pattern): the pattern's *verified*
  Databricks release state and the doc that grounds it. `release_state` is one of `ga`,
  `public_preview`, `private_preview`, `beta`, `unknown`. It is **required** on any pattern with
  `simplification_pattern: true` (a distinctive capability must declare its verified release state);
  optional otherwise. `release_state_source` must be a string whenever it is set, and a non-empty
  citation is **required** when `release_state` is `public_preview` / `private_preview` / `beta`; not
  required for `ga` / `unknown`.
  The state is a factual **disclosure**, not a warning: `flowx-route` surfaces `public_preview` as the
  label "Public Preview (production-ready)" and `private_preview` / `beta` as plain labels, while `ga`
  and `unknown` are silent (`unknown` is treated exactly like `ga`). Verify against the current public
  docs **before** recommending — see the SKILL.md GA-grounding step for the full rule. (There is no
  `doNotSuggest` eligibility gate: a verified Private Preview feature is still eligible to suggest —
  disclose the state and let the customer decide; see the SKILL.md step.)
- **`system_recommendation`** needs a non-empty `headline` and a `recommended_patterns` list;
  `cascade` (non-empty strings) and `decision_driver` are optional.
- **Relationship edges** come in two tiers:
  - `control` — an **annotation** of a proven control edge. `edge_identity` must be the
    `via_task_key` of a real `control_edges` entry whose `source_workflow`/`target_workflow` match
    your `from_pipeline`/`to_pipeline`. Do **not** set `evidence`/`confidence` — the proven edge is
    the evidence.
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
  "from_pipeline": "IngestSalesforce",
  "to_pipeline": "BuildMart",
  "lineage_edge": {"edge_type": "data", "edge_identity": "sales.curated"}
}

// RIGHT — a cross-pipeline data coupling rides the 'inferred' tier
{
  "from_pipeline": "IngestSalesforce",
  "to_pipeline": "BuildMart",
  "lineage_edge": {
    "edge_type": "inferred",
    "edge_identity": "shared table sales.curated",
    "evidence": "IngestSalesforce writes sales.curated; BuildMart reads it — the hand-off is inside notebook code the parser can't see",
    "confidence": "medium"
  }
}
```

## Idempotency & safety

See the `flowx-enrich` SKILL.md "Idempotency & safety" section: a validation failure writes nothing.
