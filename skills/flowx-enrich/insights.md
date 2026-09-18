# The agentic insights shape

This is the reference for the `insights` JSON you author before calling `enrich`. The `flowx-enrich`
SKILL.md covers the workflow (no-LLM contract, the three authoring steps, how to run `enrich`, and
when a deterministic-only pass skips it); this file covers **what to write** and the rules the
validator enforces.

Author the insights when a human reader would benefit from a migration narrative — which pipelines
collapse onto a managed capability, how the factory hangs together, what the risky couplings are.
The `flowx-route` step reads this block to present the agentic conversion option per component.

## The insights shape

You author only these four fields (the library injects `schema_version` and an `inventory_sha256`
fingerprint that binds your insights to the exact inventory they describe):

```json
{
  "overview": "One short factory-wide narrative — what this collection of pipelines is.",
  "system_recommendation": {
    "headline": "The one decision a migrator must make before any per-pipeline work",
    "recommended_patterns": [
      {"pattern": "Lakeflow Connect SQL Server connector", "fit": "Replaces the child extractor family", "simplification_pattern": true},
      {"pattern": "Parameterised Lakeflow Job", "fit": "Like-for-like orchestration fallback", "simplification_pattern": false}
    ],
    "cascade": ["5 child extractors -> managed connector pipelines"],
    "decision_driver": "Is the Lakeflow Connect connector GA/approved for this source?"
  },
  "pipeline_insights": [
    {
      "pipeline": "IngestSalesforce",
      "intent": "Land Salesforce objects into the bronze layer nightly",
      "databricks_pattern": "Managed ingestion",
      "recommended_patterns": [
        {"pattern": "Lakeflow Connect", "fit": "Managed CDC ingestion replaces the copy loop", "simplification_pattern": true}
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
- **`recommended_patterns`** (per pipeline and system-wide): **1–4** patterns. The validator enforces
  the count and that each has a non-empty `pattern` and `fit` and a boolean `simplification_pattern`;
  it does **not** enforce ordering. Set `simplification_pattern: true` **only** for a distinctive
  capability that collapses a whole legacy pattern (a managed connector, declarative `AUTO CDC`, Auto
  Loader, system tables replacing a home-grown logging tier) — not for a like-for-like port. By
  convention (not validated), order them best-first and list the `simplification_pattern: true` ones
  ahead of like-for-like ports.
- **`system_recommendation`** needs a non-empty `headline` and a `recommended_patterns` list;
  `cascade` (non-empty strings) and `decision_driver` are optional.
- **Relationship edges** come in two tiers:
  - `control` — an **annotation** of a proven control edge. `edge_identity` must be the
    `via_task_key` of a real `control_edges` entry whose `source_workflow`/`target_workflow` match
    your `from_pipeline`/`to_pipeline`. Do **not** set `evidence`/`confidence` — the proven edge is
    the evidence.
  - `inferred` — a coupling the deterministic layer never found (data flow inside notebook code, an
    external trigger, a shared table the parser didn't resolve). There is nothing to resolve
    against, so `edge_identity` is your descriptor of the coupling and you **must** supply a non-empty
    `evidence` string and a `confidence` of `high` / `medium` / `low`.
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

`enrich` is atomic and idempotent: it replaces the whole `insights` block (never stacks), recomputes
the fingerprint from the deterministic inventory, and leaves every existing inventory key
byte-identical. Re-running with the same insights rewrites the same bytes; re-running with different
insights replaces the block. A validation failure writes nothing.
