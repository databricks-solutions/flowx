# Authoring agentic insights (optional, after discover)

The deterministic discover pass records what each source workflow **is**; it cannot record what
to **do** about it. That judgment — a factory-wide architectural recommendation, each pipeline's
intent and recommended Databricks patterns, and how pipelines couple — is authored by **you, the
agent**, and merged back into `inventory.json` under a single additive `insights` key.

**There is no LLM inside flowx.** You author the insights JSON; the library only *validates and
merges* it (the same author-then-validate-merge contract the agentic gap-resolution path uses).
That keeps the deterministic inventory trustworthy and every insight accountable — foreign keys
must point at real pipelines, and every cross-pipeline edge is either an annotation of a proven
lineage edge or an explicitly-flagged inference with cited evidence.

Insights are **optional** and change nothing about conversion. They are descriptive data that a
later routing step may consume; on their own they add zero routing/IR/conversion decisions.

## When to author them

After `discover` has written `metadata/inventory.json`, and before (or instead of) convert, when a
human reader would benefit from a migration narrative: which pipelines collapse onto a managed
capability, how the factory hangs together, what the risky couplings are.

## How to author (three steps)

1. **Read the deterministic inventory.** Load `<output_dir>/metadata/inventory.json`. Note every
   pipeline `name` (these are the only valid foreign keys), and each pipeline's `lineage` block —
   in particular `lineage.control_edges`, each `{source_workflow, target_workflow, via_task_key}`.
   A deterministic **control** relationship you annotate must match one of these exactly.
2. **Read the source artifacts** you need to form judgment — the per-pipeline `raw` payloads in the
   inventory, the ADF `metadata/<pipeline>.arm.json` provenance, or the DAG source — enough to state
   each pipeline's *intent* and the Databricks patterns that fit. Ground every recommended pattern
   in a **real, publicly-documented** Databricks capability; never invent a product name.
3. **Author the insights JSON, then call `enrich`.** The library validates it against the inventory
   and, only when clean, merges it in atomically. On any violation the inventory is left untouched
   and you get the full list of problems to fix in one pass.

### Run enrich

- **MCP tool:** `flowx("enrich", {"output_dir": "<dir>", "insights": { ... }})` (inline object), or
  pass `"insights_path": "<file>"` instead. `ok` reflects validation; `result.violations` lists any
  problems.
- **venv CLI:**

  ```bash
  export PYTHONPATH="<plugin_dir>/src"
  PY="$(cat <plugin_dir>/.migration-venv)"
  "$PY" -m flowx.adapter enrich --output-dir <dir> --insights-path insights.json
  ```

  Exit code 0 means merged; exit code 1 prints the violations JSON and leaves the inventory untouched.

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
- **`recommended_patterns`** (per pipeline and system-wide): a ranked list of **1–4**, best-first.
  Each needs a non-empty `pattern` and `fit`, and a boolean `simplification_pattern`. Set
  `simplification_pattern: true` **only** for a distinctive capability that collapses a whole legacy
  pattern (a managed connector, declarative `AUTO CDC`, Auto Loader, system tables replacing a
  home-grown logging tier) — not for a like-for-like port. Rank the `true` patterns first.
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

## Idempotency & safety

`enrich` is atomic and idempotent: it replaces the whole `insights` block (never stacks), recomputes
the fingerprint from the deterministic inventory, and leaves every existing inventory key
byte-identical. Re-running with the same insights rewrites the same bytes; re-running with different
insights replaces the block. A validation failure writes nothing.
