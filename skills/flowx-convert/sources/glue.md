# Convert — AWS Glue Workflows

Source guide for `--source glue`. Translate exported workflow graphs into the shared flowx Pipeline
IR. See the parent `SKILL.md` for the shared report shape and how to run a phase.

## How it works

The convert phase parses each workflow and translates it into the source-neutral Pipeline IR, then
serialises it to `.work/translation_report.json` — the same report the other sources emit, so the
package phase consumes it unchanged. Placeholders (job and crawler nodes) are also written to
`.work/gaps.json` for LLM-assisted resolution.

A workflow becomes one Lakeflow Job. Jobs and crawlers become tasks; triggers become the schedule
and the dependency edges between those tasks.

## Node → IR mapping

| Glue node | Strategy | IR activity |
|---|---|---|
| ETL / Python-shell job | Agentic | `PlaceholderActivity` (`GlueJob`) |
| Crawler | Agentic | `PlaceholderActivity` (`GlueCrawler`) |
| Scheduled trigger | Deterministic | Job schedule (cron → Quartz) |
| On-demand trigger | Deterministic | manual root (no edge) |
| Conditional trigger | Deterministic | `depends_on` edges with a `run_if` outcome |

Predicate → `run_if`: all-`SUCCEEDED` + `AND` → default `ALL_SUCCESS`; `SUCCEEDED` + `ANY` →
`AT_LEAST_ONE_SUCCESS`; all-`FAILED` → `ALL_FAILED` / `AT_LEAST_ONE_FAILED`; anything mixed or
`TIMEOUT`/`STOPPED`/`CANCELLED` → `ALL_DONE` (Databricks gates only on success or failure).

## Run it

```bash
"$PY" -m flowx.adapter convert --source glue \
  --glue-source-path <path_to_json_or_dir> \
  --output-dir <output_dir> \
  [--pipeline <workflow_name>]
```

Then run `flowx-package` against the same `<output_dir>`.

## Deferred this increment

- **Job and crawler bodies** — surfaced as agentic gaps; deterministic GlueContext/DynamicFrame →
  Spark rewrites and crawler → Auto Loader translation are a later increment.
- **`EVENT` triggers** — EventBridge-driven starts are not mapped to a Databricks trigger.
- **Multi-trigger fan-in** — when two conditional triggers target one node with different predicates,
  the edges are merged and the first `run_if` wins; this is noted in `not_translatable`.
- **Non-cron schedules** — only `cron(...)` schedules convert; anything else is left unset with a note.

See `design/glue-ir-mapping.md` for the rationale.
