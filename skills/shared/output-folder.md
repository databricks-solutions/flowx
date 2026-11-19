# The shared output folder

All flowx phases write into one `<output_dir>` (default `./flowx_output`): the bundle at the top level,
kept files under `metadata/`, and transient files under `.work/`.

## Contents

- [metadata/: kept files](#metadata-kept-files)
- [.work/: transient files](#work-transient-files)
- [The bundle](#the-bundle)
- [Re-runs and the archive](#re-runs-and-the-archive)

## metadata/: kept files

| File | Written by | What it holds |
|---|---|---|
| `inventory.json` | discover (enrich rebuilds it) | Every task classified deterministic, agentic or unsupported |
| `source_graphs.json` | discover, ADF only | The saved source graphs (activities, lineage, detected motifs), versioned and hashed; the inventory records its hash as `source_graphs_sha256` |
| `profile_report.csv` | discover | Per-pipeline complexity: counts and a T-shirt size |
| `<pipeline>.arm.json` | discover, ADF only | The verbatim source of each pipeline |
| `agentic_insights.json` | enrich | The validated insights |
| `conversion_plan.json`, `routing_review.html` | route | The routing decisions and the review page |
| `agentic_conversion.json` | fill-agentic, merge_agentic | The agent's conversion output |
| `configuration.json` | modify | The configuration answers |
| `route_audit.json`, `packaged_bundles.json` | package | What each routing unit became; the bundle folders package wrote |
| `tco/cost_comparison.csv`, `tco/cost_comparison.md` | profile | The ADF cost estimate |

## .work/: transient files

| File | Written by | What it holds |
|---|---|---|
| `translation_report.json` | convert, then route | The report with the IR for every task |
| `<pipeline>.json` | convert | Per-pipeline IR |
| `gaps.json` | convert | Unmapped source constructs |
| `translation_report.stamped.json` | modify | The report with the configuration answers applied |
| `route_baseline/` | route | Convert's report as route first found it, never rewritten |

Package deletes `.work/` after a successful build unless `--keep-intermediates` is set.

## The bundle

Package writes `databricks.yml`, `resources/*.yml`, `src/notebooks/`, `src/setup/`, `SETUP.md` (one per
bundle) and a top-level `DEPLOY.md`.

## Re-runs and the archive

Discover starts each run afresh: over a folder that already holds a run, it moves that run's
`metadata/`, `.work/` and bundle into `metadata/archive/<UTC time>/` with an `archive.json` of what moved,
from where and when, and says so. Package moves bundle files it no longer writes into the same archive.
`metadata/archive/` sits inside the bundle folder, so `databricks bundle deploy` uploads it unless it is
git-ignored; nothing in it is a job or pipeline the bundle deploys.
