# flowx Changelog

All notable changes to flowx will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

## [0.2.0]

### Added
- Enrich step: the agent adds insights on top of the discover inventory, and asks a few open
  questions about the estate that the code can't answer
- Route step: decides deterministic or agentic conversion per connected component, records the
  decision in the conversion plan, and renders a routing review page; before suggesting an ingestion
  pattern it checks it against the current platform limits, read live from the docs, as enrich does for
  every pattern it recommends
- Fill-agentic step: applies the agent's conversion output to routed-agentic units
- Agentic component: for an activity the typed engine cannot express, the agent can author Databricks
  tasks, files, pipeline resources and job environments, which package writes into the bundle while
  flowx keeps the task name, dependencies and run policy

### Changed
- Re-running discover over an output folder that already holds a run now moves that run's outputs
  into `metadata/archive/<UTC time>/`, with an `archive.json` of what moved, instead of deleting them;
  re-running package does the same for any resource file and bundle folder it no longer writes, keeping
  the list of bundle folders it wrote in `metadata/packaged_bundles.json`

## [0.1.0]

### Added
- Initial release of the flowx library
