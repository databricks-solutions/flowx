# What fill-agentic checks

Read this from Step 3 of the `flowx-route` skill before you run `fill-agentic`. Each rule below is
enforced; a refused fill writes nothing.

## Contents

- [Which unit](#which-unit)
- [Filling again](#filling-again)
- [Names](#names)
- [The authored pipelines](#the-authored-pipelines)
- [Validation before writing](#validation-before-writing)
- [One at a time per output folder](#one-at-a-time-per-output-folder)

## Which unit

The members (one `--member` per name, taken exactly as given, so a name may hold a comma; or `--members`
comma separated; MCP accepts a list) must exactly match a routed-agentic unit in the
recorded plan: a component, or an accepted grouping's full member list. The plan's `inventory_sha256`,
`source_graphs_sha256` and `agentic_insights_sha256` must still match the current inventory, with nothing
pending. A partial group, a superset, a typo, or a deterministic component is refused. The report must
carry a routing record that matches that plan.

## Filling again

- **Same authored pipelines** (the canonical hash covers every field of every authored pipeline): the
  result is `already_applied: true` with the message "already applied, unchanged", and nothing is
  written. If the report no longer reflects the stored output, or the stored entry was edited by hand,
  it stores the pipelines again, rebuilds, validates and writes the result.
- **Different authored pipelines** replace the stored output for that unit only and add `{"from", "to"}`
  to the entry's `replaced` history (the first fill is not a replacement). The history lives in
  `agentic_conversion.json`, so it survives switching the unit to deterministic and back. An entry edited
  by hand no longer checks out, so filling it again starts a fresh history.

## Names

An authored pipeline name may reuse a member of this unit, but not another name in the same list, a
member of another unit, a pipeline another unit's applied output authored, or a name that shares a
bundle folder with another pipeline once normalised (`Sales Load` and `Sales-Load` would overwrite each
other). A stored output that is no longer applied (its unit routed deterministic since, or joined into a
grouping) holds no name.

## The authored pipelines

- Each needs a non-empty string `name` and a non-empty `tasks` list of task objects, and must not be
  marked `migration_status: "excluded"` (package skips those).
- None may still hold a `PlaceholderActivity` or an `UnsupportedActivity`, or a task whose `type` is not
  an IR task type (for example a misspelt `NotebokActivity`), at the top level or inside a ForEach, If or
  Switch: package would build each of those as a stub.
- Each must carry `"tags": {"source": "adf"}`; a missing or non-`adf` tag fails closed. An empty
  `pipelines` list is refused.

## Validation before writing

The merged report is always validated with the structural bundle invariants (a real
`prepare` then `write_bundle` pass) before it is written, with no bypass. So a duplicate key, a dangling
dependency, a cycle, or a dangling pipeline or run_job reference never lands on disk: all or nothing.

## One at a time per output folder

Route, fill-agentic and a merge into the live report each hold `metadata/.route.lock` while they work,
and refuse, writing nothing, while another holds it (route and fill-agentic return the reason under
`violations`; a merge prints it and exits 1). Run fills one after another, never in parallel.

A lock is left behind when a run is stopped mid-write (the hosted server stops a command after
`FLOWX_MCP_TIMEOUT` seconds). The next command takes it over once the process that held it has exited; a
lock written on another machine, or any lock on Windows, once it is older than `FLOWX_MCP_TIMEOUT` (1800
seconds by default). Then run route, and the refused fill or merge again. Locally you can also delete the
lock first.
