# After package: change the conversion

Read this from the `flowx-route` skill when the user wants to change a decision, an answer or an agent output after package has run.

Package prunes `.work/` once it has built the bundle, so route and fill-agentic refuse straight
after a package and say that package pruned `.work/`. Everything they rebuild from is kept in
`metadata/`: the decisions, accepted groupings and conversation in `conversion_plan.json`, every
stored fill and its `replaced` history in `agentic_conversion.json`, and modify's answers in
`configuration.json`. To change a decision, an answer or an agent output after a package, run:

1. **convert**, with the same source and options. It writes `.work/` again.
2. **route**, after editing the plan if a decision or an answer is what changes. Route takes the fresh
   report as its new baseline and re-applies the plan and the stored outputs. Fills of convert's own
   gaps come back when convert wrote the same report as before; otherwise route keeps them, says how
   many it skipped, and they come back if convert writes that report again. Merge again to fill those
   gaps in the new report.
3. **fill-agentic**, only for a unit whose output changes. Revise the fill when a changed answer
   changes the pattern or its shape; nothing flags that for you. New pipelines replace the stored ones
   and add an entry to the unit's `replaced` history.
4. **modify**, if it was used, with the same answers (`metadata/configuration.json` lists them).
5. **package**.

Package overwrites the bundle files it generates, so hand edits to them are lost, and moves any
`resources/*.yml` it does not write again (one added by hand included) into
`metadata/archive/<UTC time>/`. To keep a change to an agentic unit's resource, put it in the authored
pipeline's resource `definition` and run fill-agentic. With `--keep-intermediates` (MCP
`keep_intermediates`) package leaves `.work/` in place, so you can start at route or fill-agentic. A
re-package after the pipeline count or names change moves the earlier bundle folders it recorded in
`metadata/packaged_bundles.json` into `metadata/archive/<UTC time>/` before it writes the new ones, so
a pipeline an agentic unit replaced under a new name no longer sits beside the new one. A re-discover
moves `metadata/` and `.work/`, the plan and the fills with them, into `metadata/archive/<UTC time>/`.
A deploy uploads `metadata/archive/` with the bundle unless it is git-ignored; nothing in it is a job
or pipeline the bundle deploys.
