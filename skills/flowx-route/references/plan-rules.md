# The plan's rules and what applying it does

Read this from Step 2 of the `flowx-route` skill: which fields you may edit, what the validator refuses,
and how route rebuilds the report once no decision is pending.

## Contents

- [What you may edit](#what-you-may-edit)
- [Passing a plan](#passing-a-plan)
- [What the validator refuses](#what-the-validator-refuses)
- [What applying does](#what-applying-does)
- [The routing record](#the-routing-record)

## What you may edit

Edit only these fields in `metadata/conversion_plan.json`. Everything else is library-owned and
recomputed on every route, so edits to it are ignored; an unknown key is refused.

- each component's `decision`: `"deterministic"` or `"agentic"` (the customer's choice), and an optional
  `rationale`;
- to accept a grouping, its `accepted: true`, with each of its components decided `"agentic"`;
- `conversation`.

Route checks the edited file with the same rules as a passed plan, so a misspelt key or a value such as
`accepted: "true"` is refused and the file is left as you wrote it.

## Passing a plan

Instead of editing the file you can pass a plan: `--plan-path <file>`, or MCP `plan` inline or
`plan_path`. It can be the decisions alone
(`{"components": [{component_id, members, decision}], "suggested_groupings": [{grouping_id, accepted}], "conversation": [...]}`)
or an edited copy of the recorded plan. This is the hosted path: edit the plan route returned and send
it back as `plan`.

## What the validator refuses

All violations come back at once, and nothing is written on failure.

- `decision` must be `"deterministic"`, `"agentic"`, or `null` while pending; `rationale`, when given, is
  a non-empty string.
- Every component is listed exactly once, and its `members` must exactly match one computed connected
  component; `component_id` must match too.
- An accepted grouping has no component decided `"deterministic"`. Agentic decisions and groupings are
  ADF only; an inventory that records no `source` is refused too (run discover again).
- A grouping entry names a current suggestion by its `grouping_id`, with the same `components` and
  `members` when it lists them. Ids derive from members, so after enrich changes the suggestions, an
  edited copy of an older plan can name a grouping that no longer exists: route refuses it and says to
  run route without a plan to see the current ones.
- `conversation` entries are `{question, answer}` with non-empty strings.

## What applying does

Route rebuilds `.work/translation_report.json` and `gaps.json` from convert's baseline, the stored fills
of convert's own gaps, the plan, and the agent's outputs in `metadata/agentic_conversion.json`. Each
routing unit (a component, or an accepted grouping) becomes:

- **deterministic**: the baseline pipelines stay (outcome `deterministic`).
- **agentic with a stored output** whose members match: the output's pipelines replace the members, and
  their routed gaps are dropped (outcome `agentic-applied`). So are convert's untagged gaps for the
  activities the unit owns: its tasks, the activities a motif collapsed, and activities only the
  inventory lists for its pipelines. Such a gap is kept only when a pipeline that stays deterministic
  also holds that name.
- **agentic without one**: every task becomes a `PlaceholderActivity` with one pipeline-tagged gap, and
  the deterministic translation is kept as its `raw_definition` (outcome `agentic-not-viable`; package
  refuses until it is filled).

A stored fill of convert's own gap also takes that gap, and the gaps of anything nested in the filled
task, out of `gaps.json`, so the list shows only what is left. Convert's gaps carry no pipeline name, so
a gap is kept when another pipeline outside the routed units still has that name open.

## The routing record

When nothing is routed agentic and no gap fill is stored, route writes convert's report and gaps back
byte for byte, with no record: the non-breaking guarantee. Otherwise it stamps a routing record onto the
report (`_routing_record`): the plan's hash, the baseline report and gaps hashes, the hash of the gap
fills applied, and one entry per unit with its `members`, `decision`, `outcome`, `output_sha256` (set
only while its stored output is applied) and `fingerprint` (a grouping also lists its `components`).

The plan is freely editable: change it and route again as often as needed. The same plan gives the same
bytes, and a changed unit is re-derived.
