# Step Functions → flowx IR mapping

Design notes for the `stepfunctions` source. Captures the one real design decision — how a flat
Amazon States Language (ASL) graph maps onto flowx's IR — and what the first increment defers.

## The impedance mismatch

An ASL state machine is a **flat graph of guarded transitions**: each state names its successor via
`Next` (or, for `Choice`, a successor per rule plus `Default`). The flowx IR is **not** flat — it
mixes a task DAG (`depends_on` edges between leaf tasks) with **nested control-flow containers**
(`IfConditionActivity.if_true_activities`, `ForEachActivity.inner_activities`). The translator has to
rebuild the nested structure from the flat graph.

## Approach: structural reconstruction

`translate.py` walks each `Next` chain, emitting one IR activity per state and wiring `depends_on`
from the transition it arrived on. At a `Choice`:

1. Collect the branch targets (each rule's `Next`, plus `Default`).
2. Find the **join** — the nearest state reachable from every branch (`_find_join`, an approximate
   post-dominator: the common reachable state with the smallest worst-case hop count from the
   targets).
3. Translate each branch as a sub-region that stops at the join, nesting the result inside an
   `IfConditionActivity` (cascaded for multiple rules — rule *n*'s else-branch holds the condition for
   rule *n+1*, and the final else holds the `Default` branch).
4. Continue translating from the join as the condition's successor.

`Map` → `ForEachActivity` (its item processor's states become `inner_activities`). `Parallel` →
each branch's states translated concurrently off the same upstream, with the successor depending on
every branch tail. Cycles and dangling transitions are detected and routed to an agentic placeholder
rather than mistranslated.

This reconstruction is correct for **reducible** graphs (the shape hand-authored and console-built
state machines almost always have). Genuinely irreducible graphs degrade to a placeholder.

## Deferred (next increments)

- **EventBridge schedules.** Schedules are EventBridge rules that live *outside* the ASL. Populating
  `Pipeline.schedule` needs the rule export as a second input. (PRD P0.)
- **`Retry` / `Catch`.** Not yet mapped to `max_retries` and `Dependency(outcome="Failed")` edges.
- **JSONPath I/O processing.** `InputPath` / `Parameters` / `ResultPath` / `ResultSelector` /
  `OutputPath` are not rewritten into task parameters and task values. (PRD P0 — this is the analogue
  of ADF's `@{...}` expression rewriting and deserves its own pass.)
- **Choice predicates.** Compound (`And`/`Or`/`Not`), `*Path`, and `Is*` rules keep the raw rule JSON
  in the condition's `left` operand with an `expr` operator; only the branch structure is guaranteed.
- **Glue / Lambda bodies.** Agentic placeholders for now (Open Question 2, Option 1). Deterministic
  GlueContext/DynamicFrame → Spark rewrites are a later increment.
- **Glue Workflows.** A separate `glue` source (PRD Open Question 1 leans to two sources); see
  `glue-ir-mapping.md`.

## Shared helpers added

`flowx/sources/inventory.py` holds the source-neutral `build_inventory_dict` / `write_profile_csv` /
`classify_tasks`. The Airflow discover phase predates it and carries an equivalent inline copy that
should migrate here.
