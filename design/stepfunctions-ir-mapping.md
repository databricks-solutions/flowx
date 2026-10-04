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

- **Job schedule.** Step Functions state machines carry no schedule in their ASL; schedules live in
  EventBridge Scheduler or EventBridge Rules as separate AWS resources. Adding a second input (e.g.
  `--stepfunctions-schedule-path`) to read those exports was evaluated and deferred — the schedule is
  the shallowest part of the migration and the export friction is real. The converted Job has no
  schedule set; users configure it manually. This is documented in the skill guides and the PRD.
- **`Retry` / `Catch`.** Done. `Retry` → `max_retries` + `min_retry_interval_millis` (highest
  `MaxAttempts` entry governs; `BackoffRate` noted). `Catch` → catch handler translated as a
  top-level sibling task with `Dependency(outcome="ALL_FAILED")`. Catch handlers that rejoin the
  main path are translated but the rejoin state does not automatically pick up the handler's exit dep.
- **JSONPath I/O processing.** `Parameters` → task parameters, `ResultPath` → task values, and
  machine-input references → declared job parameters are done; see `stepfunctions-jsonpath.md`.
  `InputPath` / `OutputPath` / `ResultSelector` and nested result-path indexing are still deferred.
- **Choice predicates.** Compound (`And`/`Or`/`Not`), `*Path`, and `Is*` rules keep the raw rule JSON
  in the condition's `left` operand with an `expr` operator; only the branch structure is guaranteed.
- **Glue / Lambda bodies.** Agentic placeholders for now (Open Question 2, Option 1). Deterministic
  GlueContext/DynamicFrame → Spark rewrites are a later increment. A Task that starts a whole Glue
  *workflow* (`glue:startWorkflowRun`) is the exception — it becomes a `RunJobActivity` targeting the
  Glue source's converted job; see `aws-composition.md`.
- **Glue Workflows.** A separate `glue` source (PRD Open Question 1 leans to two sources); see
  `glue-ir-mapping.md`.

## Shared helpers added

`flowx/sources/inventory.py` holds the source-neutral `build_inventory_dict` / `write_profile_csv` /
`classify_tasks`. The Airflow discover phase predates it and carries an equivalent inline copy that
should migrate here.
