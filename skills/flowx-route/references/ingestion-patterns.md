# Qualify the target pattern before you suggest one

Read this from Step 1 of the `flowx-route` skill, before you suggest a pattern for a component that ingests from a database.

A component that ingests from a database can land in one of four patterns. Tell which components do
from the `source:` line enrich wrote in a pipeline's `conversion_notes`, or else from the signals in
"Where the facts sit" in the `flowx-enrich` skill; the connector decides which limits apply. The library
doesn't run these checks; you do, from the facts and the limits, and you record them as free text in the plan's
`conversation`.

| Pattern | What it is | Facts it needs | Decision |
|---|---|---|---|
| **D1** deterministic 1:1 | The engine's port (For each + JDBC notebooks), same behaviour | None | `deterministic` |
| **M1** Lakeflow Connect managed | Gateway or integrated CDC ingestion pipelines generated from the worklist, sharded by database or schema | Scale, repeat, fan-in, change, bar, policy, shaping | `agentic` |
| **M2** metadata-driven worklist | A job over the worklist that skips unchanged sources, reads the change feed over JDBC, MERGEs (fan-in allowed) and reseeds when retention lapses | Scale, change, fan-in, bar | `agentic` |
| **A1** agentic re-architecture | Bespoke, for example several workspaces or metastores, an accelerator or partner tool, or mixed patterns | Whatever conflicts remain | `agentic` |

The facts are the ones `flowx-enrich` asks about. **Follow-up questions:** carry each question and
answer from the insights' `conversion_notes` into `conversation`, then ask at most three more, only on
facts still open for the patterns still in play. These are usually platform-side: whether a feature is
enabled on the workspace, which compute is allowed, how many tables the metastore already holds. Never
re-ask a fact already answered, a "don't know" included. End each answer with the fact on one line,
`fact: value (source: code | data | user | assumed)`.

**Limits** are read live, as `flowx-enrich` does; flowx keeps no copy of them, because they change.
Before you suggest a pattern, open the current Databricks docs for every limit its checks below rest
on, and record each in `conversation` as `limit: <name> = <value> (source: <page>, read <date>)`.
Reuse a limit enrich recorded in `conversion_notes` only after confirming it on its page. For SQL
Server they are on the connector's limits and integrated CDC pages, the resource limits page, the Unity
Catalog metastore page, the For each and pipeline task pages and the serverless Private Link page; for
another connector, read its limits page the same way. The docs don't state three claims the checks
use, marked *Assumption:* below: say so beside the pattern, and confirm the claim with the account
team before ruling a pattern out on it. When the docs can't be reached, show the pattern as
*not qualified: limits not checked*; never use a remembered number.

Let **N** be the total tables and **L** the recommended tables per pipeline for the chosen mode
(gateway or integrated CDC). When a pipeline can't hold two tables with the same name, even from
different schemas or databases, and names repeat across databases, the pipeline count is
**P** = Σ over databases of ⌈tables in the database / L⌉; otherwise P = ⌈N / L⌉. In gateway mode, let
**G** be the gateways those P pipelines need: P when each ingestion pipeline needs its own gateway.
When the pipelines run from a pipeline task inside For each, let **F** be how many iterations that task
runs at once. Read all three rules live like any limit; if the same-name page now lets a pipeline hold
more than one table of a name, size P from what it allows and show the sum in `conversation`.

| Check | Passes when | Limits | When it fails |
|---|---|---|---|
| C1 fan-in | Fan-in isn't required, or only a few sources feed each logical table (a UNION view over a handful is fine) | *Assumption:* each destination table is written by one pipeline only | M1 out |
| C2 pipelines | P ≤ saved pipelines per workspace; in gateway mode count P + G (the pipelines and their gateways). Warn when P > concurrent updates; in gateway mode, when G running gateways plus the ingestion updates exceed it | Saved pipelines per workspace; concurrent pipeline updates per workspace; *Assumption:* a running gateway holds one concurrent update | M1 strained or out |
| C3 tables | N plus the tables the metastore already holds fits per metastore, and tables per schema and schemas per catalog fit | Tables per metastore; tables per schema; schemas per catalog | M1 out, or a quota and a multi-catalog plan |
| C4 history | Current state is enough, or every table has a change feed that carries every change, on a source tier that supports it | Source docs | M1 is limited to SCD1 without CDC and M2 gives state per run only, so SCD2 required without CDC tips to A1 |
| C5 mode | Gateway: G always-on classic gateways are acceptable. Integrated CDC: the workspace has it enabled | One gateway per ingestion pipeline; the compute a gateway runs on; integrated CDC enabled on the workspace | Pick the other mode and re-check; if neither mode passes, M1 out, so D1 or A1 |
| C6 run time | ⌈P / min(slots, chosen concurrency, F)⌉ × the minimum update run fits the allowed run time (leave F out when no For each pipeline task runs them), where slots is concurrent updates, or concurrent updates − G in gateway mode. If slots is zero or less (G at or above concurrent updates in gateway mode), M1 is out in gateway mode; re-check with integrated CDC | Concurrent pipeline updates per workspace; *Assumption:* a running gateway holds one concurrent update; the minimum run of each integrated CDC extraction; concurrent iterations of a pipeline task inside For each | M1 out |
| C7 cost floor | P × updates per day × the minimum update run × the cluster rate, plus G always-on classic gateway clusters in gateway mode, fits the budget or today's spend. Compare M2, which pays mostly for sources that changed | The minimum run of each integrated CDC extraction; the compute a gateway runs on | M1 not preferred |
| C8 retention | Change-feed retention is longer than the longest gap between successful runs, with margin | Source docs | Disclose on whichever pattern is suggested: a reseed risk, and history is lost on a full refresh |
| C9 network | Classic compute has a network path to the source; serverless has NCC private endpoints within the region's limit | NCC private endpoints per region | Choose the compute |
| C10 connections | The source servers fit the connections per metastore | Connections per metastore | M1 needs a quota, or M2 |
| C11 shaping | No expression, join or row filter has to happen at landing | None | M1 isn't 1:1; M2 or D1 |
| C12 release | The chosen mode's release state, checked live as `flowx-enrich` requires, is disclosed and the customer accepts it | *Assumption:* integrated CDC's release state, which its page doesn't label; state what the page says | M1 in the other mode only, re-checked; if that mode fails too, D1 or A1 |
| C13 jobs (M2, D1) | Tasks and parent tasks running at once, and saved jobs, fit; For each inputs fit through task values, so batch sources per task | Tasks running at once; Run job and For each parent tasks running at once; saved jobs per workspace; For each inputs passed through task values | Batch, or A1 |

**Suggestion order:**

1. **M1** when C1 to C7 and C9 to C12 pass. It's the managed option. A C8 failure doesn't rule it out; it's a
   reseed and lost-history risk you disclose on whichever pattern is suggested.
2. **M2** when M1 fails on C1, C2, C3, C6, C7, C10 or C11, and C13 passes.
3. **D1** when the customer wants like-for-like, or no database ingestion is in scope.
4. **A1** when no pattern passes, or the requirements conflict.

Beside each pattern you rule out, show the failing check and its source: the limit's page and the
date you read it, or the fact and where it came from. A pattern whose facts are still unknown
shows as *not qualified: needs <fact>*; present the remaining candidates with their assumptions marked
rather than asking again. The customer still decides, and the fill (Step 3) builds the pattern they
chose. On a source where route records deterministic decisions only, D1 is the decision and the facts
inform the per-gap fills.
