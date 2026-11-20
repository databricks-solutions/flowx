# Qualify the target pattern before you suggest one

Read this from Step 1 of the `flowx-route` skill, before you suggest a target pattern for a component,
whatever it does: ingest from a database, files, an application or an API, or transform data.

The method is the same for every source and every component. The library doesn't run it; you do, from
the facts and the current documentation, and you record it as free text in the plan's `conversation`.
This file gives the method only. It names no source system, product option, limit or size, because
those change and differ by estate: read them from the official Databricks documentation each time.

## 1. Gather the facts about the source and the target

Start from what `flowx-enrich` recorded in each pipeline's `conversion_notes`: the `source:` line, the
questions and answers, and each `fact:` and `limit:` line. Carry each question and answer into
`conversation`.

**Follow-up questions:** ask, in one turn, only about facts still open for the options still in play
that the code and data can't show, and only when a plausible answer would rule an option in or out.
These are often platform-side: what is enabled on the workspace, which compute and network are allowed,
what the workspace and metastore already hold. Never re-ask a fact already answered, a "don't know"
included. End each answer with the fact on one line, `fact: value (source: code | data | user | assumed)`.

## 2. Find every current option in the official documentation

Don't take the list of options from memory, from an earlier migration, or from this skill. For the
component's source or kind, read the current official Databricks documentation and list every option it
offers, including the ways each option can run (for ingestion, the
[ingestion overview](https://docs.databricks.com/aws/en/ingestion/overview) is a starting point). Record
each page you rely on in `conversation` with the date you read it. Check each option's release state
live, as `flowx-enrich` requires, and disclose it.

Then add what flowx itself can produce. This part describes flowx's code, so it is exact:

- **The deterministic port:** convert's 1:1 translation, the decision `deterministic`.
- **Convert's options on a deterministic decision:** `modify` runs after route, and some of its options
  emit managed targets deterministically: `use_lakeflow_connectors=lakeflow_connect` turns an eligible
  database Copy into a Lakeflow Connect pipeline, and `metadata_driven_consolidate=consolidate` turns a
  metadata-driven loop into one, for the sources the code supports.
  [`../../shared/translation-options.md`](../../shared/translation-options.md) says which Copies and
  loops each takes. A target one of these options produces is a `deterministic` decision plus that answer.
- **The agentic route:** decide `agentic` only when the target the customer chooses is one the engine
  and its options can't produce. The fill (Step 3) then authors it. A design no single product provides
  is an option too; state what it assumes the source offers and why the facts call for it.

## 3. Check each option against the current limits

**Limits** are read live, as `flowx-enrich` does; flowx keeps no copy of them, because they change.
Before you suggest an option, open the current docs for every limit and requirement it rests on: its own
requirements and limitations pages, the workspace and resource limits pages, and the pages of what it
runs on. Record each in `conversation` as `limit: <name> = <value> (source: <page>, read <date>)`.
Reuse a limit enrich recorded in `conversion_notes` only after confirming it on its page. When a count
decides the check, show the arithmetic beside the facts it uses. When the docs can't be reached, show the
option as *not qualified: limits not checked*; never use a remembered number.

## 4. Mark what the docs don't settle

When a check rests on a claim the docs don't state, mark it *Assumption:* beside the option, and confirm
it with the account team before ruling an option out on it. When two pages disagree, cite both and say
so; don't rule an option out on either alone.

## 5. Show each ruled-out option, and leave the choice to the customer

Beside each option you rule out, show the failing check and its source: the limit's page and the date
you read it, or the fact and where it came from. An option whose facts are still unknown shows as
*not qualified: needs <fact>*; present the remaining options with their assumptions marked rather than
asking again. Present every option that passes with what it costs, assumes and changes for the customer.
When you state a preference, give its reason from the facts and the pages you read. Never pick the
pattern on the customer's behalf: the customer decides, and the fill (Step 3) builds the one they chose.
On a source where route records deterministic decisions only, the decision is `deterministic` and the
facts inform the port, convert's options and the per-gap fills.
