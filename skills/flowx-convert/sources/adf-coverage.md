# ADF coverage: what flowx does with each part of a factory

Read this from the `flowx-convert` skill with `sources/adf.md`. It lists each area of an Azure Data
Factory export, what flowx produces for it, and what you and the user still have to do. It is the ADF
counterpart of `../../shared/airflow-coverage.md`.

**Anything this file and the two tables it points to don't list is unsupported: report it as
unsupported and never guess a translation.**

## Contents

- [Activities and expressions](#activities-and-expressions)
- [Triggers](#triggers)
- [Dependencies between activities](#dependencies-between-activities)
- [Parameters and global parameters](#parameters-and-global-parameters)
- [Linked services, secrets and Key Vault](#linked-services-secrets-and-key-vault)
- [Integration runtimes and on-premises sources](#integration-runtimes-and-on-premises-sources)
- [Exports](#exports)
- [Patterns across activities](#patterns-across-activities)

## Activities and expressions

- Each activity type has one strategy: deterministic, agentic or unsupported. The full list is
  [`../references/activity-mapping.md`](../references/activity-mapping.md). A type in neither the
  deterministic nor the agentic list is unsupported.
- Expression functions and how each translates are in
  [`../references/expression-functions.md`](../references/expression-functions.md). A function that
  can't be translated is left as an agentic gap.

## Triggers

Convert takes the first trigger that references a pipeline; a Databricks job has one schedule, so any
other triggers on the same pipeline are for the user to wire.

| ADF trigger | What flowx writes | What to tell the user |
|---|---|---|
| Schedule trigger | The job's `schedule` (Quartz cron and time zone); a day, week or month interval above 1 becomes a periodic trigger | Nothing, beyond checking the time zone |
| Storage events trigger | A file arrival trigger on the trigger's scope | Check that the location is reachable from the workspace |
| Tumbling window trigger | A periodic schedule, marked as approximated | Review the window boundaries; backfill and window dependencies are not carried over |
| Custom events trigger | No trigger; a manual setup note in `SETUP.md` | Wire it by hand |

Parameter values a trigger passes to its pipeline become the job's default parameter values.

## Dependencies between activities

Each dependency condition becomes the task's run condition: Succeeded, Failed, Completed (run whatever
the upstream result) and Skipped map one to one. An edge with both Succeeded and Failed becomes
Completed; an edge that includes Skipped becomes Skipped; Failed with others becomes Failed.

## Parameters and global parameters

Pipeline parameters become job parameters. For factory global parameters, convert asks one input,
`global_parameter_resolution`: `literal` (the default) writes each value in where it is used;
`bundle_variable` turns each reference into `${var.<name>}` and declares a bundle variable with the
factory value as its default, so it can change at deploy time.

## Linked services, secrets and Key Vault

Credentials that copy and web activities need become secret instructions, and package writes a setup
notebook that creates the secret scopes and secrets (the user fills in the real values). A Key Vault
secret referenced by a web activity is carried through as a Databricks secret scope and key. Package also
writes setup notebooks for the Unity Catalog volumes and connections the jobs need. Review every setup
notebook with the user before it runs.

## Integration runtimes and on-premises sources

flowx does not translate integration runtimes. A pipeline that reads through a self-hosted integration
runtime needs network access from Databricks to that source (for example a private endpoint or a
connection through a gateway): tell the user it is a prerequisite for the migrated job.

## Exports

Discover reads the factory's Git layout and an ARM template. The `flowx-extract-metadata` skill covers
getting one from Git, ADF Studio or a live pull. Fabric Data Factory uses the `adf` source.

## Patterns across activities

Mapping data flows and SSIS packages are agentic. Multi-activity patterns (metadata-driven copy, watermark
incremental loads, REST pagination, activity-and-notify) are detected as motifs and offered as
consolidation options; see `sources/adf.md`.
