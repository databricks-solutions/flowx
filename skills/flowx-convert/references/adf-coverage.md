# ADF coverage: what flowx does with each part of a factory

Read this from the `flowx-convert` skill with `references/adf.md`. It lists each area of an Azure Data
Factory export, what flowx produces for it, and what you and the user still have to do. It is the ADF
counterpart of `../../shared/airflow-coverage.md`.

**An activity type the activity mapping doesn't list is unsupported: report it as unsupported and never
guess a translation.** Anything else this file doesn't cover (a connector, a linked service type, a
pattern) is not unsupported for that reason alone: convert's report says what it translated and leaves
the rest as agentic gaps (see [`adf.md`](adf.md)).

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
| Schedule trigger (`ScheduleTrigger`) | The job's `schedule` (Quartz cron and time zone). A day or week interval above 1 becomes a periodic trigger, and a time of day it can't hold goes to `SETUP.md`. A month interval above 1 gets no schedule, only a `schedule_setup_note` in the job YAML | Check the time zone; schedule a multi-month interval by hand |
| Storage events trigger (`BlobEventsTrigger`) | A file arrival trigger whose `url` is the trigger's `scope`, the storage account's resource ID; `blobPathBeginsWith` and the events are not carried over | Replace the `url` with the storage location to watch, and re-add the path filter |
| Tumbling window trigger (`TumblingWindowTrigger`) | No schedule: only a `schedule_setup_note` in the job YAML, with the frequency and interval | Schedule the job by hand; backfill and window dependencies are not carried over |
| Custom events trigger (`CustomEventsTrigger`) | No trigger: only a `schedule_setup_note` in the job YAML | Wire it by hand |

A `schedule_setup_note` is not written to `SETUP.md`, so point the user to each job YAML that has one.

Parameter values a trigger passes to its pipeline become the job's default parameter values.

## Dependencies between activities

Each edge's dependency conditions collapse to one outcome: Succeeded and Failed together become
Completed, a list that includes Skipped becomes Skipped, and Failed with others becomes Failed. A
Databricks task has one run condition for all its upstream tasks, so the outcomes then combine per task:
any Completed or Skipped edge makes the task `ALL_DONE`, otherwise any Failed edge makes it
`AT_LEAST_ONE_FAILED`, otherwise it runs on `ALL_SUCCESS`. So a task that waits on one upstream's
Skipped also runs when it succeeds, and a task with one Succeeded and one Completed edge also runs after
the first fails: tell the user when a task mixes conditions.

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
consolidation options; see `references/adf.md`.
