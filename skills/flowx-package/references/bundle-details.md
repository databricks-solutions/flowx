# Package details

Read this from the `flowx-package` skill when you need the detail behind a step.

## Archiving and the packaged-bundles record

- A `resources/*.yml` that this package does not write again (one an earlier package wrote, or one
  added by hand) is moved into `metadata/archive/<UTC time>/` with an `archive.json`, so it no longer
  deploys and is kept for audit. The files package does write again are overwritten.
- Package records the bundle folders it wrote in `metadata/packaged_bundles.json` (`"."` for one bundle
  at the top of the output folder, otherwise one folder per bundle: per pipeline, or per group under
  `--packaging-mode per-group`). Once its preflight passes and before it writes anything, the next
  package moves every recorded folder it will not write into `metadata/archive/<UTC time>/`: a pipeline
  an agentic unit replaced under a new name, the per-pipeline folders when a convert narrows to one
  pipeline, or the top-level bundle when it widens again. So a new bundle never lands on top of an old
  one that shares its folder name. A folder package never recorded stays where it is. A malformed
  record stops package before it writes anything; discover archives it with the rest of `metadata/`.
- Whether a deploy uploads `metadata/archive/` is covered in `../../shared/output-folder.md`.

## Notification destinations

When an `activity_and_notify` motif was opted into a Slack, Teams, PagerDuty or generic webhook
destination, modify creates (or reuses by display name) the destination through the SDK and carries its
id in the report; package wires that id into the task's `webhook_notifications`. With no id in the report,
package tries to create it; failing that (for example with no workspace auth), it adds a
`notification_destination` setup task to SETUP.md and the task ships without notifications. Email
destinations use `email_notifications` and never create an SDK destination.
