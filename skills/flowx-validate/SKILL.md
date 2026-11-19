---
name: flowx-validate
description: >
  Validate a packaged bundle against the source pipelines. Static checks run first; then you review
  each job and task's orchestration against the source pipeline's intent, the library stamps and
  scores your findings, and you fix the bundle in place. The loop repeats until the approval
  threshold is met, the iteration budget runs out, or the bundle stops changing. flowx-migrate runs
  it after package.
triggers:
  - "validate bundle"
  - "validate migration"
  - "review converted pipelines"
  - "grade conversion"
  - "flowx validate"
---

# Validate the Converted Bundle

`validate` checks that the generated bundle does what the source pipeline did. You, the agent, do the
review. flowx runs no model: it runs the static checks, picks what to review, scores your findings,
stamps every object, writes the reports, and applies your fixes.

## What you review

- **Unit:** each task, rolled up to its job. Job-level findings cover what belongs to the whole job.
- **Ground truth:** the source pipeline only (`sources` in the prepare result). Grade whether the job
  keeps the source's intent, not whether it matches some target architecture.
- **Scope:** orchestration only: the task graph, dependencies, run conditions, parameters, triggers and
  schedules, branches, and loops (`for_each_task` inputs and concurrency). Do not grade generated code
  or notebooks.
- **User choices:** `customizations` holds the answers from `metadata/configuration.json`. When a
  difference follows from one of them, waive it: set `customization` to the option id and
  `customization_value` to the value the user chose, copied from `customizations`. The library checks both
  against `metadata/configuration.json` and rejects the waiver (the record fails with a `waiver rejected`
  violation) when the option is missing or the value differs. A waived finding is kept in the report but
  does not lower the score. A blocker cannot be waived.

## Scoring

The library computes every score and verdict from your findings; you never send a score.

| Severity | Penalty | Use for |
|---|---|---|
| `blocker` | 100 | The job cannot run, or runs the wrong work (missing task, wrong dependency, lost loop). |
| `major` | 20 | Behaviour differs in a way users will notice (wrong condition, parameter, schedule, concurrency). |
| `minor` | 5 | A small difference with little effect. |
| `advisory` | 0 | A note; nothing to fix. |

- Task score = `100 - sum(penalties)`, floored at 0. Customization findings carry no penalty.
- A task is `rejected` when it has a blocker or scores below 60, `approved` when it has no findings, and
  `approved_with_findings` otherwise.
- Job score = the lower of its own job-level score and the mean of its task scores. A job is rejected
  when it has a blocker or any of its tasks is rejected.
- Approval rate = the mean over jobs of `score / 100`, with rejected jobs counted as 0.

## The loop

Run the **`setup`** skill first if you haven't. Validate needs the translation report: `package` keeps a
copy at `metadata/translation_report.json`, and `report_path` / `--report` points at another one. Without
either, prepare fails with an error naming the missing file. A `pipeline` that matches no bundle directory
or job key also fails, with `Pipeline '<name>' not found`.

1. **Prepare.** Start or resume a run. Static checks run first. If they fail (`status: static_failed`),
   the iteration is already recorded with every failing bundle rejected; fix the `static` findings
   (step 4) and prepare again. `checks` lists each static check per bundle as `passed`, `failed`, or
   `skipped` with a `reason`. `databricks bundle validate` runs by default when the `databricks` CLI is on
   PATH. Where it is missing (serverless, Genie Code), or when turned off, it is recorded as `skipped`;
   tell the user it did not run. Otherwise (`status: review`) you get the `targets` to review with their
   `definition`, the `sources`, `translation`, `customizations`, and the `ruleset`. Objects unchanged
   since an `approved` stamp are listed in `skipped` and need no review. `status: terminated` means the
   run already ended; pass `reset` to start a new one.
2. **Review** every target. Write one review per target, with an empty `findings` list when it is
   correct. Every finding needs `code`, `severity`, `message`, and a `suggested_fix` that a person or an
   agent can act on; add `location` when it helps.

   ```json
   {
     "reviewer": "<your model id>",
     "reviews": [
       {"object_id": "pl_ingest", "findings": []},
       {"object_id": "pl_ingest/copy_customers", "findings": [{
         "code": "ORCH-WRONG-CONCURRENCY",
         "severity": "major",
         "message": "Source ForEach runs 5 items at a time; the job runs 20.",
         "location": "pl_ingest/resources/pl_ingest.yml",
         "suggested_fix": "Set for_each_task.concurrency to 5."
       }]},
       {"object_id": "pl_ingest/copy_orders", "findings": [{
         "code": "ORCH-COPY-AS-PIPELINE",
         "severity": "major",
         "message": "The source Copy activity became a pipeline task, as the user chose.",
         "suggested_fix": "None needed; the user chose SDP for copies.",
         "customization": "copy_activity_paradigm",
         "customization_value": "sdp"
       }]}
     ]
   }
   ```
3. **Record.** On `ok: false` fix every item in `violations` and record again; nothing was written. On
   success you get `approval_rate`, `terminated`, and the `actionable` findings. When `terminated` is set,
   stop and present the result.
4. **Remediate** while the run has not ended. Fix the actionable findings in the bundle's YAML (job
   resources or `databricks.yml`) and send each changed file whole: `[{"path": "<path relative to
   output_dir>", "content": "<new file text>"}]`. Only `databricks.yml` and YAML under a bundle's
   `resources/` can change; paths outside the bundle, including through symlinks, are refused.
   - `status: needs_approval`: the run requires approval. Show the user the `diffs`, ask, and send the same
     edits again with `approve` only if they agree.
   - `status: dry_run`: nothing was written; show the `diffs` as proposals.
5. Go back to step 1.

The loop stops with `terminated` set to:

| Reason | When |
|---|---|
| `threshold_met` | The approval rate reached the threshold (default 0.9). |
| `budget_exhausted` | The iteration budget (default 3) is used up. |
| `oscillation` | The bundle hash matches an earlier iteration, so remediation is going in circles or made no change. |
| `dry_run` | A dry run ends after its single review. |

## Run it: MCP tool or venv CLI

- **MCP tool:** one `flowx` call per step, all with the same `output_dir`:

  ```
  flowx(command="validate", parameters={"output_dir": "<dir>", "action": "prepare",
                                        "threshold": 0.9, "max_iterations": 3, "pipeline": "<optional>",
                                        "dry_run": false, "require_approval": false,
                                        "bundle_validate": true})
  flowx(command="validate", parameters={"output_dir": "<dir>", "action": "record", "reviews": {...}})
  flowx(command="validate", parameters={"output_dir": "<dir>", "action": "apply", "edits": [...], "approve": false})
  flowx(command="validate", parameters={"output_dir": "<dir>", "action": "publish", "results_table": "c.s.t"})
  ```

- **venv CLI:**

  ```bash
  export PYTHONPATH="<plugin_dir>/src"
  PY="$(cat <plugin_dir>/.migration-venv)"
  "$PY" -m flowx.adapter validate prepare --output-dir <dir> [--threshold 0.9] [--max-iterations 3] \
      [--pipeline <job>] [--dry-run] [--require-approval] [--reset] [--no-bundle-validate] [--profile <p>] [--report <file>]
  "$PY" -m flowx.adapter validate record  --output-dir <dir> --reviews-path reviews.json
  "$PY" -m flowx.adapter validate apply   --output-dir <dir> --edits-path edits.json [--approve]
  "$PY" -m flowx.adapter validate publish --output-dir <dir> --results-table <catalog.schema.table>
  ```

  Exit code 0 means the step succeeded; 1 prints the violations or error. `--no-bundle-validate` skips
  `databricks bundle validate`; the skip is still recorded in `checks`.

Run settings (`threshold`, `max_iterations`, `pipeline`, `dry_run`, `require_approval`) apply when a run
starts. A saved run resumes with its own settings; pass `reset` to change them.

## What it writes

Everything lives under `<output_dir>/metadata/validation/`:

- `validation_report.json`: the run report: `run_id`, `run_by`, `ruleset_version`, `reviewer`,
  `terminated`, `iterations`, `approval_rate`, `content_hash`, the latest `stamps`, the run settings, and
  the `static_checks` of the latest iteration, and the `trajectory` of approval rate and bundle hash per
  iteration. The bundle hash covers only deployable inputs (`databricks.yml`, `resources/`, `src/`), so
  `metadata/` and validate's own state never change it.
- `runs/<run_id>/iteration_NNN.json`: one file per iteration with its stamps, static findings and checks, the
  per-object `diffs` since the previous iteration, the reviewed `snapshots`, and the files changed by
  `remediation`. A reset keeps the old run's files and copies its report here.

Each stamp records `object_id`, `object_kind`, `verdict`, `score`, `findings`, `reviewer`,
`ruleset_version`, `reviewed_at`, `iteration`, and `content_hash`.

`publish` writes one row per object per iteration to a Unity Catalog table (`run_id`, `run_date`,
`run_by`, `iteration`, `object_id`, `object_kind`, `verdict`, `score`, `finding_count`, `blocker_count`,
`major_count`, `reviewer`, `ruleset_version`, `content_hash`). The table must be `catalog.schema.table`,
each part made of letters, digits, `_`, or `-`; each part is backtick-quoted. Publishing a run again
replaces its rows.

Don't run two validate steps on the same output folder at once.
