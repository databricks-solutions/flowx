# FIX_0603 Changes

Track of changes applied to branch `fix-0603` from the validation-implementation workflow.

## Iteration 1 — 2026-06-03

Implemented 9 of 13 plan changes (4 deferred to a future iteration: see "Deferred" below).

### Change: `expr-resolver-globalparams-and-wrappers` (P0)

- **Title**: Teach expression resolver about factory globalParameters, @json/@string/@array no-op wrappers, trailing-whitespace expressions, item() safe-nav, and @linkedService().X refs.
- **Rationale**: Drives NB-1, NB-4 (partial), NB-5, CF-003, LSC-001, LSC-005, VAR-003, VAR-005 — five separate gaps trace to the same resolver weaknesses. Loading globalParameters once at parse time and stripping leading-`@` wrappers unblocks all of them without refactor.
- **Files changed**:
  - `src/orchestra/models/adf_ast.py` — added `AdfDefinitions.global_parameters`.
  - `src/orchestra/models/ir.py` — added `TranslationContext.global_parameters` and `linked_service_parameters`, plus `get_global_parameter` / `get_linked_service_parameter` / `with_linked_service_parameters` helpers.
  - `src/orchestra/parser/adf_loader.py` — added `factory_dir` loader and `_parse_factory_global_parameters` helper; same for the ARM-template branch.
  - `src/orchestra/parser/expression_parser.py` — added `_resolve_pipeline_global_param`, `_resolve_linked_service_param`, `_resolve_item_safe_nav`, `_unwrap_noop_call`; widened `_FUNCTION_CALL_RE` to accept trailing whitespace.
  - `src/orchestra/translator/engine.py` — thread `definitions.global_parameters` into the seeded `TranslationContext`.
- **Tests added**: 5 new test classes in `tests/unit/test_expression_parser.py` covering global parameters (literal substitution + dict-shape + missing fallback + concat reduction), no-op wrappers (json/string/array), trailing-whitespace function calls, item safe-nav, and linked-service parameter resolution.
- **Commit**: `2cd8929`

### Change: `linked-service-parameter-resolution` (P0)

- **Title**: Resolve `@linkedService().X` against activity-supplied parameters with LS defaultValue fallback.
- **Rationale**: NB-4 and LSC-001 — `AdfLinkedServiceReference` dropped the activity's `parameters` dict at parse time. 166/327 emitted databricks.yml files had `spark_version: '@linkedService().clusterVersion'` and failed to deploy. Also coerces `num_workers='1'` strings to int.
- **Files changed**:
  - `src/orchestra/models/adf_ast.py` — added `AdfLinkedServiceReference.parameters` field.
  - `src/orchestra/parser/adf_loader.py` — populate `parameters` from `linkedServiceName.parameters`.
  - `src/orchestra/translator/engine.py` — added `_resolve_ls_parameters`, `_substitute_ls_params`, `_coerce_int`; `_extract_cluster_config` now accepts `ls_param_overrides` and walks string values through `_substitute_ls_params` before reading fields.
- **Tests added**: `TestCommonAttributes.test_linked_service_parameter_overrides_cluster_version` in `tests/unit/test_translators.py`.
- **Commit**: `e33ca26`

### Change: `linked-service-cluster-field-coverage` (P1)

- **Title**: Extend `_extract_cluster_config` to lift `spark_env_vars`, `custom_tags`, `driver_node_type_id`, `init_scripts`, `data_security_mode`, `cluster_log_conf`; propagate to emitted clusters.
- **Rationale**: NB-3, LSC-003 — five LS keys lost; default-cluster builder never consulted per-task cluster.
- **Files changed**:
  - `src/orchestra/translator/engine.py` — `_extract_cluster_config` now lifts the extended fields.
  - `src/orchestra/bundler/dab_writer.py` — added `_infer_bundle_cluster_extras`; `_build_default_cluster` now accepts extras and merges them into `new_cluster`.
- **Tests added**: `TestCommonAttributes.test_extended_cluster_fields_propagated` in `tests/unit/test_translators.py`; `TestClusterExtrasPropagation.test_extras_merged_into_default_cluster` in `tests/unit/test_bundler.py`.
- **Commit**: `b033123`

### Change: `library-resolution-and-stub-binding` (P0)

- **Title**: Resolve library jar/whl/maven/pypi expressions; bind stub notebooks with jar libraries to a real cluster.
- **Rationale**: NB-1 + LSC-005 + NB-2 — library jar paths shipped as literal `@concat(...)`; stub tasks with libraries were skipped during cluster binding, so the Jobs API rejected them.
- **Files changed**:
  - `src/orchestra/translator/activity_translators/notebook.py` — added `_resolve_libraries` that walks each library entry through `resolve_expression`.
  - `src/orchestra/bundler/dab_writer.py` — `_bind_cluster_to_notebook_tasks` now binds the default cluster for any stub or serverless-mode task that ships jar/whl/maven/pypi libraries.
- **Tests added**: 2 tests in `tests/unit/test_translators.py` (library resolution + global parameter substitution); 3 tests in `tests/unit/test_bundler.py` under `TestStubLibraryBinding`.
- **Commit**: `9b1af19`

### Change: `base-parameters-cleanup-and-stub-widgets` (P1)

- **Title**: Strip unresolved ADF expressions from stub notebook base_parameters.
- **Rationale**: NB-5 — stub notebook tasks shipped raw `@string(coalesce(...))` strings as widget defaults; `dbutils.widgets.get` returns them verbatim, which fails at runtime.
- **Files changed**:
  - `src/orchestra/bundler/dab_writer.py` — dropped the `notebook_path.startswith('/')` gate on `_extract_manual_parameters_from_existing_notebook_tasks` so both absolute-path and bundle-relative stubs are walked.
- **Tests added**: `TestStubBaseParameterCleanup.test_stub_notebook_strips_unresolvable_adf_expression` in `tests/unit/test_bundler.py`.
- **Commit**: `e1ba336`

### Change: `lookup-file-dataset-support` (P0)

- **Title**: Resolve Lookup `typeProperties.dataset` and emit `spark.read` for file-source lookups instead of `spark.sql('')`.
- **Rationale**: Three gaps in the lookup dimension share root cause — translator never read `typeProperties.dataset`, code_generator emitted `spark.sql('')`, multiline JSON handling was missing.
- **Files changed**:
  - `src/orchestra/translator/activity_translators/lookup.py` — resolve `typeProperties.dataset` / `activity.inputs` to the bound `AdfDataset`; surface `dataset_type`, `container`, `folder_path`, `file_name`, `multiLineJson` onto `source_properties` for file-source dataset types.
  - `src/orchestra/preparer/code_generator.py` — added `_is_file_lookup`, `_file_lookup_body`; the existing JDBC / non-DB branches now fall through to the file-source branch when the dataset is file-shaped.
- **Tests added**: `TestLookupTranslator.test_translate_lookup_resolves_json_file_dataset` in `tests/unit/test_translators.py`; `TestGenerateLookupNotebook.test_file_source_lookup_emits_spark_read` in `tests/unit/test_code_generator.py`.
- **Commit**: `3b669f6`

### Change: `foreach-inner-extra-tasks` (P0)

- **Title**: Carry inner-activity `extra_tasks` through ForEach inner-job assembly so IfCondition/Switch branches survive.
- **Rationale**: CF-001 — for_each preparer's multi-child path appended only `child_prepared.task`, dropping `extra_tasks` where IfCondition/Switch branch bodies live. 12 pipelines lost branches.
- **Files changed**:
  - `src/orchestra/preparer/activity_preparers/for_each.py` — multi-child path now `.extend(child_prepared.extra_tasks)`; single-child path escalates to the sub-job pattern when the sole child contributes extra_tasks.
- **Tests added**: 2 tests in `tests/unit/test_preparers.py` under `TestForEachPreparer` (multi-child IfCondition; single-child IfCondition escalation).
- **Commit**: `8eceaea`

### Change: `dependency-multi-condition-mapping` (P1)

- **Title**: Map ADF `dependencyConditions` lists to the correct DAB `run_if` value instead of picking the first.
- **Rationale**: CF-004 — `[Succeeded, Failed]` (= "run regardless") was reduced to `Succeeded`, silently reversing semantics for log/error-handler tasks.
- **Files changed**:
  - `src/orchestra/translator/engine.py` — added `_map_dependency_conditions` helper that reduces lists per documented mapping rules; `_build_base_kwargs` uses it.
- **Tests added**: `TestCommonAttributes.test_dependency_multi_condition_succeeded_and_failed_maps_to_completed` in `tests/unit/test_translators.py`.
- **Commit**: `fe37f9f`

### Change: `expression-resolver-bool-and-numeric-coercion` (P1)

- **Title**: Coerce LS / parameter values during resolution — stringified booleans to YAML booleans, numeric strings to ints where the cluster spec demands int.
- **Rationale**: VAR-006 — bool default `false` ends up as string `'False'`; num_workers `'1'` as string. The numeric coercion was already part of change 2; this change adds the bool/int/float pipeline-parameter coercion.
- **Files changed**:
  - `src/orchestra/translator/engine.py` — added `_coerce_parameter_default`; pipeline-parameter entries now carry `type` + properly-typed default.
  - `src/orchestra/bundler/dab_writer.py` — `pipeline_dict_to_ir` honours non-string defaults verbatim (no string-coercion when value is already bool/int/float).
- **Tests added**: 3 tests in `tests/unit/test_translators.py` covering bool/int/string coercion.
- **Commit**: `5f75b01`

### Change: `pipeline-parameters-and-variables-round-trip` (P0, partial)

- **Title**: Round-trip `pipeline.parameters` through `translation_report`.
- **Rationale**: VAR-001 — `_load_report`'s aggregated branch dropped pipeline parameters, so 329/340 bundles referenced `{{job.parameters.X}}` without declaring them.
- **Scope note**: Implemented only the parameter round-trip via the aggregator. Variable-init bridge tasks (VAR-004) and full variable-cache plumbing not implemented this iteration.
- **Files changed**:
  - `src/orchestra/bundler/dab_writer.py` — aggregator now reads `translation.parameters` and `ir.parameters`, threads them through `pipeline_dict_to_workflow`.
- **Tests added**: `TestAggregatedReportPipelineParameters.test_load_report_carries_pipeline_parameters` in `tests/unit/test_bundler.py`.
- **Commit**: `1aa3322`

## Deferred

The following P0/P1 plan items were not addressed in this iteration. They each require broader cross-file refactors than was safe inside a single iteration with the time budget available.

- **`condition-and-switch-expression-bridge` (P0)** — bridging IfCondition/Switch expressions through synthetic SetVariable tasks requires changes across the if_condition / switch translators **and** preparers **and** execute_pipeline preparer (5 files), plus the inner-task wiring for `run_job_task.job_parameters`. The transformation is non-trivial and risks regressions in existing condition_task / case_task tests. Plan for next iteration: implement behind a feature flag, then turn on after corpus validation.
- **`triggers-to-schedules` (P0)** — end-to-end schedule support touches engine + workflow_preparer + dab_writer for trigger ingestion, N:M binding, parameter overrides, and Tumbling/BlobEvents/Custom routing to SETUP.md. Plan for next iteration: land in a single focused PR.
- **`credential-and-secret-recognition` (P0)** — AzureKeyVaultSecret + AzureBlobFS CredentialReference + MSI auth across 4 files. Plan for next iteration: tackle one credential family per commit.
- **Variable defaults & init bridges (partial — half of `pipeline-parameters-and-variables-round-trip`)** — VAR-004 / VAR-006 (variable defaultValue dropped + parameter type dropped) need the variable-init bridge task pattern, which interlocks with the condition/switch bridge work above.

## Summary

- 10 commits on `fix-0603` (this section's 9 changes + one for partial round-trip)
- 528 unit tests pass after the final commit (baseline: 499 — net 29 new tests)
- No tests broken; no `--no-verify` or `--amend` used.

## Iteration 2 — 2026-06-03

Implemented 11 of 13 plan changes (C-13 folded into C-07 since they share the same surface; one residual cleanup deferred — see notes below).

### C-01 — Collapse `@concat()` to a literal when every part resolves to a literal (P0)

- **Rationale**: NB-ITER2-1 / LSC2-004. `_resolve_concat` / `_handle_concat` always emitted `kind='notebook_code'`. Library jar paths whose concat parts all resolve to literals shipped as Python source instead of strings; `notebook._resolve_libraries` only inlines literals, so installs silently broke in 185 bundles.
- **Files**: `src/orchestra/parser/expression_parser.py`, `tests/unit/test_expression_parser.py`, `tests/unit/test_translators.py`.
- **Tests**: `TestConcat.test_concat_literals`, `TestConcat.test_concat_collapses_when_all_parts_resolve_to_literals`, `TestNotebookTranslator.test_translate_notebook_resolves_library_with_globals` (updated to assert literal jar path).
- **Commit**: `cb5ddb7`.

### C-02 — Unwrap `{value, type:'Expression'}` dicts in LS params and cluster fields (P0)

- **Rationale**: NB-ITER2-2 / LSC2-003. `_substitute_ls_params` and `_extract_cluster_config` preserved the ADF expression dict-wrapper shape; `custom_tags` emitted `{DigitalCase: {value: CLI0010, type: Expression}}` (166 bundles) which is invalid for Databricks `Map[String,String]`.
- **Files**: `src/orchestra/translator/engine.py`, `tests/unit/test_translators.py`.
- **Tests**: `TestCommonAttributes.test_ls_param_expression_wrapper_unwrapped_in_custom_tags`, `TestCommonAttributes.test_ls_param_resolved_against_factory_global_parameters` (covers C-03 too).
- **Commit**: `267b08d` (also covers C-03 since both modifications share `_resolve_ls_parameters`).

### C-03 — Run activity-supplied LS parameter values through `resolve_expression` with factory globals (P0)

- **Rationale**: NB-ITER2-3 / LSC2-002. Activities passing `clusterVersion: {value:'@pipeline().globalParameters.clusterVersion', type:'Expression'}` left the raw expression in 50 IR JSONs and 4 bundles. `_resolve_ls_parameters` now threads the translation context so `@pipeline().globalParameters.X` collapses to its factory value.
- **Files**: `src/orchestra/translator/engine.py`, `tests/unit/test_translators.py`.
- **Tests**: `TestCommonAttributes.test_ls_param_resolved_against_factory_global_parameters`.
- **Commit**: folded into `267b08d`.

### C-04 — Walk nested activities when collecting workflow cluster hints (P0)

- **Rationale**: NB-ITER2-4 / LSC2-001. `prepare_workflow` iterated only `pipeline.tasks`. Pipelines whose only `DatabricksNotebook` lived inside an `IfCondition` / `Switch` / `ForEach` branch shipped the default `Standard_DS3_v2 / 15.4.x` cluster fallback in 26 bundles. New `_iter_activity_with_descendants` BFS-walks each top-level activity.
- **Files**: `src/orchestra/preparer/workflow_preparer.py`, `tests/unit/test_preparers.py`.
- **Tests**: `TestPrepareWorkflow.test_prepare_workflow_collects_cluster_hints_from_nested_activities`, `TestPrepareWorkflow.test_prepare_workflow_collects_cluster_hints_from_switch_default_branch`.
- **Commit**: `84b491a`.

### C-05 — Synthesise init SetVariable tasks for variables with defaultValue (P0)

- **Rationale**: VAREX-002. Pipeline variables carrying `defaultValue` (164 in corpus, 127 expression-typed) were never materialised as IR tasks, so `_resolve_variable`'s self-referential fallback produced 333 dangling `{{tasks.X.values.X}}` refs. `translate_pipeline` now prepends a `_init_<var>` SetVariableActivity per default-valued variable and registers the synth task in `variable_cache`. `_resolve_variable` additionally returns `None` when no setter is known to surface unresolved variables as raw expressions instead of placeholders.
- **Files**: `src/orchestra/translator/engine.py`, `src/orchestra/parser/expression_parser.py`, `tests/unit/test_translators.py`, `tests/unit/test_preparers.py` (Switch unresolved-variable assertion updated), `tests/unit/test_expression_parser.py` (rename `test_variable_fallback_to_name` → `test_variable_returns_none_when_no_setter`).
- **Tests**: `TestVariableInitTasks.test_default_valued_variable_yields_init_task`, `TestVariableInitTasks.test_default_valued_variable_with_concat_expression`.
- **Commit**: `d80c665`.

### C-06 — Route ForEach inner-job `@variables()` refs via task-value, not undeclared job parameter (P0)

- **Rationale**: VAREX-004. `collect_inner_job_params` previously declared `variables()` refs as inner-job parameters and mapped them to `{{job.parameters.<var>}}` on the parent. Parent never declared those names so the inner job received an empty string. When a parent setter task_key is known, variables now route through `{{tasks.<setter>.values.<var>}}` and are NOT declared on the inner-job parameter list.
- **Files**: `src/orchestra/bundler/inner_job_params.py`, `src/orchestra/preparer/activity_preparers/for_each.py`, `src/orchestra/preparer/workflow_preparer.py`, `tests/unit/test_for_each_inner_job_params.py` (new).
- **Tests**: 4 new tests in `tests/unit/test_for_each_inner_job_params.py`.
- **Commit**: `c119ab3`.

### C-07 — Bridge IfCondition / Switch condition_task operands through a hidden SetVariable task (P0)

- **Rationale**: CF-iter2-001 / CF-iter2-003 / VAREX-003. Per Databricks docs, `condition_task.left` / `.right` must be literal / job-parameter / task-value / task-parameter refs. Operands carrying ADF function calls (`@toUpper`, `@coalesce`, `@and`, `@or`, `@not`, `@empty`, `@concat`, ...) shipped as raw ADF expressions in 84 bundles. New `lower_to_bridge` / `merge_bridge_requests` in `activity_translators/resolve.py` turn `notebook_code` resolver results into a `BridgeRequest` that the IfCondition / Switch translators stash on the IR; the corresponding preparers synthesise a bridge notebook task and rewrite the operand to the task-value reference. Dropped the legacy `NOT_EQUAL '0'` fallback when a bridge succeeds.
- **Files**: `src/orchestra/models/ir.py` (new bridge_notebook_* fields on IfCondition/Switch), `src/orchestra/translator/activity_translators/resolve.py`, `src/orchestra/translator/activity_translators/if_condition.py`, `src/orchestra/translator/activity_translators/switch.py`, `src/orchestra/preparer/activity_preparers/if_condition.py`, `src/orchestra/preparer/activity_preparers/switch.py`, `src/orchestra/translator/engine.py` (serialise bridge fields).
- **Tests**: `TestIfConditionTranslator.test_translate_if_condition_empty_bridges_via_notebook`, `TestIfConditionPreparer.test_prepare_if_condition_emits_bridge_task`, `TestSwitchTranslator.test_translate_switch_function_call_routes_through_bridge`, plus `TestSwitchPreparer.test_resolve_switch_on_expression_is_idempotent_for_dab_refs` (covers C-13).
- **Commit**: `7494fde`.

### C-08 — Bridge ForEach `for_each_task.inputs` through a seed task when items expression is notebook_code (P0)

- **Rationale**: CF-iter2-002. Per docs, `inputs` accepts a JSON array literal or `{{tasks.X.values.Y}}` or `{{job.parameters.X}}`; `@split(...)` was rejected. 12 bundles emitted `inputs: '@split(...)'` verbatim. New `_resolve_for_each_inputs_with_bridge` emits a seed notebook task that computes the array and rewrites `inputs` to its task value.
- **Files**: `src/orchestra/preparer/activity_preparers/for_each.py`, `tests/unit/test_preparers.py`.
- **Tests**: `TestForEachPreparer.test_prepare_for_each_bridges_split_items_via_seed_task`.
- **Commit**: `92036a8`.

### C-09 — ExecutePipeline parameter resolution refuses notebook_code result kinds (P0)

- **Rationale**: VAREX-001. 62 bundles had ExecutePipeline parameter values like `'json: ' + dbutils.widgets.get('configFile')` because `resolve.py` accepted any result.kind. The sub-job's widget received the Python source text. The translator now resolves each parameter through `resolve_expression`, accepts only literal / dab_ref results, and surfaces `notebook_code` results as `parameter_approximations` for SETUP.md.
- **Files**: `src/orchestra/translator/activity_translators/execute_pipeline.py`, `tests/unit/test_translators.py`.
- **Tests**: `TestExecutePipelineTranslator.test_translate_execute_pipeline_drops_notebook_code_parameters`.
- **Commit**: `34e0bd9`.

### C-10 — Compile AdfTrigger objects into Pipeline.schedule and emit DAB schedule / trigger blocks (P0)

- **Rationale**: SCHED-001. `definitions.triggers` was parsed but never read; `Pipeline.schedule` was always `None`; 327 bundles shipped without any schedule metadata. `translate_pipeline` now matches triggers by their `pipelineReference` and compiles the first matching one into a structured schedule dict (`ScheduleTrigger.recurrence` → quartz cron + timezone; `BlobEventsTrigger` → `trigger.file_arrival`; Tumbling/Custom → manual-setup hints). Windows timezone names (`Romance Standard Time`) normalise to IANA (`Europe/Madrid`). `runtimeState='Stopped'` flips `pause_status` to `PAUSED`. `PreparedWorkflow.schedule` and `pipeline_dict_to_ir` thread the spec to the DAB writer.
- **Files**: `src/orchestra/preparer/workflow_preparer.py`, `src/orchestra/translator/engine.py`, `src/orchestra/bundler/dab_writer.py`, `tests/unit/test_translators.py`, `tests/unit/test_bundler.py`.
- **Tests**: 7 new tests in `TestScheduleCompilation` covering Daily / Weekly / Tumbling / Blob / Custom triggers, timezone normalisation, and pause-status; 2 new bundler tests (`TestScheduleEmission`) verifying the schedule / trigger blocks land in the rendered YAML.
- **Commit**: `bc10f5b`.

### C-11 — Emit per-secret SecretInstructions from Web activity auth payloads (P1)

- **Rationale**: LSC2-005. `web_activity.prepare` always emitted a single `auth-credential` SecretInstruction regardless of the underlying ADF auth shape. AzureKeyVaultSecret payloads carry `store.referenceName` and `secretName` but both were dropped; CredentialReference (managed identity) was wrongly treated as a static secret. The preparer now walks nested fields (`password` / `secret` / `clientSecret` / `pfx` / `key`), materialises per-Key Vault SecretInstructions, and routes CredentialReference / MSI payloads to a `manual_credential` SetupTask so SETUP.md flags them.
- **Files**: `src/orchestra/preparer/activity_preparers/web_activity.py`, `tests/unit/test_preparers.py`.
- **Tests**: `TestWebActivityPreparer.test_prepare_web_activity_key_vault_secret_uses_vault_scope_and_secret_name`, `TestWebActivityPreparer.test_prepare_web_activity_credential_reference_emits_setup_note`.
- **Commit**: `bce740d`.

### C-12 — Extend `_strip_dangling_task_value_refs` to cover run_job_task and condition_task fields (P1)

- **Rationale**: VAREX-005. `_strip_dangling_task_value_refs` only walked `notebook_task.base_parameters`. 333 dangling `{{tasks.X.values.Y}}` refs survived into resource YAMLs because cross-job (`run_job_task.job_parameters`) and control-flow (`condition_task.left/.right`) surfaces weren't covered. The walker now also visits these fields and blanks dangling refs so SETUP.md §4 can flag them.
- **Files**: `src/orchestra/bundler/dab_writer.py`, `tests/unit/test_bundler.py`.
- **Tests**: `TestStripDanglingTaskValueRefs.test_strips_dangling_run_job_task_job_parameters`, `TestStripDanglingTaskValueRefs.test_strips_dangling_condition_task_operands`, `TestStripDanglingTaskValueRefs.test_recurses_into_for_each_task_body`.
- **Commit**: `836a45d`.

### C-13 — Idempotent Switch on-expression resolver (P1)

- **Rationale**: CF-iter2-004. `preparer/activity_preparers/switch.py::resolve_switch_on_expression` constructed an empty `TranslationContext()` and re-resolved on-expression on the JSON-reload path, destructively stripping refs the translator had already lowered. Updated to pass through anything already containing `{{...}}` or a translator-side `__BRIDGE__::` placeholder; only bare `@`-prefixed expressions are re-resolved.
- **Files**: covered in C-07 commit (`src/orchestra/preparer/activity_preparers/switch.py`).
- **Tests**: `TestSwitchPreparer.test_resolve_switch_on_expression_is_idempotent_for_dab_refs`.
- **Commit**: folded into `7494fde`.

### Skipped / partial

- The plan's C-13 acceptance check additionally calls for `grep -nE 'TranslationContext\(\s*\)' src/orchestra/preparer/` returning zero results.  The current code still constructs bare `TranslationContext()` in ~12 sites across `preparer/code_generator.py`, `preparer/activity_preparers/{execute_pipeline,for_each,switch,databricks_job,filter,notebook,helpers}.py`.  The switch fix is the highest-impact one (it was the only example called out in the rationale); the remaining sites operate at the preparer layer where global parameters and `variable_cache` aren't readily available, so converting them to thread a typed context is a larger refactor.  Deferred to a follow-on iteration.

## Summary (iteration 2)

- 11 new commits on `fix-0603` (C-01 .. C-12, with C-13 folded into C-07)
- 559 unit tests pass after the final iteration-2 commit (iteration-1 baseline: 528 — net 31 new tests)
- No tests broken; no `--no-verify` or `--amend` used.

## Iteration 3 — 2026-06-03 / 2026-06-04

Implemented all 18 plan items across 15 commits (C-13 .. C-27). Three plan items overlapped on single edits and were bundled: C-13 folds three resolver-widening fixes; C-19 folds two ForEach inner-workflow fixes; C-21 fixes the same bool-stringification bug at three call sites.

### C-13 — Propagate globals into ForEach child context; widen LS/library resolvers to accept dab_ref (P0)

- **Rationale**: NB-ITER3-001 / CF3-002 / LSC3-004 (ForEach child context drops global_parameters / linked_service_parameters) + NB-ITER3-002 / LSC3-003 / VAREX3-006 (`_resolve_ls_parameters` refuses `dab_ref`) + NB-ITER3-004 (notebook `_resolve_libraries` refuses `dab_ref`).
- **Files**: `src/orchestra/translator/activity_translators/for_each.py`, `src/orchestra/translator/engine.py`, `src/orchestra/translator/activity_translators/notebook.py`.
- **Commit**: `504000c`.

### C-14 — Preserve bridge_notebook_code/imports/required_parameters on IR roundtrip (P0)

- **Rationale**: CF3-001 / VAREX3-001. `_reconstruct_ir` dropped bridge fields, leaving 109 bundles shipping `left: __BRIDGE__::result` with no actual `_bridge` task.
- **Files**: `src/orchestra/bundler/dab_writer.py`, `tests/unit/test_bundler.py`.
- **Commit**: `a1955e4`.

### C-15 — IfCondition emits right='False' (not '' or '0') against bridge task values (P0)

- **Rationale**: CF3-003 / VAREX3-004. 76 bundles emitted `right: ''` and 19 emitted `right: '0'` from `@not(...)` / legacy truthy fallback paths; neither compares correctly against Python `'True'/'False'`.
- **Files**: `src/orchestra/translator/activity_translators/if_condition.py`, `tests/unit/test_translators.py`.
- **Commit**: `a643a8c`.

### C-16 — Lower single-segment item()?.X safe-nav to notebook_code (P0)

- **Rationale**: CF3-005 / VAREX3-005. `expression_parser._resolve_item_safe_nav` returned None when `len(parts) < 2`, blocking 4 Switch on-expressions and SetVariable expressions from bridge lowering.
- **Files**: `src/orchestra/parser/expression_parser.py`, `tests/unit/test_expression_parser.py`.
- **Commit**: `8f7d6d6`.

### C-17 — Emit single_user_name alongside data_security_mode: SINGLE_USER (P0)

- **Rationale**: NB-ITER3-003. 189 bundles failed `databricks bundle validate` with "single_user_name must be set when data_security_mode is SINGLE_USER". Three cluster builders set SINGLE_USER mode without the name. Use `${workspace.current_user.userName}` as the closest deployable analog of ADF's MSI auth.
- **Files**: `src/orchestra/bundler/dab_writer.py`.
- **Tests**: `TestSingleUserNameOnSingleUserClusters` (3 cases) in `tests/unit/test_bundler.py`.
- **Commit**: `b41cc24`.

### C-18 — Carry schedule through aggregated translations report into pipeline_dict (P0)

- **Rationale**: SCHED3-001. `_load_report`'s aggregated branch built pipeline_dict from `{name, tasks, parameters}` and never copied `schedule`. 0 of 327 bundles contained `quartz_cron_expression` despite 8 trigger-referenced pipelines having populated schedule blocks.
- **Files**: `src/orchestra/bundler/dab_writer.py`.
- **Tests**: `TestAggregatedReportSchedule` (2 cases) in `tests/unit/test_bundler.py`.
- **Commit**: `5666708`.

### C-19 — Propagate cluster_hints + variable_task_keys into ForEach inner workflows (P0 + P1)

- **Rationale**: LSC3-001 — inner-job PreparedWorkflow drops cluster hints from nested activities, so inner-job YAMLs ship the bundle default `Standard_DS3_v2 / 15.4.x` even when the parent default cluster carries LS-derived `spark_env_vars / custom_tags / driver_node_type_id`. CF3-006 — multi-child `collect_inner_job_params` call dropped the `variable_task_keys` kwarg that the single-child path threaded.
- **Files**: `src/orchestra/preparer/activity_preparers/for_each.py`.
- **Tests**: `TestForEachPreparer.test_for_each_inner_workflow_carries_cluster_hints_from_inner_activity`, `TestForEachPreparer.test_for_each_single_child_inner_workflow_carries_cluster_hints` in `tests/unit/test_preparers.py`; `TestVariableTaskKeysRouting.test_multi_child_for_each_threads_variable_task_keys` in `tests/unit/test_for_each_inner_job_params.py`.
- **Commit**: `ece7f01`.

### C-20 — Stop emitting fake auth-credential secret for MSI WebActivity auth (P0)

- **Rationale**: LSC3-002. MSI / ManagedServiceIdentity has no static secret, so reading `auth-credential` from a secret scope shipped a broken bearer-token call against a non-existent secret in 14 generated notebooks across 5 source pipelines. The notebook now raises `NotImplementedError` pointing at SETUP.md. ServicePrincipal auth keeps the secret-based flow.
- **Files**: `src/orchestra/preparer/code_generator.py`.
- **Tests**: `TestGenerateWebActivityNotebook.test_auth_block_msi_raises_not_implemented` in `tests/unit/test_code_generator.py`.
- **Commit**: `5f2c0eb`.

### C-21 — Render Boolean variable values as lowercase true/false (P0)

- **Rationale**: VAREX3-002. Python title-case `'True'/'False'` silently inverted ADF Boolean comparisons like `@equals(variables('continue'), true)` across 28 occurrences. Fix at three call sites: `resolve_expression()` bool literal path (root cause), `_build_variable_init_activities` fallback, and `set_variable.py` translator fallback.
- **Files**: `src/orchestra/translator/engine.py`, `src/orchestra/translator/activity_translators/set_variable.py`, `src/orchestra/parser/expression_parser.py`.
- **Tests**: `TestVariableInitTasks.test_default_valued_boolean_variable_renders_lowercase`, `TestVariableInitTasks.test_set_variable_with_raw_bool_value_renders_lowercase` in `tests/unit/test_translators.py`; updated `TestLiterals.test_boolean` in `tests/unit/test_expression_parser.py`; updated `test_boolean_value` in `tests/unit/test_resolve_field.py`.
- **Commit**: `e7c35d6`.

### C-22 — Case-insensitive dataset / linked service lookup + LS URL threading (P1)

- **Rationale**: LSC3-005. ADF identifiers are documented as case-insensitive, but the loader keys dicts by original case; 2 generated lookup notebooks shipped `spark.sql('')` because a lowercase reference didn't match a mixed-case dataset filename. Also threads `linked_service.typeProperties.url` so the file lookup body assembles a fully-qualified `abfss://...` widget default.
- **Files**: `src/orchestra/models/adf_ast.py` (added `get_dataset` / `get_linked_service`), `src/orchestra/translator/activity_translators/lookup.py`, `src/orchestra/preparer/code_generator.py`.
- **Tests**: `TestLookupCaseInsensitiveAndLinkedService` (2 cases) in `tests/unit/test_translators.py`.
- **Commit**: `b9ec3d6`.

### C-23 — Resolve attribute chains on function-call results (P1)

- **Rationale**: CF3-004. `resolve_expression('@toUpper(json(pipeline().parameters.items).type)')` returned None because the bare function dispatcher only matches when the function call is the outermost token. New `_resolve_function_call_with_attribute` helper detects `funcName(args).attr.attr...`, resolves the function call, then chains `.get('<attr>')` for each segment.
- **Files**: `src/orchestra/parser/expression_parser.py`.
- **Tests**: `TestFunctionCallWithAttribute` (2 cases) in `tests/unit/test_expression_parser.py`.
- **Commit**: `696941d`.

### C-24 — Emit trigger.periodic for Day/Week/Month recurrence with interval > 1 (P1)

- **Rationale**: SCHED3-002. `_recurrence_to_quartz_cron` silently dropped `interval` for Day / Week / Month, producing cron that fired every day/week/month rather than every Nth. quartz cron cannot represent "every N days/weeks/months" without enumeration; DAB `trigger.periodic` takes `{interval, unit}` directly.
- **Files**: `src/orchestra/translator/engine.py`, `src/orchestra/bundler/dab_writer.py`.
- **Tests**: `TestScheduleCompilation.test_schedule_trigger_interval_3_days_emits_periodic`, `test_schedule_trigger_interval_2_weeks_emits_periodic`, `test_schedule_trigger_interval_1_day_still_cron` in `tests/unit/test_translators.py`; `TestScheduleEmission.test_periodic_trigger_emitted` in `tests/unit/test_bundler.py`.
- **Commit**: `563f3e8`.

### C-25 — Carry pipelineReference.parameters from triggers into job parameter defaults (P1)

- **Rationale**: SCHED3-003. `_compile_pipeline_schedule` read only `pipelineReference` from each trigger entry and silently dropped per-pipeline `parameters` overrides like `{applicationName: 'cli0010', negocio: 'GLP'}`. Scheduled runs received bare pipeline defaults. New `_extract_trigger_parameter_overrides` attaches the override map as `parameter_overrides` on the schedule spec; `_build_job_resource` mutates matching `job.parameters[*].default` after applying the schedule.
- **Files**: `src/orchestra/translator/engine.py`, `src/orchestra/bundler/dab_writer.py`.
- **Tests**: `TestScheduleCompilation.test_trigger_carries_per_pipeline_parameter_overrides` in `tests/unit/test_translators.py`; `TestScheduleEmission.test_trigger_parameter_overrides_mutate_job_parameter_defaults` in `tests/unit/test_bundler.py`.
- **Commit**: `4b2eba0`.

### C-26 — Emit manual_variable_rollup SetupTask for cross-ForEach variable reads (P1)

- **Rationale**: VAREX3-003. When a SetVariable for `X` lives only inside a ForEach inner-job and a sibling task reads `@variables('X')`, the read gets the stale init value because task values cannot cross `run_job_task` boundaries. Minimum-viable fix: detect the pattern in `prepare_workflow`, emit a SetupTask of type `manual_variable_rollup`, surface it in SETUP.md so the user adds a roll-up notebook before the sibling runs.
- **Files**: `src/orchestra/preparer/workflow_preparer.py`, `src/orchestra/bundler/prereqs_writer.py`, `src/orchestra/bundler/dab_writer.py`.
- **Tests**: `TestCrossForEachVariableReadDetection` (2 cases) in `tests/unit/test_preparers.py`; `TestManualVariableRollupSetupMd` in `tests/unit/test_bundler.py`.
- **Commit**: `87bf35e`.

### C-27 — Union scan_notebooks_for_secrets with workflow.secrets in SETUP.md (P1)

- **Rationale**: LSC3-006. `scan_notebooks_for_secrets` walked notebooks (yielding the MSI fake `auth-credential` before C-20) while `create_secrets.py` was written from `workflow.secrets` (real AKV scopes). SETUP.md Option A vs Option B disagreed. `build_prereqs` now accepts `secret_instructions`, unions them with the notebook scan (dedupe by (scope, key)) so both options stay in sync.
- **Files**: `src/orchestra/bundler/prereqs_writer.py`, `src/orchestra/bundler/dab_writer.py`.
- **Tests**: `TestSecretsUnion` (2 cases) in `tests/unit/test_prereqs_writer.py` (new file).
- **Commit**: `b64cc5a`.

## Summary (iteration 3)

- 15 new commits on `fix-0603` (C-13 .. C-27)
- 595 unit tests pass after the final iteration-3 commit (iteration-2 baseline: 559 — net 36 new tests)
- No tests broken; no `--no-verify` or `--amend` used.
- All 18 P0/P1 plan items implemented end-to-end. C-13, C-19, and C-21 each fold 2-3 closely-related plan items into a single commit since the underlying fix is the same edit (no scope creep).

## Iteration 4 — 2026-06-04

Implemented all 12 plan items across 11 commits.  C-28 + C-30 are bundled
into one commit since they share NotebookActivity IR fields and the
preparer infrastructure.

### C-28 + C-30 — Dynamic notebookPath dispatch stub + unresolved library SetupTask (P0 + P1)

- **Rationale**: NB-ITER4-001 — the notebook translator passed `notebookPath`
  through `resolve_field` so a `notebook_code` result (e.g.
  `@trim(json(...).notebook_path)`) shipped as the workspace path.
  NB-ITER4-003 — `_resolve_libraries` silently passed through unresolved jar
  paths so the cluster tried to install a file literally named
  `@concat(...)`.
- **Files**: `src/orchestra/translator/activity_translators/notebook.py`,
  `src/orchestra/models/ir.py`,
  `src/orchestra/preparer/activity_preparers/notebook.py`,
  `src/orchestra/bundler/prereqs_writer.py`,
  `src/orchestra/bundler/dab_writer.py`,
  `src/orchestra/translator/engine.py`.
- **Tests**: `test_translate_notebook_dynamic_path_marks_unresolved`,
  `test_translate_notebook_unresolved_library_captured` in
  `tests/unit/test_translators.py`;
  `test_prepare_notebook_dispatch_stub_for_unresolved_path`,
  `test_prepare_notebook_emits_unresolved_library_setup_task` in
  `tests/unit/test_preparers.py`.
- **Commit**: `27ed860`.

### C-29 — Filter unparseable spark_version / node_type_id from cluster_hints (P0)

- **Rationale**: NB-ITER4-002 — `_infer_bundle_cluster_defaults` picked the
  most-common spark_version regardless of whether it parsed as a real DBR
  version, so an `@if(equals(item()?.photon,true),...)` expression landed
  in `databricks.yml` as the spark_version default and `bundle deploy`
  rejected it.
- **Files**: `src/orchestra/bundler/dab_writer.py`.
- **Tests**: `TestUnparseableClusterHintsFiltered` (3 cases) in
  `tests/unit/test_bundler.py`.
- **Commit**: `a6f9c05`.

### C-31 — Move ForEach items-expression bridge to translator (P0)

- **Rationale**: CF4-001 — the for_each preparer constructed a bare
  `TranslationContext()` to re-resolve `@split(variables('fecha'),',')`,
  but `variable_cache` is empty on the JSON-reload path so the bridge
  never fired and DAB rejected the raw @split call as
  `for_each_task.inputs`.
- **Files**: `src/orchestra/translator/activity_translators/for_each.py`,
  `src/orchestra/models/ir.py`,
  `src/orchestra/preparer/activity_preparers/for_each.py`,
  `src/orchestra/bundler/dab_writer.py`,
  `src/orchestra/translator/engine.py`.
- **Tests**: `test_prepare_for_each_uses_ir_bridge_for_variable_based_split`
  in `tests/unit/test_preparers.py`.
- **Commit**: `d666402`.

### C-32 — IfCondition truthy fallback Boolean-aware right operand (P1)

- **Rationale**: CF4-002 — the legacy `right: '0'` fallback against
  Boolean variable refs is always-true (post-C-21 SetVariable writes
  lowercase `'true'/'false'` strings), so the false branch became
  unreachable.
- **Files**: `src/orchestra/translator/activity_translators/if_condition.py`.
- **Tests**:
  `test_translate_if_condition_boolean_variable_uses_lowercase_false`
  in `tests/unit/test_translators.py`.
- **Commit**: `d546a86`.

### C-33 — SetVariable: lower split[N]/2-arg substring and surface unresolved @-expressions (P1)

- **Rationale**: Merged VAREX4-001 + CF4-003 — 212 SetVariableActivity
  entries shipped raw `@concat(...)` text with `value_kind='literal'`
  because `resolve_expression` returned None for nested constructs like
  `split(...)[N]` and the 2-arg `substring(x, start)`.  Bundles that
  remained unresolvable now blank the variable and emit a
  `manual_variable_init` SetupTask.
- **Files**: `src/orchestra/parser/expression_parser.py`,
  `src/orchestra/translator/activity_translators/set_variable.py`,
  `src/orchestra/models/ir.py`,
  `src/orchestra/preparer/activity_preparers/set_variable.py`,
  `src/orchestra/translator/engine.py`,
  `src/orchestra/bundler/dab_writer.py`.
- **Tests**: `test_substring_two_arg_form`, `test_split_with_subscript`
  in `tests/unit/test_expression_parser.py`;
  `test_translate_set_variable_split_subscript_lowers_to_notebook_code`,
  `test_translate_set_variable_unresolved_expression_blanks_value`
  in `tests/unit/test_translators.py`.
- **Commit**: `3a68586`.

### C-34 — Expression parser: preserve quoted-string and Boolean literal types in codegen (P1)

- **Rationale**: Merged VAREX4-002 + VAREX4-003 — both live in
  `_resolve_function_call` / `_arg_to_code`.  Quoted args like `'12'`
  collapsed to bare tokens (`... == 12`, wrong type) and `'09'` produced
  a SyntaxError (leading-zero integer).  Boolean tokens `true/false`
  emitted Python `True/False` but the SetVariable side serialised
  lowercase strings post-C-21.
- **Files**: `src/orchestra/parser/expression_parser.py`,
  `src/orchestra/models/ir.py`.
- **Tests**: `test_equals_quoted_string_emits_repr`,
  `test_less_quoted_leading_zero_is_valid_python`,
  `test_equals_bool_literal_emits_lowercase_string` in
  `tests/unit/test_expression_parser.py`.
- **Commit**: `2ff3c10`.

### C-35 — Anchor _ITEM_FIELD_RE for chained item().a.b lowering (P1)

- **Rationale**: CF4-004 — the unanchored `_ITEM_FIELD_RE` matched the
  first segment of `item().condition.name` and returned
  `{{input.condition}}`, silently dropping `.name`.
- **Files**: `src/orchestra/parser/expression_parser.py`.
- **Tests**: `test_item_field_multi_segment_lowers_to_notebook_code`
  in `tests/unit/test_expression_parser.py`.
- **Commit**: `2177a11`.

### C-36 — Preserve hours/minutes/weekDays on periodic schedules (P1)

- **Rationale**: SCHED4-001 — `_recurrence_to_periodic` dropped
  `schedule.minutes/hours/weekDays/monthDays` for Day/Week/Month with
  interval > 1, so a schedule declaring "every 3 days at 02:00 UTC"
  silently fired at midnight.
- **Files**: `src/orchestra/translator/engine.py`,
  `src/orchestra/preparer/workflow_preparer.py`.
- **Tests**: Extended
  `test_schedule_trigger_interval_3_days_emits_periodic` in
  `tests/unit/test_translators.py`;
  `TestManualScheduleTimeOfDaySetupTask` in
  `tests/unit/test_preparers.py`.
- **Commit**: `37845f6`.

### C-37 — File-Lookup: unwrap expression-dict folder/file + abfss URL rewrite (P0)

- **Rationale**: Merged LSC4-001 + LSC4-003.  LSC4-001 (P0) — folder_path
  / file_name shipped as raw expression dicts; `.strip('/')` crashed the
  bundler with AttributeError, taking down 4 pipelines.  LSC4-003 (P1) —
  AzureBlobFS https URLs joined to folder/filename produce notebooks
  that can't read the source on a Databricks cluster.
- **Files**: `src/orchestra/translator/activity_translators/lookup.py`,
  `src/orchestra/preparer/code_generator.py`.
- **Tests**: `test_file_lookup_coerces_expression_dict_path_components`,
  `test_file_lookup_rewrites_https_to_abfss` in
  `tests/unit/test_code_generator.py`.
- **Commit**: `a9e2113`.

### C-38 — Web activity notebook threads resolved Key Vault scope/key (P0)

- **Rationale**: LSC4-002 — `generate_web_activity_notebook` hard-coded
  `scope=task_key, key='auth-credential'` regardless of what the C-11
  preparer resolved.  11 generated notebooks across multiple pipelines
  read the wrong secret at runtime even though the C-11 SecretInstruction
  carried the real values.
- **Files**: `src/orchestra/preparer/code_generator.py`,
  `src/orchestra/preparer/activity_preparers/web_activity.py`.
- **Tests**: Extended
  `test_prepare_web_activity_key_vault_secret_uses_vault_scope_and_secret_name`
  in `tests/unit/test_preparers.py` to assert the rendered notebook
  contains the resolved scope and key.
- **Commit**: `72f6211`.

### C-39 — Emit manual_credential SetupTask for MSI / CredentialReference cluster auth (P1)

- **Rationale**: LSC4-004 — every default cluster ships
  `single_user_name: ${workspace.current_user.userName}` regardless of
  source ADF authentication.  MSI / CredentialReference workloads now
  silently run as the deploying human user with no warning.
- **Files**: `src/orchestra/translator/engine.py`,
  `src/orchestra/preparer/workflow_preparer.py`,
  `src/orchestra/bundler/prereqs_writer.py` (rendering, landed in C-28
  commit alongside the dispatch-stub SETUP.md surface),
  `src/orchestra/bundler/dab_writer.py` (config aggregation, landed in
  C-28 commit).
- **Tests**: `TestManualCredentialFromMsiLinkedService` in
  `tests/unit/test_bundler.py`.
- **Commit**: `d8c99a6`.

## Summary (iteration 4)

- 11 new commits on `fix-0603` (C-28..C-39, with C-28+C-30 folded into a
  single commit since they share NotebookActivity IR fields and the
  preparer infrastructure).
- 616 unit tests pass after the final iteration-4 commit (iteration-3
  baseline: 595 — net 21 new tests).
- No tests broken; no `--no-verify` or `--amend` used.
- All 12 P0/P1 plan items implemented end-to-end.  P2 items
  (NB-ITER4-004, SCHED4-002) intentionally excluded per scoping rules.

## Iteration 5 — 2026-06-04

Implemented all 8 P0/P1 gaps plus the trivially-related P2 (CF5-002, folded
into C-43) across 8 commits (C-40..C-47).  C-40..C-42 were committed in an
earlier pass; C-43..C-47 complete the iteration.

### C-40 — Mine num_workers into the default job_cluster instead of hardcoding 1 (P1)

- **Rationale**: `_infer_bundle_cluster_extras` omitted `num_workers` and
  `_build_default_cluster` hardcoded `num_workers: 1`, even though
  `workflow_preparer` stores the full cluster dict (with num_workers) into
  `cluster_hints` and the IR carries num_workers != 1 for 122 tasks across
  40 pipelines.  ADF clusters with 2-4 workers deployed as 1-worker
  clusters with no warning.
- **Files**: `src/orchestra/bundler/dab_writer.py`.
- **Tests**: num_workers=2 cluster_hint -> emitted default_cluster
  new_cluster.num_workers == 2, in `tests/unit/test_bundler.py`.
- **Commit**: `038888b`.

### C-41 — IfCondition on a literal-seeded Boolean variable emits right:'false' not '0' (P1)

- **Rationale**: `_operand_is_known_boolean` only checked
  `get_variable_dab_ref`, which reads `variable_value_cache`; that cache is
  populated only when value_kind == 'dab_ref', so default-valued Boolean
  variables seeded via `_build_variable_init_activities` were never
  recognized as Boolean.  The fallback emitted NOT_EQUAL(left, '0'), always
  true for a 'true'/'false' string, making the false branch dead code.
- **Files**: `src/orchestra/models/ir.py`,
  `src/orchestra/translator/engine.py`,
  `src/orchestra/translator/activity_translators/if_condition.py`.
- **Tests**: `test_translate_if_condition_boolean_variable_by_declared_type`
  in `tests/unit/test_translators.py`.
- **Commit**: `db2a7fb`.

### C-42 — Resolve Set Pipeline Return Value list-of-pairs inner expression (P1)

- **Rationale**: `set_variable.py` recognized only str and
  `{type:Expression}` shapes; a `pipelineReturnValue` value that is a list
  of `{key, value:{type:Expression,content:...}}` pairs failed
  `_is_adf_expression` and fell to `str(value_raw)`, then the bundler
  blanked it to ''.  The inner `@variables('executionOutputs')` is
  resolvable in the same IR, so the ref is droppable rather than lost.
- **Files**: `src/orchestra/translator/activity_translators/set_variable.py`.
- **Tests**: list-of-pairs value with a resolvable inner `@variables()` ref
  asserts value_kind == 'dab_ref', in `tests/unit/test_translators.py`.
- **Commit**: `ec8ca13`.

### C-43 — Inner-ForEach IfCondition bridges locally; bundler warns when blanking a condition (P1, folds CF5-002)

- **Rationale**: For an inner IfCondition whose operand resolves to a
  parent-job task value (`{{tasks._init_continue.values.continue}}`), the
  init task lives only in the parent job.  When the ForEach body split into
  an inner job, `_strip_dangling_task_value_refs` silently blanked
  `condition_task.left/right` to '', making NOT_EQUAL('','0') always TRUE
  and running the true branch unconditionally with no SETUP.md signal.
  if_condition now recomputes a known-Boolean operand locally via a
  BridgeRequest (mirroring the Switch path) when a seeded literal default is
  available.  `_strip_dangling_task_value_refs` now returns the
  (task_key, field, original_ref) tuples it blanks; `write_bundle` threads
  them into the Prereqs so SETUP.md gets a 'Conditions neutralized to
  always-true' section (folds CF5-002: a predicate is never neutralized
  silently).
- **Files**:
  `src/orchestra/translator/activity_translators/if_condition.py`,
  `src/orchestra/translator/engine.py`, `src/orchestra/models/ir.py`,
  `src/orchestra/bundler/dab_writer.py`,
  `src/orchestra/bundler/prereqs_writer.py`.
- **Tests**:
  `test_translate_if_condition_boolean_variable_bridges_when_default_literal_known`
  in `tests/unit/test_translators.py`;
  extended `test_strips_dangling_condition_task_operands` plus
  `test_neutralized_condition_renders_setup_section` in
  `tests/unit/test_bundler.py`.
- **Commit**: `36158e5`.

### C-44 — Cron derives hour/minute from startTime when ScheduleTrigger has no schedule.hours/minutes (P1)

- **Rationale**: `_recurrence_to_quartz_cron` read only schedule.minutes/
  hours and fell back to '0'/'0'; startTime was never read.  A daily
  trigger with startTime 21:00 UTC and no schedule block emitted
  '0 0 0 * * ?' (midnight), a 21-hour offset, silently.  Per ADF docs the
  first-execution time (from startTime) is the default time-of-day.
- **Files**: `src/orchestra/translator/engine.py`.
- **Tests**: `test_schedule_trigger_derives_time_of_day_from_start_time`
  (Day recurrence, startTime '2023-03-15T21:00:00Z' -> '0 0 21 * * ?') in
  `tests/unit/test_translators.py`.
- **Commit**: `6ec5c37`.

### C-45 — Month-frequency periodic trigger no longer emits the invalid DAB unit MONTHS (P1)

- **Rationale**: `_recurrence_to_periodic` mapped Month -> MONTHS, but the
  Databricks Jobs API PeriodicTriggerConfigurationTimeUnit enum only
  defines DAYS, HOURS, WEEKS — MONTHS is rejected by bundle validate/
  deploy.  Drop Month from the unit_map (single-month routes to quartz cron
  via monthDays); an interval > 1 Month emits a manual_setup schedule note.
- **Files**: `src/orchestra/translator/engine.py`.
- **Tests**:
  `test_schedule_trigger_interval_2_months_does_not_emit_months_unit` in
  `tests/unit/test_translators.py`.
- **Commit**: `5b50b1c`.

### C-46 — Generate create_secrets.py against the Databricks SDK, not dbutils.secrets writes (P1)

- **Rationale**: `setup_generator` emitted `dbutils.secrets.createScope` and
  `dbutils.secrets.put`.  The `dbutils.secrets` submodule is read-only
  (get / getBytes / list / listScopes only); both calls raise
  AttributeError on the first cell.  Generate against
  `WorkspaceClient().secrets.create_scope` (RESOURCE_ALREADY_EXISTS
  try/except) and `w.secrets.put_secret`.
- **Files**: `src/orchestra/bundler/setup_generator.py`,
  `tests/unit/test_bundler.py`.
- **Tests**: updated the two `createScope`/`put` asserts to
  `create_scope`/`put_secret` (plus WorkspaceClient + negative asserts) in
  `test_secrets_setup_notebook_content` and the write_bundle round-trip
  test in `tests/unit/test_bundler.py`.
- **Commit**: `acdbb40`.

### C-47 — File-source Lookup substitutes dataset() parameter refs before baking the abfss:// path (P1)

- **Rationale**: `lookup.py` read the dataset reference (whose parameters
  bind digitalCase/fileName to pipeline params) but never applied them.
  `_unwrap_expression` only unwrapped the expression dict, leaving
  folderPath '@toLower(dataset().digitalCase)' and fileName
  '@dataset().fileName' verbatim; the code generator baked a literal broken
  'abfss://.../@toLower(dataset().digitalCase)/...' default that spark.read
  cannot load.  Build a dataset-parameter scope, substitute dataset().X,
  resolve the result; `_assemble_file_lookup_source_path` drops any leaked
  raw dataset() component as a safety net.
- **Files**:
  `src/orchestra/translator/activity_translators/lookup.py`,
  `src/orchestra/preparer/code_generator.py`.
- **Tests**:
  `test_translate_lookup_substitutes_dataset_parameter_refs` in
  `tests/unit/test_translators.py`.
- **Commit**: `f360a68`.

## Summary (iteration 5)

- 8 commits on `fix-0603` (C-40..C-47); C-40..C-42 landed in an earlier
  pass, C-43..C-47 completed the iteration.  CF5-002 (P2) folded into C-43.
- 624 unit tests pass after the final iteration-5 commit (iteration-4
  baseline: 616 — net 8 new tests).
- No tests broken; no `--no-verify` or `--amend` used.
- All 8 P0/P1 plan items implemented end-to-end.  P2 NB-ITER5-002
  intentionally excluded per the plan's dedup decision.
