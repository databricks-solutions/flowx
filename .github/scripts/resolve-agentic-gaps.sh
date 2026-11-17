#!/usr/bin/env bash
set -euo pipefail
mkdir -p agentic_results
echo "configured=false" >> "${GITHUB_OUTPUT}"
echo "resolved=false" >> "${GITHUB_OUTPUT}"
skip_agentic() {
  echo "::warning::$1"
  echo "- ⚠️ Agentic resolution skipped: $1" >> "${GITHUB_STEP_SUMMARY}"
}
fail_agentic() {
  echo "::error::$1"
  echo "- ❌ Agentic resolution failed: $1" >> "${GITHUB_STEP_SUMMARY}"
  exit 1
}
if [[ -z "${CLAUDE_CODE_OAUTH_TOKEN:-}" && -z "${ANTHROPIC_API_KEY:-}" ]]; then
  skip_agentic "no Claude authentication secret is configured"
  exit 0
fi
echo "configured=true" >> "${GITHUB_OUTPUT}"
[[ -x "${CLAUDE_BIN:-}" ]] || fail_agentic "Claude Code CLI is not installed"
plugin_dir="${RUNNER_TEMP}/flowx-plugin"
mkdir -p "${plugin_dir}"
cp -R .claude-plugin skills "${plugin_dir}/"
chmod -R a-w "${plugin_dir}"
expected_tasks_file="${RUNNER_TEMP}/expected-agentic-tasks.json"
jq '
  def pipelines: if has("pipelines") then .pipelines else [.] end;
  [pipelines[] as $pipeline |
   $pipeline.tasks | .. | objects | select(.type? == "PlaceholderActivity") |
   {pipeline: $pipeline.name, activity_name: .name}] | sort_by(.pipeline, .activity_name)
' "${FLOWX_OUTPUT}/.work/translation_report.json" > "${expected_tasks_file}"
jq '
  def pipelines: if has("pipelines") then .pipelines else [.] end;
  [pipelines[] as $pipeline |
   $pipeline.tasks | .. | objects | select(.type? == "PlaceholderActivity") |
   {pipeline: $pipeline.name, activity_name: .name, task_key, depends_on: (.depends_on // [])}]
' "${FLOWX_OUTPUT}/.work/translation_report.json" > "${RUNNER_TEMP}/expected-agentic-structure.json"
cp "${GITHUB_WORKSPACE}/.github/scripts/snapshot-tree.sh" "${RUNNER_TEMP}/snapshot-worktree.sh"
chmod a-w "${RUNNER_TEMP}/snapshot-worktree.sh"
source "${RUNNER_TEMP}/snapshot-worktree.sh"
worktree_snapshot="${RUNNER_TEMP}/pre-claude-worktree.sha256"
snapshot_worktree > "${worktree_snapshot}"
chmod 000 "${TF_STATE_DIR}" "${TF_DATA_DIR}"
allowed_dirs=("./${FLOWX_OUTPUT}/**" "/${ADF_EXPORT_DIR}/**" "/${plugin_dir}/**" "./agentic_results/**")
allowed_tools=""
for tool in Read Glob Grep; do
  for allowed_dir in "${allowed_dirs[@]}"; do allowed_tools+="${tool}(${allowed_dir}),"; done
done
allowed_tools+="Edit(agentic_results/**)"
denied_reads="Read(//proc/**),Read(~/.azure/**),Read(~/.config/**),Read(~/.docker/**),Read(~/.npmrc)"
denied_reads+=",Read(~/.databrickscfg),Read(./.git/**),Read(tests/integration/azure/terraform/**)"
denied_reads+=",Read(/${TF_STATE_DIR}/**),Read(/${TF_DATA_DIR}/**)"
if ! "${CLAUDE_BIN}" -p \
  --restricted \
  --add-dir "${ADF_EXPORT_DIR}" \
  --add-dir "${plugin_dir}" \
  --plugin-dir "${plugin_dir}" \
  --permission-mode dontAsk \
  --permission-prompts none \
  --tools="Read,Edit,Write,Glob,Grep,Skill" \
  --allowedTools="${allowed_tools}" \
  --disallowedTools="${denied_reads}" <<CLAUDE_PROMPT
Use the flowx:flowx-convert skill, specifically the ADF agentic-gap instructions in
${plugin_dir}/skills/flowx-convert/sources/adf.md steps 4 and 5. Read
${FLOWX_OUTPUT}/.work/translation_report.json, ${FLOWX_OUTPUT}/.work/gaps.json, and the
exported ADF definitions under ${ADF_EXPORT_DIR}. Produce one merge result for each
PlaceholderActivity in translation_report.json. This is the exact expected task list:
$(cat "${expected_tasks_file}")
Use gaps.json only to find the corresponding raw ADF activity definitions.

Write exactly one JSON file per resolved gap under agentic_results/. Each file must be a
single JSON object with this merge-result schema:
{"activity_name":"<exact placeholder activity name>","pipeline":"<exact pipeline name>",
 "task":{"type":"<valid flowx IR task type>","name":"<activity name>",...}}
Preserve the exact activity and pipeline names from the report. The task must be a complete,
valid replacement for that placeholder; task_key and depends_on may be omitted so merge keeps
the placeholder values. For a NotebookActivity, include its complete source in the non-empty
generated_source field; do not create a separate notebook file.
Do not write outside agentic_results/, deploy anything, or access
unrelated resources. Do not run shell commands; the workflow validates and merges the files.
CLAUDE_PROMPT
then
  fail_agentic "Claude Code did not complete successfully"
fi
if find agentic_results -type l -print -quit | grep -q .; then
  fail_agentic "Claude produced a symlink under agentic_results/"
fi
result_files=(agentic_results/*.json)
[[ -e "${result_files[0]}" ]] || fail_agentic "Claude produced no merge-result JSON files"
for result_file in "${result_files[@]}"; do
  jq -e '
    type == "object" and
    (.activity_name | (type == "string" and length > 0)) and
    (.pipeline | (type == "string" and length > 0)) and
    (.task | type == "object") and
    (.task.type | (type == "string" and length > 0)) and
    (.task.name | (type == "string" and length > 0)) and
    (if .task.type == "NotebookActivity" then
       (.task.generated_source | (type == "string" and length > 0))
     else true end)
  ' "${result_file}" >/dev/null || fail_agentic "invalid merge-result schema: ${result_file}"
done
expected_results="$(jq -c '[.[] | [.pipeline, .activity_name]] | sort' "${expected_tasks_file}")"
actual_results="$(jq -cs '[.[] | [.pipeline, .activity_name]] | sort' "${result_files[@]}")"
[[ "${actual_results}" == "${expected_results}" ]] ||
  fail_agentic "merge-result files do not cover every pending agentic placeholder exactly once"
echo "resolved=true" >> "${GITHUB_OUTPUT}"
echo "- ✅ Claude produced schema-valid agentic merge results." >> "${GITHUB_STEP_SUMMARY}"
