"""Offline safety checks for the Azure/GitHub setup script."""

from __future__ import annotations

import json
import os
import re
import shutil
import subprocess
from pathlib import Path

import pytest

SCRIPT = Path(__file__).parents[2] / ".github" / "scripts" / "setup-integration.sh"
EXPORT_SCRIPT = SCRIPT.with_name("export-adf.sh")
APPLY_SCRIPT = SCRIPT.with_name("apply-terraform.sh")
TEARDOWN_SCRIPT = SCRIPT.with_name("teardown-integration.sh")
TERRAFORM_DIR = Path(__file__).parent / "azure" / "terraform"
TERRAFORM = TERRAFORM_DIR / "main.tf"
README = TERRAFORM_DIR.parent / "README.md"
BASH = Path("/bin/bash")
SECRET = "must-never-appear-in-output"
RESOURCE_GROUP_SCOPE = "/subscriptions/00000000-0000-0000-0000-000000000001/resourceGroups/flowx-live-integration"
SUBCOMMANDS = ("setup", "verify", "cleanup", "identity", "providers", "role", "environment", "secrets")
SENSITIVE_ENV = {
    "CLAUDE_CODE_OAUTH_TOKEN",
    "ANTHROPIC_API_KEY",
    "DATABRICKS_HOST",
    "DATABRICKS_TOKEN",
    "RESOURCE_OWNER_EMAIL",
}


@pytest.fixture
def stub_path(tmp_path: Path) -> tuple[Path, Path]:
    """Provide deterministic CLIs that reject unexpected writes."""
    jq = shutil.which("jq")
    if jq is None:
        pytest.skip("jq is required for the setup-script tests")
    binary_dir = tmp_path / "bin"
    binary_dir.mkdir()
    mutation_log = tmp_path / "mutations"
    az_stub = binary_dir / "az"
    az_stub.write_text(
        """#!/usr/bin/env bash
set -euo pipefail
case "$*" in
  "account show "*"--query name"*) echo "Flowx Test Subscription" ;;
  "account show"*"--query id"*) echo "00000000-0000-0000-0000-000000000001" ;;
  "account show"*"--query tenantId"*) echo "00000000-0000-0000-0000-000000000002" ;;
  "account show"*) echo '{}' ;;
  *"ad app list"*) [[ "${STUB_FRESH:-}" == 1 ]] || echo "00000000-0000-0000-0000-000000000003" ;;
  *"ad sp list"*)
    [[ "${STUB_FRESH:-}" == 1 || "${STUB_NO_SP:-}" == 1 ]] ||
      echo "00000000-0000-0000-0000-000000000004" ;;
  *"federated-credential list"*) echo '{"id":"credential-id","name":"existing-name",
    "issuer":"https://token.actions.githubusercontent.com",
    "audiences":["api://AzureADTokenExchange"]}' ;;
  *"provider show"*) if [[ "${STUB_FRESH:-}" == 1 ]]; then echo "NotRegistered"; else echo "Registered"; fi ;;
  *"group show"*) [[ "${STUB_FRESH:-}" != 1 ]] || exit 3 ;;
  *"role definition list"*"-o json"*)
    actions=$STUB_ROLE_ACTIONS
    [[ "${STUB_ROLE_MISMATCH:-}" != 1 ]] || actions='["Microsoft.Resources/subscriptions/read"]'
    printf '[{"assignableScopes":["%s"],
      "permissions":[{"actions":%s,"notActions":%s,"dataActions":[],"notDataActions":[]}],
      "roleName":"flowx-live-integration-deployer","id":"role-id"}]\n' \
      "$STUB_ROLE_SCOPE" "$actions" "$STUB_ROLE_NOT_ACTIONS" ;;
  *"role definition list"*) [[ "${STUB_FRESH:-}" == 1 ]] || echo "role-id" ;;
  *"role assignment list"*)
    [[ "${STUB_NO_SP:-}" != 1 ]] || { echo 'placeholder assignment lookup' >> "$MUTATION_LOG"; exit 88; }
    if [[ "$*" == *"[?scope=="* ]]; then [[ "${STUB_LEGACY_ASSIGNMENT:-}" != 1 ]] || echo "legacy-assignment-id"
    else echo "assignment-id"; fi ;;
  *"role definition update"*)
    if [[ "${ALLOW_ROLE_UPDATE:-}" == 1 ]]; then
      previous=''
      for argument in "$@"; do
        if [[ "$previous" == --role-definition ]]; then cat "$argument" > "$ROLE_JSON_LOG"; fi
        previous=$argument
      done
    else echo "$*" >> "$MUTATION_LOG"; exit 91; fi ;;
  *"resource list"*"--tag purpose=flowx-integration-test"*) printf 'fxorphan0001\nfxorphan0001\n' ;;
  *"resource list"*"--tag run=fxorphan0001"*)
    echo "/subscriptions/s/resourceGroups/flowx-live-integration/providers/x/orphan" ;;
  *" create"*|*" update"*|*" register"*|*" delete"*|*" account set"*)
    echo "$*" >> "$MUTATION_LOG"; exit 91 ;;
esac
""",
        encoding="utf-8",
    )
    gh_stub = binary_dir / "gh"
    gh_stub.write_text(
        """#!/usr/bin/env bash
set -euo pipefail
case "$*" in
  "auth status") exit 0 ;;
  *"repo view"*) echo "databricks-solutions/flowx" ;;
  *"api users/"*) echo "12345" ;;
  *"--method PUT"*)
    if [[ "${ALLOW_ENVIRONMENT_WRITE:-}" == 1 ]]; then
      previous=''
      for argument in "$@"; do
        if [[ "$previous" == --input ]]; then cat "$argument" > "$ENVIRONMENT_JSON_LOG"; fi
        previous=$argument
      done
    else echo "$*" >> "$MUTATION_LOG"; exit 91; fi ;;
  *"api repos/"*"/environments/"*)
    reviewers='{"type":"User","reviewer":{"login":"flowx-reviewer","id":12345}}'
    if [[ "${STUB_DUPLICATE_REVIEWER:-}" == 1 ]]; then reviewers+=",$reviewers"
    elif [[ "${STUB_ONE_REVIEWER:-}" != 1 ]]; then
      reviewers+=',{"type":"User","reviewer":{"login":"second","id":67890}}'
    fi
    if [[ "${STUB_PREVENT_SELF_REVIEW:-}" == 1 ]]; then self_review=true; else self_review=false; fi
    printf '{"protection_rules":[{"type":"required_reviewers","prevent_self_review":%s,"reviewers":[%s]}]}\n' \
      "$self_review" "$reviewers" ;;
  *"secret list"*)
    printf 'AZURE_CLIENT_ID\nAZURE_TENANT_ID\nAZURE_SUBSCRIPTION_ID\nAZURE_RESOURCE_GROUP\n'
    [[ "${STUB_MISSING_OWNER:-}" == 1 ]] || printf 'RESOURCE_OWNER_EMAIL\n' ;;
  *"secret set"*)
    if [[ "${ALLOW_SECRET_WRITE:-}" == 1 ]]; then
      printf '%s\n' "$*" >> "$GH_ARGV_LOG"
      input=$(cat)
      printf '%s\n' "$input" >> "$GH_STDIN_LOG"
    else echo "$*" >> "$MUTATION_LOG"; exit 91; fi ;;
esac
""",
        encoding="utf-8",
    )
    jq_stub = binary_dir / "jq"
    jq_stub.write_text(f'#!/usr/bin/env bash\nexec "{jq}" "$@"\n', encoding="utf-8")
    for path in (az_stub, gh_stub, jq_stub):
        path.chmod(0o755)
    return binary_dir, mutation_log


@pytest.mark.parametrize("subcommand", SUBCOMMANDS)
def test_every_subcommand_dry_run_is_offline_and_secret_safe(subcommand: str, stub_path: tuple[Path, Path]) -> None:
    """Every subcommand succeeds without dispatching writes or leaking secrets."""
    binary_dir, mutation_log = stub_path
    environment = _clean_environment(binary_dir, mutation_log)
    environment["ANTHROPIC_API_KEY"] = SECRET
    result = _run_script([subcommand, "--dry-run", "--yes"], environment)
    output = result.stdout + result.stderr
    assert result.returncode == 0, output
    assert SECRET not in output
    assert not mutation_log.exists(), mutation_log.read_text() if mutation_log.exists() else ""


def test_fresh_tenant_setup_dry_run_skips_dependent_lookups(stub_path: tuple[Path, Path]) -> None:
    """A dry-run describes the complete setup when no Azure objects exist."""
    binary_dir, mutation_log = stub_path
    environment = _clean_environment(binary_dir, mutation_log)
    environment["STUB_FRESH"] = "1"
    result = _run_script(["setup", "--dry-run", "--yes"], environment)
    assert result.returncode == 0, result.stdout + result.stderr
    assert "client-id" in result.stdout
    assert "role-definition-id" in result.stdout
    assert "Microsoft.EventGrid" in result.stdout
    assert not mutation_log.exists()


def test_role_dry_run_skips_assignment_lookup_for_missing_sp(stub_path: tuple[Path, Path]) -> None:
    """A missing SP placeholder is never sent to the assignment-list API."""
    binary_dir, mutation_log = stub_path
    environment = _clean_environment(binary_dir, mutation_log)
    environment["STUB_NO_SP"] = "1"
    result = _run_script(["role", "--dry-run", "--yes"], environment)
    assert result.returncode == 0, result.stdout + result.stderr
    assert "service-principal-object-id" in result.stdout
    assert not mutation_log.exists()


def test_verify_reports_mismatched_role_actions_clearly(stub_path: tuple[Path, Path]) -> None:
    """A real nested role response with wrong actions fails without a raw jq error."""
    binary_dir, mutation_log = stub_path
    environment = _clean_environment(binary_dir, mutation_log)
    environment["STUB_ROLE_MISMATCH"] = "1"
    result = _run_script(["verify"], environment)
    output = result.stdout + result.stderr
    assert result.returncode != 0
    assert "MISSING/MISMATCH: custom role definition" in output
    assert "jq: error" not in output


def test_role_is_scoped_to_one_resource_group_with_specific_actions(
    stub_path: tuple[Path, Path], tmp_path: Path
) -> None:
    """The updated role lists exact actions, blocks key access, and is assignable only in the shared group."""
    binary_dir, mutation_log = stub_path
    role_json_log = tmp_path / "role.json"
    environment = _clean_environment(binary_dir, mutation_log)
    environment.update({"ALLOW_ROLE_UPDATE": "1", "ROLE_JSON_LOG": str(role_json_log), "STUB_ROLE_MISMATCH": "1"})
    result = _run_script(["role", "--yes"], environment)
    assert result.returncode == 0, result.stdout + result.stderr
    role = json.loads(role_json_log.read_text())
    assert role["AssignableScopes"] == [RESOURCE_GROUP_SCOPE]
    assert all(not action.endswith("*") for action in role["Actions"])
    assert not any(
        action.startswith("Microsoft.Resources/subscriptions/resourceGroups/")
        and action != ("Microsoft.Resources/subscriptions/resourceGroups/read")
        for action in role["Actions"]
    )
    assert "Microsoft.EventGrid/eventSubscriptions/write" in role["Actions"]
    assert {
        "Microsoft.DataFactory/factories/querypipelineruns/action",
        "Microsoft.DataFactory/factories/querypipelineruns/read",
        "Microsoft.DataFactory/factories/pipelineruns/queryactivityruns/action",
        "Microsoft.DataFactory/factories/pipelineruns/queryactivityruns/read",
    } <= set(role["Actions"])
    assert set(role["NotActions"]) == {
        "Microsoft.Storage/storageAccounts/listKeys/action",
        "Microsoft.Storage/storageAccounts/listAccountSas/action",
        "Microsoft.Storage/storageAccounts/regeneratekey/action",
        "Microsoft.KeyVault/vaults/accessPolicies/write",
    }
    assert not mutation_log.exists()


def test_role_dry_run_creates_group_and_assigns_role_only_there(stub_path: tuple[Path, Path]) -> None:
    """A fresh setup creates the shared resource group and assigns the role at that group, not the subscription."""
    binary_dir, mutation_log = stub_path
    environment = _clean_environment(binary_dir, mutation_log)
    environment["STUB_FRESH"] = "1"
    result = _run_script(["role", "--dry-run", "--yes"], environment)
    assert result.returncode == 0, result.stdout + result.stderr
    assert "DRY-RUN: az group create" in result.stdout
    assignment = next(line for line in result.stdout.splitlines() if "az role assignment create" in line)
    assert f"--scope {RESOURCE_GROUP_SCOPE}" in assignment
    assert not mutation_log.exists()


def test_role_removes_the_old_subscription_wide_assignment(stub_path: tuple[Path, Path]) -> None:
    """Upgrading an older setup drops the subscription-wide assignment of the same role."""
    binary_dir, mutation_log = stub_path
    environment = _clean_environment(binary_dir, mutation_log)
    environment["STUB_LEGACY_ASSIGNMENT"] = "1"
    result = _run_script(["role", "--dry-run", "--yes"], environment)
    assert result.returncode == 0, result.stdout + result.stderr
    assert "DRY-RUN: az role assignment delete --ids legacy-assignment-id" in result.stdout
    assert not mutation_log.exists()


def test_verify_fails_while_the_subscription_wide_assignment_remains(stub_path: tuple[Path, Path]) -> None:
    """Verification doesn't pass while the role still reaches the whole subscription."""
    binary_dir, mutation_log = stub_path
    environment = _clean_environment(binary_dir, mutation_log)
    environment["STUB_LEGACY_ASSIGNMENT"] = "1"
    result = _run_script(["verify"], environment)
    assert result.returncode != 0
    assert "still assigned to the whole subscription" in result.stderr


def test_verify_passes_for_a_correct_setup(stub_path: tuple[Path, Path]) -> None:
    """The stubs describe a correct setup, so the negative verify tests fail for the reason they name."""
    binary_dir, mutation_log = stub_path
    result = _run_script(["verify"], _clean_environment(binary_dir, mutation_log))
    assert result.returncode == 0, result.stdout + result.stderr


@pytest.mark.parametrize("stub", ["STUB_SELF_REVIEW", "STUB_PREVENT_SELF_REVIEW"])
def test_verify_accepts_either_self_review_setting(stub_path: tuple[Path, Path], stub: str) -> None:
    """Verify doesn't check prevent_self_review, so it passes whether self-review is allowed or not."""
    binary_dir, mutation_log = stub_path
    environment = _clean_environment(binary_dir, mutation_log)
    environment[stub] = "1"
    result = _run_script(["verify"], environment)
    assert result.returncode == 0, result.stdout + result.stderr


def test_environment_allows_self_review(stub_path: tuple[Path, Path], tmp_path: Path) -> None:
    """The environment lets a listed reviewer approve their own run, which is an accepted risk."""
    binary_dir, mutation_log = stub_path
    environment_json_log = tmp_path / "environment.json"
    environment = _clean_environment(binary_dir, mutation_log)
    environment.update({"ALLOW_ENVIRONMENT_WRITE": "1", "ENVIRONMENT_JSON_LOG": str(environment_json_log)})
    result = _run_script(["environment", "--yes"], environment)
    assert result.returncode == 0, result.stdout + result.stderr
    settings = json.loads(environment_json_log.read_text())
    assert settings["prevent_self_review"] is False
    assert len(settings["reviewers"]) == 2
    assert not mutation_log.exists()


@pytest.mark.parametrize("reviewers", ["", "only-one", "same,SAME", "one, ,"])
def test_setup_requires_two_distinct_reviewers(stub_path: tuple[Path, Path], reviewers: str) -> None:
    """Fewer than two distinct reviewers is rejected before any lookup or write."""
    binary_dir, mutation_log = stub_path
    environment = _clean_environment(binary_dir, mutation_log)
    environment["REVIEWERS"] = reviewers
    result = _run_script(["setup", "--dry-run", "--yes"], environment)
    assert result.returncode != 0
    assert "at least 2 GitHub users" in result.stderr
    assert not mutation_log.exists()


@pytest.mark.parametrize(
    ("stub", "message"),
    [
        ("STUB_ONE_REVIEWER", "environment needs at least 2 required reviewers"),
        ("STUB_DUPLICATE_REVIEWER", "environment needs at least 2 required reviewers"),
    ],
)
def test_verify_rejects_too_few_reviewers(stub_path: tuple[Path, Path], stub: str, message: str) -> None:
    """Verification fails unless the environment has at least two distinct required reviewers."""
    binary_dir, mutation_log = stub_path
    environment = _clean_environment(binary_dir, mutation_log)
    environment[stub] = "1"
    result = _run_script(["verify"], environment)
    assert result.returncode != 0
    assert message in result.stderr


def test_verify_requires_owner_secret_name(stub_path: tuple[Path, Path]) -> None:
    """Verification reports the missing required owner secret by name only."""
    binary_dir, mutation_log = stub_path
    environment = _clean_environment(binary_dir, mutation_log)
    environment["STUB_MISSING_OWNER"] = "1"
    result = _run_script(["verify"], environment)
    output = result.stdout + result.stderr
    assert result.returncode != 0
    assert "MISSING: environment secret RESOURCE_OWNER_EMAIL" in output
    assert "owner@example.invalid" not in output


def test_github_secret_value_uses_stdin_only(stub_path: tuple[Path, Path], tmp_path: Path) -> None:
    """The real secrets path omits --body and keeps secret values out of argv and output."""
    binary_dir, mutation_log = stub_path
    argv_log = tmp_path / "gh-argv"
    stdin_log = tmp_path / "gh-stdin"
    environment = _clean_environment(binary_dir, mutation_log)
    environment.update(
        {
            "GH_ARGV_LOG": str(argv_log),
            "GH_STDIN_LOG": str(stdin_log),
            "ALLOW_SECRET_WRITE": "1",
            "ANTHROPIC_API_KEY": SECRET,
        }
    )
    result = _run_script(["secrets", "--yes"], environment)
    output = result.stdout + result.stderr
    assert result.returncode == 0, output
    assert SECRET in stdin_log.read_text()
    assert SECRET not in argv_log.read_text()
    assert "--body" not in argv_log.read_text()
    assert SECRET not in output
    assert "owner@example.invalid" in stdin_log.read_text()
    assert "owner@example.invalid" not in argv_log.read_text()
    assert "owner@example.invalid" not in output


def test_secrets_rejects_malformed_owner_email(stub_path: tuple[Path, Path]) -> None:
    """The required owner value is validated before any secret write."""
    binary_dir, mutation_log = stub_path
    environment = _clean_environment(binary_dir, mutation_log)
    environment["RESOURCE_OWNER_EMAIL"] = "not-an-email"
    result = _run_script(["secrets", "--yes"], environment)
    assert result.returncode != 0
    assert "must be a non-empty email-like value" in result.stderr
    assert "not-an-email" not in result.stdout + result.stderr
    assert not mutation_log.exists()


def test_setup_requires_owner_email_before_any_lookup_or_write(stub_path: tuple[Path, Path]) -> None:
    """Non-interactive full setup validates the owner before step one."""
    binary_dir, mutation_log = stub_path
    environment = _clean_environment(binary_dir, mutation_log)
    environment.pop("RESOURCE_OWNER_EMAIL")
    result = _run_script(["setup", "--dry-run", "--yes"], environment)
    assert result.returncode != 0
    assert "RESOURCE_OWNER_EMAIL is required before setup can begin" in result.stderr
    assert not mutation_log.exists()


def test_cleanup_without_delete_only_lists(stub_path: tuple[Path, Path]) -> None:
    """Cleanup without --delete lists each leftover run once and changes nothing."""
    binary_dir, mutation_log = stub_path
    result = _run_script(["cleanup", "--yes"], _clean_environment(binary_dir, mutation_log))
    assert result.returncode == 0, result.stdout + result.stderr
    assert result.stdout.splitlines().count("fxorphan0001") == 1
    assert not mutation_log.exists()


def test_cleanup_deletes_one_runs_resources_but_never_the_group(stub_path: tuple[Path, Path]) -> None:
    """Deleting a run removes that run's resources; the shared resource group is never deleted."""
    binary_dir, mutation_log = stub_path
    environment = _clean_environment(binary_dir, mutation_log)
    result = _run_script(["cleanup", "--delete", "fxorphan0001", "--dry-run", "--yes"], environment)
    assert result.returncode == 0, result.stdout + result.stderr
    assert "DRY-RUN: az resource delete --ids /subscriptions/s/resourceGroups/flowx-live-integration/" in result.stdout
    assert "group delete" not in result.stdout
    refused = _run_script(["cleanup", "--delete", "fxother", "--dry-run", "--yes"], environment)
    assert refused.returncode != 0
    assert "not an exact run tag" in refused.stderr


def test_teardown_destroys_run_resources_and_keeps_the_resource_group(tmp_path: Path) -> None:
    """Teardown runs terraform destroy and makes no Azure CLI call that could delete the shared group."""
    state = tmp_path / "state" / "terraform.tfstate"
    state.parent.mkdir()
    state.write_text("{}", encoding="utf-8")
    result, calls = _run_teardown(tmp_path)
    assert result.returncode == 0, result.stderr
    assert calls == ["terraform destroy -auto-approve -input=false"]


def test_teardown_skips_when_apply_never_ran(tmp_path: Path) -> None:
    """With no state, as when init fails, teardown succeeds without calling Terraform or Azure."""
    result, calls = _run_teardown(tmp_path)
    assert result.returncode == 0, result.stderr
    assert "nothing to destroy" in result.stdout
    assert calls == []


def test_provider_lock_covers_every_ci_platform() -> None:
    """Each provider has one h1 hash per platform in the README's lock command, including the Linux runner.

    A lock made on one machine holds a single h1 hash per provider, and init on any other platform then fails.
    """
    lock = (TERRAFORM_DIR / ".terraform.lock.hcl").read_text(encoding="utf-8")
    providers = re.findall(r'^provider "(?P<name>[^"]+)" \{\n(?P<body>.*?)^\}', lock, re.MULTILINE | re.DOTALL)
    assert {name for name, _ in providers} == {
        "registry.terraform.io/azure/azapi",
        "registry.terraform.io/hashicorp/azurerm",
    }
    platforms = re.search(r"terraform providers lock((?: -platform=\w+)+)", README.read_text(encoding="utf-8"))
    assert platforms and "-platform=linux_amd64" in platforms.group(1)
    platform_count = platforms.group(1).count("-platform=")
    for name, body in providers:
        assert body.count('"h1:') == platform_count, name


@pytest.mark.parametrize(
    ("actions", "applied"),
    [(["create"], True), (["no-op"], True), (["update"], True), (["delete"], False), (["delete", "create"], False)],
)
def test_apply_stops_when_the_plan_deletes_anything(tmp_path: Path, actions: list[str], applied: bool) -> None:
    """Only a saved plan with no deletes or replacements is applied, and it is that exact plan."""
    result, calls = _run_apply(tmp_path, TERRAFORM_DIR, actions)
    plan = f"{tmp_path / 'state'}/tfplan"
    if applied:
        assert result.returncode == 0, result.stdout + result.stderr
        assert calls[-1] == f"apply -input=false {plan}"
    else:
        assert result.returncode != 0
        assert "would delete or replace" in result.stdout
        assert "azurerm_storage_account.test" in result.stdout
        assert not any(call.startswith("apply") for call in calls)
    assert f"plan -input=false -out={plan}" in calls
    assert any("-lockfile=readonly" in call for call in calls if call.startswith("init"))


COMMAND_RUNNING_CONFIG = {
    "local-exec": (
        "main.tf",
        'resource "terraform_data" "run" {\n  provisioner "local-exec" {\n    command = "id"\n  }\n}',
    ),
    "remote-exec": (
        "main.tf",
        'resource "null_resource" "run" {\n  provisioner "remote-exec" {\n    inline = ["id"]\n  }\n}',
    ),
    "external": ("main.tf", 'data "external" "run" {\n  program = ["id"]\n}'),
    "hcl-comment": ("main.tf", 'resource "terraform_data" "run" {\n  provisioner /* hidden */ "local-exec" {\n  }\n}'),
    "hcl-bare-label": ("main.tf", 'resource "terraform_data" "run" {\n  provisioner local-exec {\n  }\n}'),
    "hcl-split-data": ("main.tf", 'data /* hidden */ external "run" {\n  program = ["id"]\n}'),
    "json": ("main.tf.json", '{"resource": {"terraform_data": {"run": {"provisioner": [{"local-exec": {}}]}}}}'),
    "json-multiline": (
        "main.tf.json",
        '{"resource": {"terraform_data": {"run": {"provisioner"\n  :\n [{"local-exec": {}}]}}}}',
    ),
    "json-escaped": (
        "main.tf.json",
        '{"resource": {"terraform_data": {"run": {"provisi\\u006fner": [{"local-exec": {}}]}}}}',
    ),
    "json-external": ("main.tf.json", '{"data": {"external"\n: {"run": {"program": ["id"]}}}}'),
}


@pytest.mark.parametrize("location", ["root", "module"])
@pytest.mark.parametrize("case", sorted(COMMAND_RUNNING_CONFIG))
def test_apply_rejects_config_that_runs_commands(tmp_path: Path, case: str, location: str) -> None:
    """Provisioners and the external data source are refused however they're laid out, in the root or a module."""
    filename, content = COMMAND_RUNNING_CONFIG[case]
    config = tmp_path / "config"
    config.mkdir()
    target = config if location == "root" else tmp_path / "outside-module"
    target.mkdir(exist_ok=True)
    (target / filename).write_text(content + "\n", encoding="utf-8")
    modules = {"Modules": [{"Key": "", "Source": "", "Dir": "."}, {"Key": "m", "Dir": str(target)}]}
    result, calls = _run_apply(tmp_path, config, ["create"], {"STUB_MODULES_JSON": json.dumps(modules)})
    assert result.returncode != 0
    assert "must not use provisioners" in result.stdout
    assert [call.split()[0] for call in calls] == ["init"]


@pytest.mark.parametrize("link", ["tf-file", "tf-json-file", "module-dir"])
def test_apply_rejects_symlinks_that_could_hide_a_provisioner(tmp_path: Path, link: str) -> None:
    """Terraform follows symlinks the scan would skip, so a symlinked file or module dir is refused."""
    config = tmp_path / "config"
    config.mkdir()
    hidden = tmp_path / "hidden"
    hidden.mkdir()
    (hidden / "main.tf").write_text(COMMAND_RUNNING_CONFIG["local-exec"][1] + "\n", encoding="utf-8")
    (hidden / "main.tf.json").write_text(COMMAND_RUNNING_CONFIG["json"][1] + "\n", encoding="utf-8")
    modules = {"Modules": [{"Key": "", "Source": "", "Dir": "."}]}
    if link == "tf-file":
        (config / "hidden.tf").symlink_to(hidden / "main.tf")
    elif link == "tf-json-file":
        (config / "hidden.tf.json").symlink_to(hidden / "main.tf.json")
    else:
        (tmp_path / "module-link").symlink_to(hidden, target_is_directory=True)
        modules["Modules"].append({"Key": "m", "Dir": "../module-link"})
    result, calls = _run_apply(tmp_path, config, ["create"], {"STUB_MODULES_JSON": json.dumps(modules)})
    assert result.returncode != 0
    assert "must not contain symlinks" in result.stdout
    assert [call.split()[0] for call in calls] == ["init"]


@pytest.mark.parametrize("module_dir", ["../outside-module", "./../outside-module", ".terraform/../../outside-module"])
def test_apply_scans_every_module_dir_however_its_path_is_written(tmp_path: Path, module_dir: str) -> None:
    """A recorded module outside the root is scanned whatever form its relative path takes."""
    config = tmp_path / "config"
    (config / ".terraform").mkdir(parents=True)
    (tmp_path / "outside-module").mkdir()
    (tmp_path / "outside-module" / "main.tf").write_text(
        COMMAND_RUNNING_CONFIG["local-exec"][1] + "\n", encoding="utf-8"
    )
    modules = {"Modules": [{"Key": "", "Source": "", "Dir": "."}, {"Key": "m", "Dir": module_dir}]}
    result, calls = _run_apply(tmp_path, config, ["create"], {"STUB_MODULES_JSON": json.dumps(modules)})
    assert result.returncode != 0
    assert "must not use provisioners" in result.stdout
    assert [call.split()[0] for call in calls] == ["init"]


def test_apply_fails_when_grep_cannot_scan_a_file(tmp_path: Path) -> None:
    """A grep error is not read as "no match", so a file that can't be scanned stops the run."""
    config = tmp_path / "config"
    config.mkdir()
    (config / "main.tf").write_text('resource "terraform_data" "ok" {}\n', encoding="utf-8")
    grep_dir = tmp_path / "grep-bin"
    grep_dir.mkdir()
    (grep_dir / "grep").write_text("#!/usr/bin/env bash\nexit 2\n", encoding="utf-8")
    (grep_dir / "grep").chmod(0o755)
    path = f"{grep_dir}:{tmp_path / 'bin'}:{os.environ['PATH']}"
    result, calls = _run_apply(tmp_path, config, ["create"], {"PATH": path})
    assert result.returncode != 0
    assert "Could not scan ./main.tf" in result.stdout
    assert [call.split()[0] for call in calls] == ["init"]


def test_apply_fails_when_a_module_dir_cannot_be_listed(tmp_path: Path) -> None:
    """A module dir that can't be listed fails the scan instead of being skipped."""
    config = tmp_path / "config"
    config.mkdir()
    modules = {"Modules": [{"Key": "", "Source": "", "Dir": "."}, {"Key": "m", "Dir": "../missing-module"}]}
    result, calls = _run_apply(tmp_path, config, ["create"], {"STUB_MODULES_JSON": json.dumps(modules)})
    assert result.returncode != 0
    assert "Could not list the Terraform config files" in result.stdout
    assert [call.split()[0] for call in calls] == ["init"]


def test_apply_accepts_the_real_terraform_config(tmp_path: Path) -> None:
    """The checked-in config passes the scan, so the word checks don't refuse it by accident."""
    result, calls = _run_apply(tmp_path, TERRAFORM_DIR, ["create"])
    assert result.returncode == 0, result.stdout + result.stderr
    assert calls[-1].startswith("apply")


@pytest.mark.parametrize("module_kind", ["local", "downloaded"])
def test_apply_rejects_provisioners_inside_modules(tmp_path: Path, module_kind: str) -> None:
    """A provisioner in a local module or one init downloaded is found through the modules init recorded."""
    config = tmp_path / "config"
    config.mkdir()
    module_dir = tmp_path / "outside-module" if module_kind == "local" else tmp_path / "data" / "modules" / "m"
    module_dir.mkdir(parents=True)
    (module_dir / "main.tf").write_text(
        'resource "terraform_data" "run" {\n  provisioner "local-exec" {\n    command = "id"\n  }\n}\n',
        encoding="utf-8",
    )
    source = "../outside-module" if module_kind == "local" else "example.invalid/module"
    (config / "main.tf").write_text(f'module "m" {{\n  source = "{source}"\n}}\n', encoding="utf-8")
    modules = {"Modules": [{"Key": "", "Source": "", "Dir": "."}, {"Key": "m", "Dir": str(module_dir)}]}
    result, calls = _run_apply(tmp_path, config, ["create"], {"STUB_MODULES_JSON": json.dumps(modules)})
    assert result.returncode != 0
    assert "must not use provisioners" in result.stdout
    assert [call.split()[0] for call in calls] == ["init"]


def test_apply_rejects_a_changed_provider_lock_file(tmp_path: Path) -> None:
    """Only the pinned lock file, with its two providers and their hashes, gets as far as init."""
    config = tmp_path / "config"
    config.mkdir()
    lock = (TERRAFORM_DIR / ".terraform.lock.hcl").read_text(encoding="utf-8")
    (config / ".terraform.lock.hcl").write_text(
        lock + '\nprovider "registry.terraform.io/hashicorp/external" {\n  version = "2.3.5"\n}\n', encoding="utf-8"
    )
    result, calls = _run_apply(tmp_path, config, ["create"])
    assert result.returncode != 0
    assert "lock file changed" in result.stdout
    assert calls == []


@pytest.mark.parametrize("override", ["TF_CLI_CONFIG_FILE", "TF_PLUGIN_CACHE_DIR", "terraformrc"])
def test_apply_rejects_terraform_cli_configuration(tmp_path: Path, override: str) -> None:
    """CLI configuration could change where providers come from, so any of it stops the run before init."""
    extra_environment = {} if override == "terraformrc" else {override: str(tmp_path / "override")}
    if override == "terraformrc":
        (tmp_path / "home").mkdir()
        (tmp_path / "home" / ".terraformrc").write_text("provider_installation {}\n", encoding="utf-8")
    result, calls = _run_apply(tmp_path, TERRAFORM_DIR, ["create"], extra_environment)
    assert result.returncode != 0
    assert "No Terraform CLI configuration" in result.stdout
    assert calls == []


def test_export_follows_next_link_pages(tmp_path: Path) -> None:
    """The ADF export keeps every page, writes every resource type, and looks up the subscription once."""
    if shutil.which("jq") is None:
        pytest.skip("jq is required for the export-script test")
    binary_dir = tmp_path / "bin"
    binary_dir.mkdir()
    az_log = tmp_path / "az-calls"
    (binary_dir / "terraform").write_text('#!/usr/bin/env bash\necho "${@: -1}"\n', encoding="utf-8")
    (binary_dir / "az").write_text(
        """#!/usr/bin/env bash
echo "$*" >> "$AZ_LOG"
case "$*" in
  "account show"*) echo "00000000-0000-0000-0000-000000000001" ;;
  *"/pipelines?api-version"*) echo '{"value":[{"name":"first","properties":{}}],"nextLink":"https://page-two"}' ;;
  *"https://page-two"*) echo '{"value":[{"name":"second","properties":{}}]}' ;;
  *) echo '{"value":[{"name":"factory/only","properties":{}}]}' ;;
esac
""",
        encoding="utf-8",
    )
    for path in binary_dir.iterdir():
        path.chmod(0o755)
    environment = {
        **os.environ,
        "PATH": f"{binary_dir}:{os.environ['PATH']}",
        "AZ_LOG": str(az_log),
        "RUNNER_TEMP": str(tmp_path),
        "GITHUB_ENV": str(tmp_path / "github-env"),
    }
    subprocess.run([str(BASH), str(EXPORT_SCRIPT)], cwd=tmp_path, env=environment, check=True)

    exported = sorted(path.name for path in (tmp_path / "adf-export/pipelines").glob("*.json"))
    assert exported == ["first.json", "second.json"]
    for directory in ("datasets", "linked_services", "dataflows", "integration_runtimes", "triggers"):
        assert [path.name for path in (tmp_path / "adf-export" / directory).iterdir()] == ["only.json"], directory
    assert az_log.read_text().count("account show") == 1
    assert "ADF_FACTORY_NAME=factory_name" in (tmp_path / "github-env").read_text().splitlines()


@pytest.mark.parametrize("script", sorted(SCRIPT.parent.glob("*.sh")), ids=lambda path: path.name)
def test_bash_syntax(script: Path) -> None:
    """Every workflow shell script parses under the system Bash 3.2."""
    subprocess.run([str(BASH), "-n", str(script)], check=True)


def test_every_taggable_terraform_resource_uses_owner_tags() -> None:
    """The shared owner tag is attached to every taggable base resource."""
    terraform = TERRAFORM.read_text()
    assert "owner     = trimspace(var.owner_email)" in terraform
    resources = list(re.finditer(r'^resource "(?P<type>azurerm_[^"]+)" "(?P<name>[^"]+)" \{', terraform, re.MULTILINE))
    assert resources
    for index, resource in enumerate(resources):
        end = resources[index + 1].start() if index + 1 < len(resources) else len(terraform)
        body = terraform[resource.end() : end]
        identity = f"{resource.group('type')}.{resource.group('name')}"
        assert re.search(r"^\s*tags\s*=\s*local\.tags$", body, re.MULTILINE), identity

    azapi_resources = list(re.finditer(r'^resource "azapi_resource" "(?P<name>[^"]+)" \{', terraform, re.MULTILINE))
    assert azapi_resources
    for index, resource in enumerate(azapi_resources):
        end = azapi_resources[index + 1].start() if index + 1 < len(azapi_resources) else len(terraform)
        body = terraform[resource.end() : end]
        resource_type = re.search(r'^\s*type\s*=\s*"(?P<type>[^"]+)"$', body, re.MULTILINE)
        assert resource_type, resource.group("name")
        assert resource_type.group("type").startswith(
            ("Microsoft.DataFactory/factories/", "Microsoft.Storage/storageAccounts/")
        ), f"top-level azapi resource must be assessed for owner tags: {resource.group('name')}"


def _clean_environment(binary_dir: Path, mutation_log: Path) -> dict[str, str]:
    """Build a deterministic environment without inheriting developer credentials."""
    environment = {
        key: value for key, value in os.environ.items() if key not in SENSITIVE_ENV and not key.startswith("AZURE_")
    }
    environment.update(
        {
            "PATH": f"{binary_dir}:{environment['PATH']}",
            "MUTATION_LOG": str(mutation_log),
            "GITHUB_REPO": "databricks-solutions/flowx",
            "AZURE_SUBSCRIPTION_ID": "00000000-0000-0000-0000-000000000001",
            "REVIEWERS": "flowx-reviewer,second",
            "STUB_ROLE_ACTIONS": _script_json("REQUIRED_ROLE_ACTIONS"),
            "STUB_ROLE_NOT_ACTIONS": _script_json("REQUIRED_ROLE_NOT_ACTIONS"),
            "STUB_ROLE_SCOPE": RESOURCE_GROUP_SCOPE,
            "RESOURCE_OWNER_EMAIL": "owner@example.invalid",
        }
    )
    return environment


def _run_script(arguments: list[str], environment: dict[str, str]) -> subprocess.CompletedProcess[str]:
    """Run the consolidated script without allowing interactive input."""
    return subprocess.run(
        [str(BASH), str(SCRIPT), *arguments],
        stdin=subprocess.DEVNULL,
        capture_output=True,
        text=True,
        env=environment,
        check=False,
    )


def _script_json(variable: str) -> str:
    """Read one JSON list constant from the setup script so the stubs echo exactly what it expects."""
    match = re.search(rf"^{variable}='(?P<value>\[.*?\])'$", SCRIPT.read_text(), re.MULTILINE | re.DOTALL)
    assert match, variable
    return json.dumps(json.loads(match.group("value")))


def _run_apply(
    tmp_path: Path, config: Path, actions: list[str], extra_environment: dict[str, str] | None = None
) -> tuple[subprocess.CompletedProcess[str], list[str]]:
    """Run apply-terraform.sh with a stub terraform whose plan gives one resource the given actions.

    The stub's init writes STUB_MODULES_JSON, when set, where real init records the modules it found.
    """
    if shutil.which("jq") is None or not (shutil.which("sha256sum") or shutil.which("shasum")):
        pytest.skip("jq and sha256sum or shasum are required for the apply-script test")
    lock = config / ".terraform.lock.hcl"
    if not lock.exists():
        shutil.copy(TERRAFORM_DIR / ".terraform.lock.hcl", lock)
    binary_dir = tmp_path / "bin"
    binary_dir.mkdir()
    calls = tmp_path / "terraform-calls"
    plan = {"resource_changes": [{"address": "azurerm_storage_account.test", "change": {"actions": actions}}]}
    (binary_dir / "terraform").write_text(
        """#!/usr/bin/env bash
echo "$*" >> "$CALLS"
if [[ "$1" == init && -n "${STUB_MODULES_JSON:-}" ]]; then
  mkdir -p "$TF_DATA_DIR/modules"; echo "$STUB_MODULES_JSON" > "$TF_DATA_DIR/modules/modules.json"
fi
[[ "$1" != show ]] || echo "$STUB_PLAN"
""",
        encoding="utf-8",
    )
    (binary_dir / "terraform").chmod(0o755)
    environment = {
        key: value for key, value in os.environ.items() if key not in {"TF_CLI_CONFIG_FILE", "TF_PLUGIN_CACHE_DIR"}
    }
    environment.update(
        {
            "PATH": f"{binary_dir}:{os.environ['PATH']}",
            "HOME": str(tmp_path / "home"),
            "CALLS": str(calls),
            "STUB_PLAN": json.dumps(plan),
            "TF_DATA_DIR": str(tmp_path / "data"),
            "TF_STATE_DIR": str(tmp_path / "state"),
            "TF_STATE_PATH": str(tmp_path / "state" / "terraform.tfstate"),
            **(extra_environment or {}),
        }
    )
    result = subprocess.run(
        [str(BASH), str(APPLY_SCRIPT)], cwd=config, env=environment, capture_output=True, text=True, check=False
    )
    return result, calls.read_text().splitlines() if calls.exists() else []


def _run_teardown(tmp_path: Path) -> tuple[subprocess.CompletedProcess[str], list[str]]:
    """Run teardown-integration.sh with stub terraform and az CLIs that record their calls."""
    binary_dir = tmp_path / "bin"
    binary_dir.mkdir()
    calls = tmp_path / "calls"
    for tool in ("terraform", "az"):
        (binary_dir / tool).write_text(f'#!/usr/bin/env bash\necho "{tool} $*" >> "$CALLS"\n', encoding="utf-8")
        (binary_dir / tool).chmod(0o755)
    environment = {
        **os.environ,
        "PATH": f"{binary_dir}:{os.environ['PATH']}",
        "CALLS": str(calls),
        "RUNNER_TEMP": str(tmp_path),
        "CI_SCRIPTS": str(tmp_path / "ci-scripts"),
        "TF_STATE_DIR": str(tmp_path / "state"),
        "TF_STATE_PATH": str(tmp_path / "state" / "terraform.tfstate"),
        "TF_DATA_DIR": str(tmp_path / "data"),
        "GITHUB_RUN_ID": "1",
        "GITHUB_RUN_ATTEMPT": "1",
    }
    result = subprocess.run(
        [str(BASH), str(TEARDOWN_SCRIPT)], cwd=tmp_path, env=environment, capture_output=True, text=True, check=False
    )
    return result, calls.read_text().splitlines() if calls.exists() else []
