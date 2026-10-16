"""Offline safety checks for the Azure/GitHub setup script."""

from __future__ import annotations

import os
import re
import shutil
import subprocess
from pathlib import Path

import pytest

SCRIPT = Path(__file__).parents[2] / ".github" / "scripts" / "setup-integration.sh"
EXPORT_SCRIPT = SCRIPT.with_name("export-adf.sh")
TERRAFORM = Path(__file__).parent / "azure" / "terraform" / "main.tf"
BASH = Path("/bin/bash")
SECRET = "must-never-appear-in-output"
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
  *"role definition list"*"-o json"*)
    if [[ "${STUB_ROLE_MISMATCH:-}" == 1 ]]; then actions='["Microsoft.Resources/subscriptions/read"]';
    else actions='["Microsoft.Resources/subscriptions/read","Microsoft.Resources/subscriptions/resourceGroups/*",
      "Microsoft.DataFactory/factories/*","Microsoft.Storage/storageAccounts/*","Microsoft.KeyVault/vaults/*",
      "Microsoft.EventGrid/eventSubscriptions/read","Microsoft.EventGrid/eventSubscriptions/write",
      "Microsoft.EventGrid/eventSubscriptions/delete"]'; fi
    printf '[{"assignableScopes":["/subscriptions/00000000-0000-0000-0000-000000000001"],
      "permissions":[{"actions":%s,"notActions":[],"dataActions":[],"notDataActions":[]}],
      "roleName":"flowx-live-integration-deployer","id":"role-id"}]\n' "$actions" ;;
  *"role definition list"*) [[ "${STUB_FRESH:-}" == 1 ]] || echo "role-id" ;;
  *"role assignment list"*)
    [[ "${STUB_NO_SP:-}" != 1 ]] || { echo 'placeholder assignment lookup' >> "$MUTATION_LOG"; exit 88; }
    echo "assignment-id" ;;
  *"role definition update"*)
    if [[ "${ALLOW_ROLE_UPDATE:-}" == 1 ]]; then
      previous=''
      for argument in "$@"; do
        if [[ "$previous" == --role-definition ]]; then cat "$argument" > "$ROLE_JSON_LOG"; fi
        previous=$argument
      done
    else echo "$*" >> "$MUTATION_LOG"; exit 91; fi ;;
  *"group list"*) echo "flowx-orphan-exact" ;;
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
  *"api repos/"*"/environments/"*) echo '{"protection_rules":[
    {"type":"required_reviewers","reviewers":[{"type":"User","reviewer":{"login":"flowx-reviewer","id":12345}}]}
  ]}' ;;
  *"secret list"*)
    printf 'AZURE_CLIENT_ID\nAZURE_TENANT_ID\nAZURE_SUBSCRIPTION_ID\n'
    [[ "${STUB_MISSING_OWNER:-}" == 1 ]] || printf 'RESOURCE_OWNER_EMAIL\n' ;;
  *"secret set"*)
    if [[ "${ALLOW_SECRET_WRITE:-}" == 1 ]]; then
      printf '%s\n' "$*" >> "$GH_ARGV_LOG"
      input=$(cat)
      printf '%s\n' "$input" >> "$GH_STDIN_LOG"
    else echo "$*" >> "$MUTATION_LOG"; exit 91; fi ;;
  *"--method PUT"*) echo "$*" >> "$MUTATION_LOG"; exit 91 ;;
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


def test_role_updates_an_existing_definition_with_event_grid(stub_path: tuple[Path, Path], tmp_path: Path) -> None:
    """An existing pre-Event-Grid role is idempotently updated in place."""
    binary_dir, mutation_log = stub_path
    role_json_log = tmp_path / "role.json"
    environment = _clean_environment(binary_dir, mutation_log)
    environment.update({"ALLOW_ROLE_UPDATE": "1", "ROLE_JSON_LOG": str(role_json_log), "STUB_ROLE_MISMATCH": "1"})
    result = _run_script(["role", "--yes"], environment)
    assert result.returncode == 0, result.stdout + result.stderr
    role_json = role_json_log.read_text()
    assert '"Microsoft.EventGrid/eventSubscriptions/read"' in role_json
    assert '"Microsoft.EventGrid/eventSubscriptions/write"' in role_json
    assert '"Microsoft.EventGrid/eventSubscriptions/delete"' in role_json
    assert '"Microsoft.EventGrid/eventSubscriptions/*"' not in role_json
    assert not mutation_log.exists()


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
    """Cleanup without --delete cannot dispatch a resource-group deletion."""
    binary_dir, mutation_log = stub_path
    result = _run_script(["cleanup", "--yes"], _clean_environment(binary_dir, mutation_log))
    assert result.returncode == 0, result.stdout + result.stderr
    assert "flowx-orphan-exact" in result.stdout
    assert not mutation_log.exists()


def test_export_follows_next_link_pages(tmp_path: Path) -> None:
    """The ADF export keeps resources from every page and looks up the subscription once."""
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
  *) echo '{"value":[]}' ;;
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
    assert az_log.read_text().count("account show") == 1


def test_bash_syntax() -> None:
    """The consolidated setup script parses under the system Bash 3.2."""
    subprocess.run([str(BASH), "-n", str(SCRIPT)], check=True)


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
            "REVIEWERS": "flowx-reviewer",
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
