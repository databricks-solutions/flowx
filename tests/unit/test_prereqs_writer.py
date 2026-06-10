"""Unit tests for orchestra.bundler.prereqs_writer."""

from __future__ import annotations

from orchestra.bundler.prereqs_writer import build_prereqs, render_setup_md
from orchestra.models.dab import DabNotebook, SecretInstruction


class TestSecretsUnion:
    """Change fix-setup-md-secrets-union-with-secret-instructions (P1): LSC3-006."""

    def test_workflow_secrets_union_with_notebook_scanned_scopes(self):
        """SETUP.md secrets section must list both notebook-scanned and
        workflow.secrets sources without duplicating any (scope, key) pair."""
        notebooks = [
            DabNotebook(
                relative_path="notebooks/x.py",
                content=(
                    "# Databricks notebook source\n"
                    "auth_token = dbutils.secrets.get("
                    'scope="lakeh_a_pl_operational_sendMail", key="auth-credential")\n'
                ),
            )
        ]
        secret_instructions = [
            SecretInstruction(
                scope="lakeh_ls_keyvault",
                key="adapp-clientSecret",
                value_source="Azure Key Vault: lakeh-kv/adapp-clientSecret",
            ),
            # Same pair as the notebook scan -- must not be duplicated.
            SecretInstruction(
                scope="lakeh_a_pl_operational_sendMail",
                key="auth-credential",
                value_source="duplicate of notebook scan",
            ),
        ]
        prereqs = build_prereqs(
            notebooks=notebooks,
            tasks=[],
            known_bundle_jobs=set(),
            secret_instructions=secret_instructions,
        )
        # Both scopes present in the union, no duplicate keys.
        assert "lakeh_ls_keyvault" in prereqs.secrets
        assert "adapp-clientSecret" in prereqs.secrets["lakeh_ls_keyvault"]
        assert "lakeh_a_pl_operational_sendMail" in prereqs.secrets
        assert prereqs.secrets["lakeh_a_pl_operational_sendMail"] == {"auth-credential"}

    def test_setup_md_lists_unioned_secrets(self):
        """SETUP.md Option A renders every (scope, key) from the union."""
        notebooks = [
            DabNotebook(
                relative_path="notebooks/x.py",
                content=('auth_token = dbutils.secrets.get(scope="scope_from_notebook", key="key_from_notebook")\n'),
            )
        ]
        secret_instructions = [
            SecretInstruction(
                scope="scope_from_workflow",
                key="key_from_workflow",
                value_source="Azure Key Vault",
            ),
        ]
        prereqs = build_prereqs(
            notebooks=notebooks,
            tasks=[],
            known_bundle_jobs=set(),
            secret_instructions=secret_instructions,
        )
        md = render_setup_md(prereqs, bundle_name="test_bundle")
        # Both (scope, key) pairs must appear in the rendered SETUP.md.
        assert "scope_from_notebook" in md
        assert "key_from_notebook" in md
        assert "scope_from_workflow" in md
        assert "key_from_workflow" in md
