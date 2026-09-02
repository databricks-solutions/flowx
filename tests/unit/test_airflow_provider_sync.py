"""Tests for the vendored airflow-to-dabs provider pin."""

from __future__ import annotations

import json
import re
import shutil
import subprocess
import sys
from pathlib import Path

import pytest

import flowx.agentic as agentic_contract
from flowx.agentic import AgenticContractError

ROOT = Path(__file__).parents[2]
SCRIPT = ROOT / "scripts" / "sync_airflow_provider.py"
PROVIDER = ROOT / "skills" / "flowx-resolve-airflow-gaps" / "references" / "airflow-to-dabs"


def _check(destination: Path) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, str(SCRIPT), "--check", "--destination", str(destination)],
        check=False,
        capture_output=True,
        text=True,
    )


def _sync(checkout: Path, destination: Path, *, tag: str = "v9.8.7") -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [
            sys.executable,
            str(SCRIPT),
            "--source",
            str(checkout),
            "--tag",
            tag,
            "--destination",
            str(destination),
        ],
        check=False,
        capture_output=True,
        text=True,
    )


def _versionless_checkout(tmp_path: Path, *, tag: str = "v9.8.7") -> Path:
    checkout = tmp_path / "upstream"
    shutil.copytree(PROVIDER, checkout)
    manifest_path = checkout / "providers" / "flowx-gap-resolver" / "provider.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["provider"].pop("version", None)
    manifest.pop("flowx_pin")
    manifest_path.write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    subprocess.run(["git", "init", "-q"], cwd=checkout, check=True)
    subprocess.run(["git", "add", "."], cwd=checkout, check=True)
    subprocess.run(
        [
            "git",
            "-c",
            "user.name=flowx-test",
            "-c",
            "user.email=flowx-test@example.com",
            "commit",
            "-qm",
            "provider fixture",
            "--no-verify",
        ],
        cwd=checkout,
        check=True,
    )
    subprocess.run(["git", "tag", tag], cwd=checkout, check=True)
    return checkout


def test_committed_airflow_provider_pin_is_valid() -> None:
    result = _check(PROVIDER)

    assert result.returncode == 0, result.stderr
    pin = json.loads(result.stdout)
    manifest = json.loads((PROVIDER / "providers" / "flowx-gap-resolver" / "provider.json").read_text())
    assert pin == {
        "commit": manifest["flowx_pin"]["commit"],
        "content_sha256": manifest["flowx_pin"]["content_sha256"],
        "tag": manifest["flowx_pin"]["tag"],
    }
    assert agentic_contract._provider_identity()["version"] == pin["tag"].removeprefix("v")


def test_airflow_provider_sync_accepts_versionless_manifest(tmp_path: Path) -> None:
    checkout = _versionless_checkout(tmp_path)
    destination = tmp_path / "vendored"

    result = _sync(checkout, destination)

    assert result.returncode == 0, result.stderr
    manifest = json.loads(
        (destination / "providers" / "flowx-gap-resolver" / "provider.json").read_text(encoding="utf-8")
    )
    pin = json.loads(result.stdout)
    resolved_commit = subprocess.check_output(["git", "rev-parse", "v9.8.7^{commit}"], cwd=checkout, text=True).strip()
    assert "version" not in manifest["provider"]
    assert pin == {
        "tag": "v9.8.7",
        "commit": resolved_commit,
        "content_sha256": manifest["flowx_pin"]["content_sha256"],
    }
    assert re.fullmatch(r"[0-9a-f]{40}", pin["commit"])
    assert re.fullmatch(r"[0-9a-f]{64}", pin["content_sha256"])
    checked = _check(destination)
    assert checked.returncode == 0, checked.stderr
    assert json.loads(checked.stdout) == pin


def test_airflow_provider_runtime_derives_version_from_pin(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    checkout = _versionless_checkout(tmp_path)
    destination = tmp_path / "vendored"
    result = _sync(checkout, destination)
    assert result.returncode == 0, result.stderr
    monkeypatch.setattr(agentic_contract, "_provider_context_path", lambda: destination)

    assert agentic_contract._provider_identity() == {
        "name": "airflow-to-dabs",
        "version": "9.8.7",
        "repository": "https://github.com/park-peter/airflow-to-dabs",
    }


def test_airflow_provider_runtime_rejects_modified_content(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    destination = tmp_path / PROVIDER.name
    shutil.copytree(PROVIDER, destination)
    profile = destination / "providers" / "flowx-gap-resolver" / "PROFILE.md"
    profile.write_text(profile.read_text(encoding="utf-8") + "\nmodified\n", encoding="utf-8")
    monkeypatch.setattr(agentic_contract, "_provider_context_path", lambda: destination)

    with pytest.raises(AgenticContractError, match="content digest"):
        agentic_contract._provider_identity()


def test_airflow_provider_sync_requires_an_explicit_tag() -> None:
    result = subprocess.run(
        [sys.executable, str(SCRIPT), "--source", str(ROOT)],
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 2
    assert "--tag is required unless --check is used" in result.stderr


def test_airflow_provider_pin_rejects_modified_content(tmp_path: Path) -> None:
    destination = tmp_path / PROVIDER.name
    shutil.copytree(PROVIDER, destination)
    profile = destination / "providers" / "flowx-gap-resolver" / "PROFILE.md"
    profile.write_text(profile.read_text(encoding="utf-8") + "\nmodified\n", encoding="utf-8")

    result = _check(destination)

    assert result.returncode != 0
    assert "content digest does not match" in result.stderr


def test_airflow_provider_pin_rejects_noncanonical_json(tmp_path: Path) -> None:
    destination = tmp_path / PROVIDER.name
    shutil.copytree(PROVIDER, destination)
    manifest = destination / "providers" / "flowx-gap-resolver" / "provider.json"
    manifest.write_text(manifest.read_text(encoding="utf-8") + "\n", encoding="utf-8")

    result = _check(destination)

    assert result.returncode != 0
    assert "JSON is not canonical" in result.stderr
