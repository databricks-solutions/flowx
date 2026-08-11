"""Tests for the vendored airflow-to-dabs provider pin."""

from __future__ import annotations

import json
import shutil
import subprocess
import sys
from pathlib import Path

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
    assert pin["tag"] == f"v{manifest['provider']['version']}"


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
