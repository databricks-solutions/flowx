#!/usr/bin/env python3
"""Vendor and verify a tagged airflow-to-dabs provider release."""

from __future__ import annotations

import argparse
import hashlib
import json
import posixpath
import re
import shutil
import subprocess
import tempfile
from pathlib import Path, PurePosixPath
from typing import Any

REPOSITORY = "https://github.com/park-peter/airflow-to-dabs"
PROVIDER_PATH = PurePosixPath("providers/flowx-gap-resolver/provider.json")
PIN_FIELD = "flowx_pin"


class ProviderSyncError(ValueError):
    """Raised when provider source or vendored content violates the pin contract."""


def _release_version(tag: Any) -> str:
    if not isinstance(tag, str) or re.fullmatch(r"v[0-9A-Za-z][0-9A-Za-z.+-]*", tag) is None:
        raise ProviderSyncError(f"Provider release tag is invalid: {tag!r}")
    return tag[1:]


def _validate_provider_identity(provider: dict[str, Any], *, tag: str) -> None:
    identity = provider.get("provider")
    if (
        not isinstance(identity, dict)
        or identity.get("name") != "airflow-to-dabs"
        or identity.get("repository") != REPOSITORY
    ):
        raise ProviderSyncError("Provider manifest has an unsupported identity")
    version = identity.get("version")
    if version is not None and version != _release_version(tag):
        raise ProviderSyncError(f"Provider version {version!r} does not match tag {tag!r}")


def _json_object(data: bytes, *, label: str) -> dict[str, Any]:
    try:
        value = json.loads(data)
    except json.JSONDecodeError as error:
        raise ProviderSyncError(f"{label} contains invalid JSON: {error}") from error
    if not isinstance(value, dict):
        raise ProviderSyncError(f"{label} must contain a JSON object")
    return value


def _canonical_bytes(path: PurePosixPath, data: bytes, *, strip_pin: bool = False) -> bytes:
    if path.suffix != ".json":
        return data
    value = _json_object(data, label=path.as_posix())
    if strip_pin:
        value.pop(PIN_FIELD, None)
    return (json.dumps(value, indent=2, sort_keys=True) + "\n").encode()


def _resolve_path(base: PurePosixPath, relative: str) -> PurePosixPath:
    if not relative or PurePosixPath(relative).is_absolute():
        raise ProviderSyncError(f"Provider manifest contains an unsafe path: {relative!r}")
    normalized = PurePosixPath(posixpath.normpath((base / relative).as_posix()))
    if normalized.as_posix() == ".." or normalized.as_posix().startswith("../"):
        raise ProviderSyncError(f"Provider manifest path escapes the repository: {relative!r}")
    return normalized


def _allowlisted_paths(provider: dict[str, Any]) -> list[PurePosixPath]:
    base = PROVIDER_PATH.parent
    interface = provider.get("interface")
    if not isinstance(interface, dict) or interface.get("contract_versions") != ["1"]:
        raise ProviderSyncError("Provider manifest must declare flowx contract version 1")
    paths = {PROVIDER_PATH, _resolve_path(base, str(interface.get("entrypoint", "")))}
    knowledge = provider.get("knowledge")
    fixtures = provider.get("fixtures")
    if not isinstance(knowledge, list) or not isinstance(fixtures, list):
        raise ProviderSyncError("Provider manifest knowledge and fixtures must be lists")
    for item in knowledge:
        if not isinstance(item, dict) or not isinstance(item.get("path"), str):
            raise ProviderSyncError("Every provider knowledge entry requires a path")
        paths.add(_resolve_path(base, item["path"]))
    for item in fixtures:
        if not isinstance(item, str):
            raise ProviderSyncError("Every provider fixture entry must be a path string")
        paths.add(_resolve_path(base, item))
    return sorted(paths, key=lambda item: item.as_posix())


def _combined_digest(files: dict[PurePosixPath, bytes]) -> str:
    digest = hashlib.sha256()
    for path in sorted(files, key=lambda item: item.as_posix()):
        content = files[path]
        digest.update(path.as_posix().encode())
        digest.update(b"\0")
        digest.update(str(len(content)).encode())
        digest.update(b"\0")
        digest.update(content)
    return digest.hexdigest()


def _git_output(checkout: Path, *args: str) -> bytes:
    try:
        return subprocess.check_output(["git", "-C", str(checkout), *args], stderr=subprocess.STDOUT)
    except subprocess.CalledProcessError as error:
        message = error.output.decode(errors="replace").strip()
        raise ProviderSyncError(message or "git command failed") from error


def sync_provider(*, checkout: Path, tag: str, destination: Path) -> dict[str, str]:
    _release_version(tag)
    commit = _git_output(checkout, "rev-parse", f"{tag}^{{commit}}").decode().strip()
    if re.fullmatch(r"[0-9a-f]{40}", commit) is None:
        raise ProviderSyncError(f"Provider tag {tag!r} did not resolve to a commit")
    provider_data = _git_output(checkout, "show", f"{commit}:{PROVIDER_PATH.as_posix()}")
    provider = _json_object(provider_data, label=PROVIDER_PATH.as_posix())
    _validate_provider_identity(provider, tag=tag)

    source_files: dict[PurePosixPath, bytes] = {}
    for path in _allowlisted_paths(provider):
        data = _git_output(checkout, "show", f"{commit}:{path.as_posix()}")
        source_files[path] = _canonical_bytes(path, data)
    content_digest = _combined_digest(source_files)

    destination.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix=".provider-sync-", dir=destination.parent) as temporary:
        staging = Path(temporary) / destination.name
        for path, data in source_files.items():
            target = staging / path.as_posix()
            target.parent.mkdir(parents=True, exist_ok=True)
            if path == PROVIDER_PATH:
                pinned = _json_object(data, label=path.as_posix())
                pinned[PIN_FIELD] = {
                    "repository": REPOSITORY,
                    "tag": tag,
                    "commit": commit,
                    "contract_version": "1",
                    "content_sha256": content_digest,
                }
                data = _canonical_bytes(path, json.dumps(pinned).encode())
            target.write_bytes(data)
        if destination.exists():
            shutil.rmtree(destination)
        shutil.move(str(staging), destination)
    return {"tag": tag, "commit": commit, "content_sha256": content_digest}


def verify_provider(destination: Path) -> dict[str, str]:
    provider_file = destination / PROVIDER_PATH.as_posix()
    provider_bytes = provider_file.read_bytes()
    provider = _json_object(provider_bytes, label=str(provider_file))
    pin = provider.get(PIN_FIELD)
    if not isinstance(pin, dict):
        raise ProviderSyncError("Vendored provider.json is missing flowx_pin metadata")
    if pin.get("repository") != REPOSITORY or pin.get("contract_version") != "1":
        raise ProviderSyncError("Vendored provider pin has an unsupported repository or contract")
    tag = pin.get("tag")
    _release_version(tag)
    _validate_provider_identity(provider, tag=tag)

    allowlisted_paths = set(_allowlisted_paths(provider))
    actual_paths: set[PurePosixPath] = set()
    for local in destination.rglob("*"):
        if local.is_symlink():
            raise ProviderSyncError(f"Vendored provider cannot contain symlinks: {local.relative_to(destination)}")
        if local.is_file():
            actual_paths.add(PurePosixPath(local.relative_to(destination).as_posix()))
    unexpected = sorted(actual_paths - allowlisted_paths, key=lambda path: path.as_posix())
    if unexpected:
        raise ProviderSyncError(
            "Vendored provider contains files outside its manifest allowlist: "
            + ", ".join(path.as_posix() for path in unexpected)
        )

    files: dict[PurePosixPath, bytes] = {}
    for path in sorted(allowlisted_paths, key=lambda item: item.as_posix()):
        local = destination / path.as_posix()
        if not local.is_file():
            raise ProviderSyncError(f"Vendored provider reference is missing: {path.as_posix()}")
        local_bytes = local.read_bytes()
        canonical_bytes = _canonical_bytes(path, local_bytes)
        if local_bytes != canonical_bytes:
            raise ProviderSyncError(f"Vendored provider JSON is not canonical: {path.as_posix()}")
        files[path] = _canonical_bytes(path, local_bytes, strip_pin=path == PROVIDER_PATH)
    content_digest = _combined_digest(files)
    if pin.get("content_sha256") != content_digest:
        raise ProviderSyncError("Vendored provider content digest does not match flowx_pin metadata")
    commit = pin.get("commit")
    if not isinstance(commit, str) or re.fullmatch(r"[0-9a-f]{40}", commit) is None:
        raise ProviderSyncError("Vendored provider pin has an invalid commit")
    return {"tag": tag, "commit": commit, "content_sha256": content_digest}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", type=Path, help="Exact local airflow-to-dabs checkout used for synchronization.")
    parser.add_argument("--tag", help="Exact upstream release tag to vendor.")
    parser.add_argument(
        "--destination",
        type=Path,
        default=Path(__file__).resolve().parents[1]
        / "skills"
        / "flowx-resolve-airflow-gaps"
        / "references"
        / "airflow-to-dabs",
    )
    parser.add_argument(
        "--check", action="store_true", help="Verify the committed provider pin without network access."
    )
    args = parser.parse_args()
    if not args.check and args.source is None:
        parser.error("--source is required unless --check is used")
    if not args.check and args.tag is None:
        parser.error("--tag is required unless --check is used")
    try:
        result = (
            verify_provider(args.destination)
            if args.check
            else sync_provider(
                checkout=args.source.resolve() if args.source else Path(),
                tag=str(args.tag),
                destination=args.destination,
            )
        )
    except (OSError, ProviderSyncError) as error:
        parser.error(str(error))
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
