"""Subprocess bridge between MCP tools and the flowx adapter CLI.

Every MCP tool shells out to ``python -m flowx.adapter`` — the same unified
entry point the agent skills already use — then reads back the JSON/CSV
artifacts each phase writes. This reuses the tested phase contracts instead of
re-implementing their logic, so the MCP surface stays in lockstep with the CLI.
"""

from __future__ import annotations

import csv
import io
import json
import os
import shutil
import subprocess
import sys
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any

# Phases can take a while on large factories; allow generous default headroom.
DEFAULT_TIMEOUT = int(os.environ.get("FLOWX_MCP_TIMEOUT", "1800"))

# Cap on inline `adf_definitions` payloads (they pass through the agent context); above this, stage
# to a UC Volume and pass a path reference instead. Configurable via FLOWX_MAX_INLINE_BYTES.
MAX_INLINE_BYTES = int(os.environ.get("FLOWX_MAX_INLINE_BYTES", str(5_000_000)))


@dataclass
class AdapterResult:
    """Outcome of a single ``flowx.adapter`` invocation."""

    command: list[str]
    returncode: int
    stdout: str
    stderr: str

    @property
    def ok(self) -> bool:
        return self.returncode == 0

    def as_dict(self) -> dict[str, Any]:
        """Serialise the raw process outcome for inclusion in a tool result."""
        return {
            "command": " ".join(self.command),
            "ok": self.ok,
            "returncode": self.returncode,
            "stdout": self.stdout.strip(),
            "stderr": self.stderr.strip(),
        }


def run_adapter(args: list[Any], *, cwd: str | Path | None = None, timeout: int = DEFAULT_TIMEOUT) -> AdapterResult:
    """Invoke ``python -m flowx.adapter`` with the supplied arguments.

    Args:
        args: Adapter subcommand and flags (each item is stringified).
        cwd: Working directory for the subprocess. Defaults to the current one.
        timeout: Seconds before the subprocess is killed.

    Returns:
        The captured :class:`AdapterResult`.
    """
    command = [sys.executable, "-m", "flowx.adapter", *[str(arg) for arg in args]]
    proc = subprocess.run(
        command,
        cwd=str(cwd) if cwd else None,
        capture_output=True,
        text=True,
        timeout=timeout,
    )
    return AdapterResult(command=command, returncode=proc.returncode, stdout=proc.stdout, stderr=proc.stderr)


def read_json(path: Path) -> Any | None:
    """Return parsed JSON at *path*, or ``None`` when the file is absent/invalid."""
    try:
        return json.loads(path.read_text())
    except (OSError, json.JSONDecodeError):
        return None


def read_text(path: Path) -> str | None:
    """Return text at *path*, or ``None`` when it cannot be read."""
    try:
        return path.read_text()
    except OSError:
        return None


def parse_stdout_json(result: AdapterResult) -> Any | None:
    """Parse the adapter's stdout as JSON (used by inspect/inputs/workspace-paths)."""
    try:
        return json.loads(result.stdout)
    except json.JSONDecodeError:
        return None


def list_tree(root: Path, *, max_entries: int = 250) -> list[str]:
    """Return repo-relative paths of files under *root* (sorted, capped)."""
    if not root.exists():
        return []
    files = sorted(str(p.relative_to(root)) for p in root.rglob("*") if p.is_file())
    return files[:max_entries]


def summarize_inventory(output_dir: Path) -> dict[str, Any] | None:
    """Summarise ``metadata/inventory.json`` produced by the discover phase.

    The discover phase writes a ready-made ``summary`` block (pipeline/activity
    counts by strategy plus coverage); surface it directly when present.
    """
    inventory = read_json(output_dir / "metadata" / "inventory.json")
    if not isinstance(inventory, dict):
        return None

    summary = inventory.get("summary")
    if isinstance(summary, dict):
        return summary

    pipelines = inventory.get("pipelines") or []
    return {"pipeline_count": len(pipelines)}


def summarize_translation(output_dir: Path) -> dict[str, Any] | None:
    """Summarise the translation report (transient under ``.work/``)."""
    for candidate in (".work/translation_report.json", "translation_report.json"):
        report = read_json(output_dir / candidate)
        if isinstance(report, dict):
            break
    else:
        return None

    pipelines = report.get("pipelines") or []
    statuses: dict[str, int] = {}
    for pipeline in pipelines:
        for task in pipeline.get("tasks", []):
            status = str(task.get("status", "translated")).lower()
            statuses[status] = statuses.get(status, 0) + 1
    return {"pipelines": len(pipelines), "task_status_counts": statuses}


def materialize_lookup_rows(source: str) -> list[dict[str, str]]:
    """Parse a CSV file path or literal CSV string into a list of row dicts.

    Mirrors the adapter's ``materialize-lookup`` parsing so the MCP tool can
    validate input without a second subprocess hop.
    """
    text = Path(source).read_text() if Path(source).exists() else source
    reader = csv.DictReader(io.StringIO(text))
    return [dict(row) for row in reader]


def materialize_adf_definitions(definitions: dict[str, Any]) -> str:
    """Write an inline ADF-definitions payload to a temp dir and return a source path.

    A hosted MCP server (Databricks App) cannot read the user's workspace / UC Volume files, so
    the caller (which can) passes the ADF JSON inline and the server materializes it locally.

    ``definitions`` maps relative file paths — mirroring the ADF Git-export layout, e.g.
    ``"pipeline/Foo.json"``, ``"dataset/Bar.json"``, ``"linkedService/Baz.json"``,
    ``"trigger/Qux.json"`` — to JSON content (a dict, or a JSON string). The files are written
    under a fresh temp directory whose path is returned (the loader reads it as a tree).

    Special case: a single entry whose content is an ARM template (a dict with a top-level
    ``resources`` list) is written as one file and that file path is returned, so the loader
    parses it in ARM-template mode.

    Raises:
        ValueError: if the payload is empty or a key escapes the temp directory.
    """
    if not definitions:
        raise ValueError("adf_definitions is empty")

    total_bytes = sum(len(v if isinstance(v, str) else json.dumps(v)) for v in definitions.values())
    if total_bytes > MAX_INLINE_BYTES:
        raise ValueError(
            f"adf_definitions is ~{total_bytes} bytes (limit {MAX_INLINE_BYTES}); inline payloads pass "
            "through the agent's context and do not scale. Stage the ADF export to a UC Volume and pass "
            "'adf_volume_path' instead (the server reads it directly via the SDK Files API)."
        )

    base = Path(tempfile.mkdtemp(prefix="flowx-adf-"))

    if len(definitions) == 1:
        (only_value,) = definitions.values()
        content = json.loads(only_value) if isinstance(only_value, str) else only_value
        if isinstance(content, dict) and isinstance(content.get("resources"), list):
            file_path = base / "arm_template.json"
            file_path.write_text(json.dumps(content), encoding="utf-8")
            return str(file_path)

    base_resolved = base.resolve()
    for rel_path, content in definitions.items():
        dest = (base / rel_path).resolve()
        if base_resolved not in dest.parents and dest != base_resolved:
            shutil.rmtree(base, ignore_errors=True)
            raise ValueError(f"unsafe path in adf_definitions: {rel_path!r}")
        dest.parent.mkdir(parents=True, exist_ok=True)
        text = content if isinstance(content, str) else json.dumps(content)
        dest.write_text(text, encoding="utf-8")
    return str(base)


def cleanup_materialized(source: str) -> None:
    """Remove a temp tree created by :func:`materialize_adf_definitions`.

    Accepts either the returned directory or the single-file path (whose parent temp dir is
    removed). Only paths under the system temp dir are deleted, as a safety guard.
    """
    path = Path(source)
    target = path if path.is_dir() else path.parent
    if str(target.resolve()).startswith(str(Path(tempfile.gettempdir()).resolve())):
        shutil.rmtree(target, ignore_errors=True)


def read_tree(root: Path, *, max_total_bytes: int = 2_000_000) -> dict[str, Any]:
    """Return the text contents of files under *root* so a caller can persist them.

    The hosted app writes the generated bundle to ephemeral local disk that the user cannot
    reach, so the bundle contents are returned inline. Binary/unreadable files are skipped, and
    once the cumulative size passes *max_total_bytes* further files are listed under ``truncated``
    instead of being included.

    Returns:
        ``{"files": {relpath: text, ...}, "truncated": [relpath, ...]}`` ("truncated" omitted when empty).
    """
    files: dict[str, str] = {}
    truncated: list[str] = []
    total = 0
    if root.exists():
        for path in sorted(root.rglob("*")):
            if not path.is_file():
                continue
            rel = str(path.relative_to(root))
            try:
                data = path.read_text(encoding="utf-8")
            except (OSError, UnicodeDecodeError):
                continue
            if total + len(data) > max_total_bytes:
                truncated.append(rel)
                continue
            files[rel] = data
            total += len(data)
    result: dict[str, Any] = {"files": files}
    if truncated:
        result["truncated"] = truncated
    return result


def download_volume_dir(volume_path: str) -> str:
    """Download a Unity Catalog Volume directory tree to a local temp dir via the SDK Files API.

    This is the scalable input path for large factories: the bytes are pulled by the server (which
    can read the volume as its service principal) and never pass through the calling agent. Returns
    the local temp-dir path (clean up with :func:`cleanup_materialized`).
    """
    from databricks.sdk import WorkspaceClient

    client = WorkspaceClient()
    base = Path(tempfile.mkdtemp(prefix="flowx-vol-"))
    root = volume_path.rstrip("/")

    def _recurse(directory: str) -> None:
        for entry in client.files.list_directory_contents(directory):
            entry_path = entry.path or ""
            if entry.is_directory:
                _recurse(entry_path)
                continue
            rel = entry_path[len(root) :].lstrip("/")
            dest = base / rel
            dest.parent.mkdir(parents=True, exist_ok=True)
            contents = client.files.download(entry_path).contents
            dest.write_bytes(contents.read() if contents is not None else b"")

    _recurse(root)
    return str(base)


def download_workspace_dir(workspace_path: str) -> str:
    """Download a ``/Workspace`` directory tree to a local temp dir via the SDK Workspace API.

    Workspace files (e.g. an ADF Git folder cloned under ``/Workspace``) use the Workspace API
    (``w.workspace.list`` / ``w.workspace.download``), which is distinct from the Files API used for
    UC Volumes. Like the volume path, the bytes are pulled by the server and bypass the agent.
    Returns the local temp-dir path (clean up with :func:`cleanup_materialized`).
    """
    from databricks.sdk import WorkspaceClient
    from databricks.sdk.service.workspace import ObjectType

    client = WorkspaceClient()
    base = Path(tempfile.mkdtemp(prefix="flowx-ws-"))
    root = workspace_path.rstrip("/")

    def _recurse(directory: str) -> None:
        for entry in client.workspace.list(directory):
            entry_path = entry.path or ""
            if entry.object_type in (ObjectType.DIRECTORY, ObjectType.REPO):
                _recurse(entry_path)
            elif entry.object_type == ObjectType.FILE:
                rel = entry_path[len(root) :].lstrip("/")
                dest = base / rel
                dest.parent.mkdir(parents=True, exist_ok=True)
                dest.write_bytes(client.workspace.download(entry_path).read())

    _recurse(root)
    return str(base)


def upload_tree_to_volume(local_root: Path, volume_path: str) -> dict[str, Any]:
    """Upload a local directory tree to a Unity Catalog Volume via the SDK Files API.

    This is the scalable output path: the generated bundle is written to a location the user can
    reach without the (potentially large) file contents passing back through the agent.

    Returns:
        ``{"output_volume_path": <root>, "files": [relpath, ...], "count": n}``.
    """
    from databricks.sdk import WorkspaceClient

    client = WorkspaceClient()
    root = volume_path.rstrip("/")
    uploaded: list[str] = []
    for path in sorted(local_root.rglob("*")):
        if not path.is_file():
            continue
        rel = str(path.relative_to(local_root))
        client.files.upload(f"{root}/{rel}", io.BytesIO(path.read_bytes()), overwrite=True)
        uploaded.append(rel)
    return {"output_volume_path": root, "files": uploaded, "count": len(uploaded)}


def upload_tree_to_workspace(local_root: Path, workspace_path: str) -> dict[str, Any]:
    """Upload a local directory tree to a ``/Workspace`` directory via the SDK Workspace API.

    The output counterpart to :func:`download_workspace_dir`: the generated DAB lands in a workspace
    folder the user can reach without the contents passing back through the agent. Files are imported
    with ``ImportFormat.RAW`` so each one (``databricks.yml``, ``*.py``, ``*.yml``, ``SETUP.md``, …) is
    stored verbatim as a workspace **file** rather than being interpreted as a notebook (which
    ``AUTO`` would do to ``.py`` files, corrupting the bundle source tree).

    Returns:
        ``{"output_workspace_path": <root>, "files": [relpath, ...], "count": n}``.
    """
    from databricks.sdk import WorkspaceClient
    from databricks.sdk.service.workspace import ImportFormat

    client = WorkspaceClient()
    root = workspace_path.rstrip("/")
    uploaded: list[str] = []
    made_dirs: set[str] = set()
    for path in sorted(local_root.rglob("*")):
        if not path.is_file():
            continue
        rel = str(path.relative_to(local_root))
        dest = f"{root}/{rel}"
        parent = dest.rsplit("/", 1)[0]
        if parent not in made_dirs:
            client.workspace.mkdirs(parent)
            made_dirs.add(parent)
        client.workspace.upload(dest, path.read_bytes(), format=ImportFormat.RAW, overwrite=True)
        uploaded.append(rel)
    return {"output_workspace_path": root, "files": uploaded, "count": len(uploaded)}
