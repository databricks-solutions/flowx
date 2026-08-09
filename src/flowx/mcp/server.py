"""MCP server exposing flowx as a single dispatcher tool, ``flowx(command, parameters)``.

Each command is a thin wrapper over ``python -m flowx.adapter`` (see :mod:`flowx.mcp.runner`);
keeping flowx to one tool stays under host tool-count caps such as Genie Code's 20-tool limit.
"""

from __future__ import annotations

import json
import os
import tempfile
from collections.abc import Callable
from pathlib import Path
from typing import Any

from mcp.server.fastmcp import FastMCP
from mcp.server.transport_security import TransportSecuritySettings

from flowx.mcp import runner


def _allowed_origins() -> list[str]:
    """Allowed browser/MCP origins from ``FLOWX_ALLOWED_ORIGINS`` (comma-separated, default ``*``)."""
    raw = os.environ.get("FLOWX_ALLOWED_ORIGINS", "*")
    return [origin.strip() for origin in raw.split(",") if origin.strip()]


def _transport_security() -> TransportSecuritySettings:
    """Builds transport-security settings with the SDK's DNS-rebinding check disabled.

    Returns:
        Settings that skip the Host/Origin allowlist check.

    Notes:
        Behind the Databricks Apps OAuth proxy the check misfires (403/421) and adds nothing on top
        of the proxy. Browser CORS is handled separately in :func:`build_http_app`. See the "MCP
        server design notes" in AGENTS.md.
    """
    return TransportSecuritySettings(enable_dns_rebinding_protection=False)


_INSTRUCTIONS = """\
flowx translates a source orchestrator's pipelines (Azure Data Factory or Apache Airflow) into
Databricks Lakeflow Jobs packaged as Declarative Automation Bundles (DABs). Everything is driven
through the single `flowx` tool: `flowx(command="<command>", parameters={...})`.

Every discover/convert/migrate call requires `source` ("adf" | "airflow") — there is no default.
ADF reads adf_volume_path | adf_workspace_path | adf_definitions | adf_source_path; Airflow reads
airflow_source_path (a DAG .py file or directory).

Typical flow (ADF shown; swap source + source-path for Airflow):
  flowx("inputs", {"phase": "discover", "source": "adf"})   # learn a phase's inputs
  flowx("discover", {"source": "adf", "adf_source_path": "...", "output_dir": "..."})
  flowx("convert", {"source": "adf", "output_dir": "..."})
  flowx("inspect", {"report_path": "<output_dir>/.work/translation_report.json"})
  flowx("apply_answers", {"report_path": "...", "answers": ["id=value"], "output_dir": "..."})
  flowx("package", {"output_dir": "...", "catalog": "main", "schema": "default"})
For a reviewed Airflow leaf gap, call `resolve_agentic` with action `prepare`, then `stage` with
provider-authored candidates, then `apply` with an explicit `accept_gap` allowlist.
Or run it all at once:
  flowx("migrate", {"source": "airflow", "airflow_source_path": "...", "output_dir": "...",
                    "catalog": "...", "schema": "..."})

All phases share one output_dir. Provide source paths and output_dir as locations the server can
read/write (a local path, or a Unity Catalog Volume path when the host has volume access).
"""


def _phase_result(result: runner.AdapterResult, output_dir: Path, **extra: Any) -> dict[str, Any]:
    """Assemble a structured tool result from an adapter run plus artifacts."""
    payload: dict[str, Any] = {"ok": result.ok, "process": result.as_dict(), "output_dir": str(output_dir)}
    payload.update({k: v for k, v in extra.items() if v is not None})
    return payload


def _source_name(p: dict[str, Any]) -> str:
    """The migration source for a command; required as a string (no default).

    Raises ``KeyError`` when absent and ``ValueError`` when non-string; the dispatcher
    surfaces both as a clear error rather than coercing e.g. ``123`` to ``"123"``.
    """
    source = p["source"]
    if not isinstance(source, str):
        raise ValueError(f"'source' must be a string, got {type(source).__name__}")
    return source


def _resolve_source(p: dict[str, Any], path_key: str | None = None) -> tuple[str | None, Callable[[], None]]:
    """Resolve the migration source for a command into a local path the adapter can read.

    ADF input modes, in priority order — a hosted app can't read the user's files directly, so it
    relies on the first three:

    1. ``adf_volume_path`` — a UC Volume directory; the server downloads it via the SDK Files API.
    2. ``adf_workspace_path`` — a ``/Workspace`` directory (e.g. an ADF Git folder); downloaded via
       the SDK Workspace API. Both (1) and (2) scale to large factories — the bytes bypass the agent.
    3. ``adf_definitions`` — an inline ARM-JSON payload (small jobs); materialized to a temp dir.
    4. ``<source>_source_path`` (e.g. ``airflow_source_path``) or the explicit ``path_key`` — a path
       the server itself can read. ``path_key`` is an *additional* key to try (e.g. ``source_dir``),
       not a replacement, so the source's natural key still resolves.

    For ``source="airflow"`` the volume/workspace/inline modes are ADF-specific and skipped; the DAG
    path is read from ``airflow_source_path`` (or the explicit ``path_key``).
    """
    source = _source_name(p)
    if source == "adf":
        if p.get("adf_volume_path"):
            src = runner.download_volume_dir(p["adf_volume_path"])
            return src, lambda: runner.cleanup_materialized(src)
        if p.get("adf_workspace_path"):
            src = runner.download_workspace_dir(p["adf_workspace_path"])
            return src, lambda: runner.cleanup_materialized(src)
        definitions = p.get("adf_definitions")
        if definitions:
            src = runner.materialize_adf_definitions(definitions)
            return src, lambda: runner.cleanup_materialized(src)
    candidate_keys = [f"{source}_source_path"]
    if path_key:
        candidate_keys.append(path_key)
    resolved = next((p[key] for key in candidate_keys if p.get(key)), None)
    return resolved, (lambda: None)


def _bundle_output(p: dict[str, Any], out: Path) -> dict[str, Any]:
    """Deliver the generated bundle to a location the user can reach.

    The server's ``output_dir`` is local/ephemeral, so the bundle is written to the target via the
    SDK (contents bypass the agent and it scales), in priority order:

    1. ``output_volume_path`` — upload to a UC Volume via the SDK Files API.
    2. ``output_workspace_path`` — upload to a ``/Workspace`` directory via the SDK Workspace API.
    3. neither — return the contents inline as ``bundle`` for the agent to persist (small bundles).
    """
    if p.get("output_volume_path"):
        return {"bundle_uploaded": runner.upload_tree_to_volume(out, p["output_volume_path"])}
    if p.get("output_workspace_path"):
        return {"bundle_uploaded": runner.upload_tree_to_workspace(out, p["output_workspace_path"])}
    return {"bundle": runner.read_tree(out)}


def _noop() -> None:
    """Cleanup placeholder used when there is no materialized source to remove."""


def _pending_options(inspect_result: dict[str, Any]) -> list[dict[str, Any]]:
    """Extract the per-pipeline configuration options still awaiting an answer.

    Reads the payload :func:`_cmd_inspect` returns (``{"questions": {"pipelines": [...]}}``)
    and keeps only pipelines that still have unanswered ``options`` — empty when nothing needs
    input, which is the signal for ``migrate`` to package without pausing.
    """
    questions = inspect_result.get("questions") or {}
    pipelines = questions.get("pipelines") or []
    return [pipeline for pipeline in pipelines if pipeline.get("options")]


# Command handlers: map a `parameters` dict to a structured result; required keys via p[...] so a
# missing one raises KeyError, which the dispatcher converts into a clear error.


def _excluded_dags(parameters: dict[str, Any]) -> list[str]:
    """Normalizes the MCP repeatable exclusion parameter."""
    value = parameters.get("exclude_dag") or parameters.get("exclude_dags") or []
    if isinstance(value, str):
        return [value]
    return [str(item) for item in value]


def _cmd_inputs(p: dict[str, Any]) -> dict[str, Any]:
    phase = p["phase"]
    args = ["inputs", phase]
    # package is source-independent; discover/convert prompts are source-specific (source required).
    if phase != "package":
        args += ["--source", _source_name(p)]
    result = runner.run_adapter(args)
    return {"ok": result.ok, "inputs": runner.parse_stdout_json(result), "process": result.as_dict()}


def _cmd_discover(p: dict[str, Any]) -> dict[str, Any]:
    output_dir = p.get("output_dir", "./flowx_output")
    source_name = _source_name(p)
    source, cleanup = _resolve_source(p)
    if not source:
        return {
            "ok": False,
            "error": f"Provide a source path for source '{source_name}' (e.g. '{source_name}_source_path').",
        }
    try:
        args = ["discover", "--source", source_name, "--source-path", source, "--output-dir", output_dir]
        if p.get("pipeline"):
            args += ["--pipeline", p["pipeline"]]
        for dag_id in _excluded_dags(p):
            args += ["--exclude-dag", dag_id]
        result = runner.run_adapter(args)
        out = Path(output_dir)
        return _phase_result(result, out, inventory=runner.summarize_inventory(out))
    finally:
        cleanup()


def _cmd_convert(p: dict[str, Any]) -> dict[str, Any]:
    output_dir = p.get("output_dir", "./flowx_output")
    source_name = _source_name(p)
    source, cleanup = _resolve_source(p)
    try:
        args = ["convert", "--source", source_name, "--output-dir", output_dir]
        if source:
            args += ["--source-path", source]
        if p.get("pipeline"):
            args += ["--pipeline", p["pipeline"]]
        for dag_id in _excluded_dags(p):
            args += ["--exclude-dag", dag_id]
        result = runner.run_adapter(args)
        out = Path(output_dir)
        return _phase_result(result, out, translation=runner.summarize_translation(out))
    finally:
        cleanup()


def _cmd_merge_agentic(p: dict[str, Any]) -> dict[str, Any]:
    source_name = _source_name(p)
    if source_name == "airflow":
        return {
            "ok": False,
            "error": "Airflow agentic merge is disabled; use the fingerprint-bound resolve_agentic workflow.",
        }
    args = [
        "convert",
        "--source",
        source_name,
        "--merge-agentic",
        "--report",
        p["report_path"],
        "--agentic-results",
        p["agentic_results_dir"],
    ]
    if p.get("output_path"):
        args += ["--output", p["output_path"]]
    result = runner.run_adapter(args)
    return {"ok": result.ok, "process": result.as_dict()}


def _cmd_resolve_agentic(p: dict[str, Any]) -> dict[str, Any]:
    source_name = _source_name(p)
    if source_name != "airflow":
        return {"ok": False, "error": "resolve_agentic is not enabled for ADF; ADF uses the legacy merge path."}
    action = p["action"]
    if action not in {"prepare", "stage", "apply"}:
        return {"ok": False, "error": "resolve_agentic action must be prepare, stage, or apply."}
    output_dir = Path(p.get("output_dir", "./flowx_output"))
    args: list[Any] = ["resolve-agentic", action, "--source", "airflow", "--output-dir", output_dir]
    if p.get("airflow_source_path"):
        args += ["--source-path", p["airflow_source_path"]]
    if p.get("report_path"):
        args += ["--report", p["report_path"]]
    if p.get("dbt_mode"):
        args += ["--dbt-mode", p["dbt_mode"]]
    accepted_gaps = p.get("accept_gap") or p.get("accept_gaps") or []
    if isinstance(accepted_gaps, str):
        accepted_gaps = [accepted_gaps]
    for gap_id in accepted_gaps:
        args += ["--accept-gap", gap_id]
    if p.get("accept_all"):
        args.append("--accept-all")
    if p.get("review_complete"):
        args.append("--review-complete")
    if p.get("review_manifest"):
        args += ["--review-manifest", p["review_manifest"]]
    if p.get("reset"):
        args.append("--reset")
    if p.get("replace"):
        args.append("--replace")

    raw_candidate_paths = p.get("candidate_paths") or []
    candidate_paths = [raw_candidate_paths] if isinstance(raw_candidate_paths, str) else list(raw_candidate_paths)
    inline_candidates = p.get("candidates") or []
    if isinstance(inline_candidates, dict):
        inline_candidates = [inline_candidates]
    with tempfile.TemporaryDirectory(prefix="flowx-agentic-candidates-") as temporary:
        for index, candidate in enumerate(inline_candidates):
            inline_path = Path(temporary) / f"candidate-{index}.json"
            inline_path.write_text(json.dumps(candidate, indent=2), encoding="utf-8")
            candidate_paths.append(str(inline_path))
        for candidate_path in candidate_paths:
            args += ["--candidate", candidate_path]
        result = runner.run_adapter(args)
    payload = runner.parse_stdout_json(result)
    extra: dict[str, Any] = {"result": payload}
    if action == "prepare":
        extra["gaps"] = runner.read_json(output_dir / ".work" / "agentic" / "gaps.json")
    return {"ok": result.ok, "process": result.as_dict(), **extra}


def _cmd_inspect(p: dict[str, Any]) -> dict[str, Any]:
    args: list[Any] = ["inspect", p["report_path"]]
    for answer in p.get("answers") or []:
        args += ["--answer", answer]
    result = runner.run_adapter(args)
    return {"ok": result.ok, "questions": runner.parse_stdout_json(result), "process": result.as_dict()}


def _cmd_apply_answers(p: dict[str, Any]) -> dict[str, Any]:
    args: list[Any] = ["modify", p["report_path"]]
    for answer in p["answers"]:
        args += ["--answer", answer]
    if p.get("output_dir"):
        args += ["--output-dir", p["output_dir"]]
    if p.get("lookup_csv"):
        args += ["--lookup-csv", p["lookup_csv"]]
    result = runner.run_adapter(args)
    return {"ok": result.ok, "process": result.as_dict()}


def _cmd_materialize_lookup(p: dict[str, Any]) -> dict[str, Any]:
    result = runner.run_adapter(["materialize-lookup", p["source"], "--out", p["out"]])
    return {"ok": result.ok, "out": p["out"], "process": result.as_dict()}


def _cmd_workspace_paths(p: dict[str, Any]) -> dict[str, Any]:
    args: list[Any] = ["workspace-paths", p["report_path"], "--source", _source_name(p)]
    source, cleanup = _resolve_source(p, path_key="source_dir")
    try:
        if source:
            args += ["--source-dir", source]
        result = runner.run_adapter(args)
        return {"ok": result.ok, "result": runner.parse_stdout_json(result), "process": result.as_dict()}
    finally:
        cleanup()


def _cmd_package(p: dict[str, Any]) -> dict[str, Any]:
    output_dir = p.get("output_dir", "./flowx_output")
    args: list[Any] = [
        "package",
        "--output-dir",
        output_dir,
        "--catalog",
        p.get("catalog", "main"),
        "--schema",
        p.get("schema", "default"),
    ]
    if p.get("report_path"):
        args += ["--report", p["report_path"]]
    if p.get("bundle_name"):
        args += ["--bundle-name", p["bundle_name"]]
    if p.get("profile"):
        args += ["--profile", p["profile"]]
    if p.get("download_workspace_files") is False:
        args += ["--no-download-workspace-files"]
    if p.get("keep_intermediates"):
        args += ["--keep-intermediates"]
    result = runner.run_adapter(args)
    out = Path(output_dir)
    setup_md = runner.read_text(out / "SETUP.md") or runner.read_text(out / "setup" / "SETUP.md")
    extra = _bundle_output(p, out) if result.ok else {}
    return _phase_result(result, out, bundle_files=runner.list_tree(out), setup_md=setup_md, **extra)


def _cmd_migrate(p: dict[str, Any]) -> dict[str, Any]:
    """Run discover→convert→package, pausing for configuration when options are available.

    Because an MCP call can't prompt mid-flight, ``migrate`` is interactive by *handing the questions
    back to the agent*: after ``convert`` it returns the **full option schema** once
    (``status="needs_input"`` with ``pending_options`` -- every option annotated with a ``show_when``
    condition). The agent drives the whole chain locally (asking only the options whose ``show_when``
    is satisfied, performing any data lookups), then re-calls ``migrate`` a single time with the
    complete ``answers`` (``["option_id=value", ...]``), which applies them and packages
    (``status="completed"``). No per-follow-up round trip. Pass ``interactive=False`` to skip the
    prompt and package with defaults.
    """
    output_dir = p.get("output_dir", "./flowx_output")
    source_name = _source_name(p)
    catalog = p.get("catalog", "main")
    schema = p.get("schema", "default")
    pipeline = p.get("pipeline")
    excluded_dags = _excluded_dags(p)
    answers = p.get("answers") or []
    interactive = p.get("interactive", True)
    out = Path(output_dir)
    report_path = str(out / ".work" / "translation_report.json")
    steps: dict[str, Any] = {}

    # Resume: with answers in hand and a prior report present, skip re-running discover/convert.
    resume = bool(answers) and (out / ".work" / "translation_report.json").is_file()

    cleanup = _noop
    try:
        if not resume:
            source, cleanup = _resolve_source(p)
            if not source:
                return {
                    "ok": False,
                    "error": (
                        f"Provide a source path for source '{source_name}' "
                        "(adf: adf_volume_path / adf_workspace_path / adf_definitions / adf_source_path; "
                        "airflow: airflow_source_path)."
                    ),
                }
            discover_args = ["discover", "--source", source_name, "--source-path", source, "--output-dir", output_dir]
            if pipeline:
                discover_args += ["--pipeline", pipeline]
            for dag_id in excluded_dags:
                discover_args += ["--exclude-dag", dag_id]
            discover_res = runner.run_adapter(discover_args)
            steps["discover"] = _phase_result(discover_res, out, inventory=runner.summarize_inventory(out))
            if not discover_res.ok:
                return {"ok": False, "status": "failed", "failed_phase": "discover", "steps": steps}

            convert_args = ["convert", "--source", source_name, "--output-dir", output_dir, "--source-path", source]
            if pipeline:
                convert_args += ["--pipeline", pipeline]
            for dag_id in excluded_dags:
                convert_args += ["--exclude-dag", dag_id]
            convert_res = runner.run_adapter(convert_args)
            steps["convert"] = _phase_result(convert_res, out, translation=runner.summarize_translation(out))
            if not convert_res.ok:
                return {"ok": False, "status": "failed", "failed_phase": "convert", "steps": steps}

        # Interactive gate: on the first (answerless) call, hand the full option schema to the agent.
        if interactive and not answers:
            options_schema = _pending_options(_cmd_inspect({"report_path": report_path}))
            if options_schema:
                return {
                    "ok": True,
                    "status": "needs_input",
                    "pending_options": options_schema,
                    "report_path": report_path,
                    "output_dir": output_dir,
                    "steps": steps,
                    "message": (
                        "Configuration options are available. Each option carries a `show_when` "
                        "condition (a list of {option_id, in:[values]} clauses; empty = always). Ask "
                        "only the options whose `show_when` clauses are all satisfied by the answers "
                        "collected so far, validating each answer against its `choices`. When the user "
                        "has answered every applicable option, call migrate again with the full "
                        "`answers` list (['option_id=value', ...]) to apply and package in one shot. To "
                        "accept defaults and skip prompting, call migrate with interactive=false."
                    ),
                }

        # No (more) pending options: stamp the collected answers (if any) then package.
        if answers:
            apply_res = _cmd_apply_answers(
                {
                    "report_path": report_path,
                    "answers": answers,
                    "output_dir": output_dir,
                    "lookup_csv": p.get("lookup_csv"),
                }
            )
            steps["apply_answers"] = apply_res
            if not apply_res.get("ok"):
                return {"ok": False, "status": "failed", "failed_phase": "apply_answers", "steps": steps}

        package_res = runner.run_adapter(
            ["package", "--output-dir", output_dir, "--catalog", catalog, "--schema", schema]
        )
        extra = _bundle_output(p, out) if package_res.ok else {}
        steps["package"] = _phase_result(package_res, out, bundle_files=runner.list_tree(out), **extra)
        return {
            "ok": package_res.ok,
            "status": "completed" if package_res.ok else "failed",
            "failed_phase": None if package_res.ok else "package",
            "steps": steps,
        }
    finally:
        cleanup()


def _cmd_record_results(p: dict[str, Any]) -> dict[str, Any]:
    args: list[Any] = ["record-results", "--output-dir", p["output_dir"], "--results-table", p["results_table"]]
    if p.get("warehouse_id"):
        args += ["--warehouse-id", p["warehouse_id"]]
    result = runner.run_adapter(args)
    return {"ok": result.ok, "process": result.as_dict()}


def _cmd_install_dashboard(p: dict[str, Any]) -> dict[str, Any]:
    args: list[Any] = ["install-dashboard", "--results-table", p["results_table"]]
    if p.get("warehouse_id"):
        args += ["--warehouse-id", p["warehouse_id"]]
    if p.get("dashboard_name"):
        args += ["--dashboard-name", p["dashboard_name"]]
    if p.get("parent_path"):
        args += ["--parent-path", p["parent_path"]]
    result = runner.run_adapter(args)
    return {"ok": result.ok, "result": runner.parse_stdout_json(result), "process": result.as_dict()}


_COMMANDS: dict[str, Callable[[dict[str, Any]], dict[str, Any]]] = {
    "inputs": _cmd_inputs,
    "discover": _cmd_discover,
    "convert": _cmd_convert,
    "merge_agentic": _cmd_merge_agentic,
    "resolve_agentic": _cmd_resolve_agentic,
    "inspect": _cmd_inspect,
    "apply_answers": _cmd_apply_answers,
    "materialize_lookup": _cmd_materialize_lookup,
    "workspace_paths": _cmd_workspace_paths,
    "package": _cmd_package,
    "migrate": _cmd_migrate,
    "record_results": _cmd_record_results,
    "install_dashboard": _cmd_install_dashboard,
}


def build_server() -> FastMCP:
    """Construct and return the flowx :class:`FastMCP` server with the single dispatcher tool.

    ``stateless_http=True`` is required by Databricks Genie Code (no persistent ``Mcp-Session-Id``
    round-trip). ``streamable_http_path="/mcp"`` pins the transport to ``/mcp`` (Genie expects the
    server at ``<app-url>/mcp``). ``transport_security`` disables the SDK's DNS-rebinding Origin/Host
    check (see :func:`_transport_security`).
    """
    mcp = FastMCP(
        "flowx",
        instructions=_INSTRUCTIONS,
        stateless_http=True,
        streamable_http_path="/mcp",
        transport_security=_transport_security(),
    )

    # structured_output=False: suppress the auto-derived outputSchema (Genie Code rejects tools that
    # declare one); the dict is still returned as JSON text. See "MCP server design notes" in AGENTS.md.
    @mcp.tool(structured_output=False)
    def flowx(command: str, parameters: dict[str, Any] | None = None) -> dict[str, Any]:
        """Run an flowx source→Databricks migration command (source: Azure Data Factory or Apache Airflow).

        Call as ``flowx(command="<command>", parameters={...})``. Commands and their
        ``parameters`` keys (req = required; phases share ``output_dir``, default "./flowx_output").
        ``source`` ("adf" | "airflow") is **required** for discover/convert/migrate/inputs (and
        workspace_paths); there is no default. It selects both the parser and which source-path key
        applies: ADF reads adf_volume_path | adf_workspace_path | adf_definitions | adf_source_path,
        Airflow reads ``airflow_source_path`` (a DAG .py file or directory). ``package`` is
        source-independent (it consumes the translation report).

        - "inputs": phase(req: "discover"|"convert"|"package"), source(req for discover/convert) —
          list a phase's input prompts.
        - "discover": source(req), one ADF source key | airflow_source_path (req), output_dir,
          pipeline, exclude_dag | exclude_dags (Airflow, repeatable list) — parse and audit definitions.
        - "convert": source(req), (one ADF source key | airflow_source_path), output_dir, pipeline,
          exclude_dag | exclude_dags (Airflow, repeatable list).
        - "merge_agentic": source(req: "adf"), report_path(req), agentic_results_dir(req), output_path —
          merge ADF agent results. Airflow's legacy name-based merge is disabled; use resolve_agentic.
        - "resolve_agentic": source(req: "airflow"), action(req: prepare | stage | apply), output_dir,
          airflow_source_path, report_path, candidates, replace, accept_gap | accept_gaps, accept_all,
          review_complete, review_manifest, reset —
          prepare, stage, and explicitly apply fingerprint-bound Airflow leaf-gap resolutions.
        - "inspect": report_path(req) — return the full translation-option schema (every option with
          a `show_when` condition) for the agent to walk locally. See "Collecting options" below.
        - "apply_answers": report_path(req), answers(req, list of "ID=VALUE"), output_dir, lookup_csv.
        - "materialize_lookup": source(req: CSV path or literal CSV), out(req: destination JSON path).
        - "workspace_paths": source(req), report_path(req), (one ADF source key | airflow_source_path
          | source_dir).
        - "package": output_dir, output_volume_path, output_workspace_path, report_path,
          catalog(default "main"), schema(default "default"), bundle_name, profile,
          download_workspace_files(bool), keep_intermediates(bool).
        - "migrate": source(req), one ADF source key | airflow_source_path (req), output_dir,
          output_volume_path, output_workspace_path, catalog, schema, pipeline,
          exclude_dag | exclude_dags (Airflow, repeatable list),
          answers(list of "ID=VALUE"), interactive(bool, default true), lookup_csv — runs
          discover→convert→package, returning the full option schema once (status "needs_input") when
          configuration is available; re-call once with the complete answers to apply (see below).
        - "record_results": output_dir(req), results_table(req: catalog.schema.table), warehouse_id.
        - "install_dashboard": results_table(req), warehouse_id, dashboard_name, parent_path.

        Providing the source (a hosted app can't read the user's workspace/volume files directly). For
        ``source="airflow"`` pass ``airflow_source_path`` (a DAG .py file or directory the server can
        read). For ``source="adf"``, in priority order:
        - ``adf_volume_path``: a UC Volume directory the server reads via the SDK Files API. **Preferred
          for large factories** — the bytes never pass through the agent. Requires the app's service
          principal to have read on the volume.
        - ``adf_workspace_path``: a ``/Workspace`` directory (e.g. an ADF Git folder) the server reads
          via the SDK Workspace API. Also scales (bytes bypass the agent); needs SP read on that path.
        - ``adf_definitions``: an inline mapping of relative path → JSON content mirroring the ADF
          Git-export layout, e.g. {"pipeline/Foo.json": {...}, "linkedService/Bar.json": {...}} (a single
          ARM-template object is also accepted). Convenient for small jobs; capped (~5 MB) since it flows
          through the agent's context — over the cap, switch to ``adf_volume_path``.
        - ``adf_source_path`` / ``source_dir``: a path the server itself can read (local hosting / mounted volume).

        Delivering the generated DAB (the server's output_dir is local/ephemeral, so "package"/"migrate"
        write it to the target via the SDK — the contents bypass the agent):
        - Set ``output_volume_path`` to upload the bundle to a UC Volume (SDK Files API), or
          ``output_workspace_path`` to upload it to a ``/Workspace`` directory (SDK Workspace API, files
          written verbatim). Either returns ``bundle_uploaded`` = {"output_volume_path" or
          "output_workspace_path", "files":[...], "count"}. **Preferred** — required for large bundles.
        - With neither set, they return ``bundle`` = {"files": {relpath: text, ...}, "truncated": [...]}
          inline for the agent to persist (small bundles only; capped).

        Collecting options (agent-driven chain):
        - The server returns the **full option schema** in one shot — it never runs a multi-step
          prompt loop itself. "inspect" (and "migrate" on its first, answerless call via
          ``status="needs_input"``) returns ``pending_options`` =
          ``[{"pipeline_name", "options":[{option_id, prompt, rationale, choices, free_text, default,
          show_when}, ...]}, ...]``, where ``show_when`` is a list of ``{option_id, in:[values]}``
          clauses (empty = always shown).
        - The **agent** drives the conversation locally: ask an option only when every ``show_when``
          clause is satisfied by the answers gathered so far (e.g. ``notify_slack_url`` shows once
          ``notify_destination=slack``); validate each answer against ``choices`` (``free_text`` options
          accept any value); perform any data action (e.g. run the lookup query when
          ``metadata_driven_lookup_tool=have``). No round trip per follow-up.
        - When every applicable option is answered, submit **once**: "migrate" re-called with the full
          ``answers`` (applies + packages), or standalone "apply_answers" → "package". The server still
          validates every answer at apply time. ``interactive=false`` on "migrate" skips prompting.

        Returns a dict ``{"ok": bool, ...}`` with per-command summaries (inventory / translation /
        bundle_files / questions / result) and a "process" block (stdout/stderr/returncode). An unknown
        command, missing required parameter, or an oversized inline payload returns
        ``{"ok": false, "error": ...}``.

        Args:
            command: The operation to run (see the list above).
            parameters: Operation-specific keyword arguments.
        """
        handler = _COMMANDS.get(command)
        if handler is None:
            return {"ok": False, "error": f"Unknown command {command!r}. Valid commands: {', '.join(_COMMANDS)}."}
        try:
            return handler(parameters or {})
        except KeyError as missing:
            return {"ok": False, "error": f"Missing required parameter {missing} for command {command!r}."}
        except ValueError as error:
            return {"ok": False, "error": str(error)}

    return mcp


def build_http_app() -> Any:
    """Builds the streamable-HTTP ASGI app for hosting (Databricks Apps / Genie Code).

    Returns:
        FastMCP's own streamable-HTTP app, serving ``/mcp`` plus ``/`` and ``/health`` routes, with
        CORS attached (origins from ``FLOWX_ALLOWED_ORIGINS``, default ``*``).

    Notes:
        Returns FastMCP's *own* app rather than mounting it inside another Starlette app: mounting
        drops the sub-app's lifespan, leaving the StreamableHTTP session manager uninitialized so
        every ``/mcp`` request 500s. See the "MCP server design notes" in AGENTS.md.
    """
    from starlette.middleware.cors import CORSMiddleware
    from starlette.responses import JSONResponse

    mcp = build_server()

    @mcp.custom_route("/", methods=["GET"])
    async def health(_request: Any) -> JSONResponse:
        return JSONResponse({"status": "ok", "service": "mcp-flowx"})

    @mcp.custom_route("/health", methods=["GET"])
    async def health_alias(_request: Any) -> JSONResponse:
        return JSONResponse({"status": "ok", "service": "mcp-flowx"})

    # FastMCP's own app — its lifespan starts the StreamableHTTP session manager.
    app = mcp.streamable_http_app()

    allow_origins = _allowed_origins()
    # Credentialed requests cannot use the "*" wildcard per the CORS spec.
    allow_credentials = allow_origins != ["*"]
    app.add_middleware(
        CORSMiddleware,
        allow_origins=allow_origins,
        allow_credentials=allow_credentials,
        allow_methods=["*"],
        allow_headers=["*"],
        expose_headers=["Mcp-Session-Id"],
    )
    return app


def serve() -> None:
    """Entry point used by ``python -m flowx.mcp``.

    Defaults to stdio (local agents). With ``--http`` (or FLOWX_MCP_HTTP=1) it serves
    the streamable-HTTP app via uvicorn on ``--port`` / ``$DATABRICKS_APP_PORT`` / 8000.
    """
    import argparse

    parser = argparse.ArgumentParser(prog="python -m flowx.mcp", description="Run the flowx MCP server.")
    parser.add_argument("--http", action="store_true", help="Serve over streamable HTTP instead of stdio.")
    parser.add_argument("--host", default="0.0.0.0", help="Bind host for --http mode.")
    parser.add_argument(
        "--port",
        type=int,
        default=int(os.environ.get("DATABRICKS_APP_PORT", "8000")),
        help="Bind port for --http mode (defaults to $DATABRICKS_APP_PORT or 8000).",
    )
    args = parser.parse_args()

    if args.http or os.environ.get("FLOWX_MCP_HTTP") == "1":
        import uvicorn

        uvicorn.run(build_http_app(), host=args.host, port=args.port)
    else:
        build_server().run(transport="stdio")
