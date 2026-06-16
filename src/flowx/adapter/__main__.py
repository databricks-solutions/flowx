"""Unified CLI entry point that the flowx skills and MCP tools drive via subprocesses.

Exposes stateless subcommands -- the ``discover``/``convert``/``package`` phase runners plus
``inspect``, ``modify``, ``inputs``, ``materialize-lookup``, ``workspace-paths``, ``record-results``,
and ``install-dashboard`` -- so each agent turn runs as an independent process holding no session
state across user prompts.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

from flowx.adapter.constants import MOTIF_CONSOLIDATE_OPTION_PREFIX
from flowx.adapter.models import (
    DEFAULT_CONFIGURATION,
    CopyActivityParadigm,
    LakeflowConnectorType,
    MetadataDrivenAccess,
    MetadataDrivenConsolidate,
    MetadataDrivenLookupTool,
    MetadataDrivenSize,
    MotifConsolidate,
    NonDatabricksTaskCompute,
    NotifyDestination,
    NotifyEvents,
    TranslationConfiguration,
    UseLakeflowConnectors,
)
from flowx.adapter.operations import (
    apply_configuration,
    build_option_schema,
    collect_notify_args,
    collect_workspace_artifact_paths,
    detect_databricks_hosts,
    provision_notification_destinations,
    validate_answer,
)

# bundler.dab_writer + translator.engine (sqlglot) are imported lazily inside inspect/modify only, so
# the cheap commands (inputs, phase pass-throughs, materialize-lookup, workspace-paths) skip ~0.15s of
# unused import cost on every adapter subprocess.

# Maps the unified phase runner subcommands to the module CLI they forward to.
_PHASE_MODULES: dict[str, str] = {
    "discover": "flowx.parser.adf_loader",
    "convert": "flowx.translator.engine",
    "package": "flowx.bundler.dab_writer",
}
# Aliases so the inputs option ids double as CLI flags on the phase runners.
_PHASE_FLAG_ALIASES: dict[str, str] = {
    "--adf-source-path": "--source-dir",
}


def main(argv: list[str] | None = None) -> int:
    """Dispatches an ``inspect`` or ``modify`` subcommand.

    Args:
        argv: CLI arguments to parse.  Defaults to :data:`sys.argv` when
            ``None``.

    Returns:
        Exit code (0 on success, non-zero on usage or runtime errors).
    """
    raw_args = list(sys.argv[1:]) if argv is None else list(argv)
    if raw_args and raw_args[0] in _PHASE_MODULES:
        # Phase runners are pure pass-through to the underlying phase CLI;
        # bypass argparse so forwarded --flags aren't misparsed at this level.
        return _run_phase(raw_args[0], raw_args[1:])

    parser = _build_parser()
    args = parser.parse_args(argv)
    if args.command == "inspect":
        return _run_inspect(args)
    if args.command == "modify":
        return _run_modify(args)
    if args.command == "materialize-lookup":
        return _run_materialize_lookup(args)
    if args.command == "inputs":
        return _run_inputs(args)
    if args.command == "workspace-paths":
        return _run_workspace_paths(args)
    if args.command == "record-results":
        return _run_record_results(args)
    if args.command == "install-dashboard":
        return _run_install_dashboard(args)
    parser.print_help(sys.stderr)
    return 2


def _run_record_results(args: argparse.Namespace) -> int:
    """Implements ``record-results``: write per-pipeline coverage to a UC table.

    Returns 0 on success, 1 when the metadata cannot be read or the write fails.
    """
    from flowx.reporting.results import write_results

    metadata_dir = args.output_dir / "metadata"
    if not (metadata_dir / "inventory.json").exists():
        print(f"No inventory.json under {metadata_dir}; run the discover phase first.", file=sys.stderr)
        return 1
    try:
        run_id, rows = write_results(metadata_dir, args.results_table, warehouse_id=args.warehouse_id)
    except Exception as error:  # noqa: BLE001 - surface an actionable message to the agent
        print(f"Failed to record results to {args.results_table}: {error}", file=sys.stderr)
        return 1
    if not rows:
        print("No pipelines found to record.", file=sys.stderr)
        return 1
    print(f"Recorded {rows} pipeline row(s) to {args.results_table} (run_id={run_id}).")
    return 0


def _run_install_dashboard(args: argparse.Namespace) -> int:
    """Implements ``install-dashboard``: create + publish the coverage dashboard.

    Returns 0 on success, 1 when the dashboard could not be created.
    """
    from flowx.reporting.dashboard import install_dashboard

    try:
        dashboard_id, url = install_dashboard(
            args.results_table,
            warehouse_id=args.warehouse_id,
            display_name=args.dashboard_name,
            parent_path=args.parent_path,
        )
    except Exception as error:  # noqa: BLE001 - surface an actionable message to the agent
        print(f"Failed to install dashboard for {args.results_table}: {error}", file=sys.stderr)
        return 1
    print(f"Installed coverage dashboard (id={dashboard_id}).")
    if url:
        print(f"  {url}")
    return 0


def _run_workspace_paths(args: argparse.Namespace) -> int:
    """Implements the ``workspace-paths`` subcommand.

    Args:
        args: Parsed CLI namespace carrying ``report``, ``source_dir``,
            and ``out``.

    Returns:
        ``0`` on success.  The command always succeeds when the report
        can be read; missing or unreadable inputs simply produce empty
        path / host lists so the skill can detect the no-op case.
    """
    paths = collect_workspace_artifact_paths(args.report)
    suggested_hosts = detect_databricks_hosts(args.source_dir) if args.source_dir else []
    payload = {
        "paths": paths,
        "suggested_hosts": suggested_hosts,
        "needs_auth": bool(paths),
    }
    _emit_json(payload, args.out)
    return 0


def _run_inputs(args: argparse.Namespace) -> int:
    """Implements the ``inputs`` subcommand.

    Args:
        args: Parsed CLI namespace carrying ``phase`` and ``out``.

    Returns:
        ``0`` on success.  The CLI never raises here because the phase
        argument is constrained by argparse.
    """
    from flowx.adapter.session import MigrationInputSession

    session = MigrationInputSession(phase=args.phase)
    pending = session.pending()
    payload = {
        "phase": pending.phase,
        "options": [
            {
                "option_id": option.option_id,
                "prompt": option.prompt,
                "description": option.description,
                "default": option.default,
                "required": option.required,
            }
            for option in pending.options
        ],
    }
    _emit_json(payload, args.out)
    return 0


def _build_parser() -> argparse.ArgumentParser:
    """Builds the top-level argparse parser with the two subcommands.

    Returns:
        Configured :class:`argparse.ArgumentParser`.
    """
    parser = argparse.ArgumentParser(
        prog="python -m flowx.adapter",
        description="Inspect and modify a translated flowx pipeline IR.",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    inspect = subparsers.add_parser(
        "inspect",
        help="Emit the full translation-option schema for a report as JSON.",
    )
    inspect.add_argument("report", type=Path, help="Path to the translation report or pipeline IR JSON.")
    inspect.add_argument(
        "--answer",
        action="append",
        default=[],
        metavar="OPTION_ID=VALUE",
        help=(
            "Deprecated/no-op: the full option tree (every option with a `show_when` condition) is "
            "always emitted now, so the agent walks the chain locally. Accepted for back-compat and "
            "validated for format only."
        ),
    )
    inspect.add_argument(
        "--out",
        type=Path,
        default=None,
        help="Optional output file; defaults to stdout.",
    )

    modify = subparsers.add_parser(
        "modify",
        help="Apply collected answers to a translation report and write the stamped IR.",
    )
    modify.add_argument("report", type=Path, help="Path to the translation report or pipeline IR JSON.")
    modify.add_argument(
        "--answer",
        action="append",
        default=[],
        metavar="OPTION_ID=VALUE",
        help=(
            "A collected answer as OPTION_ID=VALUE (e.g. --answer notify_destination=email). "
            "Repeatable; pass one per option the user answered. Values may contain '=' (only the "
            "first '=' splits the pair)."
        ),
    )
    modify.add_argument(
        "--output-dir",
        type=Path,
        default=None,
        help=(
            "Migration output directory. The stamped IR is written to its transient "
            ".work/translation_report.stamped.json and the collected answers are written to "
            "metadata/configuration.json. Either --output-dir or --out is required."
        ),
    )
    modify.add_argument(
        "--out",
        type=Path,
        default=None,
        help=("Explicit destination for the configuration-stamped IR JSON (overrides the --output-dir convention)."),
    )
    modify.add_argument(
        "--config-out",
        type=Path,
        default=None,
        help=(
            "Explicit destination for configuration.json (the collected answers). Defaults to "
            "<output-dir>/metadata/configuration.json."
        ),
    )
    modify.add_argument(
        "--lookup-csv",
        type=str,
        default=None,
        help=(
            "Optional CSV file path or literal CSV string of lookup-value rows that consolidated "
            "metadata-driven motifs should ingest. The header row names the columns; each "
            "subsequent row becomes one dict."
        ),
    )

    workspace_paths = subparsers.add_parser(
        "workspace-paths",
        help=(
            "Detect absolute workspace paths in a stamped report and suggest "
            "Databricks workspace hosts from the ADF linked services."
        ),
    )
    workspace_paths.add_argument("report", type=Path, help="Path to the translation report or pipeline IR JSON.")
    workspace_paths.add_argument(
        "--source-dir",
        type=Path,
        default=None,
        help=(
            "Optional path to the ADF JSON export directory.  When supplied, "
            "the command reads ``linked_services/*.json`` to suggest the "
            "workspace host that ``databricks auth login --host`` should use."
        ),
    )
    workspace_paths.add_argument(
        "--out",
        type=Path,
        default=None,
        help="Optional output file; defaults to stdout.",
    )

    inputs = subparsers.add_parser(
        "inputs",
        help="Emit the migration-phase input options for an flowx phase as JSON.",
    )
    inputs.add_argument(
        "phase",
        choices=("discover", "convert", "package"),
        help="Migration phase whose input prompts the agent should surface.",
    )
    inputs.add_argument(
        "--out",
        type=Path,
        default=None,
        help="Optional output file; defaults to stdout.",
    )

    materialize = subparsers.add_parser(
        "materialize-lookup",
        help="Parse CSV-shaped lookup values into the JSON shape modify consumes.",
    )
    materialize.add_argument(
        "source",
        help=(
            "Either a path to a CSV file or a literal CSV string.  The first row "
            "is treated as headers and every subsequent row is emitted as one dict."
        ),
    )
    materialize.add_argument(
        "--out",
        type=Path,
        required=True,
        help="Destination path for the lookup-values JSON list.",
    )

    record = subparsers.add_parser(
        "record-results",
        help="Write per-pipeline migration coverage for this run to a Unity Catalog table.",
    )
    record.add_argument(
        "--output-dir",
        type=Path,
        required=True,
        help="Migration output directory (reads metadata/inventory.json + metadata/profile_report.csv).",
    )
    record.add_argument(
        "--results-table",
        type=str,
        required=True,
        help="Target UC table as catalog.schema.table.",
    )
    record.add_argument(
        "--warehouse-id",
        type=str,
        default=None,
        help="SQL warehouse id for the write. Auto-detected (prefers running serverless) when omitted.",
    )

    dashboard = subparsers.add_parser(
        "install-dashboard",
        help="Create and publish an AI/BI dashboard visualizing coverage from the results table.",
    )
    dashboard.add_argument(
        "--results-table",
        type=str,
        required=True,
        help="UC table the dashboard reads (catalog.schema.table).",
    )
    dashboard.add_argument(
        "--warehouse-id",
        type=str,
        default=None,
        help="SQL warehouse backing the dashboard. Auto-detected when omitted.",
    )
    dashboard.add_argument(
        "--dashboard-name",
        type=str,
        default=None,
        help="Dashboard display name (defaults to 'Migration Coverage \u2014 <table>').",
    )
    dashboard.add_argument(
        "--parent-path",
        type=str,
        default=None,
        help="Workspace folder for the dashboard (defaults to the current user's home).",
    )

    # Unified phase runners: `adapter <phase> -- <flags>` forwards to the phase CLI (one entry point);
    # --adf-source-path is accepted as an alias of the loader/translator --source-dir flag.
    for _phase in ("discover", "convert", "package"):
        _runner = subparsers.add_parser(
            _phase,
            help=f"Run the {_phase} phase (forwards flags to the underlying phase CLI).",
        )
        _runner.add_argument(
            "forward",
            nargs=argparse.REMAINDER,
            help="Flags forwarded to the phase CLI (e.g. --adf-source-path/--source-dir, --output-dir, --pipeline).",
        )

    return parser


def _run_phase(phase: str, forward: list[str]) -> int:
    """Forward a phase runner subcommand to the underlying phase module, **in-process**.

    ``python -m flowx.adapter discover --adf-source-path X --output-dir Y`` runs
    ``flowx.parser.adf_loader.main(["--source-dir", "X", "--output-dir", "Y"])`` in this same
    interpreter -- no second ``python -m`` spawn. The module's ``main(argv)`` reuses the existing,
    tested phase CLI surface, so there is a single entry point with no argument-surface duplication.
    Collapsing the former double-spawn (adapter process -> module process) shaves an interpreter
    start + re-import off every ``discover``/``convert``/``package`` call.

    Args:
        phase: One of ``"discover"`` / ``"convert"`` / ``"package"``.
        forward: Tokens after the phase name (flags for the phase CLI).

    Returns:
        The phase's exit code (0 on success).
    """
    import importlib

    module = importlib.import_module(_PHASE_MODULES[phase])
    mapped = [_PHASE_FLAG_ALIASES.get(token, token) for token in (forward or [])]
    try:
        return module.main(mapped) or 0
    except SystemExit as exit_signal:  # e.g. argparse usage error -> parser.error() raises SystemExit
        code = exit_signal.code
        if isinstance(code, int):
            return code
        return 0 if code is None else 1


def _run_inspect(args: argparse.Namespace) -> int:
    """Implements the ``inspect`` subcommand.

    Args:
        args: Parsed CLI namespace carrying ``report``, ``answers``, and
            ``out``.

    Returns:
        ``0`` when the report was inspected successfully, ``1`` when the
        report could not be loaded.
    """
    pipelines = _load_pipelines(args.report)
    if pipelines is None:
        return 1
    try:
        # The agent now walks the full option tree locally, so --answer no longer filters the output;
        # we still validate its format so a malformed pair is reported rather than silently ignored.
        _parse_answer_args(getattr(args, "answer", []) or [])
    except ValueError as error:
        print(f"Invalid --answer: {error}", file=sys.stderr)
        return 2
    payload = {
        "pipelines": [
            {"pipeline_name": pipeline.name, "options": build_option_schema(pipeline)} for pipeline in pipelines
        ],
    }
    _emit_json(payload, args.out)
    return 0


def _parse_answer_args(pairs: list[str]) -> dict[str, str]:
    """Parses repeatable ``--answer OPTION_ID=VALUE`` CLI args into a mapping.

    Only the first ``=`` splits each pair, so values may themselves contain
    ``=`` (e.g. a query string or base64 token).

    Args:
        pairs: Raw ``OPTION_ID=VALUE`` strings from argparse.

    Returns:
        Mapping of option_id to answer string (later values win on duplicates).

    Raises:
        ValueError: When a token has no ``=`` or an empty option id.
    """
    answers: dict[str, str] = {}
    for pair in pairs:
        if "=" not in pair:
            raise ValueError(f"expected OPTION_ID=VALUE, got {pair!r}")
        key, value = pair.split("=", 1)
        key = key.strip()
        if not key:
            raise ValueError(f"empty option id in {pair!r}")
        answers[key] = value
    return answers


def _resolve_modify_outputs(args: argparse.Namespace) -> tuple[Path, Path] | None:
    """Resolves the (stamped_report_path, configuration_json_path) for ``modify``.

    Honors explicit ``--out`` / ``--config-out`` overrides, otherwise derives both
    from ``--output-dir`` (stamped -> ``.work/``, configuration.json -> ``metadata/``).
    Returns ``None`` when neither ``--output-dir`` nor ``--out`` was supplied.
    """
    output_dir: Path | None = args.output_dir
    stamped = args.out
    if stamped is None:
        if output_dir is None:
            return None
        stamped = output_dir / ".work" / "translation_report.stamped.json"
    config_out = args.config_out
    if config_out is None:
        if output_dir is not None:
            config_out = output_dir / "metadata" / "configuration.json"
        elif stamped.parent.name == ".work":
            config_out = stamped.parent.parent / "metadata" / "configuration.json"
        else:
            config_out = stamped.parent / "configuration.json"
    return stamped, config_out


def _run_modify(args: argparse.Namespace) -> int:
    """Implements the ``modify`` subcommand.

    Args:
        args: Parsed CLI namespace carrying ``report``, ``answers``,
            ``out``, and the optional ``lookup_values``.

    Returns:
        ``0`` when the modified IR was written successfully, ``1`` when
        the report could not be loaded, ``2`` when the answers failed
        validation.
    """
    outputs = _resolve_modify_outputs(args)
    if outputs is None:
        print("modify requires --output-dir (or an explicit --out)", file=sys.stderr)
        return 2
    stamped_out, config_out = outputs

    pipelines = _load_pipelines(args.report)
    if pipelines is None:
        return 1
    try:
        answers = _parse_answer_args(args.answer or [])
        configuration = _configuration_from_answers(answers)
    except ValueError as error:
        print(f"Invalid answers: {error}", file=sys.stderr)
        return 2
    try:
        lookup_values = _parse_csv_source(args.lookup_csv) if args.lookup_csv else []
    except ValueError as error:
        print(f"Invalid --lookup-csv: {error}", file=sys.stderr)
        return 2

    stamped_pipelines = [
        _stamp_lookup_values_into_metadata_driven_motifs(apply_configuration(pipeline, configuration), lookup_values)
        for pipeline in pipelines
    ]
    # Prompt-time provisioning: create/reuse the Databricks notification destination for any non-email
    # activity_and_notify spec now, so its resolved id is baked into the report (email needs none).
    provisioned_pipelines = []
    for pipeline in stamped_pipelines:
        provisioned, messages = provision_notification_destinations(pipeline)
        provisioned_pipelines.append(provisioned)
        for message in messages:
            print(message, file=sys.stderr)
    from flowx.translator.engine import _pipeline_to_dict  # lazy: heavy import (sqlglot)

    modified = [_pipeline_to_dict(pipeline) for pipeline in provisioned_pipelines]
    _write_modified_report(args.report, modified, stamped_out)

    # Persist the collected answers as the kept configuration record.
    config_out.parent.mkdir(parents=True, exist_ok=True)
    config_out.write_text(json.dumps(answers, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(f"Wrote stamped IR to {stamped_out}", file=sys.stderr)
    print(f"Wrote configuration to {config_out}", file=sys.stderr)
    return 0


def _run_materialize_lookup(args: argparse.Namespace) -> int:
    """Implements the ``materialize-lookup`` subcommand.

    Args:
        args: Parsed CLI namespace carrying ``source`` (file path or
            literal CSV string) and ``out``.

    Returns:
        ``0`` when the JSON was written successfully, ``2`` when the
        source could not be parsed as CSV.
    """
    try:
        rows = _parse_csv_source(args.source)
    except ValueError as error:
        print(f"Invalid CSV source: {error}", file=sys.stderr)
        return 2
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(json.dumps(rows, indent=2) + "\n", encoding="utf-8")
    return 0


def _parse_csv_source(source: str) -> list[dict[str, str]]:
    """Parses a CSV file path or literal CSV string into a list of row dicts.

    Args:
        source: Either a path to a CSV file or a literal CSV string with
            a header row.

    Returns:
        List of dicts, one per data row, keyed by the header names.

    Raises:
        ValueError: When the CSV has no header row or is empty.
    """
    import csv

    source_path = Path(source)
    text = source_path.read_text(encoding="utf-8") if source_path.exists() else source
    reader = csv.DictReader(text.splitlines())
    if reader.fieldnames is None:
        raise ValueError("Source CSV is empty or missing a header row")
    return [dict(row) for row in reader]


def _stamp_lookup_values_into_metadata_driven_motifs(pipeline, lookup_values: list[dict[str, Any]]):
    """Stamps lookup values onto every metadata-driven motif marked for consolidation.

    Args:
        pipeline: Configuration-stamped pipeline IR.
        lookup_values: Rows materialised by the agent or the user.

    Returns:
        A new :class:`Pipeline` whose metadata-driven motif activities
        carry the supplied lookup rows.  When *lookup_values* is empty
        the pipeline is returned unchanged.
    """
    if not lookup_values:
        return pipeline
    import dataclasses as _dataclasses

    from flowx.models.ir import MotifActivity as _MotifActivity

    stamped_tasks = []
    for task in pipeline.tasks:
        if isinstance(task, _MotifActivity) and task.consolidate_metadata_driven:
            stamped_tasks.append(_dataclasses.replace(task, lookup_values=list(lookup_values)))
        else:
            stamped_tasks.append(task)
    return _dataclasses.replace(pipeline, tasks=stamped_tasks)


def _load_pipelines(report_path: Path) -> list[Any] | None:
    """Loads every pipeline IR contained in a report file.

    Args:
        report_path: Path to a translation report or pipeline IR JSON.

    Returns:
        List of rehydrated :class:`Pipeline` objects, or ``None`` when
        the file could not be parsed.
    """
    from flowx.bundler.dab_writer import pipeline_dict_to_ir  # lazy: heavy import, only inspect/modify

    try:
        raw = json.loads(report_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        print(f"Failed to read {report_path}: {error}", file=sys.stderr)
        return None
    pipeline_dicts = _extract_pipeline_dicts(raw)
    return [pipeline_dict_to_ir(pipeline_dict)[0] for pipeline_dict in pipeline_dicts]


def _extract_pipeline_dicts(raw: Any) -> list[dict[str, Any]]:
    """Normalises a translation report into a list of pipeline IR dicts.

    Args:
        raw: Parsed JSON content from a report file.

    Returns:
        List of dicts, each in the shape ``engine._pipeline_to_dict``
        produces.  Empty when *raw* does not contain a recognisable
        pipeline payload.
    """
    # Single pipeline IR dict (has both "tasks" and "name" at top level)
    if isinstance(raw, dict) and "tasks" in raw and "name" in raw:
        return [raw]
    # Multi-pipeline wrapper written by engine.py ({"pipelines": [...]})
    if isinstance(raw, dict) and "pipelines" in raw and isinstance(raw["pipelines"], list):
        return [p for p in raw["pipelines"] if isinstance(p, dict) and "tasks" in p and "name" in p]
    # Legacy aggregated translation report shape
    if isinstance(raw, dict) and "translations" in raw:
        return [
            {"name": entry["pipeline"], **entry["ir"]}
            for entry in raw.get("translations", [])
            if entry.get("status") == "translated" and entry.get("ir")
        ]
    return []


def _configuration_from_answers(answers: dict[str, str]) -> TranslationConfiguration:
    """Builds a :class:`TranslationConfiguration` from a validated answers dict.

    Args:
        answers: Validated mapping of option_id to answer string.

    Returns:
        Configuration with every answered field overridden and every
        unanswered field defaulted.

    Raises:
        ValueError: When an answer is not in the allowed set for its
            option.
    """
    validated = {qid: validate_answer(qid, value) for qid, value in answers.items()}
    motif_consolidations: dict[str, MotifConsolidate] = {}
    for qid, value in validated.items():
        if qid.startswith(MOTIF_CONSOLIDATE_OPTION_PREFIX):
            motif_consolidations[qid[len(MOTIF_CONSOLIDATE_OPTION_PREFIX) :]] = MotifConsolidate(value)
    return TranslationConfiguration(
        copy_activity_paradigm=CopyActivityParadigm(
            validated.get("copy_activity_paradigm", DEFAULT_CONFIGURATION.copy_activity_paradigm)
        ),
        non_databricks_task_compute=NonDatabricksTaskCompute(
            validated.get("non_databricks_task_compute", DEFAULT_CONFIGURATION.non_databricks_task_compute)
        ),
        use_lakeflow_connectors=UseLakeflowConnectors(
            validated.get("use_lakeflow_connectors", DEFAULT_CONFIGURATION.use_lakeflow_connectors)
        ),
        lakeflow_connector_type=LakeflowConnectorType(
            validated.get("lakeflow_connector_type", DEFAULT_CONFIGURATION.lakeflow_connector_type)
        ),
        metadata_driven_consolidate=MetadataDrivenConsolidate(
            validated.get("metadata_driven_consolidate", DEFAULT_CONFIGURATION.metadata_driven_consolidate)
        ),
        metadata_driven_access=MetadataDrivenAccess(
            validated.get("metadata_driven_access", DEFAULT_CONFIGURATION.metadata_driven_access)
        ),
        metadata_driven_size=MetadataDrivenSize(
            validated.get("metadata_driven_size", DEFAULT_CONFIGURATION.metadata_driven_size)
        ),
        metadata_driven_lookup_tool=MetadataDrivenLookupTool(
            validated.get("metadata_driven_lookup_tool", DEFAULT_CONFIGURATION.metadata_driven_lookup_tool)
        ),
        notify_destination=NotifyDestination(
            validated.get("notify_destination", DEFAULT_CONFIGURATION.notify_destination)
        ),
        notify_events=NotifyEvents(validated.get("notify_events", DEFAULT_CONFIGURATION.notify_events)),
        notify_destination_name=validated.get("notify_destination_name", ""),
        notify_args=collect_notify_args(validated),
        motif_consolidations=motif_consolidations,
    )


def _emit_json(payload: dict[str, Any], out: Path | None) -> None:
    """Writes a JSON payload to a file or to stdout.

    Args:
        payload: JSON-serialisable mapping to emit.
        out: Destination path; ``None`` selects stdout.
    """
    encoded = json.dumps(payload, indent=2, default=str)
    if out is None:
        print(encoded)
        return
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(encoded + "\n", encoding="utf-8")


def _write_modified_report(report_path: Path, pipelines: list[dict[str, Any]], out: Path) -> None:
    """Writes the configuration-stamped IR to *out* using the input report's shape.

    Args:
        report_path: Path the modified report was sourced from.  Used
            only to detect whether the input was a single pipeline IR
            or an aggregated translation report.
        pipelines: Stamped pipeline IR dicts to write.
        out: Destination path for the modified report.
    """
    raw = json.loads(report_path.read_text(encoding="utf-8"))
    if isinstance(raw, dict) and "translations" in raw:
        by_name = {pipeline["name"]: pipeline for pipeline in pipelines}
        for entry in raw.get("translations", []):
            stamped = by_name.get(entry.get("pipeline"))
            if stamped is not None and entry.get("ir") is not None:
                entry["ir"] = {key: value for key, value in stamped.items() if key != "name"}
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(json.dumps(raw, indent=2, default=str) + "\n", encoding="utf-8")
        return
    payload = pipelines[0] if len(pipelines) == 1 else {"pipelines": pipelines}
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(payload, indent=2, default=str) + "\n", encoding="utf-8")


if __name__ == "__main__":
    sys.exit(main())
