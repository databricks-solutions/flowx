"""CLI bridge that lets the orchestra skills drive the adapter via subprocesses.

The skills (`/orchestra:translate`, `/orchestra:migrate`) cannot keep a
Python session alive across user prompts, so this module exposes two
stateless subcommands:

* ``inspect`` reads a translation report and emits the pending questions
  as JSON for the agent to surface to the user.
* ``modify`` reads the same report plus a JSON file of answers and writes
  a preference-stamped report the prepare phase consumes verbatim.
"""

from __future__ import annotations

import argparse
import json
import sys
from dataclasses import asdict
from pathlib import Path
from typing import Any

from orchestra.adapter.models import (
    DEFAULT_PREFERENCES,
    CopyActivityParadigm,
    DatabricksTaskCompute,
    LakeflowConnectorType,
    MetadataDrivenAccess,
    MetadataDrivenConsolidate,
    MetadataDrivenLookupTool,
    MetadataDrivenSize,
    NonDatabricksTaskCompute,
    PendingQuestions,
    TranslationPreferences,
    TranslationQuestion,
    UseLakeflowConnectors,
)
from orchestra.adapter.operations import (
    apply_preferences,
    collect_workspace_artifact_paths,
    detect_databricks_hosts,
    gather_questions,
    validate_answer,
)
from orchestra.bundler.dab_writer import pipeline_dict_to_ir
from orchestra.translator.engine import _pipeline_to_dict


def main(argv: list[str] | None = None) -> int:
    """Dispatches an ``inspect`` or ``modify`` subcommand.

    Args:
        argv: CLI arguments to parse.  Defaults to :data:`sys.argv` when
            ``None``.

    Returns:
        Exit code (0 on success, non-zero on usage or runtime errors).
    """
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
    parser.print_help(sys.stderr)
    return 2


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
    from orchestra.adapter.session import MigrationInputSession

    session = MigrationInputSession(phase=args.phase)
    pending = session.pending()
    payload = {
        "phase": pending.phase,
        "questions": [
            {
                "question_id": question.question_id,
                "prompt": question.prompt,
                "description": question.description,
                "default": question.default,
                "required": question.required,
            }
            for question in pending.questions
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
        prog="python -m orchestra.adapter",
        description="Inspect and modify a translated orchestra pipeline IR.",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    inspect = subparsers.add_parser(
        "inspect",
        help="Emit pending translation questions for a report as JSON.",
    )
    inspect.add_argument("report", type=Path, help="Path to the translation report or pipeline IR JSON.")
    inspect.add_argument(
        "--answers",
        type=Path,
        default=None,
        help=(
            "Optional path to a JSON file of answers already collected; "
            "questions whose conditions depend on those answers will surface "
            "only when their conditions are met."
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
    modify.add_argument("answers", type=Path, help="Path to a JSON file mapping question_id to answer string.")
    modify.add_argument(
        "--out",
        type=Path,
        required=True,
        help="Destination path for the preference-stamped IR JSON.",
    )
    modify.add_argument(
        "--lookup-values",
        type=Path,
        default=None,
        help=(
            "Optional path to a JSON list of lookup-value rows that consolidated "
            "metadata-driven motifs should ingest.  Each row is a dict mirroring "
            "a row from the source Lookup query."
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
        help="Emit the migration-phase input questions for an orchestra phase as JSON.",
    )
    inputs.add_argument(
        "phase",
        choices=("ingest", "translate", "prepare"),
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
    return parser


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
    answers = _read_answers_optional(args.answers) if getattr(args, "answers", None) else {}
    payload = {
        "pipelines": [_pending_to_payload(gather_questions(pipeline, [], answers=answers)) for pipeline in pipelines],
    }
    _emit_json(payload, args.out)
    return 0


def _read_answers_optional(answers_path: Path) -> dict[str, str]:
    """Loads an answers JSON file supplied to ``inspect``.

    Args:
        answers_path: Path to a JSON file mapping question_id to answer.

    Returns:
        Mapping of question_id to answer string.  Returns an empty dict
        when the file is missing or unparseable so ``inspect`` still
        succeeds (question gating just sees no prior answers).
    """
    try:
        raw = json.loads(answers_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return {key: str(value) for key, value in raw.items() if isinstance(value, str)}


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
    pipelines = _load_pipelines(args.report)
    if pipelines is None:
        return 1
    try:
        answers = _load_answers(args.answers)
        preferences = _preferences_from_answers(answers)
    except ValueError as error:
        print(f"Invalid answers payload: {error}", file=sys.stderr)
        return 2
    lookup_values = _load_lookup_values(args.lookup_values) if args.lookup_values else []
    stamped_pipelines = [
        _stamp_lookup_values_into_metadata_driven_motifs(apply_preferences(pipeline, preferences), lookup_values)
        for pipeline in pipelines
    ]
    modified = [_pipeline_to_dict(pipeline) for pipeline in stamped_pipelines]
    _write_modified_report(args.report, modified, args.out)
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


def _load_lookup_values(lookup_values_path: Path) -> list[dict[str, Any]]:
    """Loads materialised lookup values from a JSON file.

    Args:
        lookup_values_path: Path to a JSON list of row dicts.

    Returns:
        The parsed list of row dicts.  Returns an empty list when the
        file is missing or unparseable so the modify pass still succeeds
        (consolidated motifs will warn and fall back to the scaffold).
    """
    try:
        raw = json.loads(lookup_values_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return []
    if not isinstance(raw, list):
        return []
    return [row for row in raw if isinstance(row, dict)]


def _stamp_lookup_values_into_metadata_driven_motifs(pipeline, lookup_values: list[dict[str, Any]]):
    """Stamps lookup values onto every metadata-driven motif marked for consolidation.

    Args:
        pipeline: Preference-stamped pipeline IR.
        lookup_values: Rows materialised by the agent or the user.

    Returns:
        A new :class:`Pipeline` whose metadata-driven motif activities
        carry the supplied lookup rows.  When *lookup_values* is empty
        the pipeline is returned unchanged.
    """
    if not lookup_values:
        return pipeline
    import dataclasses as _dataclasses

    from orchestra.models.ir import MotifActivity as _MotifActivity

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
    if isinstance(raw, dict) and "tasks" in raw and "name" in raw:
        return [raw]
    if isinstance(raw, dict) and "translations" in raw:
        return [
            {"name": entry["pipeline"], **entry["ir"]}
            for entry in raw.get("translations", [])
            if entry.get("status") == "translated" and entry.get("ir")
        ]
    return []


def _load_answers(answers_path: Path) -> dict[str, str]:
    """Loads a JSON answers file and validates its top-level shape.

    Args:
        answers_path: Path to a JSON file mapping question_id to answer.

    Returns:
        Mapping of question_id to answer string.

    Raises:
        ValueError: When the file is not a JSON object of string values.
    """
    raw = json.loads(answers_path.read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        raise ValueError(f"Expected a JSON object at {answers_path}; got {type(raw).__name__}")
    coerced: dict[str, str] = {}
    for key, value in raw.items():
        if not isinstance(value, str):
            raise ValueError(f"Answer for {key!r} must be a string; got {type(value).__name__}")
        coerced[key] = value
    return coerced


def _preferences_from_answers(answers: dict[str, str]) -> TranslationPreferences:
    """Builds a :class:`TranslationPreferences` from a validated answers dict.

    Args:
        answers: Validated mapping of question_id to answer string.

    Returns:
        Preferences with every answered field overridden and every
        unanswered field defaulted.

    Raises:
        ValueError: When an answer is not in the allowed set for its
            question.
    """
    validated = {qid: validate_answer(qid, value) for qid, value in answers.items()}
    return TranslationPreferences(
        copy_activity_paradigm=CopyActivityParadigm(
            validated.get("copy_activity_paradigm", DEFAULT_PREFERENCES.copy_activity_paradigm)
        ),
        non_databricks_task_compute=NonDatabricksTaskCompute(
            validated.get("non_databricks_task_compute", DEFAULT_PREFERENCES.non_databricks_task_compute)
        ),
        use_lakeflow_connectors=UseLakeflowConnectors(
            validated.get("use_lakeflow_connectors", DEFAULT_PREFERENCES.use_lakeflow_connectors)
        ),
        databricks_task_compute=DatabricksTaskCompute(
            validated.get("databricks_task_compute", DEFAULT_PREFERENCES.databricks_task_compute)
        ),
        lakeflow_connector_type=LakeflowConnectorType(
            validated.get("lakeflow_connector_type", DEFAULT_PREFERENCES.lakeflow_connector_type)
        ),
        metadata_driven_consolidate=MetadataDrivenConsolidate(
            validated.get("metadata_driven_consolidate", DEFAULT_PREFERENCES.metadata_driven_consolidate)
        ),
        metadata_driven_access=MetadataDrivenAccess(
            validated.get("metadata_driven_access", DEFAULT_PREFERENCES.metadata_driven_access)
        ),
        metadata_driven_size=MetadataDrivenSize(
            validated.get("metadata_driven_size", DEFAULT_PREFERENCES.metadata_driven_size)
        ),
        metadata_driven_lookup_tool=MetadataDrivenLookupTool(
            validated.get("metadata_driven_lookup_tool", DEFAULT_PREFERENCES.metadata_driven_lookup_tool)
        ),
    )


def _pending_to_payload(pending: PendingQuestions) -> dict[str, Any]:
    """Serialises pending questions for transmission over stdout.

    Args:
        pending: Outstanding questions for a single pipeline.

    Returns:
        JSON-friendly dict the agent can iterate over to prompt the user.
    """
    return {
        "pipeline_name": pending.pipeline_name,
        "questions": [_question_to_payload(question) for question in pending.questions],
    }


def _question_to_payload(question: TranslationQuestion) -> dict[str, Any]:
    """Serialises a single :class:`TranslationQuestion` to a JSON-friendly dict.

    Args:
        question: Question to serialise.

    Returns:
        Dict containing the question's fields with options flattened to
        plain dicts.
    """
    return {
        "question_id": question.question_id,
        "prompt": question.prompt,
        "rationale": question.rationale,
        "options": [asdict(option) for option in question.options],
        "affected_task_keys": list(question.affected_task_keys),
        "default": question.default,
    }


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
    """Writes the preference-stamped IR to *out* using the input report's shape.

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
