"""CLI for the ADF ``extract`` phase: ``python -m flowx.adapter extract --source adf ...``.

Three modes, one per thing the ``flowx-extract-metadata`` skill needs:

- extract (default): sign in, pull the factory from Azure, write the native layout, then check the
  result the way discover will read it.
- ``--validate-path``: check any path (ADF Studio export, Git clone, earlier extraction) and print
  the exact path to give discover.
- ``--emit-notebook``: write a notebook the user runs in their own workspace.

Exit codes: 0 the result is ready for discover (warnings may list optional resource types that were
skipped), 1 the run failed or the result isn't usable, 2 usage, missing packages, sign-in or
permission problems.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from flowx.sources.adf.extract import core
from flowx.sources.adf.extract.layout import LayoutReport, inspect_source_path
from flowx.sources.adf.extract.notebook import render_notebook


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="flowx extract --source adf",
        description="Copy an Azure Data Factory's definitions into a folder flowx discover reads.",
    )
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument(
        "--validate-path",
        type=Path,
        help="Check an existing export, ARM template or Git folder instead of extracting.",
    )
    mode.add_argument(
        "--emit-notebook",
        type=Path,
        help="Write a Databricks notebook (SOURCE format .py) the user runs to extract the factory.",
    )
    parser.add_argument("--tenant-id", help="Microsoft Entra tenant that owns the subscription.")
    parser.add_argument("--subscription-id", help="Azure subscription ID of the factory.")
    parser.add_argument("--resource-group", help="Resource group of the factory.")
    parser.add_argument("--factory-name", help="Data Factory name.")
    parser.add_argument(
        "--output-dir",
        type=Path,
        help="Folder to write the export to (for --emit-notebook: the default output path widget).",
    )
    parser.add_argument(
        "--auth",
        choices=core.AUTH_MODES,
        default="default",
        help=(
            "default: env service principal, managed identity or `az login`; device-code: sign in in a browser; "
            "service-credential: a Unity Catalog service credential (Databricks only, no sign-in)."
        ),
    )
    parser.add_argument(
        "--service-credential",
        help="Unity Catalog service credential name, for --auth service-credential (also the notebook default).",
    )
    parser.add_argument("--overwrite", action="store_true", help="Replace an existing, non-empty --output-dir.")
    parser.add_argument("--json", action="store_true", help="Print the result as JSON (for agents).")
    return parser


def _print_layout_report(report: LayoutReport, *, as_json: bool) -> None:
    if as_json:
        print(json.dumps(report.as_dict(), indent=2))
        return
    print(f"Path checked:   {report.requested_path}")
    print(f"Found:          {report.kind}")
    print(f"Use for discover: {report.discover_path or '(none)'}")
    for name, count in report.counts.items():
        print(f"  {name:<18} {count}")
    for warning in report.warnings:
        print(f"WARNING: {warning}")
    print("Ready for discover." if report.usable else "NOT ready for discover.")


def _run_validate(path: Path, *, as_json: bool) -> int:
    report = inspect_source_path(path)
    _print_layout_report(report, as_json=as_json)
    return 0 if report.usable else 1


def _run_emit_notebook(args: argparse.Namespace) -> int:
    destination: Path = args.emit_notebook
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_text(
        render_notebook(
            tenant_id=args.tenant_id or "",
            subscription_id=args.subscription_id or "",
            resource_group=args.resource_group or "",
            factory_name=args.factory_name or "",
            output_path=str(args.output_dir) if args.output_dir else "",
            service_credential=args.service_credential or "",
        ),
        encoding="utf-8",
    )
    if args.json:
        print(json.dumps({"notebook": str(destination)}))
    else:
        print(f"Wrote notebook: {destination}")
    return 0


def _run_extract(args: argparse.Namespace) -> int:
    missing = [
        flag
        for flag, value in (
            ("--subscription-id", args.subscription_id),
            ("--resource-group", args.resource_group),
            ("--factory-name", args.factory_name),
            ("--output-dir", args.output_dir),
        )
        if not value
    ]
    if args.auth == "service-credential" and not args.service_credential:
        missing.append("--service-credential")
    if missing:
        print(f"Missing required flags for extraction: {', '.join(missing)}", file=sys.stderr)
        return 2

    def progress(message: str) -> None:
        print(f"  listed {message}", file=sys.stderr)

    try:
        manifest = core.run_extraction(
            subscription_id=args.subscription_id,
            resource_group=args.resource_group,
            factory_name=args.factory_name,
            output_dir=args.output_dir,
            tenant_id=args.tenant_id,
            auth_mode=args.auth,
            service_credential=args.service_credential,
            overwrite=args.overwrite,
            progress=progress,
        )
    except (core.AzureAuthenticationError, core.SetupError) as error:
        print(str(error), file=sys.stderr)
        return 2
    except core.ExtractError as error:
        print(f"Extraction failed: {error}", file=sys.stderr)
        return 1
    except OSError as error:
        print(
            f"Extraction failed: could not write {args.output_dir}: {error}\n"
            "Use a folder you can write to; on a Unity Catalog volume, a folder inside the volume.",
            file=sys.stderr,
        )
        return 1

    report = inspect_source_path(args.output_dir)
    if args.json:
        print(json.dumps({"manifest": manifest, "validation": report.as_dict()}, indent=2))
    else:
        print(f"Extracted {args.factory_name} to {args.output_dir}")
        for folder, count in manifest["counts"].items():
            print(f"  {folder:<20} {count}")
        requests_made = manifest.get("requests") or {}
        if requests_made:
            print(f"  Azure requests: {requests_made.get('made')} ({requests_made.get('retried')} retried)")
        for warning in manifest["warnings"]:
            print(f"WARNING: {warning}")
        print()
        _print_layout_report(report, as_json=False)
    return 0 if report.usable else 1


def main(argv: list[str] | None = None) -> int:
    """Runs the extract phase CLI and returns its exit code.

    Args:
        argv: Command-line arguments; ``sys.argv[1:]`` when omitted. On Databricks, call this from a
            notebook cell to use a Unity Catalog service credential.

    Returns:
        0 when the result is ready for discover, 1 when the run failed or the result isn't usable,
        2 for usage, missing packages, sign-in or permission problems.
    """
    args = _build_parser().parse_args(argv)
    if args.validate_path is not None:
        return _run_validate(args.validate_path, as_json=args.json)
    if args.emit_notebook is not None:
        return _run_emit_notebook(args)
    return _run_extract(args)


if __name__ == "__main__":
    raise SystemExit(main())
