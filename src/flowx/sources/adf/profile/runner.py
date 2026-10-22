"""`flowx profile --source adf` entry point: parse args, build a credential, run a live ADF
scan, and write the cost report under metadata/tco/. Imported lazily by the adapter.
"""

from __future__ import annotations

import argparse
import sys

from flowx.sources.adf.profile.azure_client import (
    MissingProfileDependencyError,
    ProfileAuthenticationError,
    get_credential,
)
from flowx.sources.adf.profile.core import AdfScanner
from flowx.sources.adf.profile.report import write_reports

_UNREACHABLE_HINT = (
    "If this machine can't reach management.azure.com or prices.azure.com (for example, serverless "
    "compute with locked-down egress), run the profile locally instead."
)


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="flowx profile", description="Estimate current ADF spend (live Azure scan).")
    parser.add_argument("--source-dir", help="Accepted for adapter uniformity; ignored (profile scans Azure live).")
    parser.add_argument("--output-dir", default="./flowx_output", help="Shared flowx output directory.")
    parser.add_argument(
        "--days",
        type=int,
        default=45,
        help="How many days of run history to profile (default 45: ADF keeps pipeline runs for only 45 days).",
    )
    parser.add_argument("--subscription-id", help="Only scan this subscription.")
    parser.add_argument("--resource-group", help="Only scan this resource group.")
    parser.add_argument("--factory-name", help="Only scan this factory (needs --resource-group).")
    parser.add_argument("--tenant-id", help="Microsoft Entra tenant that owns the subscription.")
    return parser


def main(argv: list[str] | None = None) -> int:
    """Run one profile and return an exit code: 0 on success, 2 on bad flags or setup/auth problems."""
    args = _build_parser().parse_args(argv)
    if args.factory_name and not args.resource_group:
        print("--factory-name needs --resource-group", file=sys.stderr)
        return 2
    try:
        credential = get_credential()
        scanner = AdfScanner(credential=credential, days=args.days, tenant_id=args.tenant_id)
        result = scanner.scan(
            subscription_id=args.subscription_id,
            resource_group=args.resource_group,
            factory_name=args.factory_name,
        )
    except (MissingProfileDependencyError, ProfileAuthenticationError) as error:
        print(str(error), file=sys.stderr)
        return 2
    except KeyboardInterrupt:
        print("\nCancelled by user", file=sys.stderr)
        return 1
    except Exception as error:
        print(f"flowx profile failed: {type(error).__name__}: {error}\n{_UNREACHABLE_HINT}", file=sys.stderr)
        return 1

    for warning in result.warnings:
        print(f"warning: {warning}", file=sys.stderr)
    paths = write_reports(result, args.output_dir)
    source = f" ({result.pricing_source})" if result.pricing_source else ""
    print(f"Profiled {result.total_pipeline_runs} pipeline runs over {result.days} days{source}.")
    for path in paths.values():
        print(f"Wrote {path}")
    return 0
