"""`flowx profile --source adf` entry point: parse args, build a credential, run a live ADF
scan, and write the cost report under metadata/tco/. Imported lazily by the adapter.
"""

from __future__ import annotations

import argparse
import sys

from flowx.sources.adf.profiler.azure_client import (
    AdfScanner,
    MissingProfileDependencyError,
    ProfileAuthenticationError,
    get_credential,
)
from flowx.sources.adf.profiler.report import write_reports

_UNREACHABLE_HINT = (
    "If this machine can't reach management.azure.com or prices.azure.com (for example, serverless "
    "compute with locked-down egress), run the profile locally instead."
)


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="flowx profile", description="Estimate current ADF spend (live Azure scan).")
    parser.add_argument("--source-dir", help="Accepted for adapter uniformity; ignored (profile scans Azure live).")
    parser.add_argument("--output-dir", default="./flowx_output", help="Shared flowx output directory.")
    parser.add_argument("--days", type=int, default=90, help="How many days of run history to profile.")
    parser.add_argument("--subscription-id", help="Only scan this subscription.")
    parser.add_argument("--resource-group", help="Only scan this resource group.")
    parser.add_argument("--factory-name", help="Only scan this factory (takes effect together with --resource-group).")
    parser.add_argument("--tenant-id", help="Service principal tenant ID (overrides DefaultAzureCredential).")
    parser.add_argument("--client-id", help="Service principal client ID.")
    parser.add_argument("--client-secret", help="Service principal client secret.")
    return parser


def main(argv: list[str] | None = None) -> int:
    """Run one profile and return an exit code: 0 on success, 2 on setup/auth problems."""
    args = _build_parser().parse_args(argv)
    try:
        credential = get_credential(
            tenant_id=args.tenant_id, client_id=args.client_id, client_secret=args.client_secret
        )
        scanner = AdfScanner(credential=credential, days=args.days)
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

    for warning in result.permission_warnings:
        print(f"warning: {warning}", file=sys.stderr)
    paths = write_reports(result, args.output_dir)
    print(f"Profiled {result.total_pipeline_runs} pipeline runs over {result.days} days ({result.pricing_source}).")
    for path in paths.values():
        print(f"Wrote {path}")
    return 0
