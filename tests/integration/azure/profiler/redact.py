"""Turn a raw `record.py` capture into a committable, redacted capture directory.

Usage:
    python tests/integration/azure/profiler/redact.py --raw /tmp/flowx-golden/port \
        --original /tmp/flowx-golden/original/out/<timestamp> --out tests/integration/azure/profiler/capture

Every distinct GUID gets its own stable alias (`<SUBSCRIPTION_1>`, `<ID_0001>`, ...), applied the same
way in URLs and bodies, so replayed `/pipelineruns/<runId>/queryActivityruns` URLs stay distinct.
Emails, Databricks hosts, numeric workspace/job/run IDs (as same-length fake numbers), IP addresses,
and the subscription's display name are replaced too. Cost Management rows for resources outside the
captured resource group are dropped: they belong to other people's factories, and the scanner ignores
them anyway.
"""

from __future__ import annotations

import argparse
import json
import re
import shutil
from pathlib import Path

GUID = re.compile(r"[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}")
SUBSCRIPTION_GUID = re.compile(r"(/subscriptions/)(" + GUID.pattern + ")", re.IGNORECASE)
EMAIL = re.compile(r"[\w.+-]+@[\w-]+(?:\.[\w-]+)+")
DATABRICKS_HOST = re.compile(r"adb-\d+\.\d+\.azuredatabricks\.net")
LONG_NUMBER = re.compile(r"(?<![\d.])\d{15,16}(?![\d.])")
IP_ADDRESS = re.compile(r"(?<![\d.])\d{1,3}(?:\.\d{1,3}){3}(?![\d.])")

GOLDEN_FILES = ["orchestration/details/pipeline_runs_by_pipeline.csv"]


class Redactor:
    """Replaces identifiers with stable aliases: the same value always maps to the same alias."""

    def __init__(self, subscription_name: str):
        self._aliases: dict[tuple[str, str], str] = {}
        self._subscription_name = re.compile(rf"(?<![\w-]){re.escape(subscription_name)}(?![\w-])")

    def _alias(self, kind: str, value: str) -> str:
        key = (kind, value.lower())
        if key not in self._aliases:
            count = sum(1 for existing_kind, _ in self._aliases if existing_kind == kind) + 1
            width = 1 if kind == "SUBSCRIPTION" else 4
            self._aliases[key] = f"<{kind}_{count:0{width}d}>"
        return self._aliases[key]

    def text(self, value: str) -> str:
        value = SUBSCRIPTION_GUID.sub(lambda match: match.group(1) + self._alias("SUBSCRIPTION", match.group(2)), value)
        value = GUID.sub(lambda match: self._guid_alias(match.group(0)), value)
        value = EMAIL.sub("user@example.com", value)
        value = DATABRICKS_HOST.sub("<DATABRICKS_HOST>", value)
        value = LONG_NUMBER.sub(lambda match: self._number_alias(match.group(0)), value)
        value = IP_ADDRESS.sub("192.0.2.1", value)
        return self._subscription_name.sub("demo-subscription", value)

    def _number_alias(self, number: str) -> str:
        """A fake number of the same length: some IDs are bare JSON numbers, so the alias must be one too."""
        key = ("NUMBER", number)
        if key not in self._aliases:
            count = sum(1 for existing_kind, _ in self._aliases if existing_kind == "NUMBER") + 1
            self._aliases[key] = str(10 ** (len(number) - 1) + count)
        return self._aliases[key]

    def _guid_alias(self, guid: str) -> str:
        """A subscription GUID seen outside a `/subscriptions/` path keeps its subscription alias."""
        return self._aliases.get(("SUBSCRIPTION", guid.lower())) or self._alias("ID", guid)


def _in_resource_group(row: list, columns: list[str], resource_group: str) -> bool:
    resource_id = str(dict(zip(columns, row)).get("ResourceId", "")).lower()
    return f"/resourcegroups/{resource_group.lower()}/" in resource_id


def _drop_other_resource_groups(exchange: dict, resource_group: str) -> dict:
    if "Microsoft.CostManagement/query" not in exchange["url"] or not exchange.get("response"):
        return exchange
    properties = exchange["response"]["properties"]
    columns = [column["name"] for column in properties["columns"]]
    properties["rows"] = [row for row in properties["rows"] if _in_resource_group(row, columns, resource_group)]
    return exchange


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--raw", type=Path, required=True, help="record.py output directory")
    parser.add_argument("--original", type=Path, required=True, help="the original script's timestamped output")
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--subscription-name", default="field-eng")
    args = parser.parse_args()

    redactor = Redactor(args.subscription_name)
    capture = json.loads((args.raw / "capture.json").read_text())
    resource_group = capture["resource_group"]
    # Alias the scanned subscription first so it is always <SUBSCRIPTION_1>.
    redactor.text(f"/subscriptions/{capture['subscription_id']}")

    if args.out.exists():
        shutil.rmtree(args.out)
    (args.out / "golden").mkdir(parents=True)

    exchanges = [
        _drop_other_resource_groups(json.loads(line), resource_group)
        for line in (args.raw / "exchanges.jsonl").read_text().splitlines()
    ]
    (args.out / "exchanges.jsonl").write_text(
        "".join(redactor.text(json.dumps(exchange, sort_keys=True)) + "\n" for exchange in exchanges)
    )
    (args.out / "capture.json").write_text(redactor.text(json.dumps(capture, indent=2)) + "\n")
    shutil.copy(args.raw / "pricing.json", args.out / "pricing.json")
    for relative in GOLDEN_FILES:
        source = args.original / relative
        (args.out / "golden" / source.name).write_text(redactor.text(source.read_text()))
    print(f"wrote {len(exchanges)} exchanges to {args.out}")


if __name__ == "__main__":
    main()
