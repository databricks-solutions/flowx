"""Scratch recorder for the Plan 2 golden capture: a live port scan that saves every REST exchange raw.

Writes, unredacted, to the --out directory (keep it outside the repo):
  exchanges.jsonl  one {"method", "url", "body", "status", "response"} per call, in call order
  pricing.json     {"region", "rates"} as returned by the Retail Prices API during the scan
  capture.json     the pinned window end (`now`) and the scan arguments
  port/            the port's own metadata/tco report from this same scan
"""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path

from flowx.sources.adf.profiler import pricing
from flowx.sources.adf.profiler.azure_client import AdfScanner, LiveTransport, TransportHttpError, get_credential
from flowx.sources.adf.profiler.report import write_reports


class RecordingTransport:
    def __init__(self, inner, sink):
        self._inner = inner
        self._sink = sink
        self.count = 0

    def _record(self, method, url, body, call):
        entry = {"method": method, "url": url, "body": body}
        try:
            response = call()
        except TransportHttpError as error:
            entry.update(status=error.status, response=None)
            self._write(entry)
            raise
        entry.update(status=200, response=response)
        self._write(entry)
        return response

    def _write(self, entry):
        self.count += 1
        self._sink.write(json.dumps(entry) + "\n")

    def get_json(self, url):
        return self._record("GET", url, None, lambda: self._inner.get_json(url))

    def post_json(self, url, body):
        return self._record("POST", url, body, lambda: self._inner.post_json(url, body))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--subscription-id", required=True)
    parser.add_argument("--resource-group", required=True)
    parser.add_argument("--days", type=int, required=True)
    parser.add_argument("--out", type=Path, required=True)
    args = parser.parse_args()
    args.out.mkdir(parents=True, exist_ok=True)

    fetched = {}
    live_fetch = pricing.fetch_live_pricing

    def recording_fetch(region):
        rates = live_fetch(region)
        fetched.update(region=region, rates=rates)
        return rates

    pricing.fetch_live_pricing = recording_fetch

    now = datetime.now(timezone.utc)
    credential = get_credential()
    with (args.out / "exchanges.jsonl").open("w") as sink:
        transport = RecordingTransport(LiveTransport(credential), sink)
        result = AdfScanner(credential=credential, days=args.days, transport=transport, now=now).scan(
            subscription_id=args.subscription_id, resource_group=args.resource_group
        )
    (args.out / "pricing.json").write_text(json.dumps(fetched, indent=2))
    (args.out / "capture.json").write_text(
        json.dumps(
            {
                "now": now.isoformat(),
                "days": args.days,
                "subscription_id": args.subscription_id,
                "resource_group": args.resource_group,
            },
            indent=2,
        )
    )
    write_reports(result, args.out / "port")
    print(f"now={now.isoformat()} recorded {transport.count} exchanges, {result.total_pipeline_runs} runs")


if __name__ == "__main__":
    main()
