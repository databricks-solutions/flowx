"""Golden-reference tests for `flowx profile`, replayed offline from one live capture.

`tests/integration/azure/profiler/capture/` holds every Azure REST exchange from one live scan of the
profiler test factories, plus the original standalone script's per-pipeline costs from a scan run
seconds earlier over the same runs. See `tests/integration/azure/profiler/README.md` for how it was
captured and how to recapture.

Two checks:
- the port's per-pipeline estimates equal the original script's, except where the port deliberately
  differs (listed in `INTENDED_DIFFERENCES`);
- the port's report for the capture equals the checked-in `expected/` files. Regenerate them with
  `--update-expected` and review the diff like code.
"""

from __future__ import annotations

import csv
import json
import re
import shutil
from collections import defaultdict, deque
from datetime import datetime
from pathlib import Path
from typing import Any

import pytest

from flowx.sources.adf.profiler import pricing
from flowx.sources.adf.profiler.azure_client import AdfScanner, ProfileResult, TransportHttpError
from flowx.sources.adf.profiler.report import write_reports

PROFILER_ROOT = Path(__file__).parent / "azure" / "profiler"
CAPTURE = PROFILER_ROOT / "capture"
EXPECTED = CAPTURE / "expected"
AZURE_IR_FACTORY = "flowx-profiler-test-adf"
VNET_FACTORY = "flowx-profiler-test-adf-vnet"

# Pipelines where the port deliberately prices differently from the original. Orchestration, data movement,
# and run counts must still agree for them.
INTENDED_DIFFERENCES = {
    # The original looks up the IR by the run's "ManagedVnetIR (West Europe)" name, misses because of the
    # region suffix, and prices managed-VNet pipeline and external activities at Azure IR rates. The port
    # strips the suffix (#12 A) and prices them at the managed-VNet rates.
    **{
        (VNET_FACTORY, pipeline): "managed-VNet pipeline/external activity rate (#12 A)"
        for pipeline in ("pl_databricks_job", "pl_foreach", "pl_lookup_wait", "pl_web_databricks", "pl_web_public")
    },
    # The original prices a data flow as 8 vCores x its duration; the port prices the vCore-hours Azure
    # reports in the run's billingReference (A).
    (AZURE_IR_FACTORY, "pl_dataflow"): "data flow priced from billingReference vCore-hours (A)",
}
RATE_SENSITIVE_FIELDS = {"pipeline_activity_cost", "external_cost", "total_cost"}

# The original's column for each of the port's per-pipeline fields.
ORIGINAL_COLUMNS = {
    "total_runs": "totalRuns",
    "orchestration_cost": "orchCost",
    "data_movement_cost": "ingestionCost",
    "pipeline_activity_cost": "pipelineActCost",
    "external_cost": "externalCost",
    "total_cost": "totalCost",
}
# Both sides round each bucket to 4 dp; the port sums buckets while the original summed per-run
# totals, so the last digit can differ by one.
TOLERANCE = 1.5e-4


@pytest.fixture(scope="module")
def replayed() -> ProfileResult:
    capture = json.loads((CAPTURE / "capture.json").read_text())
    recorded_pricing = json.loads((CAPTURE / "pricing.json").read_text())
    exchanges = [json.loads(line) for line in (CAPTURE / "exchanges.jsonl").read_text().splitlines()]

    # Serve the rate card fetched during the capture, so the report keeps its live-pricing label.
    with pytest.MonkeyPatch.context() as monkeypatch:
        monkeypatch.setattr(pricing, "fetch_live_pricing", lambda region: recorded_pricing["rates"])
        scanner = AdfScanner(
            credential=None,
            days=capture["days"],
            transport=_SequentialReplayTransport(exchanges),
            now=datetime.fromisoformat(capture["now"]),
        )
        return scanner.scan(subscription_id=capture["subscription_id"], resource_group=capture["resource_group"])


def test_replay_sees_every_captured_run(replayed):
    assert replayed.total_pipeline_runs == 96
    assert replayed.permission_warnings == []


def test_estimates_match_the_original_script(replayed):
    original = _original_costs()
    ported = {(cost.factory_name, cost.pipeline_name): cost for cost in replayed.pipeline_costs}
    assert set(ported) == set(original)

    for key, cost in sorted(ported.items()):
        fields = ORIGINAL_COLUMNS
        if key in INTENDED_DIFFERENCES:
            fields = {field: column for field, column in fields.items() if field not in RATE_SENSITIVE_FIELDS}
        for field, column in fields.items():
            assert getattr(cost, field) == pytest.approx(float(original[key][column]), abs=TOLERANCE), (key, field)


def test_intended_differences_still_differ(replayed):
    # If one of these starts matching, the port's fix regressed or the entry is stale.
    original = _original_costs()
    ported = {(cost.factory_name, cost.pipeline_name): cost for cost in replayed.pipeline_costs}
    for key in INTENDED_DIFFERENCES:
        assert ported[key].total_cost != pytest.approx(float(original[key]["totalCost"]), abs=TOLERANCE), key


def test_report_matches_expected(replayed, tmp_path, request):
    write_reports(replayed, tmp_path)
    produced = tmp_path / "metadata" / "tco"
    expected = EXPECTED / "metadata" / "tco"
    if request.config.getoption("--update-expected", default=False):
        shutil.rmtree(EXPECTED, ignore_errors=True)
        shutil.copytree(produced, expected)
    for name in ("cost_comparison.csv", "cost_comparison.md"):
        assert (produced / name).read_text() == (expected / name).read_text(), name


def test_capture_is_redacted():
    guid = re.compile(r"[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}")
    for path in CAPTURE.rglob("*"):
        if path.is_file():
            assert not guid.search(path.read_text()), f"unredacted GUID in {path}"


def _original_costs() -> dict[tuple[str, str], dict[str, str]]:
    with (CAPTURE / "golden" / "pipeline_runs_by_pipeline.csv").open() as handle:
        return {(row["factory_name"], row["pipelineName"]): row for row in csv.DictReader(handle)}


class _SequentialReplayTransport:
    """Serves the recorded responses for each (method, URL) in the order they were recorded.

    An unrecorded request fails loudly, so a scanner change that alters its requests shows up here
    rather than as a silently different report.
    """

    def __init__(self, exchanges: list[dict[str, Any]]):
        self._responses: dict[tuple[str, str], deque[dict[str, Any]]] = defaultdict(deque)
        for exchange in exchanges:
            self._responses[(exchange["method"], exchange["url"])].append(exchange)

    def _serve(self, method: str, url: str) -> dict[str, Any]:
        queue = self._responses.get((method, url))
        if not queue:
            raise AssertionError(f"no recorded response for {method} {url}")
        exchange = queue.popleft()
        if exchange["status"] != 200:
            raise TransportHttpError(exchange["status"], url)
        return exchange["response"]

    def get_json(self, url: str) -> dict[str, Any]:
        return self._serve("GET", url)

    def post_json(self, url: str, body: dict[str, Any]) -> dict[str, Any]:
        return self._serve("POST", url)
