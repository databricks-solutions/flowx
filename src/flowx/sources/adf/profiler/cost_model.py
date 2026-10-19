"""Per-activity-run cost math for ADF, ported in behavior from the standalone profiler's
activity-run loop. Pure functions over an activity run's duration, its `output` dict, and a
rate card, so they can be unit-tested with no Azure.
"""

from __future__ import annotations

import math
from typing import Any

COPY_ACTIVITY_TYPES = ["Copy"]
PIPELINE_ACTIVITY_TYPES = [
    "Lookup",
    "GetMetadata",
    "Delete",
    "Validation",
    "Filter",
    "SetVariable",
    "AppendVariable",
    "IfCondition",
    "ForEach",
    "Until",
    "Wait",
    "Switch",
]
EXTERNAL_ACTIVITY_TYPES = [
    "SqlServerStoredProcedure",
    "DatabricksNotebook",
    "DatabricksSparkJar",
    "DatabricksSparkPython",
    "HDInsightHive",
    "HDInsightPig",
    "HDInsightSpark",
    "HDInsightMapReduce",
    "AzureMLBatchExecution",
    "AzureMLUpdateResource",
    "Custom",
    "AzureFunctionActivity",
    "WebActivity",
    "DatabricksJob",
]
DATA_FLOW_TYPES = ["ExecuteDataFlow"]
DATAFLOW_VCORE_RATE = 0.274  # $/vCore-hour (General Purpose)
DATAFLOW_MIN_VCORES = 8


def billable_duration_hours(duration_ms: int) -> float:
    """ADF bills activity time in whole minutes with a 1-minute minimum; returns hours."""
    return max(1, math.ceil((duration_ms or 0) / 60000)) / 60


def orchestration_cost(rates: dict[str, float]) -> float:
    """Per-run orchestration charge (the rate is quoted per 1000 runs)."""
    return rates["orchestration_per_1000"] / 1000


def activity_execution_cost(
    activity_type: str, ir_key: str, duration_ms: int, output: dict[str, Any], rates: dict[str, float]
) -> float:
    """Execution (non-orchestration) cost of a single activity run.

    `duration_ms` is the activity-run record's `durationInMs` (not the one inside `output`);
    `output` is the run's `output` object, read only for DIU counts; `rates` is the rate card
    for the integration runtime that ran it (`azure_ir`, `shir`, or `managed_vnet_ir`).
    """
    hours = billable_duration_hours(duration_ms)

    if activity_type in COPY_ACTIVITY_TYPES:
        if ir_key == "shir":
            return hours * rates["data_movement_per_hour"]
        dius = output.get("usedDataIntegrationUnits", output.get("usedCloudDataMovementUnits", 4))
        return hours * dius * rates["data_movement_per_diu_hour"]
    if activity_type in EXTERNAL_ACTIVITY_TYPES:
        return hours * rates["external_activity_per_hour"]
    if activity_type in DATA_FLOW_TYPES:
        return hours * DATAFLOW_MIN_VCORES * DATAFLOW_VCORE_RATE
    # Pipeline activities and any unknown type both bill at the pipeline-activity rate.
    return hours * rates["pipeline_activity_per_hour"]


def duration_bucket(activity_type: str) -> str:
    """The estimate bucket for a run priced by duration; data flows count as external, as in the original."""
    if activity_type in COPY_ACTIVITY_TYPES:
        return "data_movement"
    if activity_type in EXTERNAL_ACTIVITY_TYPES or activity_type in DATA_FLOW_TYPES:
        return "external"
    return "pipeline_activity"


# How each billingReference `activityType` maps to an estimate bucket, and the rate for each billed unit.
_BILLED_BUCKETS = {
    "datamovement": "data_movement",
    "pipelineactivity": "pipeline_activity",
    "externalactivity": "external",
    "executedataflow": "external",
}
_BILLED_UNIT_RATES = {
    ("data_movement", "diuhours"): "data_movement_per_diu_hour",
    ("data_movement", "hours"): "data_movement_per_hour",
    ("pipeline_activity", "hours"): "pipeline_activity_per_hour",
    ("external", "hours"): "external_activity_per_hour",
}


def billed_execution(output: dict[str, Any], rates: dict[str, float]) -> tuple[str, float] | None:
    """Execution cost from the billed quantities Azure reports on the run, with the bucket it belongs to.

    Azure IR and self-hosted IR runs carry `output.billingReference`: the billing bucket
    (`activityType`) and the billed quantity per meter (`billableDuration`, already rounded the way
    Azure bills it, e.g. DIU-hours for a copy or the vCore-hours a data flow cluster used). Pricing
    those is more accurate than estimating from the activity's duration. Returns None when there's no
    reference (managed-VNet runs carry none) or it holds anything this doesn't recognize, so the
    caller falls back to the duration-based estimate.
    """
    reference = output.get("billingReference")
    if not isinstance(reference, dict):
        return None
    bucket = _BILLED_BUCKETS.get(str(reference.get("activityType", "")).lower())
    entries = reference.get("billableDuration") or []
    if bucket is None or not entries:
        return None
    cost = 0.0
    for entry in entries:
        unit = str(entry.get("unit", "")).lower()
        if unit == "corehour":
            rate = DATAFLOW_VCORE_RATE if bucket == "external" else None
        else:
            rate_key = _BILLED_UNIT_RATES.get((bucket, unit))
            rate = rates.get(rate_key) if rate_key else None
        if rate is None:
            return None
        cost += float(entry.get("duration", 0) or 0) * rate
    return bucket, cost
