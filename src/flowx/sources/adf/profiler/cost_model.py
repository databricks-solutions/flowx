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
