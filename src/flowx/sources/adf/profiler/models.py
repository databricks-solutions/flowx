"""Typed result rows the profiler produces and the report renders. The loose dicts of the
original script become these so the report and the equivalence test have a stable shape.
"""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(slots=True, kw_only=True)
class ActivityRunCost:
    """The estimated cost of one activity run inside one pipeline run."""

    factory_name: str
    pipeline_name: str
    activity_name: str
    activity_type: str
    ir_type: str
    orchestration_cost: float
    execution_cost: float
    total_cost: float


@dataclass(slots=True, kw_only=True)
class PipelineCost:
    """Estimated cost of one pipeline, summed over every run in the profiling window."""

    factory_name: str
    pipeline_name: str
    total_runs: int
    orchestration_cost: float
    data_movement_cost: float
    pipeline_activity_cost: float
    external_cost: float
    total_cost: float


@dataclass(slots=True, kw_only=True)
class ActualCost:
    """One billed Cost Management row attributed to a factory."""

    factory_name: str
    meter_subcategory: str
    cost: float
    currency: str


@dataclass(slots=True, kw_only=True)
class ProfileResult:
    """Everything one profile run found, ready for the report writer.

    `factory_subscriptions` maps each factory to the subscription label the report shows, and
    `subscription_names` maps subscription IDs to display names;
    `cost_management_denied` lists subscriptions whose actuals couldn't be read; and
    `permission_warnings` collects warnings gathered during the scan (missing roles, nothing found,
    runs that couldn't be costed).
    """

    region: str
    pricing_source: str
    days: int
    total_pipeline_runs: int
    pipeline_costs: list[PipelineCost] = field(default_factory=list)
    actual_costs: list[ActualCost] = field(default_factory=list)
    activity_runs: list[ActivityRunCost] = field(default_factory=list)
    factory_subscriptions: dict[str, str] = field(default_factory=dict)
    subscription_names: dict[str, str] = field(default_factory=dict)
    cost_management_denied: list[str] = field(default_factory=list)
    permission_warnings: list[str] = field(default_factory=list)
