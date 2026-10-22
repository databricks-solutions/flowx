"""The ADF profile scan: finds factories, prices their pipeline runs, and reads billed actuals.

AdfScanner drives the scan through a Transport (live or replayed), aggregates per-pipeline
estimates, queries Cost Management, and collects the scan's warnings for the report.
"""

from __future__ import annotations

import logging
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any

from flowx.sources.adf.profile import cost_model, discovery
from flowx.sources.adf.profile import pricing as pricing_module
from flowx.sources.adf.profile.azure_client import (
    AUTH_HINT,
    AZURE_MANAGEMENT_URL,
    LiveTransport,
    ProfileAuthenticationError,
    Transport,
    TransportHttpError,
    UntrustedUrlError,
)
from flowx.sources.adf.profile.models import ActualCost, PipelineCost, ProfileResult
from flowx.sources.adf.profile.pricing import RateCards

logger = logging.getLogger(__name__)

_LIVE_PRICING = "Azure Retail Prices API"

_PIPELINE_RUNS_DENIED = (
    "{factory}: Cannot query pipeline runs ({reason}); "
    "needs Microsoft.DataFactory/factories/querypipelineruns/action, included in Data Factory Contributor"
)
_ACTIVITY_RUNS_DENIED_PREFIX = "{factory}: Cannot query activity runs ("
_ACTIVITY_RUNS_DENIED = (
    _ACTIVITY_RUNS_DENIED_PREFIX + "{reason}); "
    "needs Microsoft.DataFactory/factories/pipelineruns/queryactivityruns/action, included in Data Factory Contributor"
)
_NO_SUBSCRIPTIONS = (
    "No accessible subscriptions found. The signed-in identity may belong to the wrong tenant "
    "(on Databricks, the workspace's own identity is in Databricks' tenant, not yours).\n" + AUTH_HINT
)
_NO_FACTORIES = (
    "No Data Factory factories found in the scanned subscriptions "
    "(check --subscription-id / --resource-group and that the identity has Reader on them)."
)
_UNCOSTED_RUNS = (
    "{skipped} of {total} pipeline runs could not be costed (their activity runs couldn't be read or "
    "had unexpected data); the estimates leave them out."
)
_LIST_RATES_FALLBACK = (
    "{region}: couldn't fetch live Azure prices ({reason}); used built-in list rates, so estimates may be off"
)
_COST_MANAGEMENT_OFF_HOST = (
    "Subscription {subscription}...: Cost Management sent a next-page link outside {host}; it wasn't "
    "followed, so actuals may be incomplete"
)
_COST_MANAGEMENT_DENIED = (
    "Subscription {subscription}...: Cost Management access denied "
    "({reason}; need Billing Reader or Cost Management Reader on this subscription)"
)


@dataclass(slots=True, kw_only=True)
class _PipelineTotals:
    total_runs: int = 0
    orchestration_cost: float = 0.0
    data_movement_cost: float = 0.0
    pipeline_activity_cost: float = 0.0
    external_cost: float = 0.0

    @property
    def total_cost(self) -> float:
        return self.orchestration_cost + self.data_movement_cost + self.pipeline_activity_cost + self.external_cost


class AdfScanner:
    """Scans ADF live and estimates what its pipeline runs cost over the last `days` days.

    Ported from the standalone script's RuntimeProfiler, ADF only. Every Azure call goes through
    `transport` (live by default), so a recorded scan can be replayed in tests. Passing `pricing`
    pins the rate cards ("custom"); otherwise each factory is priced from the Retail Prices API for
    its own region, falling back to list rates.
    """

    def __init__(
        self,
        *,
        credential: Any,
        days: int,
        tenant_id: str | None = None,
        transport: Transport | None = None,
        pricing: RateCards | None = None,
        now: datetime | None = None,
    ):
        self._transport: Transport = (
            transport if transport is not None else LiveTransport(credential, tenant_id=tenant_id)
        )
        self._days = days
        self._custom_pricing = pricing
        window_end = now or datetime.now(timezone.utc)
        self._window_end = window_end
        self._window = {
            "lastUpdatedAfter": (window_end - timedelta(days=days)).isoformat(),
            "lastUpdatedBefore": window_end.isoformat(),
        }

    def scan(
        self,
        *,
        subscription_id: str | None = None,
        resource_group: str | None = None,
        factory_name: str | None = None,
    ) -> ProfileResult:
        subscriptions = discovery.list_subscriptions(self._transport, subscription_id)
        factories = [
            factory
            for subscription in subscriptions
            for factory in discovery.list_adf_factories(
                self._transport,
                subscription_id=subscription["subscription_id"],
                resource_group=resource_group,
                factory_name=factory_name,
            )
        ]
        subscription_names = {sub["subscription_id"]: sub["display_name"] for sub in subscriptions}
        regions = list(dict.fromkeys(_region(factory) for factory in factories))
        pricing_warnings: list[str] = []
        pricing_by_region = {region: self._resolve_pricing(region, pricing_warnings) for region in regions}
        result = ProfileResult(
            region=", ".join(regions),
            pricing_source=_pricing_label({region: source for region, (_, source) in pricing_by_region.items()}),
            days=self._days,
            total_pipeline_runs=0,
            subscription_names=subscription_names,
            factory_subscriptions={
                factory["name"]: subscription_names.get(factory["subscription_id"], "") or factory["subscription_id"]
                for factory in factories
            },
            warnings=pricing_warnings,
        )
        if not subscriptions:
            result.warnings.append(_NO_SUBSCRIPTIONS)
        elif not factories:
            result.warnings.append(_NO_FACTORIES)
        if not factories:
            return result

        factories_by_subscription: dict[str, list[str]] = {}
        for factory in factories:
            factories_by_subscription.setdefault(factory["subscription_id"], []).append(factory["name"])
        for subscription, names in factories_by_subscription.items():
            self._query_actual_costs(subscription, names, result)

        runs_to_cost: list[tuple[dict[str, Any], dict[str, Any], dict[str, str]]] = []
        for factory in factories:
            runs = self._fetch_pipeline_runs(factory, result)
            ir_type_map = discovery.integration_runtime_types(self._transport, factory)
            runs_to_cost.extend((factory, run, ir_type_map) for run in runs)
        result.total_pipeline_runs = len(runs_to_cost)

        totals_by_pipeline: dict[tuple[str, str], _PipelineTotals] = {}
        skipped_runs = 0
        for factory, run, ir_type_map in runs_to_cost:
            rates = pricing_by_region[_region(factory)][0]
            run_costs = self._cost_pipeline_run(factory, run, ir_type_map, rates, result)
            if run_costs is None:
                skipped_runs += 1
                continue
            key = (factory["name"], run.get("pipelineName", ""))
            totals = totals_by_pipeline.setdefault(key, _PipelineTotals())
            totals.total_runs += 1
            totals.orchestration_cost += run_costs.orchestration_cost
            totals.data_movement_cost += run_costs.data_movement_cost
            totals.pipeline_activity_cost += run_costs.pipeline_activity_cost
            totals.external_cost += run_costs.external_cost

        result.pipeline_costs = [
            PipelineCost(
                factory_name=factory_name_key,
                pipeline_name=pipeline_name,
                total_runs=totals.total_runs,
                orchestration_cost=round(totals.orchestration_cost, 4),
                data_movement_cost=round(totals.data_movement_cost, 4),
                pipeline_activity_cost=round(totals.pipeline_activity_cost, 4),
                external_cost=round(totals.external_cost, 4),
                total_cost=round(totals.total_cost, 4),
            )
            for (factory_name_key, pipeline_name), totals in totals_by_pipeline.items()
        ]
        if skipped_runs:
            result.warnings.append(_UNCOSTED_RUNS.format(skipped=skipped_runs, total=len(runs_to_cost)))
        return result

    def _resolve_pricing(self, region: str, warnings: list[str]) -> tuple[RateCards, str]:
        """Rates for `region`: custom, else live, else list rates with a warning saying why."""
        if self._custom_pricing:
            return self._custom_pricing, "custom"
        try:
            return pricing_module.fetch_live_pricing(region), _LIVE_PRICING
        except pricing_module.LivePricingUnavailable as error:
            warnings.append(_LIST_RATES_FALLBACK.format(region=region, reason=error))
            return pricing_module.DEFAULT_PRICING, "default list rates"

    def _query_actual_costs(self, subscription_id: str, factory_names: list[str], result: ProfileResult) -> None:
        """Billed ADF cost for one subscription from Cost Management, attributed to factories.

        One query per subscription, grouped by meter subcategory and resource ID. `nextLink` pages are
        followed only on management.azure.com, so the bearer token never goes to another host, and a
        repeated link ends paging. A row belongs to the factory whose name is the ID's `/factories/<name>` segment,
        compared case-insensitively because Cost Management lowercases resource IDs. (The original
        matched by substring, so a factory also took the billing of any factory whose name contained
        its own.)
        """
        url = (
            f"{AZURE_MANAGEMENT_URL}/subscriptions/{subscription_id}"
            "/providers/Microsoft.CostManagement/query?api-version=2023-11-01"
        )
        start = self._window_end - timedelta(days=self._days)
        body = {
            "type": "ActualCost",
            "timeframe": "Custom",
            "timePeriod": {
                "from": start.strftime("%Y-%m-%dT00:00:00+00:00"),
                "to": self._window_end.strftime("%Y-%m-%dT23:59:59+00:00"),
            },
            "dataset": {
                "granularity": "None",
                "aggregation": {"totalCost": {"name": "PreTaxCost", "function": "Sum"}},
                "grouping": [
                    {"type": "Dimension", "name": "MeterSubcategory"},
                    {"type": "Dimension", "name": "ResourceId"},
                ],
                "filter": {
                    "dimensions": {
                        "name": "ResourceType",
                        "operator": "In",
                        "values": ["Microsoft.DataFactory/factories"],
                    }
                },
            },
        }
        records: list[dict[str, Any]] = []
        next_url: str | None = url
        seen_urls: set[str] = set()
        try:
            while next_url and next_url not in seen_urls:
                seen_urls.add(next_url)
                properties = self._transport.post_json(next_url, body).get("properties", {})
                columns = [column["name"] for column in properties.get("columns", [])]
                records.extend(dict(zip(columns, row)) for row in properties.get("rows", []))
                next_url = properties.get("nextLink")
        except UntrustedUrlError:
            result.warnings.append(
                _COST_MANAGEMENT_OFF_HOST.format(subscription=subscription_id[:8], host=AZURE_MANAGEMENT_URL)
            )
        except ProfileAuthenticationError:
            raise
        except TransportHttpError as error:
            if error.status in (401, 403):
                result.warnings.append(
                    _COST_MANAGEMENT_DENIED.format(subscription=subscription_id[:8], reason=error.reason)
                )
                result.cost_management_denied.append(subscription_id)
                _log_denied(error)
            else:
                logger.warning(
                    "Cost Management query for sub %s... returned HTTP %s", subscription_id[:8], error.status
                )
            return
        except Exception as error:
            logger.warning("Cost Management query error for sub %s...: %s", subscription_id[:8], error)
            return

        lowered_names = list(dict.fromkeys(name.lower() for name in factory_names))
        for record in records:
            resource_id = record.get("ResourceId", "").lower()
            _, _, after_factories = resource_id.rpartition("/factories/")
            matched = after_factories.split("/")[0]
            if matched not in lowered_names:
                continue
            result.actual_costs.append(
                ActualCost(
                    factory_name=next((name for name in factory_names if name.lower() == matched), matched),
                    meter_subcategory=record.get("MeterSubcategory", "") or "",
                    cost=record.get("PreTaxCost", 0),
                    currency=record.get("Currency", "USD"),
                )
            )

    def _fetch_pipeline_runs(self, factory: dict[str, Any], result: ProfileResult) -> list[dict[str, Any]]:
        """Every pipeline run in the window, following continuation tokens; partial on failure."""
        url = f"{discovery.factory_base_url(factory)}/queryPipelineRuns?api-version=2018-06-01"
        runs: list[dict[str, Any]] = []
        try:
            for page in self._query_pages(url):
                runs.extend(page)
        except ProfileAuthenticationError:
            raise
        except TransportHttpError as error:
            if error.status in (401, 403):
                result.warnings.append(_PIPELINE_RUNS_DENIED.format(factory=factory["name"], reason=error.reason))
                _log_denied(error)
            else:
                logger.error("[%s] Pipeline runs: HTTP %s", factory["name"], error.status)
        except Exception as error:
            logger.error("[%s] Pipeline runs error: %s", factory["name"], error)
        return runs

    def _query_pages(self, url: str) -> Iterator[list[dict[str, Any]]]:
        """POST a run query over the window and yield each page's runs.

        Follows continuation tokens, even past an empty page, until there is none or one repeats.
        """
        continuation: str | None = None
        seen_tokens: set[str] = set()
        while True:
            body = dict(self._window)
            if continuation:
                body["continuationToken"] = continuation
            page = self._transport.post_json(url, body)
            yield page.get("value", [])
            continuation = page.get("continuationToken")
            if not continuation or continuation in seen_tokens:
                return
            seen_tokens.add(continuation)

    def _cost_pipeline_run(
        self,
        factory: dict[str, Any],
        run: dict[str, Any],
        ir_type_map: dict[str, str],
        rates: RateCards,
        result: ProfileResult,
    ) -> _PipelineTotals | None:
        """Cost every activity in one pipeline run.

        Returns None when the run's activity runs can't be read or contain data the cost math can't
        use (e.g. a null DIU count); like the original, only that run is dropped, not the scan. A
        401/403 also adds one missing-role warning per factory.
        """
        run_url = f"{discovery.factory_base_url(factory)}/pipelineruns/{run.get('runId', '')}"
        url = f"{run_url}/queryActivityruns?api-version=2018-06-01"
        try:
            activities = [activity for page in self._query_pages(url) for activity in page]
        except ProfileAuthenticationError:
            raise
        except TransportHttpError as error:
            prefix = _ACTIVITY_RUNS_DENIED_PREFIX.format(factory=factory["name"])
            if error.status in (401, 403) and not any(warning.startswith(prefix) for warning in result.warnings):
                result.warnings.append(_ACTIVITY_RUNS_DENIED.format(factory=factory["name"], reason=error.reason))
                _log_denied(error)
            return None
        except Exception:
            return None

        try:
            return self._cost_activities(activities, ir_type_map, rates)
        except Exception as error:
            logger.debug("[%s] Could not cost run %s: %s", factory["name"], run.get("runId", ""), error)
            return None

    @staticmethod
    def _cost_activities(
        activities: list[dict[str, Any]], ir_type_map: dict[str, str], rates: RateCards
    ) -> _PipelineTotals:
        run_costs = _PipelineTotals()
        for activity in activities:
            activity_type = activity.get("activityType", "")
            output = activity.get("output") or {}
            ir_key = discovery.classify_ir(output.get("effectiveIntegrationRuntime", ""), ir_type_map)
            activity_rates = rates.get(ir_key, rates["azure_ir"])
            orchestration = cost_model.orchestration_cost(activity_rates)
            billed = cost_model.billed_execution(output, activity_rates)
            if billed is not None:
                bucket, execution = billed
            else:
                bucket = cost_model.duration_bucket(activity_type)
                execution = cost_model.activity_execution_cost(
                    activity_type, ir_key, activity.get("durationInMs", 0) or 0, output, activity_rates
                )
            run_costs.orchestration_cost += orchestration
            if bucket == "data_movement":
                run_costs.data_movement_cost += execution
            elif bucket == "external":
                run_costs.external_cost += execution
            else:
                run_costs.pipeline_activity_cost += execution
        # Each pipeline run also pays one trigger orchestration at the Azure IR rate.
        run_costs.orchestration_cost += cost_model.orchestration_cost(rates["azure_ir"])
        return run_costs


def _region(factory: dict[str, Any]) -> str:
    """The factory's Azure region in Retail Prices form (e.g. "West Europe" -> "westeurope")."""
    return str(factory.get("location") or "eastus").lower().replace(" ", "")


def _pricing_label(source_by_region: dict[str, str]) -> str:
    """Where the rates came from, naming the region of each live fetch.

    Several regions read `region: source; ...` unless they all used the same fixed rates.
    """
    sources = set(source_by_region.values())
    if len(sources) == 1 and _LIVE_PRICING not in sources:
        return sources.pop()
    if len(source_by_region) == 1:
        [(region, source)] = source_by_region.items()
        return f"{source} ({region})"
    return "; ".join(f"{region}: {source}" for region, source in source_by_region.items())


def _log_denied(error: TransportHttpError) -> None:
    logger.warning(
        "Azure answered %s for %s (x-ms-request-id %s, x-ms-correlation-request-id %s)",
        error.reason,
        error.url,
        error.request_id or "none",
        error.correlation_id or "none",
    )
