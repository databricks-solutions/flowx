"""Live Azure access for the ADF profiler: credential selection and resource/run/cost
fetching. Every azure/requests import is lazy so importing this module costs nothing
until the profile phase actually runs.
"""

from __future__ import annotations

import logging
import time
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Protocol

from flowx.sources.adf.profiler import cost_model
from flowx.sources.adf.profiler import pricing as pricing_module
from flowx.sources.adf.profiler.models import ActivityRunCost, ActualCost, PipelineCost, ProfileResult
from flowx.sources.adf.profiler.pricing import RateCards

logger = logging.getLogger(__name__)

_LIVE_PRICING = "Azure Retail Prices API"

AZURE_MANAGEMENT_URL = "https://management.azure.com"
_MANAGEMENT_SCOPE = "https://management.azure.com/.default"
_REQUEST_TIMEOUT_SECONDS = 30
# Refresh a little before expiry so a token can't lapse between the check and the request.
_TOKEN_REFRESH_MARGIN_SECONDS = 300

_AUTH_HINT = (
    "Authenticate for your environment:\n"
    "  - Local / Claude Code:   run `az login` (a browser opens), or `az login --use-device-code`.\n"
    "  - Genie Code / serverless / CI: set a service principal via env vars\n"
    "      AZURE_CLIENT_ID / AZURE_CLIENT_SECRET / AZURE_TENANT_ID\n"
    "      (on Databricks, read them from a secret scope)."
)
_INSTALL_HINT = (
    "The `flowx profile` phase needs the Azure stack, which is an optional extra.\n"
    "Install it with:  pip install 'azure-identity>=1.17' 'requests>=2.31'   "
    "(from a source checkout: pip install -e '.[azure]')\n" + _AUTH_HINT
)


class MissingProfileDependencyError(RuntimeError):
    """Raised when the profile phase runs but the `azure` extra isn't installed."""


class ProfileAuthenticationError(RuntimeError):
    """Raised when no Azure credential works; the message says how to set one up."""

    def __init__(self, detail: str):
        super().__init__(f"Azure authentication failed: {detail}\n{_AUTH_HINT}")


def require_azure() -> None:
    """Verify every module of the `azure` extra is importable; otherwise raise with setup guidance."""
    try:
        import azure.identity  # noqa: F401
        import requests  # noqa: F401
    except ModuleNotFoundError as error:
        raise MissingProfileDependencyError(_INSTALL_HINT) from error


def get_credential() -> Any:
    """Return a DefaultAzureCredential.

    It walks env-var service principal (AZURE_TENANT_ID / AZURE_CLIENT_ID / AZURE_CLIENT_SECRET)
    -> managed identity -> `az` CLI token -> shared cache, so the same code works whether the user
    ran `az login` locally or set env vars on serverless. Any tenant is allowed so `--tenant-id`
    can pick one per token request.
    """
    require_azure()
    from azure.identity import DefaultAzureCredential

    return DefaultAzureCredential(additionally_allowed_tenants=["*"])


class TransportHttpError(Exception):
    """A non-200 answer from Azure. `status` lets callers turn 401/403 into "missing role" notes."""

    def __init__(self, status: int, url: str):
        super().__init__(f"HTTP {status} from {url}")
        self.status = status
        self.url = url


class Transport(Protocol):
    """The only way the profiler talks to Azure, so tests can replay recorded responses."""

    def get_json(self, url: str) -> dict[str, Any]: ...

    def post_json(self, url: str, body: dict[str, Any]) -> dict[str, Any]: ...


class LiveTransport:
    """Calls Azure Resource Manager over HTTPS with a bearer token from `credential`.

    Tokens are requested from `tenant_id` when one is given. A long scan can outlive one token
    (they last about an hour), so the token is refreshed shortly before it expires, and a 401 is
    retried once with a fresh one. A credential that can't produce a token raises
    ProfileAuthenticationError with setup guidance instead of a raw SDK traceback.
    """

    def __init__(self, credential: Any, *, tenant_id: str | None = None, session: Any = None):
        self._credential = credential
        self._tenant_id = tenant_id
        self._session = session
        self._headers: dict[str, str] | None = None
        self._token_expires_on = 0.0

    def _authorized_headers(self) -> dict[str, str]:
        if self._headers is None or time.time() >= self._token_expires_on - _TOKEN_REFRESH_MARGIN_SECONDS:
            from azure.core.exceptions import ClientAuthenticationError

            try:
                if self._tenant_id:
                    access_token = self._credential.get_token(_MANAGEMENT_SCOPE, tenant_id=self._tenant_id)
                else:
                    access_token = self._credential.get_token(_MANAGEMENT_SCOPE)
            except ClientAuthenticationError as error:
                raise ProfileAuthenticationError(str(error)) from error
            self._token_expires_on = float(getattr(access_token, "expires_on", float("inf")))
            self._headers = {"Authorization": f"Bearer {access_token.token}", "Content-Type": "application/json"}
        return self._headers

    def _http(self) -> Any:
        if self._session is None:
            import requests

            self._session = requests.Session()
        return self._session

    def _send(self, method: str, url: str, body: dict[str, Any] | None) -> dict[str, Any]:
        for attempt in range(2):
            headers = self._authorized_headers()
            if method == "GET":
                response = self._http().get(url, headers=headers, timeout=_REQUEST_TIMEOUT_SECONDS)
            else:
                response = self._http().post(url, headers=headers, json=body, timeout=_REQUEST_TIMEOUT_SECONDS)
            if response.status_code == 401 and attempt == 0:
                self._headers = None
                continue
            if response.status_code != 200:
                raise TransportHttpError(response.status_code, url)
            payload: dict[str, Any] = response.json()
            return payload
        raise AssertionError("unreachable")

    def get_json(self, url: str) -> dict[str, Any]:
        return self._send("GET", url, None)

    def post_json(self, url: str, body: dict[str, Any]) -> dict[str, Any]:
        return self._send("POST", url, body)


_PIPELINE_RUNS_DENIED = "{factory}: Cannot query pipeline runs (need Data Factory Contributor)"
_ACTIVITY_RUNS_DENIED = "{factory}: Cannot query activity runs (need Data Factory Contributor)"
_NO_SUBSCRIPTIONS = (
    "No accessible subscriptions found. The signed-in identity may belong to the wrong tenant "
    "(on Databricks, the workspace's own identity is in Databricks' tenant, not yours).\n" + _AUTH_HINT
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
    "(need Billing Reader or Cost Management Reader on this subscription)"
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
        # Imported here because discovery imports this module's transport types.
        from flowx.sources.adf.profiler import discovery

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
                if not next_url.startswith(f"{AZURE_MANAGEMENT_URL}/"):
                    result.warnings.append(
                        _COST_MANAGEMENT_OFF_HOST.format(subscription=subscription_id[:8], host=AZURE_MANAGEMENT_URL)
                    )
                    break
                seen_urls.add(next_url)
                properties = self._transport.post_json(next_url, body).get("properties", {})
                columns = [column["name"] for column in properties.get("columns", [])]
                records.extend(dict(zip(columns, row)) for row in properties.get("rows", []))
                next_url = properties.get("nextLink")
        except ProfileAuthenticationError:
            raise
        except TransportHttpError as error:
            if error.status in (401, 403):
                result.warnings.append(_COST_MANAGEMENT_DENIED.format(subscription=subscription_id[:8]))
                result.cost_management_denied.append(subscription_id)
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
        from flowx.sources.adf.profiler.discovery import factory_base_url

        url = f"{factory_base_url(factory)}/queryPipelineRuns?api-version=2018-06-01"
        runs: list[dict[str, Any]] = []
        try:
            for page in self._query_pages(url):
                runs.extend(page)
        except ProfileAuthenticationError:
            raise
        except TransportHttpError as error:
            if error.status in (401, 403):
                result.warnings.append(_PIPELINE_RUNS_DENIED.format(factory=factory["name"]))
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
        from flowx.sources.adf.profiler.discovery import factory_base_url

        url = (
            f"{factory_base_url(factory)}/pipelineruns/{run.get('runId', '')}/queryActivityruns?api-version=2018-06-01"
        )
        try:
            activities = [activity for page in self._query_pages(url) for activity in page]
        except ProfileAuthenticationError:
            raise
        except TransportHttpError as error:
            denied = _ACTIVITY_RUNS_DENIED.format(factory=factory["name"])
            if error.status in (401, 403) and denied not in result.warnings:
                result.warnings.append(denied)
            return None
        except Exception:
            return None

        try:
            run_costs, activity_costs = self._cost_activities(factory, activities, ir_type_map, rates)
        except Exception as error:
            logger.debug("[%s] Could not cost run %s: %s", factory["name"], run.get("runId", ""), error)
            return None
        result.activity_runs.extend(activity_costs)
        return run_costs

    @staticmethod
    def _cost_activities(
        factory: dict[str, Any], activities: list[dict[str, Any]], ir_type_map: dict[str, str], rates: RateCards
    ) -> tuple[_PipelineTotals, list[ActivityRunCost]]:
        from flowx.sources.adf.profiler.discovery import classify_ir

        run_costs = _PipelineTotals()
        activity_costs: list[ActivityRunCost] = []
        for activity in activities:
            activity_type = activity.get("activityType", "")
            output = activity.get("output") or {}
            ir_key = classify_ir(output.get("effectiveIntegrationRuntime", ""), ir_type_map)
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
            activity_costs.append(
                ActivityRunCost(
                    factory_name=factory["name"],
                    pipeline_name=activity.get("pipelineName", ""),
                    activity_name=activity.get("activityName", ""),
                    activity_type=activity_type,
                    ir_type=ir_key,
                    orchestration_cost=round(orchestration, 6),
                    execution_cost=round(execution, 6),
                    total_cost=round(orchestration + execution, 6),
                )
            )
        # Each pipeline run also pays one trigger orchestration at the Azure IR rate.
        run_costs.orchestration_cost += cost_model.orchestration_cost(rates["azure_ir"])
        return run_costs, activity_costs


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
