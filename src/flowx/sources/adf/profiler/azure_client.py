"""Live Azure access for the ADF profiler: credential selection and resource/run/cost
fetching. Every azure/requests import is lazy so importing this module costs nothing
until the profile phase actually runs.
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Protocol

from flowx.sources.adf.profiler import cost_model
from flowx.sources.adf.profiler import pricing as pricing_module
from flowx.sources.adf.profiler.models import ActivityRunCost, ActualCost, PipelineCost, ProfileResult
from flowx.sources.adf.profiler.pricing import RateCards

logger = logging.getLogger(__name__)

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
    "Install it with:  pip install 'azure-identity>=1.17'   (from a source checkout: pip install -e '.[profile]')\n"
    + _AUTH_HINT
)


class MissingProfileDependencyError(RuntimeError):
    """Raised when the profile phase runs but the `profile` extra isn't installed."""


class ProfileAuthenticationError(RuntimeError):
    """Raised when no Azure credential works; the message says how to set one up."""

    def __init__(self, detail: str):
        super().__init__(f"Azure authentication failed: {detail}\n{_AUTH_HINT}")


def require_azure() -> None:
    """Verify the Azure stack is importable; otherwise raise with setup guidance."""
    try:
        import azure.identity  # noqa: F401
    except ModuleNotFoundError as error:
        raise MissingProfileDependencyError(_INSTALL_HINT) from error


def get_credential(
    tenant_id: str | None = None,
    client_id: str | None = None,
    client_secret: str | None = None,
) -> Any:
    """Return an Azure credential.

    Explicit service-principal values win (CI / headless). Otherwise DefaultAzureCredential
    walks env-var service principal -> managed identity -> `az` CLI token -> shared cache, so the
    same code works whether the user ran `az login` locally or set env vars on serverless.
    """
    require_azure()
    from azure.identity import ClientSecretCredential, DefaultAzureCredential

    if tenant_id and client_id and client_secret:
        return ClientSecretCredential(tenant_id=tenant_id, client_id=client_id, client_secret=client_secret)
    return DefaultAzureCredential()


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

    A long scan can outlive one token (they last about an hour), so the token is refreshed shortly
    before it expires, and a 401 is retried once with a fresh one. A credential that can't produce
    a token raises ProfileAuthenticationError with setup guidance instead of a raw SDK traceback.
    """

    def __init__(self, credential: Any, *, session: Any = None):
        self._credential = credential
        self._session = session
        self._headers: dict[str, str] | None = None
        self._token_expires_on = 0.0

    def _authorized_headers(self) -> dict[str, str]:
        if self._headers is None or time.time() >= self._token_expires_on - _TOKEN_REFRESH_MARGIN_SECONDS:
            from azure.core.exceptions import ClientAuthenticationError

            try:
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
    pins the rate cards ("custom"); otherwise they come from the Retail Prices API for the first
    factory's region, falling back to list rates.
    """

    def __init__(
        self,
        *,
        credential: Any,
        days: int,
        transport: Transport | None = None,
        pricing: RateCards | None = None,
        now: datetime | None = None,
    ):
        self._transport: Transport = transport if transport is not None else LiveTransport(credential)
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
        region = next(
            (factory["location"].lower().replace(" ", "") for factory in factories if factory.get("location")),
            "eastus",
        )
        rates, pricing_source = self._resolve_pricing(region)
        result = ProfileResult(
            region=region,
            pricing_source=pricing_source,
            days=self._days,
            total_pipeline_runs=0,
            subscription_names=subscription_names,
            factory_subscriptions={
                factory["name"]: subscription_names.get(factory["subscription_id"], "") or factory["subscription_id"]
                for factory in factories
            },
        )
        if not subscriptions:
            result.permission_warnings.append(_NO_SUBSCRIPTIONS)
        elif not factories:
            result.permission_warnings.append(_NO_FACTORIES)
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
            result.permission_warnings.append(_UNCOSTED_RUNS.format(skipped=skipped_runs, total=len(runs_to_cost)))
        return result

    def _resolve_pricing(self, region: str) -> tuple[RateCards, str]:
        if self._custom_pricing:
            return self._custom_pricing, "custom"
        live = pricing_module.fetch_live_pricing(region)
        if live:
            return live, f"Azure Retail Prices API ({region})"
        return pricing_module.DEFAULT_PRICING, "default list rates"

    def _query_actual_costs(self, subscription_id: str, factory_names: list[str], result: ProfileResult) -> None:
        """Billed ADF cost for one subscription from Cost Management, attributed to factories.

        One query per subscription, grouped by meter subcategory and resource ID. A row belongs to
        the first factory whose name appears anywhere in its resource ID (a known over-match, kept
        as-is), else to a factory whose name equals the ID's last segment.
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
        try:
            data = self._transport.post_json(url, body)
        except ProfileAuthenticationError:
            raise
        except TransportHttpError as error:
            if error.status in (401, 403):
                result.permission_warnings.append(_COST_MANAGEMENT_DENIED.format(subscription=subscription_id[:8]))
                result.cost_management_denied.append(subscription_id)
            else:
                logger.warning(
                    "Cost Management query for sub %s... returned HTTP %s", subscription_id[:8], error.status
                )
            return
        except Exception as error:
            logger.warning("Cost Management query error for sub %s...: %s", subscription_id[:8], error)
            return

        properties = data.get("properties", {})
        columns = [column["name"] for column in properties.get("columns", [])]
        lowered_names = list(dict.fromkeys(name.lower() for name in factory_names))
        for row in properties.get("rows", []):
            record = dict(zip(columns, row))
            resource_id = record.get("ResourceId", "")
            matched = next((name for name in lowered_names if name in resource_id.lower()), None)
            if matched is None:
                last_segment = resource_id.rstrip("/").split("/")[-1].lower()
                matched = last_segment if last_segment in lowered_names else None
            if matched is None:
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
        continuation: str | None = None
        while True:
            body = dict(self._window)
            if continuation:
                body["continuationToken"] = continuation
            try:
                page = self._transport.post_json(url, body)
            except ProfileAuthenticationError:
                raise
            except TransportHttpError as error:
                if error.status in (401, 403):
                    result.permission_warnings.append(_PIPELINE_RUNS_DENIED.format(factory=factory["name"]))
                else:
                    logger.error("[%s] Pipeline runs: HTTP %s", factory["name"], error.status)
                return runs
            except Exception as error:
                logger.error("[%s] Pipeline runs error: %s", factory["name"], error)
                return runs
            page_runs = page.get("value", [])
            runs.extend(page_runs)
            continuation = page.get("continuationToken")
            if not continuation or not page_runs:
                return runs

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
        use (e.g. a null DIU count); like the original, only that run is dropped, not the scan.
        """
        from flowx.sources.adf.profiler.discovery import factory_base_url

        url = (
            f"{factory_base_url(factory)}/pipelineruns/{run.get('runId', '')}/queryActivityruns?api-version=2018-06-01"
        )
        try:
            activities = self._transport.post_json(url, dict(self._window)).get("value", [])
        except ProfileAuthenticationError:
            raise
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
            execution = cost_model.activity_execution_cost(
                activity_type, ir_key, activity.get("durationInMs", 0) or 0, output, activity_rates
            )
            run_costs.orchestration_cost += orchestration
            if activity_type in cost_model.COPY_ACTIVITY_TYPES:
                run_costs.data_movement_cost += execution
            elif activity_type in cost_model.EXTERNAL_ACTIVITY_TYPES or activity_type in cost_model.DATA_FLOW_TYPES:
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
