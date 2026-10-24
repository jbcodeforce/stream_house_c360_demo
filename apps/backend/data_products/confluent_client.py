"""Best-effort Confluent Cloud REST client for the Data Products graph page.

Talks to the Flink SQL Statements API (for statement phase) and the
Confluent Cloud Metrics API (for pending-records / records-in / records-out)
using credentials from ``config.settings``.

This client is used by ``providers.LiveProvider`` to populate the graph
page's live status/metrics. Live mode is explicitly best-effort: every
remote call here is wrapped so a network error, a non-2xx response, or an
unparseable payload is turned into empty/omitted data (``{}``) rather than
raised. Callers can therefore always fall back to "UNKNOWN" phases / "None"
metrics instead of breaking the page.

The Metrics API query shape (host ``api.telemetry.confluent.cloud``, the
``intervals``/``granularity``/``filter`` body, HTTP Basic auth) and the Flink
metric descriptor names (``io.confluent.flink/pending_records``,
``io.confluent.flink/compute_pool_utilization/current_cfus``) and filter
fields (``resource.flink_statement.name``, ``resource.compute_pool.id``) are
aligned with Confluent's Metrics API docs.

NOTE (verify before real live use): the Flink SQL Statements API path/version
(``{FLINK_REST_ENDPOINT}/sql/v1/organizations/{org}/environments/{env}/statements``)
has not been exercised against a live environment yet — confirm it against
Confluent's Flink REST API docs if statement phases come back empty.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Protocol

import httpx

import config

logger = logging.getLogger(__name__)

# How far back to look for the most recent metric data point. Metrics can lag
# a couple of minutes behind real time, so a short window (not just "the last
# minute") makes a fresh point reliably available.
_METRICS_LOOKBACK = timedelta(minutes=15)

_REQUEST_TIMEOUT_SECONDS = 5.0
# Confluent Cloud Metrics (Telemetry) API — query endpoint is
# {host}/v2/metrics/{dataset}/{query-type}; for Cloud resources the dataset
# is "cloud" and the query type is "query". The API authenticates with a
# Cloud API key/secret via HTTP Basic auth (passed as auth=(key, secret)
# below), NOT a bearer token.
_METRICS_API_BASE_URL = "https://api.telemetry.confluent.cloud/v2/metrics/cloud/query"


class ConfluentClientProtocol(Protocol):
    """Shape ``providers.LiveProvider`` depends on — satisfied by
    ``ConfluentClient`` and by the fake/raising test doubles in
    ``tests/test_data_products_live.py``."""

    def list_statement_phases(self) -> dict[str, str]: ...

    def get_statement_metrics(self, statement_name: str) -> dict: ...


class ConfluentClient:
    """Thin, failure-tolerant wrapper around Confluent Cloud + Flink REST APIs.

    Every public method catches its own network/HTTP/parsing errors and
    returns empty data on failure — it never raises. (``providers.LiveProvider``
    also wraps its calls to this client, so a failure here is caught twice:
    defense in depth so credentials/errors never cause the status page to
    break, no matter which layer a client implementation is swapped in at.)
    """

    def __init__(self, settings_obj: config.Settings | None = None) -> None:
        self._settings = settings_obj if settings_obj is not None else config.settings

    def list_statement_phases(self) -> dict[str, str]:
        """Return ``{statement_name: phase}`` for every known Flink statement.

        Returns ``{}`` if live credentials are not configured, or on any
        network/HTTP/parsing failure.
        """
        s = self._settings
        if not (s.FLINK_REST_ENDPOINT and s.FLINK_API_KEY and s.FLINK_API_SECRET and s.FLINK_ORG_ID and s.FLINK_ENV_ID):
            logger.info("ConfluentClient: Flink settings incomplete; skipping statement phase lookup")
            return {}

        url = (
            f"{s.FLINK_REST_ENDPOINT.rstrip('/')}/sql/v1/organizations/"
            f"{s.FLINK_ORG_ID}/environments/{s.FLINK_ENV_ID}/statements"
        )
        try:
            response = httpx.get(
                url,
                auth=(s.FLINK_API_KEY, s.FLINK_API_SECRET),
                timeout=_REQUEST_TIMEOUT_SECONDS,
            )
            response.raise_for_status()
            payload = response.json()
        except (httpx.HTTPError, ValueError):
            logger.warning("ConfluentClient: list_statement_phases failed", exc_info=True)
            return {}

        phases: dict[str, str] = {}
        for item in payload.get("data", []) if isinstance(payload, dict) else []:
            name = item.get("name")
            phase = (item.get("status") or {}).get("phase")
            if name and phase:
                phases[name] = phase
        return phases

    def _recent_interval(self) -> str:
        """An absolute ISO-8601 interval covering the recent lookback window."""
        end = datetime.now(timezone.utc)
        start = end - _METRICS_LOOKBACK
        fmt = "%Y-%m-%dT%H:%M:%SZ"
        return f"{start.strftime(fmt)}/{end.strftime(fmt)}"

    def _query_latest_value(
        self,
        metric: str,
        filters: list[tuple[str, str]],
        api_key: str,
        api_secret: str,
    ) -> float | None:
        """POST a single-metric Metrics API query and return the most recent
        data point's ``value`` (or ``None`` if there are no points).

        Raises on network/HTTP/parse failure — callers isolate each query in
        their own try/except so one failing metric doesn't drop the others.
        """
        if len(filters) == 1:
            field, value = filters[0]
            filter_obj: dict = {"field": field, "op": "EQ", "value": value}
        else:
            filter_obj = {
                "op": "AND",
                "filters": [{"field": f, "op": "EQ", "value": v} for f, v in filters],
            }
        body = {
            "aggregations": [{"metric": metric}],
            "filter": filter_obj,
            "granularity": "PT1M",
            "intervals": [self._recent_interval()],
            "limit": 1000,
        }
        response = httpx.post(
            _METRICS_API_BASE_URL,
            json=body,
            auth=(api_key, api_secret),
            timeout=_REQUEST_TIMEOUT_SECONDS,
        )
        response.raise_for_status()
        payload = response.json()
        points = payload.get("data", []) if isinstance(payload, dict) else []
        if not points:
            return None
        # Each point is {"timestamp": ISO8601, "value": <number>, ...group_by}.
        latest = max(points, key=lambda p: p.get("timestamp", ""))
        return latest.get("value")

    def get_statement_metrics(self, statement_name: str) -> dict:
        """Return raw metric fields for one statement; any key may be absent.

        Recognized keys: ``pending_records`` (statement-scoped lag) and
        ``compute_pool_cfu`` (pool-scoped CFUs in use). These two live on
        different resource dimensions, so they are two separately-scoped
        queries, each isolated so one failing doesn't drop the other.

        ``messages_in_per_sec``/``messages_out_per_sec`` are intentionally not
        populated here: the Metrics API exposes cumulative counters
        (``num_records_in``/``_out``), so a rate needs a delta between two
        reads — a future iteration can cache the previous read per statement.
        ``uptime_seconds`` is derived from the statement, not a metric.

        Returns ``{}`` if live credentials are not configured. Never raises.
        """
        s = self._settings
        if not (s.CONFLUENT_CLOUD_API_KEY and s.CONFLUENT_CLOUD_API_SECRET and s.FLINK_COMPUTE_POOL_ID):
            logger.info("ConfluentClient: Metrics API settings incomplete; skipping metrics lookup")
            return {}

        metrics: dict = {}

        # Statement-scoped: pending records (the lag / "messages behind" metric).
        try:
            pending = self._query_latest_value(
                "io.confluent.flink/pending_records",
                [
                    ("resource.compute_pool.id", s.FLINK_COMPUTE_POOL_ID),
                    ("resource.flink_statement.name", statement_name),
                ],
                s.CONFLUENT_CLOUD_API_KEY,
                s.CONFLUENT_CLOUD_API_SECRET,
            )
            if pending is not None:
                metrics["pending_records"] = int(pending)
        except (httpx.HTTPError, ValueError):
            logger.warning(
                "ConfluentClient: pending_records query failed for statement=%s",
                statement_name,
                exc_info=True,
            )

        # Pool-scoped: current CFUs in use (a compute-pool metric, not per-statement).
        try:
            cfu = self._query_latest_value(
                "io.confluent.flink/compute_pool_utilization/current_cfus",
                [("resource.compute_pool.id", s.FLINK_COMPUTE_POOL_ID)],
                s.CONFLUENT_CLOUD_API_KEY,
                s.CONFLUENT_CLOUD_API_SECRET,
            )
            if cfu is not None:
                metrics["compute_pool_cfu"] = cfu
        except (httpx.HTTPError, ValueError):
            logger.warning(
                "ConfluentClient: compute pool CFU query failed for pool=%s",
                s.FLINK_COMPUTE_POOL_ID,
                exc_info=True,
            )

        return metrics
