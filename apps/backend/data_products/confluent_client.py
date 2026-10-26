"""Best-effort Confluent Cloud REST client for the Data Products graph page.

Status comes from TWO Flink SQL REST sources, merged: the Statements API
(regular statements) and the Materialized Tables API. The c360 pipeline is
built from materialized tables, which are maintained by a hidden system
statement and so do NOT appear in the Statements list — the materialized-tables
endpoint is their source of truth, keyed by table name. Metrics come from the
Confluent Cloud Metrics API, using materialized-table utilization descriptors
(CFU) for materialized tables and statement/pool descriptors for regular
statements.

This client is used by ``providers.LiveProvider`` to populate the graph
page's live status/metrics. Live mode is explicitly best-effort: every
remote call here is wrapped so a network error, a non-2xx response, or an
unparseable payload is turned into empty/omitted data (``{}``) rather than
raised. Callers can therefore always fall back to "UNKNOWN" phases / "None"
metrics instead of breaking the page.

The Metrics API query shape (host ``api.telemetry.confluent.cloud``, the
``intervals``/``granularity``/``filter`` body, HTTP Basic auth with a Cloud API
key whose principal has the MetricsViewer role) and the Flink metric descriptor
names (``io.confluent.flink/pending_records``,
``io.confluent.flink/materialized_table_utilization/current_cfus``) are aligned
with Confluent's docs. See ``_MT_NAME_DIMENSION`` for the one filter-field
spelling that is a best-effort guess (a 422 body, now logged, names the
correct field if it is wrong).

NOTE (verify before real live use): the Flink SQL Statements and
Materialized-Tables API paths/versions (``.../sql/v1/organizations/{org}/
environments/{env}/{statements|materialized-tables}``) — confirm against
Confluent's Flink REST API docs if phases come back empty.
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


def _is_auth_error(exc: Exception) -> bool:
    """True for a 401/403 HTTP response — an auth/authorization failure."""
    return isinstance(exc, httpx.HTTPStatusError) and exc.response.status_code in (401, 403)


_METRICS_AUTH_HINT = (
    "ConfluentClient: Metrics API returned %s for %s — the Cloud API key's "
    "principal is likely missing the MetricsViewer role. Use a Cloud API key "
    "(resource scope 'cloud') whose service account/user has MetricsViewer "
    "(confluent iam rbac role-binding create --role MetricsViewer). "
    "Changing the key alone won't help; 403 means authenticated-but-not-authorized."
)


def _log_metrics_error(exc: Exception, context: str) -> None:
    """Log a Metrics API failure concisely and actionably (no raw traceback
    for the common HTTP errors). A 4xx body carries the precise reason — a 422
    names the offending field, a 403 the missing authorization — so surface it."""
    if _is_auth_error(exc):
        logger.warning(_METRICS_AUTH_HINT, exc.response.status_code, context)
    elif isinstance(exc, httpx.HTTPStatusError):
        logger.warning(
            "ConfluentClient: Metrics API %s for %s: %s",
            exc.response.status_code,
            context,
            exc.response.text[:500],
        )
    else:
        logger.warning("ConfluentClient: Metrics API query failed for %s", context, exc_info=True)

_REQUEST_TIMEOUT_SECONDS = 5.0
# Confluent Cloud Metrics (Telemetry) API — query endpoint is
# {host}/v2/metrics/{dataset}/{query-type}; for Cloud resources the dataset
# is "cloud" and the query type is "query". The API authenticates with a
# Cloud API key/secret via HTTP Basic auth (passed as auth=(key, secret)
# below), NOT a bearer token.
_METRICS_API_BASE_URL = "https://api.telemetry.confluent.cloud/v2/metrics/cloud/query"


# Metrics API filter field (dimension) that scopes a query to one
# materialized table by name. The MT utilization metrics are published with
# materialized-table dimensions (name / UID / version), not the compute-pool
# or flink_statement dimensions. BEST-EFFORT: this exact field spelling is not
# in the prose docs; it follows Confluent's `resource.<entity>.<attr>`
# convention (cf. the confirmed `resource.flink_statement.name`). If CFU shows
# "—" in live mode, the Metrics API 422 body (now logged) names the correct
# field — update this one constant.
_MT_NAME_DIMENSION = "resource.flink_materialized_table.name"


class ConfluentClientProtocol(Protocol):
    """Shape ``providers.LiveProvider`` depends on — satisfied by
    ``ConfluentClient`` and by the fake/raising test doubles in
    ``tests/test_data_products_live.py``."""

    def list_statement_phases(self) -> dict[str, str]: ...

    def list_materialized_table_phases(self) -> dict[str, str]: ...

    def get_statement_metrics(self, statement_name: str) -> dict: ...

    def get_materialized_table_metrics(self, mt_name: str) -> dict: ...


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

    def list_materialized_table_phases(self) -> dict[str, str]:
        """Return ``{materialized_table_name: phase}`` for every materialized table.

        Materialized tables are maintained by a hidden system statement, so they
        do NOT appear in the Flink Statements list — this dedicated API is their
        source of truth. Keyed by the table ``name`` (e.g. ``src_customers``),
        which matches the graph node's label/id, not the dbt statement name.

        Returns ``{}`` if live credentials are not configured, or on any
        network/HTTP/parsing failure.
        """
        s = self._settings
        if not (s.FLINK_REST_ENDPOINT and s.FLINK_API_KEY and s.FLINK_API_SECRET and s.FLINK_ORG_ID and s.FLINK_ENV_ID):
            logger.info("ConfluentClient: Flink settings incomplete; skipping materialized-table lookup")
            return {}

        url = (
            f"{s.FLINK_REST_ENDPOINT.rstrip('/')}/sql/v1/organizations/"
            f"{s.FLINK_ORG_ID}/environments/{s.FLINK_ENV_ID}/materialized-tables"
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
            logger.warning("ConfluentClient: list_materialized_table_phases failed", exc_info=True)
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
        except (httpx.HTTPError, ValueError) as exc:
            _log_metrics_error(exc, f"statement={statement_name}")

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
        except (httpx.HTTPError, ValueError) as exc:
            _log_metrics_error(exc, f"pool={s.FLINK_COMPUTE_POOL_ID}")

        return metrics

    def get_materialized_table_metrics(self, mt_name: str) -> dict:
        """Return raw metric fields for one materialized table; keys may be absent.

        Materialized tables bill on their own CFU utilization metrics (scoped by
        the materialized-table name dimension), distinct from the per-statement /
        compute-pool metrics — the compute-pool view mixes statement and MT
        usage, so it is not used here. Recognized key: ``compute_pool_cfu``
        (current CFUs the materialized table is consuming).

        Returns ``{}`` if live credentials are not configured. Never raises.
        """
        s = self._settings
        if not (s.CONFLUENT_CLOUD_API_KEY and s.CONFLUENT_CLOUD_API_SECRET):
            logger.info("ConfluentClient: Metrics API settings incomplete; skipping MT metrics lookup")
            return {}

        metrics: dict = {}
        try:
            cfu = self._query_latest_value(
                "io.confluent.flink/materialized_table_utilization/current_cfus",
                [(_MT_NAME_DIMENSION, mt_name)],
                s.CONFLUENT_CLOUD_API_KEY,
                s.CONFLUENT_CLOUD_API_SECRET,
            )
            if cfu is not None:
                metrics["compute_pool_cfu"] = cfu
        except (httpx.HTTPError, ValueError) as exc:
            _log_metrics_error(exc, f"materialized_table={mt_name}")

        return metrics
