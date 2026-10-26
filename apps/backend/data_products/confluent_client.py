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
with Confluent's docs. Materialized-table metrics are scoped by the MT's UID
(``_MT_UID_DIMENSION``), the canonical resource id — a ``.name`` filter fails
the Metrics API's authorized-resource check (403). The UID is read from the
materialized-tables list (``metadata.uid``).

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


# A 403 from the Metrics API has TWO distinct causes, and the response body
# (logged below) is what distinguishes them: (a) the Cloud API key's principal
# lacks the MetricsViewer role, or (b) the query isn't scoped to an authorized
# resource — typically because a filter dimension is wrong/unrecognized, so the
# API treats the query as filtering by no authorized resource at all.
_AUTH_ERROR_HINT = (
    " | 403/401 cause is in the body above: either the Cloud API key's principal "
    "lacks the MetricsViewer role (confluent iam rbac role-binding create --role "
    "MetricsViewer), or the query isn't scoped to an authorized resource (often a "
    "wrong/unrecognized filter dimension — see _MT_UID_DIMENSION)."
)


def _log_metrics_error(exc: Exception, context: str) -> None:
    """Log a Metrics API failure concisely and actionably (no raw traceback for
    HTTP errors). The 4xx response body carries the precise reason — a 422 names
    the offending field, a 403 says whether it's a role or a resource-scope
    problem — so always surface the body."""
    if isinstance(exc, httpx.HTTPStatusError):
        status = exc.response.status_code
        body = exc.response.text[:800]
        hint = _AUTH_ERROR_HINT if status in (401, 403) else ""
        logger.warning("ConfluentClient: Metrics API %s for %s: %s%s", status, context, body, hint)
    else:
        logger.warning("ConfluentClient: Metrics API query failed for %s", context, exc_info=True)

_REQUEST_TIMEOUT_SECONDS = 5.0
# Confluent Cloud Metrics (Telemetry) API — query endpoint is
# {host}/v2/metrics/{dataset}/{query-type}; for Cloud resources the dataset
# is "cloud" and the query type is "query". The API authenticates with a
# Cloud API key/secret via HTTP Basic auth (passed as auth=(key, secret)
# below), NOT a bearer token.
_METRICS_API_BASE_URL = "https://api.telemetry.confluent.cloud/v2/metrics/cloud/query"


# Metrics API filter field (dimension) that scopes a query to one materialized
# table. The MT utilization metrics carry the resource type
# `flink_materialized_table`, whose filterable labels are .name / .uid /
# .version (confirmed via the Metrics API resource descriptors). Authorization
# is keyed on the canonical resource id, so the query must filter by .uid — a
# .name filter fails the "must filter by an authorized resource" check (403).
# The uid comes from the materialized-tables list (metadata.uid).
_MT_UID_DIMENSION = "resource.flink_materialized_table.uid"


class ConfluentClientProtocol(Protocol):
    """Shape ``providers.LiveProvider`` depends on — satisfied by
    ``ConfluentClient`` and by the fake/raising test doubles in
    ``tests/test_data_products_live.py``."""

    def list_statement_phases(self) -> dict[str, str]: ...

    def list_materialized_tables(self) -> dict[str, dict]: ...

    def get_statement_metrics(self, statement_name: str) -> dict: ...

    def get_materialized_table_metrics(self, mt_uid: str) -> dict: ...


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

    def list_materialized_tables(self) -> dict[str, dict]:
        """Return ``{name: {"phase", "uid", "version"}}`` for every materialized table.

        Materialized tables are maintained by a hidden system statement, so they
        do NOT appear in the Flink Statements list — this dedicated API is their
        source of truth. Keyed by the table ``name`` (e.g. ``src_customers``),
        which matches the graph node's label/id, not the dbt statement name. The
        ``uid`` (metadata.uid) is the canonical resource id used to scope the
        Metrics API query (see ``get_materialized_table_metrics``).

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
            logger.warning("ConfluentClient: list_materialized_tables failed", exc_info=True)
            return {}

        tables: dict[str, dict] = {}
        for item in payload.get("data", []) if isinstance(payload, dict) else []:
            name = item.get("name")
            if not name:
                continue
            status = item.get("status") or {}
            tables[name] = {
                "phase": status.get("phase"),
                "uid": (item.get("metadata") or {}).get("uid"),
                "version": status.get("version"),
            }
        return tables

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

    def get_materialized_table_metrics(self, mt_uid: str) -> dict:
        """Return raw metric fields for one materialized table; keys may be absent.

        Materialized tables bill on their own CFU utilization metrics, scoped by
        the materialized-table UID (the canonical resource id — a .name filter
        fails the Metrics API's authorized-resource check). The compute-pool view
        mixes statement and MT usage, so it is not used here. Recognized key:
        ``compute_pool_cfu`` (current CFUs the materialized table is consuming).

        Returns ``{}`` if live credentials are not configured, or if ``mt_uid``
        is empty. Never raises.
        """
        s = self._settings
        if not (s.CONFLUENT_CLOUD_API_KEY and s.CONFLUENT_CLOUD_API_SECRET):
            logger.info("ConfluentClient: Metrics API settings incomplete; skipping MT metrics lookup")
            return {}
        if not mt_uid:
            return {}

        metrics: dict = {}
        try:
            cfu = self._query_latest_value(
                "io.confluent.flink/materialized_table_utilization/current_cfus",
                [(_MT_UID_DIMENSION, mt_uid)],
                s.CONFLUENT_CLOUD_API_KEY,
                s.CONFLUENT_CLOUD_API_SECRET,
            )
            if cfu is not None:
                metrics["compute_pool_cfu"] = cfu
        except (httpx.HTTPError, ValueError) as exc:
            _log_metrics_error(exc, f"materialized_table_uid={mt_uid}")

        return metrics
