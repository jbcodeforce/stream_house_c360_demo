"""Best-effort Confluent Cloud REST client for the Data Products graph page.

Status comes from TWO Flink SQL REST sources, merged: the Statements API
(regular statements) and the Materialized Tables API. A pipeline may be 
built from streaming tables or materialized tables, which are maintained by a hidden system
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
with Confluent's docs. Materialized-table metrics are scoped by the compute
pool id AND the MT's UID (``_MT_UID_DIMENSION``): the principal is authorized on
the pool, so filtering by the uid alone (or by ``.name``) names no authorized
resource and fails the Metrics API's authorized-resource check (403). The UID
is read from the materialized-tables list (``metadata.uid``).

NOTE (verify before real live use): the Flink SQL Statements and
Materialized-Tables API paths/versions (``.../sql/v1/organizations/{org}/
environments/{env}/{statements|materialized-tables}``) — confirm against
Confluent's Flink REST API docs if phases come back empty.
"""

from __future__ import annotations

import logging
from datetime import date, datetime, timedelta, timezone
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

# Confluent Cloud Cost (Billing) API. Org-scoped; authenticates with the Cloud
# API key/secret via HTTP Basic (same key as the Metrics API, but the Cost API
# needs an org-level billing read role, not just MetricsViewer). Line items are
# DAILY and carry the billed resource id in resource.id — for Flink that is the
# compute pool id (lfcp-...). Pagination follows metadata.next (a full URL).
_COST_API_URL = "https://api.confluent.cloud/billing/v1/costs"
_COST_PAGE_SIZE = 10000


# Metrics API filter field (dimension) that narrows an MT utilization query to
# one materialized table. The MT utilization metrics carry both the
# `compute_pool` and `flink_materialized_table` resource types; the latter's
# filterable labels are .name / .uid / .version (confirmed via the Metrics API
# resource descriptors). Authorization is on the compute pool, so this uid
# filter must be AND-ed with resource.compute_pool.id — on its own it fails the
# "must filter by an authorized resource" check (403). The uid comes from the
# materialized-tables list (metadata.uid).
_MT_UID_DIMENSION = "resource.flink_materialized_table.uid"


# Records-in/out counters backing messages_in/out_per_sec. They are COUNTER
# metrics: the Metrics API returns the count accumulated within each granularity
# interval (PT1M), so the latest point is records-per-minute and a per-second
# rate divides by the interval length. Like all Flink metrics here they are
# authorized on the compute pool, so queries AND the pool id with the per-node
# dimension (statement name or MT uid).
_RECORDS_IN_METRIC = "io.confluent.flink/num_records_in"
_RECORDS_OUT_METRIC = "io.confluent.flink/num_records_out"
_GRANULARITY_SECONDS = 60  # PT1M


class ConfluentClientProtocol(Protocol):
    """Shape ``providers.LiveProvider`` depends on — satisfied by
    ``ConfluentClient`` and by the fake/raising test doubles in
    ``tests/test_data_products_live.py``."""

    def list_statement_phases(self) -> dict[str, str]: ...

    def list_statements(self) -> dict[str, dict]: ...

    def list_materialized_tables(self) -> dict[str, dict]: ...

    def get_statement_metrics(self, statement_name: str) -> dict: ...

    def get_materialized_table_metrics(self, mt_uid: str) -> dict: ...

    def get_flink_pool_costs(self, start_date: date, end_date: date) -> dict[str, float]: ...

    def get_compute_pool_cfu(self, pool_id: str) -> float | None: ...


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

    def list_statements(self) -> dict[str, dict]:
        """Return ``{name: {"phase", "uid", "sql"}}`` for every Flink statement.

        Same GET as ``list_statement_phases`` but carries the fields the metrics
        drill-down needs: the ``uid`` (``metadata.uid``) and the defining SQL
        (``spec.statement``) — the latter is how a streaming-table node fills its
        drill-down SQL, mirroring how a materialized table reads ``spec.query``.
        ``sql`` is ``None`` when the statement omits it.

        Returns ``{}`` if live credentials are not configured, or on any
        network/HTTP/parsing failure.
        """
        s = self._settings
        if not (s.FLINK_REST_ENDPOINT and s.FLINK_API_KEY and s.FLINK_API_SECRET and s.FLINK_ORG_ID and s.FLINK_ENV_ID):
            logger.info("ConfluentClient: Flink settings incomplete; skipping statement lookup")
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
            logger.warning("ConfluentClient: list_statements failed", exc_info=True)
            return {}

        statements: dict[str, dict] = {}
        for item in payload.get("data", []) if isinstance(payload, dict) else []:
            name = item.get("name")
            if not name:
                continue
            metadata = item.get("metadata") or {}
            statements[name] = {
                "phase": (item.get("status") or {}).get("phase"),
                "uid": metadata.get("uid"),
                # The statement's defining SQL (spec.statement), for the drill-down panel.
                "sql": (item.get("spec") or {}).get("statement"),
                # Start time (metadata.created_at), the basis for uptime_seconds.
                "created_at": metadata.get("created_at"),
            }
        return statements

    def list_materialized_tables(self) -> dict[str, dict]:
        """Return ``{name: {"phase", "uid", "version", "sql"}}`` for every materialized table.

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
            metadata = item.get("metadata") or {}
            tables[name] = {
                "phase": status.get("phase"),
                "uid": metadata.get("uid"),
                "version": status.get("version"),
                # The MT's defining SQL (the CREATE ... AS SELECT body), for the
                # drill-down panel. The MT's DDL statement isn't in the
                # Statements list, so spec.query is the live source for it.
                "sql": (item.get("spec") or {}).get("query"),
                # Start time (metadata.created_at), the basis for uptime_seconds.
                "created_at": metadata.get("created_at"),
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

    def _records_rates(
        self,
        base_filters: list[tuple[str, str]],
        api_key: str,
        api_secret: str,
        context: str,
    ) -> dict:
        """Return ``messages_in_per_sec`` / ``messages_out_per_sec`` from the
        records-in/out counters over ``base_filters`` (already scoped to an
        authorized resource — the compute pool AND the per-node dimension). The
        counter's latest point is records-per-PT1M-interval, so divide by the
        interval length for a per-second rate. Each query is isolated so one
        failing metric doesn't drop the other, and the method never raises.
        """
        rates: dict = {}
        for field, metric in (
            ("messages_in_per_sec", _RECORDS_IN_METRIC),
            ("messages_out_per_sec", _RECORDS_OUT_METRIC),
        ):
            try:
                value = self._query_latest_value(metric, base_filters, api_key, api_secret)
                if value is not None:
                    rates[field] = value / _GRANULARITY_SECONDS
            except (httpx.HTTPError, ValueError) as exc:
                _log_metrics_error(exc, f"{metric} {context}")
        return rates

    @staticmethod
    def _latest_point_value(group: dict) -> float | None:
        """The most recent value in one GROUPED result entry.

        A GROUPED entry carries a ``points`` list (``{timestamp, value}``); some
        responses collapse a single interval to a flat ``value`` instead. Return
        the latest point's value, or the flat value, or ``None`` when empty.
        """
        points = group.get("points")
        if points:
            latest = max(points, key=lambda p: p.get("timestamp", ""))
            return latest.get("value")
        return group.get("value")

    def _query_statement_metric_grouped(
        self,
        metric: str,
        api_key: str,
        api_secret: str,
    ) -> dict[str, float]:
        """Return ``{statement_name: latest_value}`` for one metric across the
        compute pool.

        Unlike the MT uid (a filterable authorized resource),
        ``resource.flink_statement.name`` is only a *group-by* dimension — an EQ
        filter on it names no authorized resource and the query comes back empty.
        So the per-statement metrics filter by the authorized compute pool and
        GROUP BY the statement name, and the caller selects the one statement it
        wants from the result. Mirrors the shift_left ``metric_mgr`` pattern.

        Raises on network/HTTP/parse failure — callers isolate each query.
        """
        body = {
            "aggregations": [{"metric": metric}],
            "filter": {
                "field": "resource.compute_pool.id",
                "op": "EQ",
                "value": self._settings.FLINK_COMPUTE_POOL_ID,
            },
            "granularity": "PT1M",
            "format": "GROUPED",
            "group_by": ["resource.compute_pool.id", "resource.flink_statement.name"],
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
        groups = payload.get("data", []) if isinstance(payload, dict) else []
        values: dict[str, float] = {}
        for group in groups:
            name = group.get("resource.flink_statement.name")
            if not name:
                continue
            value = self._latest_point_value(group)
            if value is not None:
                values[name] = value
        return values

    def get_statement_metrics(self, statement_name: str) -> dict:
        """Return raw metric fields for one statement; any key may be absent.

        Recognized keys: ``pending_records`` (the lag / "messages behind"
        metric), ``compute_pool_cfu`` (pool-scoped CFUs in use), and
        ``messages_in_per_sec``/``messages_out_per_sec`` (from the records-in/out
        counters). The per-statement metrics are queried pool-scoped + GROUPED by
        statement name (``resource.flink_statement.name`` is not EQ-filterable —
        see ``_query_statement_metric_grouped``), then this statement is picked
        out of the grouped result; the pool CFU is a separate pool-only query.
        Each query is isolated so one failing doesn't drop the others.

        Returns ``{}`` if live credentials are not configured. Never raises.
        """
        s = self._settings
        if not (s.CONFLUENT_CLOUD_API_KEY and s.CONFLUENT_CLOUD_API_SECRET and s.FLINK_COMPUTE_POOL_ID):
            logger.info("ConfluentClient: Metrics API settings incomplete; skipping metrics lookup")
            return {}

        metrics: dict = {}

        # Pending records (the lag metric): grouped by statement, pick this one.
        try:
            pending_by_stmt = self._query_statement_metric_grouped(
                "io.confluent.flink/pending_records",
                s.CONFLUENT_CLOUD_API_KEY,
                s.CONFLUENT_CLOUD_API_SECRET,
            )
            if statement_name in pending_by_stmt:
                metrics["pending_records"] = int(pending_by_stmt[statement_name])
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

        # Throughput: records-in/out per second, grouped by statement, pick this
        # one. The counter's latest point is records-per-PT1M-interval, so divide
        # by the interval length for a per-second rate.
        for field, metric in (
            ("messages_in_per_sec", _RECORDS_IN_METRIC),
            ("messages_out_per_sec", _RECORDS_OUT_METRIC),
        ):
            try:
                by_stmt = self._query_statement_metric_grouped(
                    metric, s.CONFLUENT_CLOUD_API_KEY, s.CONFLUENT_CLOUD_API_SECRET
                )
                if statement_name in by_stmt:
                    metrics[field] = by_stmt[statement_name] / _GRANULARITY_SECONDS
            except (httpx.HTTPError, ValueError) as exc:
                _log_metrics_error(exc, f"{metric} statement={statement_name}")

        return metrics

    def get_materialized_table_metrics(self, mt_uid: str) -> dict:
        """Return raw metric fields for one materialized table; keys may be absent.

        Materialized tables report their own per-table CFU utilization. The
        query must be scoped by BOTH the compute pool id AND the MT uid: the
        MetricsViewer principal is authorized on the compute pool, so a uid-only
        filter names no authorized resource and the API returns 403 ("Query must
        filter by at least one of your authorized resources"). Adding
        ``resource.compute_pool.id`` scopes to the authorized pool while the uid
        narrows to this one table. Recognized keys: ``compute_pool_cfu`` (current
        CFUs the materialized table is consuming — the per-table ``current_cfus``
        gauge, not the pool-wide total) and ``messages_in_per_sec`` /
        ``messages_out_per_sec`` (from the records-in/out counters, same scope).

        Returns ``{}`` if live credentials (incl. the compute pool id) are not
        configured, or if ``mt_uid`` is empty. Never raises.
        """
        s = self._settings
        if not (s.CONFLUENT_CLOUD_API_KEY and s.CONFLUENT_CLOUD_API_SECRET and s.FLINK_COMPUTE_POOL_ID):
            logger.info("ConfluentClient: Metrics API settings incomplete; skipping MT metrics lookup")
            return {}
        if not mt_uid:
            return {}

        scope = [
            ("resource.compute_pool.id", s.FLINK_COMPUTE_POOL_ID),
            (_MT_UID_DIMENSION, mt_uid),
        ]
        metrics: dict = {}
        try:
            cfu = self._query_latest_value(
                "io.confluent.flink/materialized_table_utilization/current_cfus",
                scope,
                s.CONFLUENT_CLOUD_API_KEY,
                s.CONFLUENT_CLOUD_API_SECRET,
            )
            if cfu is not None:
                metrics["compute_pool_cfu"] = cfu
        except (httpx.HTTPError, ValueError) as exc:
            _log_metrics_error(exc, f"materialized_table_uid={mt_uid}")

        # Throughput: records-in/out per second, same pool + MT scope.
        metrics.update(
            self._records_rates(
                scope,
                s.CONFLUENT_CLOUD_API_KEY,
                s.CONFLUENT_CLOUD_API_SECRET,
                f"materialized_table_uid={mt_uid}",
            )
        )

        return metrics

    def get_flink_pool_costs(self, start_date: date, end_date: date) -> dict[str, float]:
        """Return ``{compute_pool_id: summed_amount}`` for Flink cost over the
        ``[start_date, end_date]`` window.

        Queries the Cost API, keeps only ``product == "FLINK"`` line items, and
        sums their post-discount ``amount`` by ``resource.id`` (the compute pool
        id). Follows ``metadata.next`` pagination. Authenticates with the Cloud
        API key/secret via HTTP Basic.

        Returns ``{}`` if the Cloud credentials are not configured, or on any
        network/HTTP/parsing failure. Never raises.
        """
        s = self._settings
        if not (s.CONFLUENT_CLOUD_API_KEY and s.CONFLUENT_CLOUD_API_SECRET):
            logger.info("ConfluentClient: Cloud credentials incomplete; skipping cost lookup")
            return {}

        totals: dict[str, float] = {}
        url: str | None = _COST_API_URL
        params: dict | None = {
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
            "page_size": _COST_PAGE_SIZE,
        }
        try:
            while url:
                response = httpx.get(
                    url,
                    params=params,
                    auth=(s.CONFLUENT_CLOUD_API_KEY, s.CONFLUENT_CLOUD_API_SECRET),
                    timeout=_REQUEST_TIMEOUT_SECONDS,
                )
                response.raise_for_status()
                payload = response.json()
                items = payload.get("data", []) if isinstance(payload, dict) else []
                for item in items:
                    if item.get("product") != "FLINK":
                        continue
                    pool_id = (item.get("resource") or {}).get("id")
                    if not pool_id:
                        continue
                    totals[pool_id] = totals.get(pool_id, 0.0) + float(item.get("amount") or 0)
                # metadata.next is a full URL carrying the page_token; it already
                # includes start/end, so drop the explicit params after page one.
                url = (payload.get("metadata") or {}).get("next") if isinstance(payload, dict) else None
                params = None
        except (httpx.HTTPError, ValueError):
            logger.warning("ConfluentClient: get_flink_pool_costs failed", exc_info=True)
            return {}

        return totals

    def get_compute_pool_cfu(self, pool_id: str) -> float | None:
        """Return the current CFUs in use for one compute pool, or ``None``.

        The ``compute_pool_utilization/current_cfus`` gauge, EQ-filtered on the
        authorized compute pool id. Returns ``None`` if the Cloud credentials or
        ``pool_id`` are missing, or on any failure. Never raises.
        """
        s = self._settings
        if not (s.CONFLUENT_CLOUD_API_KEY and s.CONFLUENT_CLOUD_API_SECRET and pool_id):
            return None
        try:
            return self._query_latest_value(
                "io.confluent.flink/compute_pool_utilization/current_cfus",
                [("resource.compute_pool.id", pool_id)],
                s.CONFLUENT_CLOUD_API_KEY,
                s.CONFLUENT_CLOUD_API_SECRET,
            )
        except (httpx.HTTPError, ValueError) as exc:
            _log_metrics_error(exc, f"cost_cfu pool={pool_id}")
            return None
