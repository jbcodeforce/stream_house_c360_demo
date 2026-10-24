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

NOTE (verify before real live use): the exact Flink SQL Statements API
path/version
(``{FLINK_REST_ENDPOINT}/sql/v1/organizations/{org}/environments/{env}/statements``)
and the Confluent Cloud Metrics API descriptor names used below (e.g.
``io.confluent.flink/pending_records``) are illustrative based on the
general shape of these APIs — confirm the current path and descriptor names
against Confluent's docs before pointing this at a real environment. None of
this is test-critical: tests in ``tests/test_data_products_live.py`` mock at
the client boundary (inject a fake object in place of ``ConfluentClient``),
not at the HTTP layer.
"""

from __future__ import annotations

import logging
from typing import Protocol

import httpx

import config

logger = logging.getLogger(__name__)

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

    def get_statement_metrics(self, statement_name: str) -> dict:
        """Return raw metric fields for one statement; any key may be absent.

        Recognized keys: ``pending_records``, ``messages_in_per_sec``,
        ``messages_out_per_sec``, ``uptime_seconds``, ``compute_pool_cfu``.
        Rate fields (``messages_in_per_sec``/``messages_out_per_sec``) need a
        delta between two reads to compute — the Metrics API exposes
        cumulative counters, not rates — so a single read here omits them
        rather than guessing; a future iteration could cache the previous
        read (per statement) to derive a rate.

        Returns ``{}`` if live credentials are not configured, or on any
        network/HTTP/parsing failure.
        """
        s = self._settings
        if not (s.CONFLUENT_CLOUD_API_KEY and s.CONFLUENT_CLOUD_API_SECRET and s.FLINK_COMPUTE_POOL_ID):
            logger.info("ConfluentClient: Metrics API settings incomplete; skipping metrics lookup")
            return {}

        body = {
            "aggregations": [
                {"metric": "io.confluent.flink/pending_records"},
                {"metric": "io.confluent.flink/current_compute_pool_utilization"},
            ],
            "filter": {
                "op": "AND",
                "filters": [
                    {"field": "resource.compute_pool.id", "op": "EQ", "value": s.FLINK_COMPUTE_POOL_ID},
                    {"field": "resource.statement.name", "op": "EQ", "value": statement_name},
                ],
            },
            "granularity": "PT1M",
        }
        try:
            response = httpx.post(
                _METRICS_API_BASE_URL,
                json=body,
                auth=(s.CONFLUENT_CLOUD_API_KEY, s.CONFLUENT_CLOUD_API_SECRET),
                timeout=_REQUEST_TIMEOUT_SECONDS,
            )
            response.raise_for_status()
            payload = response.json()
        except (httpx.HTTPError, ValueError):
            logger.warning(
                "ConfluentClient: get_statement_metrics failed for statement=%s",
                statement_name,
                exc_info=True,
            )
            return {}

        data_points = payload.get("data", []) if isinstance(payload, dict) else []
        if not data_points:
            return {}
        point = data_points[0]

        metrics: dict = {}
        if "pending_records" in point:
            metrics["pending_records"] = point["pending_records"]
        if "current_compute_pool_utilization" in point:
            metrics["compute_pool_cfu"] = point["current_compute_pool_utilization"]
        if "uptime_seconds" in point:
            metrics["uptime_seconds"] = point["uptime_seconds"]
        return metrics
