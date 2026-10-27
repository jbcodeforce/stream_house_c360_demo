"""Confluent Lightning Query API client — the RTCE serving layer.

The Real-Time Context Engine (RTCE) exposes a Flink-maintained table for
*current-state point lookups* over HTTP: a single synchronous POST returns the
table's current rows in milliseconds. This is the contrast to
``flink_snapshot.FlinkSnapshotClient`` — no statement lifecycle (create → poll →
page → delete) and no upsert changelog to collapse, because Lightning already
serves current state.

Modeled on the reference client in griga23/inventory-demo-streamhouse
(``storefront-app/lightning_client.py``). The request is HTTP Basic auth with
the Confluent Cloud API key/secret (``CONFLUENT_CLOUD_API_KEY``/``SECRET`` — the
Lightning API has no key of its own) and a body scoped by ``catalog_name`` =
environment *id* and ``database_name`` = Kafka cluster *id* (IDs, not the
display names the Flink SQL Statements API uses). Note the key must authenticate
the whole organization, or the API returns 401.

Best-effort like the other analytics clients: missing credentials, a non-2xx
response, or an unparseable body all resolve to ``[]`` — it never raises.
"""

from __future__ import annotations

import logging
from collections.abc import Sequence

import httpx

import config

logger = logging.getLogger(__name__)

_REQUEST_TIMEOUT_SECONDS = 5.0

# A 401/403 from the Lightning Query API almost always means the Confluent Cloud
# API key is not organization-scoped: the Lightning/SQL query endpoint requires a
# Cloud API key that authenticates the whole organization, not a cluster-,
# environment-, Flink-, or Metrics-scoped key.
_AUTH_ERROR_HINT = (
    " | 401/403: the Lightning Query API needs an ORG-SCOPED Confluent Cloud API "
    "key (CONFLUENT_CLOUD_API_KEY/SECRET that authenticates the whole organization)."
)


def _log_query_error(exc: Exception) -> None:
    """Surface the Lightning API failure actionably — the response body carries
    the precise reason (a 401 says whether the key authenticates the org)."""
    if isinstance(exc, httpx.HTTPStatusError):
        status = exc.response.status_code
        body = exc.response.text[:800]
        hint = _AUTH_ERROR_HINT if status in (401, 403) else ""
        logger.warning("LightningClient: Lightning API %s: %s%s", status, body, hint)
    else:
        logger.warning("LightningClient: query failed", exc_info=True)


class LightningClient:
    """Thin, failure-tolerant wrapper around the Lightning Query API."""

    def __init__(self, settings_obj: config.Settings | None = None) -> None:
        self._settings = settings_obj if settings_obj is not None else config.settings

    def _configured(self) -> bool:
        s = self._settings
        return bool(
            s.CLOUD_REGION
            and s.CLOUD_PROVIDER
            and s.FLINK_ENV_ID
            and s.KAFKA_CLUSTER_ID
            and s.CONFLUENT_CLOUD_API_KEY
            and s.CONFLUENT_CLOUD_API_SECRET
        )

    def _endpoint(self) -> str:
        s = self._settings
        return f"https://sql.{s.CLOUD_REGION}.{s.CLOUD_PROVIDER}.confluent.cloud/query/v1alpha1"

    def query(self, sql: str, fallback_columns: Sequence[str], max_rows: int) -> list[dict]:
        """Run one Lightning query, returning current-state rows as ``[{col: value}]``.

        Column names come from the response schema when present, else from
        ``fallback_columns`` (the columns the SELECT requested, in order).
        Returns ``[]`` when credentials are incomplete or on any failure.
        """
        if not self._configured():
            logger.info("LightningClient: settings incomplete; skipping Lightning query")
            return []

        s = self._settings
        body = {
            "catalog_name": s.FLINK_ENV_ID,
            "database_name": s.KAFKA_CLUSTER_ID,
            "query": sql,
            "options": {"max_result_rows": max_rows, "result_format": "JSON"},
            "client_info": {},
        }
        try:
            response = httpx.post(
                self._endpoint(),
                json=body,
                auth=(s.CONFLUENT_CLOUD_API_KEY, s.CONFLUENT_CLOUD_API_SECRET),
                timeout=_REQUEST_TIMEOUT_SECONDS,
            )
            response.raise_for_status()
            payload = response.json()
        except (httpx.HTTPError, ValueError) as exc:
            _log_query_error(exc)
            return []

        return self._rows(payload, fallback_columns)

    @staticmethod
    def _rows(payload: dict, fallback_columns: Sequence[str]) -> list[dict]:
        result = payload.get("result") or {}
        data = result.get("data") or []
        schema_cols = (result.get("schema") or {}).get("columns") or []
        columns = [c if isinstance(c, str) else (c or {}).get("name") for c in schema_cols]
        columns = [c for c in columns if c] or list(fallback_columns)
        return [dict(zip(columns, row)) for row in data if isinstance(row, list)]
