"""Run a bounded Flink SQL *snapshot query* against ``fct_c360_profile``.

A snapshot query is a batch-style ``SELECT`` submitted to Confluent Cloud's
Flink SQL Statements REST API: create the statement, poll until it has results,
page through them, delete the statement. The materialized table is small (one
row per customer), so the whole snapshot is pulled and aggregated in-process.

Like ``data_products.confluent_client``, this is **best-effort**: missing
credentials, a network error, a non-2xx response, or an unparseable payload all
resolve to ``[]`` (the dashboard then shows an empty state) — it never raises.

NOTE (verify before real live use): the Statements API request/response shapes
below (``spec.statement`` / ``spec.properties`` with ``sql.current-catalog`` /
``sql.current-database`` / ``compute_pool_id``; the ``status.result_schema``
column list; the ``results.data`` row arrays and ``metadata.next`` paging) are
aligned with Confluent's Flink SQL REST API docs. Confirm against the current
docs if live mode comes back empty. Catalog/database are the Confluent Cloud
environment and Kafka cluster *display names* (``FLINK_CATALOG`` /
``FLINK_DATABASE``), not their IDs.
"""

from __future__ import annotations

import logging
import time
import uuid

import httpx

import config

logger = logging.getLogger(__name__)

_REQUEST_TIMEOUT_SECONDS = 15.0
# A snapshot SELECT reaches RUNNING with a schema quickly for a small table; cap
# the wait so a stuck statement can't hang the request.
_POLL_ATTEMPTS = 20
_POLL_INTERVAL_SECONDS = 0.5
# Terminal/usable phases: a bounded query ends COMPLETED; RUNNING with a results
# page is also readable. FAILED/STOPPED are terminal failures.
_READY_PHASES = {"COMPLETED", "RUNNING"}
_FAILED_PHASES = {"FAILED", "FAILING", "STOPPED"}
# Default row cap when a caller doesn't pass one.
_DEFAULT_MAX_ROWS = 50_000
# Results come back paged. A SELECT on a materialized table is an (unbounded)
# changelog, so the result pages don't end with a null `next` — they keep a
# forward token that long-polls for more. We therefore stop on our own terms:
# once we've drained a page after already having rows, hit the row cap, or run
# out of patience. The first pages can be empty *before* any data arrives, so
# empty pages are tolerated up to a separate cap.
_MAX_RESULT_PAGES = 60
_MAX_EMPTY_POLLS = 12
# Safety ceiling on raw changelog rows read before collapsing (protects memory
# if a table's changelog is unexpectedly large).
_MAX_CHANGELOG_ROWS = 200_000

# RowKind op codes in the Statements API results. fct_c360_profile is an UPSERT
# changelog, so a streaming SELECT emits the insert and every later update for a
# key. To recover current state we keep the latest INSERT/UPDATE_AFTER per key,
# ignore UPDATE_BEFORE, and drop the key on DELETE. (Confirmed against live
# results: +I=0, -U=1, +U=2, -D=3.)
_OP_INSERT = 0
_OP_UPDATE_BEFORE = 1
_OP_UPDATE_AFTER = 2
_OP_DELETE = 3

# Settings required for a live snapshot query; _missing_settings() names the
# unset ones so a misconfiguration is actionable in the logs.
_CONFIG_FIELDS = (
    "FLINK_REST_ENDPOINT",
    "FLINK_API_KEY",
    "FLINK_API_SECRET",
    "FLINK_ORG_ID",
    "FLINK_ENV_ID",
    "FLINK_COMPUTE_POOL_ID",
    "FLINK_CATALOG",
    "FLINK_DATABASE",
)

# A 401/403 from the Flink SQL Statements API is an auth/role problem; a 404
# usually means the org/environment path or catalog/database is wrong.
_AUTH_ERROR_HINT = (
    " | 401/403: the FLINK_API_KEY lacks access to this environment/compute pool. "
    "404: check FLINK_ORG_ID / FLINK_ENV_ID in the URL and FLINK_CATALOG / FLINK_DATABASE."
)


def _log_snapshot_error(exc: httpx.HTTPStatusError, context: str) -> None:
    """Surface a Flink Statements API failure with its status + response body
    (the body names the real reason: a role problem, a bad path, an unknown
    catalog/database, a SQL error) instead of a bare traceback."""
    status = exc.response.status_code
    body = exc.response.text[:800]
    hint = _AUTH_ERROR_HINT if status in (401, 403, 404) else ""
    logger.warning("FlinkSnapshotClient: Flink API %s for %s: %s%s", status, context, body, hint)

# Columns pulled from the snapshot — only what the aggregator reads (plus the
# changelog key), so the result set stays narrow even though the table is wider.
_SNAPSHOT_COLUMNS = (
    "customer_id",
    "customer_segment",
    "generation_segment",
    "gender",
    "country",
    "age_years",
    "missing_email_flag",
    "missing_phone_flag",
    "account_count",
    "total_balance",
    "txn_count",
    "txn_total",
)


def _snapshot_sql(table: str, limit: int) -> str:
    cols = ", ".join(_SNAPSHOT_COLUMNS)
    return f"SELECT {cols} FROM {table} LIMIT {limit}"


def collapse_changelog(items: list[tuple[int, dict]], key_field: str) -> list[dict]:
    """Collapse ordered upsert-changelog ``(op, row)`` items to current state.

    Keeps the latest INSERT/UPDATE_AFTER row per ``key_field`` (so the current
    value wins), ignores UPDATE_BEFORE retractions, and removes a key on DELETE.
    Relies on changelog rows arriving in offset order (they do, read from
    earliest). Rows without the key are skipped.
    """
    state: dict[object, dict] = {}
    for op, row in items:
        key = row.get(key_field)
        if key is None:
            continue
        if op == _OP_DELETE:
            state.pop(key, None)
        elif op in (_OP_INSERT, _OP_UPDATE_AFTER):
            state[key] = row
        # _OP_UPDATE_BEFORE carries the pre-update value — ignore it.
    return list(state.values())


class FlinkSnapshotClient:
    """Thin, failure-tolerant client for one Flink SQL snapshot query."""

    def __init__(self, settings_obj: config.Settings | None = None) -> None:
        self._settings = settings_obj if settings_obj is not None else config.settings

    def _missing_settings(self) -> list[str]:
        """Names of the required Flink settings that are unset/empty."""
        return [f for f in _CONFIG_FIELDS if not getattr(self._settings, f, None)]

    def _configured(self) -> bool:
        return not self._missing_settings()

    def _base_url(self) -> str:
        s = self._settings
        return (
            f"{s.FLINK_REST_ENDPOINT.rstrip('/')}/sql/v1/organizations/"
            f"{s.FLINK_ORG_ID}/environments/{s.FLINK_ENV_ID}/statements"
        )

    def snapshot(self, table: str, limit: int) -> list[dict]:
        """Run the full-table current-state snapshot for the dashboard aggregates."""
        return self.run(_snapshot_sql(table, limit), max_rows=limit, changelog_key="customer_id")

    def run(
        self, sql: str, max_rows: int = _DEFAULT_MAX_ROWS, changelog_key: str | None = None
    ) -> list[dict]:
        """Run one Flink SQL snapshot statement, returning ``[{column: value}]``.

        The whole create → poll → page → delete lifecycle for any read-only
        snapshot query (full-table or parameterized ``WHERE`` lookups).

        ``changelog_key`` recovers *current state* from the UPSERT changelog:
        when set, the collected ``(op, row)`` changelog is collapsed to the
        latest row per that key (see ``collapse_changelog``) — essential for an
        upsert table, where a plain read otherwise yields the first (stale) row
        and repeats every historical version. When ``None``, UPDATE_BEFORE rows
        are dropped but no per-key collapse is done. ``max_rows`` caps the rows
        returned. Returns ``[]`` when credentials are incomplete or on any
        failure — never raises.
        """
        missing = self._missing_settings()
        if missing:
            logger.warning(
                "FlinkSnapshotClient: skipping snapshot query — these Flink settings are "
                "unset: %s. (The analytics dashboard needs ANALYTICS_DATA_MODE=live AND all "
                "FLINK_* settings incl. FLINK_CATALOG/FLINK_DATABASE; GRAPH_DATA_MODE drives "
                "the Data Products page, not this one.)",
                ", ".join(missing),
            )
            return []

        s = self._settings
        auth = (s.FLINK_API_KEY, s.FLINK_API_SECRET)
        name = f"c360-analytics-snapshot-{uuid.uuid4().hex[:12]}"
        statement_url = f"{self._base_url()}/{name}"
        context = f"catalog={s.FLINK_CATALOG} database={s.FLINK_DATABASE} pool={s.FLINK_COMPUTE_POOL_ID}"
        logger.info("FlinkSnapshotClient: running snapshot query (%s): %s", context, sql)

        try:
            with httpx.Client(auth=auth, timeout=_REQUEST_TIMEOUT_SECONDS) as http:
                self._create(http, name, sql)
                try:
                    columns = self._await_results(http, statement_url)
                    items = self._collect_changelog(http, statement_url, columns)
                finally:
                    self._delete(http, statement_url)
        except httpx.HTTPStatusError as exc:
            _log_snapshot_error(exc, context)
            return []
        except (httpx.HTTPError, ValueError, KeyError) as exc:
            logger.warning("FlinkSnapshotClient: snapshot query failed (%s): %s", context, exc, exc_info=True)
            return []

        if changelog_key:
            rows = collapse_changelog(items, changelog_key)
        else:
            rows = [row for op, row in items if op != _OP_UPDATE_BEFORE]
        logger.info(
            "FlinkSnapshotClient: snapshot query returned %d changelog rows → %d after collapse",
            len(items), len(rows),
        )
        return rows[:max_rows]

    def _create(self, http: httpx.Client, name: str, sql: str) -> None:
        s = self._settings
        body = {
            "name": name,
            "spec": {
                "statement": sql,
                "compute_pool_id": s.FLINK_COMPUTE_POOL_ID,
                "properties": {
                    "sql.current-catalog": s.FLINK_CATALOG,
                    "sql.current-database": s.FLINK_DATABASE,
                },
            },
        }
        http.post(self._base_url(), json=body).raise_for_status()

    def _await_results(self, http: httpx.Client, statement_url: str) -> list[str]:
        """Poll the statement until it has results; return its column names."""
        for _ in range(_POLL_ATTEMPTS):
            resp = http.get(statement_url)
            resp.raise_for_status()
            payload = resp.json()
            status = payload.get("status") or {}
            phase = status.get("phase")
            if phase in _FAILED_PHASES:
                detail = status.get("detail") or phase
                raise ValueError(f"snapshot statement {phase}: {detail}")
            if phase in _READY_PHASES:
                columns = self._column_names(status)
                if columns:
                    return columns
            time.sleep(_POLL_INTERVAL_SECONDS)
        raise ValueError("snapshot statement did not produce results in time")

    @staticmethod
    def _column_names(status: dict) -> list[str]:
        """Result column names from ``status.traits.schema`` (Confluent's Flink
        Statements API puts the SELECT schema there; ``result_schema`` stays
        null). Falls back to ``result_schema`` for forward compatibility."""
        schema = (status.get("traits") or {}).get("schema") or status.get("result_schema") or {}
        return [c.get("name") for c in schema.get("columns", []) if c.get("name")]

    def _collect_changelog(
        self, http: httpx.Client, statement_url: str, columns: list[str]
    ) -> list[tuple[int, dict]]:
        """Read the paged result stream into ordered ``(op, row)`` changelog items.

        Reads until the stream drains (an empty page after data), a safety row
        ceiling, or the page/empty-poll caps — the full changelog must be read
        so the latest value per key survives the collapse, so this does NOT stop
        at a row cap mid-stream.
        """
        items: list[tuple[int, dict]] = []
        url: str | None = f"{statement_url}/results"
        pages = 0
        empty_polls = 0
        while url and pages < _MAX_RESULT_PAGES:
            pages += 1
            resp = http.get(url)
            resp.raise_for_status()
            payload = resp.json()
            data = (payload.get("results") or {}).get("data") or []
            new = 0
            for item in data:
                if isinstance(item, dict):
                    op, values = item.get("op", _OP_INSERT), item.get("row")
                else:
                    op, values = _OP_INSERT, item
                if isinstance(values, list):
                    items.append((op, dict(zip(columns, values))))
                    new += 1
            if len(items) >= _MAX_CHANGELOG_ROWS:
                break
            if new == 0:
                # A drained page *after* we already have rows ends the read;
                # empty pages *before* any data are tolerated up to a cap (the
                # changelog stream keeps a forward `next` token either way).
                if items:
                    break
                empty_polls += 1
                if empty_polls >= _MAX_EMPTY_POLLS:
                    break
            url = (payload.get("metadata") or {}).get("next") or None
        return items

    @staticmethod
    def _delete(http: httpx.Client, statement_url: str) -> None:
        """Best-effort cleanup of the one-shot snapshot statement.

        Runs in a ``finally`` so a failed/aborted query still gets torn down;
        a failure to delete must not mask the query result, so it's swallowed.
        """
        try:
            http.delete(statement_url)
        except httpx.HTTPError:
            logger.warning("FlinkSnapshotClient: failed to delete snapshot statement", exc_info=True)
