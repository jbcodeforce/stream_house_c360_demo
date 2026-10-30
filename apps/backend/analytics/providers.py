"""Snapshot-row providers for the C360 analytics dashboard.

``mock`` returns the deterministic fixture (default — the demo runs with no
infra); ``live`` runs a Flink SQL snapshot query against ``fct_c360_profile``
on Confluent Cloud. ``get_rows()`` selects on ``ANALYTICS_DATA_MODE``, read
fresh on every call so a reloaded ``config`` (as in tests) takes effect
immediately — mirrors ``data_products.providers.get_provider``.
"""

from __future__ import annotations

import logging
import re

from analytics import mock_data
from analytics.flink_snapshot import FlinkSnapshotClient
from analytics.lightning_client import LightningClient

logger = logging.getLogger(__name__)

# The materialized table the snapshot query reads.
PROFILE_TABLE = "fct_c360_profile"

# Columns for the by-id profile lookup.
_PROFILE_COLUMNS = (
    "customer_id", "first_name", "last_name", "customer_segment", "generation_segment",
    "gender", "age_years", "city", "state", "country", "customer_since",
    "days_since_registration", "missing_email_flag", "missing_phone_flag",
    "account_count", "total_balance", "txn_count", "txn_total",
)
# customer_id is an opaque id (UUID-ish); only these characters are ever valid,
# so anything else is rejected before it reaches a SQL literal.
_CUSTOMER_ID_RE = re.compile(r"^[A-Za-z0-9._:\-]{1,128}$")


def _sql_str(value: str) -> str:
    """Render a Python string as a single-quoted SQL literal, escaping quotes.

    Doubling embedded single quotes is the standard SQL escape and keeps
    user-supplied search text from breaking out of the literal. LIKE wildcards
    (``%`` / ``_``) in the name are treated literally enough for this demo
    search; a stricter build would ESCAPE them too.
    """
    return "'" + value.replace("'", "''") + "'"


def get_rows() -> tuple[list[dict], str]:
    """Return ``(rows, source_mode)`` for the current snapshot.

    ``source_mode`` is the resolved mode string ("mock" or "live") so the
    response can label where the data came from.
    """
    import config  # noqa: PLC0415 — late import so a reloaded config is picked up

    mode = config.settings.ANALYTICS_DATA_MODE
    if mode == "mock":
        return list(mock_data.PROFILE_SNAPSHOT), "mock"
    if mode == "live":
        rows = FlinkSnapshotClient().snapshot(PROFILE_TABLE, config.settings.ANALYTICS_SNAPSHOT_LIMIT)
        return rows, "live"
    raise ValueError(f"Unknown ANALYTICS_DATA_MODE: {mode!r}")


def _mode() -> str:
    import config  # noqa: PLC0415 — late import so a reloaded config is picked up

    mode = config.settings.ANALYTICS_DATA_MODE
    if mode not in ("mock", "live"):
        raise ValueError(f"Unknown ANALYTICS_DATA_MODE: {mode!r}")
    return mode


def _profile_sql(customer_id: str) -> str:
    # No LIMIT: fct_c360_profile is an upsert changelog, so the whole changelog
    # for this one key must be read and collapsed to its latest row. A LIMIT
    # would truncate to the first (stale) version.
    cols = ", ".join(_PROFILE_COLUMNS)
    return f"SELECT {cols} FROM {PROFILE_TABLE} WHERE customer_id = {_sql_str(customer_id)}"


def get_customer_profile(customer_id: str) -> tuple[dict | None, str]:
    """Fetch one customer's profile row via a ``WHERE customer_id = …`` query.

    Returns ``(None, mode)`` when the id is malformed or no row matches.
    """
    mode = _mode()
    cid = (customer_id or "").strip()
    if not _CUSTOMER_ID_RE.match(cid):
        return None, mode
    if mode == "mock":
        return _mock_profile(cid), mode
    rows = FlinkSnapshotClient().run(_profile_sql(cid), max_rows=1, changelog_key="customer_id")
    return (rows[0] if rows else None), mode


# --- RTCE / Lightning Query API variant ----------------------------------
#
# Same by-id profile, but served by the Lightning point-lookup API (current
# state directly — no changelog collapse). Mock mode reuses the same fixture,
# so both lookup sections behave identically with no infra.


def get_customer_profile_rtce(customer_id: str) -> tuple[dict | None, str]:
    """One customer's current-state profile via a Lightning point lookup."""
    mode = _mode()
    cid = (customer_id or "").strip()
    if not _CUSTOMER_ID_RE.match(cid):
        return None, mode
    if mode == "mock":
        return _mock_profile(cid), mode
    rows = LightningClient().query(_profile_sql(cid), _PROFILE_COLUMNS, max_rows=1)
    return (rows[0] if rows else None), mode


def _mock_profile(cid: str) -> dict | None:
    return next((r for r in mock_data.PROFILE_SNAPSHOT if r.get("customer_id") == cid), None)
