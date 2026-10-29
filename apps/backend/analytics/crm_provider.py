"""Fetch the five CRM analytics data products as snapshot result sets.

Each ``fct_crm_*`` table is a small, pre-aggregated materialized table; the live
dashboard reads them with five trivial ``SELECT *`` snapshot queries run
**concurrently** (one ``confluent-sql`` connection per worker — DB-API
connections are not guaranteed thread-safe). Best-effort: a query that fails or
raises yields ``[]``/``None`` for its slot, so a partial result still renders.
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor

from analytics.confluent_sql_client import ConfluentSqlClient

logger = logging.getLogger(__name__)

SUMMARY_TABLE = "fct_crm_summary"
BY_SEGMENT_TABLE = "fct_crm_by_segment"
BY_GENERATION_TABLE = "fct_crm_by_generation"
BY_GENDER_TABLE = "fct_crm_by_gender"
BY_COUNTRY_TABLE = "fct_crm_by_country"

_TABLES = (
    SUMMARY_TABLE,
    BY_SEGMENT_TABLE,
    BY_GENERATION_TABLE,
    BY_GENDER_TABLE,
    BY_COUNTRY_TABLE,
)

_ResultSets = tuple[dict | None, list[dict], list[dict], list[dict], list[dict]]


def _query_one(client_factory: Callable[[], ConfluentSqlClient], table: str) -> list[dict]:
    """Run one table's snapshot query in its own client; isolate any failure to []."""
    try:
        return client_factory().query(f"SELECT * FROM {table}")
    except Exception as exc:  # noqa: BLE001 — one worker's failure must not sink the rest
        logger.warning("crm_provider: query for %s failed: %s", table, exc, exc_info=True)
        return []


def fetch_crm_result_sets(
    client_factory: Callable[[], ConfluentSqlClient] = ConfluentSqlClient,
) -> _ResultSets:
    """Return ``(summary_row | None, by_segment, by_generation, by_gender, by_country)``.

    Runs the five snapshot queries concurrently. ``summary_row`` is the single
    ``fct_crm_summary`` row (or ``None`` when that query returned nothing).
    """
    with ThreadPoolExecutor(max_workers=len(_TABLES)) as executor:
        results = dict(
            zip(_TABLES, executor.map(lambda t: _query_one(client_factory, t), _TABLES))
        )

    summary_rows = results[SUMMARY_TABLE]
    summary_row = summary_rows[0] if summary_rows else None
    return (
        summary_row,
        results[BY_SEGMENT_TABLE],
        results[BY_GENERATION_TABLE],
        results[BY_GENDER_TABLE],
        results[BY_COUNTRY_TABLE],
    )
