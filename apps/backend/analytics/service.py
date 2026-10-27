"""Orchestration for the C360 analytics dashboard.

Thin layer between the API router and the ``providers``/``aggregate`` modules:
the aggregate dashboard folds a full snapshot; the customer lookup resolves a
by-name search then fetches one profile via a ``WHERE customer_id = …`` query.
"""

from __future__ import annotations

from analytics import providers
from analytics.aggregate import aggregate
from analytics.models import (
    C360Analytics,
    CustomerMatch,
    CustomerProfile,
    CustomerSearchResponse,
)


def _to_int(value: object) -> int | None:
    if value is None or value == "":
        return None
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return None


def _to_float(value: object) -> float:
    if value is None or value == "":
        return 0.0
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def get_c360_analytics() -> C360Analytics:
    rows, source_mode = providers.get_rows()
    return aggregate(rows, source_mode=source_mode)


def _to_search_response(rows: list[dict], source_mode: str, query: str) -> CustomerSearchResponse:
    matches = [
        CustomerMatch(
            customer_id=str(r.get("customer_id")),
            first_name=r.get("first_name"),
            last_name=r.get("last_name"),
            customer_segment=r.get("customer_segment"),
            city=r.get("city"),
            state=r.get("state"),
        )
        for r in rows
        if r.get("customer_id")
    ]
    return CustomerSearchResponse(source_mode=source_mode, query=query.strip(), matches=matches)


def search_customers(name: str) -> CustomerSearchResponse:
    rows, source_mode = providers.search_customers(name)
    return _to_search_response(rows, source_mode, name)


def search_customers_rtce(name: str) -> CustomerSearchResponse:
    rows, source_mode = providers.search_customers_rtce(name)
    return _to_search_response(rows, source_mode, name)


def get_customer_profile(customer_id: str) -> CustomerProfile | None:
    row, source_mode = providers.get_customer_profile(customer_id)
    return _to_profile(row, source_mode)


def get_customer_profile_rtce(customer_id: str) -> CustomerProfile | None:
    row, source_mode = providers.get_customer_profile_rtce(customer_id)
    return _to_profile(row, source_mode)


def _to_profile(row: dict | None, source_mode: str) -> CustomerProfile | None:
    if row is None:
        return None
    return CustomerProfile(
        source_mode=source_mode,
        customer_id=str(row.get("customer_id")),
        first_name=row.get("first_name"),
        last_name=row.get("last_name"),
        customer_segment=row.get("customer_segment"),
        generation_segment=row.get("generation_segment"),
        gender=row.get("gender"),
        age_years=_to_int(row.get("age_years")),
        city=row.get("city"),
        state=row.get("state"),
        country=row.get("country"),
        customer_since=(str(row["customer_since"]) if row.get("customer_since") else None),
        days_since_registration=_to_int(row.get("days_since_registration")),
        missing_email_flag=_to_int(row.get("missing_email_flag")),
        missing_phone_flag=_to_int(row.get("missing_phone_flag")),
        account_count=_to_int(row.get("account_count")) or 0,
        total_balance=_to_float(row.get("total_balance")),
        txn_count=_to_int(row.get("txn_count")) or 0,
        txn_total=_to_float(row.get("txn_total")),
    )
