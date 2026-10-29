"""FastAPI router for the /analytics endpoints (C360 dashboard)."""

from __future__ import annotations

from fastapi import APIRouter, HTTPException, Query

from analytics import service
from analytics.models import DataAnalytics, CustomerProfile, CustomerSearchResponse

router = APIRouter(prefix="/analytics", tags=["analytics"])


@router.get("/c360-profile", response_model=DataAnalytics, status_code=200)
def get_c360_profile() -> DataAnalytics:
    """Cross-customer analytics for the C360 dashboard.

    Mock mode folds the fixture; live mode reads the five ``fct_crm_*`` data
    products with concurrent snapshot queries and assembles them. Always 200
    with a well-formed payload: when a live query is unconfigured or fails, the
    payload is simply empty (``row_count == 0``).
    """
    return service.get_data_analytics()


@router.get("/c360-profile/search", response_model=CustomerSearchResponse, status_code=200)
def search_customers(name: str = Query("", description="Name substring to search for")) -> CustomerSearchResponse:
    """Search ``fct_c360_profile`` by customer name (a dynamic ``LIKE`` query).

    Always 200; a blank or unmatched name yields an empty ``matches`` list.
    """
    return service.search_customers(name)


@router.get("/c360-profile/customer/{customer_id}", response_model=CustomerProfile, status_code=200)
def get_customer_profile(customer_id: str) -> CustomerProfile:
    """One customer's metrics via a ``WHERE customer_id = …`` snapshot query."""
    profile = service.get_customer_profile(customer_id)
    if profile is None:
        raise HTTPException(status_code=404, detail="Customer not found")
    return profile


# --- RTCE / Lightning Query API variants (current-state point lookups) ----


@router.get("/c360-profile/rtce/search", response_model=CustomerSearchResponse, status_code=200)
def search_customers_rtce(name: str = Query("", description="Name substring to search for")) -> CustomerSearchResponse:
    """By-name customer search served by the RTCE Lightning Query API."""
    return service.search_customers_rtce(name)


@router.get("/c360-profile/rtce/customer/{customer_id}", response_model=CustomerProfile, status_code=200)
def get_customer_profile_rtce(customer_id: str) -> CustomerProfile:
    """One customer's current-state metrics via an RTCE Lightning point lookup."""
    profile = service.get_customer_profile_rtce(customer_id)
    if profile is None:
        raise HTTPException(status_code=404, detail="Customer not found")
    return profile
