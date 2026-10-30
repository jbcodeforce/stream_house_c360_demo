"""FastAPI router for the /customers endpoints."""

from __future__ import annotations

from uuid import UUID

from fastapi import APIRouter, HTTPException, Query

from customers import service
from customers.models import Customer, CustomerCreate, CustomerUpdate

router = APIRouter(prefix="/customers", tags=["customers"])


@router.post("", response_model=Customer, status_code=201)
def create_customer(data: CustomerCreate) -> Customer:
    return service.create(data)


@router.get("", response_model=list[Customer], status_code=200)
def list_customers(
    name: str | None = Query(None, description="Case-insensitive name substring filter"),
) -> list[Customer]:
    """List customers, or search by name when ``name`` is supplied.

    This is the by-name resolver the C360 dashboard calls first: the operational
    customer store is the system of record for identity, so a name is resolved
    here (returning the matching ``customer_id``s) rather than against the
    derived ``fct_c360_profile`` analytics snapshot. A blank ``name`` yields an
    empty list; omitting ``name`` lists everyone.
    """
    if name is not None:
        return service.get_by_name(name)
    return service.list_all()


@router.get("/{customer_id}", response_model=Customer, status_code=200)
def get_customer(customer_id: UUID) -> Customer:
    customer = service.get_by_id(customer_id)
    if customer is None:
        raise HTTPException(status_code=404, detail="Customer not found")
    return customer

@router.get("/name/{customer_name}", response_model=Customer, status_code=200)
def get_customer_by_name(customer_name: str) -> Customer:
    customer = service.get_by_name(customer_name)
    if customer is None:
        raise HTTPException(status_code=404, detail="Customer not found")
    return customer[0]



@router.put("/{customer_id}", response_model=Customer, status_code=200)
def update_customer(customer_id: UUID, data: CustomerUpdate) -> Customer:
    customer = service.update(customer_id, data)
    if customer is None:
        raise HTTPException(status_code=404, detail="Customer not found")
    return customer


@router.delete("/{customer_id}", status_code=204)
def delete_customer(customer_id: UUID) -> None:
    deleted = service.delete_by_id(customer_id)
    if not deleted:
        raise HTTPException(status_code=404, detail="Customer not found")
