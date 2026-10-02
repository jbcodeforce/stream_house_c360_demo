"""FastAPI router for the /customers endpoints."""

from __future__ import annotations

from uuid import UUID

from fastapi import APIRouter, HTTPException

from customers import service
from customers.models import Customer, CustomerCreate, CustomerUpdate

router = APIRouter(prefix="/customers", tags=["customers"])


@router.post("", response_model=Customer, status_code=201)
def create_customer(data: CustomerCreate) -> Customer:
    return service.create(data)


@router.get("", response_model=list[Customer], status_code=200)
def list_customers() -> list[Customer]:
    return service.list_all()


@router.get("/{customer_id}", response_model=Customer, status_code=200)
def get_customer(customer_id: UUID) -> Customer:
    customer = service.get_by_id(customer_id)
    if customer is None:
        raise HTTPException(status_code=404, detail="Customer not found")
    return customer


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
