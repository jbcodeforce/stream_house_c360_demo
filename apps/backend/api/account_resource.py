"""FastAPI router for the /accounts endpoints."""

from __future__ import annotations

from uuid import UUID

from fastapi import APIRouter, HTTPException

from accounts import service
from accounts.models import Account, AccountCreate, AccountUpdate

router = APIRouter(prefix="/accounts", tags=["accounts"])


@router.post("", response_model=Account, status_code=201)
def create_account(data: AccountCreate) -> Account:
    return service.create(data)


@router.get("", response_model=list[Account], status_code=200)
def list_accounts() -> list[Account]:
    return service.list_all()


@router.get("/{account_id}", response_model=Account, status_code=200)
def get_account(account_id: UUID) -> Account:
    account = service.get_by_id(account_id)
    if account is None:
        raise HTTPException(status_code=404, detail="Account not found")
    return account


@router.put("/{account_id}", response_model=Account, status_code=200)
def update_account(account_id: UUID, data: AccountUpdate) -> Account:
    account = service.update(account_id, data)
    if account is None:
        raise HTTPException(status_code=404, detail="Account not found")
    return account


@router.delete("/{account_id}", status_code=204)
def delete_account(account_id: UUID) -> None:
    deleted = service.delete_by_id(account_id)
    if not deleted:
        raise HTTPException(status_code=404, detail="Account not found")
