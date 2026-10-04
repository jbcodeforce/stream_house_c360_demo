"""Accounts API tests against a bare app (no lifespan/DB/Kafka)."""

from __future__ import annotations

from datetime import date, datetime, timezone
from uuid import uuid4

import pytest
from fastapi import FastAPI, HTTPException
from fastapi.testclient import TestClient

from accounts import service
from accounts.models import Account
from api.account_resource import router


@pytest.fixture
def client():
    app = FastAPI()
    app.include_router(router, prefix="/api/v1")
    return TestClient(app)


def _account(**over) -> Account:
    now = datetime.now(tz=timezone.utc)
    base = dict(
        account_id=uuid4(), customer_id=uuid4(), account_number="ACC-1",
        account_type="CHECKING", currency="USD", balance=0.0, credit_limit=None,
        opened_date=date(2021, 1, 1), closed_date=None, status="ACTIVE",
        created_at=now, updated_at=now,
    )
    base.update(over)
    return Account(**base)


def test_list(client, monkeypatch):
    monkeypatch.setattr(service, "list_all", lambda: [_account()])
    resp = client.get("/api/v1/accounts")
    assert resp.status_code == 200
    assert len(resp.json()) == 1


def test_create_returns_201(client, monkeypatch):
    acct = _account(account_number="ACC-9")
    monkeypatch.setattr(service, "create", lambda data: acct)
    resp = client.post("/api/v1/accounts", json={
        "customer_id": str(acct.customer_id),
        "account_number": "ACC-9",
        "account_type": "CHECKING",
    })
    assert resp.status_code == 201
    assert resp.json()["account_number"] == "ACC-9"


def test_get_404(client, monkeypatch):
    monkeypatch.setattr(service, "get_by_id", lambda _id: None)
    resp = client.get(f"/api/v1/accounts/{uuid4()}")
    assert resp.status_code == 404


def test_create_duplicate_409(client, monkeypatch):
    def _raise(_data):
        raise HTTPException(status_code=409, detail="dupe")
    monkeypatch.setattr(service, "create", _raise)
    resp = client.post("/api/v1/accounts", json={
        "customer_id": str(uuid4()), "account_number": "ACC-1", "account_type": "CHECKING",
    })
    assert resp.status_code == 409


def test_delete_204(client, monkeypatch):
    monkeypatch.setattr(service, "delete_by_id", lambda _id: True)
    resp = client.delete(f"/api/v1/accounts/{uuid4()}")
    assert resp.status_code == 204
