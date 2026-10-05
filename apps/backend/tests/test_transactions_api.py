"""Transactions API tests against a bare app (no lifespan/DB/Kafka)."""

from __future__ import annotations

from datetime import datetime, timezone
from uuid import uuid4

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from api.transaction_resource import router
from transactions import service
from transactions.models import Transaction


@pytest.fixture
def client():
    app = FastAPI()
    app.include_router(router, prefix="/api/v1")
    return TestClient(app)


def _txn(**over) -> Transaction:
    now = datetime.now(tz=timezone.utc)
    base = dict(
        transaction_id=uuid4(), account_id=uuid4(), customer_id=uuid4(),
        transaction_type="CREDIT", amount=10.0, currency="USD", status="COMPLETED",
        transacted_at=now, posted_at=None, created_at=now,
    )
    base.update(over)
    return Transaction(**base)


def test_list(client, monkeypatch):
    monkeypatch.setattr(service, "list_all", lambda: [_txn()])
    resp = client.get("/api/v1/transactions")
    assert resp.status_code == 200
    assert len(resp.json()) == 1


def test_create_returns_201(client, monkeypatch):
    txn = _txn()
    monkeypatch.setattr(service, "create", lambda data: txn)
    resp = client.post("/api/v1/transactions", json={
        "account_id": str(txn.account_id), "customer_id": str(txn.customer_id),
        "transaction_type": "CREDIT", "amount": 10.0,
    })
    assert resp.status_code == 201
    assert resp.json()["transaction_type"] == "CREDIT"


def test_get_404(client, monkeypatch):
    monkeypatch.setattr(service, "get_by_id", lambda _id: None)
    resp = client.get(f"/api/v1/transactions/{uuid4()}")
    assert resp.status_code == 404
