"""Config API tests against a bare app (no lifespan, no DB, no Kafka).

Mounting just the config router avoids importing main's sink machinery, so
these tests neither need infra nor pollute the test_customers_kafka module
graph. main wiring is verified by the Task 7 smoke test.
"""

from __future__ import annotations

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

import config_store
from api.config_resource import router
from config import settings


@pytest.fixture
def client(tmp_path, monkeypatch):
    monkeypatch.setattr(settings, "RUNTIME_CONFIG_FILE", str(tmp_path / "rc.json"))
    config_store.reset_cache()
    app = FastAPI()
    app.include_router(router, prefix="/api/v1")
    return TestClient(app)


def test_get_returns_default(client):
    resp = client.get("/api/v1/config")
    assert resp.status_code == 200
    assert resp.json() == {"kafka_produce_enabled": False}


def test_put_updates_and_persists(client):
    resp = client.put("/api/v1/config", json={"kafka_produce_enabled": True})
    assert resp.status_code == 200
    assert resp.json() == {"kafka_produce_enabled": True}

    # A subsequent GET reflects the change.
    assert client.get("/api/v1/config").json() == {"kafka_produce_enabled": True}
