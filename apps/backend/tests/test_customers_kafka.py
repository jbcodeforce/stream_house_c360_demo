"""Integration tests for the Customers REST API — SINK=kafka (no infra required).

The Kafka producer is mocked at the system boundary (confluent produce call).
All reads and writes go through the in-memory CSV inventory, so these tests
run anywhere without a running database or Kafka cluster.

Seam: HTTP API at /api/v1/customers
"""

from __future__ import annotations

import importlib
import sys
from unittest.mock import patch

import pytest
from fastapi.testclient import TestClient

from tests.conftest import make_create_payload, make_full_payload

# ---------------------------------------------------------------------------
# App fixture — force SINK=kafka before importing main so Settings picks it up
# ---------------------------------------------------------------------------

@pytest.fixture(scope="module")
def client():
    """TestClient wired to SINK=kafka with Kafka produces mocked out."""
    # Patch the environment before importing main/settings
    env_patch = patch.dict("os.environ", {
        "SINK": "kafka",
        "KAFKA_BOOTSTRAP_SERVERS": "mock:9092",
        "SCHEMA_REGISTRY_URL": "http://mock-sr",
        "SCHEMA_REGISTRY_API_KEY": "key",
        "SCHEMA_REGISTRY_API_SECRET": "secret",
    })
    env_patch.start()

    # Force re-import of config so Settings re-reads the patched env
    for mod in list(sys.modules.keys()):
        if mod in ("config", "customers.service", "customers.kafka_producer",
                   "customers.inventory", "main"):
            del sys.modules[mod]

    # Mock kafka_producer at the boundary — we don't want real Confluent calls
    with patch("customers.kafka_producer.produce_create"), \
         patch("customers.kafka_producer.produce_update"):

        import main  # noqa: PLC0415 — intentional late import after env patch
        with TestClient(main.app) as test_client:
            yield test_client

    env_patch.stop()


# ---------------------------------------------------------------------------
# Health
# ---------------------------------------------------------------------------

class TestHealth:
    def test_health_returns_ok(self, client):
        resp = client.get("/health")
        assert resp.status_code == 200
        assert resp.json()["status"] == "ok"

    def test_health_reports_kafka_sink(self, client):
        resp = client.get("/health")
        assert resp.json()["sink"] == "kafka"


# ---------------------------------------------------------------------------
# GET /api/v1/customers  (list)
# ---------------------------------------------------------------------------

class TestListCustomers:
    def test_returns_200(self, client):
        resp = client.get("/api/v1/customers")
        assert resp.status_code == 200

    def test_returns_seed_customers(self, client):
        """The 10 CSV seed rows are loaded on startup."""
        resp = client.get("/api/v1/customers")
        assert len(resp.json()) >= 10

    def test_each_customer_has_required_fields(self, client):
        customers = client.get("/api/v1/customers").json()
        for c in customers:
            assert "customer_id" in c
            assert "first_name" in c
            assert "last_name" in c
            assert "email" in c


# ---------------------------------------------------------------------------
# POST /api/v1/customers  (create)
# ---------------------------------------------------------------------------

class TestCreateCustomer:
    def test_returns_201(self, client):
        resp = client.post("/api/v1/customers", json=make_create_payload())
        assert resp.status_code == 201

    def test_response_contains_generated_id(self, client):
        resp = client.post("/api/v1/customers", json=make_create_payload())
        body = resp.json()
        assert "customer_id" in body
        assert body["customer_id"] is not None

    def test_response_echoes_submitted_fields(self, client):
        payload = make_create_payload(
            first_name="Alice",
            last_name="Smith",
            segment="SMB",
        )
        body = client.post("/api/v1/customers", json=payload).json()
        assert body["first_name"] == "Alice"
        assert body["last_name"] == "Smith"
        assert body["segment"] == "SMB"

    def test_created_customer_is_retrievable(self, client):
        payload = make_create_payload(first_name="Bob", last_name="Jones")
        created = client.post("/api/v1/customers", json=payload).json()

        retrieved = client.get(f"/api/v1/customers/{created['customer_id']}").json()
        assert retrieved["customer_id"] == created["customer_id"]
        assert retrieved["first_name"] == "Bob"

    def test_created_customer_appears_in_list(self, client):
        payload = make_create_payload()
        created = client.post("/api/v1/customers", json=payload).json()

        all_ids = [c["customer_id"] for c in client.get("/api/v1/customers").json()]
        assert created["customer_id"] in all_ids

    def test_missing_required_field_returns_422(self, client):
        # email is required
        resp = client.post("/api/v1/customers", json={"first_name": "No", "last_name": "Email"})
        assert resp.status_code == 422

    def test_full_payload_all_fields_persisted(self, client):
        payload = make_full_payload(first_name="Carol", last_name="White")
        body = client.post("/api/v1/customers", json=payload).json()
        assert body["phone"] == "555-000-1234"
        assert body["city"] == "Testville"
        assert body["segment"] == "RETAIL"


# ---------------------------------------------------------------------------
# GET /api/v1/customers/{customer_id}  (read one)
# ---------------------------------------------------------------------------

class TestGetCustomer:
    def test_returns_200_for_seed_customer(self, client):
        seed_id = client.get("/api/v1/customers").json()[0]["customer_id"]
        resp = client.get(f"/api/v1/customers/{seed_id}")
        assert resp.status_code == 200

    def test_returns_correct_customer(self, client):
        all_customers = client.get("/api/v1/customers").json()
        target = all_customers[0]
        resp = client.get(f"/api/v1/customers/{target['customer_id']}")
        assert resp.json()["email"] == target["email"]

    def test_returns_404_for_unknown_id(self, client):
        resp = client.get("/api/v1/customers/00000000-0000-0000-0000-000000000000")
        assert resp.status_code == 404

    def test_invalid_uuid_returns_422(self, client):
        resp = client.get("/api/v1/customers/not-a-uuid")
        assert resp.status_code == 422


# ---------------------------------------------------------------------------
# PUT /api/v1/customers/{customer_id}  (update)
# ---------------------------------------------------------------------------

class TestUpdateCustomer:
    def _create(self, client) -> dict:
        return client.post("/api/v1/customers", json=make_create_payload(
            first_name="Update",
            last_name="Me",
        )).json()

    def test_returns_200(self, client):
        customer = self._create(client)
        resp = client.put(
            f"/api/v1/customers/{customer['customer_id']}",
            json={"first_name": "Updated"},
        )
        assert resp.status_code == 200

    def test_updated_field_is_changed(self, client):
        customer = self._create(client)
        resp = client.put(
            f"/api/v1/customers/{customer['customer_id']}",
            json={"first_name": "Changed"},
        )
        assert resp.json()["first_name"] == "Changed"

    def test_non_updated_fields_are_preserved(self, client):
        customer = self._create(client)
        resp = client.put(
            f"/api/v1/customers/{customer['customer_id']}",
            json={"city": "NewCity"},
        )
        body = resp.json()
        # last_name was not updated — must still be the original value
        assert body["last_name"] == "Me"
        assert body["city"] == "NewCity"

    def test_update_is_visible_on_subsequent_get(self, client):
        customer = self._create(client)
        client.put(
            f"/api/v1/customers/{customer['customer_id']}",
            json={"segment": "ENTERPRISE"},
        )
        retrieved = client.get(f"/api/v1/customers/{customer['customer_id']}").json()
        assert retrieved["segment"] == "ENTERPRISE"

    def test_returns_404_for_unknown_id(self, client):
        resp = client.put(
            "/api/v1/customers/00000000-0000-0000-0000-000000000000",
            json={"first_name": "Ghost"},
        )
        assert resp.status_code == 404
