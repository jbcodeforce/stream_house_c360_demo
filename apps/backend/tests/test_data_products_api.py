"""/api/v1/data-products endpoints: list, graph, status, node metrics.

Wired to SINK=kafka + GRAPH_DATA_MODE=mock (no infra) exactly like
test_cors.py, with the Kafka producers mocked at the boundary. The fixture
fully resets the imported app modules in sys.modules on teardown so suites
that reimport ``main`` afterwards start from a clean module graph.
"""

from __future__ import annotations

import sys
from unittest.mock import patch

import pytest
from fastapi.testclient import TestClient


def _reset_modules() -> None:
    """Drop the app's modules AND their packages from sys.modules.

    Popping a submodule alone leaves a stale attribute on the cached package
    object, so a later ``from package import submodule`` returns the old
    module instead of reimporting. Clearing the packages too forces a clean
    reimport for suites that run after this one.
    """
    for mod in list(sys.modules):
        if mod in ("config", "main") or mod.startswith(("customers", "api", "data_products")):
            del sys.modules[mod]


@pytest.fixture(scope="module")
def client():
    env_patch = patch.dict("os.environ", {
        "SINK": "kafka",
        "GRAPH_DATA_MODE": "mock",
        "KAFKA_BOOTSTRAP_SERVERS": "mock:9092",
        "SCHEMA_REGISTRY_URL": "http://mock-sr",
        "SCHEMA_REGISTRY_API_KEY": "key",
        "SCHEMA_REGISTRY_API_SECRET": "secret",
    })
    env_patch.start()
    _reset_modules()

    with patch("customers.kafka_producer.produce_create"), \
         patch("customers.kafka_producer.produce_update"):
        import main  # noqa: PLC0415 — intentional late import after env patch
        with TestClient(main.app) as test_client:
            yield test_client

    env_patch.stop()
    _reset_modules()


def test_list_products(client):
    r = client.get("/api/v1/data-products")
    assert r.status_code == 200 and r.json()[0]["id"] == "c360"


def test_graph_endpoint_shape(client):
    r = client.get("/api/v1/data-products/c360/graph")
    body = r.json()
    assert r.status_code == 200 and len(body["nodes"]) == 11 and len(body["edges"]) == 11


def test_status_rollup(client):
    r = client.get("/api/v1/data-products/c360/status")
    assert r.status_code == 200 and r.json()["rollup"] == {"running": 8, "total": 8, "lagging": 0}


def test_node_metrics(client):
    r = client.get("/api/v1/data-products/c360/nodes/src_customers/metrics")
    assert r.status_code == 200 and r.json()["phase"] == "RUNNING"


def test_unknown_product_graph_404(client):
    assert client.get("/api/v1/data-products/nope/graph").status_code == 404


def test_unknown_product_status_404(client):
    assert client.get("/api/v1/data-products/nope/status").status_code == 404


def test_unknown_node_metrics_404(client):
    assert client.get("/api/v1/data-products/c360/nodes/nope/metrics").status_code == 404


def test_unknown_product_metrics_404(client):
    assert client.get("/api/v1/data-products/nope/nodes/src_customers/metrics").status_code == 404
