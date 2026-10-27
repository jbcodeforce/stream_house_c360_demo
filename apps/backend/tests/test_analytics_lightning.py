"""Unit tests for the RTCE Lightning Query API client."""

from __future__ import annotations

from types import SimpleNamespace

import httpx
import pytest

from analytics import lightning_client
from analytics.lightning_client import LightningClient

_SETTINGS = SimpleNamespace(
    CLOUD_REGION="us-west-2",
    CLOUD_PROVIDER="aws",
    FLINK_ENV_ID="env-abc",
    KAFKA_CLUSTER_ID="lkc-xyz",
    CONFLUENT_GLOBAL_KEY="global-k",
    CONFLUENT_GLOBAL_SECRET="global-s",
)

_SEARCH_COLS = ("customer_id", "first_name", "last_name")


def _install_post(monkeypatch, handler) -> dict:
    """Capture the POST and return a canned response via a stub httpx.post."""
    captured: dict = {}

    def fake_post(url, json=None, auth=None, timeout=None):
        captured["url"] = url
        captured["json"] = json
        captured["auth"] = auth
        resp = handler(json)
        resp.request = httpx.Request("POST", url)  # needed for raise_for_status()
        return resp

    monkeypatch.setattr(lightning_client.httpx, "post", fake_post)
    return captured


def test_endpoint_and_body_shape(monkeypatch):
    captured = _install_post(
        monkeypatch,
        lambda body: httpx.Response(200, json={"result": {"data": []}}),
    )
    LightningClient(_SETTINGS).query("SELECT 1", _SEARCH_COLS, max_rows=50)

    assert captured["url"] == "https://sql.us-west-2.aws.confluent.cloud/query/v1alpha1"
    assert captured["auth"] == ("global-k", "global-s")  # Global resource key/secret
    assert captured["json"]["catalog_name"] == "env-abc"   # env *id*, not display name
    assert captured["json"]["database_name"] == "lkc-xyz"  # cluster *id*
    assert captured["json"]["options"] == {"max_result_rows": 50, "result_format": "JSON"}
    assert captured["json"]["query"] == "SELECT 1"


def test_parses_rows_with_response_schema(monkeypatch):
    _install_post(
        monkeypatch,
        lambda body: httpx.Response(200, json={
            "result": {
                "schema": {"columns": [{"name": "customer_id"}, {"name": "txn_count"}]},
                "data": [["c1", 43], ["c2", 10]],
            }
        }),
    )
    rows = LightningClient(_SETTINGS).query("SELECT ...", ("customer_id", "txn_count"), max_rows=50)
    assert rows == [
        {"customer_id": "c1", "txn_count": 43},
        {"customer_id": "c2", "txn_count": 10},
    ]


def test_falls_back_to_requested_columns_when_schema_absent(monkeypatch):
    _install_post(
        monkeypatch,
        lambda body: httpx.Response(200, json={"result": {"data": [["c1", "Ada", "Lovelace"]]}}),
    )
    rows = LightningClient(_SETTINGS).query("SELECT ...", _SEARCH_COLS, max_rows=1)
    assert rows == [{"customer_id": "c1", "first_name": "Ada", "last_name": "Lovelace"}]


def test_empty_result_returns_empty(monkeypatch):
    _install_post(monkeypatch, lambda body: httpx.Response(200, json={"result": {"data": []}}))
    assert LightningClient(_SETTINGS).query("SELECT ...", _SEARCH_COLS, max_rows=1) == []


def test_unconfigured_returns_empty_without_calling(monkeypatch):
    # No Global key available → unconfigured, no HTTP call.
    settings = SimpleNamespace(**{**_SETTINGS.__dict__, "CONFLUENT_GLOBAL_KEY": None})

    def boom(*a, **k):  # pragma: no cover - must not be called
        raise AssertionError("httpx.post should not be called when unconfigured")

    monkeypatch.setattr(lightning_client.httpx, "post", boom)
    assert LightningClient(settings).query("SELECT ...", _SEARCH_COLS, max_rows=1) == []


def test_http_error_degrades_to_empty(monkeypatch):
    _install_post(monkeypatch, lambda body: httpx.Response(500, text="boom"))
    assert LightningClient(_SETTINGS).query("SELECT ...", _SEARCH_COLS, max_rows=1) == []
