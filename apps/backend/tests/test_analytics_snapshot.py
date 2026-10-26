"""Unit tests for the Flink SQL snapshot client (parsing + degradation).

Uses httpx.MockTransport to stand in for the Confluent Cloud Flink Statements
API, so the create → poll → results → delete flow and the row/column mapping
are exercised without any network.
"""

from __future__ import annotations

from types import SimpleNamespace

import httpx
import pytest

from analytics import flink_snapshot
from analytics.flink_snapshot import FlinkSnapshotClient, collapse_changelog

_SETTINGS = SimpleNamespace(
    FLINK_REST_ENDPOINT="https://flink.example.com",
    FLINK_API_KEY="k",
    FLINK_API_SECRET="s",
    FLINK_ORG_ID="org",
    FLINK_ENV_ID="env",
    FLINK_COMPUTE_POOL_ID="pool",
    FLINK_CATALOG="my-env",
    FLINK_DATABASE="my-cluster",
)


def _install_transport(monkeypatch, handler) -> None:
    """Make FlinkSnapshotClient's httpx.Client route through a MockTransport."""
    real_client = httpx.Client

    def factory(*args, **kwargs):
        kwargs.pop("auth", None)
        kwargs.pop("timeout", None)
        return real_client(transport=httpx.MockTransport(handler), **kwargs)

    monkeypatch.setattr(flink_snapshot.httpx, "Client", factory)


def test_collapse_changelog_keeps_latest_per_key():
    # +I 31, -U 31, +U 43 for c1; +I 10 for c2. Current state: c1=43, c2=10.
    items = [
        (0, {"customer_id": "c1", "txn_count": 31}),
        (1, {"customer_id": "c1", "txn_count": 31}),
        (2, {"customer_id": "c1", "txn_count": 43}),
        (0, {"customer_id": "c2", "txn_count": 10}),
    ]
    assert collapse_changelog(items, "customer_id") == [
        {"customer_id": "c1", "txn_count": 43},
        {"customer_id": "c2", "txn_count": 10},
    ]


def test_collapse_changelog_drops_deleted_keys():
    items = [
        (0, {"customer_id": "c1", "v": 1}),
        (0, {"customer_id": "c2", "v": 2}),
        (3, {"customer_id": "c1", "v": 1}),  # delete c1
    ]
    assert collapse_changelog(items, "customer_id") == [{"customer_id": "c2", "v": 2}]


def test_collapse_changelog_skips_rows_without_key():
    items = [(0, {"customer_id": None, "v": 1}), (0, {"customer_id": "c2", "v": 2})]
    assert collapse_changelog(items, "customer_id") == [{"customer_id": "c2", "v": 2}]


def _status_ready(columns: list[str]) -> dict:
    # Mirrors the real Confluent shape: the SELECT schema is under
    # status.traits.schema (result_schema stays null), phase RUNNING.
    return {
        "status": {
            "phase": "RUNNING",
            "result_schema": None,
            "traits": {"schema": {"columns": [{"name": c} for c in columns]}},
        },
    }


def test_snapshot_collapses_changelog_to_current_state(monkeypatch):
    """snapshot() reads an UPSERT changelog and must return the LATEST row per
    customer_id, not the first (stale) one or every historical version."""
    columns = ["customer_id", "customer_segment", "txn_count"]

    def handler(request: httpx.Request) -> httpx.Response:
        if request.method in ("POST", "DELETE"):
            return httpx.Response(200, json={})
        if request.url.path.endswith("/results"):
            return httpx.Response(200, json={
                "results": {"data": [
                    {"op": 0, "row": ["c1", "RETAIL", 31]},   # +I  c1 = 31
                    {"op": 1, "row": ["c1", "RETAIL", 31]},   # -U  (ignored)
                    {"op": 2, "row": ["c1", "RETAIL", 43]},   # +U  c1 -> 43 (current)
                    {"op": 0, "row": ["c2", "SMB", 10]},      # +I  c2 = 10
                ]},
                "metadata": {"next": None},
            })
        return httpx.Response(200, json=_status_ready(columns))

    _install_transport(monkeypatch, handler)
    rows = FlinkSnapshotClient(_SETTINGS).snapshot("fct_c360_profile", 1000)
    assert rows == [
        {"customer_id": "c1", "customer_segment": "RETAIL", "txn_count": 43},
        {"customer_id": "c2", "customer_segment": "SMB", "txn_count": 10},
    ]


def test_snapshot_drains_changelog_pages(monkeypatch):
    """An empty first page (data not yet flowing), then a data page, then an
    empty page while `next` keeps pointing forward — the client must collect the
    data and stop on the drained-after-data page rather than loop forever."""
    columns = ["customer_id", "txn_count"]
    page2 = "https://flink.example.com/results?page_token=2"
    page3 = "https://flink.example.com/results?page_token=3"
    pages = iter([
        {"results": {"data": []}, "metadata": {"next": page2}},  # empty before data
        {"results": {"data": [{"op": 0, "row": ["c1", 7]}]}, "metadata": {"next": page3}},
        {"results": {"data": []}, "metadata": {"next": page3}},  # drained; next still set
    ])

    def handler(request: httpx.Request) -> httpx.Response:
        if request.method in ("POST", "DELETE"):
            return httpx.Response(200, json={})
        if "results" in request.url.path or request.url.query:
            return httpx.Response(200, json=next(pages))
        return httpx.Response(200, json=_status_ready(columns))

    _install_transport(monkeypatch, handler)
    rows = FlinkSnapshotClient(_SETTINGS).snapshot("fct_c360_profile", 1000)
    assert rows == [{"customer_id": "c1", "txn_count": 7}]


def test_snapshot_returns_empty_when_unconfigured():
    settings = SimpleNamespace(**{**_SETTINGS.__dict__, "FLINK_CATALOG": None})
    assert FlinkSnapshotClient(settings).snapshot("fct_c360_profile", 10) == []


def test_snapshot_returns_empty_on_failed_statement(monkeypatch):
    def handler(request: httpx.Request) -> httpx.Response:
        if request.method in ("POST", "DELETE"):
            return httpx.Response(200, json={})
        return httpx.Response(200, json={"status": {"phase": "FAILED", "detail": "boom"}})

    _install_transport(monkeypatch, handler)
    assert FlinkSnapshotClient(_SETTINGS).snapshot("fct_c360_profile", 10) == []


def test_snapshot_returns_empty_on_http_error(monkeypatch):
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(500, text="server error")

    _install_transport(monkeypatch, handler)
    assert FlinkSnapshotClient(_SETTINGS).snapshot("fct_c360_profile", 10) == []
