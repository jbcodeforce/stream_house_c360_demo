"""Tests for the live (best-effort Confluent REST) status/metrics provider.

httpx is mocked at the client boundary: these tests inject a fake/raising
object in place of ``confluent_client.ConfluentClient`` into
``providers.LiveProvider(client=...)``, so no real network call is ever
made.
"""

from __future__ import annotations

import types

import httpx

from data_products import confluent_client, descriptor, providers


class _FakeClient:
    """Stand-in for ``confluent_client.ConfluentClient``."""

    def __init__(self, phases: dict[str, str] | None = None, metrics: dict | None = None) -> None:
        self._phases = phases or {}
        self._metrics = metrics or {}

    def list_statement_phases(self) -> dict[str, str]:
        return dict(self._phases)

    def get_statement_metrics(self, statement_name: str) -> dict:
        return dict(self._metrics)


class _RaisingClient:
    """Stand-in that raises on every call, simulating an unreachable backend."""

    def __init__(self, exc: Exception) -> None:
        self._exc = exc

    def list_statement_phases(self) -> dict[str, str]:
        raise self._exc

    def get_statement_metrics(self, statement_name: str) -> dict:
        raise self._exc


def _fake_client(phases: dict[str, str] | None = None, metrics: dict | None = None) -> _FakeClient:
    return _FakeClient(phases=phases, metrics=metrics)


def _raising_client(exc: Exception) -> _RaisingClient:
    return _RaisingClient(exc)


def test_live_status_maps_phases():
    client = _fake_client(phases={
        "models-c360-sources-pipeline-src-customers": "RUNNING",
        "models-c360-facts-pipeline-fct-c360-profile": "FAILED",
    })
    status = providers.LiveProvider(client=client).get_status(descriptor.get_graph("c360"))
    byid = {s.node_id: s.phase for s in status.statements}
    assert byid["src_customers"] == "RUNNING" and byid["fct_c360_profile"] == "FAILED"


def test_live_unknown_phase_becomes_UNKNOWN():
    client = _fake_client(phases={})  # statement not reported
    status = providers.LiveProvider(client=client).get_status(descriptor.get_graph("c360"))
    assert all(s.phase in {"UNKNOWN"} or s.phase for s in status.statements)
    assert any(s.phase == "UNKNOWN" for s in status.statements)


def test_live_missing_metric_is_none():
    client = _fake_client(metrics={"pending_records": 5})  # no rate fields
    m = providers.LiveProvider(client=client).get_metrics(descriptor.get_graph("c360"), "src_customers")
    assert m.pending_records == 5 and m.messages_in_per_sec is None


def test_live_unreachable_does_not_raise():
    client = _raising_client(httpx.ConnectError("boom"))
    status = providers.LiveProvider(client=client).get_status(descriptor.get_graph("c360"))
    assert status.rollup.total == 8
    assert all(s.phase == "UNKNOWN" for s in status.statements)


# --- real-ConfluentClient HTTP-layer tests (httpx mocked, no network) ---
#
# The tests above inject a fake at the client boundary, so they never
# exercise the real URLs. These patch httpx itself to assert the real
# ConfluentClient hits the correct Confluent endpoints with HTTP Basic auth.


class _Resp:
    def __init__(self, payload: dict) -> None:
        self._payload = payload

    def raise_for_status(self) -> None:  # noqa: D401 - mimic httpx.Response
        return None

    def json(self) -> dict:
        return self._payload


def _settings(**overrides) -> types.SimpleNamespace:
    base = dict(
        CONFLUENT_CLOUD_API_KEY="ck",
        CONFLUENT_CLOUD_API_SECRET="cs",
        FLINK_COMPUTE_POOL_ID="lfcp-x",
        FLINK_REST_ENDPOINT="https://flink.us-east-2.aws.confluent.cloud",
        FLINK_API_KEY="fk",
        FLINK_API_SECRET="fs",
        FLINK_ORG_ID="org-1",
        FLINK_ENV_ID="env-1",
    )
    base.update(overrides)
    return types.SimpleNamespace(**base)


def test_metrics_posts_to_telemetry_host_with_basic_auth(monkeypatch):
    captured: dict = {}

    def fake_post(url, json=None, auth=None, timeout=None):  # noqa: A002 - match httpx signature
        captured["url"] = url
        captured["auth"] = auth
        return _Resp({"data": []})

    monkeypatch.setattr(confluent_client.httpx, "post", fake_post)
    client = confluent_client.ConfluentClient(settings_obj=_settings())

    client.get_statement_metrics("models-c360-sources-pipeline-src-customers")

    # Regression guard: the Metrics API lives on the telemetry host, not api.confluent.cloud.
    assert captured["url"] == "https://api.telemetry.confluent.cloud/v2/metrics/cloud/query"
    # HTTP Basic with the Cloud API key/secret — not a bearer token.
    assert captured["auth"] == ("ck", "cs")


def test_statement_phases_uses_flink_endpoint_with_basic_auth(monkeypatch):
    captured: dict = {}

    def fake_get(url, auth=None, timeout=None):
        captured["url"] = url
        captured["auth"] = auth
        return _Resp({"data": []})

    monkeypatch.setattr(confluent_client.httpx, "get", fake_get)
    client = confluent_client.ConfluentClient(settings_obj=_settings())

    client.list_statement_phases()

    assert captured["url"] == (
        "https://flink.us-east-2.aws.confluent.cloud/sql/v1/organizations/"
        "org-1/environments/env-1/statements"
    )
    assert captured["auth"] == ("fk", "fs")
