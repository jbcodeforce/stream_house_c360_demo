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
    """Stand-in for ``confluent_client.ConfluentClient`` (all 4 Protocol methods)."""

    def __init__(
        self,
        phases: dict[str, str] | None = None,
        metrics: dict | None = None,
        mt_tables: dict[str, dict] | None = None,
        mt_metrics: dict | None = None,
    ) -> None:
        self._phases = phases or {}
        self._metrics = metrics or {}
        self._mt_tables = mt_tables or {}
        self._mt_metrics = mt_metrics or {}

    def list_statement_phases(self) -> dict[str, str]:
        return dict(self._phases)

    def list_materialized_tables(self) -> dict[str, dict]:
        return {name: dict(info) for name, info in self._mt_tables.items()}

    def get_statement_metrics(self, statement_name: str) -> dict:
        return dict(self._metrics)

    def get_materialized_table_metrics(self, mt_uid: str) -> dict:
        return dict(self._mt_metrics)


class _RaisingClient:
    """Stand-in that raises on every call, simulating an unreachable backend."""

    def __init__(self, exc: Exception) -> None:
        self._exc = exc

    def list_statement_phases(self) -> dict[str, str]:
        raise self._exc

    def list_materialized_tables(self) -> dict[str, dict]:
        raise self._exc

    def get_statement_metrics(self, statement_name: str) -> dict:
        raise self._exc

    def get_materialized_table_metrics(self, mt_uid: str) -> dict:
        raise self._exc


def _fake_client(
    phases: dict[str, str] | None = None,
    metrics: dict | None = None,
    mt_tables: dict[str, dict] | None = None,
    mt_metrics: dict | None = None,
) -> _FakeClient:
    return _FakeClient(phases=phases, metrics=metrics, mt_tables=mt_tables, mt_metrics=mt_metrics)


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


def test_live_status_from_materialized_tables():
    # Materialized tables are keyed by table name (= graph node id/label), not
    # the dbt statement name — the MT API is their source of truth.
    client = _fake_client(mt_tables={
        "src_customers": {"phase": "RUNNING", "uid": "mt-1"},
        "fct_c360_profile": {"phase": "FAILED", "uid": "mt-2"},
    })
    status = providers.LiveProvider(client=client).get_status(descriptor.get_graph("c360"))
    byid = {s.node_id: s.phase for s in status.statements}
    assert byid["src_customers"] == "RUNNING" and byid["fct_c360_profile"] == "FAILED"


def test_live_mt_metrics_populate_cfu():
    client = _fake_client(
        mt_tables={"src_customers": {"phase": "RUNNING", "uid": "mt-1"}},
        mt_metrics={"compute_pool_cfu": 3.0},
    )
    m = providers.LiveProvider(client=client).get_metrics(descriptor.get_graph("c360"), "src_customers")
    assert m.phase == "RUNNING" and m.compute_pool_cfu == 3.0


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


def test_metrics_query_body_matches_metrics_api_schema(monkeypatch):
    calls: list[dict] = []

    def fake_post(url, json=None, auth=None, timeout=None):  # noqa: A002 - match httpx signature
        calls.append({"url": url, "auth": auth, "body": json})
        return _Resp({"data": []})

    monkeypatch.setattr(confluent_client.httpx, "post", fake_post)
    client = confluent_client.ConfluentClient(settings_obj=_settings())

    client.get_statement_metrics("models-c360-sources-pipeline-src-customers")

    # Two scoped queries: statement-level pending_records + pool-level CFUs.
    assert len(calls) == 2
    for call in calls:
        # Regression guard: telemetry host, not api.confluent.cloud; HTTP Basic (not bearer).
        assert call["url"] == "https://api.telemetry.confluent.cloud/v2/metrics/cloud/query"
        assert call["auth"] == ("ck", "cs")
        body = call["body"]
        # 422 regression guard: intervals is required and must be present/non-empty.
        assert body["intervals"] and "/" in body["intervals"][0]
        assert body["granularity"] == "PT1M"

    bodies_by_metric = {c["body"]["aggregations"][0]["metric"]: c["body"] for c in calls}
    assert "io.confluent.flink/pending_records" in bodies_by_metric
    assert "io.confluent.flink/compute_pool_utilization/current_cfus" in bodies_by_metric

    # pending_records must be scoped to the statement via the correct filter field.
    pending_body = bodies_by_metric["io.confluent.flink/pending_records"]
    filter_fields = {f["field"]: f["value"] for f in pending_body["filter"]["filters"]}
    assert filter_fields["resource.flink_statement.name"] == "models-c360-sources-pipeline-src-customers"
    assert filter_fields["resource.compute_pool.id"] == "lfcp-x"


def test_metrics_parses_latest_data_point_value(monkeypatch):
    def fake_post(url, json=None, auth=None, timeout=None):  # noqa: A002
        metric = json["aggregations"][0]["metric"]
        if metric == "io.confluent.flink/pending_records":
            return _Resp({"data": [
                {"timestamp": "2026-10-24T10:00:00Z", "value": 10},
                {"timestamp": "2026-10-24T10:05:00Z", "value": 4200},  # latest
            ]})
        return _Resp({"data": [{"timestamp": "2026-10-24T10:05:00Z", "value": 2.0}]})

    monkeypatch.setattr(confluent_client.httpx, "post", fake_post)
    client = confluent_client.ConfluentClient(settings_obj=_settings())

    metrics = client.get_statement_metrics("models-c360-sources-pipeline-src-transactions")

    # Reads `value` from the most recent point (not a key named after the metric).
    assert metrics["pending_records"] == 4200
    assert metrics["compute_pool_cfu"] == 2.0


def test_list_materialized_tables_parses_name_phase_uid(monkeypatch):
    captured: dict = {}

    def fake_get(url, auth=None, timeout=None):
        captured["url"] = url
        captured["auth"] = auth
        return _Resp({"data": [
            {"name": "src_customers", "metadata": {"uid": "mt-1"}, "status": {"phase": "RUNNING", "version": 3}},
            {"name": "fct_c360_profile", "metadata": {"uid": "mt-2"}, "status": {"phase": "FAILED", "version": 1}},
        ]})

    monkeypatch.setattr(confluent_client.httpx, "get", fake_get)
    client = confluent_client.ConfluentClient(settings_obj=_settings())

    tables = client.list_materialized_tables()

    assert captured["url"] == (
        "https://flink.us-east-2.aws.confluent.cloud/sql/v1/organizations/"
        "org-1/environments/env-1/materialized-tables"
    )
    assert captured["auth"] == ("fk", "fs")
    assert tables == {
        "src_customers": {"phase": "RUNNING", "uid": "mt-1", "version": 3},
        "fct_c360_profile": {"phase": "FAILED", "uid": "mt-2", "version": 1},
    }


def test_mt_metrics_query_uses_materialized_table_uid_dimension(monkeypatch):
    calls: list[dict] = []

    def fake_post(url, json=None, auth=None, timeout=None):  # noqa: A002
        calls.append({"url": url, "auth": auth, "body": json})
        return _Resp({"data": [{"timestamp": "2026-10-26T10:05:00Z", "value": 1.5}]})

    monkeypatch.setattr(confluent_client.httpx, "post", fake_post)
    client = confluent_client.ConfluentClient(settings_obj=_settings())

    metrics = client.get_materialized_table_metrics("mt-uid-123")

    assert len(calls) == 1
    body = calls[0]["body"]
    assert calls[0]["url"] == "https://api.telemetry.confluent.cloud/v2/metrics/cloud/query"
    assert body["aggregations"][0]["metric"] == "io.confluent.flink/materialized_table_utilization/current_cfus"
    # Scoped by the materialized-table UID (canonical resource id), not .name.
    assert body["filter"]["field"] == confluent_client._MT_UID_DIMENSION
    assert body["filter"]["value"] == "mt-uid-123"
    assert body["intervals"] and "/" in body["intervals"][0]
    assert metrics["compute_pool_cfu"] == 1.5


def test_mt_metrics_empty_uid_skips_query(monkeypatch):
    calls: list[dict] = []
    monkeypatch.setattr(confluent_client.httpx, "post", lambda *a, **k: calls.append(1))
    client = confluent_client.ConfluentClient(settings_obj=_settings())
    assert client.get_materialized_table_metrics("") == {}
    assert calls == []  # no HTTP call when the uid is unknown


def test_metrics_forbidden_is_swallowed(monkeypatch):
    # A 403 (missing MetricsViewer role) must degrade to empty metrics, never raise.
    req = httpx.Request("POST", confluent_client._METRICS_API_BASE_URL)
    resp = httpx.Response(403, request=req)

    def fake_post(url, json=None, auth=None, timeout=None):  # noqa: A002
        raise httpx.HTTPStatusError("forbidden", request=req, response=resp)

    monkeypatch.setattr(confluent_client.httpx, "post", fake_post)
    client = confluent_client.ConfluentClient(settings_obj=_settings())

    metrics = client.get_statement_metrics("models-c360-sources-pipeline-src-customers")

    assert metrics == {}


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
