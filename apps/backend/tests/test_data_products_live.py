"""Tests for the live (best-effort Confluent REST) status/metrics provider.

httpx is mocked at the client boundary: these tests inject a fake/raising
object in place of ``confluent_client.ConfluentClient`` into
``providers.LiveProvider(client=...)``, so no real network call is ever
made.
"""

from __future__ import annotations

import httpx

from data_products import descriptor, providers


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
