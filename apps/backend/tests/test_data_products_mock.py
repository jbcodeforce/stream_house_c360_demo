from __future__ import annotations

from data_products import descriptor, providers


def test_mock_status_all_running(monkeypatch):
    monkeypatch.setenv("GRAPH_DATA_MODE", "mock")
    graph = descriptor.get_graph("c360")
    status = providers.MockProvider(lagging=False).get_status(graph)
    assert status.rollup.total == 8 and status.rollup.running == 8 and status.rollup.lagging == 0
    assert all(s.phase == "RUNNING" for s in status.statements)


def test_mock_lag_marks_src_transactions():
    graph = descriptor.get_graph("c360")
    status = providers.MockProvider(lagging=True).get_status(graph)
    assert status.rollup.lagging == 1
    lagging = [s.node_id for s in status.statements if s.lagging]
    assert lagging == ["src_transactions"]


def test_mock_status_covers_statements_only():
    graph = descriptor.get_graph("c360")
    status = providers.MockProvider(lagging=False).get_status(graph)
    assert {s.node_id for s in status.statements} == {n.id for n in graph.nodes if n.type == "statement"}


def test_mock_metrics_has_full_fields():
    graph = descriptor.get_graph("c360")
    m = providers.MockProvider(lagging=False).get_metrics(graph, "src_customers")
    assert m.phase == "RUNNING" and m.pending_records == 0
    assert m.messages_in_per_sec is not None and m.sql is not None


def test_factory_selects_mock_lag(monkeypatch):
    monkeypatch.setenv("GRAPH_DATA_MODE", "mock-lag")
    import importlib; import config; importlib.reload(config)
    prov = providers.get_provider()
    assert isinstance(prov, providers.MockProvider) and prov.lagging is True
