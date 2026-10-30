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


def test_mock_metrics_source_node_is_unknown_not_raising():
    """SOURCE nodes (e.g. topic_customers) aren't in STATEMENT_FIXTURES —
    get_metrics must degrade to UNKNOWN instead of raising KeyError, mirroring
    LiveProvider's shape for a non-statement node."""
    graph = descriptor.get_graph("c360")
    m = providers.MockProvider(lagging=False).get_metrics(graph, "topic_customers")
    assert m.node_id == "topic_customers" and m.phase == "UNKNOWN"
    assert m.uptime_seconds is None
    assert m.messages_in_per_sec is None
    assert m.messages_out_per_sec is None
    assert m.pending_records is None
    assert m.compute_pool_cfu is None
    assert m.sql is None


def test_mock_and_live_agree_on_source_node_metrics_shape():
    graph = descriptor.get_graph("c360")
    mock_metrics = providers.MockProvider(lagging=False).get_metrics(graph, "topic_customers")

    class _NeverCalledClient:
        def list_statement_phases(self) -> dict[str, str]:
            raise AssertionError("should not be called for a non-statement node")

        def get_statement_metrics(self, statement_name: str) -> dict:
            raise AssertionError("should not be called for a non-statement node")

    live_metrics = providers.LiveProvider(client=_NeverCalledClient()).get_metrics(graph, "topic_customers")

    assert mock_metrics.model_dump() == live_metrics.model_dump()


def test_factory_selects_mock_lag(monkeypatch):
    monkeypatch.setenv("GRAPH_DATA_MODE", "mock-lag")
    import importlib; import config; importlib.reload(config)
    prov = providers.get_provider()
    assert isinstance(prov, providers.MockProvider) and prov.lagging is True


def test_mock_get_cost_matches_fixture_cfu():
    from data_products import mock_data

    graph = descriptor.get_graph("c360")
    cost = providers.MockProvider(lagging=False).get_cost(graph)
    expected_cfu = sum(
        mock_data.STATEMENT_FIXTURES[n.id][3]
        for n in graph.nodes
        if n.type == "statement" and not n.external and n.id in mock_data.STATEMENT_FIXTURES
    )
    assert cost.available is True
    assert cost.cfu == expected_cfu
    assert cost.amount == round(expected_cfu * mock_data.MOCK_CFU_DOLLARS_PER_MONTH, 2)
    assert cost.pool_ids == ["lfcp-c360"]  # owned pools from the graph's statement nodes
