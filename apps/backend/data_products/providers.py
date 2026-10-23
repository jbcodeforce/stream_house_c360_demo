"""Status/metrics providers for the Data Products graph page.

``MockProvider`` returns deterministic fixtures (Task 2); ``LiveProvider``
will talk to Confluent Cloud + Flink (Task 3). ``get_provider()`` selects
between them based on ``settings.GRAPH_DATA_MODE``, re-reading the setting on
every call so a reloaded ``config`` module (as in tests) takes effect
immediately.
"""

from __future__ import annotations

from typing import Protocol

from data_products import mock_data
from data_products.models import Graph, NodeMetrics, NodeStatus, StatusResponse, StatusRollup


class StatusProvider(Protocol):
    def get_status(self, graph: Graph) -> StatusResponse: ...

    def get_metrics(self, graph: Graph, node_id: str) -> NodeMetrics: ...


class MockProvider:
    """Deterministic provider backed by ``mock_data.STATEMENT_FIXTURES``."""

    def __init__(self, lagging: bool) -> None:
        self.lagging = lagging

    def _is_lagging(self, node_id: str) -> bool:
        return self.lagging and node_id == mock_data.LAGGING_NODE_ID

    def get_status(self, graph: Graph) -> StatusResponse:
        statements = [
            NodeStatus(
                node_id=node.id,
                statement_name=node.statement_name,
                phase="RUNNING",
                lagging=self._is_lagging(node.id),
            )
            for node in graph.nodes
            if node.type == "statement"
        ]
        rollup = StatusRollup(
            running=sum(1 for s in statements if s.phase == "RUNNING"),
            total=len(statements),
            lagging=sum(1 for s in statements if s.lagging),
        )
        return StatusResponse(statements=statements, rollup=rollup)

    def get_metrics(self, graph: Graph, node_id: str) -> NodeMetrics:
        uptime, msg_in, msg_out, cfu, sql = mock_data.STATEMENT_FIXTURES[node_id]
        pending_records = (
            mock_data.LAGGING_PENDING_RECORDS if self._is_lagging(node_id) else 0
        )
        return NodeMetrics(
            node_id=node_id,
            phase="RUNNING",
            uptime_seconds=uptime,
            messages_in_per_sec=msg_in,
            messages_out_per_sec=msg_out,
            pending_records=pending_records,
            compute_pool_cfu=cfu,
            sql=sql,
        )


class LiveProvider:
    """Talks to Confluent Cloud + Flink for real status/metrics (Task 3)."""

    def get_status(self, graph: Graph) -> StatusResponse:
        raise NotImplementedError("LiveProvider is implemented in Task 3")

    def get_metrics(self, graph: Graph, node_id: str) -> NodeMetrics:
        raise NotImplementedError("LiveProvider is implemented in Task 3")


def get_provider() -> StatusProvider:
    """Select a provider based on the current ``GRAPH_DATA_MODE``.

    Imports ``config`` and reads ``settings`` fresh on every call — not at
    module import time — so a reloaded (or replaced, as some test fixtures
    do via ``sys.modules``) ``config`` module is picked up immediately rather
    than reading a stale module object bound at import time.
    """
    import config  # noqa: PLC0415 — intentional late import, see docstring

    mode = config.settings.GRAPH_DATA_MODE
    if mode == "mock":
        return MockProvider(lagging=False)
    if mode == "mock-lag":
        return MockProvider(lagging=True)
    if mode == "live":
        return LiveProvider()
    raise ValueError(f"Unknown GRAPH_DATA_MODE: {mode!r}")
