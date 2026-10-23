"""Status/metrics providers for the Data Products graph page.

``MockProvider`` returns deterministic fixtures (Task 2); ``LiveProvider``
will talk to Confluent Cloud + Flink (Task 3). ``get_provider()`` selects
between them based on ``settings.GRAPH_DATA_MODE``, re-reading the setting on
every call so a reloaded ``config`` module (as in tests) takes effect
immediately.
"""

from __future__ import annotations

import logging
from typing import Protocol

from data_products import mock_data
from data_products.confluent_client import ConfluentClient, ConfluentClientProtocol
from data_products.models import Graph, NodeMetrics, NodeStatus, StatusResponse, StatusRollup

logger = logging.getLogger(__name__)

_VALID_PHASES = frozenset({"RUNNING", "PENDING", "DEGRADED", "FAILED", "STOPPED", "UNKNOWN"})

# Pending-records count above which a statement is reported as "lagging".
# Not test-critical / not tied to any SLA — a reasonable default that can be
# tuned once real backlog volumes are observed in live mode.
_LAG_THRESHOLD_PENDING_RECORDS = 1_000


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
    """Best-effort Confluent Cloud + Flink status/metrics provider.

    Talks to Confluent Cloud + Flink via an injected ``ConfluentClient``
    (defaults to a real one built from ``config.settings``). Live mode is
    explicitly best-effort and failure-tolerant: any error raised by the
    client — network failure, bad credentials, malformed response, or
    anything else — is caught here so it never propagates out of
    ``get_status``/``get_metrics``. On total failure every statement's phase
    becomes "UNKNOWN" and the response/metrics are still well-formed, so the
    graph page degrades gracefully instead of breaking.
    """

    def __init__(self, client: ConfluentClientProtocol | None = None) -> None:
        self._client: ConfluentClientProtocol = client if client is not None else ConfluentClient()

    def _safe_phases(self) -> dict[str, str]:
        try:
            return self._client.list_statement_phases()
        except Exception:  # noqa: BLE001 — best-effort: never let this surface
            logger.warning("LiveProvider: list_statement_phases failed; reporting UNKNOWN", exc_info=True)
            return {}

    def _safe_metrics(self, statement_name: str) -> dict:
        try:
            return self._client.get_statement_metrics(statement_name)
        except Exception:  # noqa: BLE001 — best-effort: never let this surface
            logger.warning("LiveProvider: get_statement_metrics failed for %s", statement_name, exc_info=True)
            return {}

    @staticmethod
    def _normalize_phase(phase: str | None) -> str:
        return phase if phase in _VALID_PHASES else "UNKNOWN"

    def get_status(self, graph: Graph) -> StatusResponse:
        phases = self._safe_phases()
        statements = []
        for node in graph.nodes:
            if node.type != "statement":
                continue
            phase = self._normalize_phase(phases.get(node.statement_name or ""))
            raw_metrics = self._safe_metrics(node.statement_name) if node.statement_name else {}
            pending_records = raw_metrics.get("pending_records")
            lagging = pending_records is not None and pending_records > _LAG_THRESHOLD_PENDING_RECORDS
            statements.append(
                NodeStatus(
                    node_id=node.id,
                    statement_name=node.statement_name,
                    phase=phase,
                    lagging=lagging,
                )
            )
        rollup = StatusRollup(
            running=sum(1 for s in statements if s.phase == "RUNNING"),
            total=len(statements),
            lagging=sum(1 for s in statements if s.lagging),
        )
        return StatusResponse(statements=statements, rollup=rollup)

    def get_metrics(self, graph: Graph, node_id: str) -> NodeMetrics:
        node = next((n for n in graph.nodes if n.id == node_id), None)
        if node is None or node.statement_name is None:
            return NodeMetrics(node_id=node_id, phase="UNKNOWN")

        phases = self._safe_phases()
        phase = self._normalize_phase(phases.get(node.statement_name))
        raw = self._safe_metrics(node.statement_name)
        return NodeMetrics(
            node_id=node_id,
            phase=phase,
            uptime_seconds=raw.get("uptime_seconds"),
            messages_in_per_sec=raw.get("messages_in_per_sec"),
            messages_out_per_sec=raw.get("messages_out_per_sec"),
            pending_records=raw.get("pending_records"),
            compute_pool_cfu=raw.get("compute_pool_cfu"),
            sql=raw.get("sql"),
        )


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
