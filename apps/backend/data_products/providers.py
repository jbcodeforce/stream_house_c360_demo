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
        fixture = mock_data.STATEMENT_FIXTURES.get(node_id)
        if fixture is None:
            # Non-statement node (e.g. a SOURCE topic) — mirror LiveProvider's
            # behavior for the same case instead of raising KeyError.
            return NodeMetrics(node_id=node_id, phase="UNKNOWN")

        uptime, msg_in, msg_out, cfu, sql = fixture
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

    def _safe_statement_phases(self) -> dict[str, str]:
        try:
            return self._client.list_statement_phases()
        except Exception:  # noqa: BLE001 — best-effort: never let this surface
            logger.warning("LiveProvider: list_statement_phases failed; reporting UNKNOWN", exc_info=True)
            return {}

    def _safe_materialized_tables(self) -> dict[str, dict]:
        """``{name: {"phase", "uid", "version"}}`` — the source of truth for the
        c360 nodes, which are materialized tables (not in the Statements list)."""
        try:
            return self._client.list_materialized_tables()
        except Exception:  # noqa: BLE001 — best-effort: never let this surface
            logger.warning("LiveProvider: list_materialized_tables failed; reporting UNKNOWN", exc_info=True)
            return {}

    @staticmethod
    def _merged_phases(statement_phases: dict[str, str], mt_tables: dict[str, dict]) -> dict[str, str]:
        """One phase lookup keyed by both statement names and MT table names.
        The two key spaces are distinct, so a node can resolve its phase by
        whichever identifier the backend keyed it under."""
        phases = dict(statement_phases)
        for name, info in mt_tables.items():
            if info.get("phase"):
                phases[name] = info["phase"]
        return phases

    @staticmethod
    def _normalize_phase(phase: str | None) -> str:
        return phase if phase in _VALID_PHASES else "UNKNOWN"

    @classmethod
    def _phase_for(cls, node, phases: dict[str, str]) -> str:
        for key in (node.id, node.label, node.statement_name):
            if key and key in phases:
                return cls._normalize_phase(phases[key])
        return "UNKNOWN"

    @staticmethod
    def _mt_entry_for(node, mt_tables: dict[str, dict]) -> dict | None:
        """The materialized-tables entry for this node, matched by table name =
        node id/label. Carries the UID (Metrics API resource id) and the
        defining SQL (spec.query)."""
        for key in (node.id, node.label):
            info = mt_tables.get(key)
            if info:
                return info
        return None

    def _safe_mt_metrics(self, uid: str) -> dict:
        try:
            return self._client.get_materialized_table_metrics(uid)
        except Exception:  # noqa: BLE001 — best-effort: never let this surface
            logger.warning("LiveProvider: get_materialized_table_metrics failed for uid=%s", uid, exc_info=True)
            return {}

    def _safe_statement_metrics(self, statement_name: str) -> dict:
        try:
            return self._client.get_statement_metrics(statement_name)
        except Exception:  # noqa: BLE001 — best-effort: never let this surface
            logger.warning("LiveProvider: get_statement_metrics failed for %s", statement_name, exc_info=True)
            return {}

    def get_status(self, graph: Graph) -> StatusResponse:
        phases = self._merged_phases(self._safe_statement_phases(), self._safe_materialized_tables())
        statements = []
        for node in graph.nodes:
            if node.type != "statement":
                continue
            # Status is phase-only in live mode: a per-node backlog signal would
            # need a metric call per node per poll (N+1), and materialized tables
            # don't expose pending_records anyway. A DEGRADED/FAILED phase still
            # colors the node via the phase mapping; the mock-lag preset is what
            # demonstrates the lag visual.
            statements.append(
                NodeStatus(
                    node_id=node.id,
                    statement_name=node.statement_name,
                    phase=self._phase_for(node, phases),
                    lagging=False,
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
        if node is None or node.type != "statement":
            return NodeMetrics(node_id=node_id, phase="UNKNOWN")

        statement_phases = self._safe_statement_phases()
        mt_tables = self._safe_materialized_tables()
        phases = self._merged_phases(statement_phases, mt_tables)

        # Metrics are scoped differently per table_type (mirrors the dbt
        # `materialized` config): a streaming_table is a regular statement,
        # scoped by statement name; a materialized_table is a hidden statement,
        # scoped by its MT UID (found via the materialized-tables REST API).
        # Query only the matching path — the other one's identifier doesn't
        # resolve for this kind of table. table_type is None for legacy/unset
        # nodes; treat that as materialized_table, the historical default for
        # the all-MT c360 pipeline.
        raw: dict = {}
        if node.table_type == "streaming_table":
            if node.statement_name:
                raw.update(self._safe_statement_metrics(node.statement_name))
        else:
            entry = self._mt_entry_for(node, mt_tables)
            if entry:
                if entry.get("uid"):
                    raw.update(self._safe_mt_metrics(entry["uid"]))
                if entry.get("sql"):
                    raw["sql"] = entry["sql"]

        return NodeMetrics(
            node_id=node_id,
            phase=self._phase_for(node, phases),
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
