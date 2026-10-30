from __future__ import annotations

from datetime import date
from typing import Literal

from pydantic import BaseModel


class GraphNode(BaseModel):
    id: str
    type: Literal["source", "statement"]
    label: str
    statement_name: str | None = None
    topic: str | None = None
    is_product: bool = False
    # How this node's live metrics are scoped. Values mirror the dbt
    # `materialized` config exactly. "materialized_table" is kept by a hidden
    # system statement (not in the Statements list), so its metrics are scoped
    # by the MT UID discovered via the REST API; "streaming_table" is a regular
    # statement scoped by statement_name. Only set on statement nodes; source
    # topics leave it None. Absent/None is treated as materialized_table.
    table_type: Literal["materialized_table", "streaming_table"] | None = None
    # The Flink compute pool backing this statement (lfcp-...). Sourced from dbt
    # (per-model config, else the default pool) by the descriptor builder. Used to
    # attribute pipeline cost per pool and to scope live metrics per node. Only
    # set on statement nodes; source topics leave it None.
    compute_pool_id: str | None = None
    # True when this statement node is an upstream import from ANOTHER data
    # product (emitted so the graph stays connected). Excluded from this product's
    # cost consolidation — its pool belongs to the owning product. Owned nodes
    # leave it False.
    external: bool = False


class GraphEdge(BaseModel):
    source: str
    target: str


class Graph(BaseModel):
    nodes: list[GraphNode]
    edges: list[GraphEdge]


class DataProductSummary(BaseModel):
    id: str
    name: str
    description: str
    statement_count: int


class NodeStatus(BaseModel):
    node_id: str
    statement_name: str | None = None
    phase: Literal["RUNNING", "PENDING", "DEGRADED", "FAILED", "STOPPED", "UNKNOWN"]
    lagging: bool


class StatusRollup(BaseModel):
    running: int
    total: int
    lagging: int


class StatusResponse(BaseModel):
    statements: list[NodeStatus]
    rollup: StatusRollup


class CostResponse(BaseModel):
    """Month-to-date Flink cost for a data product's pipeline.

    ``available`` is False when there is no cost to show (no owned pools, or the
    Cost API returned nothing for them — a lagging day, missing billing access,
    or a failure): the frontend hides the banner rather than rendering ``$0``.
    ``amount`` sums the billed cost of the distinct compute pools the product's
    own (non-external) statements use; ``cfu`` is the current total CFUs across
    those pools. ``pool_ids`` are the pools that were consolidated.
    """

    available: bool
    amount: float = 0.0
    currency: str = "USD"
    cfu: float | None = None
    period_start: date | None = None
    period_end: date | None = None
    pool_ids: list[str] = []


class NodeMetrics(BaseModel):
    node_id: str
    phase: str
    uptime_seconds: int | None = None
    messages_in_per_sec: float | None = None
    messages_out_per_sec: float | None = None
    pending_records: int | None = None
    compute_pool_cfu: float | None = None
    sql: str | None = None
