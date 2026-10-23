from __future__ import annotations

from typing import Literal

from pydantic import BaseModel


class GraphNode(BaseModel):
    id: str
    type: Literal["source", "statement"]
    label: str
    statement_name: str | None = None
    topic: str | None = None
    is_product: bool = False


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


class NodeMetrics(BaseModel):
    node_id: str
    phase: str
    uptime_seconds: int | None = None
    messages_in_per_sec: float | None = None
    messages_out_per_sec: float | None = None
    pending_records: int | None = None
    compute_pool_cfu: float | None = None
    sql: str | None = None
