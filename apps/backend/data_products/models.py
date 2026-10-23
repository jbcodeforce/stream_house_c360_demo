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
