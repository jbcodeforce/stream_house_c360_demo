"""Orchestration for the Data Products graph page.

Thin layer between the API router and the ``descriptor``/``providers``
modules: the router only ever calls into this module, never into
``descriptor`` or ``providers`` directly.
"""

from __future__ import annotations

from data_products import descriptor, providers
from data_products.models import CostResponse, DataProductSummary, Graph, NodeMetrics, StatusResponse


def list_products() -> list[DataProductSummary]:
    return descriptor.list_summaries()


def get_graph(product_id: str) -> Graph | None:
    return descriptor.get_graph(product_id)


def get_status(product_id: str) -> StatusResponse | None:
    graph = descriptor.get_graph(product_id)
    if graph is None:
        return None
    return providers.get_provider().get_status(graph)


def get_cost(product_id: str) -> CostResponse | None:
    graph = descriptor.get_graph(product_id)
    if graph is None:
        return None
    return providers.get_provider().get_cost(graph)


def get_node_metrics(product_id: str, node_id: str) -> NodeMetrics | None:
    graph = descriptor.get_graph(product_id)
    if graph is None:
        return None
    if not any(node.id == node_id for node in graph.nodes):
        return None
    return providers.get_provider().get_metrics(graph, node_id)
