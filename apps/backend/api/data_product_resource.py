"""FastAPI router for the /data-products endpoints (Data Products graph page)."""

from __future__ import annotations

from fastapi import APIRouter, HTTPException

from data_products import service
from data_products.models import CostResponse, DataProductSummary, Graph, NodeMetrics, StatusResponse

router = APIRouter(prefix="/data-products", tags=["data-products"])


@router.get("", response_model=list[DataProductSummary], status_code=200)
def list_products() -> list[DataProductSummary]:
    return service.list_products()


@router.get("/{product_id}/graph", response_model=Graph, status_code=200)
def get_graph(product_id: str) -> Graph:
    graph = service.get_graph(product_id)
    if graph is None:
        raise HTTPException(status_code=404, detail="Data product not found")
    return graph


@router.get("/{product_id}/status", response_model=StatusResponse, status_code=200)
def get_status(product_id: str) -> StatusResponse:
    status = service.get_status(product_id)
    if status is None:
        raise HTTPException(status_code=404, detail="Data product not found")
    return status


@router.get("/{product_id}/cost", response_model=CostResponse, status_code=200)
def get_cost(product_id: str) -> CostResponse:
    cost = service.get_cost(product_id)
    if cost is None:
        raise HTTPException(status_code=404, detail="Data product not found")
    return cost


@router.get("/{product_id}/nodes/{node_id}/metrics", response_model=NodeMetrics, status_code=200)
def get_node_metrics(product_id: str, node_id: str) -> NodeMetrics:
    metrics = service.get_node_metrics(product_id, node_id)
    if metrics is None:
        raise HTTPException(status_code=404, detail="Data product or node not found")
    return metrics
