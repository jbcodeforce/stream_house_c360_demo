from __future__ import annotations

import json
from pathlib import Path

from data_products.models import DataProductSummary, Graph

_DESCRIPTORS_DIR = Path(__file__).parent / "descriptors"


def _load_descriptors() -> dict[str, dict]:
    descriptors: dict[str, dict] = {}
    for path in sorted(_DESCRIPTORS_DIR.glob("*.json")):
        raw = json.loads(path.read_text())
        descriptors[raw["id"]] = raw
    return descriptors


_DESCRIPTORS = _load_descriptors()


def list_summaries() -> list[DataProductSummary]:
    summaries = []
    for raw in _DESCRIPTORS.values():
        statement_count = sum(1 for n in raw["nodes"] if n["type"] == "statement")
        summaries.append(
            DataProductSummary(
                id=raw["id"],
                name=raw["name"],
                description=raw["description"],
                statement_count=statement_count,
            )
        )
    return summaries


def get_graph(product_id: str) -> Graph | None:
    raw = _DESCRIPTORS.get(product_id)
    if raw is None:
        return None
    return Graph(nodes=raw["nodes"], edges=raw["edges"])
