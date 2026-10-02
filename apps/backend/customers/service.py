"""Dispatch layer for CRU customer operations.

Routes each operation to the correct sink based on ``settings.SINK``:
- ``"postgres"`` → :mod:`customers.db_sink` (PostgreSQL)
- ``"kafka"``    → :mod:`customers.inventory` (CSV read store) +
                   :mod:`customers.kafka_producer` (Confluent topic write)
"""

from __future__ import annotations

from datetime import datetime, timezone
from uuid import UUID, uuid4

from config import settings
from customers import db_sink, inventory, kafka_producer
from customers.models import Customer, CustomerCreate, CustomerUpdate


def create(data: CustomerCreate) -> Customer:
    if settings.SINK == "postgres":
        return db_sink.create(data)

    # kafka path: generate server-side fields, persist to CSV, produce event
    now = datetime.now(tz=timezone.utc)
    customer = Customer(
        customer_id=uuid4(),
        created_at=now,
        updated_at=now,
        **data.model_dump(),
    )
    inventory.create(customer)
    kafka_producer.produce_create(customer)
    return customer


def update(customer_id: UUID, data: CustomerUpdate) -> Customer | None:
    if settings.SINK == "postgres":
        return db_sink.update(customer_id, data)

    # kafka path: merge non-None fields onto the cached customer
    existing = inventory.get_by_id(customer_id)
    if existing is None:
        return None

    updated_fields = {k: v for k, v in data.model_dump().items() if v is not None}
    customer = existing.model_copy(
        update={**updated_fields, "updated_at": datetime.now(tz=timezone.utc)}
    )
    inventory.update(customer)
    kafka_producer.produce_update(customer)
    return customer


def get_by_id(customer_id: UUID) -> Customer | None:
    if settings.SINK == "postgres":
        return db_sink.get_by_id(customer_id)
    return inventory.get_by_id(customer_id)


def list_all() -> list[Customer]:
    if settings.SINK == "postgres":
        return db_sink.list_all()
    return inventory.list_all()


def delete_by_id(customer_id: UUID) -> bool:
    if settings.SINK == "postgres":
        return db_sink.delete_by_id(customer_id)
    return inventory.delete_by_id(customer_id)
