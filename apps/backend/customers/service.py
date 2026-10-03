"""Dispatch layer for CRU customer operations.

Routes each operation to the correct sink based on ``settings.SINK``:
- ``"postgres"`` → :mod:`customers.db_sink` (PostgreSQL)
- ``"kafka"``    → :mod:`customers.inventory` (CSV read store) +
                   :mod:`customers.kafka_producer` (Confluent topic write)
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from uuid import UUID, uuid4

import config_store
from config import settings
from customers import db_sink, inventory, kafka_producer
from customers.models import Customer, CustomerCreate, CustomerUpdate

logger = logging.getLogger("c360.service")


def _maybe_emit(op: str, customer: Customer) -> None:
    """Best-effort Kafka emission for the postgres path, gated by config.

    A producer failure is logged and swallowed — Postgres is the source of
    truth and the DB operation must not fail because Kafka is unavailable.
    """
    if not config_store.get_config().kafka_produce_enabled:
        return
    try:
        if op == "C":
            kafka_producer.produce_create(customer)
        elif op == "U":
            kafka_producer.produce_update(customer)
        elif op == "D":
            kafka_producer.produce_delete(customer)
    except Exception:
        logger.warning(
            "Kafka emit failed (op=%s, id=%s)", op, customer.customer_id, exc_info=True
        )


def create(data: CustomerCreate) -> Customer:
    if settings.SINK == "postgres":
        customer = db_sink.create(data)
        _maybe_emit("C", customer)
        return customer

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
        customer = db_sink.update(customer_id, data)
        if customer is not None:
            _maybe_emit("U", customer)
        return customer

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
        customer = db_sink.get_by_id(customer_id)
        deleted = db_sink.delete_by_id(customer_id)
        if deleted and customer is not None:
            _maybe_emit("D", customer)
        return deleted
    # kafka path: look up the customer first so we can produce a tombstone
    existing = inventory.get_by_id(customer_id)
    if existing is None:
        return False
    deleted = inventory.delete_by_id(customer_id)
    if deleted:
        kafka_producer.produce_delete(existing)
    return deleted
