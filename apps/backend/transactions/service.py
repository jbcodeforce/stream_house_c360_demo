"""Dispatch layer for transaction operations (Postgres store, create-only).

Emits a best-effort Debezium event on create when the runtime config flag is
enabled. Postgres is the source of truth; a producer failure is logged and
never fails (or hangs) the DB operation.
"""

from __future__ import annotations

import logging
from uuid import UUID, uuid4
from datetime import datetime, timezone
from config import settings
from transactions import db_sink, kafka_producer, inventory
from transactions.models import Transaction, TransactionCreate

logger = logging.getLogger("c360.transactions.service")

_EMIT_FLUSH_TIMEOUT = 5.0


def _maybe_emit(txn: Transaction) -> None:
    if settings.CDC_CONNECTOR_ENABLED:
        # A CDC connector (RDS / remote PG) owns Kafka; never dual-write.
        return
    if not settings.KAFKA_PRODUCE_ENABLED:
        return
    try:
        kafka_producer.produce_create(txn, flush_timeout=_EMIT_FLUSH_TIMEOUT)
    except Exception:
        logger.warning(
            "Kafka emit failed (op=c, id=%s)", txn.transaction_id, exc_info=True
        )


def create(data: TransactionCreate) -> Transaction:
    if settings.SINK == "postgres":
        txn = db_sink.create(data)
        _maybe_emit(txn)
        return txn
    now = datetime.now(tz=timezone.utc)
    txn = Transaction(
        customer_id=uuid4(),
        created_at=now,
        updated_at=now,
        **data.model_dump(),
    )
    inventory.create(txn)
    kafka_producer.produce_create(txn)
    return txn


def get_by_id(transaction_id: UUID) -> Transaction | None:
    return db_sink.get_by_id(transaction_id)


def list_all() -> list[Transaction]:
    return db_sink.list_all()
