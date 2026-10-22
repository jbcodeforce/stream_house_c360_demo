"""Dispatch layer for transaction operations (Postgres store, create-only).

Emits a best-effort Debezium event on create when the runtime config flag is
enabled. Postgres is the source of truth; a producer failure is logged and
never fails (or hangs) the DB operation.
"""

from __future__ import annotations

import logging
from uuid import UUID, uuid4
from datetime import datetime, timezone
from accounts import service as accounts_service
from config import settings
from transactions import db_sink, generator, kafka_producer, inventory
from transactions.generator import NoEligibleAccountsError
from transactions.models import Transaction, TransactionCreate

logger = logging.getLogger("c360.transactions.service")

_EMIT_FLUSH_TIMEOUT = 5.0
_EMIT_BATCH_FLUSH_TIMEOUT = 30.0

__all__ = ["NoEligibleAccountsError", "UnsupportedSinkError"]


class UnsupportedSinkError(Exception):
    """Raised when generation is requested for a sink other than postgres."""


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


def _maybe_emit_many(txns: list[Transaction]) -> None:
    if settings.CDC_CONNECTOR_ENABLED:
        # A CDC connector (RDS / remote PG) owns Kafka; never dual-write.
        return
    if not settings.KAFKA_PRODUCE_ENABLED:
        return
    try:
        kafka_producer.produce_create_many(txns, flush_timeout=_EMIT_BATCH_FLUSH_TIMEOUT)
    except Exception:
        logger.warning("Kafka batch emit failed (%d events)", len(txns), exc_info=True)


def generate() -> list[Transaction]:
    """Create a batch of back-dated transactions on existing accounts.

    Inserts into Postgres, then publishes to Kafka under the same gating as
    :func:`create`. Raises :class:`NoEligibleAccountsError` when no account can
    be used and :class:`UnsupportedSinkError` for non-postgres sinks.
    """
    if settings.SINK != "postgres":
        raise UnsupportedSinkError(settings.SINK)
    txns = generator.generate(accounts_service.list_all())
    db_sink.create_many(txns)
    _maybe_emit_many(txns)
    return txns


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
