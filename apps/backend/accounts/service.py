"""Dispatch layer for account CRUD operations (Postgres store).

Emits a best-effort Debezium event on create/update/delete when the runtime
config flag is enabled. Postgres is the source of truth; a producer failure
is logged and never fails (or hangs) the DB operation.
"""

from __future__ import annotations

import logging
from uuid import UUID

import config_store
from config import settings
from accounts import db_sink, kafka_producer
from accounts.models import Account, AccountCreate, AccountUpdate

logger = logging.getLogger("c360.accounts.service")

_EMIT_FLUSH_TIMEOUT = 5.0


def _maybe_emit(op: str, account: Account) -> None:
    if settings.CDC_CONNECTOR_ENABLED:
        # A CDC connector (RDS / remote PG) owns Kafka; never dual-write.
        return
    if not config_store.get_config().kafka_produce_enabled:
        return
    try:
        if op == "C":
            kafka_producer.produce_create(account, flush_timeout=_EMIT_FLUSH_TIMEOUT)
        elif op == "U":
            kafka_producer.produce_update(account, flush_timeout=_EMIT_FLUSH_TIMEOUT)
        elif op == "D":
            kafka_producer.produce_delete(account, flush_timeout=_EMIT_FLUSH_TIMEOUT)
    except Exception:
        logger.warning(
            "Kafka emit failed (op=%s, id=%s)", op, account.account_id, exc_info=True
        )


def create(data: AccountCreate) -> Account:
    account = db_sink.create(data)
    _maybe_emit("C", account)
    return account


def update(account_id: UUID, data: AccountUpdate) -> Account | None:
    account = db_sink.update(account_id, data)
    if account is not None:
        _maybe_emit("U", account)
    return account


def get_by_id(account_id: UUID) -> Account | None:
    return db_sink.get_by_id(account_id)


def list_all() -> list[Account]:
    return db_sink.list_all()


def delete_by_id(account_id: UUID) -> bool:
    account = db_sink.get_by_id(account_id)
    deleted = db_sink.delete_by_id(account_id)
    if deleted and account is not None:
        _maybe_emit("D", account)
    return deleted
