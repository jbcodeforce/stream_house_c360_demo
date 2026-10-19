"""Dispatch layer for account CRUD operations (Postgres store).

Emits a best-effort Debezium event on create/update/delete when the runtime
config flag is enabled. Postgres is the source of truth; a producer failure
is logged and never fails (or hangs) the DB operation.
"""

from __future__ import annotations

import logging
from uuid import UUID, uuid4
from datetime import datetime, timezone

from config import settings
from accounts import db_sink, kafka_producer, inventory
from accounts.models import Account, AccountCreate, AccountUpdate

logger = logging.getLogger("c360.accounts.service")

_EMIT_FLUSH_TIMEOUT = 5.0


def _maybe_emit(op: str, account: Account) -> None:
    if settings.CDC_CONNECTOR_ENABLED:
        # A CDC connector (RDS / remote PG) owns Kafka; never dual-write.
        return
    if not settings.KAFKA_PRODUCE_ENABLED:
        return
    try:
        if op == "c":
            kafka_producer.produce_create(account, flush_timeout=_EMIT_FLUSH_TIMEOUT)
        elif op == "u":
            kafka_producer.produce_update(account, flush_timeout=_EMIT_FLUSH_TIMEOUT)
        elif op == "d":
            kafka_producer.produce_delete(account, flush_timeout=_EMIT_FLUSH_TIMEOUT)
    except Exception:
        logger.warning(
            "Kafka emit failed (op=%s, id=%s)", op, account.account_id, exc_info=True
        )


def create(data: AccountCreate) -> Account:
    if settings.SINK == "postgres":
        account = db_sink.create(data)
        _maybe_emit("c", account)
        return account
    now = datetime.now(tz=timezone.utc)
    account = Account(
        account_id = uuid4(),
        created_at=now,
        updated_at=now,
        **data.model_dump(),
    )
    inventory.create(account)
    kafka_producer.produce_create(account)
    return account


def update(account_id: UUID, data: AccountUpdate) -> Account | None:
    if settings.SINK == "postgres":
        account = db_sink.update(account_id, data)
        if account is not None:
            _maybe_emit("u", account)
        return account
    existing = inventory.get_by_id(account_id)
    if existing is None:
        return None

    updated_fields = {k: v for k, v in data.model_dump().items() if v is not None}
    account = existing.model_copy(
        update={**updated_fields, "updated_at": datetime.now(tz=timezone.utc)}
    )
    inventory.update(account)
    kafka_producer.produce_update(account)
    return account

def get_by_id(account_id: UUID) -> Account | None:
    if settings.SINK == "postgres":
        return db_sink.get_by_id(account_id)
    return inventory.get_by_id(account_id)

def list_all() -> list[Account]:
    if settings.SINK == "postgres":
        return db_sink.list_all()
    return inventory.list_all()


def delete_by_id(account_id: UUID) -> bool:
    if settings.SINK == "postgres":
        account = db_sink.get_by_id(account_id)
        deleted = db_sink.delete_by_id(account_id)
        if deleted and account is not None:
            _maybe_emit("d", account)
        return deleted
    existing = inventory.get_by_id(account_id)
    if existing is None:
        return False
    deleted = inventory.delete_by_id(account_id)
    if deleted:
        kafka_producer.produce_delete(existing)
    return deleted