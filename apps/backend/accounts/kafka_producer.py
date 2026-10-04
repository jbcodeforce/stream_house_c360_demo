"""Kafka sink for the accounts service — Debezium change-event envelope."""

from __future__ import annotations

import logging
import time
from datetime import date, timezone
from decimal import Decimal
from pathlib import Path

from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import StringSerializer
from confluent_kafka.serializing_producer import SerializingProducer

from accounts.models import Account
from config import settings

_SCHEMA_PATH = Path(__file__).parent / "schema" / "schema-cdc.public.accounts-value-v1.avsc"
_SCHEMA_STR: str = _SCHEMA_PATH.read_text(encoding="utf-8")

_OP_CREATE = "c"
_OP_UPDATE = "u"
_OP_DELETE = "d"

_SOURCE_CONNECTOR = "c360-backend-bypass"
_SOURCE_DB = "c360db"
_SOURCE_SCHEMA = "public"
_SOURCE_TABLE = "accounts"

_EPOCH = date(1970, 1, 1)

logger = logging.getLogger("c360.accounts.kafka_producer")

_producer: SerializingProducer | None = None


def _to_debezium_date(d: date | None) -> int | None:
    if d is None:
        return None
    return (d - _EPOCH).days


def _to_debezium_ts(dt) -> str:  # noqa: ANN001
    if dt is None:
        return "1970-01-01T00:00:00.000000Z"
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    dt_utc = dt.astimezone(timezone.utc)
    return dt_utc.strftime("%Y-%m-%dT%H:%M:%S.") + f"{dt_utc.microsecond:06d}Z"


def _money(v: float | None) -> Decimal | None:
    if v is None:
        return None
    return Decimal(str(v)).quantize(Decimal("0.01"))


def _build_value(account: Account) -> dict:
    return {
        "account_id": str(account.account_id),
        "customer_id": str(account.customer_id),
        "account_number": account.account_number,
        "account_type": account.account_type,
        "currency": account.currency,
        "balance": _money(account.balance),
        "credit_limit": _money(account.credit_limit),
        "opened_date": _to_debezium_date(account.opened_date),
        "closed_date": _to_debezium_date(account.closed_date),
        "status": account.status,
        "created_at": _to_debezium_ts(account.created_at),
        "updated_at": _to_debezium_ts(account.updated_at),
    }


def _build_source(ts_ms: int) -> dict:
    return {
        "version": "2.0.0.bypass",
        "connector": _SOURCE_CONNECTOR,
        "name": "cdc",
        "ts_ms": ts_ms,
        "snapshot": "false",
        "db": _SOURCE_DB,
        "sequence": None,
        "schema": _SOURCE_SCHEMA,
        "table": _SOURCE_TABLE,
        "txId": None,
        "lsn": None,
        "xmin": None,
    }


def _build_envelope(account: Account, op: str) -> dict:
    ts_ms = int(time.time() * 1000)
    value = _build_value(account)
    before = value if op == _OP_DELETE else None
    after = None if op == _OP_DELETE else value
    return {
        "before": before,
        "after": after,
        "source": _build_source(ts_ms),
        "op": op,
        "ts_ms": ts_ms,
        "transaction": None,
    }


def _envelope_to_dict(envelope: dict, ctx) -> dict:  # noqa: ANN001
    return envelope


def init_producer() -> SerializingProducer:
    sr_client = SchemaRegistryClient(
        {
            "url": settings.SCHEMA_REGISTRY_URL,
            "basic.auth.user.info": (
                f"{settings.SCHEMA_REGISTRY_API_KEY}:{settings.SCHEMA_REGISTRY_API_SECRET}"
            ),
        }
    )
    avro_serializer = AvroSerializer(
        schema_registry_client=sr_client,
        schema_str=_SCHEMA_STR,
        to_dict=_envelope_to_dict,
    )
    return SerializingProducer(
        {
            "bootstrap.servers": settings.KAFKA_BOOTSTRAP_SERVERS,
            "key.serializer": StringSerializer("utf_8"),
            "value.serializer": avro_serializer,
        }
    )


def _get_producer() -> SerializingProducer:
    global _producer
    if _producer is None:
        _producer = init_producer()
    return _producer


def _on_delivery(err, msg) -> None:  # noqa: ANN001
    if err is not None:
        logger.warning("Kafka delivery failed: %s", err)


def _produce(account: Account, op: str, flush_timeout: float | None = None) -> None:
    producer = _get_producer()
    producer.produce(
        topic=settings.KAFKA_TOPIC_ACCOUNTS,
        key=str(account.account_id),
        value=_build_envelope(account, op),
        on_delivery=_on_delivery,
    )
    remaining = producer.flush() if flush_timeout is None else producer.flush(flush_timeout)
    if remaining:
        logger.warning(
            "Kafka flush left %s message(s) unconfirmed (op=%s, id=%s)",
            remaining, op, account.account_id,
        )


def produce_create(account: Account, flush_timeout: float | None = None) -> None:
    _produce(account, _OP_CREATE, flush_timeout)


def produce_update(account: Account, flush_timeout: float | None = None) -> None:
    _produce(account, _OP_UPDATE, flush_timeout)


def produce_delete(account: Account, flush_timeout: float | None = None) -> None:
    _produce(account, _OP_DELETE, flush_timeout)
