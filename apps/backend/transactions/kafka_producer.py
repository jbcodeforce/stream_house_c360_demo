"""Kafka sink for the transactions service — Debezium change-event envelope."""

from __future__ import annotations

import logging
import time
from datetime import timezone
from decimal import Decimal
from pathlib import Path

from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serializing_producer import SerializingProducer

from config import settings
from kafka_setup import ensure_kafka_resources
from transactions.models import Transaction

_SCHEMA_PATH = Path(__file__).parent / "schema" / "schema-cdc.public.transactions-value-v1.avsc"
_SCHEMA_STR: str = _SCHEMA_PATH.read_text(encoding="utf-8")

# Key schema (`cdc.public.transactions.Key`) — Avro record, as Debezium emits it
_KEY_SCHEMA_PATH = Path(__file__).parent / "schema" / "schema-cdc.public.transactions-key-v1.avsc"
_KEY_SCHEMA_STR: str = _KEY_SCHEMA_PATH.read_text(encoding="utf-8")

_OP_CREATE = "c"

_SOURCE_CONNECTOR = "c360-backend-bypass"
_SOURCE_DB = "c360db"
_SOURCE_SCHEMA = "public"
_SOURCE_TABLE = "transactions"

logger = logging.getLogger("c360.transactions.kafka_producer")

_producer: SerializingProducer | None = None


def _to_debezium_ts(dt) -> str | None:  # noqa: ANN001
    if dt is None:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    dt_utc = dt.astimezone(timezone.utc)
    return dt_utc.strftime("%Y-%m-%dT%H:%M:%S.") + f"{dt_utc.microsecond:06d}Z"


def _money(v: float | None) -> Decimal | None:
    if v is None:
        return None
    return Decimal(str(v)).quantize(Decimal("0.01"))


def _build_value(txn: Transaction) -> dict:
    return {
        "transaction_id": str(txn.transaction_id),
        "account_id": str(txn.account_id),
        "customer_id": str(txn.customer_id),
        "transaction_type": txn.transaction_type,
        "amount": _money(txn.amount),
        "currency": txn.currency,
        "description": txn.description,
        "merchant_name": txn.merchant_name,
        "merchant_category": txn.merchant_category,
        "channel": txn.channel,
        "status": txn.status,
        "transacted_at": _to_debezium_ts(txn.transacted_at),
        "posted_at": _to_debezium_ts(txn.posted_at),
        "created_at": _to_debezium_ts(txn.created_at),
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


def _build_key(txn: Transaction) -> dict:
    return {"transaction_id": str(txn.transaction_id)}


def _build_envelope(txn: Transaction, op: str) -> dict:
    ts_ms = int(time.time() * 1000)
    value = _build_value(txn)
    return {
        "before": None,
        "after": value,
        "source": _build_source(ts_ms),
        "op": op,
        "ts_ms": ts_ms,
        "transaction": None,
    }


def _envelope_to_dict(envelope: dict, ctx) -> dict:  # noqa: ANN001
    return envelope


def init_producer() -> SerializingProducer:
    logger.info(
        "[bypass] Initialising Kafka producer for %s (bootstrap=%s)",
        settings.KAFKA_TOPIC_TRANSACTIONS, settings.KAFKA_BOOTSTRAP_SERVERS,
    )
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
    key_serializer = AvroSerializer(
        schema_registry_client=sr_client,
        schema_str=_KEY_SCHEMA_STR,
        to_dict=_envelope_to_dict,
    )
    client_conf: dict = {"bootstrap.servers": settings.KAFKA_BOOTSTRAP_SERVERS}
    if settings.KAFKA_API_KEY and settings.KAFKA_API_SECRET:
        client_conf.update({
            "security.protocol": "SASL_SSL",
            "sasl.mechanisms": "PLAIN",
            "sasl.username": settings.KAFKA_API_KEY,
            "sasl.password": settings.KAFKA_API_SECRET,
        })
    ensure_kafka_resources(
        client_conf, sr_client, settings.KAFKA_TOPIC_TRANSACTIONS,
        _KEY_SCHEMA_STR, _SCHEMA_STR,
    )
    return SerializingProducer({
        **client_conf,
        "key.serializer": key_serializer,
        "value.serializer": avro_serializer,
    })


def _get_producer() -> SerializingProducer:
    global _producer
    if _producer is None:
        _producer = init_producer()
    return _producer


def _on_delivery(err, msg) -> None:  # noqa: ANN001
    if err is not None:
        logger.warning("Kafka delivery failed: %s", err)
        return
    logger.info(
        "[bypass] DELIVERED to %s [partition %s] @ offset %s",
        msg.topic(), msg.partition(), msg.offset(),
    )


def produce_create(txn: Transaction, flush_timeout: float | None = None) -> None:
    """Produce a Debezium create event (op="c") to the transactions topic."""
    producer = _get_producer()
    logger.info(
        "[bypass] Sending transaction op=c id=%s directly to Kafka topic %s (no CDC connector)",
        txn.transaction_id, settings.KAFKA_TOPIC_TRANSACTIONS,
    )
    producer.produce(
        topic=settings.KAFKA_TOPIC_TRANSACTIONS,
        key=_build_key(txn),
        value=_build_envelope(txn, _OP_CREATE),
        on_delivery=_on_delivery,
    )
    remaining = producer.flush() if flush_timeout is None else producer.flush(flush_timeout)
    if remaining:
        logger.warning(
            "Kafka flush left %s message(s) unconfirmed (op=c, id=%s)",
            remaining, txn.transaction_id,
        )


def produce_create_many(txns: list[Transaction], flush_timeout: float | None = None) -> None:
    """Produce a create event per transaction, flushing once at the end."""
    producer = _get_producer()
    for txn in txns:
        producer.produce(
            topic=settings.KAFKA_TOPIC_TRANSACTIONS,
            key=_build_key(txn),
            value=_build_envelope(txn, _OP_CREATE),
            on_delivery=_on_delivery,
        )
        producer.poll(0)
    remaining = producer.flush() if flush_timeout is None else producer.flush(flush_timeout)
    if remaining:
        logger.warning(
            "Kafka flush left %s of %s message(s) unconfirmed (op=c, batch)",
            remaining, len(txns),
        )
