"""Kafka sink for the customers service.

Produces Avro-serialised customer events using the Debezium change-event
envelope schema (`cdc.public.customers.Envelope`), matching exactly what
the Debezium PostgreSQL connector emits.  This lets downstream consumers
(Flink, ksqlDB, Tableflow) treat bypass-mode events identically to CDC
events from the real connector.

Schema is loaded from ``customers/schema/customer.avro`` — the file
downloaded directly from the Schema Registry.
"""

from __future__ import annotations

import logging
import time
from datetime import date, timezone
from pathlib import Path

from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serializing_producer import SerializingProducer
from confluent_kafka.serialization import StringSerializer

from config import settings
from customers.models import Customer

# ---------------------------------------------------------------------------
# Schema — loaded from the file downloaded from Schema Registry
# ---------------------------------------------------------------------------

_SCHEMA_PATH = Path(__file__).parent / "schema" / "customer.avro"
_SCHEMA_STR: str = _SCHEMA_PATH.read_text(encoding="utf-8")

# Debezium op codes (lowercase, matching CDC connector output)
_OP_CREATE = "c"
_OP_UPDATE = "u"
_OP_DELETE = "d"

# Synthetic source block — identifies this as a bypass producer, not real CDC
_SOURCE_CONNECTOR = "c360-backend-bypass"
_SOURCE_DB = "c360db"
_SOURCE_SCHEMA = "public"
_SOURCE_TABLE = "customers"

# ---------------------------------------------------------------------------
# Date encoding helpers
# Debezium io.debezium.time.Date → int (days since Unix epoch 1970-01-01)
# Debezium io.debezium.time.ZonedTimestamp → ISO-8601 string
# ---------------------------------------------------------------------------

_EPOCH = date(1970, 1, 1)


def _to_debezium_date(d: date | None) -> int | None:
    """Convert a Python date to days-since-epoch (Debezium Date encoding)."""
    if d is None:
        return None
    return (d - _EPOCH).days


def _to_debezium_ts(dt) -> str:
    """Convert a datetime to Debezium ZonedTimestamp format.

    Debezium emits timestamps as ``"YYYY-MM-DDTHH:MM:SS.ffffffZ"`` strings.
    """
    if dt is None:
        return "1970-01-01T00:00:00.000000Z"
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    # Normalise to UTC and format to microseconds
    dt_utc = dt.astimezone(timezone.utc)
    return dt_utc.strftime("%Y-%m-%dT%H:%M:%S.") + f"{dt_utc.microsecond:06d}Z"


# ---------------------------------------------------------------------------
# Payload builders
# ---------------------------------------------------------------------------

def _build_value(customer: Customer) -> dict:
    """Build a Debezium Value record from a Customer."""
    return {
        "customer_id": str(customer.customer_id),
        "first_name": customer.first_name,
        "last_name": customer.last_name,
        "email": customer.email,
        "phone": customer.phone,
        "date_of_birth": _to_debezium_date(
            customer.date_of_birth.date()
            if customer.date_of_birth is not None
            else None
        ),
        "gender": customer.gender,
        "address_line1": customer.address_line1,
        "address_line2": customer.address_line2,
        "city": customer.city,
        "state": customer.state,
        "postal_code": customer.postal_code,
        "country": customer.country,
        "customer_since": _to_debezium_date(customer.customer_since),
        "segment": customer.segment,
        "status": customer.status,
        "created_at": _to_debezium_ts(customer.created_at),
        "updated_at": _to_debezium_ts(customer.updated_at),
    }


def _build_source(ts_ms: int) -> dict:
    """Build a synthetic Debezium Source block."""
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


def _build_envelope(customer: Customer, op: str) -> dict:
    """Build a full Debezium Envelope message.

    - ``op="c"`` (create): before=null, after=Value
    - ``op="u"`` (update): before=null (we don't track the old row), after=Value
    - ``op="d"`` (delete): before=Value, after=null
    """
    ts_ms = int(time.time() * 1000)
    value = _build_value(customer)

    before = None
    after = None

    if op == _OP_DELETE:
        before = value
    else:
        after = value

    return {
        "before": before,
        "after": after,
        "source": _build_source(ts_ms),
        "op": op,
        "ts_ms": ts_ms,
        "transaction": None,
    }


def _envelope_to_dict(envelope: dict, ctx) -> dict:  # noqa: ANN001
    """Pass-through to_dict callback for AvroSerializer."""
    return envelope


# ---------------------------------------------------------------------------
# Module-level lazy producer
# ---------------------------------------------------------------------------

logger = logging.getLogger("c360.kafka_producer")

_producer: SerializingProducer | None = None


def _on_delivery(err, msg) -> None:  # noqa: ANN001 — confluent callback signature
    """Delivery-report callback: log failures so they are never silent."""
    if err is not None:
        logger.warning("Kafka delivery failed: %s", err)


def init_producer() -> SerializingProducer:
    """Initialise and return a SerializingProducer backed by Avro + SR."""
    sr_client = SchemaRegistryClient(
        {
            "url": settings.SCHEMA_REGISTRY_URL,
            "basic.auth.user.info": (
                f"{settings.SCHEMA_REGISTRY_API_KEY}:"
                f"{settings.SCHEMA_REGISTRY_API_SECRET}"
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
    """Return the module-level producer, initialising it on first call."""
    global _producer
    if _producer is None:
        _producer = init_producer()
    return _producer


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def _produce(customer: Customer, op: str, flush_timeout: float | None = None) -> None:
    """Produce a Debezium change event and flush.

    ``flush_timeout`` bounds the flush so a reachable Schema Registry with an
    unreachable broker cannot block the caller indefinitely (librdkafka's
    default message timeout is 5 minutes). When ``None`` the flush is
    unbounded, preserving the SINK=kafka path's at-least-once semantics.
    """
    producer = _get_producer()
    producer.produce(
        topic=settings.KAFKA_TOPIC_CUSTOMERS,
        key=str(customer.customer_id),
        value=_build_envelope(customer, op),
        on_delivery=_on_delivery,
    )
    remaining = producer.flush() if flush_timeout is None else producer.flush(flush_timeout)
    if remaining:
        logger.warning(
            "Kafka flush left %s message(s) unconfirmed (op=%s, id=%s)",
            remaining, op, customer.customer_id,
        )


def produce_create(customer: Customer, flush_timeout: float | None = None) -> None:
    """Produce a Debezium create event (op="c") to the Kafka topic."""
    _produce(customer, _OP_CREATE, flush_timeout)


def produce_update(customer: Customer, flush_timeout: float | None = None) -> None:
    """Produce a Debezium update event (op="u") to the Kafka topic."""
    _produce(customer, _OP_UPDATE, flush_timeout)


def produce_delete(customer: Customer, flush_timeout: float | None = None) -> None:
    """Produce a Debezium delete event (op="d") to the Kafka topic."""
    _produce(customer, _OP_DELETE, flush_timeout)
