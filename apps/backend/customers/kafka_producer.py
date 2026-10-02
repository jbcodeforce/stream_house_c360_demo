"""Kafka sink for the customers service.

Produces Avro-serialised customer events to a Confluent Kafka topic via
Confluent Schema Registry.  The producer is created lazily on first use
via ``_get_producer()``.
"""

from __future__ import annotations

from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serializing_producer import SerializingProducer
from confluent_kafka.serialization import StringSerializer

from config import settings
from customers.models import Customer

# ---------------------------------------------------------------------------
# Avro schema
# Derived from the Customer model fields.  UUID/datetime/date fields are
# serialised as Avro strings.  Optional fields use a ["null", "string"]
# union with a default of null.
# ---------------------------------------------------------------------------

CUSTOMER_AVRO_SCHEMA = """
{
  "type": "record",
  "name": "Customer",
  "namespace": "com.c360.customers",
  "fields": [
    {"name": "op",              "type": "string"},
    {"name": "customer_id",     "type": "string"},
    {"name": "first_name",      "type": "string"},
    {"name": "last_name",       "type": "string"},
    {"name": "email",           "type": "string"},
    {"name": "phone",           "type": ["null", "string"], "default": null},
    {"name": "date_of_birth",   "type": ["null", "string"], "default": null},
    {"name": "gender",          "type": ["null", "string"], "default": null},
    {"name": "address_line1",   "type": ["null", "string"], "default": null},
    {"name": "address_line2",   "type": ["null", "string"], "default": null},
    {"name": "city",            "type": ["null", "string"], "default": null},
    {"name": "state",           "type": ["null", "string"], "default": null},
    {"name": "postal_code",     "type": ["null", "string"], "default": null},
    {"name": "country",         "type": "string"},
    {"name": "customer_since",  "type": "string"},
    {"name": "segment",         "type": ["null", "string"], "default": null},
    {"name": "status",          "type": "string"},
    {"name": "created_at",      "type": "string"},
    {"name": "updated_at",      "type": "string"}
  ]
}
"""

# ---------------------------------------------------------------------------
# Module-level lazy producer
# ---------------------------------------------------------------------------

_producer: SerializingProducer | None = None


def _customer_to_dict(customer: dict, ctx) -> dict:  # noqa: ANN001
    """Convert a Customer payload dict to a plain Avro-compatible dict.

    This is the ``to_dict`` callback passed to ``AvroSerializer``.  The
    ``customer`` argument is already a plain dict built by ``_build_payload``
    so no further transformation is needed; the function signature matches
    what ``AvroSerializer`` expects (value, SerializationContext).
    """
    return customer


def init_producer() -> SerializingProducer:
    """Initialise and return a ``SerializingProducer`` backed by Avro + SR."""
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
        schema_str=CUSTOMER_AVRO_SCHEMA,
        to_dict=_customer_to_dict,
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


def _build_payload(customer: Customer, op: str) -> dict:
    """Build the Avro payload dict from a ``Customer`` instance."""
    return {
        "op": op,
        "customer_id": str(customer.customer_id),
        "first_name": customer.first_name,
        "last_name": customer.last_name,
        "email": customer.email,
        "phone": customer.phone,
        "date_of_birth": (
            customer.date_of_birth.isoformat() if customer.date_of_birth else None
        ),
        "gender": customer.gender,
        "address_line1": customer.address_line1,
        "address_line2": customer.address_line2,
        "city": customer.city,
        "state": customer.state,
        "postal_code": customer.postal_code,
        "country": customer.country,
        "customer_since": customer.customer_since.isoformat(),
        "segment": customer.segment,
        "status": customer.status,
        "created_at": customer.created_at.isoformat(),
        "updated_at": customer.updated_at.isoformat(),
    }


def produce_create(customer: Customer) -> None:
    """Produce a customer-created event (``op="C"``) to the Kafka topic."""
    producer = _get_producer()
    producer.produce(
        topic=settings.KAFKA_TOPIC_CUSTOMERS,
        key=str(customer.customer_id),
        value=_build_payload(customer, "C"),
    )
    producer.flush()


def produce_update(customer: Customer) -> None:
    """Produce a customer-updated event (``op="U"``) to the Kafka topic."""
    producer = _get_producer()
    producer.produce(
        topic=settings.KAFKA_TOPIC_CUSTOMERS,
        key=str(customer.customer_id),
        value=_build_payload(customer, "U"),
    )
    producer.flush()
