"""Idempotent provisioning of Kafka topics and Schema Registry subjects.

Shared by the customers / accounts / transactions producers so each one can
create its topic and its key/value schemas on first use if they are missing.
"""

from __future__ import annotations

import logging

from confluent_kafka import KafkaError, KafkaException
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.schema_registry import Schema, SchemaRegistryClient
from confluent_kafka.schema_registry.error import SchemaRegistryError

logger = logging.getLogger("c360.kafka_setup")

# -1 → let the broker apply its defaults (works on Confluent Cloud and on a
# single-broker local cluster, where a fixed replication factor of 3 would fail).
_DEFAULT_PARTITIONS = 6
_DEFAULT_REPLICATION = -1
_ADMIN_TIMEOUT_S = 30.0


def ensure_topic(
    client_conf: dict,
    topic: str,
    num_partitions: int = _DEFAULT_PARTITIONS,
    replication_factor: int = _DEFAULT_REPLICATION,
) -> None:
    """Create ``topic`` if it does not exist. Safe to call repeatedly."""
    admin = AdminClient(client_conf)
    futures = admin.create_topics(
        [NewTopic(topic, num_partitions=num_partitions, replication_factor=replication_factor)]
    )
    try:
        futures[topic].result(timeout=_ADMIN_TIMEOUT_S)
        logger.info(
            "[bypass] CREATED Kafka topic %s (partitions=%s) — it did not exist",
            topic, num_partitions,
        )
    except KafkaException as exc:
        if exc.args[0].code() == KafkaError.TOPIC_ALREADY_EXISTS:
            logger.info("[bypass] Kafka topic %s already exists — nothing to create", topic)
            return
        raise


def ensure_schema(sr_client: SchemaRegistryClient, subject: str, schema_str: str) -> None:
    """Register ``schema_str`` (Avro) under ``subject`` if it is not registered yet."""
    schema = Schema(schema_str, "AVRO")
    try:
        existing = sr_client.lookup_schema(subject, schema)
        logger.info(
            "[bypass] Schema subject %s already registered (id=%s) — nothing to register",
            subject, getattr(existing, "schema_id", "?"),
        )
        return
    except SchemaRegistryError as exc:
        if exc.http_status_code != 404:
            raise
    schema_id = sr_client.register_schema(subject, schema)
    logger.info(
        "[bypass] REGISTERED schema subject %s (id=%s) — it did not exist",
        subject, schema_id,
    )


def ensure_kafka_resources(
    client_conf: dict,
    sr_client: SchemaRegistryClient,
    topic: str,
    key_schema_str: str,
    value_schema_str: str,
) -> None:
    """Ensure the topic and its ``<topic>-key`` / ``<topic>-value`` subjects exist."""
    logger.info(
        "[bypass] Checking topic and schemas for %s (backend produces directly to Kafka)",
        topic,
    )
    ensure_topic(client_conf, topic)
    ensure_schema(sr_client, f"{topic}-key", key_schema_str)
    ensure_schema(sr_client, f"{topic}-value", value_schema_str)
