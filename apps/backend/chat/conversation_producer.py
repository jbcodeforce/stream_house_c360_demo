"""Kafka sink for agent conversations.

Each live chat turn is recorded on the ``agent_conversations`` topic as two
Avro records — a ``request`` (the user's message) and a ``response`` (the
agent's reply) — keyed by ``session_id`` and linked by a shared ``turn_id``.
A downstream Flink agent consumes this single stream to classify intents and
aggregate the most common questions users ask the real-time context engine.

Design notes:

* The **topic is owned by Terraform** (``IaC/ccloud/topics.tf``); this module
  only registers the **value schema** at runtime (TopicNameStrategy subject
  ``<topic>-value``). The key is a plain string (the session id), so there is
  no key subject.
* Producing is **best-effort**: ``produce_turn`` never raises, so a broker or
  Schema Registry hiccup degrades to a logged warning instead of breaking the
  chat reply. It is gated by ``CHAT_PRODUCE_CONVERSATIONS`` (off by default).
"""

from __future__ import annotations

import logging
import time
from pathlib import Path
from uuid import uuid4

from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import StringSerializer
from confluent_kafka.serializing_producer import SerializingProducer

from config import settings
from kafka_setup import ensure_schema

logger = logging.getLogger("c360.chat.conversation_producer")

_SCHEMA_PATH = Path(__file__).parent / "schema" / "agent_conversation.avro"
_SCHEMA_STR: str = _SCHEMA_PATH.read_text(encoding="utf-8")

# Event type discriminators carried in the ``type`` field.
_TYPE_REQUEST = "request"
_TYPE_RESPONSE = "response"

# Bounds the per-turn flush so an unreachable broker cannot block the chat
# response indefinitely (librdkafka's default message timeout is 5 minutes).
_FLUSH_TIMEOUT_S = 5.0


# ---------------------------------------------------------------------------
# Value envelope
# ---------------------------------------------------------------------------

def _build_event(
    session_id: str | None,
    turn_id: str,
    event_type: str,
    content: str,
    *,
    model: str | None = None,
    source_mode: str | None = None,
) -> dict:
    """Build one AgentConversationEvent record (``request`` or ``response``)."""
    return {
        "event_id": str(uuid4()),
        "turn_id": turn_id,
        "session_id": session_id,
        "type": event_type,
        "content": content,
        "ts_ms": int(time.time() * 1000),
        "model": model,
        "source_mode": source_mode,
    }


def _event_to_dict(event: dict, ctx) -> dict:  # noqa: ANN001 — confluent callback signature
    """Pass-through to_dict callback for AvroSerializer."""
    return event


# ---------------------------------------------------------------------------
# Module-level lazy producer
# ---------------------------------------------------------------------------

_producer: SerializingProducer | None = None


def _on_delivery(err, msg) -> None:  # noqa: ANN001 — confluent callback signature
    """Delivery-report callback: log failures so they are never silent."""
    if err is not None:
        logger.warning("agent_conversations delivery failed: %s", err)
        return
    logger.debug(
        "agent_conversations DELIVERED to %s [partition %s] @ offset %s",
        msg.topic(), msg.partition(), msg.offset(),
    )


def init_producer() -> SerializingProducer:
    """Initialise a SerializingProducer: string key + Avro value via SR.

    Registers the ``<topic>-value`` subject if missing. The topic itself is
    provisioned by Terraform, so (unlike the CDC producers) this does not call
    ``ensure_topic``.
    """
    logger.info(
        "Initialising agent_conversations producer for %s (bootstrap=%s)",
        settings.KAFKA_TOPIC_AGENT_CONVERSATIONS, settings.KAFKA_BOOTSTRAP_SERVERS,
    )
    sr_client = SchemaRegistryClient(
        {
            "url": settings.SCHEMA_REGISTRY_URL,
            "basic.auth.user.info": (
                f"{settings.SCHEMA_REGISTRY_API_KEY}:"
                f"{settings.SCHEMA_REGISTRY_API_SECRET}"
            ),
        }
    )

    value_serializer = AvroSerializer(
        schema_registry_client=sr_client,
        schema_str=_SCHEMA_STR,
        to_dict=_event_to_dict,
    )

    client_conf: dict = {"bootstrap.servers": settings.KAFKA_BOOTSTRAP_SERVERS}

    # Confluent Cloud requires SASL_SSL. Add auth only when credentials are set
    # so local/test brokers (no auth) still work without config.
    if settings.KAFKA_API_KEY and settings.KAFKA_API_SECRET:
        client_conf.update({
            "security.protocol": "SASL_SSL",
            "sasl.mechanisms": "PLAIN",
            "sasl.username": settings.KAFKA_API_KEY,
            "sasl.password": settings.KAFKA_API_SECRET,
        })

    ensure_schema(
        sr_client,
        f"{settings.KAFKA_TOPIC_AGENT_CONVERSATIONS}-value",
        _SCHEMA_STR,
    )

    return SerializingProducer({
        **client_conf,
        "key.serializer": StringSerializer("utf_8"),
        "value.serializer": value_serializer,
    })


def _get_producer() -> SerializingProducer:
    """Return the module-level producer, initialising it on first call."""
    global _producer
    if _producer is None:
        _producer = init_producer()
    return _producer


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def _produce_event(
    producer: SerializingProducer,
    session_id: str | None,
    turn_id: str,
    event_type: str,
    content: str,
    **kwargs,
) -> None:
    producer.produce(
        topic=settings.KAFKA_TOPIC_AGENT_CONVERSATIONS,
        key=session_id,
        value=_build_event(session_id, turn_id, event_type, content, **kwargs),
        on_delivery=_on_delivery,
    )


def produce_turn(
    session_id: str | None,
    request_text: str,
    response_text: str,
    *,
    model: str | None = None,
    source_mode: str = "live",
) -> None:
    """Emit one chat turn as a linked request + response pair. Best-effort.

    No-op when ``CHAT_PRODUCE_CONVERSATIONS`` is off. Never raises: any Kafka or
    Schema Registry failure is logged and swallowed so the chat reply is unharmed.
    """
    if not settings.CHAT_PRODUCE_CONVERSATIONS:
        return

    turn_id = str(uuid4())
    try:
        producer = _get_producer()
        _produce_event(producer, session_id, turn_id, _TYPE_REQUEST, request_text)
        _produce_event(
            producer, session_id, turn_id, _TYPE_RESPONSE, response_text,
            model=model, source_mode=source_mode,
        )
        remaining = producer.flush(_FLUSH_TIMEOUT_S)
        if remaining:
            logger.warning(
                "agent_conversations flush left %s message(s) unconfirmed (session=%s)",
                remaining, session_id,
            )
    except Exception:
        logger.warning(
            "agent_conversations produce failed for session=%s (best-effort, ignored)",
            session_id, exc_info=True,
        )
