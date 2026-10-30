"""Unit tests for the agent-conversations Kafka producer (no infra required).

The producer emits two Avro records per chat turn — a ``request`` and a
``response`` — to the ``agent_conversations`` topic, keyed by ``session_id`` and
linked by a shared ``turn_id`` so a downstream Flink agent can reunite the pair.
These tests mock the confluent SerializingProducer at the boundary, so no broker
or Schema Registry is touched.

Seam: chat.conversation_producer public API.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

import config
from chat import conversation_producer as cp


@pytest.fixture(autouse=True)
def _enable_and_name(monkeypatch):
    """Enable producing and pin a known topic name for assertions."""
    monkeypatch.setattr(config.settings, "CHAT_PRODUCE_CONVERSATIONS", True)
    monkeypatch.setattr(config.settings, "KAFKA_TOPIC_AGENT_CONVERSATIONS", "agent_conversations")


# ---------------------------------------------------------------------------
# _build_event — value envelope shape
# ---------------------------------------------------------------------------

class TestBuildEvent:
    def test_request_event_has_core_fields(self):
        ev = cp._build_event("sess-1", "turn-1", "request", "Who is customer 42?")
        assert ev["session_id"] == "sess-1"
        assert ev["turn_id"] == "turn-1"
        assert ev["type"] == "request"
        assert ev["content"] == "Who is customer 42?"
        assert isinstance(ev["ts_ms"], int)
        assert ev["event_id"]  # non-empty uuid

    def test_request_event_has_null_model_and_source_mode_by_default(self):
        ev = cp._build_event("sess-1", "turn-1", "request", "hi")
        assert ev["model"] is None
        assert ev["source_mode"] is None

    def test_response_event_carries_model_and_source_mode(self):
        ev = cp._build_event(
            "sess-1", "turn-1", "response", "Jane Doe, RETAIL.",
            model="Ornith-1.5", source_mode="live",
        )
        assert ev["type"] == "response"
        assert ev["model"] == "Ornith-1.5"
        assert ev["source_mode"] == "live"

    def test_event_ids_are_unique_per_event(self):
        a = cp._build_event("s", "t", "request", "x")
        b = cp._build_event("s", "t", "response", "y")
        assert a["event_id"] != b["event_id"]


# ---------------------------------------------------------------------------
# produce_turn — two events sharing a turn_id
# ---------------------------------------------------------------------------

class TestProduceTurn:
    @pytest.fixture
    def producer(self, monkeypatch):
        mock = MagicMock()
        monkeypatch.setattr(cp, "_get_producer", lambda: mock)
        return mock

    def _values(self, producer) -> list[dict]:
        """The ``value=`` dicts from each produce() call, in order."""
        return [c.kwargs["value"] for c in producer.produce.call_args_list]

    def test_emits_exactly_two_records(self, producer):
        cp.produce_turn("sess-1", "my question", "my answer")
        assert producer.produce.call_count == 2

    def test_first_is_request_second_is_response(self, producer):
        cp.produce_turn("sess-1", "my question", "my answer")
        req, resp = self._values(producer)
        assert req["type"] == "request"
        assert req["content"] == "my question"
        assert resp["type"] == "response"
        assert resp["content"] == "my answer"

    def test_pair_shares_one_turn_id(self, producer):
        cp.produce_turn("sess-1", "q", "a")
        req, resp = self._values(producer)
        assert req["turn_id"] == resp["turn_id"]

    def test_events_have_distinct_event_ids(self, producer):
        cp.produce_turn("sess-1", "q", "a")
        req, resp = self._values(producer)
        assert req["event_id"] != resp["event_id"]

    def test_both_records_keyed_by_session_id(self, producer):
        cp.produce_turn("sess-xyz", "q", "a")
        for call in producer.produce.call_args_list:
            assert call.kwargs["key"] == "sess-xyz"

    def test_records_go_to_the_configured_topic(self, producer):
        cp.produce_turn("sess-1", "q", "a")
        for call in producer.produce.call_args_list:
            assert call.kwargs["topic"] == "agent_conversations"

    def test_response_record_carries_model_and_source_mode(self, producer):
        cp.produce_turn("sess-1", "q", "a", model="Ornith-1.5", source_mode="live")
        _req, resp = self._values(producer)
        assert resp["model"] == "Ornith-1.5"
        assert resp["source_mode"] == "live"

    def test_flushes_after_producing(self, producer):
        cp.produce_turn("sess-1", "q", "a")
        producer.flush.assert_called_once()


# ---------------------------------------------------------------------------
# Enablement + best-effort behaviour
# ---------------------------------------------------------------------------

class TestEnablementAndResilience:
    def test_disabled_does_not_touch_the_producer(self, monkeypatch):
        monkeypatch.setattr(config.settings, "CHAT_PRODUCE_CONVERSATIONS", False)
        sentinel = MagicMock(side_effect=AssertionError("producer must not be built"))
        monkeypatch.setattr(cp, "_get_producer", sentinel)
        cp.produce_turn("sess-1", "q", "a")  # must be a no-op
        sentinel.assert_not_called()

    def test_producer_failure_is_swallowed(self, monkeypatch):
        boom = MagicMock(side_effect=RuntimeError("broker unreachable"))
        monkeypatch.setattr(cp, "_get_producer", boom)
        # best-effort: a Kafka failure must never break the chat reply
        cp.produce_turn("sess-1", "q", "a")
