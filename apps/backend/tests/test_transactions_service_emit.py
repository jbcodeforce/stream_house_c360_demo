"""Transactions emission is config-gated, best-effort, and bounded."""

from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import MagicMock
from uuid import uuid4

import pytest

from config_store import AppConfig
from transactions import service
from transactions.models import Transaction, TransactionCreate


def _txn() -> Transaction:
    now = datetime.now(tz=timezone.utc)
    return Transaction(
        transaction_id=uuid4(), account_id=uuid4(), customer_id=uuid4(),
        transaction_type="CREDIT", amount=10.0, currency="USD", status="COMPLETED",
        transacted_at=now, posted_at=None, created_at=now,
    )


def _payload():
    return TransactionCreate(
        account_id=uuid4(), customer_id=uuid4(), transaction_type="CREDIT", amount=10.0,
    )


@pytest.fixture
def svc(monkeypatch):
    monkeypatch.setattr(service.db_sink, "create", MagicMock())
    monkeypatch.setattr(service.kafka_producer, "produce_create", MagicMock())
    return service


def _enable(monkeypatch, enabled):
    monkeypatch.setattr(
        service.config_store, "get_config",
        lambda: AppConfig(kafka_produce_enabled=enabled),
    )


def test_create_emits_when_enabled(svc, monkeypatch):
    t = _txn()
    svc.db_sink.create.return_value = t
    _enable(monkeypatch, True)
    svc.create(_payload())
    svc.kafka_producer.produce_create.assert_called_once()
    assert svc.kafka_producer.produce_create.call_args.args[0] is t
    assert svc.kafka_producer.produce_create.call_args.kwargs["flush_timeout"] > 0


def test_create_does_not_emit_when_disabled(svc, monkeypatch):
    svc.db_sink.create.return_value = _txn()
    _enable(monkeypatch, False)
    svc.create(_payload())
    svc.kafka_producer.produce_create.assert_not_called()


def test_cdc_connector_suppresses_emit_despite_runtime_toggle(svc, monkeypatch):
    # RDS / remote PG: the CDC connector owns Kafka, so the app must not emit
    # even when the runtime toggle is ON.
    svc.db_sink.create.return_value = _txn()
    _enable(monkeypatch, True)
    monkeypatch.setattr(svc.settings, "CDC_CONNECTOR_ENABLED", True)
    svc.create(_payload())
    svc.kafka_producer.produce_create.assert_not_called()


def test_create_still_succeeds_when_producer_raises(svc, monkeypatch):
    t = _txn()
    svc.db_sink.create.return_value = t
    _enable(monkeypatch, True)
    svc.kafka_producer.produce_create.side_effect = RuntimeError("broker down")
    assert svc.create(_payload()) is t
