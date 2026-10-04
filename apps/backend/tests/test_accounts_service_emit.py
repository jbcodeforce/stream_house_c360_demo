"""Accounts emission is config-gated, best-effort, and bounded."""

from __future__ import annotations

from datetime import date, datetime, timezone
from unittest.mock import MagicMock
from uuid import uuid4

import pytest

from accounts import service
from accounts.models import Account, AccountCreate, AccountUpdate
from config_store import AppConfig


def _account() -> Account:
    now = datetime.now(tz=timezone.utc)
    return Account(
        account_id=uuid4(), customer_id=uuid4(), account_number="ACC-1",
        account_type="CHECKING", currency="USD", balance=10.0, credit_limit=None,
        opened_date=date(2021, 1, 1), closed_date=None, status="ACTIVE",
        created_at=now, updated_at=now,
    )


@pytest.fixture
def svc(monkeypatch):
    monkeypatch.setattr(service.db_sink, "create", MagicMock())
    monkeypatch.setattr(service.db_sink, "update", MagicMock())
    monkeypatch.setattr(service.db_sink, "get_by_id", MagicMock())
    monkeypatch.setattr(service.db_sink, "delete_by_id", MagicMock())
    monkeypatch.setattr(service.kafka_producer, "produce_create", MagicMock())
    monkeypatch.setattr(service.kafka_producer, "produce_update", MagicMock())
    monkeypatch.setattr(service.kafka_producer, "produce_delete", MagicMock())
    return service


def _enable(monkeypatch, enabled: bool):
    monkeypatch.setattr(
        service.config_store, "get_config",
        lambda: AppConfig(kafka_produce_enabled=enabled),
    )


def _create_payload():
    return AccountCreate(
        customer_id=uuid4(), account_number="ACC-1", account_type="CHECKING",
    )


def test_create_emits_when_enabled(svc, monkeypatch):
    a = _account()
    svc.db_sink.create.return_value = a
    _enable(monkeypatch, True)
    svc.create(_create_payload())
    svc.kafka_producer.produce_create.assert_called_once()
    assert svc.kafka_producer.produce_create.call_args.args[0] is a
    assert svc.kafka_producer.produce_create.call_args.kwargs["flush_timeout"] > 0


def test_create_does_not_emit_when_disabled(svc, monkeypatch):
    svc.db_sink.create.return_value = _account()
    _enable(monkeypatch, False)
    svc.create(_create_payload())
    svc.kafka_producer.produce_create.assert_not_called()


def test_update_skips_emit_when_no_row(svc, monkeypatch):
    svc.db_sink.update.return_value = None
    _enable(monkeypatch, True)
    assert svc.update(uuid4(), AccountUpdate(status="CLOSED")) is None
    svc.kafka_producer.produce_update.assert_not_called()


def test_delete_emits_before_deleting(svc, monkeypatch):
    a = _account()
    svc.db_sink.get_by_id.return_value = a
    svc.db_sink.delete_by_id.return_value = True
    _enable(monkeypatch, True)
    assert svc.delete_by_id(a.account_id) is True
    svc.kafka_producer.produce_delete.assert_called_once()
    assert svc.kafka_producer.produce_delete.call_args.args[0] is a


def test_create_still_succeeds_when_producer_raises(svc, monkeypatch):
    a = _account()
    svc.db_sink.create.return_value = a
    _enable(monkeypatch, True)
    svc.kafka_producer.produce_create.side_effect = RuntimeError("broker down")
    assert svc.create(_create_payload()) is a
