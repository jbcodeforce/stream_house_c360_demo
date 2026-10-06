"""Postgres-path dual-write emission is gated by the config flag.

All collaborators are patched on the service module's own references, so no
module reimport or SINK env juggling is needed.
"""

from __future__ import annotations

from datetime import date, datetime, timezone
from unittest.mock import MagicMock
from uuid import uuid4

import pytest

from customers import service
from customers.models import Customer, CustomerCreate, CustomerUpdate
from config_store import AppConfig


def _customer() -> Customer:
    now = datetime.now(tz=timezone.utc)
    return Customer(
        customer_id=uuid4(), first_name="Ada", last_name="Lovelace",
        email="ada@x.io", country="US", customer_since=date(2020, 1, 1),
        status="ACTIVE", created_at=now, updated_at=now,
    )


@pytest.fixture
def pg(monkeypatch):
    """Force the postgres path and stub the DB + producer on service."""
    monkeypatch.setattr(service.settings, "SINK", "postgres")
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


def test_create_emits_when_enabled(pg, monkeypatch):
    c = _customer()
    pg.db_sink.create.return_value = c
    _enable(monkeypatch, True)
    pg.create(CustomerCreate(first_name="Ada", last_name="L", email="a@b.c"))
    pg.kafka_producer.produce_create.assert_called_once()
    assert pg.kafka_producer.produce_create.call_args.args[0] is c


def test_create_does_not_emit_when_disabled(pg, monkeypatch):
    pg.db_sink.create.return_value = _customer()
    _enable(monkeypatch, False)
    pg.create(CustomerCreate(first_name="Ada", last_name="L", email="a@b.c"))
    pg.kafka_producer.produce_create.assert_not_called()


def test_cdc_connector_suppresses_emit_despite_runtime_toggle(pg, monkeypatch):
    # When a CDC connector owns Kafka (RDS / remote PG), the app must never
    # dual-write — even if the runtime toggle was left ON — to avoid double
    # publishing to cdc.public.*.
    pg.db_sink.create.return_value = _customer()
    pg.db_sink.get_by_id.return_value = _customer()
    pg.db_sink.delete_by_id.return_value = True
    pg.db_sink.update.return_value = _customer()
    _enable(monkeypatch, True)
    monkeypatch.setattr(pg.settings, "CDC_CONNECTOR_ENABLED", True)

    pg.create(CustomerCreate(first_name="Ada", last_name="L", email="a@b.c"))
    pg.update(uuid4(), CustomerUpdate(city="NYC"))
    pg.delete_by_id(uuid4())

    pg.kafka_producer.produce_create.assert_not_called()
    pg.kafka_producer.produce_update.assert_not_called()
    pg.kafka_producer.produce_delete.assert_not_called()


def test_update_skips_emit_when_no_row(pg, monkeypatch):
    pg.db_sink.update.return_value = None
    _enable(monkeypatch, True)
    result = pg.update(uuid4(), CustomerUpdate(city="NYC"))
    assert result is None
    pg.kafka_producer.produce_update.assert_not_called()


def test_delete_emits_full_payload_when_enabled(pg, monkeypatch):
    c = _customer()
    pg.db_sink.get_by_id.return_value = c
    pg.db_sink.delete_by_id.return_value = True
    _enable(monkeypatch, True)
    assert pg.delete_by_id(c.customer_id) is True
    pg.kafka_producer.produce_delete.assert_called_once()
    assert pg.kafka_producer.produce_delete.call_args.args[0] is c


def test_emit_uses_bounded_flush_timeout(pg, monkeypatch):
    # The emission must pass a positive, bounded flush timeout so a reachable
    # Schema Registry + unreachable broker cannot hang the request on flush().
    pg.db_sink.create.return_value = _customer()
    _enable(monkeypatch, True)
    pg.create(CustomerCreate(first_name="Ada", last_name="L", email="a@b.c"))
    kwargs = pg.kafka_producer.produce_create.call_args.kwargs
    assert kwargs.get("flush_timeout") is not None
    assert kwargs["flush_timeout"] > 0


def test_create_still_succeeds_when_producer_raises(pg, monkeypatch):
    c = _customer()
    pg.db_sink.create.return_value = c
    _enable(monkeypatch, True)
    pg.kafka_producer.produce_create.side_effect = RuntimeError("broker down")
    # Best-effort: the DB result is returned despite the producer failing.
    assert pg.create(CustomerCreate(first_name="Ada", last_name="L", email="a@b.c")) is c
