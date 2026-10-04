"""Accounts producer builds a Debezium envelope with a Decimal balance."""

from __future__ import annotations

from datetime import date, datetime, timezone
from decimal import Decimal
from unittest.mock import MagicMock, patch
from uuid import uuid4

from accounts import kafka_producer
from accounts.models import Account


def _account(**over) -> Account:
    now = datetime.now(tz=timezone.utc)
    base = dict(
        account_id=uuid4(), customer_id=uuid4(), account_number="ACC-1",
        account_type="CHECKING", currency="USD", balance=123.45,
        credit_limit=None, opened_date=date(2021, 1, 1), closed_date=None,
        status="ACTIVE", created_at=now, updated_at=now,
    )
    base.update(over)
    return Account(**base)


def test_produce_create_builds_c_event_with_decimal_balance():
    acct = _account(balance=123.45)
    fake = MagicMock()
    with patch.object(kafka_producer, "_get_producer", return_value=fake):
        kafka_producer.produce_create(acct)
    value = fake.produce.call_args.kwargs["value"]
    assert value["op"] == "c"
    assert value["after"]["balance"] == Decimal("123.45")
    assert isinstance(value["after"]["balance"], Decimal)
    assert value["after"]["credit_limit"] is None
    fake.flush.assert_called_once()


def test_produce_delete_puts_value_in_before():
    acct = _account()
    fake = MagicMock()
    with patch.object(kafka_producer, "_get_producer", return_value=fake):
        kafka_producer.produce_delete(acct)
    value = fake.produce.call_args.kwargs["value"]
    assert value["op"] == "d"
    assert value["before"] is not None and value["after"] is None


def test_bounded_flush_is_passed_through():
    acct = _account()
    fake = MagicMock()
    with patch.object(kafka_producer, "_get_producer", return_value=fake):
        kafka_producer.produce_update(acct, flush_timeout=2.5)
    fake.flush.assert_called_once_with(2.5)
