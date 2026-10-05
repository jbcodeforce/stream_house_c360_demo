"""Transactions producer builds a Debezium envelope with a Decimal amount."""

from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
from unittest.mock import MagicMock, patch
from uuid import uuid4

from transactions import kafka_producer
from transactions.models import Transaction


def _txn(**over) -> Transaction:
    now = datetime.now(tz=timezone.utc)
    base = dict(
        transaction_id=uuid4(), account_id=uuid4(), customer_id=uuid4(),
        transaction_type="CREDIT", amount=99.9, currency="USD", description="x",
        merchant_name=None, merchant_category=None, channel="ONLINE",
        status="COMPLETED", reference_id="REF-1", transacted_at=now,
        posted_at=None, created_at=now,
    )
    base.update(over)
    return Transaction(**base)


def test_produce_create_builds_c_event_with_decimal_amount():
    txn = _txn(amount=99.9)
    fake = MagicMock()
    with patch.object(kafka_producer, "_get_producer", return_value=fake):
        kafka_producer.produce_create(txn)
    value = fake.produce.call_args.kwargs["value"]
    assert value["op"] == "c"
    assert value["after"]["amount"] == Decimal("99.90")
    assert isinstance(value["after"]["amount"], Decimal)
    assert value["after"]["posted_at"] is None
    fake.flush.assert_called_once()


def test_bounded_flush_is_passed_through():
    fake = MagicMock()
    with patch.object(kafka_producer, "_get_producer", return_value=fake):
        kafka_producer.produce_create(_txn(), flush_timeout=2.5)
    fake.flush.assert_called_once_with(2.5)
