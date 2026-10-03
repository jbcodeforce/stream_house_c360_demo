"""produce_delete builds a D event and produces+flushes via the producer."""

from __future__ import annotations

from datetime import date, datetime, timezone
from unittest.mock import MagicMock, patch
from uuid import uuid4

from customers import kafka_producer
from customers.models import Customer


def _customer() -> Customer:
    now = datetime.now(tz=timezone.utc)
    return Customer(
        customer_id=uuid4(),
        first_name="Ada",
        last_name="Lovelace",
        email="ada@x.io",
        country="US",
        customer_since=date(2020, 1, 1),
        status="ACTIVE",
        created_at=now,
        updated_at=now,
    )


def test_produce_delete_produces_d_event():
    customer = _customer()
    fake_producer = MagicMock()
    with patch.object(kafka_producer, "_get_producer", return_value=fake_producer):
        kafka_producer.produce_delete(customer)

    fake_producer.produce.assert_called_once()
    kwargs = fake_producer.produce.call_args.kwargs
    assert kwargs["key"] == str(customer.customer_id)
    assert kwargs["value"]["op"] == "d"  # Debezium delete op code
    fake_producer.flush.assert_called_once()
