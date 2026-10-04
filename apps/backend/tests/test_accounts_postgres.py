"""Accounts Postgres integration tests — skipped when DATABASE_URL is unset."""

from __future__ import annotations

import os
from uuid import uuid4

import pytest

pytestmark = pytest.mark.usefixtures("skip_without_postgres")


@pytest.fixture
def account_payload():
    return {
        "customer_id": str(uuid4()),  # replaced with a real customer id below
        "account_number": f"ACC-{os.urandom(4).hex()}",
        "account_type": "CHECKING",
    }


def _a_customer_id():
    from customers import db_sink as cust
    existing = cust.list_all()
    assert existing, "seed customers required for FK"
    return existing[0].customer_id


def test_create_and_get_roundtrip(account_payload):
    from accounts import db_sink
    from accounts.models import AccountCreate

    db_sink.init_db()
    account_payload["customer_id"] = str(_a_customer_id())
    created = db_sink.create(AccountCreate(**account_payload))
    fetched = db_sink.get_by_id(created.account_id)
    assert fetched is not None
    assert fetched.account_number == account_payload["account_number"]
    assert db_sink.delete_by_id(created.account_id) is True
    assert db_sink.get_by_id(created.account_id) is None
