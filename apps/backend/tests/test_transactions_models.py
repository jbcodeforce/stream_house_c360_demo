"""Transaction model defaults and optionality."""

from __future__ import annotations

from transactions.models import Transaction, TransactionCreate


def test_create_applies_defaults():
    t = TransactionCreate(
        account_id="11111111-1111-1111-1111-111111111111",
        customer_id="22222222-2222-2222-2222-222222222222",
        transaction_type="CREDIT",
        amount=10.5,
    )
    assert t.currency == "USD"
    assert t.status == "COMPLETED"
    assert t.transacted_at is not None
    assert t.description is None
    assert t.posted_at is None


def test_transacted_at_default_is_dynamic():
    field = TransactionCreate.model_fields["transacted_at"]
    assert field.default_factory is not None


def test_transaction_has_server_fields_and_no_updated_at():
    fields = set(Transaction.model_fields)
    assert {"transaction_id", "created_at"} <= fields
    assert "updated_at" not in fields
