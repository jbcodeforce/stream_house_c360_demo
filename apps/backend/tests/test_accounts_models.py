"""Account model defaults and optionality."""

from __future__ import annotations

from datetime import date

from accounts.models import Account, AccountCreate, AccountUpdate


def test_create_applies_defaults():
    a = AccountCreate(
        customer_id="11111111-1111-1111-1111-111111111111",
        account_number="ACC-001",
        account_type="CHECKING",
    )
    assert a.currency == "USD"
    assert a.balance == 0.0
    assert a.status == "ACTIVE"
    assert a.opened_date == date.today()
    assert a.credit_limit is None
    assert a.closed_date is None


def test_update_is_all_optional():
    u = AccountUpdate()
    assert u.model_dump(exclude_unset=True) == {}


def test_account_has_server_fields():
    assert set(Account.model_fields) >= {
        "account_id", "created_at", "updated_at", "customer_id", "account_number",
    }
