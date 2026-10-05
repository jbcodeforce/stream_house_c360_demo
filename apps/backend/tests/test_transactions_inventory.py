"""Transactions inventory: load CSV and resolve account_number -> account_id."""

from __future__ import annotations

from datetime import date, datetime, timezone
from uuid import uuid4

from accounts.models import Account
from transactions import inventory

_HEADER = ("customer_id,account_number,transaction_type,amount,currency,description,"
           "merchant_name,merchant_category,channel,status,reference_id,transacted_at,posted_at\n")
_ROW1 = ("c1,CHK-1,CREDIT,99.90,USD,Credit transaction,Amazon,RETAIL,ONLINE,COMPLETED,"
         "REF-1,2024-01-01T00:00:00+00:00,2024-01-01T00:00:00+00:00\n")
_ROW_UNKNOWN = ("c9,CHK-UNKNOWN,DEBIT,5.00,USD,,,,,COMPLETED,REF-2,"
                "2024-01-02T00:00:00+00:00,\n")


def _account(number: str) -> Account:
    now = datetime.now(tz=timezone.utc)
    return Account(
        account_id=uuid4(), customer_id=uuid4(), account_number=number,
        account_type="CHECKING", currency="USD", balance=0.0, credit_limit=None,
        opened_date=date(2020, 1, 1), closed_date=None, status="ACTIVE",
        created_at=now, updated_at=now,
    )


def test_load_resolves_account_and_skips_unknown(tmp_path, monkeypatch):
    csv_path = tmp_path / "transactions.csv"
    csv_path.write_text(_HEADER + _ROW1 + _ROW_UNKNOWN, encoding="utf-8")
    monkeypatch.setenv("TRANSACTIONS_CSV_PATH", str(csv_path))

    account = _account("CHK-1")
    txns = inventory.load([account])

    assert len(txns) == 1                         # CHK-UNKNOWN skipped
    t = txns[0]
    assert t.account_id == account.account_id      # resolved from account_number
    assert t.customer_id == account.customer_id
    assert t.transaction_type == "CREDIT"
    assert t.amount == 99.90
    assert t.transaction_id is not None            # server field generated
    assert t.created_at is not None


def test_load_empty_when_no_matching_accounts(tmp_path, monkeypatch):
    csv_path = tmp_path / "transactions.csv"
    csv_path.write_text(_HEADER + _ROW1, encoding="utf-8")
    monkeypatch.setenv("TRANSACTIONS_CSV_PATH", str(csv_path))
    assert inventory.load([]) == []
