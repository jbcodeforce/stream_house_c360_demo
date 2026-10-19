"""Transactions inventory: load CSV and resolve account_number -> account_id."""

from __future__ import annotations

import csv
from datetime import date, datetime, timezone
from uuid import uuid4

from accounts.inventory import _account_id_for
from accounts.models import Account
from transactions import inventory
from transactions.models import Transaction

_CUST = "00000000-0000-0000-0000-ef1234567890"
_HEADER = ("customer_id,account_number,transaction_type,amount,currency,description,"
           "merchant_name,merchant_category,channel,status,reference_id,transacted_at,posted_at\n")
_ROW1 = (f"{_CUST},CHK-1,CREDIT,99.90,USD,Credit transaction,Amazon,RETAIL,ONLINE,COMPLETED,"
         "REF-1,2024-01-01T00:00:00+00:00,2024-01-01T00:00:00+00:00\n")

def _account(number: str) -> Account:
    now = datetime.now(tz=timezone.utc)
    return Account(
        account_id=uuid4(), customer_id=uuid4(), account_number=number,
        account_type="CHECKING", currency="USD", balance=0.0, credit_limit=None,
        opened_date=date(2020, 1, 1), closed_date=None, status="ACTIVE",
        created_at=now, updated_at=now,
    )


def _transaction(account: Account) -> Transaction:
    now = datetime.now(tz=timezone.utc)
    return Transaction(
        transaction_id=uuid4(),
        account_id=account.account_id,
        customer_id=account.customer_id,
        transaction_type="DEBIT",
        amount=10.00,
        currency="USD",
        status="COMPLETED",
        transacted_at=now,
        created_at=now,
    )


# ---------------------------------------------------------------------------
# load
# ---------------------------------------------------------------------------

def test_load_derives_account_id_from_account_number(tmp_path, monkeypatch):
    csv_path = tmp_path / "transactions.csv"
    csv_path.write_text(_HEADER + _ROW1, encoding="utf-8")
    monkeypatch.setenv("TRANSACTIONS_CSV_PATH", str(csv_path))

    txns = inventory.load()

    assert len(txns) == 1
    t = txns[0]
    assert str(t.account_id) == _account_id_for("CHK-1")  # derived from account_number
    assert str(t.customer_id) == _CUST
    assert t.transaction_type == "CREDIT"
    assert t.amount == 99.90
    assert t.transaction_id is not None             # generated
    assert t.created_at is not None


def test_load_transaction_id_is_deterministic(tmp_path, monkeypatch):
    csv_path = tmp_path / "transactions.csv"
    csv_path.write_text(_HEADER + _ROW1, encoding="utf-8")
    monkeypatch.setenv("TRANSACTIONS_CSV_PATH", str(csv_path))

    assert inventory.load()[0].transaction_id == inventory.load()[0].transaction_id


# ---------------------------------------------------------------------------
# list_all / get_by_id
# ---------------------------------------------------------------------------

def test_list_all_reflects_loaded_cache(tmp_path, monkeypatch):
    csv_path = tmp_path / "transactions.csv"
    csv_path.write_text(_HEADER + _ROW1, encoding="utf-8")
    monkeypatch.setenv("TRANSACTIONS_CSV_PATH", str(csv_path))

    account = _account("CHK-1")
    inventory.load()
    assert len(inventory.list_all()) == 1


def test_get_by_id_found_and_not_found(tmp_path, monkeypatch):
    csv_path = tmp_path / "transactions.csv"
    csv_path.write_text(_HEADER + _ROW1, encoding="utf-8")
    monkeypatch.setenv("TRANSACTIONS_CSV_PATH", str(csv_path))

    account = _account("CHK-1")
    [txn] = inventory.load()

    assert inventory.get_by_id(txn.transaction_id) is txn
    assert inventory.get_by_id(uuid4()) is None


# ---------------------------------------------------------------------------
# create
# ---------------------------------------------------------------------------

def test_create_appends_and_persists(tmp_path, monkeypatch):
    csv_path = tmp_path / "transactions.csv"
    csv_path.write_text(_HEADER + _ROW1, encoding="utf-8")
    monkeypatch.setenv("TRANSACTIONS_CSV_PATH", str(csv_path))

    account = _account("CHK-1")
    inventory.load()
    assert len(inventory.list_all()) == 1

    new_txn = _transaction(account)
    created = inventory.create(new_txn)
    assert len(inventory.list_all()) == 2
    assert inventory.get_by_id(created.transaction_id) is created

    # Persisted: header + original row + new row (2 data rows total)
    lines = csv_path.read_text(encoding="utf-8").strip().splitlines()
    assert len(lines) == 3  # header + 2 data rows


# ---------------------------------------------------------------------------
# update
# ---------------------------------------------------------------------------

def test_update_replaces_in_cache_and_csv(tmp_path, monkeypatch):
    csv_path = tmp_path / "transactions.csv"
    csv_path.write_text(_HEADER + _ROW1, encoding="utf-8")
    monkeypatch.setenv("TRANSACTIONS_CSV_PATH", str(csv_path))

    account = _account("CHK-1")
    [existing] = inventory.load()
    changed = existing.model_copy(update={"status": "PENDING"})
    inventory.update(changed)

    assert inventory.get_by_id(existing.transaction_id).status == "PENDING"
    rows = list(csv.DictReader(csv_path.open(newline="", encoding="utf-8")))
    assert rows[0]["status"] == "PENDING"


# ---------------------------------------------------------------------------
# delete_by_id
# ---------------------------------------------------------------------------

def test_delete_removes_and_rewrites_csv(tmp_path, monkeypatch):
    csv_path = tmp_path / "transactions.csv"
    csv_path.write_text(_HEADER + _ROW1, encoding="utf-8")
    monkeypatch.setenv("TRANSACTIONS_CSV_PATH", str(csv_path))

    account = _account("CHK-1")
    [existing] = inventory.load()

    assert inventory.delete_by_id(existing.transaction_id) is True
    assert inventory.list_all() == []

    rows = list(csv.DictReader(csv_path.open(newline="", encoding="utf-8")))
    assert rows == []


def test_delete_returns_false_when_not_found(tmp_path, monkeypatch):
    csv_path = tmp_path / "transactions.csv"
    csv_path.write_text(_HEADER + _ROW1, encoding="utf-8")
    monkeypatch.setenv("TRANSACTIONS_CSV_PATH", str(csv_path))

    account = _account("CHK-1")
    inventory.load()
    assert inventory.delete_by_id(uuid4()) is False
