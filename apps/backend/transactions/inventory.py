"""Seed-source for transactions, backed by a CSV keyed by account_number.

The CSV has no transaction_id/account_id (both server/derived). ``load`` takes
the already-loaded accounts, resolves each row's account_number to the real
account_id/customer_id, and generates transaction_id/created_at. Rows whose
account_number is unknown are skipped.
"""

import csv
import os
from datetime import datetime, timezone
from pathlib import Path
from uuid import UUID, uuid4

from accounts.models import Account
from transactions.models import Transaction

_CSV_PATH = Path(__file__).parent / "data" / "transactions.csv"

_transactions: list[Transaction] = []


def _csv_path() -> Path:
    """Resolve the CSV path, honouring the TRANSACTIONS_CSV_PATH override."""
    override = os.getenv("TRANSACTIONS_CSV_PATH")
    return Path(override) if override else _CSV_PATH


def _row_to_transaction(row: dict, account: Account) -> Transaction:
    cleaned = {k: (v if v != "" else None) for k, v in row.items()}
    cleaned.pop("account_number", None)
    now = datetime.now(tz=timezone.utc)
    cleaned["transaction_id"] = str(uuid4())
    cleaned["created_at"] = now
    cleaned["account_id"] = str(account.account_id)
    cleaned["customer_id"] = str(account.customer_id)
    return Transaction.model_validate(cleaned)


def load(accounts: list[Account]) -> list[Transaction]:
    """Read the CSV, resolving account_number against *accounts*."""
    global _transactions
    by_number = {a.account_number: a for a in accounts}
    result: list[Transaction] = []
    with _csv_path().open(newline="", encoding="utf-8") as fh:
        for row in csv.DictReader(fh):
            account = by_number.get(row.get("account_number", ""))
            if account is None:
                continue
            result.append(_row_to_transaction(row, account))
    _transactions = result
    return _transactions


def list_all() -> list[Transaction]:
    return _transactions


def get_by_id(transaction_id: UUID) -> Transaction | None:
    for txn in _transactions:
        if txn.transaction_id == transaction_id:
            return txn
    return None
