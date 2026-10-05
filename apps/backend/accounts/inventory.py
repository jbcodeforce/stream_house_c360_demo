"""In-memory account inventory backed by a CSV file.

The seed CSV carries only the create-time columns; server-assigned fields
(account_id, created_at, updated_at) are generated when a row is loaded, and
CSV writes keep the original 9-column schema.
"""

import csv
from datetime import datetime, timezone
from pathlib import Path
from uuid import UUID, uuid4

from accounts.models import Account

_CSV_PATH = Path(__file__).parent / "data" / "accounts.csv"

# CSV columns (server-assigned fields are not stored in the file)
_FIELDNAMES = [
    "customer_id",
    "account_number",
    "account_type",
    "currency",
    "balance",
    "credit_limit",
    "opened_date",
    "closed_date",
    "status",
]

_accounts: list[Account] = []


def _row_to_account(row: dict) -> Account:
    """Convert a CSV row to an Account, generating server-assigned fields."""
    cleaned = {k: (v if v != "" else None) for k, v in row.items()}
    now = datetime.now(tz=timezone.utc)
    cleaned["account_id"] = str(uuid4())
    cleaned["created_at"] = now
    cleaned["updated_at"] = now
    return Account.model_validate(cleaned)


def _account_to_row(account: Account) -> dict:
    """Serialise an Account to a CSV row dict (create-time columns only)."""
    return {
        "customer_id": str(account.customer_id),
        "account_number": account.account_number,
        "account_type": account.account_type,
        "currency": account.currency,
        "balance": account.balance,
        "credit_limit": account.credit_limit if account.credit_limit is not None else "",
        "opened_date": account.opened_date.isoformat(),
        "closed_date": account.closed_date.isoformat() if account.closed_date else "",
        "status": account.status,
    }


def load() -> list[Account]:
    """Read the CSV file into the in-memory cache and return it."""
    global _accounts
    with _CSV_PATH.open(newline="", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        _accounts = [_row_to_account(row) for row in reader]
    return _accounts


def list_all() -> list[Account]:
    """Return all accounts from the in-memory cache."""
    return _accounts


def get_by_id(account_id: UUID) -> Account | None:
    """Return the account with the given ID, or None if not found."""
    for account in _accounts:
        if account.account_id == account_id:
            return account
    return None


def create(account: Account) -> Account:
    """Append a new account to the in-memory cache and persist to CSV."""
    _accounts.append(account)
    with _CSV_PATH.open("a", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=_FIELDNAMES)
        writer.writerow(_account_to_row(account))
    return account


def update(account: Account) -> Account:
    """Replace an existing account in the cache and rewrite the CSV."""
    for i, existing in enumerate(_accounts):
        if existing.account_id == account.account_id:
            _accounts[i] = account
            break
    _rewrite_csv()
    return account


def delete_by_id(account_id: UUID) -> bool:
    """Remove an account from the cache and rewrite the CSV. True if found."""
    global _accounts
    before = len(_accounts)
    _accounts = [a for a in _accounts if a.account_id != account_id]
    if len(_accounts) == before:
        return False
    _rewrite_csv()
    return True


def _rewrite_csv() -> None:
    """Rewrite the entire CSV from the current in-memory cache."""
    with _CSV_PATH.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=_FIELDNAMES)
        writer.writeheader()
        for account in _accounts:
            writer.writerow(_account_to_row(account))
