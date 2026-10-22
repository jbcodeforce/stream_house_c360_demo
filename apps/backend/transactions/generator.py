"""Generate realistic back-dated transactions on existing accounts.

Pure logic (no DB / Kafka). Timestamps start at ``now - 7 days`` and advance by
15 minutes per transaction, independent of how fast the batch is produced, so
the data looks like a week of organic activity.
"""

from __future__ import annotations

import random
from datetime import datetime, timedelta, timezone
from uuid import uuid4

from accounts.models import Account
from transactions.models import Transaction

DEFAULT_COUNT = 200
START_OFFSET = timedelta(days=7)
STEP = timedelta(minutes=15)

# (description, merchant_name, merchant_category, channel) — modelled on the
# values in data/transactions.csv.
_DEBIT_PROFILES = [
    ("Online purchase", "Amazon", "RETAIL", "ONLINE"),
    ("Team breakfast", "Starbucks", "FOOD", "POS"),
    ("Fuel", "Shell", "FUEL", "POS"),
    ("Streaming subscription", "Netflix", "ENTERTAINMENT", "MOBILE"),
    ("Groceries", "Whole Foods", "GROCERY", "BRANCH"),
    ("Ride", "Uber", "TRANSPORT", "ONLINE"),
]
_CREDIT_PROFILES = [
    ("Salary", "Acme Payroll", "INCOME", "ONLINE"),
    ("Refund", "Amazon", "RETAIL", "ONLINE"),
    ("Deposit", "Branch deposit", "BANK", "BRANCH"),
]


class NoEligibleAccountsError(Exception):
    """Raised when there is no ACTIVE, open account to attach transactions to."""


def generate(
    accounts: list[Account],
    count: int = DEFAULT_COUNT,
    now: datetime | None = None,
    rng: random.Random | None = None,
) -> list[Transaction]:
    """Return *count* transactions on random eligible accounts, oldest first."""
    eligible = [a for a in accounts if a.status == "ACTIVE" and a.closed_date is None]
    if not eligible:
        raise NoEligibleAccountsError("no active accounts available")

    rng = rng or random.Random()
    now = now or datetime.now(tz=timezone.utc)
    start = (now - START_OFFSET).replace(microsecond=0)

    result: list[Transaction] = []
    for i in range(count):
        account = rng.choice(eligible)
        if rng.random() < 0.85:
            txn_type = "DEBIT"
            description, merchant, category, channel = rng.choice(_DEBIT_PROFILES)
            amount = round(rng.uniform(3, 400), 2)
        else:
            txn_type = "CREDIT"
            description, merchant, category, channel = rng.choice(_CREDIT_PROFILES)
            amount = round(rng.uniform(50, 2500), 2)
        transacted_at = start + i * STEP
        result.append(
            Transaction(
                transaction_id=uuid4(),
                account_id=account.account_id,
                customer_id=account.customer_id,
                transaction_type=txn_type,
                amount=amount,
                currency=account.currency,
                description=description,
                merchant_name=merchant,
                merchant_category=category,
                channel=channel,
                status="COMPLETED",
                transacted_at=transacted_at,
                posted_at=transacted_at,
                created_at=now,
            )
        )
    return result
