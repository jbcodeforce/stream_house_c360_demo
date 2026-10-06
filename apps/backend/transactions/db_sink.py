"""PostgreSQL sink for the transactions service (create-only).

Schema initialisation plus create/read, built on the shared connection pool
and helpers in :mod:`db`. This module owns only the transactions-specific
DDL, column list and row mapping.
"""

from __future__ import annotations

from datetime import datetime, timezone
from uuid import UUID, uuid4

import db
from transactions.models import Transaction, TransactionCreate

# ---------------------------------------------------------------------------
# Schema
# ---------------------------------------------------------------------------

_DDL_STATEMENTS = [
    """
    CREATE TABLE IF NOT EXISTS transactions (
        transaction_id   UUID           PRIMARY KEY DEFAULT gen_random_uuid(),
        account_id       UUID           NOT NULL REFERENCES accounts(account_id),
        customer_id      UUID           NOT NULL REFERENCES customers(customer_id),
        transaction_type VARCHAR(30)    NOT NULL,
        amount           NUMERIC(18, 2) NOT NULL,
        currency         CHAR(3)        NOT NULL DEFAULT 'USD',
        description      VARCHAR(500),
        merchant_name    VARCHAR(200),
        merchant_category VARCHAR(100),
        channel          VARCHAR(50),
        status           VARCHAR(20)    NOT NULL DEFAULT 'COMPLETED',
        reference_id     VARCHAR(100),
        transacted_at    TIMESTAMPTZ    NOT NULL DEFAULT NOW(),
        posted_at        TIMESTAMPTZ,
        created_at       TIMESTAMPTZ    NOT NULL DEFAULT NOW()
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_transactions_account_id ON transactions (account_id)",
    "CREATE INDEX IF NOT EXISTS idx_transactions_customer_id ON transactions (customer_id)",
    "CREATE INDEX IF NOT EXISTS idx_transactions_transacted_at ON transactions (transacted_at DESC)",
]

_INSERT_COLS = (
    "transaction_id, account_id, customer_id, transaction_type, amount, currency, "
    "description, merchant_name, merchant_category, channel, status, reference_id, "
    "transacted_at, posted_at, created_at"
)
_PLACEHOLDERS = ", ".join(["%s"] * 15)
_SEED_SQL = (
    f"INSERT INTO transactions ({_INSERT_COLS}) VALUES ({_PLACEHOLDERS}) "
    f"ON CONFLICT (transaction_id) DO NOTHING"
)  # noqa: S608 — static column list
_INSERT_RETURNING_SQL = (
    f"INSERT INTO transactions ({_INSERT_COLS}) VALUES ({_PLACEHOLDERS}) RETURNING *"
)  # noqa: S608 — static column list


def _insert_params(t: Transaction) -> tuple:
    return (
        str(t.transaction_id), str(t.account_id), str(t.customer_id),
        t.transaction_type, t.amount, t.currency, t.description, t.merchant_name,
        t.merchant_category, t.channel, t.status, t.reference_id,
        t.transacted_at, t.posted_at, t.created_at,
    )


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def init_db() -> None:
    db.init_schema(_DDL_STATEMENTS, cdc_table="transactions")


def seed_from_csv(transactions: list[Transaction]) -> None:
    db.seed_if_empty("transactions", transactions, _SEED_SQL, _insert_params, label="transactions")


def create(data: TransactionCreate) -> Transaction:
    now = datetime.now(tz=timezone.utc)
    transaction = Transaction(transaction_id=uuid4(), created_at=now, **data.model_dump())
    with db.cursor(dict_rows=True, commit=True) as cur:
        cur.execute(_INSERT_RETURNING_SQL, _insert_params(transaction))
        row = cur.fetchone()
    return Transaction(**row)


def get_by_id(transaction_id: UUID) -> Transaction | None:
    with db.cursor(dict_rows=True) as cur:
        cur.execute("SELECT * FROM transactions WHERE transaction_id = %s", (str(transaction_id),))
        row = cur.fetchone()
    return Transaction(**row) if row else None


def list_all() -> list[Transaction]:
    with db.cursor(dict_rows=True) as cur:
        cur.execute("SELECT * FROM transactions ORDER BY transacted_at DESC")
        rows = cur.fetchall()
    return [Transaction(**row) for row in rows]
