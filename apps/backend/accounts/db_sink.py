"""PostgreSQL sink for the accounts service.

Schema initialisation plus CRUD, built on the shared connection pool and
helpers in :mod:`db`. This module owns only the accounts-specific DDL,
column list and row mapping.
"""

from __future__ import annotations

from datetime import datetime, timezone
from uuid import UUID, uuid4

import psycopg2.errors
from fastapi import HTTPException

import db
from accounts.models import Account, AccountCreate, AccountUpdate

# ---------------------------------------------------------------------------
# Schema
# ---------------------------------------------------------------------------

_DDL_STATEMENTS = [
    """
    CREATE TABLE IF NOT EXISTS accounts (
        account_id      UUID            PRIMARY KEY DEFAULT gen_random_uuid(),
        customer_id     UUID            NOT NULL REFERENCES customers(customer_id),
        account_number  VARCHAR(20)     NOT NULL UNIQUE,
        account_type    VARCHAR(30)     NOT NULL,
        currency        CHAR(3)         NOT NULL DEFAULT 'USD',
        balance         NUMERIC(18, 2)  NOT NULL DEFAULT 0.00,
        credit_limit    NUMERIC(18, 2),
        opened_date     DATE            NOT NULL DEFAULT CURRENT_DATE,
        closed_date     DATE,
        status          VARCHAR(20)     NOT NULL DEFAULT 'ACTIVE',
        created_at      TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
        updated_at      TIMESTAMPTZ     NOT NULL DEFAULT NOW()
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_accounts_customer_id ON accounts (customer_id)",
    """
    CREATE OR REPLACE FUNCTION set_updated_at()
    RETURNS TRIGGER LANGUAGE plpgsql AS $$
    BEGIN
        NEW.updated_at = NOW();
        RETURN NEW;
    END;
    $$
    """,
    """
    DO $$ BEGIN
        IF NOT EXISTS (
            SELECT 1 FROM pg_trigger WHERE tgname = 'trg_accounts_updated_at'
        ) THEN
            CREATE TRIGGER trg_accounts_updated_at
            BEFORE UPDATE ON accounts
            FOR EACH ROW EXECUTE FUNCTION set_updated_at();
        END IF;
    END $$
    """,
]

_INSERT_COLS = (
    "account_id, customer_id, account_number, account_type, currency, balance, "
    "credit_limit, opened_date, closed_date, status, created_at, updated_at"
)
_PLACEHOLDERS = ", ".join(["%s"] * 12)
_SEED_SQL = (
    f"INSERT INTO accounts ({_INSERT_COLS}) VALUES ({_PLACEHOLDERS}) "
    f"ON CONFLICT (account_id) DO NOTHING"
)  # noqa: S608 — static column list
_INSERT_RETURNING_SQL = (
    f"INSERT INTO accounts ({_INSERT_COLS}) VALUES ({_PLACEHOLDERS}) RETURNING *"
)  # noqa: S608 — static column list


def _insert_params(a: Account) -> tuple:
    return (
        str(a.account_id), str(a.customer_id), a.account_number, a.account_type,
        a.currency, a.balance, a.credit_limit, a.opened_date, a.closed_date,
        a.status, a.created_at, a.updated_at,
    )


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def init_db() -> None:
    db.init_schema(_DDL_STATEMENTS, cdc_table="accounts")


def seed_from_csv(accounts: list[Account]) -> None:
    db.seed_if_empty("accounts", accounts, _SEED_SQL, _insert_params, label="accounts")


def create(data: AccountCreate) -> Account:
    now = datetime.now(tz=timezone.utc)
    account = Account(account_id=uuid4(), created_at=now, updated_at=now, **data.model_dump())
    try:
        with db.cursor(dict_rows=True, commit=True) as cur:
            cur.execute(_INSERT_RETURNING_SQL, _insert_params(account))
            row = cur.fetchone()
        return Account(**row)
    except psycopg2.errors.UniqueViolation as exc:
        raise HTTPException(status_code=409, detail="An account with that account number already exists") from exc


def update(account_id: UUID, data: AccountUpdate) -> Account | None:
    fields = {k: v for k, v in data.model_dump().items() if v is not None}
    if not fields:
        return get_by_id(account_id)
    if "customer_id" in fields:
        fields["customer_id"] = str(fields["customer_id"])

    set_clause = ", ".join(f"{col} = %s" for col in fields)
    values = list(fields.values()) + [str(account_id)]
    sql = f"UPDATE accounts SET {set_clause} WHERE account_id = %s RETURNING *"  # noqa: S608 — columns from model field names only

    with db.cursor(dict_rows=True, commit=True) as cur:
        cur.execute(sql, values)
        row = cur.fetchone()
    return Account(**row) if row else None


def get_by_id(account_id: UUID) -> Account | None:
    with db.cursor(dict_rows=True) as cur:
        cur.execute("SELECT * FROM accounts WHERE account_id = %s", (str(account_id),))
        row = cur.fetchone()
    return Account(**row) if row else None


def list_all() -> list[Account]:
    with db.cursor(dict_rows=True) as cur:
        cur.execute("SELECT * FROM accounts ORDER BY created_at DESC")
        rows = cur.fetchall()
    return [Account(**row) for row in rows]


def delete_by_id(account_id: UUID) -> bool:
    with db.cursor(commit=True) as cur:
        cur.execute("DELETE FROM accounts WHERE account_id = %s", (str(account_id),))
        return cur.rowcount > 0
