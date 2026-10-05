"""PostgreSQL sink for the accounts service."""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from uuid import UUID, uuid4

import psycopg2
import psycopg2.errors
import psycopg2.extras
import psycopg2.pool
from fastapi import HTTPException

from accounts.models import Account, AccountCreate, AccountUpdate
from config import settings

logger = logging.getLogger("c360.accounts.db_sink")

_pool: psycopg2.pool.SimpleConnectionPool | None = None


def _ensure_pool() -> psycopg2.pool.SimpleConnectionPool:
    global _pool
    if _pool is None:
        url = settings.DATABASE_URL
        if url is None:
            raise RuntimeError("DATABASE_URL is not configured")
        if "sslmode=" not in url:
            sep = "&" if "?" in url else "?"
            url = f"{url}{sep}sslmode=prefer"
        _pool = psycopg2.pool.SimpleConnectionPool(minconn=1, maxconn=5, dsn=url)
    return _pool


def _get_conn():
    return _ensure_pool().getconn()


def _put_conn(conn) -> None:
    _ensure_pool().putconn(conn)


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


def init_db() -> None:
    conn = _get_conn()
    try:
        conn.autocommit = False
        with conn.cursor() as cur:
            for stmt in _DDL_STATEMENTS:
                cur.execute(stmt)
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        _put_conn(conn)


def seed_from_csv(accounts: list[Account]) -> None:
    """Bulk-insert *accounts* only when the table is currently empty.

    Uses ON CONFLICT DO NOTHING so re-running is safe.
    """
    if not accounts:
        return

    conn = _get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM accounts")
            count = cur.fetchone()[0]
            if count > 0:
                return

            insert_sql = f"""
                INSERT INTO accounts ({_INSERT_COLS})
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (account_id) DO NOTHING
            """  # noqa: S608 — static column list
            for account in accounts:
                cur.execute(insert_sql, (
                    str(account.account_id),
                    str(account.customer_id),
                    account.account_number,
                    account.account_type,
                    account.currency,
                    account.balance,
                    account.credit_limit,
                    account.opened_date,
                    account.closed_date,
                    account.status,
                    account.created_at,
                    account.updated_at,
                ))
        conn.commit()
    except psycopg2.IntegrityError:
        # e.g. a FK violation when the referenced customers were not seeded
        # from the same CSV. Skip seeding rather than crash app startup.
        conn.rollback()
        logger.warning(
            "Skipping accounts seed: integrity error (are customers seeded from the CSV?)",
            exc_info=True,
        )
    except Exception:
        conn.rollback()
        raise
    finally:
        _put_conn(conn)


def create(data: AccountCreate) -> Account:
    now = datetime.now(tz=timezone.utc)
    account_id = uuid4()
    insert_sql = f"""
        INSERT INTO accounts ({_INSERT_COLS})
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        RETURNING *
    """  # noqa: S608 — static column list
    conn = _get_conn()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(insert_sql, (
                str(account_id),
                str(data.customer_id),
                data.account_number,
                data.account_type,
                data.currency,
                data.balance,
                data.credit_limit,
                data.opened_date,
                data.closed_date,
                data.status,
                now,
                now,
            ))
            row = cur.fetchone()
        conn.commit()
        return Account(**row)
    except psycopg2.errors.UniqueViolation as exc:
        conn.rollback()
        raise HTTPException(status_code=409, detail="An account with that account number already exists") from exc
    except Exception:
        conn.rollback()
        raise
    finally:
        _put_conn(conn)


def update(account_id: UUID, data: AccountUpdate) -> Account | None:
    fields = {k: v for k, v in data.model_dump().items() if v is not None}
    if not fields:
        return get_by_id(account_id)
    if "customer_id" in fields:
        fields["customer_id"] = str(fields["customer_id"])

    set_clause = ", ".join(f"{col} = %s" for col in fields)
    values = list(fields.values()) + [str(account_id)]
    sql = f"UPDATE accounts SET {set_clause} WHERE account_id = %s RETURNING *"  # noqa: S608 — columns from model field names only

    conn = _get_conn()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(sql, values)
            row = cur.fetchone()
        conn.commit()
        return Account(**row) if row else None
    except Exception:
        conn.rollback()
        raise
    finally:
        _put_conn(conn)


def get_by_id(account_id: UUID) -> Account | None:
    conn = _get_conn()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("SELECT * FROM accounts WHERE account_id = %s", (str(account_id),))
            row = cur.fetchone()
        return Account(**row) if row else None
    finally:
        _put_conn(conn)


def list_all() -> list[Account]:
    conn = _get_conn()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("SELECT * FROM accounts ORDER BY created_at DESC")
            rows = cur.fetchall()
        return [Account(**row) for row in rows]
    finally:
        _put_conn(conn)


def delete_by_id(account_id: UUID) -> bool:
    conn = _get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM accounts WHERE account_id = %s", (str(account_id),))
            deleted = cur.rowcount > 0
        conn.commit()
        return deleted
    except Exception:
        conn.rollback()
        raise
    finally:
        _put_conn(conn)
