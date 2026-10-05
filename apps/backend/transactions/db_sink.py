"""PostgreSQL sink for the transactions service (create-only)."""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from uuid import UUID, uuid4

import psycopg2
import psycopg2.errors
import psycopg2.extras
import psycopg2.pool

from config import settings
from transactions.models import Transaction, TransactionCreate

logger = logging.getLogger("c360.transactions.db_sink")

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


def _insert_params(t: Transaction) -> tuple:
    return (
        str(t.transaction_id), str(t.account_id), str(t.customer_id),
        t.transaction_type, t.amount, t.currency, t.description, t.merchant_name,
        t.merchant_category, t.channel, t.status, t.reference_id,
        t.transacted_at, t.posted_at, t.created_at,
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


def create(data: TransactionCreate) -> Transaction:
    now = datetime.now(tz=timezone.utc)
    transaction = Transaction(
        transaction_id=uuid4(),
        created_at=now,
        **data.model_dump(),
    )
    insert_sql = f"""
        INSERT INTO transactions ({_INSERT_COLS})
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        RETURNING *
    """  # noqa: S608 — static column list
    conn = _get_conn()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(insert_sql, _insert_params(transaction))
            row = cur.fetchone()
        conn.commit()
        return Transaction(**row)
    except Exception:
        conn.rollback()
        raise
    finally:
        _put_conn(conn)


def get_by_id(transaction_id: UUID) -> Transaction | None:
    conn = _get_conn()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                "SELECT * FROM transactions WHERE transaction_id = %s",
                (str(transaction_id),),
            )
            row = cur.fetchone()
        return Transaction(**row) if row else None
    finally:
        _put_conn(conn)


def list_all() -> list[Transaction]:
    conn = _get_conn()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("SELECT * FROM transactions ORDER BY transacted_at DESC")
            rows = cur.fetchall()
        return [Transaction(**row) for row in rows]
    finally:
        _put_conn(conn)


def seed_from_csv(transactions: list[Transaction]) -> None:
    """Bulk-insert *transactions* only when the table is currently empty.

    Resilient to integrity errors (e.g. FK violations when accounts were not
    seeded from the same CSV): logs and skips rather than crashing startup.
    """
    if not transactions:
        return

    conn = _get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM transactions")
            if cur.fetchone()[0] > 0:
                return
            insert_sql = f"""
                INSERT INTO transactions ({_INSERT_COLS})
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (transaction_id) DO NOTHING
            """  # noqa: S608 — static column list
            for t in transactions:
                cur.execute(insert_sql, _insert_params(t))
        conn.commit()
    except psycopg2.IntegrityError:
        conn.rollback()
        logger.warning(
            "Skipping transactions seed: integrity error (are accounts seeded from the CSV?)",
            exc_info=True,
        )
    except Exception:
        conn.rollback()
        raise
    finally:
        _put_conn(conn)
