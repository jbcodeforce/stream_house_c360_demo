"""PostgreSQL sink for the customers service.

Provides schema initialisation and CRU operations backed by a
psycopg2 SimpleConnectionPool.  The pool is created lazily on first use
from settings.DATABASE_URL.
"""

from __future__ import annotations

from uuid import UUID, uuid4
from datetime import datetime, timezone

import psycopg2
import psycopg2.errors
import psycopg2.extras
import psycopg2.pool
from fastapi import HTTPException

from config import settings
from customers.models import Customer, CustomerCreate, CustomerUpdate

# ---------------------------------------------------------------------------
# Connection pool (lazy init)
# ---------------------------------------------------------------------------

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


# ---------------------------------------------------------------------------
# DDL (copied verbatim from scripts/db/create_tables.py)
# ---------------------------------------------------------------------------

_DDL_STATEMENTS = [
    # customers table
    """
    CREATE TABLE IF NOT EXISTS customers (
        customer_id     UUID            PRIMARY KEY DEFAULT gen_random_uuid(),
        first_name      VARCHAR(100)    NOT NULL,
        last_name       VARCHAR(100)    NOT NULL,
        email           VARCHAR(255)    NOT NULL UNIQUE,
        phone           VARCHAR(30),
        date_of_birth   TIMESTAMPTZ,
        gender          VARCHAR(20),
        address_line1   VARCHAR(255),
        address_line2   VARCHAR(255),
        city            VARCHAR(100),
        state           VARCHAR(100),
        postal_code     VARCHAR(20),
        country         VARCHAR(60)     NOT NULL DEFAULT 'US',
        customer_since  DATE            NOT NULL DEFAULT CURRENT_DATE,
        segment         VARCHAR(50),
        status          VARCHAR(20)     NOT NULL DEFAULT 'ACTIVE',
        created_at      TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
        updated_at      TIMESTAMPTZ     NOT NULL DEFAULT NOW()
    )
    """,

    # indexes
    "CREATE INDEX IF NOT EXISTS idx_customers_email ON customers (email)",

    # updated_at trigger function
    """
    CREATE OR REPLACE FUNCTION set_updated_at()
    RETURNS TRIGGER LANGUAGE plpgsql AS $$
    BEGIN
        NEW.updated_at = NOW();
        RETURN NEW;
    END;
    $$
    """,

    # trigger for customers
    """
    DO $$ BEGIN
        IF NOT EXISTS (
            SELECT 1 FROM pg_trigger
            WHERE tgname = 'trg_customers_updated_at'
        ) THEN
            CREATE TRIGGER trg_customers_updated_at
            BEFORE UPDATE ON customers
            FOR EACH ROW EXECUTE FUNCTION set_updated_at();
        END IF;
    END $$
    """,
]

# CDC publication — customers only for this service
_CDC_PUBLICATION_SQL = """
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_publication WHERE pubname = 'c360_cdc_publication'
    ) THEN
        CREATE PUBLICATION c360_cdc_publication FOR TABLE customers;
    END IF;
END $$;
"""


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def init_db() -> None:
    """Create the schema and CDC publication if they do not already exist."""
    conn = _get_conn()
    try:
        conn.autocommit = False
        with conn.cursor() as cur:
            for stmt in _DDL_STATEMENTS:
                cur.execute(stmt)
            cur.execute(_CDC_PUBLICATION_SQL)
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        _put_conn(conn)


def seed_from_csv(customers: list[Customer]) -> None:
    """Bulk-insert *customers* only when the table is currently empty.

    Uses ON CONFLICT DO NOTHING so re-running is safe.
    """
    if not customers:
        return

    conn = _get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM customers")
            count = cur.fetchone()[0]
            if count > 0:
                return

            insert_sql = """
                INSERT INTO customers (
                    customer_id, first_name, last_name, email, phone,
                    date_of_birth, gender, address_line1, address_line2,
                    city, state, postal_code, country, customer_since,
                    segment, status, created_at, updated_at
                ) VALUES (
                    %s, %s, %s, %s, %s,
                    %s, %s, %s, %s,
                    %s, %s, %s, %s, %s,
                    %s, %s, %s, %s
                )
                ON CONFLICT (customer_id) DO NOTHING
            """
            for customer in customers:
                cur.execute(insert_sql, (
                    str(customer.customer_id),
                    customer.first_name,
                    customer.last_name,
                    customer.email,
                    customer.phone,
                    customer.date_of_birth,
                    customer.gender,
                    customer.address_line1,
                    customer.address_line2,
                    customer.city,
                    customer.state,
                    customer.postal_code,
                    customer.country,
                    customer.customer_since,
                    customer.segment,
                    customer.status,
                    customer.created_at,
                    customer.updated_at,
                ))
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        _put_conn(conn)


def create(data: CustomerCreate) -> Customer:
    """INSERT a new customer and return the persisted row."""
    now = datetime.now(tz=timezone.utc)
    customer_id = uuid4()

    insert_sql = """
        INSERT INTO customers (
            customer_id, first_name, last_name, email, phone,
            date_of_birth, gender, address_line1, address_line2,
            city, state, postal_code, country, customer_since,
            segment, status, created_at, updated_at
        ) VALUES (
            %s, %s, %s, %s, %s,
            %s, %s, %s, %s,
            %s, %s, %s, %s, %s,
            %s, %s, %s, %s
        )
        RETURNING *
    """
    conn = _get_conn()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(insert_sql, (
                str(customer_id),
                data.first_name,
                data.last_name,
                data.email,
                data.phone,
                data.date_of_birth,
                data.gender,
                data.address_line1,
                data.address_line2,
                data.city,
                data.state,
                data.postal_code,
                data.country,
                data.customer_since,
                data.segment,
                data.status,
                now,
                now,
            ))
            row = cur.fetchone()
        conn.commit()
        return Customer(**row)
    except psycopg2.errors.UniqueViolation as exc:
        conn.rollback()
        raise HTTPException(status_code=409, detail="A customer with that email already exists") from exc
    except Exception:
        conn.rollback()
        raise
    finally:
        _put_conn(conn)


def update(customer_id: UUID, data: CustomerUpdate) -> Customer | None:
    """UPDATE only the non-None fields and return the updated row, or None."""
    fields = {k: v for k, v in data.model_dump().items() if v is not None}
    if not fields:
        return get_by_id(customer_id)

    set_clause = ", ".join(f"{col} = %s" for col in fields)
    values = list(fields.values()) + [str(customer_id)]

    sql = f"UPDATE customers SET {set_clause} WHERE customer_id = %s RETURNING *"  # noqa: S608 — set_clause built from model field names only

    conn = _get_conn()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(sql, values)
            row = cur.fetchone()
        conn.commit()
        if row is None:
            return None
        return Customer(**row)
    except Exception:
        conn.rollback()
        raise
    finally:
        _put_conn(conn)


def get_by_id(customer_id: UUID) -> Customer | None:
    """SELECT a single customer by primary key, or return None."""
    conn = _get_conn()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                "SELECT * FROM customers WHERE customer_id = %s",
                (str(customer_id),),
            )
            row = cur.fetchone()
        return Customer(**row) if row else None
    finally:
        _put_conn(conn)


def list_all() -> list[Customer]:
    """SELECT all customers ordered by created_at DESC."""
    conn = _get_conn()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("SELECT * FROM customers ORDER BY created_at DESC")
            rows = cur.fetchall()
        return [Customer(**row) for row in rows]
    finally:
        _put_conn(conn)


def delete_by_id(customer_id: UUID) -> bool:
    """DELETE a customer by primary key. Returns True if a row was deleted."""
    conn = _get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "DELETE FROM customers WHERE customer_id = %s",
                (str(customer_id),),
            )
            deleted = cur.rowcount > 0
        conn.commit()
        return deleted
    except Exception:
        conn.rollback()
        raise
    finally:
        _put_conn(conn)
