"""Shared PostgreSQL access for all C360 sinks.

One lazily-created connection pool, a cursor context manager that handles
commit/rollback/return-to-pool, and reusable schema/seed/CDC helpers. Each
service's ``db_sink`` module supplies only its own DDL, column list and
row->params mapping; the connection and transaction mechanics live here so
they are written once.
"""

from __future__ import annotations

import logging
from contextlib import contextmanager
from typing import Callable, Sequence

import psycopg2
import psycopg2.extras
import psycopg2.pool

from config import settings

logger = logging.getLogger("c360.db")

# Logical-replication publication consumed by the Debezium / Confluent CDC
# connector. Each table adds itself to it during schema init.
CDC_PUBLICATION = "c360_cdc_publication"

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


def get_conn():
    return _ensure_pool().getconn()


def put_conn(conn) -> None:
    _ensure_pool().putconn(conn)


@contextmanager
def cursor(*, dict_rows: bool = False, commit: bool = False):
    """Yield a cursor from a pooled connection.

    Commits on clean exit when *commit* is True, always rolls back on error,
    and returns the connection to the pool. Use ``dict_rows=True`` to get
    ``RealDictCursor`` rows suitable for ``Model(**row)``.
    """
    conn = get_conn()
    factory = psycopg2.extras.RealDictCursor if dict_rows else None
    try:
        with conn.cursor(cursor_factory=factory) as cur:
            yield cur
        if commit:
            conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        put_conn(conn)


def _ensure_cdc_table(cur, table: str) -> None:
    """Ensure the CDC publication exists and includes *table* (idempotent)."""
    cur.execute("SELECT 1 FROM pg_publication WHERE pubname = %s", (CDC_PUBLICATION,))
    if cur.fetchone() is None:
        # Identifiers cannot be bound as parameters; names are module constants.
        cur.execute(f"CREATE PUBLICATION {CDC_PUBLICATION}")  # noqa: S608
    cur.execute(
        "SELECT 1 FROM pg_publication_tables WHERE pubname = %s AND tablename = %s",
        (CDC_PUBLICATION, table),
    )
    if cur.fetchone() is None:
        cur.execute(f"ALTER PUBLICATION {CDC_PUBLICATION} ADD TABLE {table}")  # noqa: S608


def init_schema(ddl_statements: Sequence[str], *, cdc_table: str | None = None) -> None:
    """Run *ddl_statements* in one transaction, then add *cdc_table* to the
    CDC publication. All statements are idempotent (CREATE ... IF NOT EXISTS),
    so this is safe to run on every startup."""
    conn = get_conn()
    try:
        conn.autocommit = False
        with conn.cursor() as cur:
            for stmt in ddl_statements:
                cur.execute(stmt)
            if cdc_table is not None:
                _ensure_cdc_table(cur, cdc_table)
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        put_conn(conn)


def seed_if_empty(
    table: str,
    rows: Sequence,
    insert_sql: str,
    to_params: Callable[[object], tuple],
    *,
    label: str | None = None,
) -> None:
    """Bulk-insert *rows* into *table* only when the table is currently empty.

    *insert_sql* should carry ``ON CONFLICT DO NOTHING`` so a re-run is safe.
    Integrity errors (e.g. an FK whose parent was not seeded from the same
    CSV) are logged and skipped rather than crashing startup.
    """
    if not rows:
        return
    label = label or table
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) FROM {table}")  # noqa: S608 — constant table name
            if cur.fetchone()[0] > 0:
                return
            for row in rows:
                cur.execute(insert_sql, to_params(row))
        conn.commit()
    except psycopg2.IntegrityError:
        conn.rollback()
        logger.warning(
            "Skipping %s seed: integrity error (is the referenced parent seeded?)",
            label,
            exc_info=True,
        )
    except Exception:
        conn.rollback()
        raise
    finally:
        put_conn(conn)
