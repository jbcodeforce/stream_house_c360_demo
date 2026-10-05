"""accounts.db_sink.seed_from_csv — infra-free via a mocked connection."""

from __future__ import annotations

from datetime import date, datetime, timezone
from unittest.mock import MagicMock, patch
from uuid import uuid4

from accounts import db_sink
from accounts.models import Account


def _account() -> Account:
    now = datetime.now(tz=timezone.utc)
    return Account(
        account_id=uuid4(), customer_id=uuid4(), account_number="ACC-1",
        account_type="CHECKING", currency="USD", balance=1.0, credit_limit=None,
        opened_date=date(2020, 1, 1), closed_date=None, status="ACTIVE",
        created_at=now, updated_at=now,
    )


def _fake_conn(count: int):
    cur = MagicMock()
    cur.fetchone.return_value = [count]
    conn = MagicMock()
    conn.cursor.return_value.__enter__.return_value = cur
    return conn, cur


def test_seed_noop_on_empty_list():
    with patch.object(db_sink, "_get_conn") as get:
        db_sink.seed_from_csv([])
        get.assert_not_called()


def test_seed_inserts_when_table_empty():
    conn, cur = _fake_conn(count=0)
    with patch.object(db_sink, "_get_conn", return_value=conn), \
         patch.object(db_sink, "_put_conn"):
        db_sink.seed_from_csv([_account(), _account()])
    # one COUNT select + one insert per account
    assert cur.execute.call_count == 3
    conn.commit.assert_called_once()


def test_seed_skips_when_table_not_empty():
    conn, cur = _fake_conn(count=5)
    with patch.object(db_sink, "_get_conn", return_value=conn), \
         patch.object(db_sink, "_put_conn"):
        db_sink.seed_from_csv([_account()])
    assert cur.execute.call_count == 1       # only the COUNT, no inserts
    conn.commit.assert_not_called()


def test_seed_swallows_integrity_error_and_rolls_back():
    # If customers weren't seeded from the same CSV, an account insert can hit
    # a FK violation. Seeding must log and skip, not crash app startup.
    import psycopg2
    conn, cur = _fake_conn(count=0)
    cur.execute.side_effect = [None, psycopg2.IntegrityError("fk violation")]
    with patch.object(db_sink, "_get_conn", return_value=conn), \
         patch.object(db_sink, "_put_conn"):
        db_sink.seed_from_csv([_account()])  # must NOT raise
    conn.rollback.assert_called_once()
    conn.commit.assert_not_called()
