"""transactions.db_sink.seed_from_csv — infra-free via a mocked connection."""

from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import MagicMock, patch
from uuid import uuid4

from transactions import db_sink
from transactions.models import Transaction


def _txn() -> Transaction:
    now = datetime.now(tz=timezone.utc)
    return Transaction(
        transaction_id=uuid4(), account_id=uuid4(), customer_id=uuid4(),
        transaction_type="CREDIT", amount=10.0, currency="USD", status="COMPLETED",
        transacted_at=now, posted_at=None, created_at=now,
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
        db_sink.seed_from_csv([_txn(), _txn()])
    assert cur.execute.call_count == 3       # COUNT + 2 inserts
    conn.commit.assert_called_once()


def test_seed_skips_when_table_not_empty():
    conn, cur = _fake_conn(count=5)
    with patch.object(db_sink, "_get_conn", return_value=conn), \
         patch.object(db_sink, "_put_conn"):
        db_sink.seed_from_csv([_txn()])
    assert cur.execute.call_count == 1
    conn.commit.assert_not_called()


def test_seed_swallows_integrity_error():
    import psycopg2
    conn, cur = _fake_conn(count=0)
    cur.execute.side_effect = [None, psycopg2.IntegrityError("fk")]
    with patch.object(db_sink, "_get_conn", return_value=conn), \
         patch.object(db_sink, "_put_conn"):
        db_sink.seed_from_csv([_txn()])  # must NOT raise
    conn.rollback.assert_called_once()
    conn.commit.assert_not_called()
