"""Unit tests for the shared database connection pool in db.py."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import psycopg2.pool

import db


def test_ensure_pool_creates_threaded_pool():
    with patch("db._pool", None), \
         patch("config.settings.DATABASE_URL", "postgresql://user:pass@localhost:5432/testdb"):
        pool = db._ensure_pool()
        assert isinstance(pool, psycopg2.pool.ThreadedConnectionPool)
        assert pool.minconn == 1
        assert pool.maxconn == 10
        pool.closeall()


def test_cursor_context_manager_returns_conn_to_pool():
    mock_pool = MagicMock(spec=psycopg2.pool.ThreadedConnectionPool)
    mock_conn = MagicMock()
    mock_cur = MagicMock()
    mock_conn.cursor.return_value.__enter__.return_value = mock_cur
    mock_pool.getconn.return_value = mock_conn

    with patch.object(db, "_pool", mock_pool):
        with db.cursor(dict_rows=True, commit=True) as cur:
            assert cur == mock_cur

        mock_conn.commit.assert_called_once()
        mock_pool.putconn.assert_called_once_with(mock_conn)
