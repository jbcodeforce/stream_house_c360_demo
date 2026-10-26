"""Unit tests for the customer-lookup providers: SQL building, escaping, and
mock/live row resolution."""

from __future__ import annotations

import pytest

from analytics import providers


# Patch providers._mode (not config.settings) so these unit tests are immune to
# the sys.modules config resets other suites perform on teardown.
@pytest.fixture
def mock_mode(monkeypatch):
    monkeypatch.setattr(providers, "_mode", lambda: "mock")


@pytest.fixture
def live_mode(monkeypatch):
    monkeypatch.setattr(providers, "_mode", lambda: "live")


# --- SQL building & escaping ---------------------------------------------

def test_sql_str_doubles_single_quotes():
    assert providers._sql_str("O'Brien") == "'O''Brien'"


def test_search_sql_is_a_lowercased_like_over_the_table():
    sql = providers._search_sql("smith")
    assert "FROM fct_c360_profile" in sql
    assert "LIKE '%smith%'" in sql
    assert "LOWER(first_name || ' ' || last_name)" in sql


def test_profile_sql_filters_by_equality_without_limit():
    sql = providers._profile_sql("00000000-0000-0000-0000-678901234568")
    assert "WHERE customer_id = '00000000-0000-0000-0000-678901234568'" in sql
    # No LIMIT: the full upsert changelog for the key is read then collapsed to
    # its latest row; a LIMIT would truncate to the stale first version.
    assert "LIMIT" not in sql


# --- Mock-mode resolution -------------------------------------------------

def test_search_customers_mock_finds_by_substring(mock_mode):
    rows, mode = providers.search_customers("sharma")
    assert mode == "mock"
    assert [r["last_name"] for r in rows] == ["Sharma"]


def test_search_customers_blank_returns_empty(mock_mode):
    assert providers.search_customers("   ") == ([], "mock")


def test_get_customer_profile_mock_by_id(mock_mode):
    row, mode = providers.get_customer_profile("00000000-0000-0000-0000-678901234568")
    assert mode == "mock"
    assert row is not None and row["first_name"] == "Thomas"


def test_get_customer_profile_unknown_id_returns_none(mock_mode):
    assert providers.get_customer_profile("00000000-0000-0000-0000-000000000000") == (None, "mock")


def test_get_customer_profile_rejects_malformed_id_without_query(live_mode, monkeypatch):
    # A malformed id must never reach the client (it would build a SQL literal).
    def boom(self, sql):  # pragma: no cover - must not be called
        raise AssertionError("client.run should not be called for a malformed id")

    monkeypatch.setattr(providers.FlinkSnapshotClient, "run", boom)
    assert providers.get_customer_profile("'; DROP TABLE x; --") == (None, "live")


def test_search_customers_live_delegates_to_client(live_mode, monkeypatch):
    captured = {}

    def fake_run(self, sql, max_rows=None, changelog_key=None):
        captured["sql"] = sql
        captured["max_rows"] = max_rows
        captured["changelog_key"] = changelog_key
        return [{"customer_id": "c1", "first_name": "Ada", "last_name": "Lovelace"}]

    monkeypatch.setattr(providers.FlinkSnapshotClient, "run", fake_run)
    rows, mode = providers.search_customers("ada")
    assert mode == "live"
    assert rows[0]["first_name"] == "Ada"
    assert "LIKE '%ada%'" in captured["sql"]
