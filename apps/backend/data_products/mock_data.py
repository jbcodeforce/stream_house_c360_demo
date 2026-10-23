"""Deterministic mock figures for the Data Products graph's status/metrics.

Keyed by statement node id. Used by ``providers.MockProvider`` to build
``NodeStatus``/``NodeMetrics`` without talking to Confluent Cloud or Flink.
"""

from __future__ import annotations

# node_id -> (uptime_seconds, messages_in_per_sec, messages_out_per_sec,
#             compute_pool_cfu, sql)
STATEMENT_FIXTURES: dict[str, tuple[int, float, float, float, str]] = {
    "src_customers": (
        86_400,
        12.5,
        12.5,
        2.0,
        "CREATE TABLE src_customers AS SELECT * FROM `cdc.public.customers`;",
    ),
    "src_accounts": (
        86_400,
        8.2,
        8.2,
        2.0,
        "CREATE TABLE src_accounts AS SELECT * FROM `cdc.public.accounts`;",
    ),
    "src_transactions": (
        86_400,
        45.7,
        45.7,
        4.0,
        "CREATE TABLE src_transactions AS SELECT * FROM `cdc.public.transactions`;",
    ),
    "dim_customers": (
        86_100,
        12.5,
        12.5,
        2.0,
        "CREATE TABLE dim_customers AS SELECT * FROM src_customers;",
    ),
    "dim_transactions": (
        86_100,
        45.7,
        45.7,
        4.0,
        "CREATE TABLE dim_transactions AS SELECT * FROM src_transactions;",
    ),
    "dim_account_tx_agg": (
        85_800,
        45.7,
        5.1,
        4.0,
        "CREATE TABLE dim_account_tx_agg AS SELECT account_id, COUNT(*) AS tx_count "
        "FROM dim_transactions GROUP BY account_id;",
    ),
    "dim_tx_x_account": (
        85_800,
        45.7,
        45.7,
        4.0,
        "CREATE TABLE dim_tx_x_account AS SELECT t.*, a.* FROM dim_transactions t "
        "JOIN src_accounts a ON t.account_id = a.account_id;",
    ),
    "fct_c360_profile": (
        85_500,
        12.5,
        12.5,
        8.0,
        "CREATE TABLE fct_c360_profile AS SELECT c.*, agg.* FROM dim_customers c "
        "JOIN dim_account_tx_agg agg ON c.customer_id = agg.customer_id;",
    ),
}

# Lag scenario override for src_transactions when GRAPH_DATA_MODE=mock-lag:
# phase stays RUNNING, but the statement is reported lagging with a
# non-zero backlog.
LAGGING_NODE_ID = "src_transactions"
LAGGING_PENDING_RECORDS = 15_000
