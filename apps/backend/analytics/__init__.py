"""C360 analytics: a snapshot-query dashboard over ``fct_c360_profile``.

The ``fct_c360_profile`` materialized table (one upserted row per customer,
demographics joined to account/transaction aggregates) is the serving layer a
real-time context engine reads. This package exposes a single snapshot of that
table as pre-aggregated analytics for the frontend dashboard.

Data path mirrors ``data_products``: a ``mock`` provider returns deterministic
fixtures (the default, so the demo runs with no infra) and a ``live`` provider
runs a bounded Flink SQL *snapshot query* against the table on Confluent Cloud.
``ANALYTICS_DATA_MODE`` selects between them.
"""
