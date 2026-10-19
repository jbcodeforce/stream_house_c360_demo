"""Database bootstrap: create schema + CDC publication and seed from CSV.

Defined once and reused by the FastAPI startup lifespan (see ``main.py``) and
by local tooling. Run directly against any PostgreSQL — RDS included — with
``DATABASE_URL`` set:

    DATABASE_URL=postgresql://... SINK=postgres python bootstrap.py

Everything here is idempotent: tables use ``CREATE TABLE IF NOT EXISTS``,
each table adds itself to the CDC publication only if absent, and seeding
runs only when a table is empty.
"""

from __future__ import annotations

import logging

from accounts import (
    db_sink as accounts_db_sink,
    inventory as accounts_inventory,
    kafka_producer as accounts_producer,
)
from customers import (
    db_sink as customers_db_sink,
    inventory as customers_inventory,
    kafka_producer as customers_producer,
)
from transactions import (
    db_sink as transactions_db_sink,
    inventory as transactions_inventory,
    kafka_producer as transactions_producer,
)

logger = logging.getLogger("c360.bootstrap")

_SEED_FLUSH_TIMEOUT = 5.0


def init_and_seed() -> None:
    """Create all tables + the CDC publication and seed each table if empty.

    Order matters: customers before accounts before transactions so the
    foreign keys resolve during seeding.
    """
    customers_db_sink.init_db()
    customers = customers_inventory.load()
    customers_db_sink.seed_from_csv(customers)

    accounts_db_sink.init_db()
    accounts = accounts_inventory.load()
    accounts_db_sink.seed_from_csv(accounts)

    transactions_db_sink.init_db()
    transactions = transactions_inventory.load()
    transactions_db_sink.seed_from_csv(transactions)

    logger.info(
        "Postgres ready — schema initialised, CDC publication set, tables seeded if empty"
    )


def seed_kafka() -> None:
    """Publish the CSV-loaded records to the cdc.public.* topics as create events.

    Reads the already-loaded inventories (call after ``inventory.load()``), in
    FK order: customers → accounts → transactions. Best-effort: a failure is
    logged and never prevents the app from starting.
    """
    counts = {"customers": 0, "accounts": 0, "transactions": 0}
    try:
        for customer in customers_inventory.list_all():
            customers_producer.produce_create(customer, flush_timeout=_SEED_FLUSH_TIMEOUT)
            counts["customers"] += 1
        for account in accounts_inventory.list_all():
            accounts_producer.produce_create(account, flush_timeout=_SEED_FLUSH_TIMEOUT)
            counts["accounts"] += 1
        for txn in transactions_inventory.list_all():
            transactions_producer.produce_create(txn, flush_timeout=_SEED_FLUSH_TIMEOUT)
            counts["transactions"] += 1
    except Exception:
        logger.warning("Kafka seed failed after %s", counts, exc_info=True)
        return
    logger.info("Kafka seeded from CSV — %s", counts)


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s"
    )
    init_and_seed()
