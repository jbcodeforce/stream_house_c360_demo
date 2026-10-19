"""KAFKA_SEED_ON_START: lifespan publishes CSV records to Kafka (create events)."""

from __future__ import annotations

import bootstrap
import main
from accounts import inventory as accounts_inventory
from customers import inventory as customers_inventory
from transactions import inventory as transactions_inventory


def _capture(monkeypatch) -> dict[str, list]:
    sent: dict[str, list] = {"customers": [], "accounts": [], "transactions": []}
    monkeypatch.setattr(
        bootstrap.customers_producer, "produce_create",
        lambda r, flush_timeout=None: sent["customers"].append(r),
    )
    monkeypatch.setattr(
        bootstrap.accounts_producer, "produce_create",
        lambda r, flush_timeout=None: sent["accounts"].append(r),
    )
    monkeypatch.setattr(
        bootstrap.transactions_producer, "produce_create",
        lambda r, flush_timeout=None: sent["transactions"].append(r),
    )
    return sent


def test_seed_kafka_publishes_all_loaded_records(monkeypatch):
    customers = customers_inventory.load()
    accounts = accounts_inventory.load()
    txns = transactions_inventory.load()
    sent = _capture(monkeypatch)

    bootstrap.seed_kafka()

    assert sent["customers"] == customers
    assert sent["accounts"] == accounts
    assert sent["transactions"] == txns
    assert txns and all(t.account_id for t in txns)


def test_seed_kafka_swallows_producer_errors(monkeypatch):
    customers_inventory.load()

    def boom(*_a, **_k):
        raise RuntimeError("broker down")

    monkeypatch.setattr(bootstrap.customers_producer, "produce_create", boom)
    bootstrap.seed_kafka()  # must not raise


def _run_lifespan(monkeypatch, *, seed: bool, connector: bool) -> list[int]:
    import asyncio

    calls: list[int] = []
    monkeypatch.setattr(main.settings, "SINK", "kafka")
    monkeypatch.setattr(main.settings, "KAFKA_SEED_ON_START", seed)
    monkeypatch.setattr(main.settings, "CDC_CONNECTOR_ENABLED", connector)
    monkeypatch.setattr(main.bootstrap, "seed_kafka", lambda: calls.append(1))

    async def run() -> None:
        async with main.lifespan(main.app):
            pass

    asyncio.run(run())
    return calls


def test_lifespan_seeds_when_flag_set(monkeypatch):
    assert _run_lifespan(monkeypatch, seed=True, connector=False) == [1]


def test_lifespan_skips_when_flag_unset(monkeypatch):
    assert _run_lifespan(monkeypatch, seed=False, connector=False) == []


def test_lifespan_skips_when_cdc_connector_owns_kafka(monkeypatch):
    assert _run_lifespan(monkeypatch, seed=True, connector=True) == []
