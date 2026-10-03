# Kafka Produce Config Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a runtime-toggleable "produce Kafka events on create/update/delete" setting — a config REST API, JSON-file persistence, best-effort dual-write emission in the postgres path, and a frontend Settings page.

**Architecture:** A new `config_store` module holds a single global `AppConfig { kafka_produce_enabled }`, cached in memory and persisted to a JSON file. A `/api/v1/config` router reads/writes it. `customers.service` emits a Kafka event (best-effort) after a successful postgres create/update/delete when the flag is on. The frontend gets a `/settings` page with an auto-saving toggle.

**Tech Stack:** Backend: FastAPI, pydantic, pytest. Frontend: React 19 + TypeScript, react-router-dom 7, lucide-react, vitest + RTL. Backend run with `uv`.

**Spec:** `docs/superpowers/specs/2026-10-02-kafka-produce-config-design.md`

## Global Constraints

- Backend paths are under `apps/backend/`; frontend under `apps/frontend/`.
- Config shape: `AppConfig { kafka_produce_enabled: bool }`, default `False`.
- Config REST paths: `GET /api/v1/config`, `PUT /api/v1/config`.
- Persistence: JSON file at `settings.RUNTIME_CONFIG_FILE` or default
  `apps/backend/runtime_config.json` (gitignored). Single-process; no locking.
- Behavior model: dual-write. Toggle governs ONLY the postgres path in
  `service.py`. The `SINK=kafka` path is unchanged.
- Emission is best-effort: a producer exception is logged and must NOT fail
  the DB operation.
- Default OFF so normal postgres operation needs no Kafka creds.
- Frontend: auto-save on toggle (optimistic, revert on failure).
- Commit after each task (Conventional Commits). Commit steps are the
  intended granularity once execution is authorized.
- Backend tests must stay infra-free (no DB, no live Kafka) and must NOT
  pollute the existing `test_customers_kafka` suite's module graph: prefer
  direct unit tests and bare-app TestClients over importing `main`.

## Review Focus

- **Producer raises while flag is ON** — create/update/delete must still
  return their DB result; the event is dropped with a logged warning.
  (Task 4.)
- **Config file missing or corrupt on GET/load** — `get_config` returns the
  default without raising. (Task 1.)
- **PUT fails to persist (unwritable file → 500)** — the frontend toggle
  reverts to its last known value and shows an error. (Task 6 covers the
  revert; backend surfaces the 500 via the normal path.)
- **Delete event payload completeness** — the delete path fetches the full
  customer BEFORE deleting so the "D" event carries a complete payload, and
  emits only when a row was actually deleted. (Task 4.)
- **Update that matches no row** — no "U" event is emitted when `update`
  returns None. (Task 4.)

---

## Task 1: Config store + settings field

**Files:**
- Create: `apps/backend/config_store.py`
- Modify: `apps/backend/config.py` (add `RUNTIME_CONFIG_FILE`)
- Modify: `.gitignore` (ignore `runtime_config.json`)
- Test: `apps/backend/tests/test_config_store.py`

**Interfaces:**
- Consumes: `settings` from `config`.
- Produces:
  - `AppConfig(BaseModel)` with `kafka_produce_enabled: bool = False`.
  - `get_config() -> AppConfig` (loads from file on first call, caches).
  - `set_config(config: AppConfig) -> AppConfig` (writes file + updates cache).
  - `reset_cache() -> None` (testing helper).

- [ ] **Step 1: Add the settings field**

In `apps/backend/config.py`, add to `Settings` (after `KAFKA_TOPIC_CUSTOMERS`):
```python
    RUNTIME_CONFIG_FILE: str | None = None
```

- [ ] **Step 2: Write the failing tests**

Create `apps/backend/tests/test_config_store.py`:
```python
"""Unit tests for the runtime config store (no app, no infra)."""

from __future__ import annotations

import config_store
from config import settings
from config_store import AppConfig


def test_default_when_file_absent(tmp_path, monkeypatch):
    monkeypatch.setattr(settings, "RUNTIME_CONFIG_FILE", str(tmp_path / "rc.json"))
    config_store.reset_cache()
    assert config_store.get_config().kafka_produce_enabled is False


def test_set_then_get_roundtrips_through_file(tmp_path, monkeypatch):
    f = tmp_path / "rc.json"
    monkeypatch.setattr(settings, "RUNTIME_CONFIG_FILE", str(f))
    config_store.reset_cache()

    config_store.set_config(AppConfig(kafka_produce_enabled=True))
    assert f.exists()

    config_store.reset_cache()  # force a reload from disk
    assert config_store.get_config().kafka_produce_enabled is True


def test_corrupt_file_falls_back_to_default(tmp_path, monkeypatch):
    f = tmp_path / "rc.json"
    f.write_text("{ not valid json")
    monkeypatch.setattr(settings, "RUNTIME_CONFIG_FILE", str(f))
    config_store.reset_cache()
    assert config_store.get_config().kafka_produce_enabled is False
```

- [ ] **Step 3: Run tests to verify they fail**

Run:
```bash
cd /Users/jerome/Documents/Code/stream_house_c360_demo/apps/backend
uv run pytest tests/test_config_store.py -v
```
Expected: FAIL — `config_store` module does not exist.

- [ ] **Step 4: Implement the store**

Create `apps/backend/config_store.py`:
```python
"""Runtime, user-editable application config, persisted to a JSON file.

Distinct from ``config.py`` (environment-derived settings). Holds a single
global flag controlling whether the service emits Kafka events on changes.
Single-process assumption: an in-memory cache backed by one JSON file.
"""

from __future__ import annotations

import logging
from pathlib import Path

from pydantic import BaseModel

from config import settings

logger = logging.getLogger("c360.config_store")

_DEFAULT_PATH = Path(__file__).parent / "runtime_config.json"

_cache: "AppConfig | None" = None


class AppConfig(BaseModel):
    kafka_produce_enabled: bool = False


def _path() -> Path:
    return Path(settings.RUNTIME_CONFIG_FILE) if settings.RUNTIME_CONFIG_FILE else _DEFAULT_PATH


def _load() -> AppConfig:
    path = _path()
    try:
        if path.exists():
            return AppConfig.model_validate_json(path.read_text())
    except Exception:
        logger.warning("Could not read runtime config at %s; using defaults", path, exc_info=True)
    return AppConfig()


def get_config() -> AppConfig:
    global _cache
    if _cache is None:
        _cache = _load()
    return _cache


def set_config(config: AppConfig) -> AppConfig:
    global _cache
    _path().write_text(config.model_dump_json())
    _cache = config
    return _cache


def reset_cache() -> None:
    """Testing helper: drop the in-memory cache so the next read reloads."""
    global _cache
    _cache = None
```

- [ ] **Step 5: Ignore the runtime file**

In `.gitignore` (repo root), add under the env section:
```
# Runtime app config (user-editable, not source)
**/runtime_config.json
```

- [ ] **Step 6: Run tests to verify they pass**

Run:
```bash
uv run pytest tests/test_config_store.py -v
```
Expected: PASS (3 tests).

- [ ] **Step 7: Commit**

```bash
cd /Users/jerome/Documents/Code/stream_house_c360_demo
git add apps/backend/config_store.py apps/backend/config.py apps/backend/tests/test_config_store.py .gitignore
git commit -m "feat: add runtime config store for kafka produce flag"
```

---

## Task 2: Config REST API

**Files:**
- Create: `apps/backend/api/config_resource.py`
- Modify: `apps/backend/main.py` (include the router)
- Test: `apps/backend/tests/test_config_api.py`

**Interfaces:**
- Consumes: `AppConfig`, `get_config`, `set_config` from `config_store`.
- Produces: `router` (APIRouter, prefix `/config`) with GET and PUT; wired
  into `main.app` under `/api/v1`.

- [ ] **Step 1: Write the failing tests**

Create `apps/backend/tests/test_config_api.py`:
```python
"""Config API tests against a bare app (no lifespan, no DB, no Kafka).

Mounting just the config router avoids importing main's sink machinery, so
these tests neither need infra nor pollute the test_customers_kafka module
graph. main wiring is verified by the Task 7 smoke test.
"""

from __future__ import annotations

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

import config_store
from api.config_resource import router
from config import settings


@pytest.fixture
def client(tmp_path, monkeypatch):
    monkeypatch.setattr(settings, "RUNTIME_CONFIG_FILE", str(tmp_path / "rc.json"))
    config_store.reset_cache()
    app = FastAPI()
    app.include_router(router, prefix="/api/v1")
    return TestClient(app)


def test_get_returns_default(client):
    resp = client.get("/api/v1/config")
    assert resp.status_code == 200
    assert resp.json() == {"kafka_produce_enabled": False}


def test_put_updates_and_persists(client):
    resp = client.put("/api/v1/config", json={"kafka_produce_enabled": True})
    assert resp.status_code == 200
    assert resp.json() == {"kafka_produce_enabled": True}

    # A subsequent GET reflects the change.
    assert client.get("/api/v1/config").json() == {"kafka_produce_enabled": True}
```

- [ ] **Step 2: Run tests to verify they fail**

Run:
```bash
cd /Users/jerome/Documents/Code/stream_house_c360_demo/apps/backend
uv run pytest tests/test_config_api.py -v
```
Expected: FAIL — `api.config_resource` does not exist.

- [ ] **Step 3: Implement the router**

Create `apps/backend/api/config_resource.py`:
```python
"""FastAPI router for the /config endpoints (runtime app config)."""

from __future__ import annotations

from fastapi import APIRouter

import config_store
from config_store import AppConfig

router = APIRouter(prefix="/config", tags=["config"])


@router.get("", response_model=AppConfig, status_code=200)
def get_config() -> AppConfig:
    return config_store.get_config()


@router.put("", response_model=AppConfig, status_code=200)
def update_config(data: AppConfig) -> AppConfig:
    return config_store.set_config(data)
```

- [ ] **Step 4: Wire into main**

In `apps/backend/main.py`, add the import next to the customers router import:
```python
from api.config_resource import router as config_router
```
and include it after the customers router line:
```python
app.include_router(config_router, prefix="/api/v1")
```

- [ ] **Step 5: Run tests to verify they pass**

Run:
```bash
uv run pytest tests/test_config_api.py -v
```
Expected: PASS (2 tests).

- [ ] **Step 6: Commit**

```bash
cd /Users/jerome/Documents/Code/stream_house_c360_demo
git add apps/backend/api/config_resource.py apps/backend/main.py apps/backend/tests/test_config_api.py
git commit -m "feat: add config REST API (GET/PUT /api/v1/config)"
```

---

## Task 3: produce_delete in the Kafka producer

**Files:**
- Modify: `apps/backend/customers/kafka_producer.py`
- Test: `apps/backend/tests/test_kafka_producer_delete.py`

**Interfaces:**
- Consumes: existing `_get_producer`, `_build_payload`, `settings`.
- Produces: `produce_delete(customer: Customer) -> None` (op `"D"`).

- [ ] **Step 1: Write the failing test**

Create `apps/backend/tests/test_kafka_producer_delete.py`:
```python
"""produce_delete builds a D event and produces+flushes via the producer."""

from __future__ import annotations

from datetime import date, datetime, timezone
from unittest.mock import MagicMock, patch
from uuid import uuid4

from customers import kafka_producer
from customers.models import Customer


def _customer() -> Customer:
    now = datetime.now(tz=timezone.utc)
    return Customer(
        customer_id=uuid4(),
        first_name="Ada",
        last_name="Lovelace",
        email="ada@x.io",
        country="US",
        customer_since=date(2020, 1, 1),
        status="ACTIVE",
        created_at=now,
        updated_at=now,
    )


def test_produce_delete_produces_d_event():
    customer = _customer()
    fake_producer = MagicMock()
    with patch.object(kafka_producer, "_get_producer", return_value=fake_producer):
        kafka_producer.produce_delete(customer)

    fake_producer.produce.assert_called_once()
    kwargs = fake_producer.produce.call_args.kwargs
    assert kwargs["key"] == str(customer.customer_id)
    assert kwargs["value"]["op"] == "D"
    fake_producer.flush.assert_called_once()
```

- [ ] **Step 2: Run the test to verify it fails**

Run:
```bash
cd /Users/jerome/Documents/Code/stream_house_c360_demo/apps/backend
uv run pytest tests/test_kafka_producer_delete.py -v
```
Expected: FAIL — `produce_delete` is not defined.

- [ ] **Step 3: Implement produce_delete**

In `apps/backend/customers/kafka_producer.py`, add after `produce_update`:
```python
def produce_delete(customer: Customer) -> None:
    """Produce a customer-deleted event (``op="D"``) to the Kafka topic."""
    producer = _get_producer()
    producer.produce(
        topic=settings.KAFKA_TOPIC_CUSTOMERS,
        key=str(customer.customer_id),
        value=_build_payload(customer, "D"),
    )
    producer.flush()
```

- [ ] **Step 4: Run the test to verify it passes**

Run:
```bash
uv run pytest tests/test_kafka_producer_delete.py -v
```
Expected: PASS (1 test).

- [ ] **Step 5: Commit**

```bash
cd /Users/jerome/Documents/Code/stream_house_c360_demo
git add apps/backend/customers/kafka_producer.py apps/backend/tests/test_kafka_producer_delete.py
git commit -m "feat: add produce_delete to kafka producer"
```

---

## Task 4: Service dual-write emission (postgres path)

**Files:**
- Modify: `apps/backend/customers/service.py`
- Test: `apps/backend/tests/test_service_emit.py`

**Interfaces:**
- Consumes: `config_store.get_config`, `db_sink`, `kafka_producer`,
  `produce_delete` (Task 3), `settings.SINK`.
- Produces: `_maybe_emit(op: str, customer: Customer) -> None` and emission
  calls inside the postgres branches of `create`, `update`, `delete_by_id`.

- [ ] **Step 1: Write the failing tests**

Create `apps/backend/tests/test_service_emit.py`:
```python
"""Postgres-path dual-write emission is gated by the config flag.

All collaborators are patched on the service module's own references, so no
module reimport or SINK env juggling is needed.
"""

from __future__ import annotations

from datetime import date, datetime, timezone
from unittest.mock import MagicMock
from uuid import uuid4

import pytest

from customers import service
from customers.models import Customer, CustomerCreate, CustomerUpdate
from config_store import AppConfig


def _customer() -> Customer:
    now = datetime.now(tz=timezone.utc)
    return Customer(
        customer_id=uuid4(), first_name="Ada", last_name="Lovelace",
        email="ada@x.io", country="US", customer_since=date(2020, 1, 1),
        status="ACTIVE", created_at=now, updated_at=now,
    )


@pytest.fixture
def pg(monkeypatch):
    """Force the postgres path and stub the DB + producer on service."""
    monkeypatch.setattr(service.settings, "SINK", "postgres")
    monkeypatch.setattr(service.db_sink, "create", MagicMock())
    monkeypatch.setattr(service.db_sink, "update", MagicMock())
    monkeypatch.setattr(service.db_sink, "get_by_id", MagicMock())
    monkeypatch.setattr(service.db_sink, "delete_by_id", MagicMock())
    monkeypatch.setattr(service.kafka_producer, "produce_create", MagicMock())
    monkeypatch.setattr(service.kafka_producer, "produce_update", MagicMock())
    monkeypatch.setattr(service.kafka_producer, "produce_delete", MagicMock())
    return service


def _enable(monkeypatch, enabled: bool):
    monkeypatch.setattr(
        service.config_store, "get_config",
        lambda: AppConfig(kafka_produce_enabled=enabled),
    )


def test_create_emits_when_enabled(pg, monkeypatch):
    c = _customer()
    pg.db_sink.create.return_value = c
    _enable(monkeypatch, True)
    pg.create(CustomerCreate(first_name="Ada", last_name="L", email="a@b.c"))
    pg.kafka_producer.produce_create.assert_called_once_with(c)


def test_create_does_not_emit_when_disabled(pg, monkeypatch):
    pg.db_sink.create.return_value = _customer()
    _enable(monkeypatch, False)
    pg.create(CustomerCreate(first_name="Ada", last_name="L", email="a@b.c"))
    pg.kafka_producer.produce_create.assert_not_called()


def test_update_skips_emit_when_no_row(pg, monkeypatch):
    pg.db_sink.update.return_value = None
    _enable(monkeypatch, True)
    result = pg.update(uuid4(), CustomerUpdate(city="NYC"))
    assert result is None
    pg.kafka_producer.produce_update.assert_not_called()


def test_delete_emits_full_payload_when_enabled(pg, monkeypatch):
    c = _customer()
    pg.db_sink.get_by_id.return_value = c
    pg.db_sink.delete_by_id.return_value = True
    _enable(monkeypatch, True)
    assert pg.delete_by_id(c.customer_id) is True
    pg.kafka_producer.produce_delete.assert_called_once_with(c)


def test_create_still_succeeds_when_producer_raises(pg, monkeypatch):
    c = _customer()
    pg.db_sink.create.return_value = c
    _enable(monkeypatch, True)
    pg.kafka_producer.produce_create.side_effect = RuntimeError("broker down")
    # Best-effort: the DB result is returned despite the producer failing.
    assert pg.create(CustomerCreate(first_name="Ada", last_name="L", email="a@b.c")) is c
```

- [ ] **Step 2: Run the tests to verify they fail**

Run:
```bash
cd /Users/jerome/Documents/Code/stream_house_c360_demo/apps/backend
uv run pytest tests/test_service_emit.py -v
```
Expected: FAIL — `service.config_store` / emission behavior not present
(AttributeError on `service.config_store`, and produce assertions unmet).

- [ ] **Step 3: Implement emission in service**

Edit `apps/backend/customers/service.py`. Add imports near the top (after the
existing imports):
```python
import logging

import config_store

logger = logging.getLogger("c360.service")
```

Add the helper (after the imports, before `create`):
```python
def _maybe_emit(op: str, customer: Customer) -> None:
    """Best-effort Kafka emission for the postgres path, gated by config.

    A producer failure is logged and swallowed — Postgres is the source of
    truth and the DB operation must not fail because Kafka is unavailable.
    """
    if not config_store.get_config().kafka_produce_enabled:
        return
    try:
        if op == "C":
            kafka_producer.produce_create(customer)
        elif op == "U":
            kafka_producer.produce_update(customer)
        elif op == "D":
            kafka_producer.produce_delete(customer)
    except Exception:
        logger.warning(
            "Kafka emit failed (op=%s, id=%s)", op, customer.customer_id, exc_info=True
        )
```

Update the **postgres branches** only:

`create` — replace `return db_sink.create(data)` with:
```python
        customer = db_sink.create(data)
        _maybe_emit("C", customer)
        return customer
```

`update` — replace `return db_sink.update(customer_id, data)` with:
```python
        customer = db_sink.update(customer_id, data)
        if customer is not None:
            _maybe_emit("U", customer)
        return customer
```

`delete_by_id` — replace `return db_sink.delete_by_id(customer_id)` with:
```python
        customer = db_sink.get_by_id(customer_id)
        deleted = db_sink.delete_by_id(customer_id)
        if deleted and customer is not None:
            _maybe_emit("D", customer)
        return deleted
```

Leave every `kafka`-branch line unchanged.

- [ ] **Step 4: Run the tests to verify they pass**

Run:
```bash
uv run pytest tests/test_service_emit.py -v
```
Expected: PASS (5 tests).

- [ ] **Step 5: Run the full backend suite (no regressions)**

Run:
```bash
uv run pytest -q
```
Expected: all pre-existing tests still pass; the kafka suite is unaffected
(the kafka branch was not changed). Note any pre-existing unrelated failures.

- [ ] **Step 6: Commit**

```bash
cd /Users/jerome/Documents/Code/stream_house_c360_demo
git add apps/backend/customers/service.py apps/backend/tests/test_service_emit.py
git commit -m "feat: best-effort kafka emission on postgres writes, gated by config"
```

---

## Task 5: Frontend config types + API module

**Files:**
- Create: `apps/frontend/src/types/config.ts`
- Create: `apps/frontend/src/api/config.ts`
- Test: `apps/frontend/src/api/config.test.ts`

**Interfaces:**
- Consumes: `request` from `./client`.
- Produces:
  - `AppConfig { kafka_produce_enabled: boolean }`.
  - `getConfig(): Promise<AppConfig>` → `GET /config`.
  - `updateConfig(data: AppConfig): Promise<AppConfig>` → `PUT /config`.

- [ ] **Step 1: Write the types**

Create `apps/frontend/src/types/config.ts`:
```ts
export interface AppConfig {
  kafka_produce_enabled: boolean
}
```

- [ ] **Step 2: Write the failing tests**

Create `apps/frontend/src/api/config.test.ts`:
```ts
import { afterEach, describe, expect, it, vi } from 'vitest'
import * as client from './client'
import { getConfig, updateConfig } from './config'

afterEach(() => vi.restoreAllMocks())

describe('config api', () => {
  it('getConfig GETs /config', async () => {
    const spy = vi.spyOn(client, 'request').mockResolvedValue({ kafka_produce_enabled: false })
    await getConfig()
    expect(spy).toHaveBeenCalledWith('/config')
  })

  it('updateConfig PUTs the body to /config', async () => {
    const spy = vi.spyOn(client, 'request').mockResolvedValue({ kafka_produce_enabled: true })
    await updateConfig({ kafka_produce_enabled: true })
    expect(spy).toHaveBeenCalledWith('/config', {
      method: 'PUT',
      body: JSON.stringify({ kafka_produce_enabled: true }),
    })
  })
})
```

- [ ] **Step 3: Run tests to verify they fail**

Run:
```bash
cd /Users/jerome/Documents/Code/stream_house_c360_demo/apps/frontend
npx vitest run src/api/config.test.ts
```
Expected: FAIL — `./config` module not found.

- [ ] **Step 4: Implement the module**

Create `apps/frontend/src/api/config.ts`:
```ts
import type { AppConfig } from '../types/config'
import { request } from './client'

export function getConfig(): Promise<AppConfig> {
  return request<AppConfig>('/config')
}

export function updateConfig(data: AppConfig): Promise<AppConfig> {
  return request<AppConfig>('/config', {
    method: 'PUT',
    body: JSON.stringify(data),
  })
}
```

- [ ] **Step 5: Run tests to verify they pass**

Run:
```bash
npx vitest run src/api/config.test.ts
```
Expected: PASS (2 tests).

- [ ] **Step 6: Commit**

```bash
cd /Users/jerome/Documents/Code/stream_house_c360_demo
git add apps/frontend/src/types/config.ts apps/frontend/src/api/config.ts apps/frontend/src/api/config.test.ts
git commit -m "feat: add frontend config api module and types"
```

---

## Task 6: Settings page + navigation

**Files:**
- Create: `apps/frontend/src/pages/ConfigPage.tsx`
- Modify: `apps/frontend/src/components/Sidebar.tsx` (add Settings link)
- Modify: `apps/frontend/src/components/Sidebar.test.tsx` (assert the link)
- Modify: `apps/frontend/src/App.tsx` (add `/settings` route)
- Modify: `apps/frontend/src/styles/index.css` (toggle styles)
- Test: `apps/frontend/src/pages/ConfigPage.test.tsx`

**Interfaces:**
- Consumes: `getConfig`, `updateConfig` from `../api/config`; `ApiError`.
- Produces: `ConfigPage` default export; `/settings` route; a 4th sidebar link.

- [ ] **Step 1: Write the failing tests**

Create `apps/frontend/src/pages/ConfigPage.test.tsx`:
```tsx
import { render, screen, waitFor } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { afterEach, describe, expect, it, vi } from 'vitest'
import * as api from '../api/config'
import { ApiError } from '../api/client'
import ConfigPage from './ConfigPage'

afterEach(() => vi.restoreAllMocks())

describe('ConfigPage', () => {
  it('renders the current config value', async () => {
    vi.spyOn(api, 'getConfig').mockResolvedValue({ kafka_produce_enabled: true })
    render(<ConfigPage />)
    const toggle = await screen.findByRole('checkbox')
    expect(toggle).toBeChecked()
  })

  it('auto-saves the new value when toggled', async () => {
    vi.spyOn(api, 'getConfig').mockResolvedValue({ kafka_produce_enabled: false })
    const update = vi.spyOn(api, 'updateConfig').mockResolvedValue({ kafka_produce_enabled: true })
    render(<ConfigPage />)
    const toggle = await screen.findByRole('checkbox')
    await userEvent.click(toggle)
    await waitFor(() =>
      expect(update).toHaveBeenCalledWith({ kafka_produce_enabled: true }),
    )
    expect(toggle).toBeChecked()
  })

  it('reverts the toggle and shows an error when save fails', async () => {
    vi.spyOn(api, 'getConfig').mockResolvedValue({ kafka_produce_enabled: false })
    vi.spyOn(api, 'updateConfig').mockRejectedValue(new ApiError(500, 'disk full'))
    render(<ConfigPage />)
    const toggle = await screen.findByRole('checkbox')
    await userEvent.click(toggle)
    expect(await screen.findByText(/disk full/)).toBeInTheDocument()
    await waitFor(() => expect(toggle).not.toBeChecked())
  })
})
```

- [ ] **Step 2: Run tests to verify they fail**

Run:
```bash
cd /Users/jerome/Documents/Code/stream_house_c360_demo/apps/frontend
npx vitest run src/pages/ConfigPage.test.tsx
```
Expected: FAIL — `./ConfigPage` not found.

- [ ] **Step 3: Implement ConfigPage**

Create `apps/frontend/src/pages/ConfigPage.tsx`:
```tsx
import { useEffect, useState } from 'react'
import { ApiError } from '../api/client'
import { getConfig, updateConfig } from '../api/config'

export default function ConfigPage() {
  const [enabled, setEnabled] = useState(false)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)

  useEffect(() => {
    getConfig()
      .then((c) => setEnabled(c.kafka_produce_enabled))
      .catch((e) => setError(e instanceof ApiError ? e.message : 'Failed to load config'))
      .finally(() => setLoading(false))
  }, [])

  async function onToggle() {
    const next = !enabled
    setEnabled(next) // optimistic
    setError(null)
    try {
      await updateConfig({ kafka_produce_enabled: next })
    } catch (e) {
      setEnabled(!next) // revert
      setError(e instanceof ApiError ? e.message : 'Failed to save config')
    }
  }

  if (loading) return <div className="status-msg">Loading…</div>

  return (
    <div>
      <div className="page-header">
        <h1>Settings</h1>
      </div>
      {error && <div className="status-msg status-msg--error">{error}</div>}
      <label className="switch-row">
        <input type="checkbox" checked={enabled} onChange={onToggle} />
        <span>Produce Kafka events on create / update / delete</span>
      </label>
      <p className="status-msg">
        When on, each customer create, update, or delete also emits a Kafka
        event (best-effort). Postgres remains the source of truth.
      </p>
    </div>
  )
}
```

- [ ] **Step 4: Add toggle styles**

Append to `apps/frontend/src/styles/index.css`:
```css
/* Settings toggle */
.switch-row { display: flex; align-items: center; gap: 12px; font-size: 14px; cursor: pointer; }
.switch-row input { width: 18px; height: 18px; cursor: pointer; }
```

- [ ] **Step 5: Add the sidebar link and update its test**

In `apps/frontend/src/components/Sidebar.tsx`, add `Settings` to the lucide
import and a 4th entry to `links`:
```tsx
import { Users, Wallet, ArrowLeftRight, Settings } from 'lucide-react'
```
```tsx
  { to: '/settings', label: 'Settings', Icon: Settings },
```

In `apps/frontend/src/components/Sidebar.test.tsx`, add inside the existing
test, after the transactions assertion:
```tsx
    expect(screen.getByRole('link', { name: /settings/i })).toHaveAttribute('href', '/settings')
```

- [ ] **Step 6: Add the route**

In `apps/frontend/src/App.tsx`, add the import:
```tsx
import ConfigPage from './pages/ConfigPage'
```
and the route (after the transactions route, inside the layout `<Route>`):
```tsx
        <Route path="/settings" element={<ConfigPage />} />
```

- [ ] **Step 7: Run tests to verify they pass**

Run:
```bash
npx vitest run src/pages/ConfigPage.test.tsx src/components/Sidebar.test.tsx
```
Expected: PASS (ConfigPage 3, Sidebar 1).

- [ ] **Step 8: Full suite, typecheck, build**

Run:
```bash
npx vitest run
npx tsc --noEmit
npm run build
```
Expected: all tests pass; no type errors; build succeeds.

- [ ] **Step 9: Commit**

```bash
cd /Users/jerome/Documents/Code/stream_house_c360_demo
git add apps/frontend/src
git commit -m "feat: add settings page with kafka produce toggle"
```

---

## Task 7: Docs + end-to-end smoke verification

**Files:**
- Modify: `apps/frontend/README.md` (mention the Settings page)
- Modify: `apps/backend/README.md` (document the config endpoint + flag) — if
  the file documents endpoints; otherwise skip and note in the commit.

- [ ] **Step 1: Document the frontend Settings page**

In `apps/frontend/README.md`, under the Structure list, note the Settings
page and the config API module (one line each).

- [ ] **Step 2: Start the backend (infra-free) on a free port**

Run (background), picking a port not already in use (8000 may be taken):
```bash
cd /Users/jerome/Documents/Code/stream_house_c360_demo/apps/backend
SINK=kafka KAFKA_BOOTSTRAP_SERVERS=mock:9092 SCHEMA_REGISTRY_URL=http://mock-sr \
  SCHEMA_REGISTRY_API_KEY=k SCHEMA_REGISTRY_API_SECRET=s RUNTIME_CONFIG_FILE=/tmp/c360_rc.json \
  uv run uvicorn main:app --port 8001 --log-level warning &
```

- [ ] **Step 3: Verify the config endpoint end-to-end**

Run:
```bash
rm -f /tmp/c360_rc.json
curl -s http://localhost:8001/api/v1/config            # {"kafka_produce_enabled":false}
curl -s -X PUT http://localhost:8001/api/v1/config \
  -H 'Content-Type: application/json' \
  -d '{"kafka_produce_enabled":true}'                   # {"kafka_produce_enabled":true}
curl -s http://localhost:8001/api/v1/config            # persisted: true
cat /tmp/c360_rc.json                                   # {"kafka_produce_enabled":true}
```
Expected: GET default false; PUT returns true; GET reflects true; file written.
Stop the backend process when done.

- [ ] **Step 4: Commit docs**

```bash
cd /Users/jerome/Documents/Code/stream_house_c360_demo
git add apps/frontend/README.md apps/backend/README.md
git commit -m "docs: document kafka produce config endpoint and settings page"
```

- [ ] **Step 5: Manual browser check (hand to user)**

Report that the user should: start backend (postgres) + frontend, open
`/settings`, flip the toggle, and confirm it persists across a reload.
Note that observing actual Kafka events requires real Kafka/SR creds and a
running broker.

---

## Self-Review Notes

- **Spec coverage:** config store + settings field (Task 1), config REST API
  + main wiring (Task 2), produce_delete (Task 3), service dual-write gating
  (Task 4), frontend types+api (Task 5), Settings page + nav + route (Task
  6), docs + E2E (Task 7). All spec sections mapped.
- **Review Focus coverage:** producer raises → op succeeds (Task 4,
  test_create_still_succeeds_when_producer_raises); corrupt/missing file →
  default (Task 1); PUT failure reverts toggle (Task 6,
  reverts-and-shows-error); delete payload completeness (Task 4,
  test_delete_emits_full_payload); update no-row → no emit (Task 4,
  test_update_skips_emit_when_no_row). All pinned.
- **Type consistency:** `AppConfig` field `kafka_produce_enabled` consistent
  across backend (`config_store`, `config_resource`) and frontend
  (`types/config`, `api/config`, `ConfigPage`). `_maybe_emit` op codes
  `C/U/D` match `produce_create/update/delete`.
- **Isolation:** backend tests use direct units (Task 1,4), a bare-app
  TestClient (Task 2), and object patching (Task 3,4) — none import `main`
  in a way that pollutes `test_customers_kafka`; the SINK=kafka branch is
  untouched so that suite is unaffected.
