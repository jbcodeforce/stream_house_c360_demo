# Backend – FastAPI Customers Microservice Plan

## Overview

Build a FastAPI backend under `apps/backend/` that exposes a **Customers CRU REST API** and
supports three write sinks controlled by a single `SINK` environment variable:

| `SINK` value | Write path | Read path |
|---|---|---|
| `postgres` *(default)* | INSERT / UPDATE RDS PostgreSQL | SELECT from PostgreSQL |
| `kafka` | Produce Avro message to Confluent topic | Read from in-memory CSV |

**CSV as bootstrap:** A seed file (`customers/data/customers.csv`) ships with 10 pre-populated
customers. When `SINK=postgres`, the backend auto-creates the schema on startup (migrating
`scripts/db/create_tables.py` logic) and seeds the `customers` table from the CSV if it is empty.
When `SINK=kafka`, the CSV remains the in-memory read store for all GET requests.

**Scope of this plan:** `customers` resource only. The `accounts` microservice is out of scope.

---

## Sub-Task 1 — Project Scaffolding (`pyproject.toml` + `uv.lock`)

**Intent:** Establish `apps/backend/` as a proper `uv`-managed Python project per project conventions
(AGENTS.md §D). All subsequent code depends on this being in place.

**Expected Outcomes:**
- `apps/backend/pyproject.toml` exists with `requires-python = ">=3.11"` and all required dependencies.
- `apps/backend/uv.lock` is committed.
- `uv run fastapi dev apps/backend/main.py` resolves without errors.

**Todo List:**
- [x] Create `apps/backend/pyproject.toml` with dependencies:
  `fastapi`, `uvicorn[standard]`, `psycopg2-binary`, `confluent-kafka`, `fastavro`,
  `pydantic-settings`, `python-dotenv`
- [x] Run `uv sync` inside `apps/backend/` to generate `uv.lock`

**Relevant Context:**
- `scripts/db/pyproject.toml` — reference for uv project shape in this repo
- AGENTS.md §D — Python tooling rules (uv, `requires-python = ">=3.11"`)

**Status:** `[x] done`

---

## Sub-Task 2 — Configuration (`config.py`)

**Intent:** Centralise all environment-driven settings in one place so every module imports from
`config` rather than calling `os.environ` directly.

**Expected Outcomes:**
- `apps/backend/config.py` exports a `Settings` singleton (Pydantic `BaseSettings`).
- `SINK` defaults to `"postgres"`.
- PostgreSQL and Kafka/SR credentials are all optional (only validated at runtime when the
  relevant sink is active).

**Todo List:**
- [x] Implement `Settings` class in `config.py` with fields:
  - `SINK: str = "postgres"` — allowed values: `postgres`, `kafka`
  - `DATABASE_URL: str | None` — `postgresql://user:pass@host:5432/dbname`
  - `KAFKA_BOOTSTRAP_SERVERS: str | None`
  - `SCHEMA_REGISTRY_URL: str | None`
  - `SCHEMA_REGISTRY_API_KEY: str | None`
  - `SCHEMA_REGISTRY_API_SECRET: str | None`
  - `KAFKA_TOPIC_CUSTOMERS: str = "customers"`
- [x] Expose a module-level `settings = Settings()` instance

**Relevant Context:**
- `apps/backend/config.py` — currently empty stub
- `pydantic-settings` is the dependency for env-var-backed settings

**Status:** `[x] done`

---

## Sub-Task 3 — Customer Data Model (`customers/models.py`)

**Intent:** Define the canonical Pydantic schema for a Customer, matching the PostgreSQL
`customers` table exactly (field names, types, optionality). This model is shared by all
three layers: API request/response, database writes, and Kafka Avro payload.

**Expected Outcomes:**
- `CustomerCreate` — fields accepted on POST (no `customer_id`, `created_at`, `updated_at`).
- `CustomerUpdate` — fields accepted on PUT (all optional except `customer_id`).
- `Customer` — full model including server-generated fields, used for responses.
- All models validated and importable.

**Todo List:**
- [x] Create `apps/backend/customers/models.py` with three Pydantic models:
  `CustomerCreate`, `CustomerUpdate`, `Customer`
- [x] Fields (from `create_tables.py` DDL):
  `customer_id` (UUID), `first_name`, `last_name`, `email`, `phone`, `date_of_birth`,
  `gender`, `address_line1`, `address_line2`, `city`, `state`, `postal_code`,
  `country` (default `"US"`), `customer_since` (date), `segment`, `status` (default `"ACTIVE"`),
  `created_at`, `updated_at`
- [x] Add `model_config = ConfigDict(from_attributes=True)` for ORM compatibility

**Relevant Context:**
- `scripts/db/create_tables.py` — DDL is the source of truth for field names and types
- All UUID fields use Python `uuid.UUID`; timestamps use `datetime`

**Status:** `[x] done`

---

## Sub-Task 4 — CSV Seed File + Inventory (`customers/inventory.py`)

**Intent:** The CSV file ships with 10 realistic seed customers and acts as the read store when
`SINK=kafka`. `inventory.py` owns all CSV read/write operations and the in-memory cache.

**Expected Outcomes:**
- `customers/data/customers.csv` exists with 10 rows, all columns matching the `Customer` model.
- `inventory.py` exposes: `load()`, `list_all()`, `get_by_id()`, `create()`, `update()`.
- `load()` is idempotent and returns a `list[Customer]`.

**Todo List:**
- [x] Create `apps/backend/customers/data/customers.csv` with 10 seed rows (realistic fake data,
  matching all model fields; UUIDs pre-generated)
- [x] Implement `apps/backend/customers/inventory.py`:
  - `load() -> list[Customer]` — reads the CSV into a list of `Customer` objects
  - `list_all() -> list[Customer]`
  - `get_by_id(customer_id: UUID) -> Customer | None`
  - `create(customer: Customer) -> Customer` — appends row to CSV
  - `update(customer: Customer) -> Customer` — rewrites CSV with updated row

**Relevant Context:**
- `customers/models.py` — `Customer` model shape drives CSV column headers
- When `SINK=kafka`, `main.py` will call `inventory.load()` at startup to populate the in-memory list

**Status:** `[x] done`

---

## Sub-Task 5 — PostgreSQL Sink (`customers/db_sink.py` + startup migration)

**Intent:** Migrate `scripts/db/create_tables.py` logic into the backend. On startup with
`SINK=postgres`, the app creates the schema if absent and seeds from CSV if the table is empty.
`db_sink.py` handles CRU SQL operations and reads.

**Expected Outcomes:**
- Schema (tables, indexes, triggers, CDC publication) is created on first startup.
- CSV seed is inserted if `customers` table is empty on startup.
- `db_sink.create()`, `db_sink.update()`, `db_sink.get_by_id()`, `db_sink.list_all()` work correctly.

**Todo List:**
- [x] Create `apps/backend/customers/db_sink.py` with:
  - `init_db()` — runs all DDL from `create_tables.py` (CREATE TABLE IF NOT EXISTS, indexes,
    triggers, CDC publication) using `DATABASE_URL` from settings
  - `seed_from_csv(customers: list[Customer])` — bulk INSERT of seed rows if table is empty
  - `create(customer: CustomerCreate) -> Customer` — INSERT returning full row
  - `update(customer_id: UUID, update: CustomerUpdate) -> Customer | None` — UPDATE SET … WHERE
  - `get_by_id(customer_id: UUID) -> Customer | None` — SELECT single row
  - `list_all() -> list[Customer]` — SELECT all rows
- [x] Use parameterised queries (`psycopg2`) — no string interpolation
- [x] Use `psycopg2` connection pooling (simple `psycopg2.pool.SimpleConnectionPool`)

**Relevant Context:**
- `scripts/db/create_tables.py` — DDL to replicate (do not import from scripts; copy the SQL strings)
- `scripts/db/db_utils.py` — connection helper patterns (reference only; backend uses `DATABASE_URL`)
- AGENTS.md §A — parameterised queries mandatory; TLS enforced via `sslmode=require` in URL

**Status:** `[x] done`

---

## Sub-Task 6 — Kafka Sink (`customers/kafka_producer.py`)

**Intent:** When `SINK=kafka`, write operations produce Avro-serialised messages to the
`customers` Confluent topic using the Confluent Schema Registry. This bypasses CDC and lets the
demo run without an RDS instance.

**Expected Outcomes:**
- `kafka_producer.py` produces `CustomerCreate` / `CustomerUpdate` payloads as Avro to the
  configured topic.
- Schema is registered automatically on first produce if absent.
- Produces a `op` field (`C` for create, `U` for update) to distinguish event types.

**Todo List:**
- [x] Create `apps/backend/customers/kafka_producer.py` with:
  - `init_producer()` — builds a `confluent_kafka.Producer` with SR `AvroSerializer`
  - `produce_create(customer: Customer)` — produces `{"op": "C", ...customer fields}`
  - `produce_update(customer: Customer)` — produces `{"op": "U", ...customer fields}`
- [x] Derive Avro schema from the `Customer` Pydantic model fields
- [x] Use `SCHEMA_REGISTRY_API_KEY` / `SCHEMA_REGISTRY_API_SECRET` for SR authentication
- [x] Call `producer.flush()` after each produce for demo reliability

**Relevant Context:**
- `config.py` — all Kafka/SR settings
- `customers/models.py` — source of truth for field names/types → Avro schema
- `confluent_kafka.schema_registry.avro.AvroSerializer` is the serializer to use

**Status:** `[x] done`

---

## Sub-Task 7 — Customer Service (`customers/service.py`)

**Intent:** A thin orchestration layer that dispatches CRU operations to the correct sink and
always uses `inventory.py` for reads when `SINK=kafka`.

**Expected Outcomes:**
- `service.create()` and `service.update()` route to the correct sink based on `settings.SINK`.
- `service.get_by_id()` and `service.list_all()` query PostgreSQL when `SINK=postgres`, or
  the in-memory CSV cache when `SINK=kafka`.

**Todo List:**
- [x] Create `apps/backend/customers/service.py` with:
  - `create(data: CustomerCreate) -> Customer`
  - `update(customer_id: UUID, data: CustomerUpdate) -> Customer | None`
  - `get_by_id(customer_id: UUID) -> Customer | None`
  - `list_all() -> list[Customer]`
- [x] Dispatch logic: `if settings.SINK == "postgres"` → `db_sink`, else → `inventory` + `kafka_producer`

**Relevant Context:**
- `config.py` — `settings.SINK` drives all branching
- `customers/db_sink.py`, `customers/kafka_producer.py`, `customers/inventory.py`

**Status:** `[x] done`

---

## Sub-Task 8 — API Router (`api/customer_resource.py`)

**Intent:** Expose the Customers CRU endpoints as a FastAPI `APIRouter`. The router delegates
all logic to `service.py` and handles HTTP status codes.

**Expected Outcomes:**
- `POST /customers` → 201 Created, returns `Customer`
- `GET /customers` → 200, returns `list[Customer]`
- `GET /customers/{customer_id}` → 200 or 404
- `PUT /customers/{customer_id}` → 200 or 404

**Todo List:**
- [x] Implement `apps/backend/api/customer_resource.py`:
  - Define `router = APIRouter(prefix="/customers", tags=["customers"])`
  - Wire all four endpoints to `service.*` calls
  - Return `404` with a clear message when a customer is not found

**Relevant Context:**
- `customers/service.py` — all business logic lives here, router is thin
- FastAPI `APIRouter` pattern

**Status:** `[x] done`

---

## Sub-Task 9 — Application Entrypoint (`main.py`)

**Intent:** Wire the FastAPI app, include routers, and run startup lifecycle hooks
(schema creation + CSV seeding for `SINK=postgres`; CSV cache load for `SINK=kafka`).

**Expected Outcomes:**
- `uv run uvicorn main:app --reload` starts the server.
- On startup, `SINK=postgres` initialises DB + seeds if empty; `SINK=kafka` loads CSV into memory.
- `/docs` (Swagger UI) lists all customer endpoints.
- A `/health` endpoint returns `{"status": "ok", "sink": "<current sink>"}`.

**Todo List:**
- [x] Implement `apps/backend/main.py`:
  - Create `app = FastAPI(title="C360 Backend", version="0.1.0")`
  - Add `@asynccontextmanager` lifespan handler that calls `db_sink.init_db()` +
    `db_sink.seed_from_csv()` when `SINK=postgres`, or `inventory.load()` when `SINK=kafka`
  - Include `customer_resource.router` with prefix `/api/v1`
  - Add `GET /health` endpoint
- [x] Add a `.env.example` file documenting all supported env vars

**Relevant Context:**
- `config.py` — `settings` singleton
- `api/customer_resource.py` — router to mount
- `customers/db_sink.py`, `customers/inventory.py` — startup hooks

**Status:** `[x] done`
