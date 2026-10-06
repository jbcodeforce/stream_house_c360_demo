# Web App and Backend Design

This note is for explaining the code of the backend and vite application.

## Requirements addressed
 
* [x] Web App to present customers page and be able to add new customer, read the detail of one customer, update one, or delete one
* [x] Pages for accounts CRUD operations
* [x] Pages for transactions CR operations
* [ ] Local persistence with Postgresql database running in container
* [ ] Kafka producer to write Debezium schema compliant records.
* [x] TDD for backend and frontend

## Design

The architecture is:

![](./diagrams/stream-cut1-local-arch.drawio.png)

The stack is a classic single-page app over a REST backend, with Kafka/CDC as a
secondary sink:

![](./diagrams/web_app_flow.drawio.png)

### Frontend ↔ backend integration

The frontend (`apps/frontend`, React 19 + Vite + React Router) never calls `fetch` directly from a page. It is layered so the HTTP concern lives in one place:

* `src/config.ts` — resolves the backend base URL once: `VITE_API_BASE_URL ?? http://localhost:8000/api/v1`.
* `src/api/client.ts` — a thin `request<T>()` wrapper around `fetch` that sets JSON headers, unwraps `204 No Content`, and turns non-2xx responses into a typed `ApiError` (reading FastAPI's `{ "detail": ... }` body).
* `src/api/{customers,accounts,transactions,config}.ts` — one module per domain, exposing typed CRUD functions (`listAccounts`, `getAccount`, `createAccount`, …) that map 1:1 to the REST endpoints.
* `src/types/*` — TypeScript interfaces mirroring the backend Pydantic models, so the request/response contract is checked at compile time.
* `src/pages/*` and `src/components/*` — views call only the `api/` functions; they never know the base URL or status-code handling.

On the backend side, `main.py` mounts every domain router under `/api/v1` and enables CORS for the Vite dev origin (`http://localhost:5173`). The browser's
`VITE_API_BASE_URL` and the backend's CORS allow-list are the two ends of the same contract.

### Backend design pattern (models · DAO · producer · inventory)

Each domain — **customers**, **accounts**, **transactions** — is an identical vertical slice of five small modules. Routing and HTTP stay thin; persistence,
event emission, and seeding are each isolated behind their own module:

| Layer | Module | Responsibility |
|-------|--------|----------------|
| **API** | `api/<domain>_resource.py` | FastAPI router. Validates input via the models, maps results/`404`/`409` to HTTP. No business logic. |
| **Service** | `<domain>/service.py` | Dispatch/orchestration. Calls the DAO, then does a *best-effort* Kafka emit. Postgres is the source of truth; a producer failure is logged, never fails the request. |
| **Models** | `<domain>/models.py` | Pydantic `*Create` / `*Update` / full models. The single schema contract shared by the API, the DAO, and (mirrored) the frontend types. |
| **DAO** | `<domain>/db_sink.py` | Data-access object: the domain's PostgreSQL DDL, column list, row↔model mapping, and CRUD — built on the shared pool in `db.py`. |
| **Producer** | `<domain>/kafka_producer.py` | Serializes a model into a Debezium change-event envelope (`before`/`after`/`source`/`op`) and produces Avro to Kafka via Schema Registry. |
| **Inventory** | `<domain>/inventory.py` | CSV-backed seed store. Loads the committed seed CSV into typed models used to seed PostgreSQL (and, in `SINK=kafka` mode, acts as the in-memory store). |

Cross-cutting modules keep the slices DRY:

* `db.py` — one lazily-created connection pool, a `cursor()` context manager (commit/rollback/return-to-pool), and reusable `init_schema` / `seed_if_empty`
  / CDC-publication helpers. Each `db_sink` supplies only its own DDL and mapping; the connection and transaction mechanics are written once here.
* `bootstrap.py` — on startup (`main.py` lifespan, `SINK=postgres`), creates all tables + the `c360_cdc_publication`, then seeds **customers → accounts →
  transactions** in FK order, each table only if empty (idempotent).
* `config.py` — environment-derived `Settings` (`SINK`, `CDC_CONNECTOR_ENABLED`, `DATABASE_URL`, Kafka/SR credentials, topic names).
* `config_store.py` — a runtime, user-editable JSON toggle (`kafka_produce_enabled`) exposed via `/api/v1/config`, distinct from the env-derived `config.py`.

**Who owns Kafka.** `service._maybe_emit()` only publishes when the backend is the designated producer: it is suppressed when `CDC_CONNECTOR_ENABLED=true`
(a managed Debezium/CDC connector on RDS/remote PostgreSQL owns the `cdc.public.*` topics, so the app must not double-publish) and gated by the
runtime `kafka_produce_enabled` flag. In local mode (no connector) the app is the only producer; against RDS the connector is. See `scripts/run_dev.sh` for
how that flag is resolved per target.

A new domain is added by copying the five-module slice, registering its router in `main.py`, and adding its `init_db` + `seed_from_csv` to `bootstrap.py`.

## Execute during development


* Start the database server
    ```sh
    cd apps/backend
    ./start_local_pg_server.sh
    ```

* Start the backend
    ```sh
    cd app/backend
    uv run uvicorn main:app --reload 
    ```

    Access to REST API: [http://localhost:8000/docs](http://localhost:8000/docs)


* Start the user interface
    ```sh
    cd apps/frontend
    npm run dev
    ```

    Access to the webapp: [http://localhost:5173/](http://localhost:5173/)
    
## Database schema

The database has three main tables:

* Accounts:
    ```sql
        CREATE TABLE IF NOT EXISTS accounts (
        account_id      UUID            PRIMARY KEY DEFAULT gen_random_uuid(),
        customer_id     UUID            NOT NULL REFERENCES customers(customer_id),
        account_number  VARCHAR(20)     NOT NULL UNIQUE,
        account_type    VARCHAR(30)     NOT NULL,  -- 'CHECKING', 'SAVINGS', 'CREDIT', 'LOAN'
        currency        CHAR(3)         NOT NULL DEFAULT 'USD',
        balance         NUMERIC(18, 2)  NOT NULL DEFAULT 0.00,
        credit_limit    NUMERIC(18, 2),            -- populated for CREDIT / LOAN accounts
        opened_date     DATE            NOT NULL DEFAULT CURRENT_DATE,
        closed_date     DATE,
        status          VARCHAR(20)     NOT NULL DEFAULT 'ACTIVE',
        created_at      TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
        updated_at      TIMESTAMPTZ     NOT NULL DEFAULT NOW()
    )
    ```

* Customers
    ```sql
        CREATE TABLE IF NOT EXISTS customers (
        customer_id     UUID            PRIMARY KEY DEFAULT gen_random_uuid(),
        first_name      VARCHAR(100)    NOT NULL,
        last_name       VARCHAR(100)    NOT NULL,
        email           VARCHAR(255)    NOT NULL UNIQUE,
        phone           VARCHAR(30),
        date_of_birth   TIMESTAMPTZ,
        gender          VARCHAR(20),
        address_line1   VARCHAR(255),
        address_line2   VARCHAR(255),
        city            VARCHAR(100),
        state           VARCHAR(100),
        postal_code     VARCHAR(20),
        country         VARCHAR(60)     NOT NULL DEFAULT 'US',
        customer_since  DATE            NOT NULL DEFAULT CURRENT_DATE,
        segment         VARCHAR(50),    -- e.g. 'RETAIL', 'SMB', 'ENTERPRISE'
        status          VARCHAR(20)     NOT NULL DEFAULT 'ACTIVE',
        created_at      TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
        updated_at      TIMESTAMPTZ     NOT NULL DEFAULT NOW()
    )
    ```
    
* Transactions:
    ```sql
        CREATE TABLE IF NOT EXISTS transactions (
        transaction_id   UUID           PRIMARY KEY DEFAULT gen_random_uuid(),
        account_id       UUID           NOT NULL REFERENCES accounts(account_id),
        customer_id      UUID           NOT NULL REFERENCES customers(customer_id),
        transaction_type VARCHAR(30)    NOT NULL,  -- 'DEBIT', 'CREDIT', 'TRANSFER', 'FEE', 'INTEREST'
        amount           NUMERIC(18, 2) NOT NULL,
        currency         CHAR(3)        NOT NULL DEFAULT 'USD',
        description      VARCHAR(500),
        merchant_name    VARCHAR(200),
        merchant_category VARCHAR(100),
        channel          VARCHAR(50),   -- 'ONLINE', 'ATM', 'POS', 'MOBILE', 'BRANCH'
        status           VARCHAR(20)    NOT NULL DEFAULT 'COMPLETED',
        reference_id     VARCHAR(100),  -- external reference / idempotency key
        transacted_at    TIMESTAMPTZ    NOT NULL DEFAULT NOW(),
        posted_at        TIMESTAMPTZ,
        created_at       TIMESTAMPTZ    NOT NULL DEFAULT NOW()
    )
    ```

## Tests

Both tiers are tested independently, and the test layout mirrors the module layout above — one test file per layer per domain.

### Backend (`apps/backend/tests`, pytest)

Configured in `pyproject.toml` (`testpaths = ["tests"]`, `pythonpath = ["."]`). Fixtures live in `tests/conftest.py`. The suite is split so most of it runs with
**no infrastructure**:

* **Unit / isolated** — the bulk of the suite. Exercises each layer at its boundary with the dependency mocked:
  * `test_*_models.py` — Pydantic validation and defaults on the model contract.
  * `test_*_inventory.py` / `test_*_seed.py` — CSV load, row↔model mapping, and idempotent seeding.
  * `test_*_kafka_producer.py` / `test_kafka_producer_delete.py` — the Debezium envelope shape (`before`/`after`/`op`, date/timestamp/decimal encoding) with
    the producer mocked at the boundary.
  * `test_*_service_emit.py` / `test_service_emit.py` — the dispatch rules in `service.py`: that create/update/delete emit the right op, and that emission
    is correctly **suppressed** when `CDC_CONNECTOR_ENABLED` or the runtime toggle says so, and that a producer failure never fails the request.
  * `test_*_api.py`, `test_config_api.py`, `test_config_store.py`, `test_cors.py` — router behavior via FastAPI `TestClient` (status codes, `404`/`409` mapping, CORS headers), with the DAO/producer stubbed.
  * `test_customers_kafka.py` — the full `SINK=kafka` path with Kafka/SR mocked.
* **Postgres integration** — `test_*_postgres.py`. These require a live database and are **auto-skipped when `DATABASE_URL` is unset** (via the   `skip_without_postgres` fixture / `requires_postgres` marker).

Seed isolation: the session-scoped `isolate_seed_csvs` fixture copies the committed seed CSVs to a temp dir and points `*_CSV_PATH` at the copies, so tests never mutate the checked-in seed files.

```sh
cd apps/backend
uv run pytest                 # unit suite (postgres tests skip without DATABASE_URL)
DATABASE_URL=postgresql://dbadmin:localdevonly@localhost:5432/c360db \
  uv run pytest               # include the postgres integration tests
```

### Frontend (`apps/frontend/src`, Vitest + Testing Library)

Vitest with jsdom and React Testing Library; global setup in `src/test/setup.ts`. Tests sit next to the code they cover (`*.test.ts(x)`):

* `src/api/*.test.ts` — the `request()` wrapper and each domain client, with `fetch` mocked: URL/method/body construction, `204` handling, and `ApiError`
  mapping from error bodies.
* `src/pages/*.test.tsx`, `src/components/*.test.tsx` — page and component rendering, routing, form submission, and the confirm/delete flows, with the
  `api/` modules mocked.

```sh
cd apps/frontend
npm test            # vitest run (CI mode)
npm run test:watch  # watch mode
```

