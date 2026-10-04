# Accounts (Backend + Frontend) — Design Spec

Date: 2026-10-04
Status: Approved for planning

## Intent

Add an Accounts resource mirroring the existing Customers pattern: a Postgres
CRUD REST API with config-gated best-effort Kafka (Debezium) emission, and
frontend pages (table with view/edit/delete, detail, create/edit form). An
account belongs to a customer.

Audience: internal C360 demo. Success = a user can list/view/create/edit/
delete accounts in the UI against the live backend, and — when the existing
Settings "Produce Kafka events" toggle is ON — each account create/update/
delete also emits a Debezium envelope to the accounts topic (best-effort).

## Decisions (from brainstorming)

- **Full parity** with customers: Postgres storage + config-gated dual-write
  Kafka emission using the real schema the user added
  (`accounts/schema/schema-cdc.public.accounts-value-v1.avsc`, a full
  Debezium `cdc.public.accounts.Envelope`).
- **Skip** the `SINK=kafka` CSV-inventory mode for accounts (no seed CSV).
  Accounts storage is always Postgres; the toggle governs the postgres path.
- The existing global Settings toggle (`kafka_produce_enabled`) now also
  governs accounts emission (no new toggle).
- `balance`/`credit_limit` are `float` in the Pydantic models, converted to
  `Decimal` (precision 18, scale 2) when building the Avro payload.
- Owning customer chosen in the form via a **dropdown of customers**.
- Reuse the customers producer's resilience fixes: bounded `flush_timeout`
  and an `on_delivery` logging callback; emission failures are logged and
  never fail (or hang) the DB operation.

## Backend Contract (new)

Base path `/api/v1/accounts` (FastAPI), mirroring the customers resource:

- `POST   /api/v1/accounts`        → `Account` (201); 409 on duplicate
  `account_number`.
- `GET    /api/v1/accounts`        → `Account[]`.
- `GET    /api/v1/accounts/{id}`   → `Account` (404 if missing).
- `PUT    /api/v1/accounts/{id}`   → `Account` (200, 404 if missing).
- `DELETE /api/v1/accounts/{id}`   → 204 (404 if missing).

`Account` fields (from the accounts DDL in `scripts/db/create_tables.py`):

| field | type | notes |
|-------|------|-------|
| account_id | UUID | server-assigned |
| customer_id | UUID | required (FK → customers) |
| account_number | string | required, unique |
| account_type | string | required (CHECKING/SAVINGS/CREDIT/LOAN) |
| currency | string | default "USD" |
| balance | float | default 0.0 (DB NUMERIC(18,2)) |
| credit_limit | float? | optional |
| opened_date | date | default today |
| closed_date | date? | optional |
| status | string | default "ACTIVE" |
| created_at | datetime | server-assigned, read-only |
| updated_at | datetime | server-assigned, read-only |

`AccountCreate` = all except server-assigned. `AccountUpdate` = all optional.

## Backend Structure (mirrors `customers/`)

```
apps/backend/accounts/
  __init__.py
  models.py        # AccountCreate / AccountUpdate / Account (pydantic)
  db_sink.py       # psycopg2 pool (reuse pattern), accounts DDL, CRUD
  kafka_producer.py# Debezium envelope producer for accounts
  service.py       # CRUD dispatch + _maybe_emit (config-gated, bounded flush)
apps/backend/api/account_resource.py   # APIRouter(prefix="/accounts")
```

### models.py

Mirror `customers/models.py`: `AccountCreate(BaseModel)` with the create
fields and defaults (`currency="USD"`, `balance=0.0`, `country`-style
defaults: `opened_date: date = date.today()`, `status="ACTIVE"`);
`AccountUpdate` all-optional; `Account(AccountCreate)` adds `account_id`,
`created_at`, `updated_at` with `model_config = ConfigDict(from_attributes=True)`.

### db_sink.py

Mirror `customers/db_sink.py`:
- Lazy `SimpleConnectionPool` from `settings.DATABASE_URL` (reuse the exact
  `_ensure_pool`/`_get_conn`/`_put_conn` pattern).
- `init_db()` runs `CREATE TABLE IF NOT EXISTS accounts (...)` matching the
  DDL verbatim, the `idx_accounts_customer_id` index, and the
  `trg_accounts_updated_at` trigger (the `set_updated_at` function already
  exists from the customers init; use `CREATE OR REPLACE`/guarded trigger).
- `create`, `update`, `get_by_id`, `list_all` (ORDER BY created_at DESC),
  `delete_by_id` — same shapes as customers, returning `Account`. `create`
  maps `psycopg2.errors.UniqueViolation` → `HTTPException(409, "An account
  with that account number already exists")`.
- No `seed_from_csv` (accounts has no seed file).

### kafka_producer.py

Mirror `customers/kafka_producer.py` (the current, fixed version):
- Load `_SCHEMA_STR` from
  `accounts/schema/schema-cdc.public.accounts-value-v1.avsc`.
- Debezium op codes `c`/`u`/`d`; synthetic source block with
  `table="accounts"`; topic from `settings.KAFKA_TOPIC_ACCOUNTS`.
- `_build_value(account)` encodes: UUIDs as strings; `opened_date`/
  `closed_date` via `_to_debezium_date`; `created_at`/`updated_at` via
  `_to_debezium_ts`; `balance`/`credit_limit` as `Decimal` quantized to
  2 places (the Avro `connect.data.Decimal`, scale 2 — `None` for a missing
  `credit_limit`); strings passed through.
- `_build_envelope(account, op)` identical structure (before/after/source/
  op/ts_ms/transaction).
- `_on_delivery` logging callback; `_produce(account, op, flush_timeout=None)`
  with the same bounded-flush semantics; `produce_create/update/delete`
  wrappers accepting `flush_timeout`.

### service.py

Mirror the customers service's postgres path + emission:
- `create`/`update`/`get_by_id`/`list_all`/`delete_by_id` route to
  `db_sink` (Postgres only — no SINK dispatch for accounts).
- `_maybe_emit(op, account)` — gated by `config_store.get_config()
  .kafka_produce_enabled`; calls the matching `kafka_producer.produce_*`
  with `flush_timeout=_EMIT_FLUSH_TIMEOUT` (5.0); wraps in try/except and
  logs on failure (best-effort, never fails/hangs the DB op).
- `create` emits "c"; `update` emits "u" when a row is returned;
  `delete_by_id` fetches the account (via `get_by_id`) before deleting and
  emits "d" only when a row was actually deleted.

### Config & wiring

- `config.py`: add `KAFKA_TOPIC_ACCOUNTS: str = "cdc.public.accounts"`.
- `main.py`: import and `include_router(account_router, prefix="/api/v1")`;
  in `lifespan` (postgres branch) call `accounts_db_sink.init_db()` AFTER
  `db_sink.init_db()` (customers first, FK order).

## Frontend (mirrors customers pages)

```
apps/frontend/src/types/account.ts        # Account / AccountCreate / AccountUpdate
apps/frontend/src/api/accounts.ts         # list/get/create/update/delete
apps/frontend/src/pages/AccountsPage.tsx      # table + row actions
apps/frontend/src/pages/AccountDetailPage.tsx # read-only detail
apps/frontend/src/pages/AccountFormPage.tsx   # create AND edit
```

### Routes (App.tsx)

| path | page |
|------|------|
| `/accounts` | AccountsPage (replaces the PlaceholderPage) |
| `/accounts/new` | AccountFormPage (create) |
| `/accounts/:id` | AccountDetailPage |
| `/accounts/:id/edit` | AccountFormPage (edit) |

The Sidebar already links `/accounts`; no sidebar change needed.

### Types

`account.ts`: `AccountCreate { customer_id: string; account_number: string;
account_type: string; currency?: string | null; balance?: number | null;
credit_limit?: number | null; opened_date?: string | null; closed_date?:
string | null; status?: string | null }`; `AccountUpdate =
Partial<AccountCreate>`; `Account extends AccountCreate { account_id: string;
created_at: string; updated_at: string }`.

### API module

`accounts.ts` reuses `request` from `./client`: `listAccounts`,
`getAccount`, `createAccount`, `updateAccount`, `deleteAccount` — same
shapes as `customers.ts`, paths under `/accounts`.

### AccountsPage

Mirror `CustomersPage`: fetch list on mount; loading/error states; table
columns **Account #, Type, Customer, Balance, Status, Actions**
(view/edit/delete icons); delete via the shared `ConfirmDialog` then
refetch; "Add account" button → `/accounts/new`. The Customer column shows
the owning customer's "First Last" — to render it, the page also fetches
customers once and builds a `customer_id → name` map (falls back to the raw
id when not found).

### AccountDetailPage

Mirror `CustomerDetailPage`: fetch one by id; read-only field list; Edit and
Back actions. Show the customer's name (fetched) alongside the id.

### AccountFormPage (create + edit)

Mirror `CustomerFormPage`, including the **omit-empty-fields** payload rule
(send only non-empty fields so backend defaults apply — the fix that
resolved the customers 422). Specifics:
- Fetches customers on mount to populate a **customer dropdown** (required);
  options show "First Last — email", value = `customer_id`.
- `account_type` select (CHECKING/SAVINGS/CREDIT/LOAN); `status` select
  (ACTIVE/CLOSED/FROZEN — ACTIVE default); `currency` text (default USD).
- `balance`/`credit_limit` number inputs; `opened_date`/`closed_date` date
  inputs (date values trimmed to `yyyy-MM-dd` on load, as in customers).
- Required-field validation: `customer_id`, `account_number`, `account_type`.
- Create → POST; with `:id` → load + PUT. On success navigate `/accounts`.
  Submit errors shown inline (e.g. the 409 duplicate account number).

## Error & Loading Handling

Same as customers: list/detail pages show loading + error (API message);
form submit failures render inline and keep the form populated; delete
failures surface a message and leave the row intact.

## Out of Scope (YAGNI)

- `SINK=kafka` CSV-inventory mode for accounts.
- Transactions resource (separate future work; its schema was added but is
  not part of this spec).
- Editing balance via dedicated money-movement semantics (balance is a
  plain editable field here).
- Auth, pagination, search.

## Testing

Backend (pytest, infra-free — no DB, no live Kafka):
- `accounts/models`: defaults and optionality.
- `accounts.service` emission gating (db_sink + producer patched on the
  service module): create/update/delete emit the matching producer when the
  flag is ON and `flush_timeout` is bounded; nothing emitted when OFF;
  update with no row does not emit; delete fetches before deleting and emits
  only when a row was deleted; a producer exception still returns the DB
  result.
- `accounts.kafka_producer`: `_build_value`/envelope encodes a `Decimal`
  balance and op codes correctly (producer mocked via `_get_producer`).
- Accounts API via a bare `FastAPI()` app mounting the router (no `main`
  import), with `db_sink` functions patched — GET/POST/PUT/DELETE happy
  paths and 404s; this keeps the test infra-free and avoids polluting the
  `test_customers_kafka` module graph.

Frontend (vitest + RTL), mirroring customers:
- `api/accounts.ts` builds correct requests.
- `AccountsPage` renders rows (customer name resolved) and runs the delete
  flow.
- `AccountDetailPage` renders fields and handles 404.
- `AccountFormPage` validates required fields, omits empty fields, populates
  the customer dropdown, and calls create vs update based on `:id`.
