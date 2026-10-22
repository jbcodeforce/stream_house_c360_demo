# Generate 200 Transactions (backend generator + frontend button/page) — Plan

## Overview

Add a **"Generate 200 transactions"** button on the Transactions page. The click calls a new backend endpoint that:

1. picks **existing** accounts (and their owning customers),
2. builds 200 realistic transactions with **back-dated, evenly spaced timestamps** (start = now − 7 days, +15 minutes per transaction, regardless of how fast they are produced),
3. inserts them into Postgres in one DB transaction,
4. publishes them to Kafka **only under the existing gating** (`CDC_CONNECTOR_ENABLED=false` AND `KAFKA_PRODUCE_ENABLED=true`, identical to the single create),
5. returns the 200 created transactions.

The frontend then navigates to a new page `/transactions/generated` that shows exactly those transactions, passed via **router state** (no persistence: a refresh loses the page — accepted by design).

### Decisions already made
- Kafka: insert into Postgres, reuse the existing gating exactly (no new env var, no direct-to-Kafka path).
- "New transactions" page: endpoint returns the created rows; frontend shows them via router state. **No model change / no batch marker field.**
- `reference_id` mismatch: resolved by removing it everywhere (see sub-task 1), because backend models, `kafka_producer.py` and the Avro value schema no longer carry it.

### Out of scope
- `SINK=kafka` path (the existing `service.create` Kafka branch is already inconsistent: it passes `updated_at`/`customer_id` that `Transaction` does not have). Generation supports the Postgres sink only; any other sink returns an error.
- Persisting/looking up "generated batches" across refreshes.
- A configurable count (fixed at 200, as a backend constant).

### Timestamp rule
`transacted_at[i] = floor_to_second(now_utc − 7 days) + i × 15 min`, for i = 0..199 (spans ~50 h, ending ≈ 4.9 days ago). `posted_at = transacted_at` (matches seed data). `created_at` is the real insert time. The list is returned in generation order (oldest first); the generated page shows it sorted by `transacted_at` descending, like the main page.

---

## Sub-Tasks

### 1. Remove the stale `reference_id` (prerequisite)
- **Intent**: Backend `Transaction`/`TransactionCreate`, `kafka_producer.py` and the Avro schema no longer have `reference_id`, but `db_sink.py` still reads `t.reference_id` when inserting (would raise `AttributeError` for every insert, including generated ones) and the frontend still types/displays it.
- **Expected Outcomes**: Inserting a `Transaction` works; no code references `reference_id` except (optionally) the legacy nullable DB column; frontend no longer expects it.
- **Todo List**:
  1. `db_sink.py`: drop `reference_id` from `_INSERT_COLS`, the placeholder count (15 → 14) and `_insert_params`; drop it from the `CREATE TABLE` DDL. Existing tables keep the nullable column — harmless, no migration.
  2. `transactions/inventory.py`: no change needed — `_row_to_transaction` only reads the CSV's `reference_id` column to derive a deterministic `transaction_id`, and pydantic ignores the extra key; `_FIELDNAMES` already omits it. Just confirm the existing inventory tests still pass.
  3. Frontend: remove `reference_id` from `types/transaction.ts`, `TransactionDetailPage.tsx` (field list and the `<h1>` fallback → use `transaction_id`), and `TransactionDetailPage.test.tsx`.
  4. Run backend + frontend test suites to confirm green before continuing.
- **Relevant Context**: `apps/backend/transactions/db_sink.py` (lines 20-66), `apps/backend/transactions/inventory.py`, `apps/frontend/src/types/transaction.ts`, `apps/frontend/src/pages/TransactionDetailPage.tsx` (lines 18, 46) and its test. Files were modified externally — re-read them before editing.
- **Status**: [x] done

### 2. Backend: transaction generator (pure function)
- **Intent**: Isolate generation logic so it is deterministic to test and independent of DB/Kafka.
- **Expected Outcomes**: A function that, given a list of existing accounts, a count, and a `now` reference, returns 200 `Transaction` objects with correct timestamps and valid `account_id`/`customer_id` pairs.
- **Todo List**:
  1. New module `apps/backend/transactions/generator.py` with `generate(accounts, count=200, now=None) -> list[Transaction]`.
  2. Eligible accounts: `status == "ACTIVE"` and no `closed_date`; if none → raise a domain error (mapped to HTTP 409 in sub-task 4).
  3. Per transaction: random eligible account → `account_id`, `customer_id` (from the account), `currency` (from the account); `transaction_type` DEBIT/CREDIT; plausible `amount`, `description`, `merchant_name`, `merchant_category`, `channel` drawn from small value lists modelled on the values in `transactions/data/transactions.csv`; `status="COMPLETED"`; new `uuid4` id; timestamps per the **Timestamp rule** above.
  4. Allow injecting `now` (and optionally a seeded `random.Random`) so tests are deterministic.
- **Relevant Context**: `apps/backend/transactions/models.py`, `apps/backend/accounts/models.py`, seed values in `apps/backend/transactions/data/transactions.csv`.
- **Status**: [x] done

### 3. Backend: batch insert + batched Kafka publish
- **Intent**: 200 rows must not mean 200 DB round-trips or 200 blocking 5 s Kafka flushes.
- **Expected Outcomes**: All rows inserted atomically in one DB transaction; Kafka receives all events with a single flush at the end.
- **Todo List**:
  1. `db_sink.create_many(transactions: list[Transaction]) -> list[Transaction]`: one `db.cursor(commit=True)` block, batched insert (`executemany`/`execute_values` with the existing insert SQL), returns the inserted rows.
  2. `kafka_producer.produce_create_many(txns, flush_timeout)`: produce each event, then flush once; log if messages remain unconfirmed (same warning style as `produce_create`). Reuse `_build_envelope` and `_on_delivery`. File was modified externally — re-read first.
  3. `service.generate() -> list[Transaction]`: only `SINK == "postgres"` (otherwise raise → 501/409); loads accounts via `accounts.service.list_all()`; calls the generator; `db_sink.create_many`; then a `_maybe_emit_many` that applies **the same two gates as `_maybe_emit`** (return if `CDC_CONNECTOR_ENABLED`; return if not `KAFKA_PRODUCE_ENABLED`), wraps producer errors in try/except + warning log (best-effort, never fails the request).
- **Relevant Context**: `apps/backend/transactions/service.py` (`_maybe_emit`, `create`), `apps/backend/transactions/db_sink.py`, `apps/backend/transactions/kafka_producer.py`, `apps/backend/db.py` (`cursor`), `apps/backend/accounts/service.py`.
- **Status**: [x] done

### 4. Backend: API endpoint
- **Intent**: Expose generation to the frontend.
- **Expected Outcomes**: `POST /api/v1/transactions/generate` → 201 with `list[Transaction]` (200 items); 409 with a generic message when there are no eligible accounts.
- **Todo List**:
  1. Add the route to `api/transaction_resource.py` (no request body; static path, declared before `/{transaction_id}` for clarity).
  2. Map the "no eligible accounts" error to `HTTPException(409)` with a generic detail (no internals leaked).
- **Relevant Context**: `apps/backend/api/transaction_resource.py`.
- **Status**: [x] done

### 5. Backend tests
- **Intent**: Lock in timestamps, selection and gating.
- **Expected Outcomes**: New tests pass alongside the existing suite.
- **Todo List**:
  1. Generator: returns exactly 200; `transacted_at` starts at `now − 7d` and each next is exactly +15 min; every `(account_id, customer_id)` pair matches an input ACTIVE account; closed/inactive accounts never used; raises when no eligible accounts.
  2. Service (monkeypatch pattern from `tests/test_transactions_service_emit.py`): `CDC_CONNECTOR_ENABLED=True` → no publish; `KAFKA_PRODUCE_ENABLED=False` → no publish; both OK → `produce_create_many` called once with all 200; producer exception swallowed and rows still returned.
  3. API (pattern from `tests/test_transactions_api.py`): 201 + 200 items with `service.generate` mocked; 409 on the domain error.
  4. Optional Postgres integration test guarded by the existing `skip_without_postgres` fixture for `create_many`.
- **Relevant Context**: `apps/backend/tests/test_transactions_api.py`, `test_transactions_service_emit.py`, `conftest.py`.
- **Status**: [x] done

### 6. Frontend: API function + button
- **Intent**: Let the user trigger generation from the Transactions page.
- **Expected Outcomes**: A "Generate 200 transactions" button next to "Add transaction"; while in flight it is disabled and shows a busy label; on success it navigates to `/transactions/generated` passing the response as router state; on failure an error message is shown on the page.
- **Todo List**:
  1. `api/transactions.ts`: `generateTransactions(): Promise<Transaction[]>` → `POST /transactions/generate` via the existing `request` helper.
  2. `TransactionsPage.tsx`: add button (class `btn btn--secondary`), local `generating` state, reuse the page's existing `error` display, `navigate('/transactions/generated', { state: { transactions } })`.
- **Relevant Context**: `apps/frontend/src/pages/TransactionsPage.tsx`, `apps/frontend/src/api/transactions.ts`, `apps/frontend/src/api/client.ts`, `src/styles/index.css`.
- **Status**: [x] done

### 7. Frontend: generated-transactions page + route
- **Intent**: Show the transactions the last click created.
- **Expected Outcomes**: `/transactions/generated` lists the passed transactions with account number and customer name (same lookups as the main page), with the **full date and time** (HH:MM) of `transacted_at` since the 15-minute spacing is the point of the demo; a link back to Transactions. If opened without router state (e.g. after refresh) it shows "No generated transactions to display" with a link back — no crash.
- **Todo List**:
  1. New `pages/GeneratedTransactionsPage.tsx` using `useLocation().state`, fetching accounts/customers for the lookups (same approach as `TransactionsPage`), sorted by `transacted_at` descending, showing a count header (e.g. "200 transactions generated").
  2. Register `<Route path="/transactions/generated" …/>` in `App.tsx` (static segment outranks `/transactions/:id` in React Router v7).
  3. Row "View" action reuses `/transactions/:id`.
- **Relevant Context**: `apps/frontend/src/App.tsx`, `apps/frontend/src/pages/TransactionsPage.tsx` (table markup), `apps/frontend/src/components/Sidebar.tsx` (no nav change needed).
- **Status**: [x] done

### 8. Frontend tests
- **Intent**: Cover the new UI behaviour using the existing Vitest + Testing Library setup.
- **Expected Outcomes**: Tests pass with the existing suite.
- **Todo List**:
  1. `api/transactions.test.ts`: `generateTransactions` POSTs to `/transactions/generate` (pattern: existing spy-based tests).
  2. `TransactionsPage` test: click → API called, button disabled while pending, navigation with state on success, error shown on failure.
  3. `GeneratedTransactionsPage` test: renders rows from router state; empty-state when no state.
- **Relevant Context**: `apps/frontend/src/api/transactions.test.ts`, `apps/frontend/src/test/setup.ts`, existing page tests (e.g. `TransactionDetailPage.test.tsx`).
- **Status**: [x] done

### 9. Docs
- **Intent**: Make the feature and its gating discoverable.
- **Expected Outcomes**: README/docs mention the endpoint and that Kafka publishing follows `CDC_CONNECTOR_ENABLED` / `KAFKA_PRODUCE_ENABLED`.
- **Todo List**:
  1. `apps/backend/README.md`: add `POST /transactions/generate` with timestamp rule and gating note.
  2. `docs/demo_script.md` (currently open in the editor): add a short step for the demo if it covers the transactions page.
- **Relevant Context**: `apps/backend/README.md`, `docs/demo_script.md`.
- **Status**: [x] done

---

## Validation (end of each sub-task)
- Backend: `cd apps/backend && uv run pytest -q`
- Frontend: run the project's Vitest + typecheck/lint scripts from `apps/frontend/package.json`.
- Manual (after sub-task 7): start backend + frontend, click the button, confirm 200 rows with timestamps 15 min apart starting ~7 days ago; with `KAFKA_PRODUCE_ENABLED=true` and a local DB, confirm 200 events on `cdc.public.transactions`; with a remote `DATABASE_URL` (CDC connector owns Kafka), confirm nothing is published by the backend.
