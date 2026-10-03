# Kafka Produce Config — Design Spec

Date: 2026-10-02
Status: Approved for planning

## Intent

Let the user control, at runtime, whether the backend also emits a Kafka
event when a record is created, updated, or deleted — without changing the
system of record (PostgreSQL). Exposed as a config REST API and a Settings
page in the frontend.

Audience: internal C360 demo. Success = a user can open a Settings page,
flip a single "Produce Kafka events on changes" switch, and have subsequent
customer create/update/delete operations emit (or not emit) Kafka events
accordingly, with the setting surviving a backend restart.

## Decisions (from brainstorming)

- **Behavior model: dual-write.** PostgreSQL stays the store. When the flag
  is ON, create/update/delete in the postgres path ALSO emit a Kafka event.
- **Granularity: single global switch** — one boolean covering create,
  update, and delete together.
- **Persistence: JSON file on disk** — survives restarts.
- **Default: OFF** — normal postgres operation requires no Kafka creds.
- **`SINK=kafka` path unchanged** — producing is intrinsic to that mode;
  the toggle governs only the postgres (dual-write) path.
- **Best-effort emission** — if a Kafka produce fails (e.g. no creds, broker
  unreachable), log a warning and still return success for the DB operation.
- **Frontend: auto-save** — flipping the switch immediately PUTs the config.

## Backend

### Config model & store

New module `apps/backend/config_store.py`:

- `AppConfig` (pydantic BaseModel): `kafka_produce_enabled: bool = False`.
- JSON file path from `settings.RUNTIME_CONFIG_FILE`, defaulting to
  `runtime_config.json` next to the backend. The file is gitignored.
- `get_config() -> AppConfig`: returns the in-memory config, loading from the
  file on first call; if the file is missing or unreadable, returns the
  default and does not raise.
- `set_config(config: AppConfig) -> AppConfig`: updates the in-memory config
  and writes it to the file; returns the stored config.
- Single-process assumption (one uvicorn worker); no locking needed.

Add to `apps/backend/config.py` `Settings`:
`RUNTIME_CONFIG_FILE: str | None = None` (when None, the store uses its
default path).

### Config REST API

New `apps/backend/api/config_resource.py`, router prefix `/config`,
included under `/api/v1` in `main.py`:

- `GET  /api/v1/config` → `AppConfig` (200).
- `PUT  /api/v1/config` body `AppConfig` → `AppConfig` (200), persisted.

### Kafka producer

`apps/backend/customers/kafka_producer.py`: add
`produce_delete(customer: Customer) -> None` mirroring `produce_create` /
`produce_update`, with `op="D"`.

### Service wiring

`apps/backend/customers/service.py`: in the **postgres** branch of
`create`, `update`, and `delete_by_id`, after a successful DB operation and
when `get_config().kafka_produce_enabled` is True, emit the matching Kafka
event via a best-effort helper:

```
def _maybe_emit(op, customer):   # op in {"C","U","D"}
    if not config_store.get_config().kafka_produce_enabled:
        return
    try:
        <produce_create|produce_update|produce_delete>(customer)
    except Exception:
        logger.warning("kafka emit failed for op=%s id=%s", op, id, exc_info=True)
```

- `create`: emit "C" with the returned Customer.
- `update`: emit "U" with the returned Customer (skip when update returned
  None / no row).
- `delete_by_id`: emit "D" — the delete path must fetch the customer before
  deletion so a full Customer payload is available for the event; emit only
  when a row was actually deleted.

The `SINK=kafka` branch is unchanged.

## Frontend

### Types & API

- `apps/frontend/src/types/config.ts`: `AppConfig { kafka_produce_enabled: boolean }`.
- `apps/frontend/src/api/config.ts`:
  - `getConfig(): Promise<AppConfig>` → `GET /config`.
  - `updateConfig(data: AppConfig): Promise<AppConfig>` → `PUT /config`.

### Settings page

`apps/frontend/src/pages/ConfigPage.tsx`:

- Fetches config on mount; loading and error states (reuse existing
  `status-msg` styles).
- Renders a labeled toggle switch "Produce Kafka events on create / update /
  delete" bound to `kafka_produce_enabled`.
- Auto-saves on flip: optimistic UI update, PUT, and on failure revert the
  switch and show an error message.

### Navigation

- `Sidebar.tsx`: add a fourth link, "Settings" (lucide `Settings` gear
  icon) → `/settings`.
- `App.tsx`: add route `/settings` → `ConfigPage`.

## Error & Loading Handling

- Config page shows a loading indicator while fetching and an error message
  (with the API message) on GET or PUT failure; a failed PUT reverts the
  toggle to its last known value.
- Backend GET never fails on a missing file (returns default). PUT failures
  (e.g. unwritable file) surface as a 500 via the normal FastAPI path.

## Out of Scope (YAGNI)

- Per-table or per-operation granularity (single global switch only).
- Multi-worker / multi-instance config sharing (single-process assumption).
- Authentication on the config endpoints.
- Toggling the `SINK=kafka` path's intrinsic production.
- A Kafka connectivity/health check in the UI.

## Testing

Backend (pytest, infra-free using the existing SINK=kafka harness pattern
where an app is needed; config store tested directly):

- `config_store`: default when file absent; `set_config` then `get_config`
  round-trips through the file; corrupt/unreadable file falls back to
  default without raising.
- Config API: `GET` returns current; `PUT` updates and persists (a
  subsequent `GET` reflects it).
- Service gating (postgres path, db_sink mocked): flag ON → the matching
  producer is called on create/update/delete; flag OFF → producer not
  called; producer raising → the service call still returns the DB result
  (best-effort).

Frontend (vitest + RTL):

- `api/config.ts`: `getConfig`/`updateConfig` hit the right paths/verbs.
- `ConfigPage`: renders the current value from a mocked `getConfig`;
  flipping the switch calls `updateConfig` with the new value; a failed
  `updateConfig` reverts the switch and shows an error.
