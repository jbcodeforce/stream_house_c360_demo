# Simplify Backend Configuration Plan

## Overview

Remove the JSON-file-backed runtime config (`apps/backend/config_store.py`) and make `apps/backend/config.py` the single source of configuration, driven only by environment variables.

- The runtime `kafka_produce_enabled` toggle becomes an env setting `KAFKA_PRODUCE_ENABLED: bool = False` in `Settings`.
- The `.env` file location is read from a new env var `ENV_CONFIG_FILE` (fallback `.env` when unset), instead of the hard-coded `env_file=".env"`.
- The `/api/v1/config` router, its tests, `RUNTIME_CONFIG_FILE`, and the frontend Settings page/API/types/sidebar link are removed (confirmed with user).

Non-goals: no change to the `CDC_CONNECTOR_ENABLED` guard logic; no change to other settings.

## Sub-Tasks

### 1. Backend: env-only settings and service gating
- **Intent**: Replace `config_store` usage with `settings.KAFKA_PRODUCE_ENABLED` and make the env file path configurable.
- **Expected Outcomes**: `config_store.py` gone; services gate on `settings`; `Settings` loads from the file named by `ENV_CONFIG_FILE`; no `RUNTIME_CONFIG_FILE`.
- **Todo List**:
  1. In `config.py`: add `KAFKA_PRODUCE_ENABLED: bool = False`; remove `RUNTIME_CONFIG_FILE`; set `env_file` from `os.environ.get("ENV_CONFIG_FILE", ".env")` (evaluated at import, since pydantic-settings needs it at class definition); update the comment mentioning the "runtime `kafka_produce_enabled` toggle".
  2. In `customers/service.py`, `accounts/service.py`, `transactions/service.py`: drop `import config_store`; replace `config_store.get_config().kafka_produce_enabled` with `settings.KAFKA_PRODUCE_ENABLED`.
  3. In `main.py`: remove the `config_router` import and `include_router`.
  4. Delete `config_store.py`, `api/config_resource.py`, `tests/test_config_store.py`, `tests/test_config_api.py`.
  5. Update `tests/test_service_emit.py`, `test_accounts_service_emit.py`, `test_transactions_service_emit.py`: remove `AppConfig`/`config_store` patching; instead `monkeypatch.setattr(service.settings, "KAFKA_PRODUCE_ENABLED", enabled)`.
  6. Check `tests/conftest.py` (loads `.env.local` via dotenv) — keep as is unless it conflicts with `ENV_CONFIG_FILE`.
  7. Run `cd apps/backend && uv run pytest`.
- **Relevant Context**: `apps/backend/config.py`, `config_store.py`, `*/service.py` (lines ~24-37 gate on `CDC_CONNECTOR_ENABLED` then the toggle), `main.py:11,42`, three `tests/test_*_emit.py` files (patch `service.config_store.get_config`).
- **Status**: `[ ] pending`

### 2. Frontend: remove Settings page
- **Intent**: Drop UI for the removed `/config` API.
- **Expected Outcomes**: No references to `getConfig`/`updateConfig`/`AppConfig`; build and tests pass.
- **Todo List**:
  1. Delete `src/pages/ConfigPage.tsx`, `ConfigPage.test.tsx`, `src/api/config.ts`, `config.test.ts`, `src/types/config.ts`.
  2. `App.tsx`: remove `ConfigPage` import and `/settings` route.
  3. `components/Sidebar.tsx`: remove the Settings nav item (and unused `Settings` icon import); `Sidebar.test.tsx`: remove the settings assertion and update the "three sections" wording if needed.
  4. Update `apps/frontend/README.md` if it mentions Settings.
  5. Run the frontend tests/typecheck/lint (`npm test`, `npm run build`).
- **Relevant Context**: `apps/frontend/src/App.tsx:12,30`, `components/Sidebar.tsx:8`.
- **Status**: `[ ] pending`

### 3. Config files, scripts, and docs
- **Intent**: Keep docs and env templates consistent.
- **Expected Outcomes**: No stale references to `config_store`, `RUNTIME_CONFIG_FILE`, `/config`, or the runtime toggle; `ENV_CONFIG_FILE` and `KAFKA_PRODUCE_ENABLED` documented.
- **Todo List**:
  1. `apps/backend/.env.example`: replace the "runtime kafka_produce_enabled toggle" wording with `KAFKA_PRODUCE_ENABLED=false`; note `ENV_CONFIG_FILE`.
  2. `apps/backend/README.md` (lines ~20, 61-79, 100): rewrite the runtime-toggle section to describe `KAFKA_PRODUCE_ENABLED`, remove the `GET/PUT /config` docs, document `ENV_CONFIG_FILE`.
  3. `docs/web_app_design.md` (line 59 and test list line 180), `docs/sre_aws.md:202`, `scripts/set_env_from_tf.sh:63` and `scripts/run_dev.sh` header comment (line 48-50, mention `ENV_CONFIG_FILE`): update wording.
  4. `.gitignore`: remove the `**/runtime_config.json` line.
  5. Leave historical docs under `docs/superpowers/` and `.bob/plans/` untouched (point-in-time records).
- **Relevant Context**: grep hits for `runtime_config|kafka_produce|RUNTIME_CONFIG`.
- **Status**: `[ ] pending`

## Notes
- `ENV_CONFIG_FILE` is a process env var read at import time, so it cannot itself be defined inside the `.env` it points to.
- Behavior change: the Kafka dual-write flag now requires a restart to change, and its default stays `false`.
