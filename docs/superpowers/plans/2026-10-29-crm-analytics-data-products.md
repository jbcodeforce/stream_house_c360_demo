# CRM Analytics Data Products Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Serve the C360 dashboard's live aggregates from the `crm` Flink data products via five simple snapshot `SELECT`s through `confluent-sql`, instead of pulling every customer row and folding in Python.

**Architecture:** In live mode, `service.get_data_analytics()` runs five snapshot queries (one per `fct_crm_*` table) concurrently through a failure-tolerant `ConfluentSqlClient`, then a pure `crm_aggregate.assemble()` folds the five result sets into the existing response model (renamed `C360Analytics` → `DataAnalytics`). Mock mode is unchanged — it still folds the fixture through `aggregate()`, which stays the behavioural reference. The HTTP route, response JSON shape, and the per-customer lookup paths are untouched.

**Tech Stack:** Python 3.11, FastAPI, pydantic, `confluent-sql` (new), pytest (+ monkeypatch); frontend TypeScript/React, vitest.

**Spec:** `docs/superpowers/specs/2026-10-29-crm-analytics-data-products-design.md`

## Global Constraints

- Backend tests run from `apps/backend/` via `uv run pytest` (`testpaths=["tests"]`, `pythonpath=["."]`).
- Frontend tests run from `apps/frontend/` via `npm test` (`vitest run`).
- `confluent-sql` imports as `confluent_sql`; the package name in `pyproject.toml` is `confluent-sql`.
- The live client must be **failure-tolerant**: missing settings or any exception → `[]`/empty payload, never raises (mirrors `FlinkSnapshotClient`).
- `confluent_sql.connect()` arguments map to existing settings only — no new config fields: `organization_id=FLINK_ORG_ID`, `environment_id=FLINK_ENV_ID`, `cloud_provider=CLOUD_PROVIDER`, `cloud_region=CLOUD_REGION`, `compute_pool_id=FLINK_COMPUTE_POOL_ID`, `flink_api_key=FLINK_API_KEY`, `flink_api_secret=FLINK_API_SECRET`, `database=FLINK_DATABASE`, `database_kafka_cluster_id=KAFKA_CLUSTER_ID`.
- Live `assemble()` must match `aggregate()` exactly where both run: null/empty category keys → `"Unknown"`; every distribution ordered `(-count, label)`; `by_country` trimmed to top-8 + `"Other"` (`_COUNTRY_TOP_N = 8`); `avg_balance_per_customer = total_balance / total_customers` (None at 0 rows); `row_count = total_customers`.
- Table→field mapping (from the deployed `crm` contracts):
  `fct_crm_summary(qscope, total_customers, total_accounts, total_balance, total_transactions, total_txn_amount, avg_age, missing_email, missing_phone, complete)`;
  `fct_crm_by_segment(customer_segment, customer_count, total_balance)`;
  `fct_crm_by_generation(generation_segment, generation_count)`;
  `fct_crm_by_gender(gender, gender_count)`;
  `fct_crm_by_country(country, country_count)`.

## Review Focus

- **Empty summary row** (live query returns no rows): payload must be the empty state (`row_count == 0`, averages `None`), no divide-by-zero on `avg_balance_per_customer`. → Task 3 tests.
- **Partial failure** (one of the five queries returns `[]`, others succeed): `assemble()` produces a partial payload rather than crashing; a distribution with no rows is `[]`. → Task 3 + Task 4 tests.
- **`Decimal` cells** (money/avg come back as `Decimal`): coerced to JSON-serialisable `float` in the model. → Task 3 test with `Decimal` inputs.
- **Null category key** (e.g. `gender=None` row): relabelled to `"Unknown"`, matching mock. → Task 3 test.
- **A worker raising in the concurrent fetch**: isolated to `[]` for that table, the overall fetch still returns. → Task 4 test.

---

### Task 1: Rename `C360Analytics` → `DataAnalytics`

Mechanical rename of the response *type* only. Route path `/analytics/c360-profile`, the frontend function `getC360Analytics`, and all JSON field names stay the same (so `test_analytics_api.py`, which asserts JSON keys, is unaffected).

**Files:**
- Modify: `apps/backend/analytics/models.py` (class `C360Analytics` → `DataAnalytics`; update module docstring to say it serves c360 + crm)
- Modify: `apps/backend/analytics/aggregate.py` (import + return annotation + constructor)
- Modify: `apps/backend/analytics/service.py` (import + return annotation)
- Modify: `apps/backend/api/analytics_resource.py` (import + `response_model=`)
- Modify: `apps/frontend/src/types/analytics.ts` (`interface C360Analytics` → `DataAnalytics`)
- Modify: `apps/frontend/src/api/analytics.ts` (type import + `getC360Analytics(): Promise<DataAnalytics>`)
- Modify: `apps/frontend/src/pages/AnalyticsPage.tsx` (type import + `useState<DataAnalytics | null>`)
- Modify: `apps/frontend/src/pages/AnalyticsPage.test.tsx` (type import + sample annotation)

**Interfaces:**
- Produces: `DataAnalytics` pydantic model (identical fields to today's `C360Analytics`) consumed by Tasks 3–5; `DataAnalytics` TS interface consumed by the frontend.

- [ ] **Step 1: Rename the backend symbol** across the four backend files (grep `C360Analytics` to find every reference; no other code changes).
- [ ] **Step 2: Run backend tests to verify they still pass**

Run: `cd apps/backend && uv run pytest tests/test_analytics_aggregate.py tests/test_analytics_api.py -q`
Expected: PASS (fields and JSON unchanged).

- [ ] **Step 3: Rename the frontend type** across the four frontend files (grep `C360Analytics`).
- [ ] **Step 4: Run frontend analytics tests + type-check**

Run: `cd apps/frontend && npm test -- analytics && npx tsc --noEmit`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add apps/backend/analytics apps/backend/api apps/frontend/src
git commit -m "refactor(analytics): rename C360Analytics response model to DataAnalytics"
```

---

### Task 2: `confluent-sql` dependency + `ConfluentSqlClient`

A thin, failure-tolerant snapshot-query wrapper, same posture as `FlinkSnapshotClient`.

**Files:**
- Modify: `apps/backend/pyproject.toml` (add `confluent-sql` to `dependencies`)
- Create: `apps/backend/analytics/confluent_sql_client.py`
- Test: `apps/backend/tests/test_confluent_sql_client.py`

**Interfaces:**
- Produces: `class ConfluentSqlClient` with `__init__(self, settings_obj: config.Settings | None = None)` and `query(self, sql: str) -> list[dict]` — runs one snapshot `SELECT`, returns `[{column: value}]` from `cursor.description` + rows; `[]` when any required setting is unset or on any exception. Consumed by Task 4.
- Required-settings tuple `_CONFIG_FIELDS` = the nine `connect()` coordinates in Global Constraints.

- [ ] **Step 1: Write the failing tests** in `tests/test_confluent_sql_client.py`

```python
# _SETTINGS = SimpleNamespace with all nine fields populated.

def test_query_returns_empty_when_a_setting_is_unset():
    settings = SimpleNamespace(**{**_SETTINGS.__dict__, "CLOUD_REGION": None})
    assert ConfluentSqlClient(settings).query("SELECT * FROM t") == []

def test_query_maps_rows_to_dicts(monkeypatch):
    # Fake confluent_sql.connect -> conn.cursor() with .description + rows.
    # description = [("a",), ("b",)]; rows = [(1, 2), (3, 4)]
    monkeypatch.setattr(confluent_sql_client.confluent_sql, "connect", fake_connect)
    assert ConfluentSqlClient(_SETTINGS).query("SELECT a, b FROM t") == [
        {"a": 1, "b": 2}, {"a": 3, "b": 4},
    ]

def test_query_returns_empty_on_exception(monkeypatch):
    # fake_connect raises -> [].
    monkeypatch.setattr(confluent_sql_client.confluent_sql, "connect", boom)
    assert ConfluentSqlClient(_SETTINGS).query("SELECT * FROM t") == []

def test_query_closes_cursor_and_connection(monkeypatch):
    # Assert close() called on both even when iteration succeeds.
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd apps/backend && uv run pytest tests/test_confluent_sql_client.py -q`
Expected: FAIL (module/class not defined).

- [ ] **Step 3: Add the dependency and implement the client**

Add `"confluent-sql"` to `[project].dependencies` in `pyproject.toml`, then `uv sync`. Implement `ConfluentSqlClient.query`: return `[]` if `_missing_settings()`; else `confluent_sql.connect(**coords)`, `cur = conn.cursor()`, `cur.execute(sql)`, read column names from `cur.description`, zip with each fetched row into dicts; `close()` cursor and connection in `finally`; wrap the whole body in `try/except Exception` → log + `[]`.

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd apps/backend && uv run pytest tests/test_confluent_sql_client.py -q`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add apps/backend/pyproject.toml apps/backend/uv.lock apps/backend/analytics/confluent_sql_client.py apps/backend/tests/test_confluent_sql_client.py
git commit -m "feat(analytics): add confluent-sql snapshot query client"
```

---

### Task 3: `crm_aggregate.assemble()` — fold five result sets into `DataAnalytics`

Pure function, no I/O. The behavioural twin of `aggregate()` for the live path.

**Files:**
- Create: `apps/backend/analytics/crm_aggregate.py`
- Test: `apps/backend/tests/test_crm_aggregate.py`

**Interfaces:**
- Consumes: `DataAnalytics`, `CategoryCount`, `CategoryValue`, `DataQuality` from `analytics.models`; reuse `_COUNTRY_TOP_N` and the numeric coercion from `analytics.aggregate` (import `_num`, or lift shared helpers — implementer's call, keep DRY).
- Produces: `assemble(summary_row: dict | None, by_segment: list[dict], by_generation: list[dict], by_gender: list[dict], by_country: list[dict], *, source_mode: str, query_ms: float = 0.0) -> DataAnalytics`. Consumed by Task 5.

- [ ] **Step 1: Write the failing tests** in `tests/test_crm_aggregate.py`

```python
# Representative result sets mirroring the live probe.
SUMMARY = {"qscope": "ALL", "total_customers": 10, "total_accounts": 9,
           "total_balance": Decimal("343671"), "total_transactions": 279,
           "total_txn_amount": Decimal("41819"), "avg_age": 41.8,
           "missing_email": 0, "missing_phone": 0, "complete": 10}
SEG = [{"customer_segment": "RETAIL", "customer_count": 6, "total_balance": Decimal("260601")},
       {"customer_segment": "SMB", "customer_count": 4, "total_balance": Decimal("83070")}]

def test_scalars_and_derived_average():
    r = assemble(SUMMARY, SEG, [], [], [], source_mode="live")
    assert r.total_customers == 10 and r.row_count == 10
    assert r.total_balance == pytest.approx(343671.0)      # Decimal -> float
    assert r.avg_age == pytest.approx(41.8)
    assert r.avg_balance_per_customer == pytest.approx(34367.1)
    assert r.source_mode == "live"

def test_segment_feeds_both_count_and_balance_series():
    r = assemble(SUMMARY, SEG, [], [], [], source_mode="live")
    assert [(c.label, c.count) for c in r.by_segment] == [("RETAIL", 6), ("SMB", 4)]
    assert [(c.label, c.value) for c in r.balance_by_segment] == [
        ("RETAIL", pytest.approx(260601.0)), ("SMB", pytest.approx(83070.0))]

def test_distribution_ordered_and_tie_broken_by_label():
    gender = [{"gender": "M", "gender_count": 5}, {"gender": "F", "gender_count": 5}]
    r = assemble(SUMMARY, SEG, [], gender, [], source_mode="live")
    assert [(c.label, c.count) for c in r.by_gender] == [("F", 5), ("M", 5)]

def test_null_category_relabelled_unknown():
    gender = [{"gender": None, "gender_count": 3}]
    r = assemble(SUMMARY, SEG, [], gender, [], source_mode="live")
    assert r.by_gender[0].label == "Unknown"

def test_country_top_n_folds_tail_into_other():
    country = [{"country": f"C{i}", "country_count": 1} for i in range(12)]
    r = assemble(SUMMARY, SEG, [], [], country, source_mode="live")
    assert r.by_country[-1].label == "Other" and len(r.by_country) == 9

def test_empty_summary_is_safe():
    r = assemble(None, [], [], [], [], source_mode="live")
    assert r.row_count == 0 and r.total_customers == 0
    assert r.avg_age is None and r.avg_balance_per_customer is None
    assert r.by_segment == [] and r.data_quality.complete == 0
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd apps/backend && uv run pytest tests/test_crm_aggregate.py -q`
Expected: FAIL (function not defined).

- [ ] **Step 3: Implement `assemble(...)`** in `analytics/crm_aggregate.py` per the signature above. Scalars + `data_quality` from `summary_row` (coerce `Decimal`/`None` via `_num`); `avg_age` passthrough coerced to float-or-None; `avg_balance_per_customer = total_balance / total_customers` (None when `total_customers == 0`); `row_count = total_customers`. For each distribution, build `CategoryCount`/`CategoryValue` with null→`"Unknown"` relabel and `(-count, label)` ordering (reuse the sort key shape from `aggregate._counts`); apply the top-8+`"Other"` fold to `by_country`.

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd apps/backend && uv run pytest tests/test_crm_aggregate.py -q`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add apps/backend/analytics/crm_aggregate.py apps/backend/tests/test_crm_aggregate.py
git commit -m "feat(analytics): assemble DataAnalytics from crm aggregate result sets"
```

---

### Task 4: `crm_provider.fetch_crm_result_sets()` — concurrent five-query fetch

**Files:**
- Create: `apps/backend/analytics/crm_provider.py`
- Test: `apps/backend/tests/test_crm_provider.py`

**Interfaces:**
- Consumes: `ConfluentSqlClient` (Task 2).
- Produces: `fetch_crm_result_sets(client_factory: Callable[[], ConfluentSqlClient] = ConfluentSqlClient) -> tuple[dict | None, list[dict], list[dict], list[dict], list[dict]]` returning `(summary_row_or_None, by_segment, by_generation, by_gender, by_country)`. Runs the five `SELECT * FROM fct_crm_*` queries concurrently (`ThreadPoolExecutor(max_workers=5)`), each in its own `client_factory()` instance (own connection). `summary_row` is the first row of the summary result or `None`. A table whose query returns `[]` yields `[]`/`None` for its slot. Consumed by Task 5. Table-name constants `SUMMARY_TABLE`, `BY_SEGMENT_TABLE`, `BY_GENERATION_TABLE`, `BY_GENDER_TABLE`, `BY_COUNTRY_TABLE`.

- [ ] **Step 1: Write the failing tests** in `tests/test_crm_provider.py`

```python
def test_fetch_dispatches_one_select_per_table():
    # fake factory returns a client whose .query records sql and returns a
    # canned result keyed by table name.
    summary, seg, gen, gender, country = fetch_crm_result_sets(client_factory=fake_factory)
    assert summary == {"qscope": "ALL", "total_customers": 10, ...}
    assert seg == [{"customer_segment": "RETAIL", ...}]
    assert recorded_sql == {  # one SELECT * per table, in any order
        "SELECT * FROM fct_crm_summary", "SELECT * FROM fct_crm_by_segment",
        "SELECT * FROM fct_crm_by_generation", "SELECT * FROM fct_crm_by_gender",
        "SELECT * FROM fct_crm_by_country"}

def test_empty_summary_yields_none_slot():
    # factory client returns [] for the summary table.
    summary, *_ = fetch_crm_result_sets(client_factory=empty_summary_factory)
    assert summary is None

def test_one_table_raising_is_isolated():
    # factory whose client.query raises for by_country only (simulating a
    # worker error the client itself didn't swallow) -> country slot == [],
    # other slots still populated, no exception propagates.
    summary, seg, gen, gender, country = fetch_crm_result_sets(client_factory=country_boom_factory)
    assert country == [] and summary is not None
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd apps/backend && uv run pytest tests/test_crm_provider.py -q`
Expected: FAIL (module not defined).

- [ ] **Step 3: Implement `fetch_crm_result_sets`** per the Interfaces signature. Submit five `client_factory().query(f"SELECT * FROM {table}")` calls to a `ThreadPoolExecutor`; collect results by table; wrap each `future.result()` in `try/except Exception` → `[]` so a raising worker is isolated; `summary_row = summary_rows[0] if summary_rows else None`; return the five slots in fixed order.

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd apps/backend && uv run pytest tests/test_crm_provider.py -q`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add apps/backend/analytics/crm_provider.py apps/backend/tests/test_crm_provider.py
git commit -m "feat(analytics): concurrent snapshot fetch of crm aggregate products"
```

---

### Task 5: Wire the service + API to the crm path; docs

Switch the dashboard's live path to the crm products; keep mock mode folding the fixture.

**Files:**
- Modify: `apps/backend/analytics/service.py` (`get_c360_analytics` → `get_data_analytics`; branch on mode)
- Modify: `apps/backend/api/analytics_resource.py` (call `service.get_data_analytics()`)
- Modify: `apps/backend/config.py` (update the `ANALYTICS_DATA_MODE` comment: live now reads the `crm` tables via `confluent-sql`)
- Test: `apps/backend/tests/test_analytics_api.py` (add a live-degradation case)
- Modify: `docs/web_app_design.md`, `docs/demo_script.md` (crm-data-product talking point)

**Interfaces:**
- Consumes: `crm_provider.fetch_crm_result_sets` (Task 4), `crm_aggregate.assemble` (Task 3), `aggregate` (mock path), `providers._mode()`.
- Produces: `service.get_data_analytics() -> DataAnalytics`.

- [ ] **Step 1: Write the failing test** in `tests/test_analytics_api.py`

```python
# Reuse the existing live_client fixture pattern (ANALYTICS_DATA_MODE=live, NO
# Confluent creds) and assert the live path degrades to an empty 200.
def test_c360_profile_live_without_creds_is_empty_200(live_client):
    r = live_client.get("/api/v1/analytics/c360-profile")
    assert r.status_code == 200
    body = r.json()
    assert body["source_mode"] == "live"
    assert body["row_count"] == 0
    assert body["by_segment"] == []
```

- [ ] **Step 2: Run it to verify it fails**

Run: `cd apps/backend && uv run pytest tests/test_analytics_api.py -q`
Expected: FAIL (live path still uses the old `get_rows`/snapshot flow, or the fixture/assertion is new).

- [ ] **Step 3: Implement the branch** in `service.get_data_analytics()`:

```
mode = providers._mode()            # "mock" | "live"
if mode == "mock":
    rows, query_ms = _timed(lambda: list(mock_data.PROFILE_SNAPSHOT))
    return aggregate(rows, source_mode="mock", query_ms=query_ms)
result_sets, query_ms = _timed(crm_provider.fetch_crm_result_sets)
return crm_aggregate.assemble(*result_sets, source_mode="live", query_ms=query_ms)
```

Point `api/analytics_resource.py` at `service.get_data_analytics()`. Update the `config.py` comment.

- [ ] **Step 4: Run the full analytics suite**

Run: `cd apps/backend && uv run pytest tests/ -k analytics -q`
Expected: PASS (mock payload unchanged; new live-degradation test green).

- [ ] **Step 5: Update docs** — in `docs/web_app_design.md` describe the analytics live path as "five crm data products read by snapshot `SELECT`s via confluent-sql"; add the demo talking point to `docs/demo_script.md`.

- [ ] **Step 6: Commit**

```bash
git add apps/backend/analytics/service.py apps/backend/api/analytics_resource.py apps/backend/config.py apps/backend/tests/test_analytics_api.py docs/web_app_design.md docs/demo_script.md
git commit -m "feat(analytics): serve live dashboard from crm data products via confluent-sql"
```

---

## Notes / out of scope

- `FlinkSnapshotClient.snapshot()`, `_snapshot_sql`, `_SNAPSHOT_COLUMNS`, and `providers.get_rows()` become dead for the dashboard but are left in place (the per-customer lookups still use `FlinkSnapshotClient.run()`/`collapse_changelog`, and `test_analytics_snapshot.py` covers them). Removing them is a separate cleanup.
- Per-customer paths (`search_customers`, `get_customer_profile`, RTCE variants) stay on their existing clients.
- The `fct_crm_summary` `avg_age` DOUBLE fix is applied in the SQL/yml and requires a redeploy of that model (done outside this plan).
