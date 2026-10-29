# CRM analytics data products → dashboard via snapshot queries

**Date:** 2026-10-29
**Status:** Draft for approval

## Intent and why

The C360 analytics dashboard (`GET /analytics/c360-profile`) currently pulls
**every customer row** of `fct_c360_profile` over the wire (up to
`ANALYTICS_SNAPSHOT_LIMIT = 50_000`) and folds the aggregates in Python
(`analytics/aggregate.py`). That contradicts the demo's thesis — *build the
analytics as data products in Flink and expose them through a simple query*.

The aggregates now exist as a dedicated **`crm`** Flink data product (five
`streaming_table` models, already deployed). This change rewires the dashboard's
live path to run **five trivial snapshot `SELECT`s** against those tables and
assemble the response — so Flink does the aggregation and only summary rows
cross the wire. The snapshot queries are issued through the official
**`confluent-sql`** DB-API driver (per the maintainer's direction to use it).

## Goals

- Live dashboard aggregates come from the `crm` tables, not Python folding.
- Snapshot queries run through `confluent-sql` (snapshot-mode cursors).
- The HTTP response shape is unchanged (frontend keeps working), except the
  payload model is renamed `C360Analytics` → `DataAnalytics`.
- Mock mode keeps working with zero infra (unchanged default).
- Failure-tolerant: an unconfigured/failed live query yields an empty payload
  (`row_count == 0`), never a 500 — matching today's contract.

## Non-goals

- Changing the `crm` Flink SQL / contracts. They are built, deployed, and (per
  the maintainer) the earlier by_gender/by_country swap is already fixed. This
  doc treats them as a fixed upstream.
- Migrating the **per-customer** paths (`search_customers`,
  `get_customer_profile`, and their RTCE/Lightning variants) to `confluent-sql`.
  They keep using `FlinkSnapshotClient` / `LightningClient`. (Can follow later.)
- Changing the HTTP route path `/analytics/c360-profile` or the frontend API
  function name `getC360Analytics`. Only the payload *type* is renamed. (See
  "Calls I made" — open to widening if desired.)

## Anti-goals (would make this a failure even if it "works")

- Mock mode requiring any Flink/Confluent configuration to render.
- A live-query failure turning into a dashboard error instead of an empty state.
- Numbers that silently disagree with what `aggregate()` produced for the same
  data (the mock path stays the behavioural reference).

## Current state (as of this doc)

- `api/analytics_resource.py::get_c360_profile` → `service.get_c360_analytics()`
  → `providers.get_rows()` (mock fixture **or** full-table
  `FlinkSnapshotClient.snapshot`) → `aggregate(rows)`.
- `confluent-sql` is **not** yet a dependency.
- Config already carries every coordinate `confluent_sql.connect()` needs
  (`FLINK_ORG_ID`, `FLINK_ENV_ID`, `CLOUD_PROVIDER`, `CLOUD_REGION`,
  `FLINK_COMPUTE_POOL_ID`, `FLINK_API_KEY/SECRET`, `FLINK_DATABASE`).

## The data products (fixed upstream — reference)

All in `pipelines/models/crm/facts/`, `streaming_table`, upsert changelog,
avro-registry, each distributed by its PK. Built from `fct_c360_profile`.

| Table | PK | Columns | Feeds |
|---|---|---|---|
| `fct_crm_summary` | `qscope='ALL'` | total_customers, total_accounts, total_balance, total_transactions, total_txn_amount, avg_age, missing_email, missing_phone, complete | all scalar KPIs + `data_quality` |
| `fct_crm_by_segment` | `customer_segment` | customer_count, total_balance | `by_segment` **and** `balance_by_segment` |
| `fct_crm_by_generation` | `generation_segment` | generation_count | `by_generation` |
| `fct_crm_by_gender` | `gender` | gender_count | `by_gender` |
| `fct_crm_by_country` | `country` | country_count | `by_country` |

Derived, not stored: `avg_balance_per_customer = total_balance / total_customers`
(computed in the backend). `by_country` is trimmed to top-8 + "Other" in the
backend (the table stays full-fidelity).

## Backend design

### New: `confluent-sql` dependency

Add `confluent-sql` to `apps/backend/pyproject.toml` dependencies.
`import confluent_sql`.

### New: `analytics/confluent_sql_client.py` — `ConfluentSqlClient`

**Snapshot semantics (resolved by research, confirm with a live probe):**
`confluent-sql` defaults to **snapshot mode**, which uses Confluent Cloud's
native *snapshot query* — it returns the table's current state (one latest row
per PK) as a **finite** result set and terminates. This is why it replaces the
changelog-draining in `FlinkSnapshotClient`: no `collapse_changelog`, no
long-poll loop, no row cap. Residual risk until probed live (see "Spike" below).

**Catalog/database:** the catalog is the environment, resolved from
`environment_id` (no separate catalog param — `FLINK_CATALOG` is *not* required
by `connect()`). Database is `FLINK_DATABASE` (cluster display name); pass
`database_kafka_cluster_id=KAFKA_CLUSTER_ID` (the `lkc-…`) as well so the driver
skips the CMK cluster-id lookup.

A thin, **failure-tolerant** wrapper mirroring `FlinkSnapshotClient`'s posture
(missing creds or any error → `[]`, never raises; logs the reason). One method:

```python
def query(self, sql: str) -> list[dict]:
    """Run one snapshot SELECT; return [{column: value}]. [] on any failure."""
```

- Lists required settings (the `connect()` coordinates); if any is unset, log and
  return `[]` (same pattern as `_missing_settings`).
- `connect(organization_id=FLINK_ORG_ID, environment_id=FLINK_ENV_ID,
  cloud_provider=CLOUD_PROVIDER, cloud_region=CLOUD_REGION,
  compute_pool_id=FLINK_COMPUTE_POOL_ID, flink_api_key=FLINK_API_KEY,
  flink_api_secret=FLINK_API_SECRET, database=FLINK_DATABASE,
  database_kafka_cluster_id=KAFKA_CLUSTER_ID)`.
- Required-settings list therefore **adds** `CLOUD_PROVIDER` and `CLOUD_REGION`
  (today optional, set only for RTCE/Lightning). This couples analytics-live to
  those being provisioned — see "What's left to the builder".
- `cursor = conn.cursor()` (snapshot mode is the default), `cursor.execute(sql)`,
  build dicts from `cursor.description` column names + rows, `close()` in
  `finally`.
- Catch `Exception` broadly (DB-API error taxonomy varies) and return `[]`, same
  as the REST client's best-effort contract.

### New: `analytics/crm_provider.py` (or extend `providers.py`)

```python
def get_crm_analytics() -> tuple[dict, str]:
    """Return (assembled-aggregate dict, source_mode) for the dashboard.

    mock  → fold mock_data.PROFILE_SNAPSHOT via the existing aggregate() path.
    live  → run the five crm snapshot SELECTs and assemble.
    """
```

- **Mock mode:** unchanged behaviour — reuse `aggregate(PROFILE_SNAPSHOT, "mock")`.
  (Keeps every existing mock test valid and the empty-state contract intact.)
- **Live mode:** issue the five `SELECT * FROM fct_crm_*` snapshot queries
  **concurrently** via a `ThreadPoolExecutor` (each worker uses its own
  `ConfluentSqlClient` → its own connection, since DB-API connections are not
  guaranteed thread-safe). Latency is the demo's point, so 5 parallel round-trips
  beat 5 sequential. If `fct_crm_summary` comes back empty, return the empty
  payload (`row_count == 0`) — the dashboard shows its empty state.

### New: `analytics/crm_aggregate.py::assemble(summary_row, by_segment, by_generation, by_gender, by_country, source_mode, query_ms)`

Pure function (unit-testable, no I/O) that maps the five result sets to a
`DataAnalytics`:

- scalars + `data_quality` from the single `fct_crm_summary` row;
  `avg_balance_per_customer = total_balance / total_customers` (None if 0 rows);
  `row_count = total_customers`.
- `by_segment` → `CategoryCount(label=customer_segment, count=customer_count)`,
  largest-first; `balance_by_segment` → `CategoryValue(label=customer_segment,
  value=total_balance)`, largest-first.
- `by_generation` / `by_gender` → `CategoryCount` from their `*_count` columns.
- `by_country` → `CategoryCount` from `country_count`, largest-first, then the
  top-8 + "Other" rollup (reuse the existing `_COUNTRY_TOP_N` logic).
- Decimal cells come back as `Decimal`/str; coerce with the existing `_num`
  helpers.
- **Match `aggregate()` exactly** (anti-goal: no silent mock/live divergence):
  relabel null/empty category keys to `"Unknown"` and order every distribution
  by `(-count, label)` — the ascending-label tie-break from `aggregate._counts`.
  Covered by explicit unit tests (null-key row, tie-break).

### Changed: `analytics/service.py`

`get_c360_analytics()` → `get_data_analytics()` (keep a thin alias if any caller
needs the old name): time the provider call with `_timed`, return the assembled
`DataAnalytics`.

### Changed: `analytics/models.py`

Rename `class C360Analytics` → `class DataAnalytics` (fields unchanged). Update
the module docstring to say it serves c360 + crm products.

### Changed: `api/analytics_resource.py`

`response_model=DataAnalytics`; import rename. Route path unchanged.

### Config

No new fields. Update the `ANALYTICS_DATA_MODE` comment to note live mode now
reads the `crm` tables via `confluent-sql`. `ANALYTICS_SNAPSHOT_LIMIT` becomes
unused by this path (the per-customer paths may still reference it) — leave it.

### Fate of `aggregate.py` and `FlinkSnapshotClient`

- `aggregate.py` **stays** — it is the mock-mode assembler and the behavioural
  reference.
- `FlinkSnapshotClient` **stays** — still used by the per-customer lookups.
- `providers.get_rows()` is no longer called by the dashboard path; keep it (the
  per-customer paths and tests may use it) unless it becomes dead — decide during
  implementation.

## Frontend design

- `types/analytics.ts`: rename `interface C360Analytics` → `DataAnalytics`.
- `api/analytics.ts`: `getC360Analytics(): Promise<DataAnalytics>` (function name
  kept; return type renamed). Route unchanged.
- `pages/AnalyticsPage.tsx`, `pages/AnalyticsPage.test.tsx`, `api/analytics.test.ts`:
  update the type import/annotation.

## Tests

- **`crm_aggregate` (new, unit):** feed representative five-table result sets →
  assert the assembled `DataAnalytics` (ordering, top-8+Other, derived average,
  data-quality, empty-input → empty payload).
- **`ConfluentSqlClient` (new, unit):** missing settings → `[]`; a stubbed
  connection/cursor → dict rows; an exception → `[]`.
- **Live provider (new, unit):** monkeypatch the client to return canned rows →
  assert assembly and concurrency path; empty summary → empty payload.
- **Existing mock/service tests:** keep passing after the rename (update the
  symbol name only).
- **Frontend:** existing analytics tests pass after the type rename.
- TDD per `superpowers:test-driven-development` for the new pure/unit pieces.

## Docs

- `docs/web_app_design.md`: update the analytics section to describe the `crm`
  data products + snapshot-query path.
- `docs/demo_script.md`: note the "aggregates as Flink data products, read by a
  simple snapshot query via confluent-sql" talking point.

## Calls I made (builder's discretion, flagged for your check)

1. **Keep route path + `getC360Analytics` name**; rename only the type. Less
   churn; the route is a stable contract. Widen if you'd rather generalize both.
2. **Mock mode keeps folding via `aggregate()`** rather than mock fixtures for
   the five tables — far less to maintain and preserves the behavioural
   reference. (Alternative: add five mock fixtures.)
3. **Concurrent five-query fetch** via thread pool, one connection per worker.
4. **Keep `aggregate.py` and `FlinkSnapshotClient`**; scope the migration to the
   dashboard aggregates only.

## What's left to the builder

- Exact `confluent-sql` error types to catch and the precise `cursor.description`
  shape (confirm against the installed version at implementation time).
- Whether `providers.get_rows()` becomes dead and can be removed.
- Decimal/None coercion details reusing existing `_num`/`_to_*` helpers.

## Resolved by live probe (2026-10-29)

A throwaway `confluent-sql` spike ran against the live tables and confirmed:

- **Snapshot mode is finite + current-state + terminating.**
  `SELECT * FROM fct_crm_summary` → **1** row (`qscope='ALL'`), clean end, ~8.0s;
  `SELECT * FROM fct_crm_by_segment` → **2** rows (one per segment), clean end,
  ~4.8s. No changelog collapse needed. (Builder-check #1 closed.)
- **Catalog/db/region resolve** from `environment_id` + `FLINK_DATABASE` +
  `database_kafka_cluster_id=KAFKA_CLUSTER_ID`; `CLOUD_PROVIDER`/`CLOUD_REGION`
  are set in the live env (`backend/.env` / `ENV_CONFIG_FILE`). (#2, #3 closed.)
- **Latency ~5–8s per snapshot query** — accepted by the maintainer. Confirms
  the concurrent five-query fetch (caps total at ~max ≈ 8s, not the ~30s sum).
  The analytics page is intentionally not sub-second (unlike RTCE lookups).
- **`avg_age` fix applied upstream:** `fct_crm_summary.sql` now
  `AVG(CAST(age_years AS DOUBLE))` and the yml types it `double` (was integer-
  truncated). **Needs redeploy of `fct_crm_summary`.** Removes the mock/live
  divergence on the age average. (Builder-check #4 — SQL half fixed; the
  assembler still mirrors `aggregate()`'s Unknown-relabel + tie-break.)
