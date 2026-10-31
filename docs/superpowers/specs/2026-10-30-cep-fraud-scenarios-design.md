# CEP Fraud Scenarios — Design

**Date:** 2026-10-30  
**Status:** Draft for approval  
**Repo:** `stream_house_c360_demo`

---

## Intent & Why

The existing Flink pipeline is a pure **analytics/aggregation** DAG: CDC events
flow through `src_*` → `dim_*` → `fct_c360_profile`, and everything downstream
reads rolled-up aggregates (balances, totals) via snapshot queries. This is a
solid streaming-analytics story, but it does not demonstrate Flink's **Complex
Event Processing (CEP)** capability — pattern matching over ordered event
sequences.

`MATCH_RECOGNIZE` is Flink SQL's native CEP clause. It detects user-defined
event sequences within a partitioned, time-ordered stream. The canonical use
case is fraud detection: a "card probe" (small test charge) immediately followed
by one or more large charges on the same account is a well-known fraudster
pattern. It is also the single clearest way to show the difference between batch
analytics and streaming CEP in one demo.

This feature adds:

1. A **controlled scenario system** — the backend can inject deterministic
   transaction sequences (not random) via a REST API, so a solution engineer can
   trigger a fraud sequence on demand.
2. **Three `MATCH_RECOGNIZE` Flink SQL models** — one per CEP pattern — deployed
   alongside the existing c360 pipeline.
3. A **CEP page** in the frontend that shows all three patterns, lets the SE fire
   a scenario with one click, and displays the matching events as they land.

The audience is an SE running a live demo. The demo narrative must be
deterministic and self-contained (no random data generation, no waiting for
organic traffic to accidentally form a pattern).

---

## Goals

- A `POST /api/v1/scenarios/{scenario_id}/run` endpoint triggers a controlled,
  scripted transaction sequence that is guaranteed to fire a `MATCH_RECOGNIZE`
  query.
- Three CEP patterns are defined as dbt/Flink SQL models:
  `fct_fraud_probe`, `fct_merchant_streak`, `fct_smb_spike`.
- A new **CEP** page in the frontend (`/cep`) lists the three patterns, lets the
  SE pick one, trigger it, and see the matched events.
- The scenario system is **self-contained** — it only needs `SINK=postgres` and
  the existing Kafka path. No new infra dependencies.
- `CEP_DATA_MODE: mock | live` mirrors the other mode toggles: mock returns a
  deterministic canned result; live injects real transactions and reads matched
  events from Flink.

## Non-Goals

- Changing the existing `fct_c360_profile` / analytics pipeline. The three new
  Flink models are **additions**, not replacements.
- A general-purpose "replay" or "load test" tool — only the specific scripted
  patterns are exposed.
- A streaming WebSocket push from Flink to the browser. The CEP page **polls**
  the backend.
- Per-customer alert routing or notification logic — this is a demo page, not
  production alerting.

## Anti-Goals

- A scenario that relies on random timing or random amounts to trigger the match.
  Every scenario is fully scripted (account ID, amounts, timestamps, sequence).
- A demo that requires the SE to wait >30 seconds to see a match. Scripted
  injection lets the match land within the Flink window.
- Secrets in the frontend — all Confluent/Kafka calls go through the backend.

---

## Domain Model Changes (prerequisite, in progress)

The following CSV-level changes are already being applied as a separate task:

| Change | Effect on CEP |
|---|---|
| Segment renamed `RETAIL` → `PRIVATE` | `fct_smb_spike` can filter `customer_segment = 'SMB'` precisely |
| Carter (`00000000-0000-0000-0000-ef1234567890`) gets a second account (`SAV-39958839`, SAVINGS, ACTIVE) | Fraud probe fires on CHECKING; SAVINGS stays clean — demonstrates per-account isolation |
| Duplicate Linda Sharma row removed | Clean 10-row seed |

These changes are prerequisites. The spec assumes them as done.

---

## Three CEP Patterns

### Pattern 1 — Fraud Probe (`fct_fraud_probe`)

**What it detects:** On the same account, a small charge (amount ≤ $10 — a
card-verification test) is immediately followed by one or more large charges
(amount ≥ $500) within a 1-hour event-time window.

**Why it matters:** This is the classic card-cloning telltale. The fraudster
tests a stolen card with a micro-charge, then immediately drains it.

**Scenario triggering it (`fraud-probe`):**
```
Account: CHK-39958838 (Carter's CHECKING)
t+0s   DEBIT  $2.99   "Card verification"   Amazon     RETAIL  ONLINE
t+30s  DEBIT  $899.00 "Electronics"         Amazon     RETAIL  ONLINE
t+60s  DEBIT  $749.00 "Gift cards"          Amazon     RETAIL  ONLINE
```
Carter's SAVINGS account (`SAV-39958839`) gets a benign $45 grocery charge at
the same time — the query must not match it.

**Flink SQL sketch:**
```sql
SELECT * FROM src_transactions
MATCH_RECOGNIZE (
    PARTITION BY account_id
    ORDER BY transacted_at
    MEASURES
        PROBE.account_id       AS account_id,
        PROBE.transacted_at    AS probe_time,
        LAST(LARGE.amount)     AS last_large_amount,
        COUNT(LARGE.*)         AS large_count
    ONE ROW PER MATCH
    AFTER MATCH SKIP TO NEXT ROW
    PATTERN (PROBE LARGE+)
    WITHIN INTERVAL '1' HOUR
    DEFINE
        PROBE AS amount <= 10.00,
        LARGE AS amount >= 500.00
)
```

---

### Pattern 2 — Merchant Streak (`fct_merchant_streak`)

**What it detects:** A customer makes 3 or more consecutive transactions in the
same merchant category (e.g. three `FOOD` charges in a row). Signals a habit or
a loyalty opportunity.

**Why it matters:** Demonstrates CEP for engagement/loyalty analytics, not just
fraud. Shows `MATCH_RECOGNIZE` is general-purpose.

**Scenario triggering it (`merchant-streak`):**
```
Customer: Maria Gonzalez (00000000-0000-0000-0000-f01234567891)
Account:  CHK-83197857
t+0s   DEBIT  $12.50  "Coffee"              Starbucks  FOOD  POS
t+5m   DEBIT  $34.00  "Team lunch"          Chipotle   FOOD  POS
t+10m  DEBIT  $89.00  "Client dinner"       Nobu       FOOD  POS
```

**Flink SQL sketch:**
```sql
SELECT * FROM src_transactions
MATCH_RECOGNIZE (
    PARTITION BY customer_id
    ORDER BY transacted_at
    MEASURES
        FIRST(A.customer_id)         AS customer_id,
        FIRST(A.merchant_category)   AS streak_category,
        FIRST(A.transacted_at)       AS streak_start,
        LAST(A.transacted_at)        AS streak_end,
        COUNT(A.*)                   AS streak_length
    ONE ROW PER MATCH
    AFTER MATCH SKIP PAST LAST ROW
    PATTERN (A{3,})
    WITHIN INTERVAL '2' HOUR
    DEFINE
        A AS (COUNT(A.*) = 1
              OR A.merchant_category = LAST(A.merchant_category))
)
```

---

### Pattern 3 — SMB Spike (`fct_smb_spike`)

**What it detects:** An SMB customer makes 3 consecutive large charges (amount
≥ $200) followed by a CREDIT (refund or expense reversal) on the same account.
Signals a business expense dispute or an anomalous spike worth flagging for the
relationship manager.

**Why it matters:** Ties the `PRIVATE` vs `SMB` segment distinction directly to
a business-relevant pattern. Shows `MATCH_RECOGNIZE` working with a join to
customer context.

**Scenario triggering it (`smb-spike`):**
```
Customer: David Okafor (00000000-0000-0000-0000-234567890124) — SMB
Account:  CHK-39587039  (re-activated as ACTIVE for this demo)
t+0s   DEBIT  $420.00  "Office supplies"    Staples    RETAIL   POS
t+5m   DEBIT  $380.00  "Cloud software"     AWS        RETAIL   ONLINE
t+10m  DEBIT  $610.00  "Hardware"           Apple      RETAIL   ONLINE
t+15m  CREDIT $1200.00 "Expense reversal"   Acme Corp  INCOME   ONLINE
```

**Flink SQL sketch:**
```sql
SELECT * FROM src_transactions t
JOIN src_customers c ON t.customer_id = c.customer_id
WHERE c.customer_segment = 'SMB'
MATCH_RECOGNIZE (
    PARTITION BY t.account_id
    ORDER BY t.transacted_at
    MEASURES
        FIRST(LARGE.account_id)   AS account_id,
        FIRST(LARGE.transacted_at) AS spike_start,
        SUM(LARGE.amount)          AS spike_total,
        REV.amount                 AS reversal_amount
    ONE ROW PER MATCH
    AFTER MATCH SKIP TO NEXT ROW
    PATTERN (LARGE{3,} REV)
    WITHIN INTERVAL '1' HOUR
    DEFINE
        LARGE AS transaction_type = 'DEBIT' AND amount >= 200.00,
        REV   AS transaction_type = 'CREDIT'
)
```

> **Note to builder:** Flink SQL's `MATCH_RECOGNIZE` applies after all joins and
> filters in the containing query. The exact placement of the `WHERE` clause and
> join with `src_customers` must be verified against the Confluent Cloud Flink SQL
> dialect at implementation time. An alternative is to pre-join in a CTE.

---

## Backend Design

### New subcomponent: `apps/backend/scenarios/`

```
apps/backend/scenarios/
├── __init__.py
├── models.py          # ScenarioRun, ScenarioResult, ScenarioTransaction
├── registry.py        # SCENARIOS dict: id → ScenarioDefinition (scripted sequences)
├── runner.py          # execute(scenario_id) → ScenarioResult
└── service.py         # orchestrates: runner + db_sink + kafka_producer
```

This mirrors the structure of `transactions/`, `accounts/`, `customers/`.

#### `models.py`

```python
class ScenarioTransaction(BaseModel):
    """One scripted transaction within a scenario."""
    account_id: UUID
    customer_id: UUID
    transaction_type: str        # DEBIT | CREDIT
    amount: float
    currency: str = "USD"
    description: str
    merchant_name: str
    merchant_category: str
    channel: str
    status: str = "COMPLETED"
    offset_seconds: int          # seconds after scenario start; runner adds to now

class ScenarioDefinition(BaseModel):
    scenario_id: str             # e.g. "fraud-probe"
    pattern: str                 # e.g. "fct_fraud_probe"
    label: str                   # human label for the UI
    description: str
    transactions: list[ScenarioTransaction]

class ScenarioRun(BaseModel):
    scenario_id: str
    triggered_at: datetime

class ScenarioResult(BaseModel):
    scenario_id: str
    pattern: str
    label: str
    triggered_at: datetime
    transactions: list[Transaction]  # actual Transaction rows after insert
```

#### `registry.py`

A module-level `SCENARIOS: dict[str, ScenarioDefinition]` populated at import
time with the three hard-coded scenario definitions above. No database, no file
— the sequences are committed Python literals. UUIDs for `account_id` and
`customer_id` are the seed values from the CSV (so they resolve against real
rows in Postgres).

The `offset_seconds` field allows the runner to shift all timestamps relative to
`datetime.now(utc)` so every scenario "just happened" whenever it is triggered.

#### `runner.py`

```python
def build_transactions(
    defn: ScenarioDefinition,
    now: datetime | None = None,
) -> list[TransactionCreate]:
    """Materialise the scripted sequence into TransactionCreate rows,
    shifting offset_seconds relative to now."""
```

Pure function (no I/O). Returns `list[TransactionCreate]` ready for the sink.
Deterministic given the same `now` and the same `ScenarioDefinition`.

#### `service.py`

```python
def run(scenario_id: str) -> ScenarioResult:
    """Look up the scenario, build rows, write to DB, publish to Kafka."""
```

- Resolves from `registry.SCENARIOS`; raises `UnknownScenarioError` if not found.
- Calls `runner.build_transactions(defn)` → `list[TransactionCreate]`.
- Delegates to `transactions.db_sink.create_many(txns)` (same path as the
  generate endpoint — no new DB logic).
- If `KAFKA_PRODUCE_ENABLED` and not `CDC_CONNECTOR_ENABLED`: calls
  `transactions.kafka_producer.produce_create_many(txns)` (same Kafka path).
- Returns `ScenarioResult` with the inserted `Transaction` rows.

#### Error types

```python
class UnknownScenarioError(ValueError): ...
```

### New: `apps/backend/api/scenario_resource.py`

```
/api/v1/scenarios
```

| Method | Path | Response | Status |
|---|---|---|---|
| `GET` | `/scenarios` | `list[ScenarioDefinition]` | 200 |
| `POST` | `/scenarios/{scenario_id}/run` | `ScenarioResult` | 201 |

`GET /scenarios` returns all defined scenarios (from the registry). Useful for
the CEP page to populate its list without hard-coding names in the frontend.

`POST /scenarios/{scenario_id}/run`:
- 201 + `ScenarioResult` on success.
- 404 if `scenario_id` not in registry.
- 409 if `SINK != "postgres"` (same guard as `/transactions/generate`).

Router registered in `main.py` at `/api/v1`.

### Config

Add one new setting to `config.py::Settings`:

```python
# CEP scenarios page: "mock" returns canned matched-event fixtures;
# "live" injects real transactions and reads from Flink MATCH_RECOGNIZE output.
CEP_DATA_MODE: str = "mock"
```

Add to `summary()` dict: `"CEP_DATA_MODE": self.CEP_DATA_MODE`.

No new credential settings — live mode reuses the existing Flink/Kafka coords.

---

## Flink SQL Models (new, under `pipelines/`)

Three new dbt models in `pipelines/models/c360/facts/`:

```
pipelines/models/c360/facts/
├── fct_fraud_probe.sql
├── fct_fraud_probe.yml
├── fct_merchant_streak.sql
├── fct_merchant_streak.yml
├── fct_smb_spike.sql
└── fct_smb_spike.yml
```

All three are `materialized_table` with:
- `changelog.mode: append` (each match is a new row — matches are never retracted)
- `kafka.cleanup-policy: delete`
- `scan.startup.mode: earliest-offset`
- `value.format: avro-registry`

They read from `{{ ref('src_transactions') }}` (Pattern 1 and 2) or
`{{ ref('src_transactions') }}` joined to `{{ ref('src_customers') }}` (Pattern 3).

They are added to `deploy_manifest.json` under the existing `pipeline` group,
after the `src_*` and `dim_*` layers they depend on.

> The exact Flink SQL syntax (particularly join placement for Pattern 3 and the
> `DEFINE` clause details) must be validated against the Confluent Cloud Flink
> SQL reference before deployment. The sketches above are design-intent, not
> final SQL.

---

## Frontend Design

### New page: `CEPPage` (`/cep`)

```
apps/frontend/src/
├── pages/CEPPage.tsx
├── pages/CEPPage.test.tsx
├── api/scenarios.ts
└── types/scenario.ts
```

#### Layout

```
┌───────────────────────────────────────────────────────┐
│  Complex Event Processing                             │
│  Three MATCH_RECOGNIZE patterns — trigger a scenario  │
│  and watch Flink detect the event sequence in real    │
│  time.                                                │
├──────────────┬──────────────┬──────────────┐
│ 🔴 Fraud    │ 🟢 Merchant  │ 🟡 SMB Spike │
│ Probe        │ Streak       │              │
│              │              │              │
│ [Trigger]    │ [Trigger]    │ [Trigger]    │
├──────────────┴──────────────┴──────────────┘
│                                                       │
│  Pattern: fct_fraud_probe                             │
│  ┌──────────────────────────────────────────────────┐│
│  │ SQL  CREATE TABLE … MATCH_RECOGNIZE (…)          ││
│  └──────────────────────────────────────────────────┘│
│                                                       │
│  Last triggered: 14 seconds ago                       │
│  Matched events                                       │
│  ┌──────────────────────────────────────────────────┐│
│  │ account_id  probe_time          large_count  …  ││
│  │ CHK-399…    2026-10-30 23:35   2            …  ││
│  └──────────────────────────────────────────────────┘│
└───────────────────────────────────────────────────────┘
```

Three cards in a horizontal row (one per pattern). Clicking a card selects it
and shows the detail panel below: the pattern SQL, a **Trigger** button, and a
results table. Clicking **Trigger** calls `POST /scenarios/{id}/run` and then
polls `GET /scenarios/{id}/matches` until results appear (or a 15s timeout).

In `CEP_DATA_MODE=mock`, the Trigger button returns canned match rows
immediately without touching Postgres or Kafka, so the demo works with no
infrastructure.

#### `types/scenario.ts`

```typescript
interface ScenarioDefinition {
  scenario_id: string;
  pattern: string;
  label: string;
  description: string;
}

interface MatchedEvent {
  [key: string]: string | number | null;  // column bag from Flink output
}

interface ScenarioResult {
  scenario_id: string;
  pattern: string;
  label: string;
  triggered_at: string;
  transactions: Transaction[];           // injected rows (for audit trail)
}
```

#### `api/scenarios.ts`

```typescript
listScenarios(): Promise<ScenarioDefinition[]>
runScenario(id: string): Promise<ScenarioResult>
```

Both call through the existing `request<T>()` client in `src/api/client.ts`.

#### Sidebar entry

Add to `Sidebar.tsx` links array:

```typescript
{ to: '/cep', label: 'CEP Patterns', Icon: Zap }
```

`Zap` is already available in `lucide-react`.

#### Route

Add to `App.tsx`:

```tsx
<Route path="/cep" element={<CEPPage />} />
```

---

## Tests

### Backend (unit, no infra)

| File | What it covers |
|---|---|
| `tests/test_scenarios_registry.py` | `SCENARIOS` has exactly 3 entries; each has non-empty `transactions` list |
| `tests/test_scenarios_runner.py` | `build_transactions` shifts offsets correctly; amounts match the definition; returns the right count |
| `tests/test_scenarios_service.py` | Success path: db_sink and kafka_producer called once; `UnknownScenarioError` on bad id; Kafka skipped when `KAFKA_PRODUCE_ENABLED=False` |
| `tests/test_scenarios_api.py` | `GET /scenarios` → 200 + list; `POST .../run` → 201; unknown id → 404; non-postgres sink → 409 |

All tests follow the same monkeypatch-at-boundary pattern used by
`test_transactions_generate.py`: no real DB, no real Kafka.

### Frontend (Vitest)

`CEPPage.test.tsx`: mock `api/scenarios`; assert three pattern cards render;
clicking Trigger calls `runScenario`; result table renders on mock response.

---

## Calls Made

1. **Scenarios are committed Python literals**, not DB records. They don't need
   CRUD — they are part of the demo script. If a new pattern is needed, it is
   added in code, not via an admin UI.

2. **`POST .../run` returns the injected `Transaction` rows**, not the Flink
   match output. Flink match output is polled separately (mock: canned; live:
   queries the `fct_fraud_probe` topic). This keeps the injection path fast and
   synchronous, while the "did Flink fire?" check is async.

3. **Okafor's account** (`CHK-39587039`) is currently `INACTIVE` in the CSV
   seed. The `smb-spike` scenario needs it active. The runner will upsert-or-skip
   the account status as a pre-step, or alternatively the CSV seed will be
   updated to mark it `ACTIVE`. Decision to builder.

4. **`CEP_DATA_MODE=mock`** is the default, matching the posture of
   `GRAPH_DATA_MODE` and `ANALYTICS_DATA_MODE`. A fresh clone demos without any
   Kafka/Flink connectivity.

5. **Pattern SQL is design-intent**, not final. The Confluent Cloud Flink SQL
   dialect imposes constraints on `MATCH_RECOGNIZE` placement within a query
   (especially with joins). The builder must validate each sketch against the
   reference docs before writing the `.sql` files.

---

## What's Left to the Builder

- Validate all three `MATCH_RECOGNIZE` SQL sketches against the Confluent Cloud
  Flink SQL reference (`MATCH_RECOGNIZE` section) and adjust syntax as needed.
- Decide whether Okafor's account is re-activated in the CSV seed or in the
  scenario runner as a pre-step (Call 3 above).
- Author the `GET /scenarios/{id}/matches` endpoint and its mock/live split
  (polling path for Flink match results). This spec scopes the injection
  (`/run`) side; the results-read side is a follow-on.
- Wire `CEP_DATA_MODE` into the `apps/backend/.env.example` with a comment.
- Choose exact CSS for the three-card layout (consistent with the existing
  plain-CSS house style in `src/styles/index.css`).
