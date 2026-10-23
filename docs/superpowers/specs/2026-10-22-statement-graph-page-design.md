# Statement Graph Page — Design

**Date:** 2026-10-22
**Status:** Draft — awaiting approval
**Repo:** `stream_house_c360_demo`

## Intent & Why

The frontend (`apps/frontend`) is a demo tool for **solution engineers** showing
off "the art of the possible" with Confluent Cloud + CLI + REST API. Today it has
CRUD pages for Customers / Accounts / Transactions. We are adding a new section
that visualizes a **data product** as the graph of deployed **Flink statements**
that build it, with **live status and metrics pulled from the Confluent Cloud REST
API**.

The demo narrative the page must support:

1. SE opens **Data Products** in the left nav → sees the list (just **c360** today).
2. Selects **c360** → sees the **statement DAG**, and at a glance that everything
   is **green and flowing** (the hero moment).
3. **Drills into one node** → sees that statement's Confluent metrics.

The topology is stable, so it is served as **static backend metadata**. The dynamic
layer — per-statement status and lag/metrics — comes from the **Confluent Cloud
REST API through a backend proxy** (keeps the Flink/Cloud API keys server-side).

## Topology (verified against the repo)

The c360 pipeline deploys **8 Flink statements in 4 dependency layers** (names +
edges from `pipelines/models/deploy_manifest.json` and the `ref()`/`source()` calls
in `pipelines/models/c360/**/*.sql`). The earlier `src_dedup_*` names were stale
`target/compiled/` artifacts and are **not** deployed.

- **CDC sources (topics, not statements):** `cdc.public.customers`,
  `cdc.public.accounts`, `cdc.public.transactions`.
- **Layer 1 — `src_*`:** `src_customers` ← customers, `src_accounts` ← accounts,
  `src_transactions` ← transactions.
- **Layer 2 — `dim_*`:** `dim_customers` ← src_customers; `dim_transactions` ←
  src_transactions; `dim_account_tx_agg` ← src_accounts + dim_transactions.
- **Layer 3 — leaves:** `dim_tx_x_account` ← dim_customers + dim_account_tx_agg;
  `fct_c360_profile` ← dim_transactions + dim_customers.

**Terminal data-product node (user decision):** **`fct_c360_profile`** — the named
fact. It is currently a **stub** (projects only `customer_id, first_name, last_name`);
building it out is out of scope here, but it is the designated "product" node the
drill-down narrative builds toward. `dim_tx_x_account` is the other leaf and is
rendered as a normal statement node.

## Goals

- A **Data Products** left-nav entry → a list page → a per-product **graph page**.
- Graph renders the c360 topology as a **layered DAG of all 8 statements** (4 layers,
  left→right), with the three CDC source topics as upstream nodes.
- **Node color = status** applies to **statements**; the CDC source/sink **topic**
  nodes render **neutral** ("connected", not green/red). Statement status:
  green = RUNNING, amber = lag/degraded, red = FAILED/STOPPED. Animated edges convey
  "flowing". (A topic-level activity signal could color topics later — out of scope now.)
- A **health summary bar** at the top of the graph counting **statements** ("7 / 8
  RUNNING · 1 with lag · refreshed Ns ago") — confirmed as a key element.
- **Click a node → drill-down panel** with Confluent metrics: status, uptime,
  messages in/s, messages out/s, pending records (lag), compute pool used, and the
  statement SQL.
- A **mock / offline mode** so "green and flowing" is guaranteed during a live demo
  even with no cloud connectivity, and a **live mode** that calls Confluent.
- Status **auto-refreshes** on a short interval to make the graph feel live.

## Non-Goals (this iteration)

- No **write/control** actions (no start/stop/resume statements from the UI) — read-only.
- No **historical time-series charts** in the drill-down — current/point-in-time
  metrics only (the mockup's SQL + metric rows, not sparklines).
- **One data product (c360)**; the list is built to hold more but only c360 ships.
- No per-statement **exception/stack-trace** viewer (could be a later add).
- No frontend auth / multi-tenant concerns.

## Anti-Goals (what makes this a failure even if it "works")

- A demo that **breaks or shows red** because of transient cloud/network issues —
  mock mode must make the happy path deterministic.
- **Secrets in the frontend** — the browser must never see a Confluent API key;
  all Confluent calls go through the backend proxy.
- A graph that is **hand-maintained in two places** — the node/edge topology has a
  single source of truth (the backend descriptor, see below).

## Constraints & Existing Structure

- **Frontend:** React 19 + Vite 8, `react-router-dom` 7, `lucide-react`, plain CSS
  in `src/styles/index.css` (no CSS framework). Pages under `src/pages`, routed in
  `src/App.tsx` inside `AppLayout`; nav in `src/components/Sidebar.tsx`. API calls go
  through `src/api/client.ts` → `request<T>()` against `VITE_API_BASE_URL`
  (default `http://localhost:8000/api/v1`). Tests use Vitest + Testing Library;
  existing pages each have a `.test.tsx`.
- **Backend:** FastAPI, routers under `apps/backend/api/*` mounted at `/api/v1`,
  CORS allows `http://localhost:5173`. `config.py::Settings` (pydantic-settings)
  already loads Kafka + Schema Registry creds from env; **uv** is the runner.
- **Topology source:** dbt/Flink project under `pipelines/`. The DAG
  (`pipelines/target/manifest.json`, `pipelines/models/deploy_manifest.json`):
  sources `cdc.public.{customers,accounts,transactions}` → statements
  `src_{customers,accounts,transactions}` (+ `src_dedup_*`). Deployed statement
  names follow the `deploy_manifest.json` pattern, e.g.
  `models-c360-sources-pipeline-src-dedup-customers`.
- **Confluent posture (from CLAUDE.md):** credentials are referenced from
  `~/.confluent/.env`; never write literal keys into code or committed files.

## Decisions Made (my calls unless marked "theirs")

### D1. Graph library — **React Flow** (`@xyflow/react`) with a layered layout
Not currently a dependency. Standard fit for React 19 interactive DAGs; supports
custom nodes (status color + lag badge) and a right-side panel. Because the real
topology is a **4-layer DAG** (not three flat lanes), node positions come from a
**layered/left-to-right layout** — `dagre` (or `@dagrejs/dagre`) to compute
coordinates, or React Flow's layouting example. *(My call.)*

### D2. Topology as a committed backend **descriptor file**
A single source of truth: `apps/backend/data_products/c360.json`, hand-authored from
the verified topology above (it could later be generated from the dbt manifest). It
encodes nodes (`id`, `type` ∈ `source|statement`, `label`, the **deployed statement
name** from `deploy_manifest.json`, the backing **topic**, and a `is_product` flag on
`fct_c360_profile`) and edges. The backend serves it verbatim. The deployed statement
names are known (pattern `models-c360-<subdir>-pipeline-<model>`), so no naming guess
remains. *(My call; topology confirmed with the user.)*

### D3. Backend **proxy** with a `GRAPH_DATA_MODE` toggle: `mock` | `live` (**mock-first**)
New settings in `config.py`: `GRAPH_DATA_MODE` (default **`mock`** so a fresh clone
demos green out of the box), plus live-mode config:
`CONFLUENT_CLOUD_API_KEY/SECRET` (Metrics API), `FLINK_API_KEY/SECRET`,
`FLINK_ORG_ID`, `FLINK_ENV_ID`, `FLINK_COMPUTE_POOL_ID`, `FLINK_REST_ENDPOINT`
(region host). All read from env (`~/.confluent/.env` style); none committed.
- **mock mode:** status/metrics come from a deterministic in-repo fixture
  (everything RUNNING, realistic rates, zero/low lag). The mockup's amber
  "lagging" node is a *mock preset variant* so the SE can show the unhappy state on
  demand (e.g. `GRAPH_DATA_MODE=mock-lag`), but the default preset is all-green.
- **live mode (best-effort):** the 8 statements are **not confirmed deployed/running**
  in the target env yet (user), and two models are unfinished — `fct_c360_profile` is a
  stub and `dim_account_tx_agg` may have an invalid clause order — so live mode could
  legitimately return non-RUNNING/red for some nodes. Therefore **mock is the primary
  demo path**; live mode is wired but **best-effort and status-only-tolerant**: it maps
  whatever Confluent returns to the common shape, and where a metric is unavailable it
  returns `null` and the UI shows "—" rather than failing. Mock remains the default so a
  fresh clone demos green. *(My call, per user's "mock-first" decision.)*

### D4. Confluent REST endpoints used in live mode
- **Statement status:** Flink SQL REST API — list/get statements under the org/env,
  read `status.phase` (RUNNING / PENDING / DEGRADED / FAILED / STOPPED / …).
- **Metrics (rates, pending records/lag, compute pool):** Confluent Cloud **Metrics
  API** (`api.telemetry.confluent.cloud/v2/metrics/.../query`) with Flink metric
  descriptors, filtered by compute pool / statement.
- **Exact metric descriptor names are an implementation detail the builder must
  verify against current Confluent docs** (e.g. records-in/out and
  pending-records metrics). Where a per-second rate isn't exposed directly, derive
  it from counter deltas between polls. *(My call; naming flagged for the builder.)*

### D5. Routing, nav, and refresh
- Nav: add **Data Products** to `Sidebar.tsx` (lucide icon, e.g. `Workflow`/`Share2`).
- Routes in `App.tsx`: `/data-products` (list) and `/data-products/:id` (graph).
- The graph page **polls status every 5s** (drives node colors, health bar, and the
  "refreshed Ns ago" label). The drill-down metrics fetch on node selection and
  refresh on the same interval while a node is open. *(My call.)*

### D6. Visual style
Match the existing plain-CSS look; add graph/panel styles to `src/styles/index.css`
following the mockup (lanes, status dots, health pills, metric rows, dark SQL box).
No new CSS framework. *(My call.)*

## Backend API (new router, `apps/backend/api/data_product_resource.py`, `/api/v1`)

All responses are JSON; `mock` and `live` modes return identical shapes.

- `GET /data-products`
  → `[{ id, name, description, statement_count }]` (from descriptor files).
- `GET /data-products/{id}/graph`
  → `{ nodes: [{ id, type, label, statement_name, topic }], edges: [{ source, target }] }`
  (static descriptor, D2).
- `GET /data-products/{id}/status`
  → `[{ node_id, statement_name, phase, lagging: bool }]` for **statement** nodes only
  (source/sink topic nodes carry no phase and render neutral), plus a rollup the health
  bar uses (`running`, `total`, `lagging`) counted over statements. Drives node coloring.
  (mock or live.)
- `GET /data-products/{id}/nodes/{node_id}/metrics`
  → `{ status, uptime_seconds, messages_in_per_sec, messages_out_per_sec,
       pending_records, compute_pool_cfu, sql }`. (mock or live.)

Live-mode Confluent calls live behind a small client module
(`apps/backend/confluent/`), unit-tested with mocked HTTP; the resource layer never
talks to Confluent directly.

## Frontend pieces

- `src/api/dataProducts.ts` — typed wrappers over the four endpoints (via
  `request<T>()`).
- `src/types/dataProduct.ts` — `DataProduct`, `GraphNode`, `GraphEdge`,
  `NodeStatus`, `NodeMetrics`.
- `src/pages/DataProductsPage.tsx` — list (table like the existing pages),
  row → `/data-products/:id`.
- `src/pages/StatementGraphPage.tsx` — React Flow DAG (custom status-colored node),
  health bar, selection → drill-down panel; 5s polling.
- `src/components/Sidebar.tsx` — new nav entry; `src/App.tsx` — two routes.
- Styles appended to `src/styles/index.css`.
- Vitest tests alongside new pages/api (mirroring existing `.test.tsx` convention),
  mocking the api module; assert health rollup, node coloring by status, and panel
  rendering from a fixture.

## What's Left to the Builder

- Verify exact **Confluent Metrics API descriptor names** and the Flink Statements
  API path/version against current docs (D4); implement rate derivation where needed.
- Decide the precise **uptime** source (statement `createdAt`/updated timestamp vs a
  metric) in live mode.
- Choose the exact lucide icon and finalize spacing/CSS to match the house style.
- Author the committed **`c360.json` descriptor** from the verified topology (8
  statements, edges, `is_product` on `fct_c360_profile`, backing topic per node).
- Wire `GRAPH_DATA_MODE` presets (`mock`, `mock-lag`, `live`) and document the env
  vars in `apps/backend/.env.example` (names only, no secrets).
- In live mode, tolerate unhealthy/unfinished statements gracefully (null metrics →
  "—"); do not let a FAILED statement break the page.

## Resolved Decisions (from brainstorming)

- **Topology** verified against the repo: 8 statements, 4 layers (see Topology section).
  The `src_dedup_*` names were stale artifacts and are dropped.
- **Terminal data-product node:** `fct_c360_profile` (user), rendered with an
  `is_product` flag; it is a stub today and building it out is out of scope.
- **Live mode:** mock-first; live is best-effort and metric-tolerant (user).
- **Topic nodes** (CDC sources / sink) render neutral; status color is for statements.
