# Statement Graph Page Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a "Data Products" section to the demo frontend that renders the c360 Flink-statement pipeline as a live, layered DAG with per-statement status/lag and a click-to-drill metrics panel, fed by a backend proxy that runs mock-first and best-effort live against the Confluent Cloud REST API.

**Architecture:** The backend serves a committed topology descriptor (static) plus status/metrics from a provider chosen by `GRAPH_DATA_MODE` (`mock` default, `mock-lag`, `live`); mock and live return identical shapes. The frontend is a React Flow layered graph + drill-down panel that polls the status endpoint and fetches metrics on node selection. No Confluent credentials ever reach the browser.

**Tech Stack:** Backend — FastAPI, pydantic-settings, httpx (already a dependency), uv, pytest. Frontend — React 19, Vite 8, react-router-dom 7, `@xyflow/react` (React Flow) + `@dagrejs/dagre` for layout, lucide-react, plain CSS, Vitest + Testing Library.

**Spec:** `docs/superpowers/specs/2026-10-22-statement-graph-page-design.md`

## Global Constraints

- Python `>=3.11`; all Python run via `uv` (`uv run ...`); add deps with `uv add`, never hand-edit `pyproject.toml` / `uv.lock`.
- No secrets in code or committed files; live-mode Confluent creds come from env only (`~/.confluent/.env` style). `.env.example` lists names, never values.
- Backend routes mounted under `/api/v1`; CORS already allows `http://localhost:5173` — do not change it.
- `GRAPH_DATA_MODE` ∈ `{mock, mock-lag, live}`, default **`mock`** so a fresh clone demos green.
- Frontend `request<T>()` already bakes in the `/api/v1` base — api wrappers pass paths **without** that prefix (e.g. `/data-products`).
- No new CSS framework; append styles to `src/styles/index.css` in the existing plain-CSS style.
- Status color and health-bar counts apply to **statement** nodes only; `source` (topic) nodes render neutral.
- Terminal data-product node is `fct_c360_profile` (`is_product: true`); it is a stub today — do not try to fix the SQL.
- Read-only: no endpoints or UI that start/stop/alter statements.

## Review Focus

- **Unknown data-product id** (`GET /data-products/zzz/graph`): expected `404`, not `500`. → Task 4.
- **Unknown node id for metrics** (`GET /data-products/c360/nodes/zzz/metrics`): expected `404`. → Task 4.
- **Live Confluent unreachable/timeout:** the status/metrics endpoint must still return a valid payload (statements marked `UNKNOWN`/neutral), never surface a 5xx to the browser. → Task 3.
- **Live statement in FAILED/unrecognized phase, or a metric missing:** phase maps to a known enum (red for FAILED), a missing metric becomes `null` and renders as "—"; the page does not crash. → Task 3 (mapping) + Task 8 ("—" rendering).
- **A single failed status poll on the frontend:** the graph keeps the last good state rather than blanking; the next poll recovers. → Task 8.

---

### Task 1: Backend topology models + committed descriptor + loader

**Files:**
- Create: `apps/backend/data_products/__init__.py`
- Create: `apps/backend/data_products/models.py`
- Create: `apps/backend/data_products/descriptors/c360.json`
- Create: `apps/backend/data_products/descriptor.py`
- Test: `apps/backend/tests/test_data_products_descriptor.py`

**Interfaces:**
- Produces:
  - `models.py`: `GraphNode(id: str, type: Literal["source","statement"], label: str, statement_name: str | None, topic: str | None, is_product: bool = False)`; `GraphEdge(source: str, target: str)`; `Graph(nodes: list[GraphNode], edges: list[GraphEdge])`; `DataProductSummary(id: str, name: str, description: str, statement_count: int)`.
  - `descriptor.py`: `list_summaries() -> list[DataProductSummary]`; `get_graph(product_id: str) -> Graph | None` (None when unknown).

- [ ] **Step 1: Write `apps/backend/data_products/descriptors/c360.json`** with exactly these nodes and edges (topology verified in the spec):

```json
{
  "id": "c360",
  "name": "Customer 360",
  "description": "Customer 360 profile built from CDC sources via Flink.",
  "nodes": [
    { "id": "topic_customers", "type": "source", "label": "cdc.public.customers", "topic": "cdc.public.customers" },
    { "id": "topic_accounts", "type": "source", "label": "cdc.public.accounts", "topic": "cdc.public.accounts" },
    { "id": "topic_transactions", "type": "source", "label": "cdc.public.transactions", "topic": "cdc.public.transactions" },
    { "id": "src_customers", "type": "statement", "label": "src_customers", "statement_name": "models-c360-sources-pipeline-src-customers", "topic": "src_customers" },
    { "id": "src_accounts", "type": "statement", "label": "src_accounts", "statement_name": "models-c360-sources-pipeline-src-accounts", "topic": "src_accounts" },
    { "id": "src_transactions", "type": "statement", "label": "src_transactions", "statement_name": "models-c360-sources-pipeline-src-transactions", "topic": "src_transactions" },
    { "id": "dim_customers", "type": "statement", "label": "dim_customers", "statement_name": "models-c360-dimensions-pipeline-dim-customers", "topic": "dim_customers" },
    { "id": "dim_transactions", "type": "statement", "label": "dim_transactions", "statement_name": "models-c360-dimensions-pipeline-dim-transactions", "topic": "dim_transactions" },
    { "id": "dim_account_tx_agg", "type": "statement", "label": "dim_account_tx_agg", "statement_name": "models-c360-dimensions-pipeline-dim-account-tx-agg", "topic": "dim_account_tx_agg" },
    { "id": "dim_tx_x_account", "type": "statement", "label": "dim_tx_x_account", "statement_name": "models-c360-dimensions-pipeline-dim-tx-x-account", "topic": "dim_tx_x_account" },
    { "id": "fct_c360_profile", "type": "statement", "label": "fct_c360_profile", "statement_name": "models-c360-facts-pipeline-fct-c360-profile", "topic": "fct_c360_profile", "is_product": true }
  ],
  "edges": [
    { "source": "topic_customers", "target": "src_customers" },
    { "source": "topic_accounts", "target": "src_accounts" },
    { "source": "topic_transactions", "target": "src_transactions" },
    { "source": "src_customers", "target": "dim_customers" },
    { "source": "src_transactions", "target": "dim_transactions" },
    { "source": "src_accounts", "target": "dim_account_tx_agg" },
    { "source": "dim_transactions", "target": "dim_account_tx_agg" },
    { "source": "dim_customers", "target": "dim_tx_x_account" },
    { "source": "dim_account_tx_agg", "target": "dim_tx_x_account" },
    { "source": "dim_transactions", "target": "fct_c360_profile" },
    { "source": "dim_customers", "target": "fct_c360_profile" }
  ]
}
```

- [ ] **Step 2: Write the failing test** in `test_data_products_descriptor.py`:

```python
def test_c360_graph_has_8_statements_3_sources():
    graph = descriptor.get_graph("c360")
    statements = [n for n in graph.nodes if n.type == "statement"]
    sources = [n for n in graph.nodes if n.type == "source"]
    assert len(statements) == 8 and len(sources) == 3

def test_fct_c360_profile_is_the_product_node():
    graph = descriptor.get_graph("c360")
    product = [n for n in graph.nodes if n.is_product]
    assert [n.id for n in product] == ["fct_c360_profile"]

def test_unknown_product_returns_none():
    assert descriptor.get_graph("nope") is None

def test_list_summaries_reports_statement_count_8():
    summaries = descriptor.list_summaries()
    c360 = next(s for s in summaries if s.id == "c360")
    assert c360.statement_count == 8
```

- [ ] **Step 3: Run tests, verify they fail** — `cd apps/backend && uv run pytest tests/test_data_products_descriptor.py -v` → FAIL (module not found).
- [ ] **Step 4: Implement `models.py`** (the Pydantic models above) and **`descriptor.py`** (`list_summaries`, `get_graph`): load every `*.json` under `descriptors/`, parse into `Graph`; `statement_count` = count of `type == "statement"` nodes. Use `pathlib` relative to the module file.
- [ ] **Step 5: Run tests, verify they pass.**
- [ ] **Step 6: Commit** — `git add apps/backend/data_products apps/backend/tests/test_data_products_descriptor.py && git commit -m "feat(backend): c360 data-product topology descriptor + loader"`

---

### Task 2: Mock status & metrics provider + `GRAPH_DATA_MODE` config

**Files:**
- Modify: `apps/backend/config.py` (add settings)
- Create: `apps/backend/data_products/mock_data.py`
- Create: `apps/backend/data_products/providers.py`
- Test: `apps/backend/tests/test_data_products_mock.py`

**Interfaces:**
- Produces:
  - `models.py` additions (define here): `NodeStatus(node_id: str, statement_name: str | None, phase: Literal["RUNNING","PENDING","DEGRADED","FAILED","STOPPED","UNKNOWN"], lagging: bool)`; `StatusRollup(running: int, total: int, lagging: int)`; `StatusResponse(statements: list[NodeStatus], rollup: StatusRollup)`; `NodeMetrics(node_id: str, phase: str, uptime_seconds: int | None, messages_in_per_sec: float | None, messages_out_per_sec: float | None, pending_records: int | None, compute_pool_cfu: float | None, sql: str | None)`.
  - `providers.py`: `class StatusProvider(Protocol)` with `get_status(graph: Graph) -> StatusResponse` and `get_metrics(graph: Graph, node_id: str) -> NodeMetrics`; `get_provider() -> StatusProvider` (factory reading `settings.GRAPH_DATA_MODE`).
- Consumes: `descriptor.Graph` (Task 1).

- [ ] **Step 1: Write the failing test** in `test_data_products_mock.py`:

```python
def test_mock_status_all_running(monkeypatch):
    monkeypatch.setenv("GRAPH_DATA_MODE", "mock")
    graph = descriptor.get_graph("c360")
    status = providers.MockProvider(lagging=False).get_status(graph)
    assert status.rollup.total == 8 and status.rollup.running == 8 and status.rollup.lagging == 0
    assert all(s.phase == "RUNNING" for s in status.statements)

def test_mock_lag_marks_src_transactions():
    graph = descriptor.get_graph("c360")
    status = providers.MockProvider(lagging=True).get_status(graph)
    assert status.rollup.lagging == 1
    lagging = [s.node_id for s in status.statements if s.lagging]
    assert lagging == ["src_transactions"]

def test_mock_status_covers_statements_only():
    graph = descriptor.get_graph("c360")
    status = providers.MockProvider(lagging=False).get_status(graph)
    assert {s.node_id for s in status.statements} == {n.id for n in graph.nodes if n.type == "statement"}

def test_mock_metrics_has_full_fields():
    graph = descriptor.get_graph("c360")
    m = providers.MockProvider(lagging=False).get_metrics(graph, "src_customers")
    assert m.phase == "RUNNING" and m.pending_records == 0
    assert m.messages_in_per_sec is not None and m.sql is not None

def test_factory_selects_mock_lag(monkeypatch):
    monkeypatch.setenv("GRAPH_DATA_MODE", "mock-lag")
    import importlib; import config; importlib.reload(config)
    prov = providers.get_provider()
    assert isinstance(prov, providers.MockProvider) and prov.lagging is True
```

- [ ] **Step 2: Run tests, verify they fail.**
- [ ] **Step 3: Add settings to `config.py`** — `GRAPH_DATA_MODE: str = "mock"` plus the live-mode fields (unused until Task 3): `CONFLUENT_CLOUD_API_KEY/SECRET`, `FLINK_API_KEY/SECRET`, `FLINK_ORG_ID`, `FLINK_ENV_ID`, `FLINK_COMPUTE_POOL_ID`, `FLINK_REST_ENDPOINT`, all `str | None = None`.
- [ ] **Step 4: Implement `mock_data.py`** — deterministic per-node figures (all `RUNNING`; realistic `messages_in/out_per_sec`, `pending_records = 0`, a short SQL snippet per statement; `uptime_seconds` fixed) — and **`providers.py`** `MockProvider(lagging: bool)`: `get_status` builds `NodeStatus` for statement nodes (when `lagging`, set `src_transactions` `lagging=True`, non-zero `pending_records` in its metrics, phase stays `RUNNING`); `get_metrics` returns the node's mock figures; `rollup` counts statements. `get_provider()` maps `mock`→`MockProvider(False)`, `mock-lag`→`MockProvider(True)`, `live`→`LiveProvider()` (Task 3; a placeholder raising `NotImplementedError` is acceptable until then).
- [ ] **Step 5: Run tests, verify they pass.**
- [ ] **Step 6: Commit** — `git commit -m "feat(backend): mock status/metrics provider + GRAPH_DATA_MODE"`

---

### Task 3: Live provider (best-effort Confluent REST via httpx)

**Files:**
- Create: `apps/backend/data_products/confluent_client.py`
- Modify: `apps/backend/data_products/providers.py` (`LiveProvider`)
- Test: `apps/backend/tests/test_data_products_live.py`

**Interfaces:**
- Produces: `confluent_client.ConfluentClient` with `list_statement_phases() -> dict[str, str]` (statement_name → phase) and `get_statement_metrics(statement_name) -> dict` (raw metric fields, any may be absent); `providers.LiveProvider` implementing the `StatusProvider` protocol by mapping client output onto `NodeStatus`/`NodeMetrics`.
- Consumes: `settings` live fields (Task 2), `descriptor.Graph` (Task 1), `models` (Tasks 1–2).

- [ ] **Step 1: Write the failing test** in `test_data_products_live.py` (httpx mocked at the client boundary — no network):

```python
def test_live_status_maps_phases():
    client = _fake_client(phases={
        "models-c360-sources-pipeline-src-customers": "RUNNING",
        "models-c360-facts-pipeline-fct-c360-profile": "FAILED",
    })
    status = providers.LiveProvider(client=client).get_status(descriptor.get_graph("c360"))
    byid = {s.node_id: s.phase for s in status.statements}
    assert byid["src_customers"] == "RUNNING" and byid["fct_c360_profile"] == "FAILED"

def test_live_unknown_phase_becomes_UNKNOWN():
    client = _fake_client(phases={})  # statement not reported
    status = providers.LiveProvider(client=client).get_status(descriptor.get_graph("c360"))
    assert all(s.phase in {"UNKNOWN"} or s.phase for s in status.statements)
    assert any(s.phase == "UNKNOWN" for s in status.statements)

def test_live_missing_metric_is_none():
    client = _fake_client(metrics={"pending_records": 5})  # no rate fields
    m = providers.LiveProvider(client=client).get_metrics(descriptor.get_graph("c360"), "src_customers")
    assert m.pending_records == 5 and m.messages_in_per_sec is None

def test_live_unreachable_does_not_raise():
    client = _raising_client(httpx.ConnectError("boom"))
    status = providers.LiveProvider(client=client).get_status(descriptor.get_graph("c360"))
    assert status.rollup.total == 8
    assert all(s.phase == "UNKNOWN" for s in status.statements)
```

- [ ] **Step 2: Run tests, verify they fail.**
- [ ] **Step 3: Implement `confluent_client.py`** — httpx calls to the Flink Statements API (phase from `status.phase`) and the Cloud Metrics API (pending-records / records-in-out), authed from `settings`. **Verify exact Flink Statements API path/version and Metrics API descriptor names against current Confluent docs**; where only counters are exposed, derive per-second rates from deltas between two reads (or leave the rate `None` if a single read). Wrap each remote call so network/HTTP errors return empty/omitted data, never propagate.
- [ ] **Step 4: Implement `LiveProvider`** — for each statement node: phase from the client map (default `"UNKNOWN"`), `lagging = pending_records > threshold` when known; `get_metrics` maps raw fields, absent → `None`. On any client-level failure, all phases become `UNKNOWN` and the response is still well-formed.
- [ ] **Step 5: Run tests, verify they pass.**
- [ ] **Step 6: Commit** — `git commit -m "feat(backend): best-effort live Confluent status/metrics provider"`

---

### Task 4: Data-product service, router, and app wiring

**Files:**
- Create: `apps/backend/data_products/service.py`
- Create: `apps/backend/api/data_product_resource.py`
- Modify: `apps/backend/main.py` (include router)
- Modify: `apps/backend/.env.example` (document new env var names)
- Test: `apps/backend/tests/test_data_products_api.py`

**Interfaces:**
- Produces (`service.py`): `list_products() -> list[DataProductSummary]`; `get_graph(id) -> Graph | None`; `get_status(id) -> StatusResponse | None`; `get_node_metrics(id, node_id) -> NodeMetrics | None` (None when product or node unknown). Service calls `descriptor` + `providers.get_provider()`.
- Produces (router, mounted at `/api/v1`): `GET /data-products`, `GET /data-products/{id}/graph`, `GET /data-products/{id}/status`, `GET /data-products/{id}/nodes/{node_id}/metrics` — 404 on unknown id/node.
- Consumes: Tasks 1–3.

- [ ] **Step 1: Write the failing test** in `test_data_products_api.py` (reuse the `TestClient` + env-patch + `_reset_modules` pattern from `tests/test_cors.py`, with `SINK=kafka`, `GRAPH_DATA_MODE=mock`, Kafka producers patched):

```python
def test_list_products(client):
    r = client.get("/api/v1/data-products")
    assert r.status_code == 200 and r.json()[0]["id"] == "c360"

def test_graph_endpoint_shape(client):
    r = client.get("/api/v1/data-products/c360/graph")
    body = r.json()
    assert r.status_code == 200 and len(body["nodes"]) == 11 and len(body["edges"]) == 11

def test_status_rollup(client):
    r = client.get("/api/v1/data-products/c360/status")
    assert r.status_code == 200 and r.json()["rollup"] == {"running": 8, "total": 8, "lagging": 0}

def test_node_metrics(client):
    r = client.get("/api/v1/data-products/c360/nodes/src_customers/metrics")
    assert r.status_code == 200 and r.json()["phase"] == "RUNNING"

def test_unknown_product_graph_404(client):
    assert client.get("/api/v1/data-products/nope/graph").status_code == 404

def test_unknown_node_metrics_404(client):
    assert client.get("/api/v1/data-products/c360/nodes/nope/metrics").status_code == 404
```

- [ ] **Step 2: Run tests, verify they fail.**
- [ ] **Step 3: Implement `service.py`** (orchestration above) and **`data_product_resource.py`** (`APIRouter(prefix="/data-products", tags=["data-products"])`, `response_model` per endpoint, `HTTPException(404, ...)` on None), then **include it in `main.py`** alongside the other routers (`app.include_router(data_product_router, prefix="/api/v1")`).
- [ ] **Step 4: Document env vars in `.env.example`** — `GRAPH_DATA_MODE` and the live-mode `CONFLUENT_*/FLINK_*` names (names + a one-line comment, no values).
- [ ] **Step 5: Run tests, verify they pass**; then run the whole backend suite `uv run pytest -q` to confirm no regressions (module-reset interactions).
- [ ] **Step 6: Commit** — `git commit -m "feat(backend): data-product graph/status/metrics API"`

---

### Task 5: Frontend deps, types, and API client

**Files:**
- Modify: `apps/frontend/package.json` (add `@xyflow/react`, `@dagrejs/dagre`)
- Create: `apps/frontend/src/types/dataProduct.ts`
- Create: `apps/frontend/src/api/dataProducts.ts`
- Test: `apps/frontend/src/api/dataProducts.test.ts`

**Interfaces:**
- Produces (`types/dataProduct.ts`): `DataProduct`, `GraphNode`, `GraphEdge`, `Graph`, `NodeStatus`, `StatusRollup`, `StatusResponse`, `NodeMetrics` (TS mirrors of the backend models; `phase` as a string union).
- Produces (`api/dataProducts.ts`): `listDataProducts(): Promise<DataProduct[]>`; `getGraph(id): Promise<Graph>`; `getStatus(id): Promise<StatusResponse>`; `getNodeMetrics(id, nodeId): Promise<NodeMetrics>` — all via `request<T>()`, paths without the `/api/v1` prefix.

- [ ] **Step 1: Add deps** — `cd apps/frontend && npm install @xyflow/react @dagrejs/dagre`.
- [ ] **Step 2: Write the failing test** in `dataProducts.test.ts` (mirror `api/accounts.test.ts`: spy on `client.request`):

```ts
it('listDataProducts GETs /data-products', async () => {
  const spy = vi.spyOn(client, 'request').mockResolvedValue([])
  await listDataProducts()
  expect(spy).toHaveBeenCalledWith('/data-products')
})
it('getGraph GETs /data-products/:id/graph', async () => { /* expect '/data-products/c360/graph' */ })
it('getStatus GETs /data-products/:id/status', async () => { /* expect '/data-products/c360/status' */ })
it('getNodeMetrics GETs /data-products/:id/nodes/:nodeId/metrics', async () => {
  const spy = vi.spyOn(client, 'request').mockResolvedValue({})
  await getNodeMetrics('c360', 'src_customers')
  expect(spy).toHaveBeenCalledWith('/data-products/c360/nodes/src_customers/metrics')
})
```

- [ ] **Step 3: Run tests, verify they fail** — `npm test -- dataProducts`.
- [ ] **Step 4: Implement `types/dataProduct.ts` and `api/dataProducts.ts`.**
- [ ] **Step 5: Run tests, verify they pass.**
- [ ] **Step 6: Commit** — `git commit -m "feat(frontend): data-product types + API client"`

---

### Task 6: Layered graph layout helper

**Files:**
- Create: `apps/frontend/src/lib/graphLayout.ts`
- Test: `apps/frontend/src/lib/graphLayout.test.ts`

**Interfaces:**
- Produces: `layoutGraph(nodes: GraphNode[], edges: GraphEdge[]): { id: string; position: { x: number; y: number } }[]` — assigns coordinates with `@dagrejs/dagre`, left→right (`rankdir: "LR"`).
- Consumes: `GraphNode`/`GraphEdge` (Task 5).

- [ ] **Step 1: Write the failing test** in `graphLayout.test.ts`:

```ts
it('places a source upstream (smaller x) of the statement it feeds', () => {
  const nodes = [
    { id: 'topic_customers', type: 'source', label: '' },
    { id: 'src_customers', type: 'statement', label: '' },
  ] as GraphNode[]
  const edges = [{ source: 'topic_customers', target: 'src_customers' }]
  const pos = Object.fromEntries(layoutGraph(nodes, edges).map((n) => [n.id, n.position]))
  expect(pos.topic_customers.x).toBeLessThan(pos.src_customers.x)
})
it('returns a position for every node', () => { /* length === nodes.length */ })
```

- [ ] **Step 2: Run tests, verify they fail.**
- [ ] **Step 3: Implement `layoutGraph`** with dagre (`rankdir: LR`, fixed node width/height, sensible rank/node separation).
- [ ] **Step 4: Run tests, verify they pass.**
- [ ] **Step 5: Commit** — `git commit -m "feat(frontend): dagre layered layout helper"`

---

### Task 7: Data Products list page, nav entry, routes

**Files:**
- Create: `apps/frontend/src/pages/DataProductsPage.tsx`
- Modify: `apps/frontend/src/components/Sidebar.tsx`
- Modify: `apps/frontend/src/components/Sidebar.test.tsx`
- Modify: `apps/frontend/src/App.tsx`
- Test: `apps/frontend/src/pages/DataProductsPage.test.tsx`

**Interfaces:**
- Consumes: `listDataProducts` (Task 5).
- Produces: route `/data-products` (list) and `/data-products/:id` (graph page, Task 8) registered in `App.tsx`; a `Data Products` nav link.

- [ ] **Step 1: Write the failing tests.** `DataProductsPage.test.tsx` (mock `../api/dataProducts`, render inside `MemoryRouter`):

```tsx
it('lists data products with their statement counts', async () => {
  vi.spyOn(api, 'listDataProducts').mockResolvedValue([
    { id: 'c360', name: 'Customer 360', description: 'x', statement_count: 8 },
  ])
  render(<MemoryRouter><DataProductsPage /></MemoryRouter>)
  expect(await screen.findByText('Customer 360')).toBeInTheDocument()
  expect(screen.getByText('8')).toBeInTheDocument()
})
```

Extend `Sidebar.test.tsx` with: renders a `Data Products` link pointing to `/data-products`.

- [ ] **Step 2: Run tests, verify they fail.**
- [ ] **Step 3: Implement `DataProductsPage`** (table styled like `AccountsPage`: load via `useEffect`, loading/error states, each row links to `/data-products/:id`); **add the nav entry** to `Sidebar.tsx` (lucide `Workflow` icon, label `Data Products`, to `/data-products`); **register both routes** in `App.tsx` (the `:id` route points to `StatementGraphPage`, created in Task 8 — import and stub a placeholder component in this task if needed to compile, replaced in Task 8).
- [ ] **Step 4: Run tests, verify they pass.**
- [ ] **Step 5: Commit** — `git commit -m "feat(frontend): data products list page + nav"`

---

### Task 8: Statement graph page (DAG, health bar, drill-down panel, polling)

**Files:**
- Create: `apps/frontend/src/pages/StatementGraphPage.tsx`
- Create: `apps/frontend/src/components/StatementNode.tsx` (custom React Flow node)
- Modify: `apps/frontend/src/App.tsx` (point `/data-products/:id` at the real page)
- Modify: `apps/frontend/src/styles/index.css` (graph/panel/health styles)
- Modify: `apps/frontend/src/test/setup.ts` (add a `ResizeObserver` polyfill for React Flow under jsdom)
- Test: `apps/frontend/src/pages/StatementGraphPage.test.tsx`

**Interfaces:**
- Consumes: `getGraph`, `getStatus`, `getNodeMetrics` (Task 5), `layoutGraph` (Task 6).

- [ ] **Step 1: Write the failing tests** (mock `../api/dataProducts`; mock `@xyflow/react` to a lightweight stub that renders each node via a passed renderer so node/status assertions work in jsdom, OR assert on the health bar + panel which render outside the canvas):

```tsx
it('shows the health rollup from status', async () => {
  mockGraph(); mockStatus({ running: 7, total: 8, lagging: 1 })
  renderGraphPage()
  expect(await screen.findByText(/7 \/ 8 RUNNING/)).toBeInTheDocument()
  expect(screen.getByText(/1 with lag/)).toBeInTheDocument()
})
it('opens the metrics panel when a node is selected', async () => {
  mockGraph(); mockStatus(); vi.spyOn(api, 'getNodeMetrics').mockResolvedValue(sampleMetrics)
  renderGraphPage()
  await selectNode('src_customers')
  expect(await screen.findByText(/Pending records/)).toBeInTheDocument()
})
it('renders a missing metric as an em dash', async () => {
  vi.spyOn(api, 'getNodeMetrics').mockResolvedValue({ ...sampleMetrics, messages_in_per_sec: null })
  renderGraphPage(); await selectNode('src_customers')
  expect(await screen.findByText('—')).toBeInTheDocument()
})
it('keeps the last good status when a poll fails', async () => {
  const spy = vi.spyOn(api, 'getStatus')
    .mockResolvedValueOnce(goodStatus)
    .mockRejectedValueOnce(new Error('blip'))
  renderGraphPage()
  expect(await screen.findByText(/8 \/ 8 RUNNING/)).toBeInTheDocument()
  await advancePollInterval()
  expect(screen.getByText(/8 \/ 8 RUNNING/)).toBeInTheDocument() // unchanged, not blanked
})
```

- [ ] **Step 2: Run tests, verify they fail.**
- [ ] **Step 3: Add the `ResizeObserver` polyfill** to `src/test/setup.ts`.
- [ ] **Step 4: Implement `StatementNode.tsx`** — custom node showing label + status dot; class by `type` (`source` neutral) and by `phase`/`lagging` for statements (green/amber/red), `is_product` gets an accent.
- [ ] **Step 5: Implement `StatementGraphPage.tsx`** — read `:id`; fetch `getGraph` once, position with `layoutGraph`, render React Flow with `StatementNode`; poll `getStatus` every 5s (store last good status; a rejected poll keeps prior state and sets a subtle "stale" flag, never clears nodes); health bar from rollup + "refreshed Ns ago"; on node select, fetch `getNodeMetrics` and show the drill-down panel (status, uptime, in/out rate, pending records, compute pool, SQL; `null` → "—"); refresh open-node metrics on the same interval.
- [ ] **Step 6: Point the route** `/data-products/:id` at `StatementGraphPage` in `App.tsx` (replace the Task 7 stub); **append styles** to `index.css` (lanes/health pills/status dots/metric rows/dark SQL box per the approved mockup).
- [ ] **Step 7: Run tests, verify they pass**; run the full frontend suite `npm test` and `npm run lint`.
- [ ] **Step 8: Commit** — `git commit -m "feat(frontend): c360 statement graph page with drill-down metrics"`

---

## Notes for the implementer

- Build backend Tasks 1–4 first (they have no frontend dependency), then frontend 5–8. Task 7 temporarily stubs the Task 8 page so routing compiles; Task 8 replaces it.
- The only place real Confluent API shapes are unverified is Task 3 — confirm Flink Statements API path/version and Metrics API descriptor names against current docs before implementing, and keep every remote call failure-tolerant (the demo runs in `mock` by default).
- Do not commit any `.env` with secrets; `.env.example` carries names only.
