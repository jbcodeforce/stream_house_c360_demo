# Flink Pipeline Cost Banner Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Show a data product's month-to-date Flink pipeline cost (`$X this month · N CFU`) at the top of StatementGraphPage, computed from the Confluent Cost API via a statement→compute-pool mapping.

**Architecture:** Each statement node carries a `compute_pool_id` (sourced from dbt, emitted by the descriptor builder). A new best-effort Cost API client sums this-month FLINK line items for the distinct pools a product owns; a provider method (`get_cost`, live + mock) returns a `CostResponse`; a new route serves it; the frontend renders a slow-polled banner that hides when cost is unavailable. A second compute pool (crm) is added in Terraform, and the existing live metrics path is made pool-aware so crm nodes don't regress.

**Tech Stack:** Python/FastAPI + httpx + pydantic (backend), Terraform (Confluent provider), dbt (confluent adapter), React + TypeScript + Vite + @xyflow/react (frontend).

**Spec:** `docs/superpowers/specs/2026-10-30-flink-pipeline-cost-design.md`

## Global Constraints

- Every Confluent client method is **best-effort: catches `httpx.HTTPError`/`ValueError`, never raises**, returns empty (`{}`/`[]`/`None`). Providers wrap client calls in `try/except` too (defense in depth).
- Cost is additive-only to the UI: when unavailable, the banner is hidden — **never render `$0`, never break the graph**.
- Backend tests run with `cd apps/backend && uv run pytest`; frontend with `cd apps/frontend && npm test`.
- Descriptor JSONs are **generated** — regenerate via `scripts/build_descriptors.py`, never hand-edit `c360.json`/`crm.json`.
- Secrets never written literally to files/code; config reads from env (`config.Settings`).
- Cost API: `GET https://api.confluent.cloud/billing/v1/costs`, HTTP Basic `auth=(CONFLUENT_CLOUD_API_KEY, CONFLUENT_CLOUD_API_SECRET)`, required `start_date`/`end_date` (ISO date), `page_size`≤10000, `page_token` pagination, filter `product == "FLINK"`, sum field `amount`, pool id at `resource.id`.
- Commit style: Conventional Commits. Do not `git push`. Only commit as the plan's commit steps direct.

## Review Focus

- **Cost API 403 (principal lacks billing role) / network failure** → `get_flink_pool_costs` returns `{}`, provider returns `CostResponse(available=False)`, banner hidden. (Task 5 + Task 6 tests.)
- **Line items for a pool the product does not own** (shared/other-product pool) → excluded from the sum; only `pool_ids` collected from non-`external` statement nodes count. (Task 6 test.)
- **Pagination**: a response carrying `page_token`/next page → all pages followed before summing. (Task 5 test.)
- **Node with no `compute_pool_id`** → builder falls back to default pool; `external` nodes never contribute a pool. (Task 4 + Task 6 tests.)
- **Cost lag (current day empty)** → partial-month sum still returned with `available=True` when any matching line item exists; `available=False` only when none match. (Task 6 test.)

---

### Task 1: Terraform — crm compute pool + billing role

**Files:**
- Modify: `IaC/ccloud/flink.tf` (add second pool + role binding)
- Modify: `IaC/ccloud/variables.tf` (add `flink_crm_max_cfu`)
- Modify: `IaC/ccloud/outputs.tf` (add `flink_crm_compute_pool_id`)

**Interfaces:**
- Produces: TF output `flink_crm_compute_pool_id` (the crm pool id), consumed by Task 2.

- [ ] **Step 1: Add the crm compute pool** in `flink.tf`, mirroring `confluent_flink_compute_pool.pool`: `resource "confluent_flink_compute_pool" "crm_pool"` with `display_name = "${var.prefix}-crm-flink-pool"`, same `cloud`/`region`/`environment` block, `max_cfu = var.flink_crm_max_cfu`, same `depends_on`.
- [ ] **Step 2: Add the variable** `flink_crm_max_cfu` in `variables.tf` next to `flink_max_cfu` (same type/default, e.g. `50`).
- [ ] **Step 3: Add the output** `flink_crm_compute_pool_id` in `outputs.tf` next to `flink_compute_pool_id`, value `confluent_flink_compute_pool.crm_pool.id`.
- [ ] **Step 4: Add the billing role binding** in `flink.tf`: a `confluent_role_binding` granting the principal behind `CONFLUENT_CLOUD_API_KEY` an org-level billing/cost read role, `crn_pattern` = the organization CRN. Confirm the exact role name against the Confluent provider docs / `confluent_role` data (candidates: a billing-read role if the org exposes one, else `OrganizationAdmin`); record the chosen role in a comment citing why.
- [ ] **Step 5: Validate**

Run: `cd IaC/ccloud && terraform fmt -check && terraform validate`
Expected: formatted + "Success! The configuration is valid." (Do **not** run `terraform apply` — the owner applies.)

- [ ] **Step 6: Commit**

```bash
git add IaC/ccloud/flink.tf IaC/ccloud/variables.tf IaC/ccloud/outputs.tf
git commit -m "feat(infra): add crm flink compute pool and cost-api billing role"
```

---

### Task 2: Env bridge + backend config for the crm pool id

**Files:**
- Modify: `scripts/set_env_from_tf.sh:56` (export) and `:131` (env-dict)
- Modify: `apps/backend/.env.example` (document the var)
- Modify: `apps/backend/config.py` (`Settings` field + `summary()`)
- Test: `apps/backend/tests/test_config.py`

**Interfaces:**
- Consumes: TF output `flink_crm_compute_pool_id` (Task 1).
- Produces: `config.settings.FLINK_CRM_COMPUTE_POOL_ID: str | None`, consumed by Tasks 4, 8 (fallback-mapping / verification).

- [ ] **Step 1: Write the failing test** in `test_config.py` (mirror existing field tests): assert `Settings()` has `FLINK_CRM_COMPUTE_POOL_ID` (default `None`/empty per existing Flink-var convention) and that `summary()` includes a `FLINK_CRM_COMPUTE_POOL_ID` entry.
- [ ] **Step 2: Run test to verify it fails**

Run: `cd apps/backend && uv run pytest tests/test_config.py -v -k crm`
Expected: FAIL (attribute/key missing)

- [ ] **Step 3: Add the setting** `FLINK_CRM_COMPUTE_POOL_ID` to `Settings` in `config.py` next to `FLINK_COMPUTE_POOL_ID`, matching its type/default, and add it to `summary()` the same way the existing Flink coords are listed.
- [ ] **Step 4: Wire the env bridge**: in `scripts/set_env_from_tf.sh` add `export FLINK_CRM_COMPUTE_POOL_ID=$(terraform -chdir="${CCLOUD_DIR}" output -raw flink_crm_compute_pool_id ...)` next to line 56, and add the matching key to the env-dict near line 131. Document it in `apps/backend/.env.example` in the live section.
- [ ] **Step 5: Run test to verify it passes**

Run: `cd apps/backend && uv run pytest tests/test_config.py -v -k crm`
Expected: PASS

- [ ] **Step 6: Commit**

```bash
git add apps/backend/config.py apps/backend/.env.example scripts/set_env_from_tf.sh apps/backend/tests/test_config.py
git commit -m "feat(config): wire FLINK_CRM_COMPUTE_POOL_ID from terraform"
```

---

### Task 3: dbt — route crm models to the crm pool

**Files:**
- Modify: `pipelines/dbt_project.yml` (folder-level `compute_pool_id` for `crm`)

**Interfaces:**
- Produces: crm models' `config.compute_pool_id` in `pipelines/target/manifest.json`, consumed by Task 4.

- [ ] **Step 1: Add folder-level config** under `models:` in `pipelines/dbt_project.yml`: for the `streamhouse: crm:` path set `+compute_pool_id: "{{ env_var('FLINK_CRM_COMPUTE_POOL_ID') }}"`. Leave c360 unset (inherits the profile default `FLINK_COMPUTE_POOL_ID`). Folder-level `dbt_project.yml` config is chosen over per-model `config()` because it reliably serializes into each node's `config` in the manifest.
- [ ] **Step 2: Verify it surfaces in the manifest (the build-time gate from the spec)**

Run: `cd pipelines && FLINK_CRM_COMPUTE_POOL_ID=lfcp-test uv run dbt parse` then inspect `target/manifest.json` for a crm model, e.g. `python -c "import json;m=json.load(open('target/manifest.json'));print([n['config'].get('compute_pool_id') for k,n in m['nodes'].items() if k.startswith('model.streamhouse.fct_crm')])"`
Expected: each crm model prints `lfcp-test` (not `None`).
If `None`: fall back to reading `config.meta.compute_pool_id` (set via `+meta:`) — update Task 4's read accordingly and note it here.

- [ ] **Step 3: Commit**

```bash
git add pipelines/dbt_project.yml
git commit -m "feat(dbt): route crm models to the crm compute pool"
```

---

### Task 4: Descriptor builder — compute_pool_id, external marker, cross-product nodes

**Files:**
- Modify: `apps/backend/data_products/models.py:8-21` (`GraphNode`)
- Modify: `apps/backend/data_products/descriptor_builder.py:102-134` (node emission + edge loop)
- Modify: `scripts/build_descriptors.py` (pass the default pool + per-model pool lookup into `build_descriptors`)
- Regenerate: `apps/backend/data_products/descriptors/c360.json`, `crm.json`
- Test: `apps/backend/tests/test_descriptor_builder.py`

**Interfaces:**
- Consumes: manifest `config.compute_pool_id` (Task 3); `FLINK_COMPUTE_POOL_ID` default.
- Produces: `GraphNode.compute_pool_id: str | None`, `GraphNode.external: bool`; cross-product upstream nodes emitted with `external=True`, `is_product=False`, and the owning product's pool. Consumed by Task 6 (cost) and Task 8 (metrics).

- [ ] **Step 1: Write failing tests** in `test_descriptor_builder.py`:
  - `test_statement_node_carries_compute_pool_id`: a manifest model with `config.compute_pool_id = "lfcp-crm"` produces a statement node with `compute_pool_id == "lfcp-crm"`.
  - `test_statement_node_falls_back_to_default_pool`: a model with no `config.compute_pool_id` produces `compute_pool_id == <default_pool arg>`.
  - `test_cross_product_dependency_emits_external_node`: a crm model depending on a c360 model yields (a) a node for the c360 model with `external is True`, `is_product is False`, `compute_pool_id == <c360 pool>`, and (b) an edge into the crm model.
  - `test_source_node_has_no_pool_or_external`: source nodes keep `{id,type,label,topic}` (no `compute_pool_id`, `external` omitted/False).
- [ ] **Step 2: Run tests to verify they fail**

Run: `cd apps/backend && uv run pytest tests/test_descriptor_builder.py -v -k "pool or external"`
Expected: FAIL

- [ ] **Step 3: Extend the model** in `models.py`: add `compute_pool_id: str | None = None` and `external: bool = False` to `GraphNode`. Mirror `is_product` handling in `descriptor_builder._serialize_node` so a `False` `external` is dropped from the serialized dict.
- [ ] **Step 4: Change `build_descriptors` signature** to `build_descriptors(manifest, statement_name_fn, *, pool_for: Callable[[dict], str | None], default_pool: str | None)` where `pool_for(model)` returns `model["config"].get("compute_pool_id")`. In the statement-node emission (`:105-115`) set `compute_pool_id = pool_for(model) or default_pool`. In the edge loop (`:117-134`) add an `elif dep_uid in models_by_uid` branch (the currently-dropped cross-product case): create a node for the dependency keyed by its model name with `type="statement"`, `external=True`, `is_product=False`, `compute_pool_id = pool_for(dep) or default_pool`, `table_type` from the dep's `config.materialized`, and append the edge. De-dup external nodes like `source_nodes`.
- [ ] **Step 5: Update the CLI** `scripts/build_descriptors.py` to read each model's `config.compute_pool_id` and pass `pool_for` + `default_pool=os.environ.get("FLINK_COMPUTE_POOL_ID")` into `build_descriptors`.
- [ ] **Step 6: Run tests to verify they pass**

Run: `cd apps/backend && uv run pytest tests/test_descriptor_builder.py -v`
Expected: PASS

- [ ] **Step 7: Regenerate descriptors and eyeball the diff**

Run: `cd pipelines && uv run python ../scripts/build_descriptors.py` then `git diff apps/backend/data_products/descriptors/`
Expected: statement nodes gain `compute_pool_id`; `crm.json`'s `fct_c360_profile` becomes `external: true` (was `is_product: true`), crm edges preserved; crm statement nodes point at the crm pool, c360 at the default. (Requires `FLINK_COMPUTE_POOL_ID`/`FLINK_CRM_COMPUTE_POOL_ID` in env for real ids; test ids acceptable for the structural check.)

- [ ] **Step 8: Commit**

```bash
git add apps/backend/data_products/models.py apps/backend/data_products/descriptor_builder.py scripts/build_descriptors.py apps/backend/data_products/descriptors/ apps/backend/tests/test_descriptor_builder.py
git commit -m "feat(descriptors): emit compute_pool_id and external cross-product nodes"
```

---

### Task 5: Cost API client methods

**Files:**
- Modify: `apps/backend/data_products/confluent_client.py` (new methods + protocol)
- Test: `apps/backend/tests/test_data_products_live.py` (httpx mocked)

**Interfaces:**
- Produces on `ConfluentClient` and `ConfluentClientProtocol`:
  - `get_flink_pool_costs(self, start_date: date, end_date: date) -> dict[str, float]` — `{pool_id: summed_amount}` over all `product == "FLINK"` line items in the window, following `page_token` pagination; best-effort `{}`.
  - `get_compute_pool_cfu(self, pool_id: str) -> float | None` — current CFUs for one pool (`compute_pool_utilization/current_cfus`, EQ on `resource.compute_pool.id`); best-effort `None`.

- [ ] **Step 1: Write failing tests** (mock `httpx.get`, as existing live tests do):
  - `test_get_flink_pool_costs_sums_by_pool`: response with FLINK line items for two pools + one non-FLINK item → returns `{pool_a: sum, pool_b: sum}`, non-FLINK excluded.
  - `test_get_flink_pool_costs_follows_pagination`: first page returns a `page_token`/`next` cursor, second page returns the rest → both pages summed; assert the second request carried the token.
  - `test_get_flink_pool_costs_best_effort`: `httpx.get` raises `httpx.HTTPError` → returns `{}` (no raise). Also assert a missing-credentials short-circuit returns `{}`.
  - `test_get_compute_pool_cfu_returns_latest`: mocked query → returns the latest value; failure → `None`.
- [ ] **Step 2: Run tests to verify they fail**

Run: `cd apps/backend && uv run pytest tests/test_data_products_live.py -v -k "cost or cfu"`
Expected: FAIL

- [ ] **Step 3: Implement the methods** in `confluent_client.py`. `get_flink_pool_costs`: GET `https://api.confluent.cloud/billing/v1/costs` with params `start_date`/`end_date` (ISO date strings), `page_size=10000`, loop on the response's next-page token; `auth=(CONFLUENT_CLOUD_API_KEY, CONFLUENT_CLOUD_API_SECRET)`; short-circuit `{}` when those creds are missing; for each `data` item where `product == "FLINK"`, add `float(item.get("amount") or 0)` to `totals[item["resource"]["id"]]`; wrap in the module's best-effort `try/except (httpx.HTTPError, ValueError)`. `get_compute_pool_cfu`: reuse `_query_latest_value` with `[("resource.compute_pool.id", pool_id)]`. Add both to `ConfluentClientProtocol`.
- [ ] **Step 4: Run tests to verify they pass**

Run: `cd apps/backend && uv run pytest tests/test_data_products_live.py -v -k "cost or cfu"`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add apps/backend/data_products/confluent_client.py apps/backend/tests/test_data_products_live.py
git commit -m "feat(cost): add Cost API client for flink pool costs and pool cfu"
```

---

### Task 6: CostResponse model + provider.get_cost (mock + live)

**Files:**
- Modify: `apps/backend/data_products/models.py` (`CostResponse`)
- Modify: `apps/backend/data_products/providers.py` (`StatusProvider` protocol + both providers)
- Modify: `apps/backend/data_products/mock_data.py` (synthetic cost rate)
- Test: `apps/backend/tests/test_data_products_mock.py`, `apps/backend/tests/test_data_products_live.py`

**Interfaces:**
- Consumes: `GraphNode.compute_pool_id`/`external` (Task 4); `get_flink_pool_costs`/`get_compute_pool_cfu` (Task 5).
- Produces:
  - `CostResponse(BaseModel)`: `available: bool`, `amount: float = 0.0`, `currency: str = "USD"`, `cfu: float | None = None`, `period_start: date | None = None`, `period_end: date | None = None`, `pool_ids: list[str] = []`.
  - `StatusProvider.get_cost(self, graph: Graph) -> CostResponse` on both providers.
  - Helper `_owned_pool_ids(graph) -> list[str]` = sorted distinct `compute_pool_id` of nodes where `type == "statement" and not external and compute_pool_id`.

- [ ] **Step 1: Write failing tests**:
  - mock: `test_mock_get_cost_available`: `MockProvider.get_cost(graph)` returns `available is True`, `amount > 0`, `cfu` = sum of fixture CFUs over the graph's non-external statement nodes, `pool_ids` = owned pools.
  - live `test_live_get_cost_sums_owned_pools_only`: fake client returns costs for an owned pool **and** an unowned pool → `amount` equals only the owned pool's cost; `pool_ids` excludes the external node's pool.
  - live `test_live_get_cost_unavailable_when_no_matching_costs`: client returns `{}` (403/lag) → `available is False`, graph-cost hidden semantics.
  - live `test_live_get_cost_partial_month_available`: client returns a matching item → `available is True` even if only part of the month.
- [ ] **Step 2: Run tests to verify they fail**

Run: `cd apps/backend && uv run pytest tests/test_data_products_mock.py tests/test_data_products_live.py -v -k cost`
Expected: FAIL

- [ ] **Step 3: Implement**. Add `CostResponse` to `models.py`. Add `get_cost` to the `StatusProvider` protocol. `MockProvider.get_cost`: `cfu = sum(STATEMENT_FIXTURES[n.id][3] ...)` over non-external statement nodes present in fixtures; `amount = round(cfu * mock_data.MOCK_CFU_DOLLARS_PER_MONTH, 2)`; `available=True`; `pool_ids=_owned_pool_ids(graph)`; period = month-to-date. `LiveProvider.get_cost`: `pool_ids=_owned_pool_ids(graph)`; if empty → `CostResponse(available=False)`; compute `period_start`=first of current month UTC, `period_end`=today UTC; `costs = self._client.get_flink_pool_costs(...)` (wrapped best-effort); `amount = sum(costs[p] for p in pool_ids if p in costs)`; `available = any(p in costs for p in pool_ids)`; `cfu = sum of get_compute_pool_cfu(p) for p in pool_ids` (None-safe; `None` if none resolved). Add `MOCK_CFU_DOLLARS_PER_MONTH` to `mock_data.py`.
- [ ] **Step 4: Run tests to verify they pass**

Run: `cd apps/backend && uv run pytest tests/test_data_products_mock.py tests/test_data_products_live.py -v -k cost`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add apps/backend/data_products/models.py apps/backend/data_products/providers.py apps/backend/data_products/mock_data.py apps/backend/tests/test_data_products_mock.py apps/backend/tests/test_data_products_live.py
git commit -m "feat(cost): add CostResponse and provider get_cost (mock + live)"
```

---

### Task 7: Service + route for cost

**Files:**
- Modify: `apps/backend/data_products/service.py`
- Modify: `apps/backend/api/data_product_resource.py`
- Test: `apps/backend/tests/test_data_products_api.py`

**Interfaces:**
- Consumes: `providers.get_provider().get_cost(graph)` (Task 6).
- Produces: `service.get_cost(product_id: str) -> CostResponse | None`; `GET /data-products/{product_id}/cost` → `CostResponse` (404 when product unknown).

- [ ] **Step 1: Write failing tests** in `test_data_products_api.py`: `test_get_cost_route_returns_cost_response` (200 + `available`/`amount`/`cfu` keys, mock mode) and `test_get_cost_route_404_unknown_product`.
- [ ] **Step 2: Run tests to verify they fail**

Run: `cd apps/backend && uv run pytest tests/test_data_products_api.py -v -k cost`
Expected: FAIL

- [ ] **Step 3: Implement** `service.get_cost` (mirror `get_status`: load graph, `None` if missing, else `providers.get_provider().get_cost(graph)`) and the route (mirror `get_status`, `response_model=CostResponse`).
- [ ] **Step 4: Run tests to verify they pass**

Run: `cd apps/backend && uv run pytest tests/test_data_products_api.py -v -k cost`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add apps/backend/data_products/service.py apps/backend/api/data_product_resource.py apps/backend/tests/test_data_products_api.py
git commit -m "feat(cost): add GET /data-products/{id}/cost endpoint"
```

---

### Task 8: Make the live metrics path pool-aware

**Files:**
- Modify: `apps/backend/data_products/confluent_client.py` (metrics method signatures)
- Modify: `apps/backend/data_products/providers.py` (`LiveProvider` passes node pool)
- Test: `apps/backend/tests/test_data_products_live.py`

**Interfaces:**
- Consumes: `GraphNode.compute_pool_id` (Task 4).
- Produces: `get_statement_metrics(self, statement_name, compute_pool_id: str | None = None)`, `get_materialized_table_metrics(self, mt_uid, compute_pool_id: str | None = None)`, `_query_statement_metric_grouped(..., compute_pool_id: str)`; effective pool = `compute_pool_id or self._settings.FLINK_COMPUTE_POOL_ID`. Protocol updated to match (optional param, back-compatible).

- [ ] **Step 1: Write failing test** `test_statement_metrics_uses_node_compute_pool`: calling `get_statement_metrics("s", compute_pool_id="lfcp-crm")` issues Metrics API queries whose `resource.compute_pool.id` filter equals `lfcp-crm` (assert on the mocked request body), not the default pool.
- [ ] **Step 2: Run test to verify it fails**

Run: `cd apps/backend && uv run pytest tests/test_data_products_live.py -v -k pool_aware`
Expected: FAIL

- [ ] **Step 3: Thread the pool id** through `_query_statement_metric_grouped`, `get_statement_metrics`, `get_materialized_table_metrics` (default to `FLINK_COMPUTE_POOL_ID` when `None`). In `LiveProvider.get_metrics`, pass `node.compute_pool_id` to `_safe_statement_metrics`/`_safe_mt_metrics` (add the param to those wrappers). Update `ConfluentClientProtocol`.
- [ ] **Step 4: Run tests to verify they pass**

Run: `cd apps/backend && uv run pytest tests/test_data_products_live.py -v`
Expected: PASS (existing live tests still green — param is optional)

- [ ] **Step 5: Commit**

```bash
git add apps/backend/data_products/confluent_client.py apps/backend/data_products/providers.py apps/backend/tests/test_data_products_live.py
git commit -m "fix(metrics): scope live node metrics to the node's compute pool"
```

---

### Task 9: Frontend cost banner

**Files:**
- Modify: `apps/frontend/src/types/dataProduct.ts` (`Cost` type + `external` on `GraphNode`)
- Modify: `apps/frontend/src/api/dataProducts.ts` (`getCost`)
- Modify: `apps/frontend/src/pages/StatementGraphPage.tsx` (banner + slow poll)
- Test: `apps/frontend/src/api/dataProducts.test.ts`

**Interfaces:**
- Consumes: `GET /data-products/{id}/cost` → `Cost` (Task 7).
- Produces: `getCost(id: string): Promise<Cost>`; `Cost { available: boolean; amount: number; currency: string; cfu: number | null; period_start: string | null; period_end: string | null; pool_ids: string[] }`.

- [ ] **Step 1: Write failing test** in `dataProducts.test.ts` (mirror existing wrappers): `getCost('c360')` calls `GET /data-products/c360/cost` and returns the parsed `Cost`.
- [ ] **Step 2: Run test to verify it fails**

Run: `cd apps/frontend && npm test -- dataProducts`
Expected: FAIL

- [ ] **Step 3: Add the type + wrapper**: `Cost` interface in `types/dataProduct.ts` (and add `external?: boolean` to `GraphNode`); `getCost` in `api/dataProducts.ts` mirroring `getStatus`.
- [ ] **Step 4: Run test to verify it passes**

Run: `cd apps/frontend && npm test -- dataProducts`
Expected: PASS

- [ ] **Step 5: Render the banner** in `StatementGraphPage.tsx`: fetch cost on mount and on a `COST_POLL_MS = 300_000` interval (separate from the 5s status poll); store in state. In `.page-header`/`.health-bar`, when `cost?.available`, render `{formatCurrency(cost.amount)} this month{cost.cfu != null && ` · ${Math.round(cost.cfu)} CFU`}` using `formatCurrency` from `lib/format.ts`. When `available` is false or the fetch failed, render nothing (banner hidden). Guard the fetch in try/catch so a cost failure never affects the graph.
- [ ] **Step 6: Verify the build/typecheck and manual render**

Run: `cd apps/frontend && npm run build`
Expected: typecheck + build pass. Then manually (mock mode) confirm the banner shows `$… this month · N CFU` on StatementGraphPage and the graph is unaffected when the cost route is made to 404.

- [ ] **Step 7: Commit**

```bash
git add apps/frontend/src/types/dataProduct.ts apps/frontend/src/api/dataProducts.ts apps/frontend/src/pages/StatementGraphPage.tsx apps/frontend/src/api/dataProducts.test.ts
git commit -m "feat(ui): show pipeline cost banner on StatementGraphPage"
```

---

### Task 10: Live verification gate (spec risk resolution)

**Files:** none (verification + a note appended to the spec's "Live verification" section)

- [ ] **Step 1: Confirm Flink cost granularity** against the live org (owner-assisted, requires the Task 1 role applied): fetch one `GET /billing/v1/costs` day with Flink usage and confirm `product == "FLINK"` items carry the compute pool id (`lfcp-…`) in `resource.id`.
- [ ] **Step 2: Record the outcome** in the spec's "Live verification" section. If cost is only environment-granular, note the degradation (per-env attribution) and that mock mode carries the demo. No code change if per-pool confirmed.
- [ ] **Step 3: Commit** (docs only)

```bash
git add docs/superpowers/specs/2026-10-30-flink-pipeline-cost-design.md
git commit -m "docs(cost): record live Cost API granularity verification"
```
