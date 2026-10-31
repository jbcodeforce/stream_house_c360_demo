# Flink Pipeline Cost Banner — Design

**Date:** 2026-10-30
**Status:** Draft for approval

## Intent & why

The StatementGraphPage shows a data product's Flink pipeline as a graph of
statements. This feature adds a cost banner at the top of that page showing the
**month-to-date dollar cost of the whole pipeline**, computed from the Confluent
Cloud Cost API.

The point is a **demonstration**: prove that we can attribute real billed dollars
to a complete Flink pipeline by mapping each statement to its compute pool and
consolidating the pools' cost from the Cost API. It is a credibility/story
feature for prospects, not an internal spend-monitoring tool.

Billing is pool-level in Confluent (CFUs are billed per compute pool, not per
statement), so the design models cost at **compute-pool granularity** and
attributes it to a product via a statement→pool mapping sourced from dbt.

## Goals

- Display one aggregated cost figure per data product at the top of
  StatementGraphPage: **`$<amount> this month · <N> CFU`** (decision: banner
  format B).
- Attribute cost by collecting the distinct compute pool ids used by the
  product's own statements and summing this-month Flink cost for those pools.
- Model two compute pools: the existing pool for **c360**, a new pool for
  **crm**, so the demo shows two products with genuinely different pools and
  costs.
- Keep the mechanism general: cost consolidation is driven by per-statement
  `compute_pool_id` in the descriptor, not hardcoded per product.
- Work in **mock mode** with a synthetic cost (decision: yes), so the demo runs
  with no live cloud or billing access.

## Non-goals

- Per-statement dollar attribution. The Cost API bills per pool; splitting a
  pool across statements would be invented numbers. Out of scope.
- Historical cost charts, trends, or time-series. One current month-to-date
  number only.
- Cost alerting, budgets, or forecasting.
- Non-Flink cost (Kafka, Connect, SR, etc.). Only `product = FLINK` line items.

## Anti-goals (what makes this a failure even if it "works")

- A number that silently double-counts or misattributes cost across products
  (e.g. counting the shared `fct_c360_profile` upstream node toward crm). The
  shared node MUST be excluded from crm (decision: exclude).
- A banner that breaks or blanks the page when the Cost API is slow, returns no
  data for the current (lagging) day, or the principal lacks billing access.
  Cost is best-effort; its absence must degrade gracefully, never break the
  graph.
- Hand-edited descriptor JSON that the next `build_descriptors.py` run silently
  overwrites.

## Constraints & facts that shape the design

- **Descriptor JSONs are generated** from the dbt `manifest.json` by
  `scripts/build_descriptors.py` + `apps/backend/data_products/descriptor_builder.py`.
  Any new node field must be emitted by the builder, not hand-written into
  `c360.json` / `crm.json`.
- **dbt supports per-model `compute_pool_id`** (`_UNIVERSAL_CONFIG_KEYS` in the
  confluent adapter). Unset models inherit the profile default
  (`~/.dbt/profiles.yml` → `FLINK_COMPUTE_POOL_ID`).
- **Cost API** `GET /billing/v1/costs`:
  - Required query params `start_date`, `end_date` (dates); `page_size` (≤10000,
    default 5000) + `page_token` pagination. Granularity is **DAILY** — one line
    item per resource per day per line_type.
  - Line item fields used: `product` (filter `== FLINK`), `line_type`
    (`FLINK_NUM_CFUS`), `resource.id` (the compute pool id, `lfcp-…`),
    `resource.environment.id`, `amount` (post-discount dollars), `quantity`,
    `unit`, `start_date`/`end_date`.
  - Auth: Cloud API key via HTTP Basic — same `auth=(key, secret)` httpx pattern
    as the existing Metrics client.
  - **"This month"** = `start_date` = first day of current month (UTC),
    `end_date` = today (UTC). Sum `amount` across days for the target pool ids.
- **Cost data lags ~a day.** The banner changes slowly; it must poll on a slow
  interval, independent of the 5s status poll.
- **Open risk (confirm during build):** the doc's `resource.id` examples list
  Kafka/Connect/SR/ksqlDB but do not explicitly spell out the Flink compute-pool
  form. If live Flink cost turns out to be reported only at environment
  granularity (no per-pool `resource.id`), per-pool consolidation degrades to
  per-environment. Mitigation: the builder confirms against a real response;
  mock mode keeps the demo safe regardless. See "Live verification" below.

## Decisions already made (by the owner)

1. **Granularity:** compute-pool level (not per-statement approximation).
2. **Banner format B:** `$<amount> this month · <N> CFU`.
3. **Mock mode shows a synthetic cost:** yes.
4. **Shared upstream node excluded:** crm cost excludes `fct_c360_profile`
   (count only each product's own statements' pools).
5. **Two pools:** existing pool = c360; new pool = crm.
6. The owner runs `terraform apply`; this work authors the TF + wiring only.
7. **Billing role to Terraform:** add an org-level billing-read role binding for
   the Cost API principal in the TF (vs. mock-only or manual grant).

## The "N CFU" value in the banner

The banner's `· <N> CFU` is the **current total CFUs** across the product's
compute pool(s), not a billing figure. Source it the same way the existing
per-node metrics drill-down already gets `compute_pool_cfu` (Metrics API,
`io.confluent.flink/compute_pool_utilization/current_cfus`), summed over the
distinct pools. In mock mode it is a synthetic constant consistent with the
synthetic cost. (Builder's call: if wiring live current-CFU per pool proves
heavier than expected, N may be sourced from the already-fetched per-statement
metrics the page holds; the displayed semantics — "current CFU for this
pipeline" — stay the same.)

## Design — layer by layer

### 1. Terraform (`IaC/ccloud/`)

- Add `resource "confluent_flink_compute_pool" "crm_pool"` in `flink.tf`
  mirroring the existing `pool` (same `cloud`/`region`/`environment`,
  `display_name = "${var.prefix}-crm-flink-pool"`).
- Add `variable "flink_crm_max_cfu"` in `variables.tf` next to `flink_max_cfu`
  (same default, e.g. 50).
- Add `output "flink_crm_compute_pool_id"` in `outputs.tf` next to
  `flink_compute_pool_id`. Keep `flink_compute_pool_id` as the c360/default
  pool.
- The existing Flink service account and API key are shared and need no change
  for the second pool (API keys are env+region scoped).
- **Billing role (decided):** add a `confluent_role_binding` granting the
  principal used by the Cost API (`CONFLUENT_CLOUD_API_KEY`) an org-level
  billing/cost read role, bound at the organization CRN. Exact role name
  confirmed at build time (see "Billing role access" below). Enables the live
  real-dollars path on apply.

### 2. Env bridge

- `scripts/set_env_from_tf.sh`: add an export of `FLINK_CRM_COMPUTE_POOL_ID`
  from the new output (~line 56) and a matching entry in the env-dict that
  rewrites `apps/backend/.env` (~line 131).
- `apps/backend/.env.example`: document `FLINK_CRM_COMPUTE_POOL_ID` in the live
  section.
- `apps/backend/config.py`: add `FLINK_CRM_COMPUTE_POOL_ID` to `Settings`, and
  include it (masked/plain as appropriate) in `summary()`/`log_summary()`.

### 3. dbt (`pipelines/`)

- Set `compute_pool_id` for the crm models so they resolve to the crm pool. Two
  acceptable spellings (builder picks the cleaner for this repo):
  - per-model `config(compute_pool_id="{{ env_var('FLINK_CRM_COMPUTE_POOL_ID') }}")`
    in each `models/crm/**/*.sql`, or
  - a folder-level block under `models:` in `pipelines/dbt_project.yml` for the
    `crm` path.
- c360 models are left unset and inherit the profile default
  (`FLINK_COMPUTE_POOL_ID`).
- dbt renders the env var at parse time, so `manifest.json` carries the resolved
  pool id in each model's `config`.

### 4. Descriptor generation

- Add `compute_pool_id: str | None = None` and `external: bool = False` to
  `GraphNode` (`apps/backend/data_products/models.py`). `external` marks a node
  that is an upstream import from **another** product (see cross-product note).
- In `descriptor_builder.py`, emit `compute_pool_id` for statement nodes.
  **Reading the pool (resolves builder-check #3):** the per-model pool must land
  in the serialized manifest `config`. The current manifest has no
  `compute_pool_id` config key on any model, and it is unverified whether a
  per-model `config(compute_pool_id=…)` promotes into `node.config` for this
  adapter version. Therefore:
  - Prefer setting the crm pool via a **folder-level block in
    `dbt_project.yml`** (`models: streamhouse: crm: +compute_pool_id: …`), which
    reliably serializes into each node's `config` — more robust than per-model
    `config()` for the manifest read.
  - Builder reads `node.config["compute_pool_id"]`; if absent, falls back to
    `node.config.get("meta", {})` then to the profile/default pool
    (`FLINK_COMPUTE_POOL_ID`).
  - **Build-time verification gate:** after `dbt run`, confirm the crm nodes'
    manifest `config` actually carries the crm pool id before trusting the read.
    If neither config nor meta surfaces it, fall back to a builder-side
    product→pool env mapping (`crm → FLINK_CRM_COMPUTE_POOL_ID`, default
    otherwise) so attribution never silently collapses to one pool.
- **Cross-product upstream (resolves builder-check #1 — call made, see
  handover):** `crm.json` today contains `fct_c360_profile`, a node the current
  builder cannot emit — it only emits same-product or source dependencies, so a
  naive regen would drop that node and all crm edges. Extend
  `descriptor_builder.py` so a dependency on a model from **another** product is
  emitted as a node+edge marked `external: true`, `is_product: false`, with the
  **owning** product's pool as its `compute_pool_id`. This (a) makes `crm.json`
  reproducible by the generator, and (b) provides the exact owned/external
  discriminator the anti-double-count rule needs. Note: this changes the
  committed `crm.json` — `fct_c360_profile` becomes `external: true` instead of
  its current (misleading) `is_product: true`; the crm graph still renders it as
  upstream.
- Regenerate `c360.json` / `crm.json` via `scripts/build_descriptors.py`.
  **Do not hand-edit the JSONs.**

### 5. Backend cost path

- **Client** (`apps/backend/data_products/confluent_client.py`): add
  `get_org_costs(start_date, end_date) -> list[dict]` (or a focused
  `get_flink_pool_costs`) using `httpx.get` against
  `https://api.confluent.cloud/billing/v1/costs`, `auth=(CONFLUENT_CLOUD_API_KEY,
  CONFLUENT_CLOUD_API_SECRET)`, paginating on `page_token`. Best-effort: catch
  `httpx.HTTPError`/`ValueError`, return `[]`. Filter to `product == FLINK`.
- **Model** (`models.py`): `CostResponse { amount: float, currency: str = "USD",
  cfu: float | None, period_start: date, period_end: date, pool_ids: list[str],
  available: bool }`. `available=False` signals "no cost to show" so the frontend
  can hide rather than render `$0`.
- **Provider** (`providers.py`): add `get_cost(graph) -> CostResponse` to both:
  - `LiveProvider`: collect distinct `compute_pool_id` from the graph's
    statement nodes (excluding non-owned upstream — see below), call the client
    for this month, sum `amount` for those pool ids, attach summed current CFU.
  - `MockProvider`: return a deterministic synthetic `CostResponse` (plausible
    amount + CFU), `available=True`.
- **Service** (`data_products/service.py`): `get_cost(product_id)` loads the
  descriptor and delegates to the active provider.
- **Route** (`api/data_product_resource.py`): `GET /data-products/{id}/cost` →
  `CostResponse`.

**"Product's own statements" rule (anti-double-count):** when collecting pool
ids, include a statement node only if `type == "statement"` AND `external ==
False`. The `external` marker (emitted by the builder, see §4) is the single
discriminator — `fct_c360_profile` inside `crm.json` is `external: true` and so
its (c360) pool is never counted toward crm. Observable result MUST be: crm's
cost counts only the crm pool, c360's counts only the c360 pool.

### 5b. Make the existing metrics path pool-aware (required by the pool split)

Resolves builder-check #2 — a correctness fix, not just a new feature. The
existing live status/metrics drill-down hardcodes the single default pool in its
Metrics API queries (`confluent_client.py` `get_statement_metrics` /
`get_materialized_table_metrics`, using `settings.FLINK_COMPUTE_POOL_ID`). Once
crm statements run on the crm pool, those per-node queries for crm nodes would
target the wrong pool and return empty/403 — a silent regression of the existing
feature. Fix: thread each node's `compute_pool_id` (now present on the descriptor
node) through the metrics/status calls, falling back to
`FLINK_COMPUTE_POOL_ID` when a node has none. This is in scope and is a
prerequisite for the two-pool demo working live. (Call made — see handover; it
enlarges the backend change beyond the banner itself.)

### 6. Frontend

- `types/dataProduct.ts`: add `Cost` type mirroring `CostResponse`.
- `api/dataProducts.ts`: `getCost(id)` → `GET /data-products/{id}/cost`.
- `StatementGraphPage.tsx`: fetch cost on mount and on a **slow** interval
  (e.g. 5 min; cost lags ~a day), render in the `.page-header` / `.health-bar`
  as `$<amount> this month · <N> CFU`. When `available === false` or the fetch
  fails, hide the banner (no `$0`, no error) — the graph is unaffected.
- Format via existing `lib/format.ts` (add a currency formatter if none).

## Billing role access — DECIDED: add to Terraform

The feature's premise is showing **real billed dollars**. The existing
`CONFLUENT_CLOUD_API_KEY` principal is provisioned for MetricsViewer; the Cost
API (`/billing/v1/costs`) needs an **org-level billing read role**.

**Decision (owner):** add the billing role binding to the Terraform. §1 adds a
`confluent_role_binding` granting the principal org-level cost/billing read, so
`terraform apply` enables the real-dollars live path. Applying this binding may
itself require org-admin privileges on the account running apply — the owner
runs apply. The **exact role name** (e.g. `OrganizationAdmin`, or a dedicated
billing/metrics-billing role if available on this org) is confirmed at build
time, since Confluent's granular billing-read role availability varies by org;
the binding targets the organization CRN of the existing Flink service account
principal.

## Live verification (resolves the open risk)

During build, against the live environment, fetch one real
`GET /billing/v1/costs` response for a day with Flink usage and confirm that
`product == FLINK` line items carry the compute pool id in `resource.id`
(`lfcp-…`). If confirmed, the per-pool design stands as written. If Flink cost
is only environment-granular, record it here and fall back to per-environment
attribution (the two-pool demo still differentiates products only if the pools
are in different environments — if not, document the limitation and lean on mock
mode for the demo). This verification gates the live path; mock mode ships
regardless.

**Status (2026-10-30): PENDING — owner action.** The implementation shipped with
mock mode fully working and the live path best-effort (hidden on 403/empty). The
live granularity check could not be run during build: it needs the crm compute
pool applied (`terraform apply`), the billing role granted, and real Flink usage
to bill against — none available in the build environment. Owner to run after
applying the infra: fetch one `GET /billing/v1/costs` day with Flink usage,
confirm `product == FLINK` items carry the pool id in `resource.id`, and record
the outcome here. Also refresh the committed descriptors (`dbt run` +
`scripts/build_descriptors.py`) so their `compute_pool_id` values are the real
pool ids rather than the build-time placeholders (`lfcp-c360` / `lfcp-crm`).

## Testing

- Backend unit tests mirroring existing `tests/`: descriptor builder emits
  `compute_pool_id` with correct fallback; provider `get_cost` sums only the
  owned pools and excludes the shared upstream; mock provider returns a stable
  synthetic `CostResponse`; cost route shape. Client HTTP is mocked
  (httpx), asserting the Cost API query params (`start_date`/`end_date`/paging)
  and the FLINK + pool-id filtering.
- Frontend: a light test/manual check that the banner renders format B and
  hides when `available === false`.

## What's left to the builder

- Exact dbt spelling (per-model `config()` vs `dbt_project.yml` folder block).
- The precise "owned statement" signal in the descriptor (reuse `is_product` /
  folder, or add an explicit marker) — subject to the fixed observable result.
- How live current-CFU per pool is summed for the banner's `N CFU`, vs reusing
  metrics the page already holds.
- Synthetic values for mock mode.
- Currency formatting helper details.
