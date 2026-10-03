# Customers Frontend — Design Spec

Date: 2026-10-02
Status: Approved for planning

## Intent

Build a Vite + React + TypeScript frontend that exposes the backend Customer
360 REST API. The first deliverable is the **Customers** page: a table of
customers with per-row actions to view, edit, and delete a record. Edit (and
create) open a separate form page. A shared header and a left sidebar
(Customers / Accounts / Transactions) frame every page.

Audience: internal demo of the C360 streaming pipeline. Success = a user can
browse the customer list, view a customer's detail, create a customer, edit an
existing one, and delete one, all against the live FastAPI backend.

## Stack Decisions

- **Language:** TypeScript (Vite `react-ts` template).
- **Styling:** Plain CSS. Icons from `lucide-react`.
- **Data layer:** Native `fetch` wrapped in a small typed API module. No
  TanStack Query / axios.
- **Backend wiring:** Enable CORS on the FastAPI backend for the Vite dev
  origin (`http://localhost:5173`). No Vite proxy.
- **Routing:** `react-router-dom`.

## Backend Contract (existing)

Base path: `/api/v1/customers` (FastAPI, see
`apps/backend/api/customer_resource.py`).

- `GET    /api/v1/customers`        → `Customer[]`
- `GET    /api/v1/customers/{id}`   → `Customer` (404 if missing)
- `POST   /api/v1/customers`        → `Customer` (201)
- `PUT    /api/v1/customers/{id}`   → `Customer` (200, 404 if missing)
- `DELETE /api/v1/customers/{id}`   → 204 (404 if missing)

`Customer` fields (from `apps/backend/customers/models.py`):

| field | type | notes |
|-------|------|-------|
| customer_id | UUID | server-assigned |
| first_name | string | required |
| last_name | string | required |
| email | string | required |
| phone | string? | |
| date_of_birth | datetime? | edited via date input |
| gender | string? | |
| address_line1 | string? | |
| address_line2 | string? | |
| city | string? | |
| state | string? | |
| postal_code | string? | |
| country | string | default "US" |
| customer_since | date | default today |
| segment | string? | |
| status | string | default "ACTIVE" |
| created_at | datetime | server-assigned, read-only |
| updated_at | datetime | server-assigned, read-only |

`CustomerCreate` = all fields except the server-assigned ones.
`CustomerUpdate` = same fields, all optional.

## Directory Layout

```
apps/frontend/
  package.json
  tsconfig.json
  vite.config.ts
  index.html
  src/
    main.tsx                 # bootstrap + <BrowserRouter>
    App.tsx                  # routes inside <AppLayout>
    config.ts                # API base URL (import.meta.env.VITE_API_BASE_URL)
    api/client.ts            # fetch wrapper: base URL, JSON, error handling
    api/customers.ts         # listCustomers/getCustomer/createCustomer/updateCustomer/deleteCustomer
    types/customer.ts        # Customer, CustomerCreate, CustomerUpdate
    components/
      AppLayout.tsx          # Header + Sidebar + <Outlet/> content area
      Header.tsx             # app title, shared across pages
      Sidebar.tsx            # nav links w/ active state
      ConfirmDialog.tsx      # reusable confirm modal (used for delete)
    pages/
      CustomersPage.tsx      # table + row actions + "Add customer"
      CustomerDetailPage.tsx # read-only detail view
      CustomerFormPage.tsx   # create AND edit form
      PlaceholderPage.tsx    # "coming soon" for Accounts/Transactions
    styles/
      index.css              # base + layout
      (component-scoped css as needed)
```

## Routes

| path | page | notes |
|------|------|-------|
| `/` | — | redirect to `/customers` |
| `/customers` | CustomersPage | table view + Add button |
| `/customers/new` | CustomerFormPage | create mode (no id) |
| `/customers/:id` | CustomerDetailPage | view icon target |
| `/customers/:id/edit` | CustomerFormPage | edit mode (loads record) |
| `/accounts` | PlaceholderPage | "coming soon" |
| `/transactions` | PlaceholderPage | "coming soon" |

## Components & Responsibilities

- **AppLayout** — fixed header on top, fixed sidebar on left, scrollable
  content region rendering `<Outlet/>`. Pure layout, no data.
- **Header** — static app title/branding. No props needed initially.
- **Sidebar** — three `NavLink`s (Customers, Accounts, Transactions) with
  active styling. Icons from lucide-react.
- **ConfirmDialog** — props: `open`, `title`, `message`, `onConfirm`,
  `onCancel`. Reusable; used by CustomersPage for delete.
- **CustomersPage** — fetches list on mount; holds `loading`/`error`/`data`
  state. Renders table columns: Name (first + last), Email, Phone, Segment,
  Status, Actions (view/edit/delete icons). Delete opens ConfirmDialog, calls
  `deleteCustomer`, then refetches. "Add customer" button → `/customers/new`.
- **CustomerDetailPage** — fetches one by id; renders read-only field list;
  "Edit" and "Back" actions.
- **CustomerFormPage** — controlled form over editable fields. If `:id`
  present: fetch record, populate, submit via `updateCustomer` (PUT). Else:
  blank form, submit via `createCustomer` (POST). Client-side required-field
  validation for first_name, last_name, email. On success → navigate
  `/customers`. Shows submit error inline.
- **PlaceholderPage** — generic "coming soon" with a title prop.

## Data Layer

- `config.ts`: `API_BASE_URL = import.meta.env.VITE_API_BASE_URL ??
  "http://localhost:8000/api/v1"`.
- `api/client.ts`: `request<T>(path, options)` — prefixes base URL, sets JSON
  headers, throws a typed `ApiError` (status + message) on non-2xx, returns
  parsed JSON (or `undefined` for 204).
- `api/customers.ts`: the five CRUD functions, typed against
  `types/customer.ts`.

## Error & Loading Handling

- List/detail pages render a loading indicator while fetching and an error
  message (with the API message) on failure.
- Form submit failures render inline near the submit button; the form stays
  populated.
- Delete failures surface a message and leave the row intact.

## Backend Change

Add `fastapi.middleware.cors.CORSMiddleware` to `apps/backend/main.py`:
allow origin `http://localhost:5173` (dev), methods `GET,POST,PUT,DELETE`,
all headers. Keep it minimal and dev-oriented.

## Out of Scope (YAGNI)

- Authentication / authorization.
- Pagination, sorting, search/filter (list endpoint returns all records).
- Accounts and Transactions data pages (no backend yet — placeholders only).
- State caching libraries, optimistic updates.
- Production build/deploy pipeline and Dockerization.

## Testing

- Component/unit tests with Vitest + React Testing Library:
  - `api/customers.ts` builds correct requests (mock fetch).
  - CustomersPage renders rows from mocked list and triggers delete flow.
  - CustomerFormPage validates required fields and calls create vs update
    based on presence of id.
- Manual verification against the running backend for the full CRUD loop.
