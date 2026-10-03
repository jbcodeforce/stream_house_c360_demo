# Customers Frontend Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a Vite + React + TypeScript frontend whose Customers page lists customers from the backend REST API with per-row view/edit/delete actions, plus separate detail and create/edit form pages.

**Architecture:** A React SPA under `apps/frontend/`. A shared `AppLayout` (header + left sidebar) wraps all routes via react-router. A thin typed `fetch` wrapper (`api/client.ts`) backs a `api/customers.ts` CRUD module consumed by the pages. The FastAPI backend gets a CORS middleware so the Vite dev server (`:5173`) can call it directly.

**Tech Stack:** Vite, React 18, TypeScript, react-router-dom, lucide-react (icons), Vitest + React Testing Library (tests), plain CSS. Backend: FastAPI + Starlette CORSMiddleware, pytest.

**Spec:** `docs/superpowers/specs/2026-10-02-customers-frontend-design.md`

## Global Constraints

- Frontend lives entirely under `apps/frontend/` (sibling of `apps/backend/`).
- Language: TypeScript. UI icons: `lucide-react`. No TanStack Query / axios — native `fetch` only. No Tailwind / component library — plain CSS.
- API base URL resolves from `import.meta.env.VITE_API_BASE_URL`, defaulting to `http://localhost:8000/api/v1`.
- Backend base path for customers is `/api/v1/customers` (do not change it).
- Dev frontend origin is `http://localhost:5173`; the backend CORS allowlist must include exactly that origin.
- Required customer fields (client validation + types): `first_name`, `last_name`, `email`. All other create/update fields optional.
- Node 26 / npm 11 available. Backend managed with `uv` (`uv run pytest`).
- Commit after each task with Conventional Commits (`feat:`, `test:`, `chore:`). Do NOT commit unless the executor is explicitly running under an approved execution flow; follow the repo rule "commit only when asked" — these commit steps are the intended granularity once execution is authorized.

## Review Focus

These are behaviors the spec implies but that no single task's happy-path test fully exercises. Each has a test pinned to the owning task below.

- **Non-2xx API response** (e.g. 404 on detail, 500 on list) — the client must throw a typed `ApiError` carrying the status and server message, and pages must show the message, not a blank screen. (Task 3 tests the throw; Tasks 6/7 test the page display.)
- **204 No Content on delete** — `client.request` must not attempt `response.json()` on an empty 204 body or it throws a parse error. (Task 3.)
- **Create vs update dispatch** — `CustomerFormPage` must POST when there is no `:id` and PUT when there is; sending the wrong verb silently corrupts data. (Task 8.)
- **Optional/empty form fields** — blank optional inputs must be sent as `null`/omitted, not empty strings, so the backend defaults apply. (Task 8.)
- **CORS preflight/actual request from the dev origin** — a request from `http://localhost:5173` must come back with the right `access-control-allow-origin` header, or every call fails in the browser. (Task 10.)

---

## Task 1: Scaffold the Vite React-TS app

**Files:**
- Create: `apps/frontend/` (via Vite scaffolder), then prune boilerplate.
- Create: `apps/frontend/.gitignore` (if scaffolder doesn't produce one covering `node_modules`, `dist`).

**Interfaces:**
- Produces: a runnable Vite app with `npm run dev`, `npm run build`, `npm test` scripts; React + TypeScript configured.

- [ ] **Step 1: Scaffold with Vite**

Run from `apps/`:
```bash
cd /Users/jerome/Documents/Code/stream_house_c360_demo/apps
npm create vite@latest frontend -- --template react-ts
cd frontend
npm install
```

- [ ] **Step 2: Add runtime + dev dependencies**

```bash
cd /Users/jerome/Documents/Code/stream_house_c360_demo/apps/frontend
npm install react-router-dom lucide-react
npm install -D vitest @testing-library/react @testing-library/jest-dom @testing-library/user-event jsdom
```

- [ ] **Step 3: Configure Vitest**

Edit `apps/frontend/vite.config.ts` to add a `test` block and a setup file:
```ts
/// <reference types="vitest/config" />
import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

export default defineConfig({
  plugins: [react()],
  server: { port: 5173 },
  test: {
    globals: true,
    environment: 'jsdom',
    setupFiles: './src/test/setup.ts',
  },
})
```

Create `apps/frontend/src/test/setup.ts`:
```ts
import '@testing-library/jest-dom'
```

Add a `test` script to `apps/frontend/package.json` scripts:
```json
"test": "vitest run",
"test:watch": "vitest"
```

- [ ] **Step 4: Prune boilerplate**

Delete `src/App.css`, `src/assets/react.svg`, and the default logo markup. Replace `src/App.tsx` with a temporary stub:
```tsx
export default function App() {
  return <div>C360</div>
}
```
Leave `src/main.tsx` as generated for now (replaced in Task 5).

- [ ] **Step 5: Verify it builds and the test runner works**

Run:
```bash
npm run build
npx vitest run
```
Expected: build succeeds; vitest runs with "no test files found" (exit 0) — that is fine at this stage.

- [ ] **Step 6: Commit**

```bash
cd /Users/jerome/Documents/Code/stream_house_c360_demo
git add apps/frontend
git commit -m "chore: scaffold vite react-ts frontend"
```

---

## Task 2: Customer types

**Files:**
- Create: `apps/frontend/src/types/customer.ts`

**Interfaces:**
- Produces: `Customer`, `CustomerCreate`, `CustomerUpdate` TypeScript types used by every later task.

- [ ] **Step 1: Write the types**

Create `apps/frontend/src/types/customer.ts`:
```ts
export interface CustomerCreate {
  first_name: string
  last_name: string
  email: string
  phone?: string | null
  date_of_birth?: string | null // ISO date/datetime string
  gender?: string | null
  address_line1?: string | null
  address_line2?: string | null
  city?: string | null
  state?: string | null
  postal_code?: string | null
  country?: string | null // backend defaults to "US"
  customer_since?: string | null // ISO date; backend defaults to today
  segment?: string | null
  status?: string | null // backend defaults to "ACTIVE"
}

export type CustomerUpdate = Partial<CustomerCreate>

export interface Customer extends CustomerCreate {
  customer_id: string
  created_at: string
  updated_at: string
}
```

- [ ] **Step 2: Typecheck**

Run:
```bash
cd /Users/jerome/Documents/Code/stream_house_c360_demo/apps/frontend
npx tsc --noEmit
```
Expected: no errors.

- [ ] **Step 3: Commit**

```bash
cd /Users/jerome/Documents/Code/stream_house_c360_demo
git add apps/frontend/src/types/customer.ts
git commit -m "feat: add customer TypeScript types"
```

---

## Task 3: API client wrapper

**Files:**
- Create: `apps/frontend/src/config.ts`
- Create: `apps/frontend/src/api/client.ts`
- Test: `apps/frontend/src/api/client.test.ts`

**Interfaces:**
- Consumes: nothing.
- Produces:
  - `API_BASE_URL: string` (from `config.ts`).
  - `class ApiError extends Error { status: number }`.
  - `request<T>(path: string, options?: RequestInit): Promise<T>` — prefixes `API_BASE_URL`, sets `Content-Type: application/json`, throws `ApiError` on non-2xx, returns parsed JSON, or `undefined as T` for 204.

- [ ] **Step 1: Write config**

Create `apps/frontend/src/config.ts`:
```ts
export const API_BASE_URL =
  import.meta.env.VITE_API_BASE_URL ?? 'http://localhost:8000/api/v1'
```

- [ ] **Step 2: Write the failing tests**

Create `apps/frontend/src/api/client.test.ts`:
```ts
import { afterEach, describe, expect, it, vi } from 'vitest'
import { ApiError, request } from './client'

afterEach(() => vi.restoreAllMocks())

function mockFetch(resp: Partial<Response> & { json?: () => Promise<unknown> }) {
  vi.stubGlobal('fetch', vi.fn().mockResolvedValue(resp as Response))
}

describe('request', () => {
  it('returns parsed JSON on 200', async () => {
    mockFetch({ ok: true, status: 200, json: async () => ({ a: 1 }) })
    const data = await request<{ a: number }>('/x')
    expect(data).toEqual({ a: 1 })
  })

  it('returns undefined on 204 without parsing body', async () => {
    const json = vi.fn()
    mockFetch({ ok: true, status: 204, json })
    const data = await request<void>('/x', { method: 'DELETE' })
    expect(data).toBeUndefined()
    expect(json).not.toHaveBeenCalled()
  })

  it('throws ApiError with status and server detail on non-2xx', async () => {
    mockFetch({ ok: false, status: 404, json: async () => ({ detail: 'Customer not found' }) })
    await expect(request('/x')).rejects.toMatchObject({
      name: 'ApiError',
      status: 404,
      message: 'Customer not found',
    })
    expect((await request('/x').catch((e) => e)) instanceof ApiError).toBe(true)
  })
})
```

- [ ] **Step 3: Run tests to verify they fail**

Run:
```bash
cd /Users/jerome/Documents/Code/stream_house_c360_demo/apps/frontend
npx vitest run src/api/client.test.ts
```
Expected: FAIL — `./client` has no `request`/`ApiError` export.

- [ ] **Step 4: Implement the client**

Create `apps/frontend/src/api/client.ts`:
```ts
import { API_BASE_URL } from '../config'

export class ApiError extends Error {
  status: number
  constructor(status: number, message: string) {
    super(message)
    this.name = 'ApiError'
    this.status = status
  }
}

export async function request<T>(path: string, options: RequestInit = {}): Promise<T> {
  const response = await fetch(`${API_BASE_URL}${path}`, {
    ...options,
    headers: { 'Content-Type': 'application/json', ...(options.headers ?? {}) },
  })

  if (!response.ok) {
    let message = `Request failed with status ${response.status}`
    try {
      const body = (await response.json()) as { detail?: string }
      if (body?.detail) message = body.detail
    } catch {
      // non-JSON error body — keep default message
    }
    throw new ApiError(response.status, message)
  }

  if (response.status === 204) return undefined as T
  return (await response.json()) as T
}
```

- [ ] **Step 5: Run tests to verify they pass**

Run:
```bash
npx vitest run src/api/client.test.ts
```
Expected: PASS (3 tests).

- [ ] **Step 6: Commit**

```bash
cd /Users/jerome/Documents/Code/stream_house_c360_demo
git add apps/frontend/src/config.ts apps/frontend/src/api/client.ts apps/frontend/src/api/client.test.ts
git commit -m "feat: add typed fetch api client with ApiError"
```

---

## Task 4: Customers API module

**Files:**
- Create: `apps/frontend/src/api/customers.ts`
- Test: `apps/frontend/src/api/customers.test.ts`

**Interfaces:**
- Consumes: `request` from `./client`; types from `../types/customer`.
- Produces:
  - `listCustomers(): Promise<Customer[]>`
  - `getCustomer(id: string): Promise<Customer>`
  - `createCustomer(data: CustomerCreate): Promise<Customer>`
  - `updateCustomer(id: string, data: CustomerUpdate): Promise<Customer>`
  - `deleteCustomer(id: string): Promise<void>`

- [ ] **Step 1: Write the failing tests**

Create `apps/frontend/src/api/customers.test.ts`:
```ts
import { afterEach, describe, expect, it, vi } from 'vitest'
import * as client from './client'
import {
  createCustomer,
  deleteCustomer,
  getCustomer,
  listCustomers,
  updateCustomer,
} from './customers'

afterEach(() => vi.restoreAllMocks())

describe('customers api', () => {
  it('listCustomers GETs /customers', async () => {
    const spy = vi.spyOn(client, 'request').mockResolvedValue([])
    await listCustomers()
    expect(spy).toHaveBeenCalledWith('/customers')
  })

  it('getCustomer GETs /customers/:id', async () => {
    const spy = vi.spyOn(client, 'request').mockResolvedValue({})
    await getCustomer('abc')
    expect(spy).toHaveBeenCalledWith('/customers/abc')
  })

  it('createCustomer POSTs body to /customers', async () => {
    const spy = vi.spyOn(client, 'request').mockResolvedValue({})
    const body = { first_name: 'A', last_name: 'B', email: 'a@b.c' }
    await createCustomer(body)
    expect(spy).toHaveBeenCalledWith('/customers', {
      method: 'POST',
      body: JSON.stringify(body),
    })
  })

  it('updateCustomer PUTs body to /customers/:id', async () => {
    const spy = vi.spyOn(client, 'request').mockResolvedValue({})
    await updateCustomer('abc', { city: 'NYC' })
    expect(spy).toHaveBeenCalledWith('/customers/abc', {
      method: 'PUT',
      body: JSON.stringify({ city: 'NYC' }),
    })
  })

  it('deleteCustomer DELETEs /customers/:id', async () => {
    const spy = vi.spyOn(client, 'request').mockResolvedValue(undefined)
    await deleteCustomer('abc')
    expect(spy).toHaveBeenCalledWith('/customers/abc', { method: 'DELETE' })
  })
})
```

- [ ] **Step 2: Run tests to verify they fail**

Run:
```bash
cd /Users/jerome/Documents/Code/stream_house_c360_demo/apps/frontend
npx vitest run src/api/customers.test.ts
```
Expected: FAIL — `./customers` module not found.

- [ ] **Step 3: Implement the module**

Create `apps/frontend/src/api/customers.ts`:
```ts
import type { Customer, CustomerCreate, CustomerUpdate } from '../types/customer'
import { request } from './client'

export function listCustomers(): Promise<Customer[]> {
  return request<Customer[]>('/customers')
}

export function getCustomer(id: string): Promise<Customer> {
  return request<Customer>(`/customers/${id}`)
}

export function createCustomer(data: CustomerCreate): Promise<Customer> {
  return request<Customer>('/customers', {
    method: 'POST',
    body: JSON.stringify(data),
  })
}

export function updateCustomer(id: string, data: CustomerUpdate): Promise<Customer> {
  return request<Customer>(`/customers/${id}`, {
    method: 'PUT',
    body: JSON.stringify(data),
  })
}

export function deleteCustomer(id: string): Promise<void> {
  return request<void>(`/customers/${id}`, { method: 'DELETE' })
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run:
```bash
npx vitest run src/api/customers.test.ts
```
Expected: PASS (5 tests).

- [ ] **Step 5: Commit**

```bash
cd /Users/jerome/Documents/Code/stream_house_c360_demo
git add apps/frontend/src/api/customers.ts apps/frontend/src/api/customers.test.ts
git commit -m "feat: add customers api module"
```

---

## Task 5: App shell — layout, header, sidebar, routing

**Files:**
- Create: `apps/frontend/src/components/Header.tsx`
- Create: `apps/frontend/src/components/Sidebar.tsx`
- Create: `apps/frontend/src/components/AppLayout.tsx`
- Create: `apps/frontend/src/pages/PlaceholderPage.tsx`
- Create: `apps/frontend/src/styles/index.css`
- Modify: `apps/frontend/src/App.tsx` (routes)
- Modify: `apps/frontend/src/main.tsx` (BrowserRouter + import css)
- Test: `apps/frontend/src/components/Sidebar.test.tsx`

**Interfaces:**
- Consumes: react-router-dom.
- Produces:
  - `AppLayout` — renders `<Header/>`, `<Sidebar/>`, and `<Outlet/>`.
  - `PlaceholderPage({ title }: { title: string })`.
  - Route table in `App.tsx` with the paths from the spec. Page components for customers are imported here (created in Tasks 6–8); until then use temporary inline stubs noted in Step 5.

- [ ] **Step 1: Write the failing test for Sidebar**

Create `apps/frontend/src/components/Sidebar.test.tsx`:
```tsx
import { render, screen } from '@testing-library/react'
import { MemoryRouter } from 'react-router-dom'
import { describe, expect, it } from 'vitest'
import Sidebar from './Sidebar'

describe('Sidebar', () => {
  it('renders nav links for the three sections', () => {
    render(
      <MemoryRouter>
        <Sidebar />
      </MemoryRouter>,
    )
    expect(screen.getByRole('link', { name: /customers/i })).toHaveAttribute('href', '/customers')
    expect(screen.getByRole('link', { name: /accounts/i })).toHaveAttribute('href', '/accounts')
    expect(screen.getByRole('link', { name: /transactions/i })).toHaveAttribute(
      'href',
      '/transactions',
    )
  })
})
```

- [ ] **Step 2: Run the test to verify it fails**

Run:
```bash
cd /Users/jerome/Documents/Code/stream_house_c360_demo/apps/frontend
npx vitest run src/components/Sidebar.test.tsx
```
Expected: FAIL — `./Sidebar` not found.

- [ ] **Step 3: Implement Header, Sidebar, AppLayout, PlaceholderPage**

Create `apps/frontend/src/components/Header.tsx`:
```tsx
export default function Header() {
  return (
    <header className="app-header">
      <span className="app-header__title">Customer 360</span>
    </header>
  )
}
```

Create `apps/frontend/src/components/Sidebar.tsx`:
```tsx
import { Users, Wallet, ArrowLeftRight } from 'lucide-react'
import { NavLink } from 'react-router-dom'

const links = [
  { to: '/customers', label: 'Customers', Icon: Users },
  { to: '/accounts', label: 'Accounts', Icon: Wallet },
  { to: '/transactions', label: 'Transactions', Icon: ArrowLeftRight },
]

export default function Sidebar() {
  return (
    <nav className="sidebar">
      {links.map(({ to, label, Icon }) => (
        <NavLink
          key={to}
          to={to}
          className={({ isActive }) => `sidebar__link${isActive ? ' sidebar__link--active' : ''}`}
        >
          <Icon size={18} />
          <span>{label}</span>
        </NavLink>
      ))}
    </nav>
  )
}
```

Create `apps/frontend/src/components/AppLayout.tsx`:
```tsx
import { Outlet } from 'react-router-dom'
import Header from './Header'
import Sidebar from './Sidebar'

export default function AppLayout() {
  return (
    <div className="app">
      <Header />
      <div className="app__body">
        <Sidebar />
        <main className="app__content">
          <Outlet />
        </main>
      </div>
    </div>
  )
}
```

Create `apps/frontend/src/pages/PlaceholderPage.tsx`:
```tsx
export default function PlaceholderPage({ title }: { title: string }) {
  return (
    <div className="placeholder">
      <h1>{title}</h1>
      <p>Coming soon.</p>
    </div>
  )
}
```

- [ ] **Step 4: Add base styles**

Create `apps/frontend/src/styles/index.css`:
```css
:root { --sidebar-w: 220px; --header-h: 56px; --border: #e2e8f0; --accent: #2563eb; }
* { box-sizing: border-box; }
body { margin: 0; font-family: system-ui, -apple-system, sans-serif; color: #0f172a; }
.app__body { display: flex; min-height: calc(100vh - var(--header-h)); }
.app-header {
  height: var(--header-h); display: flex; align-items: center; padding: 0 20px;
  border-bottom: 1px solid var(--border); background: #0f172a; color: #fff;
}
.app-header__title { font-weight: 600; font-size: 18px; }
.sidebar {
  width: var(--sidebar-w); border-right: 1px solid var(--border); padding: 16px 8px;
  display: flex; flex-direction: column; gap: 4px;
}
.sidebar__link {
  display: flex; align-items: center; gap: 10px; padding: 10px 12px; border-radius: 8px;
  color: #334155; text-decoration: none; font-size: 14px;
}
.sidebar__link:hover { background: #f1f5f9; }
.sidebar__link--active { background: #e0e7ff; color: var(--accent); font-weight: 600; }
.app__content { flex: 1; padding: 24px; overflow: auto; }
.placeholder h1 { margin-top: 0; }

/* Table */
.table { width: 100%; border-collapse: collapse; font-size: 14px; }
.table th, .table td { text-align: left; padding: 10px 12px; border-bottom: 1px solid var(--border); }
.table th { color: #64748b; font-weight: 600; }
.row-actions { display: flex; gap: 8px; }
.icon-btn { background: none; border: none; cursor: pointer; color: #64748b; padding: 4px; border-radius: 6px; }
.icon-btn:hover { background: #f1f5f9; color: var(--accent); }
.icon-btn--danger:hover { color: #dc2626; }
.page-header { display: flex; justify-content: space-between; align-items: center; margin-bottom: 16px; }
.btn { background: var(--accent); color: #fff; border: none; padding: 9px 14px; border-radius: 8px; cursor: pointer; font-size: 14px; text-decoration: none; display: inline-block; }
.btn--secondary { background: #e2e8f0; color: #0f172a; }
.status-msg { padding: 16px; color: #64748b; }
.status-msg--error { color: #dc2626; }

/* Form */
.form { max-width: 640px; display: grid; gap: 14px; }
.form__row { display: grid; gap: 4px; }
.form__row label { font-size: 13px; color: #334155; font-weight: 500; }
.form input, .form select { padding: 9px 10px; border: 1px solid var(--border); border-radius: 8px; font-size: 14px; }
.form__error { color: #dc2626; font-size: 13px; }
.form__actions { display: flex; gap: 10px; margin-top: 8px; }
.field-error { color: #dc2626; font-size: 12px; }

/* Detail */
.detail dl { display: grid; grid-template-columns: 200px 1fr; gap: 8px 16px; font-size: 14px; }
.detail dt { color: #64748b; }

/* Dialog */
.dialog-backdrop { position: fixed; inset: 0; background: rgba(15,23,42,.45); display: flex; align-items: center; justify-content: center; }
.dialog { background: #fff; border-radius: 12px; padding: 24px; max-width: 420px; }
.dialog__actions { display: flex; gap: 10px; justify-content: flex-end; margin-top: 20px; }
```

- [ ] **Step 5: Wire routes and bootstrap**

Replace `apps/frontend/src/App.tsx`:
```tsx
import { Navigate, Route, Routes } from 'react-router-dom'
import AppLayout from './components/AppLayout'
import PlaceholderPage from './pages/PlaceholderPage'
import CustomersPage from './pages/CustomersPage'
import CustomerDetailPage from './pages/CustomerDetailPage'
import CustomerFormPage from './pages/CustomerFormPage'

export default function App() {
  return (
    <Routes>
      <Route element={<AppLayout />}>
        <Route index element={<Navigate to="/customers" replace />} />
        <Route path="/customers" element={<CustomersPage />} />
        <Route path="/customers/new" element={<CustomerFormPage />} />
        <Route path="/customers/:id" element={<CustomerDetailPage />} />
        <Route path="/customers/:id/edit" element={<CustomerFormPage />} />
        <Route path="/accounts" element={<PlaceholderPage title="Accounts" />} />
        <Route path="/transactions" element={<PlaceholderPage title="Transactions" />} />
      </Route>
    </Routes>
  )
}
```

Note: `CustomersPage`, `CustomerDetailPage`, and `CustomerFormPage` are created in Tasks 6–8. To keep this task independently runnable, create three one-line stub files now and replace them in later tasks:
```tsx
// apps/frontend/src/pages/CustomersPage.tsx (temporary stub — replaced in Task 6)
export default function CustomersPage() { return <div>Customers</div> }
```
```tsx
// apps/frontend/src/pages/CustomerDetailPage.tsx (temporary stub — replaced in Task 7)
export default function CustomerDetailPage() { return <div>Detail</div> }
```
```tsx
// apps/frontend/src/pages/CustomerFormPage.tsx (temporary stub — replaced in Task 8)
export default function CustomerFormPage() { return <div>Form</div> }
```

Replace `apps/frontend/src/main.tsx`:
```tsx
import { StrictMode } from 'react'
import { createRoot } from 'react-dom/client'
import { BrowserRouter } from 'react-router-dom'
import App from './App'
import './styles/index.css'

createRoot(document.getElementById('root')!).render(
  <StrictMode>
    <BrowserRouter>
      <App />
    </BrowserRouter>
  </StrictMode>,
)
```

- [ ] **Step 6: Run the Sidebar test and typecheck**

Run:
```bash
npx vitest run src/components/Sidebar.test.tsx
npx tsc --noEmit
```
Expected: Sidebar test PASS; no type errors.

- [ ] **Step 7: Commit**

```bash
cd /Users/jerome/Documents/Code/stream_house_c360_demo
git add apps/frontend/src
git commit -m "feat: add app layout, header, sidebar, and routing"
```

---

## Task 6: Customers table page

**Files:**
- Create: `apps/frontend/src/components/ConfirmDialog.tsx`
- Modify (replace stub): `apps/frontend/src/pages/CustomersPage.tsx`
- Test: `apps/frontend/src/pages/CustomersPage.test.tsx`

**Interfaces:**
- Consumes: `listCustomers`, `deleteCustomer` from `../api/customers`; `Customer` type; react-router `useNavigate`/`Link`.
- Produces: `ConfirmDialog({ open, title, message, onConfirm, onCancel })`; `CustomersPage` default export.

- [ ] **Step 1: Write the failing tests**

Create `apps/frontend/src/pages/CustomersPage.test.tsx`:
```tsx
import { render, screen, waitFor, within } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { MemoryRouter } from 'react-router-dom'
import { afterEach, describe, expect, it, vi } from 'vitest'
import * as api from '../api/customers'
import { ApiError } from '../api/client'
import CustomersPage from './CustomersPage'

const sample = [
  {
    customer_id: '1', first_name: 'Ada', last_name: 'Lovelace', email: 'ada@x.io',
    phone: '555', segment: 'VIP', status: 'ACTIVE',
    country: 'US', customer_since: '2020-01-01', created_at: '', updated_at: '',
  },
]

afterEach(() => vi.restoreAllMocks())

function renderPage() {
  return render(
    <MemoryRouter>
      <CustomersPage />
    </MemoryRouter>,
  )
}

describe('CustomersPage', () => {
  it('renders a row per customer from the API', async () => {
    vi.spyOn(api, 'listCustomers').mockResolvedValue(sample as never)
    renderPage()
    expect(await screen.findByText('Ada Lovelace')).toBeInTheDocument()
    expect(screen.getByText('ada@x.io')).toBeInTheDocument()
  })

  it('shows an error message when the list request fails', async () => {
    vi.spyOn(api, 'listCustomers').mockRejectedValue(new ApiError(500, 'boom'))
    renderPage()
    expect(await screen.findByText(/boom/)).toBeInTheDocument()
  })

  it('deletes a customer after confirmation and refetches', async () => {
    const list = vi.spyOn(api, 'listCustomers')
    list.mockResolvedValueOnce(sample as never).mockResolvedValueOnce([] as never)
    const del = vi.spyOn(api, 'deleteCustomer').mockResolvedValue(undefined)
    renderPage()
    await screen.findByText('Ada Lovelace')

    const row = screen.getByText('Ada Lovelace').closest('tr')!
    await userEvent.click(within(row).getByRole('button', { name: /delete/i }))
    await userEvent.click(screen.getByRole('button', { name: /^delete$/i }))

    await waitFor(() => expect(del).toHaveBeenCalledWith('1'))
    await waitFor(() => expect(screen.queryByText('Ada Lovelace')).not.toBeInTheDocument())
  })
})
```

- [ ] **Step 2: Run tests to verify they fail**

Run:
```bash
cd /Users/jerome/Documents/Code/stream_house_c360_demo/apps/frontend
npx vitest run src/pages/CustomersPage.test.tsx
```
Expected: FAIL — current CustomersPage is the stub.

- [ ] **Step 3: Implement ConfirmDialog**

Create `apps/frontend/src/components/ConfirmDialog.tsx`:
```tsx
interface Props {
  open: boolean
  title: string
  message: string
  onConfirm: () => void
  onCancel: () => void
}

export default function ConfirmDialog({ open, title, message, onConfirm, onCancel }: Props) {
  if (!open) return null
  return (
    <div className="dialog-backdrop" onClick={onCancel}>
      <div className="dialog" role="dialog" aria-modal="true" onClick={(e) => e.stopPropagation()}>
        <h2 style={{ marginTop: 0 }}>{title}</h2>
        <p>{message}</p>
        <div className="dialog__actions">
          <button className="btn btn--secondary" onClick={onCancel}>Cancel</button>
          <button className="btn" style={{ background: '#dc2626' }} onClick={onConfirm}>Delete</button>
        </div>
      </div>
    </div>
  )
}
```

- [ ] **Step 4: Implement CustomersPage**

Replace `apps/frontend/src/pages/CustomersPage.tsx`:
```tsx
import { Eye, Pencil, Trash2 } from 'lucide-react'
import { useEffect, useState } from 'react'
import { Link, useNavigate } from 'react-router-dom'
import ConfirmDialog from '../components/ConfirmDialog'
import { ApiError } from '../api/client'
import { deleteCustomer, listCustomers } from '../api/customers'
import type { Customer } from '../types/customer'

export default function CustomersPage() {
  const navigate = useNavigate()
  const [customers, setCustomers] = useState<Customer[]>([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)
  const [pendingDelete, setPendingDelete] = useState<Customer | null>(null)

  async function load() {
    setLoading(true)
    setError(null)
    try {
      setCustomers(await listCustomers())
    } catch (e) {
      setError(e instanceof ApiError ? e.message : 'Failed to load customers')
    } finally {
      setLoading(false)
    }
  }

  useEffect(() => {
    void load()
  }, [])

  async function confirmDelete() {
    if (!pendingDelete) return
    try {
      await deleteCustomer(pendingDelete.customer_id)
      setPendingDelete(null)
      await load()
    } catch (e) {
      setError(e instanceof ApiError ? e.message : 'Failed to delete customer')
      setPendingDelete(null)
    }
  }

  return (
    <div>
      <div className="page-header">
        <h1>Customers</h1>
        <Link to="/customers/new" className="btn">Add customer</Link>
      </div>

      {loading && <div className="status-msg">Loading…</div>}
      {error && <div className="status-msg status-msg--error">{error}</div>}

      {!loading && !error && (
        <table className="table">
          <thead>
            <tr>
              <th>Name</th><th>Email</th><th>Phone</th><th>Segment</th><th>Status</th><th>Actions</th>
            </tr>
          </thead>
          <tbody>
            {customers.map((c) => (
              <tr key={c.customer_id}>
                <td>{c.first_name} {c.last_name}</td>
                <td>{c.email}</td>
                <td>{c.phone ?? '—'}</td>
                <td>{c.segment ?? '—'}</td>
                <td>{c.status ?? '—'}</td>
                <td>
                  <div className="row-actions">
                    <button className="icon-btn" aria-label="View" onClick={() => navigate(`/customers/${c.customer_id}`)}><Eye size={18} /></button>
                    <button className="icon-btn" aria-label="Edit" onClick={() => navigate(`/customers/${c.customer_id}/edit`)}><Pencil size={18} /></button>
                    <button className="icon-btn icon-btn--danger" aria-label="Delete" onClick={() => setPendingDelete(c)}><Trash2 size={18} /></button>
                  </div>
                </td>
              </tr>
            ))}
            {customers.length === 0 && (
              <tr><td colSpan={6} className="status-msg">No customers yet.</td></tr>
            )}
          </tbody>
        </table>
      )}

      <ConfirmDialog
        open={pendingDelete !== null}
        title="Delete customer"
        message={pendingDelete ? `Delete ${pendingDelete.first_name} ${pendingDelete.last_name}? This cannot be undone.` : ''}
        onConfirm={confirmDelete}
        onCancel={() => setPendingDelete(null)}
      />
    </div>
  )
}
```

- [ ] **Step 5: Run tests to verify they pass**

Run:
```bash
npx vitest run src/pages/CustomersPage.test.tsx
```
Expected: PASS (3 tests).

- [ ] **Step 6: Commit**

```bash
cd /Users/jerome/Documents/Code/stream_house_c360_demo
git add apps/frontend/src/components/ConfirmDialog.tsx apps/frontend/src/pages/CustomersPage.tsx apps/frontend/src/pages/CustomersPage.test.tsx
git commit -m "feat: add customers table page with delete flow"
```

---

## Task 7: Customer detail page

**Files:**
- Modify (replace stub): `apps/frontend/src/pages/CustomerDetailPage.tsx`
- Test: `apps/frontend/src/pages/CustomerDetailPage.test.tsx`

**Interfaces:**
- Consumes: `getCustomer` from `../api/customers`; react-router `useParams`, `Link`.
- Produces: `CustomerDetailPage` default export.

- [ ] **Step 1: Write the failing tests**

Create `apps/frontend/src/pages/CustomerDetailPage.test.tsx`:
```tsx
import { render, screen } from '@testing-library/react'
import { MemoryRouter, Route, Routes } from 'react-router-dom'
import { afterEach, describe, expect, it, vi } from 'vitest'
import * as api from '../api/customers'
import { ApiError } from '../api/client'
import CustomerDetailPage from './CustomerDetailPage'

const customer = {
  customer_id: '1', first_name: 'Ada', last_name: 'Lovelace', email: 'ada@x.io',
  phone: '555', segment: 'VIP', status: 'ACTIVE', country: 'US',
  customer_since: '2020-01-01', created_at: '', updated_at: '',
}

afterEach(() => vi.restoreAllMocks())

function renderAt(id: string) {
  return render(
    <MemoryRouter initialEntries={[`/customers/${id}`]}>
      <Routes>
        <Route path="/customers/:id" element={<CustomerDetailPage />} />
      </Routes>
    </MemoryRouter>,
  )
}

describe('CustomerDetailPage', () => {
  it('renders the customer fields', async () => {
    vi.spyOn(api, 'getCustomer').mockResolvedValue(customer as never)
    renderAt('1')
    expect(await screen.findByText('ada@x.io')).toBeInTheDocument()
    expect(screen.getByText(/Ada/)).toBeInTheDocument()
  })

  it('shows the error message on 404', async () => {
    vi.spyOn(api, 'getCustomer').mockRejectedValue(new ApiError(404, 'Customer not found'))
    renderAt('nope')
    expect(await screen.findByText(/Customer not found/)).toBeInTheDocument()
  })
})
```

- [ ] **Step 2: Run tests to verify they fail**

Run:
```bash
cd /Users/jerome/Documents/Code/stream_house_c360_demo/apps/frontend
npx vitest run src/pages/CustomerDetailPage.test.tsx
```
Expected: FAIL — stub page.

- [ ] **Step 3: Implement CustomerDetailPage**

Replace `apps/frontend/src/pages/CustomerDetailPage.tsx`:
```tsx
import { useEffect, useState } from 'react'
import { Link, useParams } from 'react-router-dom'
import { ApiError } from '../api/client'
import { getCustomer } from '../api/customers'
import type { Customer } from '../types/customer'

const FIELDS: [keyof Customer, string][] = [
  ['first_name', 'First name'],
  ['last_name', 'Last name'],
  ['email', 'Email'],
  ['phone', 'Phone'],
  ['date_of_birth', 'Date of birth'],
  ['gender', 'Gender'],
  ['address_line1', 'Address line 1'],
  ['address_line2', 'Address line 2'],
  ['city', 'City'],
  ['state', 'State'],
  ['postal_code', 'Postal code'],
  ['country', 'Country'],
  ['customer_since', 'Customer since'],
  ['segment', 'Segment'],
  ['status', 'Status'],
]

export default function CustomerDetailPage() {
  const { id } = useParams<{ id: string }>()
  const [customer, setCustomer] = useState<Customer | null>(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)

  useEffect(() => {
    if (!id) return
    setLoading(true)
    setError(null)
    getCustomer(id)
      .then(setCustomer)
      .catch((e) => setError(e instanceof ApiError ? e.message : 'Failed to load customer'))
      .finally(() => setLoading(false))
  }, [id])

  if (loading) return <div className="status-msg">Loading…</div>
  if (error) return <div className="status-msg status-msg--error">{error}</div>
  if (!customer) return null

  return (
    <div className="detail">
      <div className="page-header">
        <h1>{customer.first_name} {customer.last_name}</h1>
        <div style={{ display: 'flex', gap: 10 }}>
          <Link to={`/customers/${customer.customer_id}/edit`} className="btn">Edit</Link>
          <Link to="/customers" className="btn btn--secondary">Back</Link>
        </div>
      </div>
      <dl>
        {FIELDS.map(([key, label]) => (
          <div key={key} style={{ display: 'contents' }}>
            <dt>{label}</dt>
            <dd>{(customer[key] as string | null) ?? '—'}</dd>
          </div>
        ))}
      </dl>
    </div>
  )
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run:
```bash
npx vitest run src/pages/CustomerDetailPage.test.tsx
```
Expected: PASS (2 tests).

- [ ] **Step 5: Commit**

```bash
cd /Users/jerome/Documents/Code/stream_house_c360_demo
git add apps/frontend/src/pages/CustomerDetailPage.tsx apps/frontend/src/pages/CustomerDetailPage.test.tsx
git commit -m "feat: add customer detail page"
```

---

## Task 8: Customer create/edit form page

**Files:**
- Modify (replace stub): `apps/frontend/src/pages/CustomerFormPage.tsx`
- Test: `apps/frontend/src/pages/CustomerFormPage.test.tsx`

**Interfaces:**
- Consumes: `getCustomer`, `createCustomer`, `updateCustomer` from `../api/customers`; react-router `useParams`, `useNavigate`.
- Produces: `CustomerFormPage` default export (handles both create and edit).

- [ ] **Step 1: Write the failing tests**

Create `apps/frontend/src/pages/CustomerFormPage.test.tsx`:
```tsx
import { render, screen, waitFor } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { MemoryRouter, Route, Routes } from 'react-router-dom'
import { afterEach, describe, expect, it, vi } from 'vitest'
import * as api from '../api/customers'
import CustomerFormPage from './CustomerFormPage'

afterEach(() => vi.restoreAllMocks())

function renderAt(path: string) {
  return render(
    <MemoryRouter initialEntries={[path]}>
      <Routes>
        <Route path="/customers/new" element={<CustomerFormPage />} />
        <Route path="/customers/:id/edit" element={<CustomerFormPage />} />
        <Route path="/customers" element={<div>list</div>} />
      </Routes>
    </MemoryRouter>,
  )
}

describe('CustomerFormPage', () => {
  it('validates required fields before submitting (create mode)', async () => {
    const create = vi.spyOn(api, 'createCustomer').mockResolvedValue({} as never)
    renderAt('/customers/new')
    await userEvent.click(screen.getByRole('button', { name: /save/i }))
    expect(await screen.findByText(/first name is required/i)).toBeInTheDocument()
    expect(create).not.toHaveBeenCalled()
  })

  it('creates a customer and sends empty optionals as null', async () => {
    const create = vi.spyOn(api, 'createCustomer').mockResolvedValue({} as never)
    renderAt('/customers/new')
    await userEvent.type(screen.getByLabelText(/first name/i), 'Ada')
    await userEvent.type(screen.getByLabelText(/last name/i), 'Lovelace')
    await userEvent.type(screen.getByLabelText(/email/i), 'ada@x.io')
    await userEvent.click(screen.getByRole('button', { name: /save/i }))
    await waitFor(() => expect(create).toHaveBeenCalledTimes(1))
    const payload = create.mock.calls[0][0]
    expect(payload).toMatchObject({ first_name: 'Ada', last_name: 'Lovelace', email: 'ada@x.io' })
    expect(payload.phone).toBeNull()
  })

  it('loads an existing customer and updates via PUT in edit mode', async () => {
    vi.spyOn(api, 'getCustomer').mockResolvedValue({
      customer_id: '1', first_name: 'Ada', last_name: 'Lovelace', email: 'ada@x.io',
      phone: '555', country: 'US', customer_since: '2020-01-01', created_at: '', updated_at: '',
    } as never)
    const update = vi.spyOn(api, 'updateCustomer').mockResolvedValue({} as never)
    const create = vi.spyOn(api, 'createCustomer')
    renderAt('/customers/1/edit')
    expect(await screen.findByDisplayValue('Ada')).toBeInTheDocument()
    await userEvent.clear(screen.getByLabelText(/city/i))
    await userEvent.type(screen.getByLabelText(/city/i), 'London')
    await userEvent.click(screen.getByRole('button', { name: /save/i }))
    await waitFor(() => expect(update).toHaveBeenCalledWith('1', expect.objectContaining({ city: 'London' })))
    expect(create).not.toHaveBeenCalled()
  })
})
```

- [ ] **Step 2: Run tests to verify they fail**

Run:
```bash
cd /Users/jerome/Documents/Code/stream_house_c360_demo/apps/frontend
npx vitest run src/pages/CustomerFormPage.test.tsx
```
Expected: FAIL — stub page.

- [ ] **Step 3: Implement CustomerFormPage**

Replace `apps/frontend/src/pages/CustomerFormPage.tsx`:
```tsx
import { useEffect, useState, type FormEvent } from 'react'
import { useNavigate, useParams } from 'react-router-dom'
import { ApiError } from '../api/client'
import { createCustomer, getCustomer, updateCustomer } from '../api/customers'
import type { CustomerCreate } from '../types/customer'

type FormState = Record<keyof CustomerCreate, string>

const EMPTY: FormState = {
  first_name: '', last_name: '', email: '', phone: '', date_of_birth: '', gender: '',
  address_line1: '', address_line2: '', city: '', state: '', postal_code: '',
  country: 'US', customer_since: '', segment: '', status: 'ACTIVE',
}

const TEXT_FIELDS: [keyof CustomerCreate, string, string?][] = [
  ['first_name', 'First name'],
  ['last_name', 'Last name'],
  ['email', 'Email', 'email'],
  ['phone', 'Phone'],
  ['date_of_birth', 'Date of birth', 'date'],
  ['gender', 'Gender'],
  ['address_line1', 'Address line 1'],
  ['address_line2', 'Address line 2'],
  ['city', 'City'],
  ['state', 'State'],
  ['postal_code', 'Postal code'],
  ['country', 'Country'],
  ['customer_since', 'Customer since', 'date'],
  ['segment', 'Segment'],
  ['status', 'Status'],
]

function toPayload(form: FormState): CustomerCreate {
  const out = {} as Record<string, string | null>
  for (const key of Object.keys(form) as (keyof FormState)[]) {
    const value = form[key].trim()
    out[key] = value === '' ? null : value
  }
  // Required fields are guaranteed non-null by validation before this runs.
  return out as unknown as CustomerCreate
}

export default function CustomerFormPage() {
  const { id } = useParams<{ id: string }>()
  const navigate = useNavigate()
  const isEdit = Boolean(id)

  const [form, setForm] = useState<FormState>(EMPTY)
  const [errors, setErrors] = useState<Partial<Record<keyof CustomerCreate, string>>>({})
  const [submitError, setSubmitError] = useState<string | null>(null)
  const [loading, setLoading] = useState(isEdit)

  useEffect(() => {
    if (!id) return
    getCustomer(id)
      .then((c) => {
        const next = { ...EMPTY }
        for (const key of Object.keys(EMPTY) as (keyof FormState)[]) {
          const v = c[key]
          next[key] = v == null ? '' : String(v)
        }
        setForm(next)
      })
      .catch((e) => setSubmitError(e instanceof ApiError ? e.message : 'Failed to load customer'))
      .finally(() => setLoading(false))
  }, [id])

  function validate(): boolean {
    const next: Partial<Record<keyof CustomerCreate, string>> = {}
    if (!form.first_name.trim()) next.first_name = 'First name is required'
    if (!form.last_name.trim()) next.last_name = 'Last name is required'
    if (!form.email.trim()) next.email = 'Email is required'
    setErrors(next)
    return Object.keys(next).length === 0
  }

  async function onSubmit(e: FormEvent) {
    e.preventDefault()
    setSubmitError(null)
    if (!validate()) return
    const payload = toPayload(form)
    try {
      if (isEdit && id) {
        await updateCustomer(id, payload)
      } else {
        await createCustomer(payload)
      }
      navigate('/customers')
    } catch (err) {
      setSubmitError(err instanceof ApiError ? err.message : 'Failed to save customer')
    }
  }

  if (loading) return <div className="status-msg">Loading…</div>

  return (
    <div>
      <div className="page-header">
        <h1>{isEdit ? 'Edit customer' : 'New customer'}</h1>
      </div>
      <form className="form" onSubmit={onSubmit} noValidate>
        {TEXT_FIELDS.map(([key, label, type]) => (
          <div className="form__row" key={key}>
            <label htmlFor={key}>{label}</label>
            <input
              id={key}
              type={type ?? 'text'}
              value={form[key]}
              onChange={(e) => setForm({ ...form, [key]: e.target.value })}
            />
            {errors[key] && <span className="field-error">{errors[key]}</span>}
          </div>
        ))}
        {submitError && <div className="form__error">{submitError}</div>}
        <div className="form__actions">
          <button type="submit" className="btn">Save</button>
          <button type="button" className="btn btn--secondary" onClick={() => navigate('/customers')}>Cancel</button>
        </div>
      </form>
    </div>
  )
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run:
```bash
npx vitest run src/pages/CustomerFormPage.test.tsx
```
Expected: PASS (3 tests).

- [ ] **Step 5: Run the full frontend suite + typecheck + build**

Run:
```bash
npx vitest run
npx tsc --noEmit
npm run build
```
Expected: all tests pass; no type errors; build succeeds.

- [ ] **Step 6: Commit**

```bash
cd /Users/jerome/Documents/Code/stream_house_c360_demo
git add apps/frontend/src/pages/CustomerFormPage.tsx apps/frontend/src/pages/CustomerFormPage.test.tsx
git commit -m "feat: add customer create/edit form page"
```

---

## Task 9: Frontend env + README

**Files:**
- Create: `apps/frontend/.env.example`
- Create: `apps/frontend/README.md`

**Interfaces:**
- Produces: documented `VITE_API_BASE_URL` and run instructions.

- [ ] **Step 1: Add env example**

Create `apps/frontend/.env.example`:
```
# Base URL of the C360 backend REST API
VITE_API_BASE_URL=http://localhost:8000/api/v1
```

- [ ] **Step 2: Write README**

Create `apps/frontend/README.md`:
```markdown
# C360 Frontend

Vite + React + TypeScript UI for the Customer 360 backend.

## Setup

```bash
npm install
cp .env.example .env   # adjust VITE_API_BASE_URL if needed
```

## Develop

```bash
npm run dev    # http://localhost:5173
```

The backend must be running (default `http://localhost:8000`) with CORS
allowing `http://localhost:5173`.

## Test / build

```bash
npm test
npm run build
```

## Structure

- `src/api/` — typed fetch client and customers CRUD module
- `src/components/` — AppLayout, Header, Sidebar, ConfirmDialog
- `src/pages/` — Customers table, detail, create/edit form, placeholders
- `src/types/` — Customer types mirroring the backend schema
```

- [ ] **Step 3: Commit**

```bash
cd /Users/jerome/Documents/Code/stream_house_c360_demo
git add apps/frontend/.env.example apps/frontend/README.md
git commit -m "docs: add frontend env example and readme"
```

---

## Task 10: Enable CORS on the backend

**Files:**
- Modify: `apps/backend/main.py`
- Test: `apps/backend/tests/test_cors.py`

**Interfaces:**
- Consumes: FastAPI app from `main.py`.
- Produces: CORS responses including `access-control-allow-origin` for the dev origin.

- [ ] **Step 1: Write the failing test**

Create `apps/backend/tests/test_cors.py`:
```python
"""CORS must allow the Vite dev origin."""

from fastapi.testclient import TestClient

from main import app

client = TestClient(app)


def test_cors_allows_dev_origin():
    resp = client.get(
        "/health",
        headers={"Origin": "http://localhost:5173"},
    )
    assert resp.status_code == 200
    assert resp.headers.get("access-control-allow-origin") == "http://localhost:5173"


def test_cors_preflight_for_customers():
    resp = client.options(
        "/api/v1/customers",
        headers={
            "Origin": "http://localhost:5173",
            "Access-Control-Request-Method": "POST",
        },
    )
    assert resp.status_code in (200, 204)
    assert resp.headers.get("access-control-allow-origin") == "http://localhost:5173"
```

- [ ] **Step 2: Run the test to verify it fails**

Run:
```bash
cd /Users/jerome/Documents/Code/stream_house_c360_demo/apps/backend
uv run pytest tests/test_cors.py -v
```
Expected: FAIL — no `access-control-allow-origin` header (CORS not configured).

Note: `TestClient` triggers the lifespan (which inits the Postgres/Kafka sink). If these tests cannot reach a DB in the execution environment, mirror the setup already used by the existing `tests/` (check `tests/conftest.py`) — e.g. set `SINK` to a test value or reuse their fixtures — rather than inventing a new one.

- [ ] **Step 3: Add CORS middleware**

Modify `apps/backend/main.py` — add the import and middleware registration after `app = FastAPI(...)`:
```python
from fastapi.middleware.cors import CORSMiddleware

# ... after app = FastAPI(...)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173"],
    allow_methods=["GET", "POST", "PUT", "DELETE", "OPTIONS"],
    allow_headers=["*"],
)
```

- [ ] **Step 4: Run the test to verify it passes**

Run:
```bash
uv run pytest tests/test_cors.py -v
```
Expected: PASS (2 tests).

- [ ] **Step 5: Run the full backend suite**

Run:
```bash
uv run pytest -v
```
Expected: no regressions (pre-existing failures unrelated to CORS, if any, are out of scope — note them).

- [ ] **Step 6: Commit**

```bash
cd /Users/jerome/Documents/Code/stream_house_c360_demo
git add apps/backend/main.py apps/backend/tests/test_cors.py
git commit -m "feat: enable CORS for the frontend dev origin"
```

---

## Task 11: End-to-end manual verification

**Files:** none (verification only).

- [ ] **Step 1: Start the backend**

Run (in one terminal):
```bash
cd /Users/jerome/Documents/Code/stream_house_c360_demo/apps/backend
uv run uvicorn main:app --reload --port 8000
```
Confirm `http://localhost:8000/health` returns `{"status":"ok",...}`.

- [ ] **Step 2: Start the frontend**

Run (in another terminal):
```bash
cd /Users/jerome/Documents/Code/stream_house_c360_demo/apps/frontend
npm run dev
```
Open `http://localhost:5173`.

- [ ] **Step 3: Exercise the full flow**

Verify, in the browser:
- `/customers` lists customers from the backend (no CORS error in console).
- View icon opens `/customers/:id` detail and shows fields.
- Edit icon opens the populated form; saving returns to the list with the change applied.
- "Add customer" creates a record (required-field validation blocks empty submit).
- Delete icon opens the confirm dialog; confirming removes the row.
- Sidebar: Accounts / Transactions show the placeholder page.

- [ ] **Step 4: Record the result**

Note any discrepancies. If all pass, the feature is complete.

---

## Self-Review Notes

- **Spec coverage:** layout/header/sidebar (Task 5), customers table with view/edit/delete icons (Task 6), detail page (Task 7), create+edit form (Task 8), routes incl. placeholders (Task 5), data layer (Tasks 2–4), CORS backend change (Task 10), env/readme (Task 9), testing (every task + Task 11). All spec sections mapped.
- **Review Focus coverage:** non-2xx (Task 3 + 6/7), 204 delete (Task 3), create-vs-update dispatch (Task 8), empty optionals → null (Task 8), CORS from dev origin (Task 10). All pinned.
- **Type consistency:** `request`/`ApiError` (Task 3) consumed verbatim in Tasks 4/6/7/8; `Customer`/`CustomerCreate`/`CustomerUpdate` (Task 2) used consistently; `ConfirmDialog` prop names match between Task 6 definition and usage.
