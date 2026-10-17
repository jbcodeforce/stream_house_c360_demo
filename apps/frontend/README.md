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
- `src/pages/` — Customers and Accounts (table, detail, create/edit form),
  and the Transactions placeholder
- `src/types/` — Customer and Account types mirroring the backend
