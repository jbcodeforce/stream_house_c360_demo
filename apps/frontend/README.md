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

- `src/api/` — typed fetch client, customers CRUD module, and config module
- `src/components/` — AppLayout, Header, Sidebar, ConfirmDialog
- `src/pages/` — Customers table, detail, create/edit form, placeholders, and
  the Settings page (toggle Kafka event production on create/update/delete)
- `src/types/` — Customer and AppConfig types mirroring the backend schema
