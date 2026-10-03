# A Fast API Backend to simulate different microservices

The goal is to offer REST APIs on customers, accounts, transactions resources. The backend, FAST API based implements the following architecture.

![](../../docs/diagrams/backend.drawio.png)

Each component is isolated in term of code base so it can be packaged, if needed as microservice. 
 
The structure is the same for all compoents:

* REST API resource 
* Snativeupports two write sinks controlled by a single `SINK` environment variable:

| `SINK` value | Write path | Read path |
|---|---|---|
| `postgres` *(default)* | INSERT / UPDATE RDS PostgreSQL | SELECT from PostgreSQL |
| `kafka` | Produce Avro message to Confluent topic | Read from in-memory CSV |

* CVS bootstrap for controlled data. When `SINK=postgres`, the backend auto-creates the schema on startup.  When `SINK=kafka`, the CSV remains the in-memory read store for all GET requests.
* Centralise all environment-driven settings in one place so every module imports from `config` rather than calling `os.environ` directly.
* When `SINK=kafka`, write operations produce Avro-serialised messages to the Confluent topic using the Confluent Schema Registry. This bypasses CDC and lets the demo run without an RDS instance.

* Define the canonical Pydantic schema for a Customer, matching the PostgreSQL table exactly (field names, types, optionality). This model is shared by all three layers: API request/response, database writes, and Kafka Avro payload.
* Inventory pattern to support in-memory data for demo
* Leverage a thin orchestration layer that dispatches CRU operations to the correct sink and always uses `inventory.py` for reads when `SINK=kafka`.
* Expose the Customers CRU endpoints as a FastAPI `APIRouter`. The router delegates all logic to `service.py` and handles HTTP status codes.
* Local postgres server for integration tests and demonstration
* The `main.py` wires the FastAPI app, include routers, and run startup lifecycle hooks (schema creation + CSV seeding for `SINK=postgres`; CSV cache load for `SINK=kafka`).
    ```sh
    uv run uvicorn main:app --reload
    ```

* TheFastAPI backend under `apps/backend/`
* This is a `uv` managed project

## Customers CRUD operations

Exposes a **Customers CRU REST API** and 

The seed file (`customers/data/customers.csv`) ships with 10 pre-populated customers. 

**Expected Outcomes:**
- `POST /customers` → 201 Created, returns `Customer`
- `GET /customers` → 200, returns `list[Customer]`
- `GET /customers/{customer_id}` → 200 or 404
- `PUT /customers/{customer_id}` → 200 or 404

## Accounts CRU operations


## Runtime config

A runtime, user-editable flag controls whether customer writes also emit a
Kafka event (dual-write) when Postgres is the sink. It is persisted to a JSON
file (`RUNTIME_CONFIG_FILE`, default `runtime_config.json`) and defaults to
`false`.

- `GET /config` → 200, returns `{ "kafka_produce_enabled": bool }`
- `PUT /config` body `{ "kafka_produce_enabled": bool }` → 200, persisted

When enabled, `create`/`update`/`delete` on the Postgres path emit a
best-effort Kafka event (`c`/`u`/`d`); a produce failure is logged and never
fails the DB operation. The `SINK=kafka` path is unaffected.

## Testing

1. Start the container
```sh
cd apps/backend
./start_local_pg_server.sh
```

2. Point the backend at it
```sh
cp .env.local .env
```

3. Start the API (auto-creates schema + seeds 10 customers on first boot)
```sh
uv run uvicorn main:app --reload
```

4. Access the REST API at [http://localhost:8000/docs](http://localhost:8000/docs) for the Swagger UI or http://localhost:8000/api/v1/customers

### Integration tests

```sh
uv run pytest -v tests/
```