# A Fast API Backend to simulate different microservices

The goal is to offer REST APIs on customers, accounts, transactions resources. The backend, FAST API based implements the following architecture.

![](../../docs/diagrams/backend.drawio.png)


## Customers CRU operations

## Accounts CRU operations


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