"""C360 Backend — application entrypoint."""

import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

import bootstrap
from api.account_resource import router as account_router
from api.config_resource import router as config_router
from api.customer_resource import router as customers_router
from api.transaction_resource import router as transaction_router
from config import settings
from customers import inventory as customers_inventory

logger = logging.getLogger("c360.main")


@asynccontextmanager
async def lifespan(app: FastAPI):
    if settings.SINK == "postgres":
        bootstrap.init_and_seed()
    elif settings.SINK == "kafka":
        customers = customers_inventory.load()
        logger.info("Kafka sink ready — %d customers loaded from CSV", len(customers))
    yield


app = FastAPI(title="C360 Backend", version="0.1.0", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173"],
    allow_methods=["GET", "POST", "PUT", "DELETE", "OPTIONS"],
    allow_headers=["*"],
)

app.include_router(customers_router, prefix="/api/v1")
app.include_router(account_router, prefix="/api/v1")
app.include_router(transaction_router, prefix="/api/v1")
app.include_router(config_router, prefix="/api/v1")


@app.get("/health")
def health():
    return {"status": "ok", "sink": settings.SINK}
