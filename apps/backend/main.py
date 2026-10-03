"""C360 Backend — application entrypoint."""

import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from api.customer_resource import router as customers_router
from config import settings
from customers import db_sink, inventory

logger = logging.getLogger("c360.main")


@asynccontextmanager
async def lifespan(app: FastAPI):
    if settings.SINK == "postgres":
        db_sink.init_db()
        customers = inventory.load()
        db_sink.seed_from_csv(customers)
        logger.info("Postgres sink ready — schema initialised and seeded if empty")
    elif settings.SINK == "kafka":
        customers = inventory.load()
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


@app.get("/health")
def health():
    return {"status": "ok", "sink": settings.SINK}
